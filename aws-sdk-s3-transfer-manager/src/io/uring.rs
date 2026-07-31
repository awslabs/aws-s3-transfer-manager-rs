/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! O_DIRECT positioned-write path for download-to-file (Linux only).
//!
//! Bypasses the page cache (`O_DIRECT`) using positioned writes against a
//! descriptor opened with `O_DIRECT`. Network `Bytes` segments are aligned at
//! receive time: the body reader accumulates each part into one page-aligned
//! [`AlignedBuf`], so a part reaches the write path as a single segment whose
//! address AND length are page multiples and can be passed directly to the
//! kernel — no staging-buffer copy, no io_uring.
//!
//! Alignment is checked upfront: a write whose file position or length is not a
//! multiple of the direct-I/O alignment falls back to the buffered `pwritev`
//! path on a separate plain descriptor for the same file. In practice only the
//! final (tail) run of a download and unaligned range-GET starts take the
//! fallback; every full part-sized run goes direct.

use bytes::Buf;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

/// Direct-I/O alignment for file position, length, and buffer address.
///
/// O_DIRECT requires alignment to the logical block size of the underlying
/// device (queryable via statx `stx_dio_offset_align`, kernel 6.1+). 4096
/// covers both 512e and 4Kn devices and matches the page size on most
/// platforms, so it is used as a safe static bound.
pub(crate) const DIRECT_IO_ALIGN: u64 = 4096;

/// Whether the O_DIRECT download write path is requested.
///
/// Single source of truth: both the sink selection (`make_file_sink`) and the
/// receive-time alignment in the body reader gate on this, so the two can never
/// disagree. Disagreement would hand unaligned buffers to an O_DIRECT sink.
pub(crate) fn direct_io_enabled() -> bool {
    std::env::var_os("S3_TM_DIRECT_IO").is_some_and(|v| v == "1")
}

/// A growable heap buffer whose base address is always page-aligned.
///
/// Used to accumulate a whole part's worth of network segments into one
/// contiguous, page-aligned allocation. O_DIRECT constrains the buffer address,
/// the file offset, AND the length — so a part must arrive at the write path as
/// a single segment of page-multiple length, not as a chain of arbitrarily sized
/// network chunks.
pub(crate) struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

// The allocation is owned exclusively by this struct (no aliasing).
unsafe impl Send for AlignedBuf {}
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    fn layout_for(cap: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(cap, DIRECT_IO_ALIGN as usize)
            .expect("capacity within isize::MAX with page alignment")
    }

    /// Allocate with room for `cap` bytes (rounded up to a page multiple).
    pub(crate) fn with_capacity(cap: usize) -> Self {
        let cap = cap.next_multiple_of(DIRECT_IO_ALIGN as usize).max(DIRECT_IO_ALIGN as usize);
        let layout = Self::layout_for(cap);
        // SAFETY: layout has non-zero size.
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self { ptr, len: 0, cap }
    }

    /// Grow to hold at least `needed` total bytes, preserving contents.
    fn reserve(&mut self, needed: usize) {
        if needed <= self.cap {
            return;
        }
        let new_cap = needed
            .max(self.cap * 2)
            .next_multiple_of(DIRECT_IO_ALIGN as usize);
        let new_layout = Self::layout_for(new_cap);
        // SAFETY: new_layout has non-zero size.
        let new_ptr = unsafe { std::alloc::alloc(new_layout) };
        if new_ptr.is_null() {
            std::alloc::handle_alloc_error(new_layout);
        }
        // SAFETY: both allocations are valid for self.len bytes and disjoint.
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, new_ptr, self.len);
            std::alloc::dealloc(self.ptr, Self::layout_for(self.cap));
        }
        self.ptr = new_ptr;
        self.cap = new_cap;
    }

    /// Append `src`, growing the allocation if needed.
    pub(crate) fn extend_from_slice(&mut self, src: &[u8]) {
        if src.is_empty() {
            return;
        }
        self.reserve(self.len + src.len());
        // SAFETY: reserve() guarantees capacity for len + src.len() bytes.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.ptr.add(self.len), src.len()) };
        self.len += src.len();
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Freeze into a `Bytes` that keeps the page-aligned allocation alive.
    ///
    /// The resulting `Bytes` has a page-aligned address; its length is whatever
    /// was accumulated. A page-multiple length means the write path can hand it
    /// straight to an O_DIRECT `write()`; otherwise (the object's tail part) the
    /// sink's upfront alignment check routes the run to the buffered fallback.
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        if self.len == 0 {
            return bytes::Bytes::new();
        }
        bytes::Bytes::from_owner(self)
    }
}

impl AsRef<[u8]> for AlignedBuf {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        // SAFETY: ptr is valid for len initialized bytes and exclusively owned.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout in with_capacity()/reserve().
        unsafe { std::alloc::dealloc(self.ptr, Self::layout_for(self.cap)) };
    }
}

/// O_DIRECT write target for a single output file.
///
/// Holds two descriptors for the same file: one opened with `O_DIRECT` for
/// aligned writes, and the original plain descriptor for unaligned fallback
/// writes (mixing direct and buffered I/O on disjoint byte ranges of the same
/// file is safe). Construction fails if the filesystem rejects `O_DIRECT`
/// (e.g. tmpfs) so callers can fall back to the buffered sink entirely.
pub(crate) struct UringDirectSink {
    direct_file: File,
    fallback_file: File,
    /// Whether the transfer manager created this file (vs caller-provided).
    /// Only an owned file is preallocated, matching the buffered sink.
    owns_file: bool,
    /// Diagnostic counters: how many write calls (and bytes) took the O_DIRECT
    /// path vs the buffered fallback. Reported on drop so a benchmark run can
    /// confirm the direct path was actually exercised.
    direct_writes: std::sync::atomic::AtomicU64,
    direct_bytes: std::sync::atomic::AtomicU64,
    fallback_writes: std::sync::atomic::AtomicU64,
    fallback_bytes: std::sync::atomic::AtomicU64,
}

impl Drop for UringDirectSink {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering::Relaxed;
        let dw = self.direct_writes.load(Relaxed);
        let db = self.direct_bytes.load(Relaxed);
        let fw = self.fallback_writes.load(Relaxed);
        let fb = self.fallback_bytes.load(Relaxed);
        let total = db + fb;
        tracing::info!(
            direct_writes = dw,
            direct_bytes = db,
            direct_mib = db as f64 / (1024.0 * 1024.0),
            fallback_writes = fw,
            fallback_bytes = fb,
            fallback_mib = fb as f64 / (1024.0 * 1024.0),
            direct_pct = if total > 0 {
                db as f64 / total as f64 * 100.0
            } else {
                0.0
            },
            "direct I/O sink write summary"
        );
    }
}

impl std::fmt::Debug for UringDirectSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringDirectSink").finish_non_exhaustive()
    }
}

impl UringDirectSink {
    /// Build a sink from `fallback_file` (an already-open plain descriptor for
    /// the output file). A second descriptor for the same file is opened with
    /// `O_DIRECT` via `/proc/self/fd`, so no path is required and the sink
    /// works for caller-provided files.
    ///
    /// On failure (filesystem does not support `O_DIRECT`, e.g. tmpfs; or
    /// io_uring unavailable) the error is returned together with
    /// `fallback_file` so the caller can construct a buffered sink instead.
    pub(crate) fn new(fallback_file: File, owns_file: bool) -> Result<Self, (io::Error, File)> {
        use std::os::unix::fs::OpenOptionsExt;
        // Reopen through the procfs magic symlink: yields a fresh open file
        // description for the same inode with its own status flags.
        let proc_path = format!("/proc/self/fd/{}", fallback_file.as_raw_fd());
        let direct_file = match std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(proc_path)
        {
            Ok(f) => f,
            Err(e) => return Err((e, fallback_file)),
        };

        Ok(Self {
            direct_file,
            fallback_file,
            owns_file,
            direct_writes: std::sync::atomic::AtomicU64::new(0),
            direct_bytes: std::sync::atomic::AtomicU64::new(0),
            fallback_writes: std::sync::atomic::AtomicU64::new(0),
            fallback_bytes: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Whether a positioned write of `len` bytes at `pos` satisfies the
    /// O_DIRECT constraints. Buffer address alignment is supplied by the
    /// receive-time [`AlignedBuf`] accumulation; only position and length are
    /// checked here.
    fn is_aligned(pos: u64, len: u64) -> bool {
        pos % DIRECT_IO_ALIGN == 0 && len % DIRECT_IO_ALIGN == 0 && len > 0
    }

    /// Direct vs buffered-fallback write totals: `(direct_writes,
    /// direct_bytes, fallback_writes, fallback_bytes)`. Lets a caller report
    /// the split at a deterministic point instead of relying on `Drop`.
    #[allow(dead_code)]
    pub(crate) fn write_stats(&self) -> (u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.direct_writes.load(Relaxed),
            self.direct_bytes.load(Relaxed),
            self.fallback_writes.load(Relaxed),
            self.fallback_bytes.load(Relaxed),
        )
    }
    /// Write aligned segments directly to the O_DIRECT descriptor.
    ///
    /// Precondition: every segment in `buf` is page-aligned in BOTH address and
    /// length. The body reader guarantees this by accumulating each part into a
    /// single [`AlignedBuf`]; a part whose length is not a page multiple (the
    /// object's tail) is rejected by [`Self::is_aligned`] before reaching here
    /// and takes the buffered fallback instead.
    ///
    /// The checks are real returns, not `debug_assert`s: a violation in a release
    /// build would otherwise surface as a bare `EINVAL` from the kernel with no
    /// indication of which constraint was broken.
    fn write_direct(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        mut pos: u64,
    ) -> io::Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let mut f = self.direct_file.try_clone()?;

        while buf.has_remaining() {
            let chunk = buf.chunk();
            let len = chunk.len();
            let addr = chunk.as_ptr() as u64;

            if addr % DIRECT_IO_ALIGN != 0 || len as u64 % DIRECT_IO_ALIGN != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "O_DIRECT requires page-aligned address and length, got \
                         addr%{align}={addr_rem} len={len} (len%{align}={len_rem}) at pos={pos}",
                        align = DIRECT_IO_ALIGN,
                        addr_rem = addr % DIRECT_IO_ALIGN,
                        len_rem = len as u64 % DIRECT_IO_ALIGN,
                    ),
                ));
            }

            f.seek(SeekFrom::Start(pos))?;
            f.write_all(chunk)?;

            pos += len as u64;
            buf.advance(len);
        }
        Ok(())
    }
}

impl crate::operation::download::body::SinkWrite for UringDirectSink {
    fn write_all_at(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        pos: u64,
    ) -> io::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let len = buf.remaining() as u64;
        // Alignment decided upfront: unaligned writes (the tail run, and
        // range-GETs starting mid-block) never attempt O_DIRECT.
        if Self::is_aligned(pos, len) {
            let n = self.direct_writes.fetch_add(1, Relaxed);
            let total = self.direct_bytes.fetch_add(len, Relaxed) + len;
            if n == 0 {
                // Deterministic proof the direct path is live. Drop-based
                // reporting is unreliable: the sink can outlive the process's
                // output machinery at exit.
                println!("[TM] first O_DIRECT write issued: pos={pos} len={len}");
            }
            tracing::trace!(
                pos,
                len,
                direct_writes = n + 1,
                direct_bytes = total,
                "O_DIRECT write"
            );
            self.write_direct(buf, pos)
        } else {
            let n = self.fallback_writes.fetch_add(1, Relaxed);
            let total = self.fallback_bytes.fetch_add(len, Relaxed) + len;
            if n == 0 {
                println!(
                    "[TM] first buffered-fallback write: pos={pos} len={len} \
                     (pos_aligned={} len_aligned={}) -- this data goes through the page cache",
                    pos % DIRECT_IO_ALIGN == 0,
                    len % DIRECT_IO_ALIGN == 0,
                );
            }
            tracing::debug!(
                pos,
                len,
                fallback_writes = n + 1,
                fallback_bytes = total,
                "unaligned write takes buffered fallback"
            );
            crate::io::fs::write_all_at(&self.fallback_file, buf, pos)
        }
    }

    fn preallocate(&self, len: u64) {
        if self.owns_file {
            match crate::io::fs::preallocate(&self.fallback_file, len) {
                Ok(()) => println!(
                    "[TM] preallocated {len} bytes ({:.2} GiB) -- writes are \
                     overwrites, not size-extending appends",
                    len as f64 / (1024.0 * 1024.0 * 1024.0)
                ),
                Err(e) => {
                    println!(
                        "[TM] preallocate FAILED ({e}) -- writes will extend the file \
                         and serialize on the inode lock"
                    );
                    tracing::warn!(error = %e, "failed to preallocate file space");
                }
            }
        } else {
            println!(
                "[TM] preallocate SKIPPED (caller-owned file) -- writes may extend \
                 the file and serialize on the inode lock"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::body::SinkWrite;
    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;
    use std::io::Write;

    const ALIGN: usize = DIRECT_IO_ALIGN as usize;

    /// Create a test dir on a real (non-tmpfs) filesystem. O_DIRECT is
    /// rejected by tmpfs, which commonly backs /tmp, so tests anchor next to
    /// the build artifacts (the source tree's filesystem).
    fn test_dir() -> tempfile::TempDir {
        let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/direct-io-tests");
        std::fs::create_dir_all(&base).unwrap();
        tempfile::tempdir_in(base).unwrap()
    }

    /// Build a sink for a fresh file, or None when the filesystem does not
    /// support O_DIRECT (skip the test rather than fail).
    fn new_sink(path: &std::path::Path) -> Option<UringDirectSink> {
        let fallback = File::create(path).unwrap();
        match UringDirectSink::new(fallback, true) {
            Ok(sink) => Some(sink),
            Err((e, _file)) => {
                eprintln!("skipping O_DIRECT test: {e}");
                None
            }
        }
    }

    /// Build a page-aligned `Bytes` the way the body reader does.
    fn aligned(src: &[u8]) -> Bytes {
        let mut b = AlignedBuf::with_capacity(src.len());
        b.extend_from_slice(src);
        b.into_bytes()
    }

    fn seg(chunks: &[&[u8]]) -> SegmentedBuf<Bytes> {
        let mut s = SegmentedBuf::new();
        for c in chunks {
            s.push(aligned(c));
        }
        s
    }

    #[test]
    fn aligned_write_goes_direct() {
        let dir = test_dir();
        let path = dir.path().join("direct.bin");
        let Some(sink) = new_sink(&path) else { return };

        let data = vec![0xABu8; ALIGN * 2];
        let mut buf = seg(&[&data]);
        sink.write_all_at(&mut buf, 0).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, data);
    }

    #[test]
    fn aligned_write_at_offset() {
        let dir = test_dir();
        let path = dir.path().join("offset.bin");
        let Some(sink) = new_sink(&path) else { return };

        // Write block 1 first, then block 0 — out-of-order positioned writes.
        let block1 = vec![0xBBu8; ALIGN];
        let block0 = vec![0xAAu8; ALIGN];
        sink.write_all_at(&mut seg(&[&block1]), ALIGN as u64)
            .unwrap();
        sink.write_all_at(&mut seg(&[&block0]), 0).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[..ALIGN], &block0[..]);
        assert_eq!(&contents[ALIGN..], &block1[..]);
    }

    #[test]
    fn aligned_buf_is_page_aligned_and_preserves_content() {
        let data = vec![0xABu8; 8192];
        let b = aligned(&data);
        assert_eq!(b.as_ptr() as usize % ALIGN, 0);
        assert_eq!(&b[..], &data[..]);
    }

    #[test]
    fn aligned_buf_stays_aligned_across_growth() {
        // Accumulate many arbitrarily-sized network-like chunks; the frozen
        // buffer must be one contiguous page-aligned segment.
        let mut b = AlignedBuf::with_capacity(0);
        let mut expected = Vec::new();
        for i in 0..200u32 {
            let chunk = vec![(i % 251) as u8; 1000 + (i as usize % 7)];
            expected.extend_from_slice(&chunk);
            b.extend_from_slice(&chunk);
        }
        assert_eq!(b.len(), expected.len());
        let frozen = b.into_bytes();
        assert_eq!(frozen.as_ptr() as usize % ALIGN, 0);
        assert_eq!(&frozen[..], &expected[..]);
    }

    #[test]
    fn unaligned_length_is_rejected_not_einval() {
        // A page-aligned address with a non-page-multiple length must produce a
        // descriptive error from write_direct rather than a bare EINVAL.
        let dir = test_dir();
        let path = dir.path().join("badlen.bin");
        let Some(sink) = new_sink(&path) else { return };

        let mut buf = SegmentedBuf::new();
        buf.push(aligned(&vec![0u8; ALIGN + 7]));
        let err = sink.write_direct(&mut buf, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("page-aligned"), "{err}");
    }

    #[test]
    fn aligned_segments_written_directly() {
        let dir = test_dir();
        let path = dir.path().join("gather.bin");
        let Some(sink) = new_sink(&path) else { return };

        // Multiple aligned segments written in one call.
        let mut expected = Vec::with_capacity(ALIGN * 4);
        let mut buf = SegmentedBuf::new();
        for i in 0..4u8 {
            let chunk = vec![i; ALIGN];
            expected.extend_from_slice(&chunk);
            buf.push(aligned(&chunk));
        }

        sink.write_all_at(&mut buf, 0).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn write_many_aligned_segments() {
        let dir = test_dir();
        let path = dir.path().join("large.bin");
        let Some(sink) = new_sink(&path) else { return };

        // Many aligned segments written in one write_all_at call.
        let total = ALIGN * 256; // 1 MiB
        assert_eq!(total % ALIGN, 0);
        let mut expected = Vec::with_capacity(total);
        let mut buf = SegmentedBuf::new();
        let seg_size = 64 * 1024; // 64 KiB per segment
        for i in 0..(total / seg_size) {
            let chunk = vec![(i % 251) as u8; seg_size];
            expected.extend_from_slice(&chunk);
            buf.push(aligned(&chunk));
        }

        sink.write_all_at(&mut buf, 0).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn unaligned_tail_takes_fallback() {
        let dir = test_dir();
        let path = dir.path().join("tail.bin");
        let Some(sink) = new_sink(&path) else { return };

        // Aligned body followed by an unaligned tail — the download shape.
        let body = vec![0x11u8; ALIGN * 4];
        let tail = vec![0x22u8; 1234];
        sink.write_all_at(&mut seg(&[&body]), 0).unwrap();
        sink.write_all_at(&mut seg(&[&tail]), body.len() as u64)
            .unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), body.len() + tail.len());
        assert_eq!(&contents[..body.len()], &body[..]);
        assert_eq!(&contents[body.len()..], &tail[..]);
    }

    #[test]
    fn unaligned_position_takes_fallback() {
        let dir = test_dir();
        let path = dir.path().join("unaligned-pos.bin");
        let Some(sink) = new_sink(&path) else { return };

        // Aligned length at an unaligned position (range-GET start shape).
        let data = vec![0x33u8; ALIGN];
        sink.write_all_at(&mut seg(&[&data]), 100).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), 100 + ALIGN);
        assert_eq!(&contents[100..], &data[..]);
    }

    #[test]
    fn empty_write_is_ok() {
        let dir = test_dir();
        let path = dir.path().join("empty.bin");
        let Some(sink) = new_sink(&path) else { return };

        let mut buf = SegmentedBuf::<Bytes>::new();
        sink.write_all_at(&mut buf, 0).unwrap();
        assert!(std::fs::read(&path).unwrap().is_empty());
    }

    #[test]
    fn mixed_direct_and_fallback_full_download_shape() {
        // Simulates a real multi-part download: several aligned part-sized
        // runs written out of order, plus an unaligned tail.
        let dir = test_dir();
        let path = dir.path().join("download.bin");
        let Some(sink) = new_sink(&path) else { return };

        let part = ALIGN * 16; // 64 KiB "parts"
        let tail_len = 5000; // unaligned tail
        let num_parts = 4;
        let total = part * num_parts + tail_len;

        let mut expected = vec![0u8; total];
        // Out-of-order part writes: 2, 0, 3, 1, then the tail.
        for &p in &[2usize, 0, 3, 1] {
            let fill = (p + 1) as u8;
            let data = vec![fill; part];
            expected[p * part..(p + 1) * part].copy_from_slice(&data);
            sink.write_all_at(&mut seg(&[&data]), (p * part) as u64)
                .unwrap();
        }
        let tail = vec![0xEEu8; tail_len];
        expected[num_parts * part..].copy_from_slice(&tail);
        sink.write_all_at(&mut seg(&[&tail]), (num_parts * part) as u64)
            .unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn tmpfs_rejects_o_direct_construction() {
        // /tmp is tmpfs on most Linux hosts; if it isn't here, the
        // construction succeeds and the assertion is skipped.
        let dir = tempfile::tempdir().unwrap(); // honors TMPDIR (/tmp)
        let path = dir.path().join("f.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(b"x").unwrap();
        let fallback = File::options().write(true).open(&path).unwrap();
        match UringDirectSink::new(fallback, true) {
            Err((e, _file)) => assert_eq!(e.raw_os_error(), Some(libc::EINVAL)),
            Ok(_) => eprintln!("TMPDIR filesystem supports O_DIRECT; skipping"),
        }
    }
}
