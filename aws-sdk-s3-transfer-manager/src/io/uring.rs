/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! O_DIRECT + io_uring positioned-write path for download-to-file (Linux only).
//!
//! Bypasses the page cache (`O_DIRECT`) and submits positioned writes through an
//! io_uring instance instead of per-write `pwritev` syscalls. Network `Bytes`
//! segments are not block-aligned, so payloads are gathered into a reusable
//! page-aligned staging buffer before submission — one memcpy in exchange for
//! eliminating the page-cache copy and writeback.
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
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};

/// Direct-I/O alignment for file position, length, and buffer address.
///
/// O_DIRECT requires alignment to the logical block size of the underlying
/// device (queryable via statx `stx_dio_offset_align`, kernel 6.1+). 4096
/// covers both 512e and 4Kn devices and matches the page size on most
/// platforms, so it is used as a safe static bound.
pub(crate) const DIRECT_IO_ALIGN: u64 = 4096;

/// Size of each staging buffer. One drain run is often many parts coalesced, and
/// runs larger than a buffer are written in buffer-sized slices.
const STAGING_BUF_SIZE: usize = 8 * 1024 * 1024;

/// Number of O_DIRECT writes kept in flight per sink, and therefore the number
/// of staging buffers allocated.
///
/// Depth matters because a single write's completion latency bounds throughput
/// when writes are issued one at a time: an 8 MiB write completing in ~0.8 ms
/// caps a serial issuer near 10 GiB/s, and far lower when latency is higher.
/// Keeping several writes outstanding lets the device overlap them.
///
/// The cost is memory: `depth * STAGING_BUF_SIZE` per output file, charged
/// outside the transfer manager's memory budget. The default is deliberately
/// modest; override with `S3_TM_DIRECT_IO_QUEUE_DEPTH`.
const DEFAULT_QUEUE_DEPTH: usize = 16;

/// Resolve the per-sink queue depth, clamped to a sane range.
fn queue_depth() -> usize {
    std::env::var("S3_TM_DIRECT_IO_QUEUE_DEPTH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|d| d.clamp(1, 64))
        .unwrap_or(DEFAULT_QUEUE_DEPTH)
}

/// Page-aligned heap buffer for staging unaligned network segments before an
/// O_DIRECT write.
struct AlignedBuf {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

// The raw pointer is owned exclusively by this struct (no aliasing).
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    fn new(size: usize, align: usize) -> io::Result<Self> {
        let layout = std::alloc::Layout::from_size_align(size, align)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: layout has non-zero size.
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "aligned staging buffer allocation failed",
            ));
        }
        Ok(Self { ptr, layout })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for layout.size() bytes and exclusively owned.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: allocated with the same layout in new().
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}

/// State serialized behind a mutex: the ring and the staging buffers are both
/// single-user resources, and `SinkWrite` is `&self` + `Sync`.
///
/// `staging` holds one buffer per in-flight slot. A slot's buffer is owned by
/// the kernel from the moment its write is submitted until its completion is
/// reaped, so slots cannot share a buffer.
struct Inner {
    ring: IoUring,
    staging: Vec<AlignedBuf>,
    /// Byte count submitted for each slot, to validate the completion result.
    expected: Vec<usize>,
}

/// O_DIRECT + io_uring write target for a single output file.
///
/// Holds two descriptors for the same file: one opened with `O_DIRECT` for
/// aligned writes, and the original plain descriptor for unaligned fallback
/// writes (mixing direct and buffered I/O on disjoint byte ranges of the same
/// file is safe). Construction fails if the filesystem rejects `O_DIRECT`
/// (e.g. tmpfs) so callers can fall back to the buffered sink entirely.
pub(crate) struct UringDirectSink {
    inner: Mutex<Inner>,
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

        let depth = queue_depth();
        // The ring must hold every outstanding submission; io_uring rounds the
        // entry count up to a power of two. Sizing it at least `depth` makes a
        // submission-queue-full push impossible while at most `depth` writes are
        // in flight, which the write loop relies on.
        let ring = match IoUring::new(depth.next_power_of_two().max(2) as u32) {
            Ok(r) => r,
            Err(e) => return Err((e, fallback_file)),
        };
        let mut staging = Vec::with_capacity(depth);
        for _ in 0..depth {
            match AlignedBuf::new(STAGING_BUF_SIZE, DIRECT_IO_ALIGN as usize) {
                Ok(b) => staging.push(b),
                Err(e) => return Err((e, fallback_file)),
            }
        }

        Ok(Self {
            inner: Mutex::new(Inner {
                ring,
                staging,
                expected: vec![0; depth],
            }),
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
    /// O_DIRECT constraints. The staging buffer supplies address alignment,
    /// so only position and length matter here.
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

    /// Copy `buf` into staging buffers and write it out via io_uring, keeping up
    /// to `queue_depth()` writes in flight.
    ///
    /// A run is often many coalesced parts — a 100 MiB run becomes 13 slices of
    /// 8 MiB — so issuing slices one at a time and waiting for each would cap
    /// throughput at one write's latency regardless of how much data is ready.
    /// Slices are instead submitted from a pool of free buffers and completions
    /// reaped as they land, returning each buffer to the pool.
    ///
    /// Completions can arrive out of order, so a buffer is reusable only once its
    /// own completion has been seen. Each submission tags its slot in
    /// `user_data`; that slot returns to the free list when the matching CQE is
    /// reaped. The kernel owns a buffer for exactly the interval between its
    /// submission and its completion.
    fn write_direct(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        mut pos: u64,
    ) -> io::Result<()> {
        let inner = &mut *self.inner.lock().unwrap();
        // Split the borrow so the ring and the buffer pool can be used together.
        let Inner {
            ring,
            staging,
            expected,
        } = inner;
        let fd = types::Fd(self.direct_file.as_raw_fd());

        let depth = staging.len();
        let mut free: Vec<usize> = (0..depth).rev().collect();

        loop {
            // Fill and submit while data remains and a buffer is available.
            while buf.has_remaining() && !free.is_empty() {
                let slot = *free.last().expect("free is non-empty");

                let dst = staging[slot].as_mut_slice();
                let mut filled = 0usize;
                while buf.has_remaining() && filled < dst.len() {
                    let chunk = buf.chunk();
                    let n = chunk.len().min(dst.len() - filled);
                    dst[filled..filled + n].copy_from_slice(&chunk[..n]);
                    filled += n;
                    buf.advance(n);
                }
                debug_assert_eq!(
                    filled as u64 % DIRECT_IO_ALIGN,
                    0,
                    "callers only route fully-aligned writes here and the staging \
                     buffer size is a multiple of the alignment"
                );

                expected[slot] = filled;
                let ptr = staging[slot].ptr;
                let sqe = opcode::Write::new(fd, ptr, filled as u32)
                    .offset(pos)
                    .build()
                    .user_data(slot as u64);
                // SAFETY: `slot` was on the free list, so the kernel does not
                // already own this buffer; the buffer lives in `staging` for the
                // duration of this call and the slot is not reused until its
                // completion is reaped below.
                unsafe {
                    ring.submission().push(&sqe).expect(
                        "ring is sized at >= depth entries and at most depth \
                         writes are in flight, so the submission queue cannot be full",
                    );
                }
                free.pop();
                pos += filled as u64;
            }

            if free.len() == depth {
                break; // nothing in flight and nothing left to submit
            }

            ring.submit_and_wait(1)?;

            // Reap everything available, returning each slot to the pool.
            let completions: Vec<(u64, i32)> = ring
                .completion()
                .map(|cqe| (cqe.user_data(), cqe.result()))
                .collect();
            for (slot, res) in completions {
                let slot = slot as usize;
                if res < 0 {
                    return Err(io::Error::from_raw_os_error(-res));
                }
                if res as usize != expected[slot] {
                    // Short direct write: extremely rare (device error boundary).
                    // The bytes were already consumed from `buf` into staging, so
                    // surface an error rather than complicating the resubmit path
                    // for a case that indicates device trouble.
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "short O_DIRECT write: {} of {} bytes",
                            res, expected[slot]
                        ),
                    ));
                }
                free.push(slot);
            }
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
            tracing::trace!(pos, len, direct_writes = n + 1, direct_bytes = total, "O_DIRECT write");
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

    fn seg(chunks: &[&[u8]]) -> SegmentedBuf<Bytes> {
        let mut s = SegmentedBuf::new();
        for c in chunks {
            s.push(Bytes::copy_from_slice(c));
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
        sink.write_all_at(&mut seg(&[&block1]), ALIGN as u64).unwrap();
        sink.write_all_at(&mut seg(&[&block0]), 0).unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[..ALIGN], &block0[..]);
        assert_eq!(&contents[ALIGN..], &block1[..]);
    }

    #[test]
    fn many_small_segments_are_gathered() {
        let dir = test_dir();
        let path = dir.path().join("gather.bin");
        let Some(sink) = new_sink(&path) else { return };

        // 2 aligned blocks built from many small unaligned network-like segments.
        let mut expected = Vec::with_capacity(ALIGN * 2);
        let mut buf = SegmentedBuf::new();
        let mut i = 0u8;
        while expected.len() < ALIGN * 2 {
            let n = 1000.min(ALIGN * 2 - expected.len());
            let chunk = vec![i; n];
            expected.extend_from_slice(&chunk);
            buf.push(Bytes::from(chunk));
            i = i.wrapping_add(1);
        }

        sink.write_all_at(&mut buf, 0).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn write_larger_than_staging_buffer() {
        let dir = test_dir();
        let path = dir.path().join("large.bin");
        let Some(sink) = new_sink(&path) else { return };

        // 1.5x the staging buffer forces the slice loop to run twice.
        let total = STAGING_BUF_SIZE + STAGING_BUF_SIZE / 2;
        assert_eq!(total % ALIGN, 0);
        let mut expected = Vec::with_capacity(total);
        let mut buf = SegmentedBuf::new();
        let seg_size = 64 * 1024;
        for i in 0..(total / seg_size) {
            let chunk = vec![(i % 251) as u8; seg_size];
            expected.extend_from_slice(&chunk);
            buf.push(Bytes::from(chunk));
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
