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

/// Default size of each staging buffer, and therefore the size of each write the
/// device actually sees.
///
/// This is *not* the part size. A run larger than one buffer is issued as several
/// buffer-sized writes, so this constant — not the transfer's part size — bounds
/// the write size reaching the disk.
///
/// Write size dominates throughput on a striped array: on a nine-volume gp3 RAID0,
/// a single outstanding 8 MiB write sustains ~1.4 GiB/s while a single 128 MiB
/// write sustains ~15 GiB/s. Override with `S3_TM_DIRECT_IO_BLOCK_MIB`.
const DEFAULT_STAGING_BUF_MIB: usize = 8;

/// Resolve the staging buffer size in bytes, clamped to a sane range.
///
/// Configured in whole MiB rather than bytes so the result is always a multiple of
/// the direct-I/O alignment and cannot be set to a value `O_DIRECT` would reject.
fn staging_buf_size() -> usize {
    let mib = std::env::var("S3_TM_DIRECT_IO_BLOCK_MIB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .map(|m| m.clamp(1, 1024))
        .unwrap_or(DEFAULT_STAGING_BUF_MIB);
    mib * 1024 * 1024
}

/// Guards the one-time write-size printout. Process-wide rather than per-sink so
/// a multi-file transfer reports it once instead of once per output file.
static S_WRITE_SIZE_ONCE: std::sync::Once = std::sync::Once::new();

/// Number of O_DIRECT writes kept in flight per sink, and therefore the number
/// of staging buffers allocated.
///
/// Depth matters because a single write's completion latency bounds throughput
/// when writes are issued one at a time: an 8 MiB write completing in ~0.8 ms
/// caps a serial issuer near 10 GiB/s, and far lower when latency is higher.
/// Keeping several writes outstanding lets the device overlap them.
///
/// The cost is memory: `depth * staging_buf_size()` per output file, charged
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
/// The pool state persists across `write_all_at` calls so writes stay in flight
/// between runs: a run that only fills a few slices does not have to drain before
/// returning, and the next run reuses whatever slots have since completed. This is
/// what keeps the device busy when runs are smaller than the queue depth.
///
/// A slot's buffer is owned by the kernel from the moment its write is submitted
/// until its completion is reaped, so a slot must not be refilled while in flight.
struct Inner {
    ring: IoUring,
    staging: Vec<AlignedBuf>,
    /// Byte count submitted for each slot, to validate the completion result.
    expected: Vec<usize>,
    /// Slots not currently owned by the kernel.
    free: Vec<usize>,
    /// Submitted writes whose completions have not been reaped.
    in_flight: usize,
    /// First error seen while reaping. Surfaced from a later `write_all_at` or
    /// from `flush`, since the write that failed had already returned.
    deferred_error: Option<io::Error>,
    /// Whether any SQE has been pushed but not yet handed to the kernel.
    unsubmitted: bool,
}

impl Inner {
    /// Reap every completion currently available, returning slots to the pool.
    /// Errors are recorded rather than returned: the originating `write_all_at`
    /// has already returned to its caller.
    fn reap_available(&mut self) {
        let completions: Vec<(u64, i32)> = self
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for (slot, res) in completions {
            let slot = slot as usize;
            if res < 0 {
                self.record_error(io::Error::from_raw_os_error(-res));
            } else if res as usize != self.expected[slot] {
                self.record_error(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "short O_DIRECT write: {} of {} bytes",
                        res, self.expected[slot]
                    ),
                ));
            }
            self.free.push(slot);
            self.in_flight -= 1;
        }
    }

    fn record_error(&mut self, e: io::Error) {
        if self.deferred_error.is_none() {
            self.deferred_error = Some(e);
        }
    }

    /// Hand any queued submissions to the kernel without blocking.
    fn submit_pending(&mut self) -> io::Result<()> {
        if self.unsubmitted {
            self.ring.submit()?;
            self.unsubmitted = false;
        }
        Ok(())
    }

    /// Block until at least one completion is available, then reap.
    fn wait_and_reap(&mut self) -> io::Result<()> {
        self.ring.submit_and_wait(1)?;
        self.unsubmitted = false;
        self.reap_available();
        Ok(())
    }

    /// Wait for every outstanding write to complete.
    fn drain_in_flight(&mut self) -> io::Result<()> {
        self.submit_pending()?;
        while self.in_flight > 0 {
            self.wait_and_reap()?;
        }
        Ok(())
    }
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

        // Soundness: the kernel may still own staging buffers via submitted writes,
        // and those buffers are freed when `inner` drops immediately after this.
        // Draining here is the last line of defense if `flush` was never called
        // (an aborted or panicking transfer). Without it the kernel could write
        // into freed memory.
        if let Ok(mut inner) = self.inner.lock() {
            if let Err(e) = inner.drain_in_flight() {
                tracing::warn!(
                    error = %e,
                    "failed to drain in-flight O_DIRECT writes on sink drop"
                );
            }
            if let Some(e) = inner.deferred_error.take() {
                tracing::warn!(
                    error = %e,
                    "O_DIRECT write error discovered during drop; the transfer \
                     should already have failed via flush"
                );
            }
        }

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
        let buf_size = staging_buf_size();
        let mut staging = Vec::with_capacity(depth);
        for _ in 0..depth {
            match AlignedBuf::new(buf_size, DIRECT_IO_ALIGN as usize) {
                Ok(b) => staging.push(b),
                Err(e) => return Err((e, fallback_file)),
            }
        }

        Ok(Self {
            inner: Mutex::new(Inner {
                ring,
                staging,
                expected: vec![0; depth],
                free: (0..depth).rev().collect(),
                in_flight: 0,
                deferred_error: None,
                unsubmitted: false,
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

    /// Copy `buf` into staging buffers and submit it via io_uring, returning as soon
    /// as the last slice is queued — **without** waiting for completions.
    ///
    /// Writes stay in flight across calls. Only when every slot is busy does this
    /// block, and then just long enough for one to free. That keeps the device busy
    /// even when a single run is smaller than the queue depth, which per-call
    /// pipelining could not do: a one-part run has one slice and would otherwise
    /// issue one write and wait for it.
    ///
    /// Safe to return early because the payload is *copied* into the staging buffer,
    /// so the caller may free the source `Bytes` immediately. What must not happen is
    /// refilling a slot the kernel still owns, which the free list prevents.
    ///
    /// Completions are reaped opportunistically, so an error may be observed after
    /// the write that caused it has returned. Such errors are recorded and surfaced
    /// from a later call or from [`flush`](Self::flush).
    fn write_direct(
        &self,
        buf: &mut bytes_utils::SegmentedBuf<bytes::Bytes>,
        mut pos: u64,
    ) -> io::Result<()> {
        let inner = &mut *self.inner.lock().unwrap();

        // Surface a failure from an earlier, already-returned write before doing
        // more work: the transfer should fail rather than keep writing.
        if let Some(e) = inner.deferred_error.take() {
            return Err(e);
        }

        let fd = types::Fd(self.direct_file.as_raw_fd());

        while buf.has_remaining() {
            // Acquire a slot, waiting only if every buffer is with the kernel.
            while inner.free.is_empty() {
                inner.wait_and_reap()?;
            }
            let slot = *inner.free.last().expect("free is non-empty");

            // Split the borrow so the buffer pool and the ring can be used together.
            let Inner {
                ring,
                staging,
                expected,
                free,
                in_flight,
                unsubmitted,
                ..
            } = inner;

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

            // Print the real per-write size exactly once for the process. This is
            // not the part size: a run larger than one staging buffer is issued as
            // several buffer-sized writes, so the part size does not determine the
            // size the device actually sees.
            S_WRITE_SIZE_ONCE.call_once(|| {
                // Re-read config rather than plumb the values down: this runs once,
                // and it reads the same source the sink was constructed from.
                let cap = staging_buf_size();
                let d = queue_depth();
                println!(
                    "[TM] O_DIRECT write size: {} bytes ({:.2} MiB) per write \
                     -- staging buffer is {:.2} MiB (S3_TM_DIRECT_IO_BLOCK_MIB), \
                     depth {} (S3_TM_DIRECT_IO_QUEUE_DEPTH), \
                     {:.2} MiB of staging per file",
                    filled,
                    filled as f64 / (1024.0 * 1024.0),
                    cap as f64 / (1024.0 * 1024.0),
                    d,
                    (cap * d) as f64 / (1024.0 * 1024.0),
                );
            });

            let sqe = opcode::Write::new(fd, ptr, filled as u32)
                .offset(pos)
                .build()
                .user_data(slot as u64);
            // SAFETY: `slot` came off the free list, so the kernel does not already
            // own this buffer. The buffer lives in `staging` until the sink is
            // dropped, and `Drop` drains in-flight writes before that happens, so
            // the kernel never holds a dangling pointer. The slot is not refilled
            // until its completion is reaped.
            unsafe {
                ring.submission().push(&sqe).expect(
                    "ring is sized at >= depth entries and at most depth writes \
                     are in flight, so the submission queue cannot be full",
                );
            }
            free.pop();
            *in_flight += 1;
            *unsubmitted = true;
            pos += filled as u64;
        }

        // Hand the batch to the kernel so it starts now rather than at the next
        // blocking wait — without this the writes would not actually be in flight.
        inner.submit_pending()?;
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

    fn flush(&self) -> io::Result<()> {
        let inner = &mut *self.inner.lock().unwrap();
        inner.drain_in_flight()?;
        match inner.deferred_error.take() {
            Some(e) => Err(e),
            None => Ok(()),
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

        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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

        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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
        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }

    #[test]
    fn write_larger_than_staging_buffer() {
        let dir = test_dir();
        let path = dir.path().join("large.bin");
        let Some(sink) = new_sink(&path) else { return };

        // 1.5x the staging buffer forces the slice loop to run twice.
        let buf_size = super::staging_buf_size();
        let total = buf_size + buf_size / 2;
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
        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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

        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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

        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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
        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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

        // Writes stay in flight past `write_all_at`; flush before reading back.
        SinkWrite::flush(&sink).unwrap();
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
