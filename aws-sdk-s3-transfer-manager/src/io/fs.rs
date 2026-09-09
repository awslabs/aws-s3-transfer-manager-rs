/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Positioned filesystem operations used by transfer data paths.
//!
//! Reads and writes operate at explicit offsets without changing the file
//! cursor, allowing disjoint ranges of one open file to progress concurrently.
//! The read facade initializes pooled writable runs, while the write facade
//! preserves segmented buffers for vectored I/O where the platform supports
//! it. Preallocation prepares an output file for later writes but does not
//! flush data or provide a durability boundary.

use bytes::{Buf, BufMut};
use std::fs::File;
use std::io;

use crate::io::PartBuffer;

/// Reads exactly enough bytes to fill every writable run in `dst`.
///
/// The first byte is read from `offset`; subsequent runs continue from the end
/// of the preceding run. The file cursor is unchanged.
///
/// A run is published to [`PartBuffer`] only after the positioned read has
/// initialized that complete run. If a later read fails, earlier completed
/// runs remain initialized, but the failing run remains unpublished. Callers
/// must discard the complete buffer on error rather than freezing a partial
/// upload payload.
///
/// Returns [`io::ErrorKind::UnexpectedEof`] when the requested range extends
/// beyond the file and an error when the range offset cannot be represented.
pub(crate) fn read_exact_at(file: &File, dst: &mut PartBuffer, mut offset: u64) -> io::Result<()> {
    while dst.remaining_mut() != 0 {
        // SAFETY: the returned slice remains `MaybeUninit<u8>` until the
        // positioned read initializes the complete range.
        let uninitialized = unsafe { dst.chunk_mut().as_uninit_slice_mut() };
        let count = uninitialized.len();
        if count == 0 {
            return Err(io::Error::other(
                "pooled file buffer exposed no writable range",
            ));
        }

        // SAFETY: `uninitialized` is the exclusive writable range returned by
        // `BufMut`. `sys::read_exact_at` initializes the complete slice on
        // success, after which `advance_mut` publishes exactly that range.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                uninitialized.as_mut_ptr().cast::<u8>(),
                uninitialized.len(),
            )
        };
        sys::read_exact_at(file, bytes, offset)?;
        // SAFETY: the positioned read initialized every byte in `bytes`.
        unsafe {
            dst.advance_mut(count);
        }
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::other("file read offset overflowed"))?;
    }
    Ok(())
}

/// Writes all remaining bytes from `buf` beginning at `offset`.
///
/// The file cursor is unchanged. Disjoint calls may therefore write the same
/// open file concurrently without serializing on cursor position. The buffer
/// is advanced after each successful write; an error leaves it positioned
/// after the bytes already written so the caller can observe partial progress.
///
/// Unix uses vectored positioned writes to preserve segment boundaries.
/// Platforms without positioned-write support return
/// [`io::ErrorKind::Unsupported`].
pub(crate) fn write_all_at(file: &File, buf: &mut impl Buf, offset: u64) -> io::Result<()> {
    sys::write_all_at(file, buf, offset)
}

/// Prepares `file` for an expected final length of `len` bytes.
///
/// Linux reserves the range with `posix_fallocate`, allowing allocation
/// failures to surface before transfer writes begin. Other Unix systems and
/// Windows set the logical file length without guaranteeing physical space;
/// unsupported platforms treat the request as a no-op.
///
/// This operation does not flush file data or metadata. Successful return is
/// not a durability guarantee.
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    sys::preallocate(file, len)
}

#[cfg(all(unix, not(miri)))]
mod sys {
    //! Native Unix positioned I/O.

    use bytes::Buf;
    use std::fs::File;
    use std::io::{self, IoSlice};
    use std::os::unix::fs::FileExt;
    use std::os::unix::io::AsFd;

    /// Maximum number of I/O vector entries per system call.
    const MAX_IO_SLICES: usize = 128;

    /// Fills one contiguous destination range without changing the file cursor.
    pub(super) fn read_exact_at(file: &File, dst: &mut [u8], offset: u64) -> io::Result<()> {
        file.read_exact_at(dst, offset)
    }

    /// Drains segmented input with as few positioned writes as the iovec limit permits.
    pub(super) fn write_all_at(file: &File, buf: &mut impl Buf, offset: u64) -> io::Result<()> {
        let fd = file.as_fd();
        let mut pos = offset as i64;

        while buf.has_remaining() {
            let mut slices = [IoSlice::new(&[]); MAX_IO_SLICES];
            let n = buf.chunks_vectored(&mut slices);
            let written = nix::sys::uio::pwritev(fd, &slices[..n], pos).map_err(io::Error::from)?;
            pos += written as i64;
            buf.advance(written);
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        nix::fcntl::posix_fallocate(file.as_fd(), 0, len as i64).map_err(io::Error::from)
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        file.set_len(len)
    }
}

#[cfg(all(unix, miri))]
mod sys {
    //! Miri-compatible Unix operations.
    //!
    //! Scalar positioned writes replace unsupported `pwritev` FFI while
    //! retaining the same buffer advancement and offset behavior.

    use bytes::Buf;
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

    pub(super) fn read_exact_at(file: &File, dst: &mut [u8], offset: u64) -> io::Result<()> {
        file.read_exact_at(dst, offset)
    }

    pub(super) fn write_all_at(file: &File, buf: &mut impl Buf, offset: u64) -> io::Result<()> {
        let mut pos = offset;
        while buf.has_remaining() {
            let chunk = buf.chunk();
            let written = file.write_at(chunk, pos)?;
            pos += written as u64;
            buf.advance(written);
        }
        Ok(())
    }

    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        file.set_len(len)
    }
}

#[cfg(windows)]
mod sys {
    //! Windows positioned I/O.
    //!
    //! `seek_read` and `seek_write` do not mutate the shared file cursor.

    use bytes::Buf;
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::FileExt;

    pub(super) fn read_exact_at(file: &File, dst: &mut [u8], mut offset: u64) -> io::Result<()> {
        while !dst.is_empty() {
            let count = file.seek_read(dst, offset)?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected end of file",
                ));
            }
            dst = &mut dst[count..];
            offset = offset
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::other("file read offset overflowed"))?;
        }
        Ok(())
    }

    pub(super) fn write_all_at(file: &File, buf: &mut impl Buf, offset: u64) -> io::Result<()> {
        let mut pos = offset;

        while buf.has_remaining() {
            let chunk = buf.chunk();
            let written = file.seek_write(chunk, pos)?;
            pos += written as u64;
            buf.advance(written);
        }
        Ok(())
    }

    pub(super) fn preallocate(file: &File, len: u64) -> io::Result<()> {
        // Windows has no native fallocate equivalent. set_len calls
        // SetEndOfFile which extends the file logical size; matches the
        // macOS path. On both platforms preallocation is best-effort
        // length-setting without disk-space reservation; only Linux's
        // posix_fallocate guarantees ENOSPC at preallocate time.
        file.set_len(len)
    }
}

#[cfg(not(any(unix, windows)))]
mod sys {
    //! Fallbacks for platforms without positioned filesystem operations.

    use bytes::Buf;
    use std::fs::File;
    use std::io;

    pub(super) fn read_exact_at(_file: &File, _dst: &mut [u8], _offset: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "positioned reads not supported on this platform",
        ))
    }

    pub(super) fn write_all_at(_file: &File, _buf: &mut impl Buf, _offset: u64) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "positioned writes not supported on this platform",
        ))
    }

    pub(super) fn preallocate(_file: &File, _len: u64) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Waker};

    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;

    use crate::io::PartBuffer;
    use crate::memory::BufferPool;
    use crate::types::MemoryBudgetConfig;

    fn test_pool() -> BufferPool {
        BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(1024 * 1024))
            .build()
            .unwrap()
    }

    #[test]
    fn read_exact_at_initializes_pooled_runs() {
        let pool = test_pool();
        let part_size = pool.carrier_size() * 2 + 37;
        let data = (0..part_size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp, &data).unwrap();
        let mut buffer = PartBuffer::new(pool.clone(), part_size);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(buffer.poll_acquire(&mut cx, part_size).is_ready());

        read_exact_at(tmp.as_file(), &mut buffer, 0).unwrap();
        let mut frozen = buffer.freeze();
        assert_eq!(frozen.copy_to_bytes(frozen.remaining()).as_ref(), data);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn write_all_at_single_segment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        let data = Bytes::from_static(b"hello world");
        let mut seg = SegmentedBuf::new();
        seg.push(data);

        write_all_at(&file, &mut seg, 0).unwrap();
        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"hello world");
    }

    #[test]
    fn write_all_at_multiple_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        let mut seg = SegmentedBuf::new();
        seg.push(Bytes::from_static(b"aaa"));
        seg.push(Bytes::from_static(b"bbb"));
        seg.push(Bytes::from_static(b"ccc"));

        write_all_at(&file, &mut seg, 0).unwrap();
        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"aaabbbccc");
    }

    #[test]
    fn write_all_at_nonzero_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        // Write at offset 0
        let mut seg0 = SegmentedBuf::new();
        seg0.push(Bytes::from_static(b"AAAA"));
        write_all_at(&file, &mut seg0, 0).unwrap();

        // Write at offset 4
        let mut seg1 = SegmentedBuf::new();
        seg1.push(Bytes::from_static(b"BBBB"));
        write_all_at(&file, &mut seg1, 4).unwrap();

        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"AAAABBBB");
    }

    #[test]
    fn write_all_at_out_of_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        // Write second chunk first
        let mut seg1 = SegmentedBuf::new();
        seg1.push(Bytes::from_static(b"BBBB"));
        write_all_at(&file, &mut seg1, 4).unwrap();

        // Write first chunk second
        let mut seg0 = SegmentedBuf::new();
        seg0.push(Bytes::from_static(b"AAAA"));
        write_all_at(&file, &mut seg0, 0).unwrap();

        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"AAAABBBB");
    }

    #[test]
    fn write_all_at_empty_buf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        let mut seg = SegmentedBuf::<Bytes>::new();
        write_all_at(&file, &mut seg, 0).unwrap();
        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty());
    }

    /// More segments than a single pwritev call can handle (128 IoSlice limit).
    /// Exercises the loop that issues multiple system calls.
    #[test]
    fn write_all_at_exceeds_io_slice_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        let num_segments = 192;
        let segment_size = 4096;
        let mut seg = SegmentedBuf::new();
        let mut expected = Vec::with_capacity(num_segments * segment_size);
        for i in 0..num_segments {
            let byte = (i % 256) as u8;
            let data = vec![byte; segment_size];
            expected.extend_from_slice(&data);
            seg.push(Bytes::from(data));
        }

        write_all_at(&file, &mut seg, 0).unwrap();
        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), expected.len());
        assert_eq!(contents, expected);
    }

    /// Two 8MB writes at different offsets, each composed of many small segments.
    #[cfg_attr(miri, ignore)] // 16MB through miri's interpreter is too slow
    #[test]
    fn write_all_at_large_parts_at_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        let file = File::create(&path).unwrap();

        let part_size: usize = 8 * 1024 * 1024;
        let segment_size: usize = 16 * 1024;
        let num_segments = part_size / segment_size;

        let mut seg0 = SegmentedBuf::new();
        for _ in 0..num_segments {
            seg0.push(Bytes::from(vec![0xAA; segment_size]));
        }
        write_all_at(&file, &mut seg0, 0).unwrap();

        let mut seg1 = SegmentedBuf::new();
        for _ in 0..num_segments {
            seg1.push(Bytes::from(vec![0xBB; segment_size]));
        }
        write_all_at(&file, &mut seg1, part_size as u64).unwrap();

        drop(file);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), part_size * 2);
        assert!(contents[..part_size].iter().all(|&b| b == 0xAA));
        assert!(contents[part_size..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn preallocate_sets_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prealloc.bin");
        let file = File::create(&path).unwrap();

        let len = 1024 * 1024; // 1 MB
        preallocate(&file, len).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), len);
    }
}
