/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Platform-specific filesystem operations.

use bytes::Buf;
use std::fs::File;
use std::io;

/// Write all data from `buf` to `file` at the given byte `offset`.
///
/// Uses positioned writes — the file's cursor position is not affected.
/// Multiple concurrent calls with different offsets are safe.
pub(crate) fn write_all_at(file: &File, buf: &mut impl Buf, offset: u64) -> io::Result<()> {
    sys::write_all_at(file, buf, offset)
}

/// Pre-allocate disk space for `file` so that at least `len` bytes can be
/// written without triggering per-write metadata updates or late ENOSPC.
///
/// This is best-effort: on platforms without fallocate support the call is a
/// no-op.
pub(crate) fn preallocate(file: &File, len: u64) -> io::Result<()> {
    sys::preallocate(file, len)
}

#[cfg(all(unix, not(miri)))]
mod sys {
    use bytes::Buf;
    use std::fs::File;
    use std::io::{self, IoSlice};
    use std::os::unix::io::AsFd;

    /// Maximum number of I/O vector entries per system call.
    const MAX_IO_SLICES: usize = 128;

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

// Under miri, use simple seek+pwrite to avoid unsupported pwritev FFI.
// The buffer chunking logic is still exercised by the same tests.
#[cfg(all(unix, miri))]
mod sys {
    use bytes::Buf;
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

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
    use bytes::Buf;
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::FileExt;

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

    pub(super) fn preallocate(_file: &File, _len: u64) -> io::Result<()> {
        // No-op on Windows. set_len here calls SetEndOfFile, which moves the
        // file's logical end but not its valid-data-length (VDL). A positioned
        // write past the VDL then forces NTFS to synchronously zero-fill the gap
        // from the VDL to the write offset. Download chunks arrive out of order,
        // so writes land far ahead of the VDL routinely, and each one blocks on a
        // multi-hundred-MiB (multi-second) zero-fill on the writing thread —
        // which on the managed runtime is the same reactor driving that thread's
        // S3 connections, stalling them until the service resets the idle
        // sockets. set_len also reserves no disk space and gives no ENOSPC
        // guarantee, so skipping it loses nothing. (SetFileValidData would avoid
        // the zero-fill but requires the SE_MANAGE_VOLUME privilege and exposes
        // stale on-disk data; not worth it.)
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
mod sys {
    use bytes::Buf;
    use std::fs::File;
    use std::io;

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
    use bytes::Bytes;
    use bytes_utils::SegmentedBuf;

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
