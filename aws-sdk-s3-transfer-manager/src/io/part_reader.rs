/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::cmp;
use std::fs::File;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes};

use crate::io::error::Error;
use crate::io::path_body::PathBody;
use crate::io::stream::RawInputStream;
use crate::io::InputStream;
use crate::io::PartData;
use crate::metrics::unit::ByteUnit;

use super::stream::{BoxStream, StreamContext};

/// Builder for creating a `PartReader`
#[derive(Debug)]
pub(crate) struct Builder {
    stream: Option<RawInputStream>,
    part_size: usize,
    direct_io: bool,
    io_counters: Option<std::sync::Arc<crate::metrics::IOCounters>>,
}

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            stream: None,
            part_size: 5 * ByteUnit::Mebibyte.as_bytes_u64() as usize,
            direct_io: false,
            io_counters: None,
        }
    }

    /// Set the input stream to read from.
    pub(crate) fn stream(mut self, stream: InputStream) -> Self {
        self.stream = Some(stream.inner);
        self
    }

    /// Set the target part size that should be used when reading data.
    ///
    /// All parts except for possibly the last one should be of this size.
    pub(crate) fn part_size(mut self, part_size: usize) -> Self {
        self.part_size = part_size;
        self
    }

    /// Set direct I/O mode (read on calling thread vs spawn_blocking).
    pub(crate) fn direct_io(mut self, direct: bool) -> Self {
        self.direct_io = direct;
        self
    }

    /// Set the I/O counters for recording throughput metrics.
    pub(crate) fn io_counters(
        mut self,
        io_counters: std::sync::Arc<crate::metrics::IOCounters>,
    ) -> Self {
        self.io_counters = Some(io_counters);
        self
    }

    pub(crate) fn build(self) -> Result<PartReader, Error> {
        let stream = self.stream.expect("input stream set");
        let io_counters = self.io_counters.expect("io_counters set");
        PartReader::new(stream, self.part_size, self.direct_io, io_counters)
    }
}

#[derive(Debug)]
pub(crate) struct PartReader {
    inner: Inner,
    stream_cx: StreamContext,
}

impl PartReader {
    fn new(
        raw: RawInputStream,
        part_size: usize,
        direct_io: bool,
        io_counters: std::sync::Arc<crate::metrics::IOCounters>,
    ) -> Result<Self, Error> {
        let inner = match raw {
            RawInputStream::Buf(buf) => Inner::Bytes(BytesPartReader::new(buf)),
            RawInputStream::Fs(path_body) => Inner::Fs(PathBodyPartReader::new(path_body)?),
            RawInputStream::Dyn(box_body) => Inner::Dyn(DynPartReader::new(box_body)),
        };

        let stream_cx = StreamContext::new(part_size, direct_io, io_counters);
        Ok(Self { inner, stream_cx })
    }

    #[allow(dead_code)] // TODO(phase3): re-wire upload part validation
    pub(crate) fn part_size(&self) -> usize {
        self.stream_cx.part_size()
    }
}

#[derive(Debug)]
enum Inner {
    Bytes(BytesPartReader),
    Fs(PathBodyPartReader),
    Dyn(DynPartReader),
}

impl PartReader {
    pub(crate) async fn next_part(&self) -> Result<Option<PartData>, Error> {
        match &self.inner {
            Inner::Bytes(bytes) => bytes.next_part(&self.stream_cx).await,
            Inner::Fs(path_body) => path_body.next_part(&self.stream_cx).await,
            Inner::Dyn(part_stream) => part_stream.next_part(&self.stream_cx).await,
        }
    }

    pub(crate) async fn full_object_checksum(&self) -> Option<String> {
        match &self.inner {
            Inner::Dyn(part_stream) => part_stream.full_object_checksum().await,
            _ => None,
        }
    }
}

#[derive(Debug)]
struct PartReaderState {
    // current start offset
    offset: u64,
    // current part number
    part_number: u64,
    // total number of bytes remaining to be read
    remaining: u64,
}

impl PartReaderState {
    /// Create a new `PartReaderState`
    fn new(content_length: u64) -> Self {
        Self {
            offset: 0,
            part_number: 1,
            remaining: content_length,
        }
    }

    /// Set the initial offset to start reading from
    fn with_offset(self, offset: u64) -> Self {
        Self { offset, ..self }
    }

    pub(crate) fn is_end(&self) -> bool {
        self.remaining == 0
    }
}

/// Implementation for in-memory input streams.
#[derive(Debug)]
struct BytesPartReader {
    buf: Bytes,
    state: Mutex<PartReaderState>,
}

impl BytesPartReader {
    fn new(buf: Bytes) -> Self {
        let content_length = buf.remaining() as u64;
        Self {
            buf,
            state: Mutex::new(PartReaderState::new(content_length)), // std Mutex
        }
    }
}

impl BytesPartReader {
    async fn next_part(&self, stream_cx: &StreamContext) -> Result<Option<PartData>, Error> {
        let mut state = self.state.lock().expect("lock valid");
        if state.is_end() {
            return Ok(None);
        }

        let expected_offset = (state.part_number - 1) * stream_cx.part_size() as u64;
        if state.offset != expected_offset {
            return Err(Error::offset_not_aligned_with_part_number(
                state.offset,
                state.part_number,
            ));
        }

        let start = state.offset as usize;
        let end = cmp::min(start + stream_cx.part_size(), self.buf.len());
        let data = self.buf.slice(start..end);
        let part_number = state.part_number;
        state.part_number += 1;
        state.offset += data.len() as u64;
        state.remaining -= data.len() as u64;
        let part = PartData::new(part_number, data).mark_last(state.is_end());
        Ok(Some(part))
    }
}

/// Implementation for path based input streams
#[derive(Debug)]
struct PathBodyPartReader {
    body: PathBody,
    state: Mutex<PartReaderState>, // std Mutex
    file: Arc<File>,
}

impl PathBodyPartReader {
    fn new(body: PathBody) -> Result<Self, Error> {
        // TODO(vnext): Consider O_DIRECT for large sequential uploads (requires aligned
        // buffers from buffer pool). Also consider fadvise(POSIX_FADV_SEQUENTIAL) — generation
        // order is sequential so kernel readahead would help, but concurrent execution creates
        // some scatter. Benchmark before adding.
        // TODO(vnext): does this need to be async now?
        let file = Arc::new(File::open(&body.path)?);
        let offset = body.offset;
        let content_length = body.length;
        Ok(Self {
            body,
            state: Mutex::new(PartReaderState::new(content_length).with_offset(offset)),
            file,
        })
    }
}

impl PathBodyPartReader {
    async fn next_part(&self, stream_cx: &StreamContext) -> Result<Option<PartData>, Error> {
        let (offset, part_number, part_size, is_last) = match self.advance(stream_cx)? {
            Some(PathBodyReadCursor {
                offset,
                part_number,
                part_size,
                is_last,
            }) => (offset, part_number, part_size, is_last),
            None => return Ok(None),
        };
        // grab a buffer to fill from the context
        let mut dst = stream_cx.new_buffer(part_size as usize);
        // SAFETY: We set the length to capacity so the read has a full slice to fill.
        // read_file_chunk reads exactly part_size bytes on success, so the buffer
        // will be fully initialized after a successful read.
        unsafe { dst.set_len(dst.capacity()) }

        if stream_cx.direct_io() {
            // Managed threads: read directly, no thread pool hop.
            file_util::read_file_chunk(&self.file, dst.deref_mut(), offset)?;
        } else {
            // Shared runtime: offload to blocking thread pool.
            let fd = Arc::clone(&self.file);
            dst = tokio::task::spawn_blocking(move || {
                file_util::read_file_chunk(&fd, dst.deref_mut(), offset)?;
                Ok::<_, std::io::Error>(dst)
            })
            .await??;
        }

        stream_cx.io_counters().record(&crate::metrics::IoSample {
            disk_read: part_size,
            ..Default::default()
        });

        Ok(Some(PartData::new(part_number, dst).mark_last(is_last)))
    }

    // Advances the `PartReaderState` to the next state and returns `PathBodyReadCursor`
    // (offset, part_number, part_size, is_last), which will be used in the upcoming
    // `read_file_chunk` execution.
    fn advance(&self, stream_cx: &StreamContext) -> Result<Option<PathBodyReadCursor>, Error> {
        let mut state = self.state.lock().expect("lock valid");
        if state.is_end() {
            return Ok(None);
        }
        let offset = state.offset;
        let part_number = state.part_number;

        let expected_offset = self.body.offset + (part_number - 1) * stream_cx.part_size() as u64;
        if offset != expected_offset {
            return Err(Error::offset_not_aligned_with_part_number(
                offset,
                part_number,
            ));
        }
        let part_size = cmp::min(stream_cx.part_size() as u64, state.remaining);
        state.offset += part_size;
        state.part_number += 1;
        state.remaining -= part_size;

        Ok(Some(PathBodyReadCursor {
            offset,
            part_number,
            part_size,
            is_last: state.is_end(),
        }))
    }
}

#[derive(Debug, Clone, Copy)]
struct PathBodyReadCursor {
    offset: u64,
    part_number: u64,
    part_size: u64,
    is_last: bool,
}

pub(crate) mod file_util {
    #[cfg(unix)]
    pub(crate) use unix::read_file_chunk;
    #[cfg(windows)]
    pub(crate) use windows::read_file_chunk;

    #[cfg(unix)]
    mod unix {
        use std::fs::File;
        use std::io;
        use std::os::unix::fs::FileExt;

        pub(crate) fn read_file_chunk(
            file: &File,
            dst: &mut [u8],
            offset: u64,
        ) -> Result<(), io::Error> {
            file.read_exact_at(dst, offset)
        }
    }

    #[cfg(windows)]
    mod windows {
        use std::fs::File;
        use std::io;
        use std::os::windows::fs::FileExt;

        pub(crate) fn read_file_chunk(
            file: &File,
            dst: &mut [u8],
            offset: u64,
        ) -> Result<(), io::Error> {
            let mut pos = 0;
            while pos < dst.len() {
                let n = file.seek_read(&mut dst[pos..], offset + pos as u64)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "unexpected end of file",
                    ));
                }
                pos += n;
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
struct DynPartReader {
    inner: tokio::sync::Mutex<BoxStream>,
}

impl DynPartReader {
    fn new(inner: BoxStream) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(inner),
        }
    }
    async fn next_part(&self, stream_cx: &StreamContext) -> Result<Option<PartData>, Error> {
        // TODO - can we do better than a mutex here? should we spawn a dedicated task and use channels instead
        let mut stream = self.inner.lock().await;
        match stream.next(stream_cx).await {
            Some(result) => result.map(Some).map_err(|err| err.into()),
            None => Ok(None),
        }
    }

    // this is only async because it locks a Tokio Mutex
    async fn full_object_checksum(&self) -> Option<String> {
        let stream = self.inner.lock().await;
        stream.full_object_checksum()
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::task::Poll;

    use bytes::{Buf, Bytes};
    use tempfile::NamedTempFile;

    use crate::io::part_reader::{
        Builder, BytesPartReader, PartData, PartReader, PathBodyPartReader,
    };
    use crate::io::path_body::PathBody;
    use crate::io::stream::{PartStream, StreamContext};
    use crate::io::InputStream;

    fn test_stream_cx(part_size: usize) -> StreamContext {
        StreamContext::new(part_size, false, test_io_counters())
    }

    fn test_io_counters() -> std::sync::Arc<crate::metrics::IOCounters> {
        std::sync::Arc::new(crate::metrics::IOCounters::new(
            std::time::Duration::from_secs(1),
        ))
    }

    async fn collect_parts(reader: PartReader) -> Vec<PartData> {
        let mut parts = Vec::new();
        let mut expected_part_number = 1;
        while let Some(part) = reader.next_part().await.unwrap() {
            assert_eq!(expected_part_number, part.part_number);
            expected_part_number += 1;
            parts.push(part);
        }
        parts
    }

    #[tokio::test]
    async fn test_bytes_part_reader() {
        let data = Bytes::from("a lep is a ball, a tay is a hammer, a flix is a comb");
        let stream = InputStream::from(data.clone());
        let expected = data.chunks(5).collect::<Vec<_>>();
        let reader = Builder::new()
            .part_size(5)
            .stream(stream)
            .io_counters(test_io_counters())
            .build()
            .unwrap();
        let parts = collect_parts(reader).await;
        let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();

        assert_eq!(expected, actual);
    }

    async fn path_reader_test(limit: Option<usize>, offset: Option<usize>) {
        let part_size = 5;
        let mut tmp = NamedTempFile::new().unwrap();
        let mut data = Bytes::from("a lep is a ball, a tay is a hammer, a flix is a comb");
        tmp.write_all(data.chunk()).unwrap();

        let mut builder = InputStream::read_from().path(tmp.path());
        if let Some(limit) = limit {
            data.truncate(limit);
            builder = builder.length((limit - offset.unwrap_or_default()) as u64);
        }

        if let Some(offset) = offset {
            data.advance(offset);
            builder = builder.offset(offset as u64);
        }

        let expected = data.chunks(part_size).collect::<Vec<_>>();

        let stream = builder.build().unwrap();
        let reader = Builder::new()
            .part_size(part_size)
            .stream(stream)
            .io_counters(test_io_counters())
            .build()
            .unwrap();

        let parts = collect_parts(reader).await;
        let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_path_part_reader() {
        path_reader_test(None, None).await;
    }

    #[tokio::test]
    async fn test_path_part_reader_with_offset() {
        path_reader_test(None, Some(8)).await;
    }

    #[tokio::test]
    async fn test_path_part_reader_with_explicit_length() {
        path_reader_test(Some(12), None).await;
    }

    #[tokio::test]
    async fn test_path_part_reader_with_length_and_offset() {
        path_reader_test(Some(23), Some(4)).await;
    }

    #[derive(Debug)]
    struct TestStream {
        data: Vec<Bytes>,
        idx: usize,
    }

    impl TestStream {
        fn new(data: Vec<Bytes>) -> Self {
            Self { data, idx: 0 }
        }
    }

    impl PartStream for TestStream {
        fn poll_part(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _stream_cx: &StreamContext,
        ) -> Poll<Option<std::io::Result<PartData>>> {
            if self.idx < self.data.len() {
                let part = PartData::new(self.idx as u64 + 1, self.data[self.idx].clone());
                self.as_mut().idx += 1;
                Poll::Ready(Some(Ok(part)))
            } else {
                Poll::Ready(None)
            }
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            unimplemented!()
        }
    }

    // sanity test custom PollPart is wired up and can be supplied to input stream
    #[tokio::test]
    async fn test_dyn_reader() {
        let data = Bytes::from("a lep is a ball, a tay is a hammer, a flix is a comb");
        let expected = data.chunks(5).collect::<Vec<_>>();
        let stream = TestStream::new(
            data.chunks(5)
                .map(|x| Bytes::from(x.to_owned()))
                .collect::<Vec<_>>(),
        );
        let stream = InputStream::from_part_stream(stream);
        let reader = Builder::new()
            .part_size(5)
            .stream(stream)
            .io_counters(test_io_counters())
            .build()
            .unwrap();
        let parts = collect_parts(reader).await;
        let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_bytes_part_reader_offset_not_aligned_error() {
        let data = Bytes::from("test data for alignment error");
        let reader = BytesPartReader::new(data);
        let stream_cx = test_stream_cx(5);

        // First call should succeed
        let result = reader.next_part(&stream_cx).await;
        assert!(result.is_ok());

        // Manually corrupt the offset to create misalignment
        {
            let mut state = reader.state.lock().unwrap();
            state.offset = 99; // Invalid offset that doesn't align with part_number
        }

        // Second call should fail with offset_not_aligned_with_part_number error
        let result = reader.next_part(&stream_cx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bytes_part_reader_detects_last_true() {
        let data = Bytes::from("test");
        let reader = BytesPartReader::new(data);
        let stream_cx = test_stream_cx(10);

        let result = reader.next_part(&stream_cx).await.unwrap().unwrap();
        assert!(result.is_last.unwrap());
    }

    #[test]
    fn test_path_body_part_reader_advance() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 30]).unwrap();

        let path_body = PathBody {
            path: tmp.path().to_path_buf(),
            offset: 10,
            length: 20,
        };
        let reader = PathBodyPartReader::new(path_body).unwrap();
        let stream_cx = test_stream_cx(5);

        // First advance should succeed
        let result = reader.advance(&stream_cx).unwrap().unwrap();
        assert_eq!(result.offset, 10);
        assert_eq!(result.part_number, 1);
        assert_eq!(result.part_size, 5);
        assert!(!result.is_last);

        // Second advance should succeed
        let result = reader.advance(&stream_cx).unwrap().unwrap();
        assert_eq!(result.offset, 15);
        assert_eq!(result.part_number, 2);
        assert_eq!(result.part_size, 5);
        assert!(!result.is_last);

        // Manually corrupt the offset to test validation
        {
            let mut state = reader.state.lock().unwrap();
            state.offset = 99; // Invalid offset
        }

        // Third advance should fail due to misaligned offset
        let result = reader.advance(&stream_cx);
        assert!(result.is_err());
    }

    #[test]
    fn test_path_body_part_reader_advance_detects_last_part() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 3]).unwrap();

        let path_body = PathBody {
            path: tmp.path().to_path_buf(),
            offset: 0,
            length: 3,
        };
        let reader = PathBodyPartReader::new(path_body).unwrap();
        let stream_cx = test_stream_cx(10);

        let result = reader.advance(&stream_cx).unwrap().unwrap();
        assert!(result.is_last);
    }
}
