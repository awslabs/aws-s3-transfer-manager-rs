/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::cmp;
use std::ops::DerefMut;
use std::sync::Mutex;

use bytes::{Buf, Bytes, BytesMut};

use crate::io::error::Error;
use crate::io::path_body::PathBody;
use crate::io::stream::RawInputStream;
use crate::io::InputStream;
use crate::metrics::unit::ByteUnit;

use super::stream::{BoxStream, StreamContext};

/// Builder for creating a `PartReader`
#[derive(Debug)]
pub(crate) struct Builder {
    stream: Option<RawInputStream>,
    part_size: usize,
}

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            stream: None,
            part_size: 5 * ByteUnit::Mebibyte.as_bytes_u64() as usize,
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

    pub(crate) fn build(self) -> PartReader {
        let stream = self.stream.expect("input stream set");
        PartReader::new(stream, self.part_size)
    }
}

#[derive(Debug)]
pub(crate) struct PartReader {
    inner: Inner,
    stream_cx: StreamContext,
}

impl PartReader {
    fn new(raw: RawInputStream, part_size: usize) -> Self {
        let inner = match raw {
            RawInputStream::Buf(buf) => Inner::Bytes(BytesPartReader::new(buf)),
            RawInputStream::Fs(path_body) => Inner::Fs(PathBodyPartReader::new(path_body)),
            RawInputStream::Dyn(box_body) => Inner::Dyn(DynPartReader::new(box_body)),
        };

        let stream_cx = StreamContext { part_size };
        Self { inner, stream_cx }
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
}

/// Contents and (optional) metadata for a single part of a [multipart upload].
///
/// [multipart upload]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
#[derive(Debug, Clone)]
pub struct PartData {
    // 1-indexed
    pub(crate) part_number: u64,
    pub(crate) data: Bytes,
}

impl PartData {
    /// Create a new part
    fn new(part_number: u64, data: impl Into<Bytes>) -> Self {
        Self {
            part_number,
            data: data.into(),
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
        if state.remaining == 0 {
            return Ok(None);
        }

        let start = state.offset as usize;
        let end = cmp::min(start + stream_cx.part_size(), self.buf.len());
        let data = self.buf.slice(start..end);
        let part_number = state.part_number;
        state.part_number += 1;
        state.offset += data.len() as u64;
        state.remaining -= data.len() as u64;
        let part = PartData::new(part_number, data);
        Ok(Some(part))
    }
}

/// Implementation for path based input streams
#[derive(Debug)]
struct PathBodyPartReader {
    body: PathBody,
    state: Mutex<PartReaderState>, // std Mutex
}

impl PathBodyPartReader {
    fn new(body: PathBody) -> Self {
        let offset = body.offset;
        let content_length = body.length;
        Self {
            body,
            state: Mutex::new(PartReaderState::new(content_length).with_offset(offset)), // std Mutex
        }
    }
}

impl PathBodyPartReader {
    async fn next_part(&self, stream_cx: &StreamContext) -> Result<Option<PartData>, Error> {
        let (offset, part_number, part_size) = {
            let mut state = self.state.lock().expect("lock valid");
            if state.remaining == 0 {
                return Ok(None);
            }
            let offset = state.offset;
            let part_number = state.part_number;

            let part_size = cmp::min(stream_cx.part_size() as u64, state.remaining);
            state.offset += part_size;
            state.part_number += 1;
            state.remaining -= part_size;

            (offset, part_number, part_size)
        };
        let path = self.body.path.clone();
        let handle = tokio::task::spawn_blocking(move || {
            // TODO(aws-sdk-rust#1159) - replace allocation with memory pool
            let mut dst = BytesMut::with_capacity(part_size as usize);
            // we need to set the length so that the raw &[u8] slice has the correct
            // size, we are guaranteed to read exactly part_size data from file on success
            unsafe { dst.set_len(dst.capacity()) }
            file_util::read_file_chunk_sync(dst.deref_mut(), path, offset)?;
            let data = dst.freeze();
            Ok::<PartData, Error>(PartData::new(part_number, data))
        });

        handle.await?.map(Some)
    }
}

mod file_util {
    #[cfg(unix)]
    pub(super) use unix::read_file_chunk_sync;
    #[cfg(windows)]
    pub(super) use windows::read_file_chunk_sync;

    #[cfg(unix)]
    mod unix {
        use std::fs::File;
        use std::io;
        use std::os::unix::fs::FileExt;
        use std::path::Path;

        pub(crate) fn read_file_chunk_sync(
            dst: &mut [u8],
            path: impl AsRef<Path>,
            offset: u64,
        ) -> Result<(), io::Error> {
            let file = File::open(path)?;
            file.read_exact_at(dst, offset)
        }
    }

    #[cfg(windows)]
    mod windows {
        use std::fs::File;
        use std::io;
        use std::io::{Read, Seek, SeekFrom};
        use std::path::Path;

        pub(crate) fn read_file_chunk_sync(
            dst: &mut [u8],
            path: impl AsRef<Path>,
            offset: u64,
        ) -> Result<(), io::Error> {
            let mut file = File::open(path)?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(dst)
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
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::task::Poll;

    use bytes::{Buf, Bytes};
    use tempfile::NamedTempFile;

    use crate::io::part_reader::{Builder, PartData, PartReader};
    use crate::io::stream::{PartStream, StreamContext};
    use crate::io::InputStream;

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
        let reader = Builder::new().part_size(5).stream(stream).build();
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
        let reader = Builder::new().part_size(part_size).stream(stream).build();

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
            _stream_cx: &StreamContext,
            _cx: &mut std::task::Context<'_>,
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
        let reader = Builder::new().part_size(5).stream(stream).build();
        let parts = collect_parts(reader).await;
        let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }
}
