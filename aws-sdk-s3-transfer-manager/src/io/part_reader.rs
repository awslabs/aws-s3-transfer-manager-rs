/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::cmp;
use std::fs::File;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use tokio::sync::Notify;

use crate::io::error::Error;
use crate::io::fs::read_exact_at;
use crate::io::path_body::PathBody;
use crate::io::stream::RawInputStream;
use crate::io::InputStream;
use crate::io::PartData;
use crate::memory::BufferPool;
use crate::metrics::unit::ByteUnit;

use super::stream::{BoxStream, StreamContext};

/// Builder for creating a `PartReader`
#[derive(Debug)]
pub(crate) struct Builder {
    stream: Option<RawInputStream>,
    part_size: usize,
    direct_io: bool,
    buffer_pool: Option<BufferPool>,
    metrics: Option<std::sync::Arc<crate::transfer::MetricsState>>,
    telemetry: Option<std::sync::Arc<crate::telemetry::Telemetry>>,
}

impl Builder {
    pub(crate) fn new() -> Self {
        Self {
            stream: None,
            part_size: 5 * ByteUnit::Mebibyte.as_bytes_u64() as usize,
            direct_io: false,
            buffer_pool: None,
            metrics: None,
            telemetry: None,
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

    /// Sets the shared pool available to stream producers.
    pub(crate) fn buffer_pool(mut self, buffer_pool: BufferPool) -> Self {
        self.buffer_pool = Some(buffer_pool);
        self
    }

    /// Set the metrics state for recording I/O metrics.
    pub(crate) fn metrics(
        mut self,
        metrics: std::sync::Arc<crate::transfer::MetricsState>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set the per-client telemetry for forwarding I/O samples.
    pub(crate) fn telemetry(
        mut self,
        telemetry: std::sync::Arc<crate::telemetry::Telemetry>,
    ) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    pub(crate) fn build(self) -> Result<PartReader, Error> {
        let stream = self.stream.expect("input stream set");
        let buffer_pool = self.buffer_pool.expect("buffer pool set");
        let metrics = self.metrics.expect("metrics set");
        let telemetry = self.telemetry.expect("telemetry set");
        PartReader::new(
            stream,
            self.part_size,
            self.direct_io,
            buffer_pool,
            metrics,
            telemetry,
        )
    }
}

/// Produces numbered upload parts from memory, files, or a custom [`PartStream`].
///
/// Each started read is independent work owned by [`NextPartFuture`]. Custom streams remain
/// logically single-owner because [`PartStreamGate`] hands their mutable stream value to exactly one
/// future at a time.
///
/// [`PartStream`]: crate::io::PartStream
#[derive(Debug)]
pub(crate) struct PartReader {
    inner: Inner,
    stream_cx: Arc<StreamContext>,
}

impl PartReader {
    fn new(
        raw: RawInputStream,
        part_size: usize,
        direct_io: bool,
        buffer_pool: BufferPool,
        metrics: std::sync::Arc<crate::transfer::MetricsState>,
        telemetry: std::sync::Arc<crate::telemetry::Telemetry>,
    ) -> Result<Self, Error> {
        let inner = match raw {
            RawInputStream::Buf(buf) => Inner::Bytes(BytesPartReader::new(buf)),
            RawInputStream::Fs(path_body) => {
                Inner::Fs(Arc::new(PathBodyPartReader::new(path_body)?))
            }
            RawInputStream::Dyn(box_body) => Inner::Dyn(Arc::new(DynPartReader::new(box_body))),
        };

        let stream_cx = Arc::new(StreamContext::new(
            part_size,
            buffer_pool,
            direct_io,
            metrics,
            telemetry,
        ));
        Ok(Self { inner, stream_cx })
    }

    #[allow(dead_code)] // TODO: re-wire upload part validation
    pub(crate) fn part_size(&self) -> usize {
        self.stream_cx.part_size()
    }

    /// Starts one owned source operation.
    ///
    /// In-memory reads are completed synchronously. File reads retain their asynchronous operation.
    /// Custom streams serialize mutable polling through a private gate: callers arriving while a
    /// poll is actively making progress wait for direct handoff, while callers arriving after the
    /// source reports `Poll::Pending` receive [`PartReadStart::Blocked`] without creating another
    /// source future.
    pub(crate) async fn start_part_read(self: &Arc<Self>) -> PartReadStart {
        match &self.inner {
            Inner::Bytes(bytes) => {
                PartReadStart::Ready(NextPartFuture::ready(bytes.next_part(&self.stream_cx)))
            }
            Inner::Fs(path_body) => {
                let path_body = Arc::clone(path_body);
                let stream_cx = Arc::clone(&self.stream_cx);
                PartReadStart::Ready(NextPartFuture::file(async move {
                    path_body.next_part(&stream_cx).await
                }))
            }
            Inner::Dyn(part_stream) => match part_stream.gate.acquire().await {
                SourceAccess::Acquired(stream) => {
                    let future = DynNextPartFuture {
                        part_stream: Arc::clone(part_stream),
                        stream_cx: Arc::clone(&self.stream_cx),
                        stream: Some(stream),
                    };
                    PartReadStart::Ready(NextPartFuture::dynamic(future))
                }
                SourceAccess::Blocked => PartReadStart::Blocked,
                SourceAccess::Finished => PartReadStart::Finished,
            },
        }
    }

    /// Returns whether a custom stream cannot start another source operation.
    pub(crate) fn source_unavailable(&self) -> bool {
        match &self.inner {
            Inner::Dyn(part_stream) => part_stream.gate.is_unavailable(),
            Inner::Bytes(_) | Inner::Fs(_) => false,
        }
    }

    /// Number of parts this reader has produced.
    ///
    /// Exact for custom streams because the count is advanced before the gate returns the stream.
    /// File readers count claimed ranges, so their value may lead an in-flight or failed read.
    pub(crate) fn parts_yielded(&self) -> u64 {
        match &self.inner {
            Inner::Bytes(bytes) => bytes.parts_yielded(),
            Inner::Fs(path_body) => path_body.parts_yielded(),
            Inner::Dyn(part_stream) => part_stream.parts_yielded.load(Ordering::Acquire),
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
enum Inner {
    Bytes(BytesPartReader),
    Fs(Arc<PathBodyPartReader>),
    Dyn(Arc<DynPartReader>),
}

/// Result of obtaining authority to start one part read.
pub(crate) enum PartReadStart {
    /// The exact source operation to poll and retain if it blocks.
    Ready(NextPartFuture),
    /// A custom stream already has one externally blocked operation.
    Blocked,
    /// The source has already reported end-of-stream.
    Finished,
}

/// Owned source operation that may move between scheduler executions.
pub(crate) struct NextPartFuture {
    inner: NextPartFutureInner,
}

enum NextPartFutureInner {
    /// In-memory reads complete while the operation is created.
    Ready(Option<Result<Option<PartData>, Error>>),
    /// File reads remain boxed until the pooled file-read path supplies a concrete future.
    File(Pin<Box<dyn Future<Output = Result<Option<PartData>, Error>> + Send + 'static>>),
    /// A custom stream poll that owns the stream gate until completion or drop.
    Dynamic(DynNextPartFuture),
}

impl NextPartFuture {
    fn ready(result: Result<Option<PartData>, Error>) -> Self {
        Self {
            inner: NextPartFutureInner::Ready(Some(result)),
        }
    }

    fn file(
        future: impl Future<Output = Result<Option<PartData>, Error>> + Send + 'static,
    ) -> Self {
        Self {
            inner: NextPartFutureInner::File(Box::pin(future)),
        }
    }

    fn dynamic(future: DynNextPartFuture) -> Self {
        Self {
            inner: NextPartFutureInner::Dynamic(future),
        }
    }
}

impl Future for NextPartFuture {
    type Output = Result<Option<PartData>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().inner {
            NextPartFutureInner::Ready(result) => Poll::Ready(
                result
                    .take()
                    .expect("completed part-read future polled again"),
            ),
            NextPartFutureInner::File(future) => future.as_mut().poll(cx),
            NextPartFutureInner::Dynamic(future) => Pin::new(future).poll(cx),
        }
    }
}

impl std::fmt::Debug for NextPartFuture {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let source = match &self.inner {
            NextPartFutureInner::Ready(_) => "memory",
            NextPartFutureInner::File(_) => "file",
            NextPartFutureInner::Dynamic(_) => "custom",
        };
        formatter
            .debug_struct("NextPartFuture")
            .field("source", &source)
            .finish_non_exhaustive()
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
    fn parts_yielded(&self) -> u64 {
        self.state.lock().expect("lock valid").part_number - 1
    }

    fn next_part(&self, stream_cx: &StreamContext) -> Result<Option<PartData>, Error> {
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

/// Produces pooled multipart payloads from disjoint ranges of one open file.
///
/// Range claims are serialized, but the resulting positional reads may complete
/// concurrently and out of part-number order.
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
        let file = Arc::new(File::open(&body.path).map_err(|e| {
            Error::from(std::io::Error::new(
                e.kind(),
                format!("failed to open {}: {e}", body.path.display()),
            ))
        })?);
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
    fn parts_yielded(&self) -> u64 {
        self.state.lock().expect("lock valid").part_number - 1
    }

    /// Claims and reads the next file range.
    ///
    /// The complete range is admitted before file I/O begins. The returned
    /// payload retains its pooled storage independently of this reader.
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
        let part_size = part_size as usize;
        let mut dst = stream_cx.part_buffer();
        poll_fn(|cx| dst.poll_acquire(cx, part_size)).await?;

        if stream_cx.direct_io() {
            // Managed threads: read directly, no thread pool hop.
            read_exact_at(&self.file, &mut dst, offset)?;
        } else {
            // Shared runtime: offload to blocking thread pool.
            let fd = Arc::clone(&self.file);
            dst = tokio::task::spawn_blocking(move || {
                read_exact_at(&fd, &mut dst, offset)?;
                Ok::<_, std::io::Error>(dst)
            })
            .await??;
        }

        stream_cx.record_io(&crate::metrics::IoSample {
            disk_read: part_size as u64,
            ..Default::default()
        });

        Ok(Some(
            PartData::from_segmented(part_number, dst.freeze()).mark_last(is_last),
        ))
    }

    /// Claims the next disjoint file range without performing I/O.
    ///
    /// A successful claim advances the shared cursor exactly once. Completion
    /// order does not affect later range offsets or part numbers.
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

/// One claimed file range and its multipart metadata.
#[derive(Debug, Clone, Copy)]
struct PathBodyReadCursor {
    offset: u64,
    part_number: u64,
    part_size: u64,
    is_last: bool,
}

/// Custom-stream reader state shared by the reader and its currently active future.
#[derive(Debug)]
struct DynPartReader {
    gate: PartStreamGate,
    /// Counted explicitly because a custom stream has no internal part cursor.
    parts_yielded: AtomicU64,
}

impl DynPartReader {
    fn new(inner: BoxStream) -> Self {
        Self {
            gate: PartStreamGate::new(inner),
            parts_yielded: AtomicU64::new(0),
        }
    }

    async fn full_object_checksum(&self) -> Option<String> {
        let state = self.gate.state.lock().expect("part stream gate poisoned");
        match &*state {
            GateState::Available(stream) | GateState::Finished(stream) => {
                stream.full_object_checksum()
            }
            GateState::Polling | GateState::Blocked => {
                panic!("part stream checksum requested while a source operation remained")
            }
        }
    }
}

/// Transfers exclusive ownership of a custom stream between part-read futures.
///
/// `Polling` is a short handoff state while the active future has not reported whether it can make
/// progress. Once that future observes `Poll::Pending`, `Blocked` lets later executions return
/// immediately instead of waiting behind the same external dependency.
#[derive(Debug)]
struct PartStreamGate {
    state: Mutex<GateState>,
    changed: Notify,
}

/// Ownership state for one custom [`PartStream`].
///
/// [`PartStream`]: crate::io::PartStream
#[derive(Debug)]
enum GateState {
    /// No source operation is active; the next caller may take the stream.
    Available(BoxStream),
    /// One future owns the stream and has not yet reported source blockage.
    Polling,
    /// The owning future returned `Poll::Pending` and is retained by transfer state.
    Blocked,
    /// End-of-stream was observed; the stream is retained only for final metadata.
    Finished(BoxStream),
}

/// Result of trying to take custom-stream polling authority.
enum SourceAccess {
    /// The caller owns the stream until it releases, finishes, or drops the operation.
    Acquired(BoxStream),
    /// A retained future is waiting on the external source.
    Blocked,
    /// End-of-stream was already observed.
    Finished,
}

impl PartStreamGate {
    fn new(stream: BoxStream) -> Self {
        Self {
            state: Mutex::new(GateState::Available(stream)),
            changed: Notify::new(),
        }
    }

    async fn acquire(&self) -> SourceAccess {
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().expect("part stream gate poisoned");
                match &*state {
                    GateState::Blocked => return SourceAccess::Blocked,
                    GateState::Polling => {}
                    GateState::Finished(_) => return SourceAccess::Finished,
                    GateState::Available(_) => {
                        let GateState::Available(stream) =
                            std::mem::replace(&mut *state, GateState::Polling)
                        else {
                            unreachable!("available state changed under lock");
                        };
                        return SourceAccess::Acquired(stream);
                    }
                }
            }
            changed.await;
        }
    }

    fn is_unavailable(&self) -> bool {
        matches!(
            &*self.state.lock().expect("part stream gate poisoned"),
            GateState::Blocked | GateState::Finished(_)
        )
    }

    fn mark_blocked(&self) {
        let notify = {
            let mut state = self.state.lock().expect("part stream gate poisoned");
            match &*state {
                GateState::Polling => {
                    *state = GateState::Blocked;
                    true
                }
                GateState::Blocked => false,
                GateState::Available(_) | GateState::Finished(_) => {
                    panic!("part stream became blocked without polling authority")
                }
            }
        };
        if notify {
            self.changed.notify_waiters();
        }
    }

    fn release(&self, stream: BoxStream) {
        {
            let mut state = self.state.lock().expect("part stream gate poisoned");
            assert!(
                matches!(&*state, GateState::Polling | GateState::Blocked),
                "part stream released without polling authority"
            );
            *state = GateState::Available(stream);
        }
        self.changed.notify_one();
    }

    fn finish(&self, stream: BoxStream) {
        {
            let mut state = self.state.lock().expect("part stream gate poisoned");
            assert!(
                matches!(&*state, GateState::Polling | GateState::Blocked),
                "part stream finished without polling authority"
            );
            *state = GateState::Finished(stream);
        }
        self.changed.notify_waiters();
    }
}

/// Exact custom-stream operation retained across scheduler executions.
struct DynNextPartFuture {
    part_stream: Arc<DynPartReader>,
    stream_cx: Arc<StreamContext>,
    stream: Option<BoxStream>,
}

impl Future for DynNextPartFuture {
    type Output = Result<Option<PartData>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let stream = this
            .stream
            .as_mut()
            .expect("custom stream future polled after completion");

        match stream.poll_next(cx, &this.stream_cx) {
            Poll::Pending => {
                this.part_stream.gate.mark_blocked();
                Poll::Pending
            }
            Poll::Ready(result) => {
                let stream = this
                    .stream
                    .take()
                    .expect("custom stream disappeared at completion");
                if result.is_none() {
                    this.part_stream.gate.finish(stream);
                } else {
                    this.part_stream.gate.release(stream);
                }
                match result {
                    Some(Ok(part)) => {
                        this.part_stream
                            .parts_yielded
                            .fetch_add(1, Ordering::Release);
                        Poll::Ready(Ok(Some(part)))
                    }
                    Some(Err(error)) => Poll::Ready(Err(error.into())),
                    None => Poll::Ready(Ok(None)),
                }
            }
        }
    }
}

impl Drop for DynNextPartFuture {
    fn drop(&mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        self.part_stream.gate.release(stream);
    }
}

#[cfg(test)]
mod test {
    use std::future::Future;
    use std::io::Write;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};

    use bytes::{Buf, Bytes};
    use tempfile::NamedTempFile;

    use crate::io::part_reader::{
        Builder, BytesPartReader, PartData, PartReadStart, PartReader, PathBodyPartReader,
    };
    use crate::io::path_body::PathBody;
    use crate::io::stream::{PartStream, StreamContext};
    use crate::io::InputStream;
    use crate::memory::BufferPool;
    use crate::types::MemoryBudgetConfig;

    fn test_stream_cx(part_size: usize) -> StreamContext {
        StreamContext::new(
            part_size,
            test_pool(),
            false,
            test_metrics(),
            test_telemetry(),
        )
    }

    fn test_pool() -> BufferPool {
        BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(1024 * 1024))
            .build()
            .unwrap()
    }

    fn test_metrics() -> std::sync::Arc<crate::transfer::MetricsState> {
        std::sync::Arc::new(crate::transfer::MetricsState::new())
    }

    fn test_telemetry() -> std::sync::Arc<crate::telemetry::Telemetry> {
        std::sync::Arc::new(crate::telemetry::Telemetry::new(
            std::time::Duration::from_secs(1),
        ))
    }

    async fn collect_parts(reader: PartReader) -> Vec<PartData> {
        let reader = Arc::new(reader);
        let mut parts = Vec::new();
        let mut expected_part_number = 1;
        loop {
            let PartReadStart::Ready(future) = reader.start_part_read().await else {
                panic!("test source unexpectedly blocked");
            };
            let Some(part) = future.await.unwrap() else {
                break;
            };
            assert_eq!(expected_part_number, part.part_number);
            expected_part_number += 1;
            parts.push(part);
        }
        parts
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_bytes_part_reader() {
        let data = Bytes::from("a lep is a ball, a tay is a hammer, a flix is a comb");
        let stream = InputStream::from(data.clone());
        let expected = data.chunks(5).collect::<Vec<_>>();
        let reader = Builder::new()
            .part_size(5)
            .stream(stream)
            .buffer_pool(test_pool())
            .metrics(test_metrics())
            .telemetry(test_telemetry())
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
        let pool = test_pool();

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
            .buffer_pool(pool.clone())
            .metrics(test_metrics())
            .telemetry(test_telemetry())
            .build()
            .unwrap();

        let parts = collect_parts(reader).await;
        {
            let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();
            assert_eq!(expected, actual);
        }
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (parts.len() * pool.carrier_size()) as u64
        );
        drop(parts);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_part_reader() {
        path_reader_test(None, None).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_part_reader_with_offset() {
        path_reader_test(None, Some(8)).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_part_reader_with_explicit_length() {
        path_reader_test(Some(12), None).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_part_reader_with_length_and_offset() {
        path_reader_test(Some(23), Some(4)).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn path_reader_fills_multiple_pooled_runs_without_gathering() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let part_size = carrier_size * 2 + 37;
        let data = (0..part_size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&data).unwrap();

        let reader = Arc::new(
            Builder::new()
                .part_size(part_size)
                .direct_io(true)
                .stream(InputStream::read_from().path(tmp.path()).build().unwrap())
                .buffer_pool(pool.clone())
                .metrics(test_metrics())
                .telemetry(test_telemetry())
                .build()
                .unwrap(),
        );
        let PartReadStart::Ready(future) = reader.start_part_read().await else {
            panic!("file source unexpectedly blocked");
        };
        let part = future.await.unwrap().unwrap();
        assert_eq!(part.data.len(), part_size);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 3) as u64
        );

        let segments = part.data.into_segments();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].as_ref(), data);
        drop(segments);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn dropping_file_read_waiting_for_admission_cancels_its_request() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let capacity = pool.metrics().configured_capacity_bytes() as usize;
        let reservation = pool.try_reserve(capacity).unwrap().unwrap();
        let holder = pool.acquire(&reservation, capacity).unwrap();
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&vec![0x5a; carrier_size]).unwrap();

        let reader = Arc::new(
            Builder::new()
                .part_size(carrier_size)
                .stream(InputStream::read_from().path(tmp.path()).build().unwrap())
                .buffer_pool(pool.clone())
                .metrics(test_metrics())
                .telemetry(test_telemetry())
                .build()
                .unwrap(),
        );
        let PartReadStart::Ready(mut future) = reader.start_part_read().await else {
            panic!("file source unexpectedly blocked");
        };
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(Pin::new(&mut future).poll(&mut cx).is_pending());
        assert_eq!(pool.metrics().queued_reservations(), 1);

        drop(future);
        assert_eq!(pool.metrics().queued_reservations(), 0);
        drop(holder);
        drop(reservation);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn short_file_read_releases_pooled_storage() {
        let pool = test_pool();
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"short").unwrap();
        let stream = InputStream::read_from()
            .path(tmp.path())
            .length(10)
            .build()
            .unwrap();
        let reader = Arc::new(
            Builder::new()
                .part_size(10)
                .stream(stream)
                .buffer_pool(pool.clone())
                .metrics(test_metrics())
                .telemetry(test_telemetry())
                .build()
                .unwrap(),
        );
        let PartReadStart::Ready(future) = reader.start_part_read().await else {
            panic!("file source unexpectedly blocked");
        };

        assert!(future.await.is_err());
        assert_eq!(pool.metrics().active_planned_demand_bytes(), 0);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
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
    #[cfg_attr(miri, ignore)]
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
            .buffer_pool(test_pool())
            .metrics(test_metrics())
            .telemetry(test_telemetry())
            .build()
            .unwrap();
        let parts = collect_parts(reader).await;
        let actual = parts.iter().map(|p| p.data.chunk()).collect::<Vec<_>>();
        assert_eq!(expected, actual);
    }

    #[derive(Debug)]
    struct BlockingStream {
        ready: Arc<AtomicBool>,
        source_waker: Arc<Mutex<Option<Waker>>>,
        yielded: bool,
    }

    impl PartStream for BlockingStream {
        fn poll_part(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _stream_cx: &StreamContext,
        ) -> Poll<Option<std::io::Result<PartData>>> {
            if self.yielded {
                return Poll::Ready(None);
            }
            if self.ready.load(Ordering::Acquire) {
                self.yielded = true;
                return Poll::Ready(Some(Ok(PartData::new(1, Bytes::from_static(b"ready")))));
            }
            *self.source_waker.lock().expect("source waker poisoned") = Some(cx.waker().clone());
            Poll::Pending
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::exact(5)
        }
    }

    fn blocking_reader() -> (Arc<PartReader>, Arc<AtomicBool>, Arc<Mutex<Option<Waker>>>) {
        let ready = Arc::new(AtomicBool::new(false));
        let source_waker = Arc::new(Mutex::new(None));
        let stream = BlockingStream {
            ready: Arc::clone(&ready),
            source_waker: Arc::clone(&source_waker),
            yielded: false,
        };
        let reader = Builder::new()
            .part_size(5)
            .stream(InputStream::from_part_stream(stream))
            .buffer_pool(test_pool())
            .metrics(test_metrics())
            .telemetry(test_telemetry())
            .build()
            .unwrap();
        (Arc::new(reader), ready, source_waker)
    }

    // Tokio's test runtime initializes kqueue, which Miri cannot execute.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn custom_stream_pending_blocks_waiters_until_exact_future_resumes() {
        let (reader, ready, source_waker) = blocking_reader();
        let PartReadStart::Ready(mut first) = reader.start_part_read().await else {
            panic!("fresh custom stream was blocked");
        };

        let waiter_reader = Arc::clone(&reader);
        let waiter = tokio::spawn(async move { waiter_reader.start_part_read().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "waiter bypassed active stream poll");

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        assert!(reader.source_unavailable());

        assert!(matches!(waiter.await.unwrap(), PartReadStart::Blocked));

        ready.store(true, Ordering::Release);
        source_waker
            .lock()
            .expect("source waker poisoned")
            .take()
            .expect("source did not register a waker")
            .wake();

        let part = first.await.unwrap().expect("source should yield a part");
        assert_eq!(part.data, Bytes::from_static(b"ready"));
        assert!(!reader.source_unavailable());
        assert_eq!(reader.parts_yielded(), 1);
    }

    // Tokio's test runtime initializes kqueue, which Miri cannot execute.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn custom_stream_ready_completion_hands_gate_to_one_waiter() {
        let data = vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")];
        let reader = Arc::new(
            Builder::new()
                .part_size(6)
                .stream(InputStream::from_part_stream(TestStream::new(data)))
                .buffer_pool(test_pool())
                .metrics(test_metrics())
                .telemetry(test_telemetry())
                .build()
                .unwrap(),
        );

        let PartReadStart::Ready(first) = reader.start_part_read().await else {
            panic!("fresh custom stream was blocked");
        };
        let waiter_reader = Arc::clone(&reader);
        let waiter = tokio::spawn(async move { waiter_reader.start_part_read().await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "waiter bypassed active stream poll");

        assert_eq!(
            first.await.unwrap().unwrap().data,
            Bytes::from_static(b"first")
        );
        let PartReadStart::Ready(second) = waiter.await.unwrap() else {
            panic!("ready handoff reported source blockage");
        };
        assert_eq!(
            second.await.unwrap().unwrap().data,
            Bytes::from_static(b"second")
        );
        assert!(!reader.source_unavailable());
    }

    #[derive(Debug)]
    struct EofOnceStream {
        polls: Arc<AtomicUsize>,
    }

    impl PartStream for EofOnceStream {
        fn poll_part(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _stream_cx: &StreamContext,
        ) -> Poll<Option<std::io::Result<PartData>>> {
            assert_eq!(
                self.polls.fetch_add(1, Ordering::Relaxed),
                0,
                "custom stream was polled after end-of-stream"
            );
            Poll::Ready(None)
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::default()
        }
    }

    // Tokio's test runtime initializes kqueue, which Miri cannot execute.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn custom_stream_eof_retires_waiters_without_another_poll() {
        let polls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(
            Builder::new()
                .part_size(5)
                .stream(InputStream::from_part_stream(EofOnceStream {
                    polls: Arc::clone(&polls),
                }))
                .buffer_pool(test_pool())
                .metrics(test_metrics())
                .telemetry(test_telemetry())
                .build()
                .unwrap(),
        );
        let PartReadStart::Ready(eof) = reader.start_part_read().await else {
            panic!("fresh custom stream was unavailable");
        };
        let waiter_reader = Arc::clone(&reader);
        let waiter = tokio::spawn(async move { waiter_reader.start_part_read().await });
        tokio::task::yield_now().await;

        assert!(eof.await.unwrap().is_none());
        assert!(matches!(waiter.await.unwrap(), PartReadStart::Finished));
        assert!(reader.source_unavailable());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }

    // Tokio's test runtime initializes kqueue, which Miri cannot execute.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn dropping_blocked_custom_future_restores_stream_authority() {
        let (reader, _ready, _source_waker) = blocking_reader();
        let PartReadStart::Ready(mut first) = reader.start_part_read().await else {
            panic!("fresh custom stream was blocked");
        };
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
        assert!(reader.source_unavailable());

        drop(first);
        assert!(!reader.source_unavailable());
        assert!(matches!(
            reader.start_part_read().await,
            PartReadStart::Ready(_)
        ));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_bytes_part_reader_offset_not_aligned_error() {
        let data = Bytes::from("test data for alignment error");
        let reader = BytesPartReader::new(data);
        let stream_cx = test_stream_cx(5);

        // First call should succeed
        let result = reader.next_part(&stream_cx);
        assert!(result.is_ok());

        // Manually corrupt the offset to create misalignment
        {
            let mut state = reader.state.lock().unwrap();
            state.offset = 99; // Invalid offset that doesn't align with part_number
        }

        // Second call should fail with offset_not_aligned_with_part_number error
        let result = reader.next_part(&stream_cx);
        assert!(result.is_err());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_bytes_part_reader_detects_last_true() {
        let data = Bytes::from("test");
        let reader = BytesPartReader::new(data);
        let stream_cx = test_stream_cx(10);

        let result = reader.next_part(&stream_cx).unwrap().unwrap();
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
