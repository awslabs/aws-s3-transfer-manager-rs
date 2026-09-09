/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! HTTP body implementations for file-backed `PutObject` uploads.
//!
//! [`FileBodySource`] opens the file once before the retryable SDK body is
//! constructed. Every attempt clones that same file identity and the client's
//! shared memory pool.
//!
//! [`DirectFileBody`] performs positioned reads on a managed polling thread.
//! [`OffloadedFileBody`] moves the same operation through `spawn_blocking` and
//! a bounded channel when the shared Tokio runtime must not block. Both emit
//! owner-backed `Bytes` frames from pooled chunks and reacquire storage for each
//! retry attempt.

use std::fs::File;
use std::future::poll_fn;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_1x::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

use crate::io::fs::read_exact_at;
use crate::io::PartBuffer;
use crate::memory::{BufferPool, SegmentedBytes};

/// Byte count per chunk yielded as an [`http_body_1x::Frame`].
///
/// Bounds peak memory for arbitrarily large PutObject bodies independent of
/// the configured MPU threshold. A user could configure
/// [`Config::multipart_threshold`](crate::Config) above S3's single-PUT
/// limit (5 GB); without chunking, the body would buffer the entire payload.
pub(crate) const FILE_BODY_CHUNK_SIZE: usize = 1024 * 1024;

/// Channel capacity for [`OffloadedFileBody`]. Bounds the number of chunks
/// the reader task may stage ahead of the HTTP layer. Pool accounting remains
/// authoritative when body frames are retained downstream.
const FILE_BODY_READAHEAD: usize = 4;

/// Stable file identity and memory domain shared by all body attempts.
#[derive(Clone, Debug)]
pub(crate) struct FileBodySource {
    file: Arc<File>,
    pool: BufferPool,
}

impl FileBodySource {
    /// Opens `path` once for use by every SDK and transfer-manager retry.
    pub(crate) fn open(path: &Path, pool: BufferPool) -> io::Result<Self> {
        let file = File::open(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to open {}: {error}", path.display()),
            )
        })?;
        Ok(Self {
            file: Arc::new(file),
            pool,
        })
    }
}

/// Payload bytes not yet emitted by a file body.
#[derive(Debug)]
struct FileBodyProgress {
    pending: SegmentedBytes,
    remaining: u64,
}

impl FileBodyProgress {
    fn new(length: u64) -> Self {
        Self {
            pending: SegmentedBytes::from(Bytes::new()),
            remaining: length,
        }
    }

    /// Makes one completed pooled chunk available to the HTTP layer.
    fn publish(&mut self, data: SegmentedBytes) -> io::Result<()> {
        if !self.pending.is_empty() {
            return Err(io::Error::other(
                "file body replaced bytes that had not been emitted",
            ));
        }

        let count = data.len() as u64;
        if count == 0 && self.remaining != 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file body produced no bytes before its declared length",
            ));
        }
        if count > self.remaining {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file body produced more bytes than its declared length",
            ));
        }

        self.pending = data;
        Ok(())
    }

    /// Removes the next presentation segment and records it as emitted.
    fn take_frame(&mut self) -> Option<Frame<Bytes>> {
        let bytes = self.pending.take_front_segment()?;
        self.remaining = self
            .remaining
            .checked_sub(bytes.len() as u64)
            .expect("published file body segment exceeded its remaining length");
        Some(Frame::data(bytes))
    }

    /// Releases retained bytes and makes an error terminal for this body.
    fn terminate(&mut self) {
        self.pending = SegmentedBytes::from(Bytes::new());
        self.remaining = 0;
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }
}

/// File-backed HTTP body for callers on a managed thread with direct I/O.
///
/// Pool admission may return `Poll::Pending`. Once storage is available,
/// `poll_frame` reads the next chunk synchronously. This body is valid only
/// when the transfer manager owns the polling thread.
///
/// Each retry starts at `offset` but retains the same [`FileBodySource`].
#[derive(Debug)]
pub(crate) struct DirectFileBody {
    source: FileBodySource,
    /// Absolute byte offset of the next read.
    offset: u64,
    /// Bytes not yet read from the file.
    unread: u64,
    /// Read bytes not yet emitted by the body.
    progress: FileBodyProgress,
    /// Admission or mutable storage for the next chunk.
    buffer: Option<PartBuffer>,
}

impl DirectFileBody {
    pub(crate) fn new(source: FileBodySource, offset: u64, length: u64) -> Self {
        Self {
            source,
            offset,
            unread: length,
            progress: FileBodyProgress::new(length),
            buffer: None,
        }
    }
}

impl Body for DirectFileBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        loop {
            if let Some(frame) = this.progress.take_frame() {
                return Poll::Ready(Some(Ok(frame)));
            }
            if this.unread == 0 {
                if this.progress.is_end_stream() {
                    return Poll::Ready(None);
                }
                this.progress.terminate();
                return Poll::Ready(Some(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "file body ended before its declared length",
                ))));
            }

            let to_read = (this.unread as usize).min(FILE_BODY_CHUNK_SIZE);
            let buffer = this
                .buffer
                .get_or_insert_with(|| PartBuffer::new(this.source.pool.clone(), to_read));
            match buffer.poll_acquire(cx, to_read) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(error)) => {
                    this.unread = 0;
                    this.progress.terminate();
                    this.buffer = None;
                    return Poll::Ready(Some(Err(error)));
                }
            }

            if let Err(error) = read_exact_at(&this.source.file, buffer, this.offset) {
                this.unread = 0;
                this.progress.terminate();
                this.buffer = None;
                return Poll::Ready(Some(Err(error)));
            }

            let data = this
                .buffer
                .take()
                .expect("completed direct file read lost its buffer")
                .freeze();
            this.offset += to_read as u64;
            this.unread -= to_read as u64;
            if let Err(error) = this.progress.publish(data) {
                this.unread = 0;
                this.progress.terminate();
                return Poll::Ready(Some(Err(error)));
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.progress.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.progress.is_end_stream()
    }
}

/// File-backed HTTP body for callers on the shared tokio runtime.
///
/// A spawned reader task acquires and reads pooled chunks via `spawn_blocking`,
/// then pushes their immutable segments through a bounded channel.
/// `poll_frame` drains the channel with ordinary waker registration.
///
/// The reader task is spawned lazily on the first `poll_frame` call. Dropping
/// the body aborts admission or channel waits. An in-flight `spawn_blocking`
/// read cannot be cancelled, so it retains its buffer until the operating
/// system call completes.
pub(crate) struct OffloadedFileBody {
    state: OffloadedState,
    /// Received bytes not yet emitted by the body.
    progress: FileBodyProgress,
}

enum OffloadedState {
    /// Reader task not yet spawned.
    Pending { source: FileBodySource, offset: u64 },
    /// Reader task streaming chunks through `rx`.
    Active {
        rx: mpsc::Receiver<io::Result<SegmentedBytes>>,
        task: tokio::task::JoinHandle<()>,
    },
}

impl std::fmt::Debug for OffloadedFileBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &self.state {
            OffloadedState::Pending { .. } => "pending",
            OffloadedState::Active { .. } => "active",
        };
        f.debug_struct("OffloadedFileBody")
            .field("state", &state)
            .field("progress", &self.progress)
            .finish_non_exhaustive()
    }
}

impl OffloadedFileBody {
    pub(crate) fn new(source: FileBodySource, offset: u64, length: u64) -> Self {
        Self {
            state: OffloadedState::Pending { source, offset },
            progress: FileBodyProgress::new(length),
        }
    }
}

impl Drop for OffloadedFileBody {
    fn drop(&mut self) {
        if let OffloadedState::Active { task, .. } = &self.state {
            task.abort();
        }
    }
}

impl Body for OffloadedFileBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();
        loop {
            if let Some(frame) = this.progress.take_frame() {
                return Poll::Ready(Some(Ok(frame)));
            }
            if this.progress.is_end_stream() {
                return Poll::Ready(None);
            }
            if let OffloadedState::Pending { source, offset } = &this.state {
                let (tx, rx) = mpsc::channel(FILE_BODY_READAHEAD);
                let task = tokio::spawn(read_task(
                    source.clone(),
                    *offset,
                    this.progress.remaining,
                    tx,
                ));
                this.state = OffloadedState::Active { rx, task };
            }

            let rx = match &mut this.state {
                OffloadedState::Active { rx, .. } => rx,
                OffloadedState::Pending { .. } => unreachable!("transitioned above"),
            };
            match rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    this.progress.terminate();
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "file reader stopped before the declared body length",
                    ))));
                }
                Poll::Ready(Some(Ok(data))) => {
                    if let Err(error) = this.progress.publish(data) {
                        this.progress.terminate();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    this.progress.terminate();
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.progress.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.progress.is_end_stream()
    }
}

/// Acquires, fills, and publishes bounded chunks for [`OffloadedFileBody`].
async fn read_task(
    source: FileBodySource,
    start_offset: u64,
    length: u64,
    tx: mpsc::Sender<io::Result<SegmentedBytes>>,
) {
    let mut offset = start_offset;
    let mut remaining = length;

    while remaining > 0 {
        let to_read = (remaining as usize).min(FILE_BODY_CHUNK_SIZE);
        let read_offset = offset;
        let mut buffer = PartBuffer::new(source.pool.clone(), to_read);
        if let Err(error) = poll_fn(|cx| buffer.poll_acquire(cx, to_read)).await {
            let _ = tx.send(Err(error)).await;
            return;
        }
        let file = Arc::clone(&source.file);

        let read_result = tokio::task::spawn_blocking(move || -> io::Result<SegmentedBytes> {
            read_exact_at(&file, &mut buffer, read_offset)?;
            Ok(buffer.freeze())
        })
        .await;

        match read_result {
            Ok(Ok(data)) => {
                let count = data.len() as u64;
                if tx.send(Ok(data)).await.is_err() {
                    return;
                }
                offset += count;
                remaining -= count;
            }
            Ok(Err(e)) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
            Err(_join_err) => {
                let _ = tx
                    .send(Err(io::Error::other("file read task panicked")))
                    .await;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;
    use std::future::poll_fn;
    use std::io::Write;
    use std::task::Waker;

    use crate::io::InputStream;
    use crate::types::MemoryBudgetConfig;

    const TEST_POOL_CAPACITY: usize = 4 * 1024 * 1024;

    async fn collect_body<B>(mut body: B) -> Result<Vec<u8>, B::Error>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: Debug,
    {
        let mut out = Vec::new();
        loop {
            let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            match frame {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        out.extend_from_slice(&data);
                    }
                }
                Some(Err(e)) => return Err(e),
                None => return Ok(out),
            }
        }
    }

    fn write_tempfile(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(contents).unwrap();
        tmp.flush().unwrap();
        tmp
    }

    fn test_pool() -> BufferPool {
        BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(TEST_POOL_CAPACITY))
            .build()
            .unwrap()
    }

    fn source(path: &Path, pool: &BufferPool) -> FileBodySource {
        FileBodySource::open(path, pool.clone()).unwrap()
    }

    fn poll_body_once<B>(body: &mut B) -> Poll<Option<Result<Frame<Bytes>, B::Error>>>
    where
        B: Body<Data = Bytes> + Unpin,
    {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        Pin::new(body).poll_frame(&mut cx)
    }

    #[test]
    fn file_body_progress_tracks_unemitted_segments() {
        let mut data = SegmentedBytes::from(Bytes::from_static(b"left"));
        data.append(SegmentedBytes::from(Bytes::from_static(b"right")));
        let mut progress = FileBodyProgress::new(data.len() as u64);

        progress.publish(data).unwrap();
        assert_eq!(progress.size_hint().exact(), Some(9));
        assert_eq!(
            progress.take_frame().unwrap().into_data().unwrap(),
            Bytes::from_static(b"left")
        );
        assert_eq!(progress.size_hint().exact(), Some(5));
        assert!(!progress.is_end_stream());
        assert_eq!(
            progress.take_frame().unwrap().into_data().unwrap(),
            Bytes::from_static(b"right")
        );
        assert_eq!(progress.size_hint().exact(), Some(0));
        assert!(progress.is_end_stream());
    }

    #[test]
    fn file_body_progress_rejects_invalid_publication_lengths() {
        let mut empty = FileBodyProgress::new(1);
        let error = empty
            .publish(SegmentedBytes::from(Bytes::new()))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

        let mut oversized = FileBodyProgress::new(1);
        let error = oversized
            .publish(SegmentedBytes::from(Bytes::from_static(b"too long")))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    // ---- DirectFileBody ----

    #[test]
    fn direct_body_size_hint_tracks_unemitted_bytes() {
        let size = FILE_BODY_CHUNK_SIZE + 37;
        let payload = vec![0x5a; size];
        let tmp = write_tempfile(&payload);
        let pool = test_pool();
        let mut body = DirectFileBody::new(source(tmp.path(), &pool), 0, size as u64);

        assert_eq!(body.size_hint().exact(), Some(size as u64));
        let frame = match poll_body_once(&mut body) {
            Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
            result => panic!("expected one body frame, got {result:?}"),
        };
        assert_eq!(
            body.size_hint().exact(),
            Some(size as u64 - frame.len() as u64)
        );
        assert!(!body.is_end_stream());

        drop(frame);
        drop(body);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn direct_body_reads_small_file_single_frame() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let pool = test_pool();
        let body = DirectFileBody::new(source(tmp.path(), &pool), 0, payload.len() as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, payload);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn direct_body_chunks_payload_larger_than_chunk_size() {
        // Must span at least 2 chunks.
        let size = FILE_BODY_CHUNK_SIZE + FILE_BODY_CHUNK_SIZE / 4;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let pool = test_pool();
        let body = DirectFileBody::new(source(tmp.path(), &pool), 0, size as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out.len(), size);
        assert_eq!(out, payload);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn direct_body_reads_from_offset() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let offset = 1024;
        let length = 2048;
        let pool = test_pool();
        let body = DirectFileBody::new(source(tmp.path(), &pool), offset, length);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, &payload[offset as usize..(offset + length) as usize]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn direct_body_frames_retain_pool_ownership() {
        let payload = b"owner-backed body frame";
        let tmp = write_tempfile(payload);
        let pool = test_pool();
        let mut body = DirectFileBody::new(source(tmp.path(), &pool), 0, payload.len() as u64);

        let frame = match poll_body_once(&mut body) {
            Poll::Ready(Some(Ok(frame))) => frame.into_data().unwrap(),
            result => panic!("expected one body frame, got {result:?}"),
        };
        assert_eq!(frame, payload[..]);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            pool.carrier_size() as u64
        );

        drop(frame);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        assert!(matches!(poll_body_once(&mut body), Poll::Ready(None)));
    }

    #[test]
    fn dropping_direct_body_cancels_queued_admission() {
        let payload = b"blocked direct body";
        let tmp = write_tempfile(payload);
        let pool = test_pool();
        let reservation = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let acquisition = pool
            .acquire(&reservation, TEST_POOL_CAPACITY)
            .expect("test pool should be fully available");
        let mut body = DirectFileBody::new(source(tmp.path(), &pool), 0, payload.len() as u64);

        assert!(poll_body_once(&mut body).is_pending());
        assert_eq!(pool.metrics().queued_reservations(), 1);
        drop(body);
        assert_eq!(pool.metrics().queued_reservations(), 0);

        drop(acquisition);
        reservation.close_acquisition();
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    // ---- OffloadedFileBody ----

    #[test]
    fn empty_offloaded_body_finishes_without_spawning_reader() {
        let tmp = write_tempfile(&[]);
        let pool = test_pool();
        let mut body = OffloadedFileBody::new(source(tmp.path(), &pool), 0, 0);

        assert!(matches!(poll_body_once(&mut body), Poll::Ready(None)));
        assert!(matches!(body.state, OffloadedState::Pending { .. }));
        assert_eq!(pool.metrics().reservation_enqueues_total(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's runtime uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn offloaded_body_rejects_early_reader_completion() {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let task = tokio::spawn(async {});
        let mut body = OffloadedFileBody {
            state: OffloadedState::Active { rx, task },
            progress: FileBodyProgress::new(1),
        };

        let error = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .expect("reader completion should produce a terminal frame")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(body.is_end_stream());
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn offloaded_body_reads_small_file() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let pool = test_pool();
        let body = OffloadedFileBody::new(source(tmp.path(), &pool), 0, payload.len() as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, payload);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn offloaded_body_chunks_payload_larger_than_chunk_size() {
        let size = FILE_BODY_CHUNK_SIZE * 3 + 512;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let pool = test_pool();
        let body = OffloadedFileBody::new(source(tmp.path(), &pool), 0, size as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out.len(), size);
        assert_eq!(out, payload);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn offloaded_body_reads_from_offset() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let offset = 1024;
        let length = 2048;
        let pool = test_pool();
        let body = OffloadedFileBody::new(source(tmp.path(), &pool), offset, length);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, &payload[offset as usize..(offset + length) as usize]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    async fn dropping_offloaded_body_cancels_queued_admission() {
        let payload = b"blocked offloaded body";
        let tmp = write_tempfile(payload);
        let pool = test_pool();
        let reservation = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let acquisition = pool
            .acquire(&reservation, TEST_POOL_CAPACITY)
            .expect("test pool should be fully available");
        let mut body = OffloadedFileBody::new(source(tmp.path(), &pool), 0, payload.len() as u64);

        assert!(poll_body_once(&mut body).is_pending());
        for _ in 0..100 {
            if pool.metrics().queued_reservations() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(pool.metrics().queued_reservations(), 1);

        drop(body);
        for _ in 0..100 {
            if pool.metrics().queued_reservations() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(pool.metrics().queued_reservations(), 0);

        drop(acquisition);
        reservation.close_acquisition();
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[cfg_attr(miri, ignore)] // Tokio's I/O driver uses unsupported platform FFI under Miri.
    #[tokio::test]
    #[cfg(unix)]
    async fn retries_keep_the_open_file_identity_after_path_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("body");
        let replacement = directory.path().join("replacement");
        let original = b"original opened file";
        let changed = b"replacement contents";
        assert_eq!(original.len(), changed.len());
        std::fs::write(&path, original).unwrap();

        let pool = test_pool();
        let stream = InputStream::read_from().path(&path).build().unwrap();
        let body = stream.into_sdk_body(true, pool.clone()).unwrap();
        let retry = body.try_clone().expect("file body should be retryable");
        std::fs::write(&replacement, changed).unwrap();
        std::fs::rename(&replacement, &path).unwrap();

        assert_eq!(collect_body(body).await.unwrap(), original);
        assert_eq!(collect_body(retry).await.unwrap(), original);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn file_body_source_surfaces_open_error_before_request_dispatch() {
        let stream = InputStream::read_from()
            .path("/nonexistent/path/that/should/not/exist/xyz")
            .length(1024)
            .build()
            .unwrap();

        let error = stream.into_sdk_body(true, test_pool()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
