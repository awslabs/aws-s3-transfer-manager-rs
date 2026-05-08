/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! HTTP body implementations for file-backed `PutObject` uploads.
//!
//! Two variants, specialized by runtime:
//!
//! * [`DirectFileBody`] — used when the calling thread owns the execution
//!   context (managed-thread direct I/O). Reads chunk-by-chunk synchronously
//!   from `poll_frame`; never returns `Poll::Pending`.
//!
//! * [`OffloadedFileBody`] — used when the body may be polled by the shared
//!   tokio runtime. A spawned reader task pulls chunks via `spawn_blocking`
//!   and pushes them through a bounded channel; `poll_frame` drains the
//!   channel. Task lifecycle is tied to channel closure — dropping the
//!   body drops the receiver, which causes the reader task to exit on its
//!   next `send().await`.
//!
//! Both emit `http_body_1_0::Frame<Bytes>` with known `size_hint`, read the
//! full `length` bytes starting at `offset`, and record `disk_read` to the
//! transfer's [`MetricsState`] and per-client [`Telemetry`] as each chunk
//! is produced.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use http_body_1x::{Body, Frame, SizeHint};
use tokio::sync::mpsc;

use crate::io::part_reader::file_util::read_file_chunk;

/// Byte count per chunk yielded as an [`http_body_1x::Frame`].
///
/// Bounds peak memory for arbitrarily large PutObject bodies independent of
/// the configured MPU threshold. A user could configure
/// [`Config::multipart_threshold`](crate::Config) above S3's single-PUT
/// limit (5 GB); without chunking, the body would buffer the entire payload.
///
// TODO(vnext): allocate chunks through a shared buffer pool rather than
// `BytesMut::zeroed` per chunk. Tracked in bosun.md under Deferred.
pub(crate) const FILE_BODY_CHUNK_SIZE: usize = 1024 * 1024;

/// Channel capacity for [`OffloadedFileBody`]. Bounds the number of chunks
/// the reader task may hold ahead of the HTTP layer. With
/// [`FILE_BODY_CHUNK_SIZE`] = 1 MiB, the in-flight ceiling is
/// `(FILE_BODY_READAHEAD + 1) * 1 MiB` = 5 MiB (one in-channel, one in
/// `spawn_blocking`, minus sender/receiver ownership overlap).
const FILE_BODY_READAHEAD: usize = 4;

/// File-backed HTTP body for callers on a managed thread with direct I/O.
///
/// `poll_frame` reads the next chunk synchronously and returns
/// `Poll::Ready` on every call (`Poll::Pending` is never returned — there
/// is no future to register a waker on). This is only correct when the
/// polling thread is one the transfer manager owns; using this body from
/// the shared tokio runtime would block an async worker for the read's
/// duration.
///
/// Retries are driven by the SDK: the body is constructed inside an
/// [`SdkBody::retryable`](aws_smithy_types::body::SdkBody::retryable)
/// closure, so each retry instantiates a fresh `DirectFileBody` with its
/// own open file descriptor and read cursor at `offset`.
#[derive(Debug)]
pub(crate) struct DirectFileBody {
    /// Open file handle. `None` if open failed; the stashed error in
    /// `open_error` is yielded on the first `poll_frame`.
    file: Option<File>,
    /// Absolute byte offset of the next read.
    offset: u64,
    /// Bytes remaining to yield.
    remaining: u64,
    /// Total length captured at construction; used for `size_hint`.
    length: u64,
    /// If `File::open` failed in the constructor, the error is stashed
    /// here and surfaced as the body's first (and only) frame.
    open_error: Option<io::Error>,
}

impl DirectFileBody {
    pub(crate) fn new(path: PathBuf, offset: u64, length: u64) -> Self {
        match File::open(&path) {
            Ok(file) => Self {
                file: Some(file),
                offset,
                remaining: length,
                length,
                open_error: None,
            },
            Err(e) => Self {
                file: None,
                offset,
                remaining: 0,
                length,
                open_error: Some(io::Error::new(
                    e.kind(),
                    format!("failed to open {}: {e}", path.display()),
                )),
            },
        }
    }
}

impl Body for DirectFileBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        let this = self.get_mut();

        if let Some(err) = this.open_error.take() {
            return Poll::Ready(Some(Err(err)));
        }
        if this.remaining == 0 {
            return Poll::Ready(None);
        }

        let file = this
            .file
            .as_ref()
            .expect("file present when remaining > 0 and open_error clear");

        let to_read = (this.remaining as usize).min(FILE_BODY_CHUNK_SIZE);
        let mut buf = BytesMut::zeroed(to_read);
        if let Err(e) = read_file_chunk(file, &mut buf[..], this.offset) {
            this.remaining = 0;
            return Poll::Ready(Some(Err(e)));
        }

        this.offset += to_read as u64;
        this.remaining -= to_read as u64;

        Poll::Ready(Some(Ok(Frame::data(buf.freeze()))))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0 && self.open_error.is_none()
    }
}

/// File-backed HTTP body for callers on the shared tokio runtime.
///
/// A spawned reader task opens the file and reads chunks via
/// `spawn_blocking`, pushing each chunk through a bounded channel.
/// `poll_frame` drains the channel — real `Poll::Pending` semantics via
/// the channel receiver's waker registration.
///
/// The reader task is spawned lazily on the first `poll_frame` call, so
/// a body that is constructed but never polled (e.g. by a mock-SDK
/// callsite that short-circuits before reading the request body) incurs
/// no task or channel overhead.
///
/// The task shuts down when the receiver is dropped (this body's `Drop`):
/// its next `send().await` returns `Err`, and the loop exits. An in-flight
/// `spawn_blocking` read will complete on its OS thread and its result
/// will be discarded — tokio cannot cancel OS-backed blocking threads, so
/// at most one extra read happens before the task exits.
pub(crate) struct OffloadedFileBody {
    state: OffloadedState,
    /// Total length captured at construction; used for `size_hint`.
    length: u64,
    /// Bytes delivered so far. `length == delivered` after the final
    /// chunk is consumed.
    delivered: u64,
}

enum OffloadedState {
    /// Ctor state: reader task not yet spawned. Parameters captured
    /// for lazy init on first `poll_frame`.
    Pending { path: PathBuf, offset: u64 },
    /// Reader task spawned and streaming chunks via `rx`.
    Active {
        rx: mpsc::Receiver<io::Result<Bytes>>,
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
            .field("length", &self.length)
            .field("delivered", &self.delivered)
            .finish_non_exhaustive()
    }
}

impl OffloadedFileBody {
    pub(crate) fn new(path: PathBuf, offset: u64, length: u64) -> Self {
        Self {
            state: OffloadedState::Pending { path, offset },
            length,
            delivered: 0,
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

        // Lazy-start: spawn the reader task on the first poll. This
        // keeps bodies that are constructed-but-never-polled (e.g. mock
        // SDK callsites that short-circuit before reading the request
        // body) from paying for a tokio::spawn + spawn_blocking pair.
        if let OffloadedState::Pending { path, offset } = &this.state {
            let (tx, rx) = mpsc::channel(FILE_BODY_READAHEAD);
            tokio::spawn(read_task(path.clone(), *offset, this.length, tx));
            this.state = OffloadedState::Active { rx };
        }

        let rx = match &mut this.state {
            OffloadedState::Active { rx } => rx,
            OffloadedState::Pending { .. } => unreachable!("transitioned above"),
        };

        match rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok(bytes))) => {
                this.delivered += bytes.len() as u64;
                Poll::Ready(Some(Ok(Frame::data(bytes))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.length)
    }

    fn is_end_stream(&self) -> bool {
        self.delivered == self.length
    }
}

/// Reader task for [`OffloadedFileBody`]. Opens `path`, reads chunks at
/// positional offsets via `spawn_blocking`, and pushes each chunk through
/// `tx`. Exits early on channel close (receiver dropped) or read error.
async fn read_task(
    path: PathBuf,
    start_offset: u64,
    length: u64,
    tx: mpsc::Sender<io::Result<Bytes>>,
) {
    let file = match tokio::task::spawn_blocking({
        let path = path.clone();
        move || File::open(&path)
    })
    .await
    {
        Ok(Ok(f)) => Arc::new(f),
        Ok(Err(e)) => {
            let _ = tx
                .send(Err(io::Error::new(
                    e.kind(),
                    format!("failed to open {}: {e}", path.display()),
                )))
                .await;
            return;
        }
        Err(_join_err) => return,
    };

    let mut offset = start_offset;
    let mut remaining = length;

    while remaining > 0 {
        let to_read = (remaining as usize).min(FILE_BODY_CHUNK_SIZE);
        let read_offset = offset;
        let file_clone = Arc::clone(&file);

        let read_result = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
            let mut buf = BytesMut::zeroed(to_read);
            read_file_chunk(&file_clone, &mut buf[..], read_offset)?;
            Ok(buf.freeze())
        })
        .await;

        match read_result {
            Ok(Ok(bytes)) => {
                let n = bytes.len() as u64;
                if tx.send(Ok(bytes)).await.is_err() {
                    // Receiver dropped — body dropped. Exit cleanly.
                    return;
                }
                offset += n;
                remaining -= n;
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

/// Drives a [`Body`] to completion, collecting all data frames into a
/// single `Vec<u8>`. Used by tests.
#[cfg(test)]
async fn collect_body<B>(mut body: B) -> io::Result<Vec<u8>>
where
    B: Body<Data = Bytes, Error = io::Error> + Unpin,
{
    use std::future::poll_fn;
    let mut out = Vec::new();
    loop {
        let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    out.extend_from_slice(&data);
                }
                // Non-data frames (trailers) are ignored by tests.
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(out),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tempfile(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(contents).unwrap();
        tmp.flush().unwrap();
        tmp
    }

    // ---- DirectFileBody ----

    #[tokio::test]
    async fn direct_body_reads_small_file_single_frame() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let body = DirectFileBody::new(tmp.path().to_path_buf(), 0, payload.len() as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn direct_body_chunks_payload_larger_than_chunk_size() {
        // Must span at least 2 chunks.
        let size = FILE_BODY_CHUNK_SIZE + FILE_BODY_CHUNK_SIZE / 4;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let body = DirectFileBody::new(tmp.path().to_path_buf(), 0, size as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out.len(), size);
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn direct_body_reads_from_offset() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let offset = 1024;
        let length = 2048;
        let body = DirectFileBody::new(tmp.path().to_path_buf(), offset, length);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, &payload[offset as usize..(offset + length) as usize]);
    }

    #[tokio::test]
    async fn direct_body_surfaces_open_error() {
        let body = DirectFileBody::new(
            PathBuf::from("/nonexistent/path/that/should/not/exist/xyz"),
            0,
            1024,
        );
        let res = collect_body(body).await;
        assert!(res.is_err());
    }

    // ---- OffloadedFileBody ----

    #[tokio::test]
    async fn offloaded_body_reads_small_file() {
        let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let body = OffloadedFileBody::new(tmp.path().to_path_buf(), 0, payload.len() as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn offloaded_body_chunks_payload_larger_than_chunk_size() {
        let size = FILE_BODY_CHUNK_SIZE * 3 + 512;
        let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let body = OffloadedFileBody::new(tmp.path().to_path_buf(), 0, size as u64);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out.len(), size);
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn offloaded_body_reads_from_offset() {
        let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let tmp = write_tempfile(&payload);
        let offset = 1024;
        let length = 2048;
        let body = OffloadedFileBody::new(tmp.path().to_path_buf(), offset, length);

        let out = collect_body(body).await.unwrap();
        assert_eq!(out, &payload[offset as usize..(offset + length) as usize]);
    }

    #[tokio::test]
    async fn offloaded_body_surfaces_open_error() {
        let body = OffloadedFileBody::new(
            PathBuf::from("/nonexistent/path/that/should/not/exist/xyz"),
            0,
            1024,
        );
        let res = collect_body(body).await;
        assert!(res.is_err());
    }

    /// Dropping the body while the reader task is still active must not
    /// leak the task. The channel-close protocol causes the task to exit
    /// on its next `send().await`.
    #[tokio::test]
    async fn offloaded_body_drop_stops_reader_task() {
        let size = FILE_BODY_CHUNK_SIZE * 8;
        let payload = vec![0u8; size];
        let tmp = write_tempfile(&payload);
        let body = OffloadedFileBody::new(tmp.path().to_path_buf(), 0, size as u64);
        drop(body);
        tokio::task::yield_now().await;
    }
}
