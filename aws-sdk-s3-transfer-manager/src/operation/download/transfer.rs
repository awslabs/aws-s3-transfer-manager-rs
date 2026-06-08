/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes_utils::SegmentedBuf;

use crate::error::{self, ChunkId, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::{BodySlot, BodyWriter, ChunkOutput};
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::DownloadState;
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::DownloadInput;
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::BucketType;

/// Download-specific work data.
#[derive(Debug)]
pub(crate) enum DownloadWork {
    Discovery,
    GetObjectRange {
        range: std::ops::RangeInclusive<u64>,
        slot: Option<BodySlot>,
        etag: Option<Arc<str>>,
    },
}

impl DownloadWork {
    /// Extract the slot from this work item, if present.
    #[cfg(test)]
    fn take_slot(&mut self) -> Option<BodySlot> {
        match self {
            DownloadWork::Discovery => None,
            DownloadWork::GetObjectRange { slot, .. } => slot.take(),
        }
    }
}

/// Early return if transfer is terminal (failed/cancelled by another work item).
macro_rules! bail_if_terminal {
    ($self:expr) => {
        if !$self.inner.ctx.is_active() {
            $self.decrement_in_flight();
            return WorkOutcome::Cancelled;
        }
    };
}

/// Download transfer that generates and executes download work.
///
/// Cheap to clone - all state is behind `Arc`.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    inner: Arc<DownloadTransferInner>,
}

/// Internal state for download transfer.
#[derive(Debug)]
struct DownloadTransferInner {
    /// Common transfer lifecycle management
    ctx: TransferContext,
    /// State machine for work progression
    state: Mutex<DownloadState>,
    /// The original request
    request: Arc<DownloadInput>,
    /// Type of S3 bucket targeted by this operation.
    // TODO(vnext): unify bucket representation (name + kind) across operations.
    #[allow(dead_code)]
    bucket_type: BucketType,
    /// Sequence window for backpressure control
    writer: BodyWriter,
    /// Object metadata from discovery (set once discovery completes)
    object_meta: std::sync::OnceLock<ObjectMetadata>,
    /// Notified when discovery completes (success or failure)
    discovery_notify: tokio::sync::Notify,
}

impl DownloadTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        bucket_type: BucketType,
        input: DownloadInput,
        writer: BodyWriter,
    ) -> Self {
        let inner = Arc::new(DownloadTransferInner {
            ctx,
            state: Mutex::new(DownloadState::new()),
            request: Arc::new(input),
            bucket_type,
            writer,
            object_meta: std::sync::OnceLock::new(),
            discovery_notify: tokio::sync::Notify::new(),
        });
        Self { inner }
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    /// Get the transfer ID.
    pub(crate) fn id(&self) -> TransferId {
        self.inner.ctx.id
    }

    /// Body writer for backpressure control and chunk delivery.
    pub(crate) fn writer(&self) -> &BodyWriter {
        &self.inner.writer
    }

    /// Object metadata from discovery.
    pub(crate) fn object_meta(&self) -> Option<&ObjectMetadata> {
        self.inner.object_meta.get()
    }

    /// Notified when discovery completes.
    pub(crate) fn discovery_notify(&self) -> &tokio::sync::Notify {
        &self.inner.discovery_notify
    }

    /// The target part size to use for this download.
    fn target_part_size_bytes(&self) -> u64 {
        self.inner.ctx.handle.download_part_size_bytes()
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - waiting for in-flight work to complete
    /// - `PollWork::Done` - transfer complete
    #[tracing::instrument(level = "debug", skip(self), fields(tid = %self.id()))]
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            tracing::debug!("not active, returning Done");
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().unwrap();

        match &mut *state {
            DownloadState::PendingDiscovery => {
                *state = DownloadState::DiscoveryInFlight;
                PollWork::Ready(IoRequest {
                    data: Some(Box::new(DownloadWork::Discovery)),
                })
            }
            DownloadState::DiscoveryInFlight => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            DownloadState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
                ..
            } => {
                // Check seq window before generating work
                let Some(slot) = self.inner.writer.try_claim() else {
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                };

                if let Some(range) = remaining.take() {
                    let part_size = self.target_part_size_bytes();
                    let start = *range.start();
                    let end = *range.end();
                    let chunk_end = cmp::min(start + part_size - 1, end);
                    let chunk_range = start..=chunk_end;

                    if chunk_end < end {
                        *remaining = Some((chunk_end + 1)..=end);
                    }

                    *ranges_in_flight += 1;

                    PollWork::Ready(IoRequest {
                        data: Some(Box::new(DownloadWork::GetObjectRange {
                            range: chunk_range,
                            slot: Some(slot),
                            etag: etag.clone(),
                        })),
                    })
                } else if *ranges_in_flight > 0 {
                    // All ranges generated, waiting for in-flight to complete
                    self.inner.ctx.set_pending();
                    PollWork::Pending
                } else {
                    // All done - success
                    self.complete(state);
                    PollWork::Done
                }
            }
            DownloadState::Terminal => PollWork::Done,
        }
    }

    #[tracing::instrument(level = "debug", skip(self, work), fields(tid = %self.id(), work = ?work.data))]
    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::Discovery => self.execute_discovery().await,
            DownloadWork::GetObjectRange { range, slot, etag } => {
                self.execute_get_range(
                    range.clone(),
                    slot.take().expect("slot already consumed"),
                    etag.clone(),
                )
                .await
            }
        }
    }

    async fn execute_discovery(&self) -> WorkOutcome {
        let input = self.inner.request.as_ref();

        let discovery = match discover_obj(self, input).await {
            Ok(d) => d,
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, e);
            }
        };

        let ObjectDiscovery {
            remaining,
            object_meta,
            initial_chunk,
            chunk_meta,
        } = discovery;

        // Store object_meta for object_meta() and join()
        let _ = self.inner.object_meta.set(object_meta.clone());
        // Notify waiters that discovery completed
        self.inner.discovery_notify.notify_waiters();

        let etag: Option<Arc<str>> = object_meta.e_tag.as_deref().map(Arc::from);

        // Optimization: Preallocate space for the full object/download size if there is a
        // destination/sink and it supports it. e.g. pre-allocate disk space for the full
        // download to avoid per-write metadata updates and late ENOSPC errors.
        let chunk_content_len = chunk_meta
            .as_ref()
            .and_then(|m| m.content_length)
            .unwrap_or(0) as u64;
        let total_size =
            chunk_content_len + remaining.as_ref().map_or(0, |r| r.end() - r.start() + 1);
        self.inner.writer.preallocate(total_size);
        self.inner.ctx.set_total_bytes(total_size);

        // If there's an initial chunk, claim seq BEFORE waking to prevent race
        // where poll_work exhausts the window before we can claim our seq.
        // Invariant: initial_chunk.is_some() == chunk_meta.is_some()
        let initial_work = match (initial_chunk, chunk_meta) {
            (Some(stream), Some(meta)) => {
                let slot = self
                    .inner
                    .writer
                    .try_claim()
                    .expect("seq window should have capacity at start");
                Some((stream, meta, slot))
            }
            (None, _) => None,
            (Some(_), None) => {
                panic!("invalid discovery state: initial_chunk present without chunk_meta")
            }
        };

        {
            let mut work = self.inner.state.lock().unwrap();
            *work = DownloadState::Transferring {
                remaining,
                ranges_in_flight: if initial_work.is_some() { 1 } else { 0 },
                etag,
            };
        }

        // State changed from DiscoveryInFlight - try to wake
        self.inner.ctx.try_wake();

        // If discovery returned an initial chunk, process it
        match initial_work {
            Some((stream, chunk_meta, slot)) => {
                self.execute_read_discovery_body(stream, slot, chunk_meta)
                    .await
            }
            None => WorkOutcome::Success { data: None },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
        slot: BodySlot,
        chunk_meta: ChunkMetadata,
    ) -> WorkOutcome {
        let seq = slot.seq();
        // Read the body from the discovery response
        let mut segmented = SegmentedBuf::new();
        let mut bytes_received: u64 = 0;
        let mut body_stream = stream;
        while let Some(result) = body_stream.next().await {
            let data = match result {
                Ok(data) => data,
                Err(e) => {
                    self.decrement_in_flight();
                    let guard = self.inner.state.lock().unwrap();
                    return self.fail(guard, error::chunk_failed(ChunkId::Download(seq), e));
                }
            };
            bytes_received += data.len() as u64;
            segmented.push(data);
            if !self.inner.ctx.is_active() {
                self.decrement_in_flight();
                return WorkOutcome::Cancelled;
            }
        }

        let chunk = ChunkOutput {
            seq,
            offset: chunk_meta
                .content_range
                .as_deref()
                .and_then(crate::http::header::parse_content_range)
                .map(|r| *r.start())
                .unwrap_or(0),
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        slot.fill(chunk);
        if let Err(e) = self.inner.writer.try_flush() {
            self.decrement_in_flight();
            let guard = self.inner.state.lock().unwrap();
            return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        self.decrement_in_flight();

        WorkOutcome::Success { data: None }
    }

    async fn execute_get_range(
        &self,
        range: std::ops::RangeInclusive<u64>,
        slot: BodySlot,
        etag: Option<Arc<str>>,
    ) -> WorkOutcome {
        let seq = slot.seq();
        let input = self.inner.request.as_ref();
        let range_header = format!("bytes={}-{}", range.start(), range.end());

        let result = self
            .inner
            .ctx
            .handle
            .telemetry
            .recv_latencies
            .guarded(|| {
                let rh = range_header.clone();
                let etag = etag.clone();
                let ctx = self.inner.ctx.clone();
                let mut req = self
                    .inner
                    .ctx
                    .s3_client()
                    .get_object()
                    .bucket(input.bucket().unwrap_or_default())
                    .key(input.key().unwrap_or_default())
                    .range(rh.clone());

                if let Some(ref etag) = etag {
                    req = req.if_match(etag.as_ref());
                }

                async move {
                    let resp = req.send().await.map_err(crate::error::Error::from)?;
                    validate_content_range(seq, &rh, resp.content_range())?;
                    let chunk_meta = ChunkMetadata::from(&resp);
                    let mut segmented = SegmentedBuf::new();
                    let mut bytes_received: u64 = 0;
                    let mut body_stream = resp.body;
                    while let Some(result) = body_stream.next().await {
                        let data = result.map_err(|e| {
                            crate::error::Error::new(crate::error::ErrorKind::IOError, e)
                        })?;
                        bytes_received += data.len() as u64;
                        segmented.push(data);
                        if !ctx.is_active() {
                            return Err(crate::error::Error::new(
                                crate::error::ErrorKind::OperationCancelled,
                                "transfer cancelled during body read",
                            ));
                        }
                    }
                    Ok::<_, crate::error::Error>((chunk_meta, segmented, bytes_received))
                }
            })
            .await;

        let (chunk_meta, segmented, bytes_received) = match result {
            Ok(val) => val,
            Err(e) => return self.fail_range(seq, e),
        };

        bail_if_terminal!(self);
        let chunk = ChunkOutput {
            seq,
            offset: *range.start(),
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        slot.fill(chunk);
        if let Err(e) = self.inner.writer.try_flush() {
            self.decrement_in_flight();
            let guard = self.inner.state.lock().unwrap();
            return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        self.decrement_in_flight();

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            seq,
            offset = *range.start(),
            "chunk downloaded",
        );

        WorkOutcome::Success { data: None }
    }

    /// Fail a range request with an error.
    fn fail_range(&self, seq: u64, e: impl Into<crate::error::BoxError>) -> WorkOutcome {
        self.decrement_in_flight();
        let guard = self.inner.state.lock().unwrap();
        self.fail(guard, error::chunk_failed(ChunkId::Download(seq), e))
    }

    fn decrement_in_flight(&self) {
        let should_wake = {
            let mut work = self.inner.state.lock().unwrap();
            if let DownloadState::Transferring {
                ranges_in_flight, ..
            } = &mut *work
            {
                *ranges_in_flight = ranges_in_flight.saturating_sub(1);
                *ranges_in_flight == 0
            } else {
                false
            }
        };
        if should_wake {
            self.inner.ctx.try_wake();
        }
    }

    /// Transition to terminal failed state. Requires holding the work lock.
    fn fail(
        &self,
        mut guard: std::sync::MutexGuard<'_, DownloadState>,
        error: Error,
    ) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            "download failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        // Order matters: set status/error before any wakeups
        self.inner.ctx.set_failed(error);
        // Transition to Terminal
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
                     // Wake all waiters
        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
        WorkOutcome::Failed { classification }
    }

    /// Transition to terminal success state. Requires holding the work lock.
    fn complete(&self, mut guard: std::sync::MutexGuard<'_, DownloadState>) {
        if let Err(e) = self.inner.writer.finalize() {
            self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            return;
        }
        self.inner.ctx.set_completed();
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
    }
}

impl Transfer for DownloadTransfer {
    fn ctx(&self) -> &TransferContext {
        DownloadTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        DownloadTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(DownloadTransfer::execute(self, work))
    }

    fn on_terminal(&self) {
        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
    }
}

/// Validate that the response Content-Range matches the requested range.
fn validate_content_range(
    seq: u64,
    requested_range: &str,
    response_content_range: Option<&str>,
) -> Result<(), Error> {
    let normalized = requested_range
        .strip_prefix("bytes=")
        .unwrap_or(requested_range);

    if response_content_range
        .map(|range| range.contains(normalized))
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(error::chunk_failed(
            ChunkId::Download(seq),
            format!(
                "content range mismatch: requested {}, response {:?}",
                requested_range, response_content_range
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::DownloadInput;
    use crate::scheduler::test_util::{assert_done, assert_pending, assert_ready};
    use crate::transfer::TransferContext;
    use crate::transfer::{IoRequest, WorkOutcome};
    use crate::types::BucketType;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    const MB: u64 = 1024 * 1024;

    fn create_download(object_size: u64, part_size: u64) -> DownloadTransfer {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        create_download_with_client(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]),
            part_size,
        )
    }

    fn create_download_with_client(client: aws_sdk_s3::Client, part_size: u64) -> DownloadTransfer {
        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_slot_body(
            crate::operation::download::body::DEFAULT_BODY_SLOT_CAPACITY,
        );
        let (ctx, _completion_rx) = TransferContext::new(handle);

        DownloadTransfer::new(ctx, BucketType::Standard, input, writer)
    }

    /// Execute work using DownloadTransfer directly.
    async fn execute(transfer: &DownloadTransfer, work: &mut IoRequest) -> WorkOutcome {
        transfer.execute(work).await
    }

    /// Run discovery to completion
    async fn skip_discovery(transfer: &DownloadTransfer) {
        let mut work = assert_ready(transfer.poll_work());
        execute(transfer, &mut work).await;
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_initial_poll_returns_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::Discovery));
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_pending_while_discovery_in_flight() {
        let transfer = create_download(24 * MB, 8 * MB);
        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_generates_ranges_after_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::GetObjectRange { .. }));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_one_with_initial_chunk() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    1,
                    "seq should start at 1 when initial chunk claims seq=0"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_zero_without_initial_chunk() {
        let head_obj = mock!(aws_sdk_s3::Client::head_object).then_output(|| {
            aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
                .content_length(24 * MB as i64)
                .e_tag("test-etag")
                .build()
        });
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(8 * MB as i64)
                .content_range(format!("bytes 0-{}/{}", 8 * MB - 1, 24 * MB))
                .e_tag("test-etag")
                .body(ByteStream::from(vec![0u8; 8 * MB as usize]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[head_obj, get_obj]);

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(8 * MB))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=0-")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_slot_body(
            crate::operation::download::body::DEFAULT_BODY_SLOT_CAPACITY,
        );
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);

        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    0,
                    "seq should start at 0 when no initial chunk"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_increments_sequentially() {
        let transfer = create_download(32 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut seqs = Vec::new();
        while let PollWork::Ready(mut w) = transfer.poll_work() {
            let data = w.data_mut::<DownloadWork>();
            if let DownloadWork::GetObjectRange { slot, .. } = data {
                seqs.push(slot.as_ref().unwrap().seq());
            }
        }

        assert_eq!(seqs.len(), 3, "expected multiple ranges");
        for i in 1..seqs.len() {
            assert_eq!(seqs[i], seqs[i - 1] + 1, "seqs should be sequential");
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_pending_when_range_in_flight() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        // generate range work but don't complete
        let _range = assert_ready(transfer.poll_work());
        // transfer is considered active and shouldn't transition to done until all in-flight work is complete and handle is joined/dropped
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_done_when_all_complete() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        execute(&transfer, &mut range).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_out_of_order_completion() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range1 = assert_ready(transfer.poll_work()); // seq=1
        let mut range2 = assert_ready(transfer.poll_work()); // seq=2

        // Complete in reverse order
        execute(&transfer, &mut range2).await;
        execute(&transfer, &mut range1).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_failure_transitions_to_failed() {
        let _logs = show_test_logs();
        // Fail seq 1 (first range after discovery)
        let transfer = FailureConfig::new(24 * MB, 8 * MB).fail(1).build();

        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut range).await;

        assert!(matches!(outcome, WorkOutcome::Failed { .. }));
        assert!(transfer.ctx().is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation_transitions_to_cancelled() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        transfer.ctx().set_cancelled();
        transfer.ctx().signal_terminal();

        assert!(transfer.ctx().is_cancelled());
        assert_done(transfer.poll_work());
    }

    fn create_download_with_capacity(
        object_size: u64,
        part_size: u64,
        capacity: usize,
    ) -> (
        DownloadTransfer,
        crate::operation::download::body::SlotBodyConsumer,
    ) {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]);

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, consumer) = crate::operation::download::body::new_slot_body(capacity);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);
        (transfer, consumer)
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_window_limits_work_generation() {
        // Create download with many parts but small slot buffer capacity
        let (transfer, _consumer) = create_download_with_capacity(128 * MB, 8 * MB, 3);

        skip_discovery(&transfer).await;
        // After discovery: claimed=1 (initial chunk took seq=0), consumed=0
        // Capacity=3 means: claimed < consumed + capacity → claimed < 3
        // So we can claim seq 1, 2 (claimed becomes 2, then 3)

        let _w1 = assert_ready(transfer.poll_work()); // seq=1, claimed=2
        let _w2 = assert_ready(transfer.poll_work()); // seq=2, claimed=3

        // Window exhausted (claimed=3, consumed=0, capacity=3: 3 >= 0+3)
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_window_consume_enables_more_work() {
        let (transfer, consumer) = create_download_with_capacity(128 * MB, 8 * MB, 2);

        skip_discovery(&transfer).await;
        // After discovery: seq 0 claimed and filled by discovery, consumed=0
        // Capacity=2 means: claimed < consumed + 2

        let mut w1 = assert_ready(transfer.poll_work()); // seq=1, claimed=2
        assert_pending(transfer.poll_work()); // claimed=2 >= 0+2

        // Consume seq 0 (filled by discovery) to open the window
        consumer.try_take_next(); // consume seq 0, consumed=1

        let mut w2 = assert_ready(transfer.poll_work()); // seq=2, claimed=3
        assert_pending(transfer.poll_work()); // claimed=3 >= 1+2

        // Fill seq 1 from its work item, then consume it
        let slot1 = w1.data_mut::<DownloadWork>().take_slot().expect("has slot");
        let mut seg = bytes_utils::SegmentedBuf::new();
        seg.push(bytes::Bytes::from("data"));
        slot1.fill(ChunkOutput {
            seq: 1,
            offset: 0,
            data: crate::io::AggregatedBytes(seg),
            metadata: Default::default(),
        });
        consumer.try_take_next(); // consume seq 1, consumed=2

        let w3 = assert_ready(transfer.poll_work()); // seq=3, claimed=4
        assert_pending(transfer.poll_work());

        // Fill seq 2 from its work item, then consume it
        let slot2 = w2.data_mut::<DownloadWork>().take_slot().expect("has slot");
        let mut seg = bytes_utils::SegmentedBuf::new();
        seg.push(bytes::Bytes::from("data"));
        slot2.fill(ChunkOutput {
            seq: 2,
            offset: 0,
            data: crate::io::AggregatedBytes(seg),
            metadata: Default::default(),
        });
        consumer.try_take_next(); // consume seq 2

        let _w4 = assert_ready(transfer.poll_work()); // seq=4
        assert_pending(transfer.poll_work());

        // Clean up remaining slots
        drop(w3);
    }

    use crate::http::header::Range;
    use aws_smithy_mocks::MockResponse;
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use std::collections::HashMap;

    /// Failure behavior for a specific seq
    #[derive(Clone)]
    enum FailureBehavior {
        /// Always fail
        Always,
        /// Fail first N attempts, then succeed
        #[allow(dead_code)] // TODO: re-enable with retry tests
        Times(usize),
    }

    /// Builder for downloads with configurable failure injection
    struct FailureConfig {
        object_size: u64,
        part_size: u64,
        failures: HashMap<u64, FailureBehavior>,
    }

    impl FailureConfig {
        fn new(object_size: u64, part_size: u64) -> Self {
            Self {
                object_size,
                part_size,
                failures: HashMap::new(),
            }
        }

        /// Fail this seq always (retryable 500 error)
        fn fail(mut self, seq: u64) -> Self {
            self.failures.insert(seq, FailureBehavior::Always);
            self
        }

        fn build(self) -> DownloadTransfer {
            let object_size = self.object_size;
            let part_size = self.part_size;
            let failures = Arc::new(self.failures);
            let call_counts: Arc<std::sync::Mutex<HashMap<u64, usize>>> =
                Arc::new(std::sync::Mutex::new(HashMap::new()));

            let get_obj = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
                let seq = req
                    .range()
                    .and_then(|r| r.parse::<Range>().ok())
                    .map(|r| match r.0 {
                        crate::http::header::ByteRange::Inclusive(start, _) => start / part_size,
                        crate::http::header::ByteRange::AllFrom(start) => start / part_size,
                        _ => 0,
                    })
                    .unwrap_or(0);

                // Track calls per seq
                let mut counts = call_counts.lock().unwrap();
                let call_num = *counts.entry(seq).and_modify(|c| *c += 1).or_insert(1);
                drop(counts);

                // Check failure config
                let should_fail = match failures.get(&seq) {
                    Some(FailureBehavior::Always) => true,
                    Some(FailureBehavior::Times(n)) => call_num <= *n,
                    None => false,
                };

                if should_fail {
                    MockResponse::Http(Response::new(
                        StatusCode::try_from(500).unwrap(),
                        SdkBody::from("internal error"),
                    ))
                } else {
                    let start = seq * part_size;
                    let end = std::cmp::min(start + part_size, object_size) - 1;
                    let chunk = vec![0u8; (end - start + 1) as usize];

                    MockResponse::Output(
                        GetObjectOutput::builder()
                            .content_length((end - start + 1) as i64)
                            .content_range(format!("bytes {}-{}/{}", start, end, object_size))
                            .e_tag("test-etag")
                            .body(ByteStream::from(chunk))
                            .build(),
                    )
                }
            });

            create_download_with_client(
                mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj], |conf| {
                    conf.retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                }),
                part_size,
            )
        }
    }

    #[test]
    fn test_validate_content_range_success() {
        assert!(validate_content_range(0, "bytes=1024-2047", Some("bytes 1024-2047/4096")).is_ok());
        assert!(validate_content_range(0, "1024-2047", Some("bytes 1024-2047/4096")).is_ok());
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        assert!(
            validate_content_range(0, "bytes=1024-2047", Some("bytes 2048-3071/4096")).is_err()
        );
    }

    #[test]
    fn test_validate_content_range_missing() {
        assert!(validate_content_range(0, "bytes=1024-2047", None).is_err());
    }
}
