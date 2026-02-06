/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::sync::{Arc, Mutex};

use bytes_utils::SegmentedBuf;

use crate::error::{self, ChunkId, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::ChunkOutput;
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::{DownloadState, SeqWindow};
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::DownloadInput;
use crate::operation::{ChunkSender, TransferContext};
use crate::scheduler::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};
use crate::types::BucketType;

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
    /// Type of S3 bucket targeted by this operation
    bucket_type: BucketType,
    /// Sequence window for backpressure control
    seq_window: SeqWindow,
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
        chunk_tx: ChunkSender,
    ) -> Self {
        let inner = Arc::new(DownloadTransferInner {
            ctx,
            state: Mutex::new(DownloadState::new(chunk_tx)),
            request: Arc::new(input),
            bucket_type,
            seq_window: SeqWindow::default(),
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

    /// The original request.
    pub(crate) fn request(&self) -> &DownloadInput {
        &self.inner.request
    }

    /// Type of S3 bucket targeted by this operation.
    pub(crate) fn bucket_type(&self) -> BucketType {
        self.inner.bucket_type
    }

    /// Sequence window for backpressure control.
    pub(crate) fn seq_window(&self) -> &SeqWindow {
        &self.inner.seq_window
    }

    /// Object metadata from discovery.
    pub(crate) fn object_meta(&self) -> Option<&ObjectMetadata> {
        self.inner.object_meta.get()
    }

    /// Notified when discovery completes.
    pub(crate) fn discovery_notify(&self) -> &tokio::sync::Notify {
        &self.inner.discovery_notify
    }

    /// Get the cancellation token for this transfer.
    pub(crate) fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        self.inner.ctx.cancellation_token()
    }

    /// The target part size to use for this download.
    fn target_part_size_bytes(&self) -> u64 {
        self.inner.ctx.handle.download_part_size_bytes()
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - blocked waiting for in-flight work
    /// - `PollWork::Done` - transfer complete
    #[tracing::instrument(level = "debug", skip(self), fields(tid = %self.id()))]
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            tracing::debug!("not active, returning Done");
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().unwrap();

        match &mut *state {
            DownloadState::PendingDiscovery { chunk_tx } => {
                let chunk_tx_clone = chunk_tx.clone();
                *state = DownloadState::DiscoveryInFlight {
                    chunk_tx: chunk_tx.clone(),
                };
                PollWork::Ready(WorkItem {
                    kind: WorkKind::Network,
                    data: WorkData::Discovery {
                        chunk_tx: chunk_tx_clone,
                    },
                })
            }
            DownloadState::DiscoveryInFlight { .. } => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            DownloadState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
                chunk_tx,
                ..
            } => {
                // Check seq window before generating work
                let Some(seq) = self.inner.seq_window.try_claim() else {
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

                    PollWork::Ready(WorkItem {
                        kind: WorkKind::Network,
                        data: WorkData::GetObjectRange {
                            range: chunk_range,
                            seq,
                            etag: etag.clone(),
                            chunk_tx: chunk_tx.clone(),
                        },
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

    #[tracing::instrument(level = "debug", skip(self, work), fields(tid = %self.id(), work = %work.data.debug_label()))]
    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match &mut work.data {
            WorkData::Discovery { chunk_tx } => self.execute_discovery(chunk_tx.clone()).await,
            WorkData::ReadDiscoveryBody {
                stream,
                seq,
                chunk_meta,
                chunk_tx,
            } => {
                self.execute_read_discovery_body(
                    std::mem::take(stream),
                    *seq,
                    std::mem::take(chunk_meta),
                    chunk_tx.clone(),
                )
                .await
            }
            WorkData::GetObjectRange {
                range,
                seq,
                etag,
                chunk_tx,
            } => {
                self.execute_get_range(range.clone(), *seq, etag.clone(), chunk_tx.clone())
                    .await
            }
            _ => unreachable!("download transfer received unexpected work data"),
        }
    }

    async fn execute_discovery(&self, chunk_tx: ChunkSender) -> WorkOutcome {
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

        // If there's an initial chunk, claim seq BEFORE waking to prevent race
        // where poll_work exhausts the window before we can claim our seq.
        // Invariant: initial_chunk.is_some() == chunk_meta.is_some()
        let initial_work = match (initial_chunk, chunk_meta) {
            (Some(stream), Some(meta)) => {
                let seq = self
                    .inner
                    .seq_window
                    .try_claim()
                    .expect("seq window should have capacity at start");
                Some((stream, meta, seq))
            }
            (None, None) => None,
            _ => panic!(
                "invalid discovery state: initial_chunk and chunk_meta must both be Some or None"
            ),
        };

        {
            let mut work = self.inner.state.lock().unwrap();
            *work = DownloadState::Transferring {
                remaining,
                ranges_in_flight: if initial_work.is_some() { 1 } else { 0 },
                etag,
                object_meta,
                chunk_tx: chunk_tx.clone(),
            };
        }

        // State changed from DiscoveryInFlight - try to wake
        self.inner.ctx.try_wake();

        // If discovery returned an initial chunk, schedule work to read it
        match initial_work {
            Some((stream, chunk_meta, seq)) => WorkOutcome::Success {
                schedule_next: Some(WorkKind::Network),
                data: WorkData::ReadDiscoveryBody {
                    stream,
                    seq,
                    chunk_meta,
                    chunk_tx,
                },
            },
            None => WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::Discovery { chunk_tx },
            },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
        seq: u64,
        chunk_meta: ChunkMetadata,
        chunk_tx: ChunkSender,
    ) -> WorkOutcome {
        // Read the body from the discovery response
        let body = match stream.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => {
                self.decrement_in_flight();
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, error::chunk_failed(ChunkId::Download(seq), e));
            }
        };

        let mut segmented = SegmentedBuf::new();
        segmented.push(body);
        let chunk = ChunkOutput {
            seq,
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        let send_result = chunk_tx.send(Ok(chunk)).await;
        self.decrement_in_flight();

        match send_result {
            Ok(()) => WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::Discovery {
                    chunk_tx: chunk_tx.clone(),
                },
            },
            Err(_) => WorkOutcome::Cancelled,
        }
    }

    async fn execute_get_range(
        &self,
        range: std::ops::RangeInclusive<u64>,
        seq: u64,
        etag: Option<Arc<str>>,
        chunk_tx: ChunkSender,
    ) -> WorkOutcome {
        let input = self.inner.request.as_ref();
        let range_header = format!("bytes={}-{}", range.start(), range.end());

        let mut req = self
            .inner
            .ctx
            .s3_client()
            .get_object()
            .bucket(input.bucket().unwrap_or_default())
            .key(input.key().unwrap_or_default())
            .range(range_header);

        if let Some(ref etag) = etag {
            req = req.if_match(etag.as_ref());
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return self.fail_range(seq, e),
        };

        bail_if_terminal!(self);

        // Extract metadata before consuming body
        let chunk_meta = ChunkMetadata::from(&resp);

        // TODO(redux): Handle ByteStreamError with retry (SDK doesn't retry these)
        let body = match resp.body.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => return self.fail_range(seq, e),
        };

        bail_if_terminal!(self);

        let mut segmented = SegmentedBuf::new();
        segmented.push(body);
        let chunk = ChunkOutput {
            seq,
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        let send_result = chunk_tx.send(Ok(chunk)).await;
        self.decrement_in_flight();

        match send_result {
            Ok(()) => WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::GetObjectRange {
                    range,
                    seq,
                    etag,
                    chunk_tx,
                },
            },
            Err(_) => WorkOutcome::Cancelled,
        }
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
        // Order matters: set status/error before any wakeups
        self.inner.ctx.set_failed(error);
        // Transition to Terminal - releases chunk_tx
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
                     // Wake all waiters
        self.inner.discovery_notify.notify_waiters();
        self.inner.ctx.signal_terminal();
        WorkOutcome::Failed
    }

    /// Transition to terminal success state. Requires holding the work lock.
    fn complete(&self, mut guard: std::sync::MutexGuard<'_, DownloadState>) {
        self.inner.ctx.set_completed();
        *guard = DownloadState::Terminal;
        drop(guard); // release lock before signaling waiters
        self.inner.ctx.signal_terminal();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::DownloadInput;
    use crate::operation::TransferContext;
    use crate::scheduler::test_util::{assert_done, assert_pending, assert_ready};
    use crate::scheduler::{TransferId, WorkData, WorkItem, WorkOutcome};
    use crate::types::BucketType;
    use crate::DEFAULT_CONCURRENCY;
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

        let handle = Arc::new(crate::client::Handle {
            config,
            scheduler: crate::runtime::scheduler::Scheduler::new(
                crate::types::ConcurrencyMode::Explicit(8),
            ),
            new_scheduler: crate::scheduler::Scheduler::new(DEFAULT_CONCURRENCY),
        });

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let (chunk_tx, _chunk_rx) = tokio::sync::mpsc::channel(8);
        let (ctx, _completion_rx) = TransferContext::new(id, handle);

        DownloadTransfer::new(ctx, BucketType::Standard, input, chunk_tx)
    }

    /// Execute and handle follow-on work (e.g., ReadDiscoveryBody).
    /// Uses DownloadTransfer directly for type-specific behavior.
    async fn execute(transfer: &DownloadTransfer, work: &mut WorkItem) -> WorkOutcome {
        let outcome = transfer.execute(work).await;
        if let WorkOutcome::Success {
            schedule_next: Some(kind),
            data,
        } = outcome
        {
            let mut follow_on = WorkItem { kind, data };
            return transfer.execute(&mut follow_on).await;
        }
        outcome
    }

    /// Run discovery to completion
    async fn skip_discovery(transfer: &DownloadTransfer) {
        let mut work = assert_ready(transfer.poll_work());
        execute(transfer, &mut work).await;
    }

    #[test]
    fn test_initial_poll_returns_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        let work = assert_ready(transfer.poll_work());
        assert!(matches!(work.data, WorkData::Discovery { .. }));
    }

    #[test]
    fn test_pending_while_discovery_in_flight() {
        let transfer = create_download(24 * MB, 8 * MB);
        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[tokio::test]
    async fn test_generates_ranges_after_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let work = assert_ready(transfer.poll_work());
        assert!(matches!(work.data, WorkData::GetObjectRange { .. }));
    }

    #[tokio::test]
    async fn test_seq_starts_at_one_with_initial_chunk() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let work = assert_ready(transfer.poll_work());
        match work.data {
            WorkData::GetObjectRange { seq, .. } => {
                assert_eq!(
                    seq, 1,
                    "seq should start at 1 when initial chunk claims seq=0"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[tokio::test]
    #[ignore = "needs HeadObject mock path"]
    async fn test_seq_starts_at_zero_without_initial_chunk() {
        // TODO: create_download with HeadObject mock (no initial chunk)
        todo!()
    }

    #[tokio::test]
    async fn test_seq_increments_sequentially() {
        let transfer = create_download(32 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut seqs = Vec::new();
        loop {
            match transfer.poll_work() {
                PollWork::Ready(w) => {
                    if let WorkData::GetObjectRange { seq, .. } = w.data {
                        seqs.push(seq);
                    }
                }
                _ => break,
            }
        }

        assert_eq!(seqs.len(), 3, "expected multiple ranges");
        for i in 1..seqs.len() {
            assert_eq!(seqs[i], seqs[i - 1] + 1, "seqs should be sequential");
        }
    }

    #[tokio::test]
    async fn test_pending_when_range_in_flight() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        // generate range work but don't complete
        let _range = assert_ready(transfer.poll_work());
        // transfer is considered active and shouldn't transition to done until all in-flight work is complete and handle is joined/dropped
        assert_pending(transfer.poll_work());
    }

    #[tokio::test]
    async fn test_done_when_all_complete() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        execute(&transfer, &mut range).await;

        assert_done(transfer.poll_work());
    }

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

    #[tokio::test]
    async fn test_failure_transitions_to_failed() {
        let _logs = show_test_logs();
        // Fail seq 1 (first range after discovery)
        let transfer = FailureConfig::new(24 * MB, 8 * MB).fail(1).build();

        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut range).await;

        assert!(matches!(outcome, WorkOutcome::Failed));
        assert!(transfer.ctx().is_failed());
    }

    #[tokio::test]
    #[ignore = "needs cancellation support"]
    async fn test_cancellation_transitions_to_cancelled() {
        todo!()
    }

    #[tokio::test]
    async fn test_seq_window_limits_work_generation() {
        // Create download with many parts but small seq window
        let transfer = create_download(128 * MB, 8 * MB); // 16 parts
        transfer.seq_window().set_max_gap(3);

        skip_discovery(&transfer).await;
        // After discovery: claimed=1 (initial chunk took seq=0), consumed=0
        // Gap=3 means: claimed < consumed + gap → claimed < 3
        // So we can claim seq 1, 2 (claimed becomes 2, then 3)

        let _w1 = assert_ready(transfer.poll_work()); // seq=1, claimed=2
        let _w2 = assert_ready(transfer.poll_work()); // seq=2, claimed=3

        // Gap exhausted (claimed=3, consumed=0, gap=3: 3 >= 0+3)
        assert_pending(transfer.poll_work());
    }

    #[tokio::test]
    async fn test_seq_window_consume_enables_more_work() {
        let transfer = create_download(128 * MB, 8 * MB);
        transfer.seq_window().set_max_gap(2);

        skip_discovery(&transfer).await;
        // After discovery: claimed=1, consumed=0
        // Gap=2 means: claimed < 2, so we can only claim seq=1

        let _w1 = assert_ready(transfer.poll_work()); // seq=1, claimed=2
        assert_pending(transfer.poll_work()); // claimed=2 >= 0+2

        // Simulate consumer reading seq 0
        transfer.seq_window().consume(0);
        // Now consumed=1, so claimed < 1+2=3

        let _w2 = assert_ready(transfer.poll_work()); // seq=2, claimed=3
        assert_pending(transfer.poll_work()); // claimed=3 >= 1+2

        // Consume seq 1
        transfer.seq_window().consume(1);
        // Now consumed=2, so claimed < 2+2=4

        let _w3 = assert_ready(transfer.poll_work()); // seq=3, claimed=4
        assert_pending(transfer.poll_work());
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

        /// Fail this seq N times, then succeed (for retry testing)
        fn fail_times(mut self, seq: u64, times: usize) -> Self {
            self.failures.insert(seq, FailureBehavior::Times(times));
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
}
