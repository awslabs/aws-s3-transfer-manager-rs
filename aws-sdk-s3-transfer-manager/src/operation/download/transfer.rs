/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::sync::Arc;

use bytes_utils::SegmentedBuf;
use tokio_util::sync::CancellationToken;

use crate::error::{self, ChunkId, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::ChunkOutput;
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::{DownloadContext, DownloadWorkState};
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::ChunkSender;
use crate::scheduler::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};

/// Download transfer that generates and executes download work.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    ctx: DownloadContext,
    cancellation_token: CancellationToken,
}

impl DownloadTransfer {
    pub(crate) fn new(ctx: DownloadContext) -> Self {
        Self {
            ctx,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub(crate) fn id(&self) -> TransferId {
        self.ctx.id
    }

    pub(crate) fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    #[tracing::instrument(level = "debug", skip(self), fields(tid = %self.id()))]
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.ctx.is_active() {
            tracing::debug!("not active, returning Done");
            return PollWork::Done;
        }

        let mut work = self.ctx.state.work.lock().unwrap();

        match &mut *work {
            DownloadWorkState::PendingDiscovery { chunk_tx } => {
                let chunk_tx_clone = chunk_tx.clone();
                *work = DownloadWorkState::DiscoveryInFlight {
                    chunk_tx: chunk_tx.clone(),
                };
                PollWork::Ready(WorkItem {
                    transfer_id: self.id(),
                    kind: WorkKind::Network,
                    data: WorkData::Discovery {
                        chunk_tx: chunk_tx_clone,
                    },
                })
            }
            DownloadWorkState::DiscoveryInFlight { .. } => PollWork::Pending,
            DownloadWorkState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
                chunk_tx,
                ..
            } => {
                if let Some(range) = remaining.take() {
                    let part_size = self.ctx.target_part_size_bytes();
                    let start = *range.start();
                    let end = *range.end();
                    let chunk_end = cmp::min(start + part_size - 1, end);
                    let chunk_range = start..=chunk_end;

                    if chunk_end < end {
                        *remaining = Some((chunk_end + 1)..=end);
                    }

                    let seq = self.ctx.next_seq();
                    *ranges_in_flight += 1;

                    PollWork::Ready(WorkItem {
                        transfer_id: self.id(),
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
                    PollWork::Pending
                } else {
                    // All done - success
                    self.complete(work);
                    PollWork::Done
                }
            }
            DownloadWorkState::Terminal => PollWork::Done,
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
        let input = self.ctx.state.request();

        let discovery = match discover_obj(&self.ctx, input).await {
            Ok(d) => d,
            Err(e) => {
                let guard = self.ctx.state.work.lock().unwrap();
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
        let _ = self.ctx.state.object_meta.set(object_meta.clone());
        // Notify waiters that discovery completed
        self.ctx.state.discovery_notify.notify_waiters();

        let etag: Option<Arc<str>> = object_meta.e_tag.as_deref().map(Arc::from);

        // If there's an initial chunk, we need to account for it in ranges_in_flight
        let has_initial_chunk = initial_chunk.is_some();

        {
            let mut work = self.ctx.state.work.lock().unwrap();
            *work = DownloadWorkState::Transferring {
                remaining,
                ranges_in_flight: if has_initial_chunk { 1 } else { 0 },
                etag,
                object_meta,
                chunk_tx: chunk_tx.clone(),
            };
        }

        // If discovery returned an initial chunk, schedule work to read it
        match (initial_chunk, chunk_meta) {
            (Some(stream), Some(chunk_meta)) => {
                let seq = self.ctx.next_seq(); // Claim seq for initial chunk
                WorkOutcome::Success {
                    schedule_next: Some(WorkKind::Network),
                    data: WorkData::ReadDiscoveryBody {
                        stream,
                        seq,
                        chunk_meta,
                        chunk_tx,
                    },
                }
            }
            _ => WorkOutcome::Success {
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
                let guard = self.ctx.state.work.lock().unwrap();
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
        let input = self.ctx.state.request();
        let range_header = format!("bytes={}-{}", range.start(), range.end());

        let mut req = self
            .ctx
            .client()
            .get_object()
            .bucket(input.bucket().unwrap_or_default())
            .key(input.key().unwrap_or_default())
            .range(range_header);

        if let Some(ref etag) = etag {
            req = req.if_match(etag.as_ref());
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                self.decrement_in_flight();
                let guard = self.ctx.state.work.lock().unwrap();
                return self.fail(guard, error::chunk_failed(ChunkId::Download(seq), e));
            }
        };

        // Extract metadata before consuming body
        let chunk_meta = ChunkMetadata::from(&resp);

        // TODO(redux): Handle ByteStreamError with retry (SDK doesn't retry these)
        let body = match resp.body.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => {
                self.decrement_in_flight();
                let guard = self.ctx.state.work.lock().unwrap();
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

    fn decrement_in_flight(&self) {
        let mut work = self.ctx.state.work.lock().unwrap();
        if let DownloadWorkState::Transferring {
            ranges_in_flight, ..
        } = &mut *work
        {
            *ranges_in_flight = ranges_in_flight.saturating_sub(1);
        }
    }

    /// Transition to terminal failed state. Requires holding the work lock.
    fn fail(
        &self,
        mut guard: std::sync::MutexGuard<'_, DownloadWorkState>,
        error: Error,
    ) -> WorkOutcome {
        // Order matters: set status/error before any wakeups
        self.ctx.set_failed(error);
        // Transition to Terminal - releases chunk_tx
        *guard = DownloadWorkState::Terminal;
        drop(guard); // release lock before signaling waiters
                     // Wake all waiters
        self.ctx.state.discovery_notify.notify_waiters();
        self.ctx.signal_terminal();
        WorkOutcome::Failed
    }

    /// Transition to terminal success state. Requires holding the work lock.
    fn complete(&self, mut guard: std::sync::MutexGuard<'_, DownloadWorkState>) {
        self.ctx.set_completed();
        *guard = DownloadWorkState::Terminal;
        drop(guard); // release lock before signaling waiters
        self.ctx.signal_terminal();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::DownloadInput;
    use crate::scheduler::{PollWork, WorkData, WorkItem};
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
            new_scheduler: crate::scheduler::Scheduler::new(
                DEFAULT_CONCURRENCY,
                DEFAULT_CONCURRENCY,
            ),
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
        let (ctx, _completion_rx) =
            DownloadContext::new(id, handle, BucketType::Standard, input, chunk_tx);

        DownloadTransfer::new(ctx)
    }

    fn assert_ready(poll: PollWork) -> WorkItem {
        match poll {
            PollWork::Ready(w) => w,
            PollWork::Pending => panic!("expected Ready, got Pending"),
            PollWork::Done => panic!("expected Ready, got Done"),
        }
    }

    fn assert_pending(poll: PollWork) {
        assert!(matches!(poll, PollWork::Pending), "expected Pending");
    }

    fn assert_done(poll: PollWork) {
        assert!(matches!(poll, PollWork::Done), "expected Done");
    }

    /// Execute and handle follow-on work (e.g., ReadDiscoveryBody)
    async fn execute(transfer: &DownloadTransfer, work: &mut WorkItem) -> WorkOutcome {
        let outcome = transfer.execute(work).await;
        if let WorkOutcome::Success {
            schedule_next: Some(kind),
            data,
        } = outcome
        {
            let mut follow_on = WorkItem {
                transfer_id: transfer.id(),
                kind,
                data,
            };
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
        assert!(transfer.ctx.is_failed());
    }

    #[tokio::test]
    #[ignore = "needs cancellation support"]
    async fn test_cancellation_transitions_to_cancelled() {
        todo!()
    }

    // =========================================================================
    // Failure injection
    // =========================================================================

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
