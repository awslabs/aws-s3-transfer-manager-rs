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
