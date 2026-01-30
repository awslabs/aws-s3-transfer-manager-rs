/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes_utils::SegmentedBuf;
use tokio::sync::oneshot;

use crate::error::{self, ChunkId, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::ChunkOutput;
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::{DownloadContext, DownloadWorkState};
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::scheduler::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};

use tokio_util::sync::CancellationToken;

/// Sender to signal that the download state machine has completed (success, failure, or cancel)
pub(crate) type CompletionSender = oneshot::Sender<()>;
/// Receiver to wait for the download state machine to complete
pub(crate) type CompletionReceiver = oneshot::Receiver<()>;

/// Download transfer that generates and executes download work.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    ctx: DownloadContext,
    done: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
    completion_tx: Arc<Mutex<Option<CompletionSender>>>,
}

impl DownloadTransfer {
    pub(crate) fn new(ctx: DownloadContext, completion_tx: CompletionSender) -> Self {
        Self {
            ctx,
            done: Arc::new(AtomicBool::new(false)),
            cancellation_token: CancellationToken::new(),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
        }
    }

    pub(crate) fn id(&self) -> TransferId {
        self.ctx.id
    }

    pub(crate) fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        if self.done.load(Ordering::Acquire) {
            return PollWork::Done;
        }

        let mut work = self.ctx.state.work.lock().unwrap();

        match &mut *work {
            DownloadWorkState::PendingDiscovery { chunk_tx } => {
                let chunk_tx = chunk_tx.clone();
                *work = DownloadWorkState::DiscoveryInFlight { chunk_tx };
                PollWork::Ready(WorkItem {
                    transfer_id: self.id(),
                    kind: WorkKind::Network,
                    data: WorkData::Discovery,
                })
            }
            DownloadWorkState::DiscoveryInFlight { .. } => PollWork::Pending,
            DownloadWorkState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
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
                        },
                    })
                } else if *ranges_in_flight > 0 {
                    // All ranges generated, waiting for in-flight to complete
                    PollWork::Pending
                } else {
                    // All done
                    self.done.store(true, Ordering::Release);
                    *work = DownloadWorkState::Done;
                    PollWork::Done
                }
            }
            DownloadWorkState::Done => PollWork::Done,
        }
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match &mut work.data {
            WorkData::Discovery => self.execute_discovery().await,
            WorkData::ReadDiscoveryBody {
                stream,
                seq,
                chunk_meta,
            } => {
                self.execute_read_discovery_body(std::mem::take(stream), *seq, std::mem::take(chunk_meta))
                    .await
            }
            WorkData::GetObjectRange { range, seq, etag } => {
                self.execute_get_range(range.clone(), *seq, etag.clone())
                    .await
            }
            _ => unreachable!("download transfer received unexpected work data"),
        }
    }

    async fn execute_discovery(&self) -> WorkOutcome {
        let input = self.ctx.state.request();

        let discovery = match discover_obj(&self.ctx, input).await {
            Ok(d) => d,
            Err(e) => return self.fail(e),
        };

        let ObjectDiscovery {
            remaining,
            object_meta,
            initial_chunk,
            chunk_meta,
        } = discovery;

        let etag: Option<Arc<str>> = object_meta.e_tag.as_deref().map(Arc::from);

        // If there's an initial chunk, we need to account for it in ranges_in_flight
        let has_initial_chunk = initial_chunk.is_some();

        {
            let mut work = self.ctx.state.work.lock().unwrap();
            let chunk_tx = match std::mem::replace(&mut *work, DownloadWorkState::Done) {
                DownloadWorkState::DiscoveryInFlight { chunk_tx } => chunk_tx,
                _ => panic!("unexpected state in execute_discovery"),
            };
            *work = DownloadWorkState::Transferring {
                remaining,
                ranges_in_flight: if has_initial_chunk { 1 } else { 0 },
                etag,
                object_meta,
                chunk_tx,
            };
        }

        // If discovery returned an initial chunk, schedule work to read it
        match (initial_chunk, chunk_meta) {
            (Some(stream), Some(chunk_meta)) => WorkOutcome::Success {
                schedule_next: Some(WorkKind::Network),
                data: WorkData::ReadDiscoveryBody {
                    stream,
                    seq: 0,
                    chunk_meta,
                },
            },
            _ => WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::Discovery,
            },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
        seq: u64,
        chunk_meta: ChunkMetadata,
    ) -> WorkOutcome {
        // Read the body from the discovery response
        let body = match stream.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => {
                self.decrement_in_flight();
                return self.fail(error::chunk_failed(ChunkId::Download(seq), e));
            }
        };

        let mut segmented = SegmentedBuf::new();
        segmented.push(body);
        let chunk = ChunkOutput {
            seq,
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        {
            let work = self.ctx.state.work.lock().unwrap();
            let chunk_tx = match &*work {
                DownloadWorkState::Transferring { chunk_tx, .. } => chunk_tx,
                _ => {
                    return self.fail(error::from_kind(error::ErrorKind::RuntimeError)(
                        "unexpected state",
                    ))
                }
            };
            let _ = chunk_tx.try_send(Ok(chunk));
        }

        self.decrement_in_flight();

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::Discovery, // No follow-on work
        }
    }

    async fn execute_get_range(
        &self,
        range: RangeInclusive<u64>,
        seq: u64,
        etag: Option<Arc<str>>,
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
                return self.fail(error::chunk_failed(ChunkId::Download(seq), e));
            }
        };

        // Extract metadata before consuming body
        let chunk_meta = ChunkMetadata::from(&resp);

        // TODO(redux): Handle ByteStreamError with retry (SDK doesn't retry these)
        let body = match resp.body.collect().await {
            Ok(b) => b.into_bytes(),
            Err(e) => {
                self.decrement_in_flight();
                return self.fail(error::chunk_failed(ChunkId::Download(seq), e));
            }
        };

        let mut segmented = SegmentedBuf::new();
        segmented.push(body);
        let chunk = ChunkOutput {
            seq,
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
        };

        {
            let work = self.ctx.state.work.lock().unwrap();
            let chunk_tx = match &*work {
                DownloadWorkState::Transferring { chunk_tx, .. } => chunk_tx,
                _ => {
                    return self.fail(error::from_kind(error::ErrorKind::RuntimeError)(
                        "unexpected state",
                    ))
                }
            };
            let _ = chunk_tx.try_send(Ok(chunk));
        }

        self.decrement_in_flight();

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::GetObjectRange { range, seq, etag },
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

    fn fail(&self, error: Error) -> WorkOutcome {
        self.done.store(true, Ordering::Release);
        self.ctx.set_failed(error);
        // Return a placeholder error - the real error is stored in ctx.status
        // and will be retrieved by Body when channel closes
        WorkOutcome::Failed {
            error: crate::error::from_kind(crate::error::ErrorKind::RuntimeError)(
                "transfer failed",
            ),
        }
    }
}
