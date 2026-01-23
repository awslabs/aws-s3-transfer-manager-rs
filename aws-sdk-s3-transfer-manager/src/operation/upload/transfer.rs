/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload transfer implementation for scheduler integration.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::error::Error;
use crate::io::PartData;
use crate::operation::upload::context::{UploadContext, UploadWorkState};
use crate::operation::upload::{UploadOutput, UploadOutputBuilder};
use crate::scheduler::{TransferId, WorkData, WorkItem, WorkOutcome, WorkPhase};

/// Channel for delivering upload result to handle
pub(crate) type UploadResultSender = oneshot::Sender<Result<UploadOutput, Error>>;
pub(crate) type UploadResultReceiver = oneshot::Receiver<Result<UploadOutput, Error>>;

// TODO(redux): what about retries?

/// Upload transfer that generates and executes upload work.
///
/// Clone is cheap (Arc).
#[derive(Debug, Clone)]
pub(crate) struct UploadTransfer {
    id: TransferId,
    ctx: UploadContext,
    done: Arc<AtomicBool>,
    result_tx: Arc<Mutex<Option<UploadResultSender>>>,
}

impl UploadTransfer {
    /// Create a new upload transfer with result channel
    pub(crate) fn new(id: TransferId, ctx: UploadContext, result_tx: UploadResultSender) -> Self {
        Self {
            id,
            ctx,
            done: Arc::new(AtomicBool::new(false)),
            result_tx: Arc::new(Mutex::new(Some(result_tx))),
        }
    }

    /// Create stub for testing
    #[cfg(test)]
    pub(crate) fn stub(id: TransferId, part_count: usize) -> Self {
        use crate::operation::upload::UploadInput;
        use crate::types::BucketType;
        use crate::DEFAULT_CONCURRENCY;

        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let handle = Arc::new(crate::client::Handle {
            config: crate::Config::builder().client(s3_client).build(),
            scheduler: crate::runtime::scheduler::Scheduler::new(
                crate::types::ConcurrencyMode::Explicit(8),
            ),
            new_scheduler: crate::scheduler::Scheduler::new(
                DEFAULT_CONCURRENCY,
                DEFAULT_CONCURRENCY,
            ),
        });

        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let content_length = part_count as u64 * 8 * 1024 * 1024;
        let ctx = UploadContext::new(handle, BucketType::Standard, input, content_length);

        // Create a dummy channel for testing (receiver is dropped)
        let (result_tx, _) = oneshot::channel();

        Self {
            id,
            ctx,
            done: Arc::new(AtomicBool::new(false)),
            result_tx: Arc::new(Mutex::new(Some(result_tx))),
        }
    }

    pub(crate) fn id(&self) -> TransferId {
        self.id
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub(crate) fn next_work(&self) -> Option<WorkItem> {
        let mut work = self.ctx.state.work.lock().unwrap();

        match &mut *work {
            UploadWorkState::Uninitialized => None,
            UploadWorkState::PendingInit { init_in_flight, .. } => {
                if *init_in_flight {
                    return None;
                }
                *init_in_flight = true;
                Some(WorkItem {
                    transfer_id: self.id,
                    phase: WorkPhase::Network,
                    data: WorkData::CreateMPU,
                })
            }
            UploadWorkState::Active {
                next_part,
                total_parts,
                parts_in_flight,
                ..
            } => {
                if *next_part > *total_parts {
                    return None;
                }
                let part_number = *next_part;
                *next_part += 1;
                *parts_in_flight += 1;
                Some(WorkItem {
                    transfer_id: self.id,
                    phase: WorkPhase::DataIO,
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: None,
                    },
                })
            }
            UploadWorkState::Completing { complete_in_flight } => {
                if *complete_in_flight {
                    return None;
                }
                *complete_in_flight = true;
                Some(WorkItem {
                    transfer_id: self.id,
                    phase: WorkPhase::Network,
                    data: WorkData::CompleteMPU,
                })
            }
            UploadWorkState::Done => None,
        }
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        match &mut work.data {
            WorkData::CreateMPU => self.execute_create_mpu().await,
            WorkData::UploadPart {
                part_number,
                part_data,
            } => {
                self.execute_upload_part(*part_number, part_data, work.phase)
                    .await
            }
            WorkData::CompleteMPU => self.execute_complete_mpu().await,
        }
    }

    async fn execute_create_mpu(&self) -> WorkOutcome {
        // TODO: Actually call CreateMultipartUpload via SDK
        let upload_id = "stub-upload-id".to_string();

        let total_parts = {
            let work = self.ctx.state.work.lock().unwrap();
            match &*work {
                UploadWorkState::PendingInit { content_length, .. } => {
                    (*content_length).div_ceil(8 * 1024 * 1024)
                }
                _ => 0,
            }
        };

        {
            let mut work = self.ctx.state.work.lock().unwrap();
            *work = UploadWorkState::Active {
                upload_id,
                next_part: 1,
                total_parts,
                parts_in_flight: 0,
            };
        }

        WorkOutcome::Success {
            next_phase: None,
            data: WorkData::CreateMPU,
        }
    }

    async fn execute_upload_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
        phase: WorkPhase,
    ) -> WorkOutcome {
        match phase {
            WorkPhase::DataIO => {
                // TODO: Actually read from part_reader
                let data = PartData::new(part_number, bytes::Bytes::from_static(b"stub"));
                *part_data = Some(data);

                WorkOutcome::Success {
                    next_phase: Some(WorkPhase::Network),
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: part_data.take(),
                    },
                }
            }
            WorkPhase::Network => {
                // TODO: Actually presign and send HTTP request
                let _data = part_data
                    .take()
                    .expect("part_data should be set after DataIO");

                let should_complete = {
                    let mut work = self.ctx.state.work.lock().unwrap();
                    if let UploadWorkState::Active {
                        parts_in_flight,
                        next_part,
                        total_parts,
                        ..
                    } = &mut *work
                    {
                        *parts_in_flight -= 1;
                        *next_part > *total_parts && *parts_in_flight == 0
                    } else {
                        false
                    }
                };

                if should_complete {
                    let mut work = self.ctx.state.work.lock().unwrap();
                    *work = UploadWorkState::Completing {
                        complete_in_flight: false,
                    };
                }

                WorkOutcome::Success {
                    next_phase: None,
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: None,
                    },
                }
            }
        }
    }

    async fn execute_complete_mpu(&self) -> WorkOutcome {
        // TODO: Actually call CompleteMultipartUpload via SDK

        {
            let mut work = self.ctx.state.work.lock().unwrap();
            *work = UploadWorkState::Done;
        }
        self.done.store(true, Ordering::Release);

        // Send result to handle
        // TODO: Build actual UploadOutput from response
        let result = UploadOutputBuilder::default()
            .build()
            .expect("UploadOutput has no required fields");
        if let Some(tx) = self.result_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(result));
        }

        WorkOutcome::Success {
            next_phase: None,
            data: WorkData::CompleteMPU,
        }
    }
}
