/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload transfer implementation for scheduler integration.

use std::cmp;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Buf;
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::error::{self, Error};
use crate::io::part_reader::Builder as PartReaderBuilder;
use crate::io::PartData;
use crate::operation::upload::context::{UploadContext, UploadWorkState};
use crate::operation::upload::input::convert::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::{UploadOutput, UploadOutputBuilder};
use crate::scheduler::{TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

pub(crate) type UploadResultSender = oneshot::Sender<Result<UploadOutput, Error>>;
pub(crate) type UploadResultReceiver = oneshot::Receiver<Result<UploadOutput, Error>>;

/// Upload transfer that generates and executes upload work.
#[derive(Debug, Clone)]
pub(crate) struct UploadTransfer {
    id: TransferId,
    ctx: UploadContext,
    done: Arc<AtomicBool>,
    result_tx: Arc<Mutex<Option<UploadResultSender>>>,
}

impl UploadTransfer {
    pub(crate) fn new(id: TransferId, ctx: UploadContext, result_tx: UploadResultSender) -> Self {
        Self {
            id,
            ctx,
            done: Arc::new(AtomicBool::new(false)),
            result_tx: Arc::new(Mutex::new(Some(result_tx))),
        }
    }

    #[cfg(test)]
    pub(crate) fn stub(id: TransferId, part_count: usize) -> Self {
        use crate::io::InputStream;
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
        let stream = InputStream::from(vec![0u8; content_length as usize]);
        let ctx = UploadContext::new(handle, BucketType::Standard, input, stream);

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
            UploadWorkState::PendingInit { init_in_flight, .. } => {
                if *init_in_flight {
                    return None;
                }
                *init_in_flight = true;
                Some(WorkItem {
                    transfer_id: self.id,
                    kind: WorkKind::Network,
                    data: WorkData::CreateMPU,
                })
            }
            UploadWorkState::Transferring {
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
                    kind: WorkKind::DataIO,
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: None,
                    },
                })
            }
            UploadWorkState::Completing {
                complete_in_flight, ..
            } => {
                if *complete_in_flight {
                    return None;
                }
                *complete_in_flight = true;
                Some(WorkItem {
                    transfer_id: self.id,
                    kind: WorkKind::Network,
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
                self.execute_upload_part(*part_number, part_data, work.kind)
                    .await
            }
            WorkData::CompleteMPU => self.execute_complete_mpu().await,
        }
    }

    async fn execute_create_mpu(&self) -> WorkOutcome {
        let client = self.ctx.client();
        let req = self.ctx.state.request();

        let mpu_req = copy_fields_to_mpu_request(req, client.create_multipart_upload());

        let resp = match mpu_req
            .send()
            .instrument(tracing::debug_span!("send-create-multipart-upload"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };

        let upload_id = resp.upload_id().expect("upload_id present").to_string();
        let response_builder = UploadOutputBuilder::from(resp);

        let (stream, content_length) = {
            let mut work = self.ctx.state.work.lock().unwrap();
            match std::mem::replace(&mut *work, UploadWorkState::Done) {
                UploadWorkState::PendingInit {
                    stream,
                    content_length,
                    ..
                } => (stream, content_length),
                _ => panic!("unexpected state"),
            }
        };

        let part_size = cmp::max(
            self.ctx.handle.upload_part_size_bytes(),
            content_length.div_ceil(MAX_PARTS),
        );

        let total_parts = content_length.div_ceil(part_size);

        let part_reader = Arc::new(
            PartReaderBuilder::new()
                .stream(stream)
                .part_size(part_size.try_into().expect("valid part size"))
                .build(),
        );

        {
            let mut work = self.ctx.state.work.lock().unwrap();
            *work = UploadWorkState::Transferring {
                upload_id,
                part_reader,
                next_part: 1,
                total_parts,
                parts_in_flight: 0,
                completed_parts: Vec::with_capacity(total_parts as usize),
                response_builder,
            };
        }

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::CreateMPU,
        }
    }

    async fn execute_upload_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
        kind: WorkKind,
    ) -> WorkOutcome {
        match kind {
            WorkKind::DataIO => self.execute_read_part(part_number, part_data).await,
            WorkKind::Network => self.execute_send_part(part_number, part_data).await,
        }
    }

    async fn execute_read_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
    ) -> WorkOutcome {
        let part_reader = {
            let work = self.ctx.state.work.lock().unwrap();
            match &*work {
                UploadWorkState::Transferring { part_reader, .. } => part_reader.clone(),
                _ => panic!("unexpected state for read_part"),
            }
        };

        match part_reader
            .next_part()
            .instrument(tracing::debug_span!("read-upload-body"))
            .await
        {
            Ok(Some(data)) => {
                *part_data = Some(data);
                WorkOutcome::Success {
                    schedule_next: Some(WorkKind::Network),
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: part_data.take(),
                    },
                }
            }
            Ok(None) => {
                tracing::warn!("part_reader returned None for part {}", part_number);
                WorkOutcome::Success {
                    schedule_next: None,
                    data: WorkData::UploadPart {
                        part_number,
                        part_data: None,
                    },
                }
            }
            Err(e) => self.fail(e.into()),
        }
    }

    async fn execute_send_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
    ) -> WorkOutcome {
        let data = part_data
            .take()
            .expect("part_data should be set after DataIO");

        // Get upload_id before the async call
        let upload_id = {
            let work = self.ctx.state.work.lock().unwrap();
            match &*work {
                UploadWorkState::Transferring { upload_id, .. } => upload_id.clone(),
                _ => panic!("unexpected state for send_part"),
            }
        };

        let part_num_i32 = part_number as i32;
        let content_length = data.data.remaining() as i64;

        let req = copy_fields_to_upload_part_request(
            &self.ctx.state.request,
            self.ctx
                .client()
                .upload_part()
                .upload_id(&upload_id)
                .part_number(part_num_i32)
                .content_length(content_length)
                .body(ByteStream::from(data.data)),
            data.checksum.as_ref(),
        );

        let resp = match req
            .customize()
            .disable_payload_signing()
            .send()
            .instrument(tracing::debug_span!("send-upload-part", part_number))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };

        let completed = CompletedPart::builder()
            .part_number(part_num_i32)
            .set_e_tag(resp.e_tag.clone())
            .set_checksum_crc32(resp.checksum_crc32.clone())
            .set_checksum_crc32_c(resp.checksum_crc32_c.clone())
            .set_checksum_crc64_nvme(resp.checksum_crc64_nvme.clone())
            .set_checksum_sha1(resp.checksum_sha1.clone())
            .set_checksum_sha256(resp.checksum_sha256.clone())
            .build();

        // Single lock to record completion and potentially transition state
        {
            let mut work = self.ctx.state.work.lock().unwrap();
            let should_complete = if let UploadWorkState::Transferring {
                parts_in_flight,
                next_part,
                total_parts,
                completed_parts,
                ..
            } = &mut *work
            {
                completed_parts.push(completed);
                *parts_in_flight -= 1;
                *next_part > *total_parts && *parts_in_flight == 0
            } else {
                false
            };

            if should_complete {
                if let UploadWorkState::Transferring {
                    upload_id,
                    completed_parts,
                    response_builder,
                    ..
                } = std::mem::replace(&mut *work, UploadWorkState::Done)
                {
                    *work = UploadWorkState::Completing {
                        upload_id,
                        completed_parts,
                        response_builder,
                        complete_in_flight: false,
                    };
                }
            }
        }

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::UploadPart {
                part_number,
                part_data: None,
            },
        }
    }

    async fn execute_complete_mpu(&self) -> WorkOutcome {
        let (upload_id, mut completed_parts, response_builder) = {
            let mut work = self.ctx.state.work.lock().unwrap();
            match std::mem::replace(&mut *work, UploadWorkState::Done) {
                UploadWorkState::Completing {
                    upload_id,
                    completed_parts,
                    response_builder,
                    ..
                } => (upload_id, completed_parts, response_builder),
                _ => panic!("unexpected state for complete_mpu"),
            }
        };

        completed_parts.sort_by_key(|p| p.part_number);

        let req = self.ctx.state.request();
        let complete_req = self
            .ctx
            .client()
            .complete_multipart_upload()
            .bucket(req.bucket().unwrap_or_default())
            .key(req.key().unwrap_or_default())
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            );

        let resp = match complete_req
            .send()
            .instrument(tracing::debug_span!("send-complete-multipart-upload"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };

        let result = response_builder
            .update_from_complete_mpu(&resp)
            .build()
            .expect("valid response");

        self.done.store(true, Ordering::Release);

        if let Some(tx) = self.result_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(result));
        }

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::CompleteMPU,
        }
    }

    /// Mark transfer as failed and send error to handle
    fn fail(&self, error: Error) -> WorkOutcome {
        self.done.store(true, Ordering::Release);
        if let Some(tx) = self.result_tx.lock().unwrap().take() {
            // Send a generic error to the handle - the actual error goes to WorkOutcome
            let _ = tx.send(Err(error::Error::new(
                error::ErrorKind::RuntimeError,
                "upload failed",
            )));
        }
        WorkOutcome::Failed { error }
    }
}
