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
use bytes::{Buf, Bytes};
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::error::{self, Error};
use crate::io::part_reader::Builder as PartReaderBuilder;
use crate::io::{InputStream, PartData};
use crate::operation::upload::context::{UploadContext, UploadWorkState};
use crate::operation::upload::input::convert::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::{UploadOutput, UploadOutputBuilder};
use crate::scheduler::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};

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

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - blocked waiting for in-flight work
    /// - `PollWork::Done` - transfer complete
    pub(crate) fn poll_work(&self) -> PollWork {
        if self.done.load(Ordering::Acquire) {
            return PollWork::Done;
        }

        let mut work = self.ctx.state.work.lock().unwrap();

        match &mut *work {
            UploadWorkState::PendingInit {
                init_in_flight,
                content_length,
                stream,
            } => {
                if *init_in_flight {
                    return PollWork::Pending;
                }

                let use_mpu = stream.is_mpu_only()
                    || *content_length >= self.ctx.handle.mpu_threshold_bytes();
                if use_mpu {
                    *init_in_flight = true;
                    PollWork::Ready(WorkItem {
                        transfer_id: self.id,
                        kind: WorkKind::Network,
                        data: WorkData::CreateMPU,
                    })
                } else {
                    // Take ownership of stream by replacing state
                    match std::mem::replace(&mut *work, UploadWorkState::PutObjectInFlight) {
                        UploadWorkState::PendingInit { stream, .. } => PollWork::Ready(WorkItem {
                            transfer_id: self.id,
                            kind: WorkKind::Network,
                            data: WorkData::PutObject {
                                stream: Some(stream),
                            },
                        }),
                        _ => unreachable!(),
                    }
                }
            }
            UploadWorkState::Transferring {
                next_part,
                total_parts,
                parts_in_flight,
                ..
            } => {
                if *next_part > *total_parts {
                    // All parts generated, waiting for in-flight to complete
                    if *parts_in_flight > 0 {
                        return PollWork::Pending;
                    }
                    // Should have transitioned to Completing - unexpected
                    return PollWork::Pending;
                }
                let part_number = *next_part;
                *next_part += 1;
                *parts_in_flight += 1;
                PollWork::Ready(WorkItem {
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
                    return PollWork::Pending;
                }
                *complete_in_flight = true;
                PollWork::Ready(WorkItem {
                    transfer_id: self.id,
                    kind: WorkKind::Network,
                    data: WorkData::CompleteMPU,
                })
            }
            UploadWorkState::PutObjectInFlight { .. } => PollWork::Pending,
            UploadWorkState::Done => PollWork::Done,
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
            WorkData::PutObject { stream } => self.execute_put_object(stream).await,
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
                    part_reader,
                    completed_parts,
                    response_builder,
                    ..
                } = std::mem::replace(&mut *work, UploadWorkState::Done)
                {
                    *work = UploadWorkState::Completing {
                        upload_id,
                        part_reader,
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

    async fn execute_put_object(&self, stream: &mut Option<InputStream>) -> WorkOutcome {
        use crate::operation::upload::input::convert::copy_fields_to_put_object_request;

        let stream = stream
            .take()
            .expect("stream should be present for PutObject");

        // TODO(redux): Currently PutObject does not use our DataIO scheduling - the actual
        // disk I/O happens lazily when the SDK consumes the ByteStream during HTTP send.
        // For true scheduler control over disk I/O (important for large numbers of small files),
        // InputStream internals will need to be tightly integrated with our DataIO work layer.
        let byte_stream = match stream.into_byte_stream().await {
            Ok(bs) => bs,
            Err(e) => return self.fail(e.into()),
        };

        let req = self.ctx.state.request();
        let put_req = copy_fields_to_put_object_request(
            req,
            self.ctx.client().put_object().body(byte_stream),
        );

        let resp = match put_req
            .send()
            .instrument(tracing::debug_span!("send-put-object"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };

        let result = UploadOutputBuilder::from(resp)
            .build()
            .expect("valid response");

        self.done.store(true, Ordering::Release);

        if let Some(tx) = self.result_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(result));
        }

        WorkOutcome::Success {
            schedule_next: None,
            data: WorkData::PutObject { stream: None },
        }
    }

    async fn execute_complete_mpu(&self) -> WorkOutcome {
        let (upload_id, mut completed_parts, response_builder, part_reader) = {
            let mut work = self.ctx.state.work.lock().unwrap();
            match std::mem::replace(&mut *work, UploadWorkState::Done) {
                UploadWorkState::Completing {
                    upload_id,
                    completed_parts,
                    response_builder,
                    part_reader,
                    ..
                } => (upload_id, completed_parts, response_builder, part_reader),
                _ => panic!("unexpected state for complete_mpu"),
            }
        };

        completed_parts.sort_by_key(|p| p.part_number);

        let req = self.ctx.state.request();
        let base_req = self
            .ctx
            .client()
            .complete_multipart_upload()
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            );

        let complete_req =
            super::input::convert::copy_fields_to_complete_mpu_request(req, base_req, || async {
                part_reader.full_object_checksum().await
            })
            .await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::InputStream;
    use crate::operation::upload::UploadInput;
    use crate::scheduler::{PollWork, WorkData, WorkKind};
    use crate::types::BucketType;
    use crate::DEFAULT_CONCURRENCY;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use bytes::Bytes;

    /// Create an UploadTransfer for testing with a mocked S3 client
    fn create_test_transfer(
        s3_client: aws_sdk_s3::Client,
        content: Vec<u8>,
    ) -> (UploadTransfer, UploadResultReceiver) {
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

        let stream = InputStream::from(content);
        let ctx = UploadContext::new(handle, BucketType::Standard, input, stream);

        let (result_tx, result_rx) = oneshot::channel();
        let id = TransferId {
            id: 1,
            parent: None,
        };
        let transfer = UploadTransfer::new(id, ctx, result_tx);
        (transfer, result_rx)
    }

    fn mock_s3_client_for_mpu() -> aws_sdk_s3::Client {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        });

        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());

        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload).then_output(|| {
            CompleteMultipartUploadOutput::builder()
                .e_tag("final-etag")
                .build()
        });

        mock_client!(
            aws_sdk_s3,
            RuleMode::MatchAny,
            &[create_mpu, upload_part, complete_mpu]
        )
    }

    // ==================== poll_work tests ====================

    #[test]
    fn test_poll_work_initial_state_returns_create_mpu() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024]; // 16MB = 2 parts at 8MB default
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let work = transfer.poll_work();
        assert!(matches!(work, PollWork::Ready(w) if matches!(w.data, WorkData::CreateMPU)));
    }

    #[test]
    fn test_poll_work_pending_while_init_in_flight() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        // First poll returns CreateMPU
        let work = transfer.poll_work();
        assert!(matches!(work, PollWork::Ready(_)));

        // Second poll should return Pending (init_in_flight = true)
        let work = transfer.poll_work();
        assert!(matches!(work, PollWork::Pending));
    }

    #[tokio::test]
    async fn test_poll_work_generates_parts_after_create_mpu() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024]; // 16MB = 2 parts
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        // Get and execute CreateMPU
        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };
        transfer.execute(&mut work).await;

        // Now should generate UploadPart work items
        let work1 = transfer.poll_work();
        assert!(
            matches!(work1, PollWork::Ready(w) if matches!(w.data, WorkData::UploadPart { part_number: 1, .. }))
        );

        let work2 = transfer.poll_work();
        assert!(
            matches!(work2, PollWork::Ready(w) if matches!(w.data, WorkData::UploadPart { part_number: 2, .. }))
        );

        // After all parts generated, should be Pending
        let work3 = transfer.poll_work();
        assert!(matches!(work3, PollWork::Pending));
    }

    // ==================== execute tests ====================

    #[tokio::test]
    async fn test_execute_create_mpu_transitions_to_transferring() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };

        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(
            outcome,
            WorkOutcome::Success {
                schedule_next: None,
                ..
            }
        ));

        // After CreateMPU, should be able to generate parts
        let next = transfer.poll_work();
        assert!(
            matches!(next, PollWork::Ready(w) if matches!(w.data, WorkData::UploadPart { .. }))
        );
    }

    #[tokio::test]
    async fn test_execute_read_part_returns_schedule_next_network() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024]; // 16MB = 2 parts (above MPU threshold)
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        // Execute CreateMPU
        let mut create_work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };
        transfer.execute(&mut create_work).await;

        // Get UploadPart work (DataIO phase)
        let mut part_work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };
        assert_eq!(part_work.kind, WorkKind::DataIO);

        // Execute DataIO phase - should return schedule_next = Network
        let outcome = transfer.execute(&mut part_work).await;
        match outcome {
            WorkOutcome::Success {
                schedule_next,
                data,
            } => {
                assert_eq!(schedule_next, Some(WorkKind::Network));
                // Data should have part_data populated
                if let WorkData::UploadPart { part_data, .. } = data {
                    assert!(part_data.is_some());
                } else {
                    panic!("expected UploadPart data");
                }
            }
            _ => panic!("expected Success"),
        }
    }

    #[tokio::test]
    async fn test_execute_full_mpu_flow() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024]; // 16MB = 2 parts (above MPU threshold)
        let (transfer, rx) = create_test_transfer(s3_client, content);

        // 1. CreateMPU
        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };
        transfer.execute(&mut work).await;

        // 2. UploadPart - DataIO phase
        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };
        let outcome = transfer.execute(&mut work).await;

        // 3. UploadPart - Network phase (continue with schedule_next data)
        let mut work = match outcome {
            WorkOutcome::Success {
                schedule_next: Some(WorkKind::Network),
                data,
            } => WorkItem {
                transfer_id: transfer.id(),
                kind: WorkKind::Network,
                data,
            },
            _ => panic!("expected schedule_next Network"),
        };
        transfer.execute(&mut work).await;

        // 3b. Second UploadPart - DataIO phase
        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready for part 2"),
        };
        let outcome = transfer.execute(&mut work).await;

        // 3c. Second UploadPart - Network phase
        let mut work = match outcome {
            WorkOutcome::Success {
                schedule_next: Some(WorkKind::Network),
                data,
            } => WorkItem {
                transfer_id: transfer.id(),
                kind: WorkKind::Network,
                data,
            },
            _ => panic!("expected schedule_next Network for part 2"),
        };
        transfer.execute(&mut work).await;

        // 4. CompleteMPU
        let mut work = match transfer.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready for CompleteMPU"),
        };
        assert!(matches!(work.data, WorkData::CompleteMPU));
        transfer.execute(&mut work).await;

        // 5. Should be Done
        assert!(matches!(transfer.poll_work(), PollWork::Done));

        // 6. Result should be available
        let result = rx.await.expect("result channel");
        assert!(result.is_ok());
    }
}
