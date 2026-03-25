/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload transfer implementation for scheduler integration.

use std::cmp;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Buf;
use tokio::sync::oneshot;
use tracing::Instrument;

use std::future::Future;
use std::pin::Pin;

use crate::error::{self, Error};
use crate::io::part_reader::Builder as PartReaderBuilder;
use crate::io::{InputStream, PartData};
use crate::metrics::IoSample;
use crate::operation::upload::context::UploadState;
use crate::operation::upload::input::convert::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::{UploadInput, UploadOutput, UploadOutputBuilder};
use crate::transfer::{IoKind, IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::BucketType;

/// Upload-specific work data.
#[derive(Debug)]
pub(crate) enum UploadWork {
    CreateMPU,
    UploadPart {
        part_number: u64,
        part_data: Option<PartData>,
    },
    CompleteMPU,
    PutObject {
        stream: Option<InputStream>,
    },
}

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

pub(crate) type UploadResultSender = oneshot::Sender<Result<UploadOutput, Error>>;
pub(crate) type UploadResultReceiver = oneshot::Receiver<Result<UploadOutput, Error>>;

/// Upload transfer that generates and executes upload work.
///
/// Cheap to clone - all state is behind `Arc`.
#[derive(Debug, Clone)]
pub(crate) struct UploadTransfer {
    inner: Arc<UploadTransferInner>,
}

/// Internal state for upload transfer.
#[derive(Debug)]
struct UploadTransferInner {
    /// Common transfer lifecycle management
    ctx: TransferContext,
    /// State machine for work progression
    state: Mutex<UploadState>,
    /// The original request (body taken for processing)
    request: Arc<UploadInput>,
    /// Type of S3 bucket targeted by this operation
    #[allow(dead_code)] // TODO(phase4): hedging/routing
    bucket_type: BucketType,
    /// Notified when CreateMPU completes (success or failure)
    create_mpu_complete: tokio::sync::Notify,
    /// Channel to send result to handle
    result_tx: Mutex<Option<UploadResultSender>>,
}

impl UploadTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        bucket_type: BucketType,
        request: UploadInput,
        stream: InputStream,
        result_tx: UploadResultSender,
    ) -> Self {
        // TODO: For unknown content length (streaming uploads), this will need adjustment.
        let content_length = stream
            .size_hint()
            .upper()
            .expect("content_length required; unknown length not yet supported");

        let inner = Arc::new(UploadTransferInner {
            ctx,
            state: Mutex::new(UploadState::PendingInit {
                stream,
                content_length,
                init_in_flight: false,
            }),
            request: Arc::new(request),
            bucket_type,
            create_mpu_complete: tokio::sync::Notify::new(),
            result_tx: Mutex::new(Some(result_tx)),
        });

        Self { inner }
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    /// Get the transfer ID.
    /// The original request (sans the body as it will have been taken for processing)
    pub(crate) fn request(&self) -> &UploadInput {
        &self.inner.request
    }

    /// Get the upload_id if MPU was started.
    pub(crate) fn upload_id(&self) -> Option<String> {
        let state = self.inner.state.lock().unwrap();
        match &*state {
            UploadState::Transferring { upload_id, .. }
            | UploadState::Completing { upload_id, .. } => Some(upload_id.clone()),
            _ => None,
        }
    }

    /// Check if CreateMPU is currently in flight.
    pub(crate) fn is_create_mpu_in_flight(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        matches!(
            &*state,
            UploadState::PendingInit {
                init_in_flight: true,
                ..
            }
        )
    }

    /// Get notified when CreateMPU completes.
    pub(crate) fn create_mpu_complete_notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.inner.create_mpu_complete.notified()
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready(work)` - work available to execute
    /// - `PollWork::Pending` - blocked waiting for in-flight work
    /// - `PollWork::Done` - transfer complete
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().unwrap();

        match &mut *state {
            UploadState::PendingInit {
                init_in_flight,
                content_length,
                stream,
            } => {
                if *init_in_flight {
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }

                let use_mpu = stream.is_mpu_only()
                    || *content_length >= self.inner.ctx.handle.mpu_threshold_bytes();
                if use_mpu {
                    *init_in_flight = true;
                    PollWork::Ready(IoRequest {
                        kind: IoKind::Network,
                        data: Some(Box::new(UploadWork::CreateMPU)),
                    })
                } else {
                    // Take ownership of stream by replacing state
                    match std::mem::replace(&mut *state, UploadState::PutObjectInFlight) {
                        UploadState::PendingInit { stream, .. } => PollWork::Ready(IoRequest {
                            kind: IoKind::Network,
                            data: Some(Box::new(UploadWork::PutObject {
                                stream: Some(stream),
                            })),
                        }),
                        _ => unreachable!(),
                    }
                }
            }
            UploadState::Transferring {
                next_part,
                total_parts,
                parts_in_flight,
                ..
            } => {
                if *next_part > *total_parts {
                    if *parts_in_flight > 0 {
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }
                let part_number = *next_part;
                *next_part += 1;
                *parts_in_flight += 1;
                PollWork::Ready(IoRequest {
                    kind: IoKind::Disk,
                    data: Some(Box::new(UploadWork::UploadPart {
                        part_number,
                        part_data: None,
                    })),
                })
            }
            UploadState::Completing {
                complete_in_flight, ..
            } => {
                if *complete_in_flight {
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }
                *complete_in_flight = true;
                PollWork::Ready(IoRequest {
                    kind: IoKind::Network,
                    data: Some(Box::new(UploadWork::CompleteMPU)),
                })
            }
            UploadState::PutObjectInFlight { .. } => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            UploadState::Done => PollWork::Done,
        }
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let kind = work.kind;
        let data = work.data_mut::<UploadWork>();
        match data {
            UploadWork::CreateMPU => self.execute_create_mpu().await,
            UploadWork::UploadPart {
                part_number,
                part_data,
            } => {
                self.execute_upload_part(*part_number, part_data, kind)
                    .await
            }
            UploadWork::CompleteMPU => self.execute_complete_mpu().await,
            UploadWork::PutObject { stream } => self.execute_put_object(stream).await,
        }
    }

    async fn execute_create_mpu(&self) -> WorkOutcome {
        let outcome = self.do_execute_create_mpu().await;
        // unblock any waiters that CreateMPU is complete (success or failure)
        self.inner.create_mpu_complete.notify_waiters();
        // state changed - try to wake if we were pending
        self.inner.ctx.try_wake();
        outcome
    }

    async fn do_execute_create_mpu(&self) -> WorkOutcome {
        let client = self.inner.ctx.s3_client();

        let mpu_req =
            copy_fields_to_mpu_request(&self.inner.request, client.create_multipart_upload());

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
            let mut state = self.inner.state.lock().unwrap();
            match std::mem::replace(&mut *state, UploadState::Done) {
                UploadState::PendingInit {
                    stream,
                    content_length,
                    ..
                } => (stream, content_length),
                _ => panic!("unexpected state"),
            }
        };

        let part_size = cmp::max(
            self.inner.ctx.handle.upload_part_size_bytes(),
            content_length.div_ceil(MAX_PARTS),
        );

        let total_parts = content_length.div_ceil(part_size);

        tracing::trace!("upload request using multipart upload with part size: {part_size} bytes");

        let part_reader = Arc::new(
            match PartReaderBuilder::new()
                .stream(stream)
                .part_size(part_size.try_into().expect("valid part size"))
                .direct_io(
                    self.inner
                        .ctx
                        .handle
                        .scheduler
                        .runtime()
                        .components()
                        .direct_io(),
                )
                .build()
            {
                Ok(reader) => reader,
                Err(e) => return self.fail(e.into()),
            },
        );

        {
            let mut state = self.inner.state.lock().unwrap();
            *state = UploadState::Transferring {
                upload_id,
                part_reader,
                next_part: 1,
                total_parts,
                parts_in_flight: 0,
                completed_parts: Vec::with_capacity(total_parts as usize),
                response_builder,
            };
        }

        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            total_parts,
            part_size,
            "MPU created, transferring",
        );

        WorkOutcome::Success {
            schedule_next: None,
            data: None,
            metrics: None,
        }
    }

    async fn execute_upload_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
        kind: IoKind,
    ) -> WorkOutcome {
        match kind {
            IoKind::Disk => self.execute_read_part(part_number, part_data).await,
            IoKind::Network => self.execute_send_part(part_number, part_data).await,
        }
    }

    async fn execute_read_part(
        &self,
        part_number: u64,
        part_data: &mut Option<PartData>,
    ) -> WorkOutcome {
        let part_reader = {
            let state = self.inner.state.lock().unwrap();
            match &*state {
                UploadState::Transferring { part_reader, .. } => part_reader.clone(),
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
                    schedule_next: Some(IoKind::Network),
                    data: Some(Box::new(UploadWork::UploadPart {
                        part_number,
                        part_data: part_data.take(),
                    })),
                    metrics: None,
                }
            }
            Ok(None) => {
                tracing::warn!("part_reader returned None for part {}", part_number);
                self.maybe_transition_to_completing();
                WorkOutcome::Success {
                    schedule_next: None,
                    data: None,
                    metrics: None,
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

        let upload_id = {
            let state = self.inner.state.lock().unwrap();
            match &*state {
                UploadState::Transferring { upload_id, .. } => upload_id.clone(),
                _ => panic!("unexpected state for send_part"),
            }
        };

        let part_num_i32 = part_number as i32;
        let content_length = data.data.remaining() as i64;

        let bytes_sent = content_length as u64;

        let upload_id = upload_id.clone();
        let data_bytes = data.data;
        let checksum = data.checksum;

        let resp = match self
            .inner
            .ctx
            .handle
            .upload_latencies
            .guarded(|| {
                let body = ByteStream::from(data_bytes.clone());
                let req = copy_fields_to_upload_part_request(
                    &self.inner.request,
                    self.inner
                        .ctx
                        .s3_client()
                        .upload_part()
                        .upload_id(&upload_id)
                        .part_number(part_num_i32)
                        .content_length(content_length)
                        .body(body),
                    checksum.as_ref(),
                );
                async move {
                    req.customize()
                        .disable_payload_signing()
                        .send()
                        .instrument(tracing::debug_span!("send-upload-part", part_number))
                        .await
                }
            })
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e),
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

        {
            let mut state = self.inner.state.lock().unwrap();
            if let UploadState::Transferring {
                completed_parts, ..
            } = &mut *state
            {
                completed_parts.push(completed);
            }
        }

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            part_number,
            bytes_sent,
            "part uploaded",
        );

        self.maybe_transition_to_completing();

        WorkOutcome::Success {
            schedule_next: None,
            metrics: Some(IoSample {
                network_tx: bytes_sent,
                ..Default::default()
            }),
            data: None,
        }
    }

    fn maybe_transition_to_completing(&self) {
        let mut state = self.inner.state.lock().unwrap();
        let should_complete = if let UploadState::Transferring {
            parts_in_flight,
            next_part,
            total_parts,
            ..
        } = &mut *state
        {
            *parts_in_flight -= 1;
            *next_part > *total_parts && *parts_in_flight == 0
        } else {
            false
        };

        if should_complete {
            if let UploadState::Transferring {
                upload_id,
                part_reader,
                completed_parts,
                response_builder,
                ..
            } = std::mem::replace(&mut *state, UploadState::Done)
            {
                *state = UploadState::Completing {
                    upload_id,
                    part_reader,
                    completed_parts,
                    response_builder,
                    complete_in_flight: false,
                };
            }
            drop(state);
            self.inner.ctx.try_wake();
        }
    }

    async fn execute_put_object(&self, stream: &mut Option<InputStream>) -> WorkOutcome {
        use crate::operation::upload::input::convert::copy_fields_to_put_object_request;

        let stream = stream
            .take()
            .expect("stream should be present for PutObject");

        // TODO: Currently PutObject does not use our DataIO scheduling - the actual
        // disk I/O happens lazily when the SDK consumes the ByteStream during HTTP send.
        // For true scheduler control over disk I/O (important for large numbers of small files),
        // InputStream internals will need to be tightly integrated with our DataIO work layer.
        let byte_stream = match stream.into_byte_stream().await {
            Ok(bs) => bs,
            Err(e) => return self.fail(e.into()),
        };

        let put_req = copy_fields_to_put_object_request(
            &self.inner.request,
            self.inner.ctx.s3_client().put_object().body(byte_stream),
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

        self.inner.ctx.set_completed();
        self.inner.ctx.signal_terminal();

        if let Some(tx) = self.inner.result_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(result));
        }

        WorkOutcome::Success {
            metrics: None,
            schedule_next: None,
            data: None,
        }
    }

    async fn execute_complete_mpu(&self) -> WorkOutcome {
        let (upload_id, mut completed_parts, response_builder, part_reader) = {
            let mut state = self.inner.state.lock().unwrap();
            match std::mem::replace(&mut *state, UploadState::Done) {
                UploadState::Completing {
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

        let base_req = self
            .inner
            .ctx
            .s3_client()
            .complete_multipart_upload()
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts))
                    .build(),
            );

        let complete_req = super::input::convert::copy_fields_to_complete_mpu_request(
            &self.inner.request,
            base_req,
            || async { part_reader.full_object_checksum().await },
        )
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

        self.inner.ctx.set_completed();
        self.inner.ctx.signal_terminal();

        if let Some(tx) = self.inner.result_tx.lock().unwrap().take() {
            let _ = tx.send(Ok(result));
        }

        WorkOutcome::Success {
            metrics: None,
            schedule_next: None,
            data: None,
        }
    }

    fn fail(&self, error: Error) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            %error,
            "upload failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        self.inner.ctx.set_failed(error::Error::new(
            error::ErrorKind::RuntimeError,
            "upload failed",
        ));
        self.inner.ctx.signal_terminal();

        if let Some(tx) = self.inner.result_tx.lock().unwrap().take() {
            let _ = tx.send(Err(error::Error::new(
                error::ErrorKind::RuntimeError,
                "upload failed",
            )));
        }
        WorkOutcome::Failed { classification }
    }
}

impl Transfer for UploadTransfer {
    fn ctx(&self) -> &TransferContext {
        UploadTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        UploadTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(UploadTransfer::execute(self, work))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::InputStream;
    use crate::scheduler::test_util::{assert_pending, assert_ready};
    use crate::transfer::IoKind;
    use crate::DEFAULT_CONCURRENCY;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    fn create_test_transfer(
        s3_client: aws_sdk_s3::Client,
        content: Vec<u8>,
    ) -> (UploadTransfer, UploadResultReceiver) {
        let handle = Arc::new(crate::client::Handle::with_config_and_scheduler(
            crate::Config::builder().client(s3_client).build(),
            crate::scheduler::Scheduler::new(DEFAULT_CONCURRENCY),
        ));

        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let stream = InputStream::from(content);
        let (result_tx, result_rx) = oneshot::channel();

        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = UploadTransfer::new(ctx, BucketType::Standard, input, stream, result_tx);
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

    #[test]
    fn test_poll_work_initial_state_returns_create_mpu() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::CreateMPU));
    }

    #[test]
    fn test_poll_work_pending_while_init_in_flight() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[tokio::test]
    async fn test_poll_work_generates_parts_after_create_mpu() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());
        transfer.execute(&mut work).await;

        let mut work1 = assert_ready(transfer.poll_work());
        let data1 = work1.data_mut::<UploadWork>();
        assert!(matches!(
            data1,
            UploadWork::UploadPart { part_number: 1, .. }
        ));

        let mut work2 = assert_ready(transfer.poll_work());
        let data2 = work2.data_mut::<UploadWork>();
        assert!(matches!(
            data2,
            UploadWork::UploadPart { part_number: 2, .. }
        ));

        assert_pending(transfer.poll_work());
    }

    #[tokio::test]
    async fn test_execute_create_mpu_transitions_to_transferring() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());

        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(
            outcome,
            WorkOutcome::Success {
                schedule_next: None,
                ..
            }
        ));

        let mut next = assert_ready(transfer.poll_work());
        let data = next.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::UploadPart { .. }));
    }

    #[tokio::test]
    async fn test_execute_read_part_returns_schedule_next_network() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, _rx) = create_test_transfer(s3_client, content);

        let mut create_work = assert_ready(transfer.poll_work());
        transfer.execute(&mut create_work).await;

        let mut part_work = assert_ready(transfer.poll_work());
        assert_eq!(part_work.kind, IoKind::Disk);

        let outcome = transfer.execute(&mut part_work).await;
        match outcome {
            WorkOutcome::Success {
                schedule_next,
                data,
                ..
            } => {
                assert_eq!(schedule_next, Some(IoKind::Network));
                if let Some(mut boxed_data) = data {
                    let upload_work = (*boxed_data)
                        .as_any_mut()
                        .downcast_mut::<UploadWork>()
                        .unwrap();
                    if let UploadWork::UploadPart { part_data, .. } = upload_work {
                        assert!(part_data.is_some());
                    } else {
                        panic!("expected UploadPart data");
                    }
                } else {
                    panic!("expected Some data");
                }
            }
            _ => panic!("expected Success"),
        }
    }

    #[tokio::test]
    async fn test_execute_full_mpu_flow() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let (transfer, rx) = create_test_transfer(s3_client, content);

        // 1. CreateMPU
        let mut work = assert_ready(transfer.poll_work());
        transfer.execute(&mut work).await;

        // 2. UploadPart - DataIO phase
        let mut work = assert_ready(transfer.poll_work());
        let outcome = transfer.execute(&mut work).await;

        // 3. UploadPart - Network phase
        let mut work = match outcome {
            WorkOutcome::Success {
                schedule_next: Some(IoKind::Network),
                data,
                ..
            } => IoRequest {
                kind: IoKind::Network,
                data,
            },
            _ => panic!("expected schedule_next Network"),
        };
        transfer.execute(&mut work).await;

        // 3b. Second UploadPart - DataIO phase
        let mut work = assert_ready(transfer.poll_work());
        let outcome = transfer.execute(&mut work).await;

        // 3c. Second UploadPart - Network phase
        let mut work = match outcome {
            WorkOutcome::Success {
                schedule_next: Some(IoKind::Network),
                data,
                ..
            } => IoRequest {
                kind: IoKind::Network,
                data,
            },
            _ => panic!("expected schedule_next Network for part 2"),
        };
        transfer.execute(&mut work).await;

        // 4. CompleteMPU
        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::CompleteMPU));
        transfer.execute(&mut work).await;

        // 5. Should be Done
        use crate::scheduler::test_util::assert_done;
        assert_done(transfer.poll_work());

        // 6. Result should be available
        let result = rx.await.expect("result channel");
        assert!(result.is_ok());
    }
}
