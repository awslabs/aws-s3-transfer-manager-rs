/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload transfer implementation for scheduler integration.

use std::cmp;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_types::timeout::TimeoutConfig;
use bytes::Buf;
use tracing::Instrument;

use std::future::Future;
use std::pin::Pin;

use crate::error::Error;
use crate::io::part_reader::Builder as PartReaderBuilder;
use crate::io::InputStream;
use crate::operation::upload::context::UploadState;
use crate::operation::upload::input::convert::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::{UploadInput, UploadOutput, UploadOutputBuilder};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::BucketType;

/// Upload-specific work data.
#[derive(Debug)]
pub(crate) enum UploadWork {
    CreateMPU,
    UploadPart,
    CompleteMPU,
    PutObject { stream: Option<InputStream> },
}

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

/// Bound on time-to-first-response-byte for a `PutObject` call. Applied
/// per-operation via [`TimeoutConfig::read_timeout`] through a
/// `.config_override(...)`; leaves any caller-provided `TimeoutConfig`
/// fields on the client (connect / operation / operation-attempt) intact.
///
/// 30s is wide enough for slow networks and tight enough to surface a
/// stuck connection before the transfer stalls. On timeout the SDK's
/// standard retry strategy rebuilds the body from the source (see
/// `SdkBody::retryable` wiring in `InputStream::into_sdk_body`) and
/// retries on a fresh connection.
///
/// Scoped to `PutObject` only. `UploadPart` uses the adaptive
/// `LatencyTracker::guarded` wrapper; control-plane operations rely on
/// SDK or caller configuration.
/// TODO(vnext): remove/replace when read timeout is defaulted for the SDK with new BMV + connection pool
const PUT_OBJECT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

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
    /// Type of S3 bucket targeted by this operation.
    // TODO(vnext): unify bucket representation (name + kind) across operations.
    #[allow(dead_code)]
    bucket_type: BucketType,
    /// Notified when CreateMPU completes (success or failure)
    create_mpu_complete: tokio::sync::Notify,
    /// Stored result for handle to retrieve
    result: Mutex<Option<UploadOutput>>,
}

impl UploadTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        bucket_type: BucketType,
        request: UploadInput,
        stream: InputStream,
    ) -> Self {
        // TODO: For unknown content length (streaming uploads), this will need adjustment.
        let content_length = stream
            .size_hint()
            .upper()
            .expect("content_length required; unknown length not yet supported");

        ctx.set_total_bytes(content_length);

        let inner = Arc::new(UploadTransferInner {
            ctx,
            state: Mutex::new(UploadState::PendingInit {
                stream: Some(stream),
                content_length: Some(content_length),
                init_in_flight: false,
            }),
            request: Arc::new(request),
            bucket_type,
            create_mpu_complete: tokio::sync::Notify::new(),
            result: Mutex::new(None),
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
        let state = self.inner.state.lock().expect("lock poisoned");
        match &*state {
            UploadState::Transferring { upload_id, .. } => Some(upload_id.clone()),
            UploadState::Completing { upload_id, .. } => upload_id.clone(),
            _ => None,
        }
    }

    /// Take the stored result (used by handle after completion).
    pub(crate) fn take_result(&self) -> Option<UploadOutput> {
        self.inner.result.lock().expect("lock poisoned").take()
    }

    /// Check if CreateMPU is currently in flight.
    pub(crate) fn is_create_mpu_in_flight(&self) -> bool {
        let state = self.inner.state.lock().expect("lock poisoned");
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
    /// - `PollWork::Pending` - waiting for in-flight work to complete
    /// - `PollWork::Done` - transfer complete
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().expect("lock poisoned");

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

                let use_mpu = stream.as_ref().is_some_and(|s| s.is_mpu_only())
                    || content_length.unwrap_or(0) >= self.inner.ctx.handle.mpu_threshold_bytes();
                if use_mpu {
                    *init_in_flight = true;
                    PollWork::Ready(IoRequest {
                        data: Some(Box::new(UploadWork::CreateMPU)),
                    })
                } else {
                    let taken_stream = stream.take().expect("stream already taken");
                    *state = UploadState::PutObjectInFlight;
                    PollWork::Ready(IoRequest {
                        data: Some(Box::new(UploadWork::PutObject {
                            stream: Some(taken_stream),
                        })),
                    })
                }
            }
            UploadState::Transferring {
                parts_dispatched,
                total_parts,
                parts_in_flight,
                ..
            } => {
                if *parts_dispatched >= *total_parts {
                    if *parts_in_flight > 0 {
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }
                    // All parts sent and completed — transition to Completing
                    if let UploadState::Transferring {
                        upload_id,
                        part_reader,
                        completed_parts,
                        response_builder,
                        ..
                    } = std::mem::replace(&mut *state, UploadState::Done)
                    {
                        *state = UploadState::Completing {
                            upload_id: Some(upload_id),
                            part_reader: Some(part_reader),
                            completed_parts: Some(completed_parts),
                            response_builder: Some(response_builder),
                            complete_in_flight: false,
                        };
                    }
                    drop(state);
                    self.inner.ctx.try_wake();
                    return PollWork::Pending;
                }
                *parts_dispatched += 1;
                *parts_in_flight += 1;
                PollWork::Ready(IoRequest {
                    data: Some(Box::new(UploadWork::UploadPart)),
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
                    data: Some(Box::new(UploadWork::CompleteMPU)),
                })
            }
            UploadState::PutObjectInFlight => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            UploadState::Done => PollWork::Done,
        }
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<UploadWork>();
        match data {
            UploadWork::CreateMPU => self.execute_create_mpu().await,
            UploadWork::UploadPart => self.execute_upload_part().await,
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
            .customize()
            .config_override(crate::retry::bucket_partition_override(
                self.inner.request.bucket(),
            ))
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
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::PendingInit {
                    stream,
                    content_length,
                    ..
                } => (
                    stream.take().expect("stream already taken"),
                    content_length.take().expect("content_length already taken"),
                ),
                _ => panic!("unexpected state for create_mpu"),
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
                .direct_io(self.inner.ctx.handle.runtime.components().direct_io())
                .metrics(std::sync::Arc::clone(&self.inner.ctx.metrics))
                .telemetry(std::sync::Arc::clone(&self.inner.ctx.handle.telemetry))
                .build()
            {
                Ok(reader) => reader,
                Err(e) => return self.fail(e.into()),
            },
        );

        {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            *state = UploadState::Transferring {
                upload_id,
                part_reader,
                parts_dispatched: 0,
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

        WorkOutcome::Success { data: None }
    }

    async fn execute_upload_part(&self) -> WorkOutcome {
        let part_reader = {
            let state = self.inner.state.lock().expect("lock poisoned");
            match &*state {
                UploadState::Transferring { part_reader, .. } => part_reader.clone(),
                _ => panic!("unexpected state for read_part"),
            }
        };

        let data = match part_reader
            .next_part()
            .instrument(tracing::debug_span!("read-upload-body"))
            .await
        {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::trace!("part_reader exhausted");
                self.maybe_transition_to_completing();
                return WorkOutcome::Success { data: None };
            }
            Err(e) => return self.fail(e.into()),
        };

        // 2. Send part over network
        let upload_id = {
            let state = self.inner.state.lock().expect("lock poisoned");
            match &*state {
                UploadState::Transferring { upload_id, .. } => upload_id.clone(),
                _ => panic!("unexpected state for send_part"),
            }
        };

        let part_number = data.part_number;
        let part_num_i32 = part_number as i32;
        let content_length = data.data.remaining() as i64;
        let bytes_sent = content_length as u64;

        let data_bytes = data.data;
        let checksum = data.checksum;

        let send_latencies = &self.inner.ctx.handle.telemetry.send_latencies;
        // Retry the latency deadline (stragglers) and transient transport errors.
        // The SDK normally retries the UploadPart dispatch over the rewindable
        // in-memory body, but under a concurrent ENOBUFS-style burst its shared
        // retry token bucket can exhaust, surfacing a transient dispatch failure
        // un-recovered. This outer loop re-issues those with full-jittered
        // backoff so the re-issue lands after in-flight parts complete and
        // refill the quota. Throttling/503 is NOT retried here — that is the
        // SDK token bucket's job and a TM re-issue would amplify the storm.
        let backoff = crate::retry::Backoff::transient();
        let result = crate::retry::retry_guarded(
            send_latencies,
            |ge, retry_index| crate::retry::classify_upload_part_retry(ge, retry_index, &backoff),
            || {
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
                        .config_override(crate::retry::bucket_partition_override(
                            self.inner.request.bucket(),
                        ))
                        .disable_payload_signing()
                        .send()
                        .instrument(tracing::debug_span!("send-upload-part", part_number))
                        .await
                        .map_err(crate::error::Error::from)
                }
            },
        )
        .await;
        let resp = match result {
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
            let mut state = self.inner.state.lock().expect("lock poisoned");
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

        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_tx: bytes_sent,
            ..Default::default()
        });

        self.maybe_transition_to_completing();

        WorkOutcome::Success { data: None }
    }

    fn maybe_transition_to_completing(&self) {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        let should_complete = if let UploadState::Transferring {
            parts_in_flight,
            parts_dispatched,
            total_parts,
            ..
        } = &mut *state
        {
            *parts_in_flight -= 1;
            *parts_dispatched >= *total_parts && *parts_in_flight == 0
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
                    upload_id: Some(upload_id),
                    part_reader: Some(part_reader),
                    completed_parts: Some(completed_parts),
                    response_builder: Some(response_builder),
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

        let content_length = stream
            .size_hint()
            .upper()
            .expect("content length must be known for PutObject");

        let is_file_backed = stream.is_file_backed();
        let direct_io = self.inner.ctx.handle.runtime.components().direct_io();

        // Hand the request body off to the SDK as a retryable `SdkBody`:
        // in-memory sources ride `SdkBody::from(Bytes)`'s built-in rebuild
        // path; file-backed sources go through our `DirectFileBody` or
        // `OffloadedFileBody` so that (a) retries get a fresh file
        // descriptor and read cursor, (b) the read path stays on the TM's
        // own I/O machinery rather than the SDK's `FsBuilder` +
        // `tokio::fs` path, and (c) peak memory is bounded by the chunk
        // size regardless of how large the object is.
        let sdk_body = stream.into_sdk_body(direct_io);
        let byte_stream = ByteStream::new(sdk_body);

        let put_req = copy_fields_to_put_object_request(
            &self.inner.request,
            self.inner.ctx.s3_client().put_object().body(byte_stream),
        );

        // Per-operation `read_timeout` caps time-to-first-response-byte.
        // Only `read_timeout` is overridden; caller-provided connect /
        // operation / operation-attempt timeouts remain intact.
        let timeout_cfg = TimeoutConfig::builder()
            .read_timeout(PUT_OBJECT_READ_TIMEOUT)
            .build();
        let mut config_override =
            aws_sdk_s3::config::Builder::default().timeout_config(timeout_cfg);
        if let Some(bucket) = self.inner.request.bucket() {
            config_override =
                config_override.retry_partition(crate::retry::bucket_retry_partition(bucket));
        }

        // Per-call SDK telemetry: latency + error attribution.
        let transfer_id = self.inner.ctx.id;
        let send_start = std::time::Instant::now();
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            content_length,
            is_file_backed,
            "put_object.send_enter",
        );

        let resp = match put_req
            .customize()
            .config_override(config_override)
            .disable_payload_signing()
            .send()
            .instrument(tracing::debug_span!("send-put-object"))
            .await
        {
            Ok(resp) => {
                tracing::debug!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %transfer_id,
                    elapsed_ms = send_start.elapsed().as_millis() as u64,
                    "put_object.send_exit_ok",
                );
                resp
            }
            Err(e) => {
                tracing::debug!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %transfer_id,
                    elapsed_ms = send_start.elapsed().as_millis() as u64,
                    error = %e,
                    "put_object.send_exit_err",
                );
                return self.fail(e.into());
            }
        };

        let result = UploadOutputBuilder::from(resp)
            .metrics(self.inner.ctx.metrics())
            .build()
            .expect("valid response");

        *self.inner.result.lock().expect("lock poisoned") = Some(result);

        // A successful response means the SDK fully read the body and
        // the bytes made it to the wire. For file-backed sources, the
        // body implementations above have therefore read `content_length`
        // bytes from disk. We attribute the metric here (rather than
        // per-chunk inside the body) so that there is a single semantic
        // anchor — "recorded when the SDK confirms success" — consistent
        // with how `network_tx` is attributed.
        let disk_read = if is_file_backed { content_length } else { 0 };
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_tx: content_length,
            disk_read,
            ..Default::default()
        });

        self.inner.ctx.set_completed();
        self.inner.ctx.signal_terminal();

        WorkOutcome::Success { data: None }
    }

    async fn execute_complete_mpu(&self) -> WorkOutcome {
        let (upload_id, mut completed_parts, response_builder, part_reader) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::Completing {
                    upload_id,
                    completed_parts,
                    response_builder,
                    part_reader,
                    ..
                } => (
                    upload_id.take().expect("upload_id already taken"),
                    completed_parts
                        .take()
                        .expect("completed_parts already taken"),
                    response_builder
                        .take()
                        .expect("response_builder already taken"),
                    part_reader.take().expect("part_reader already taken"),
                ),
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
            .customize()
            .config_override(crate::retry::bucket_partition_override(
                self.inner.request.bucket(),
            ))
            .send()
            .instrument(tracing::debug_span!("send-complete-multipart-upload"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };

        let result = response_builder
            .update_from_complete_mpu(&resp)
            .metrics(self.inner.ctx.metrics())
            .build()
            .expect("valid response");

        *self.inner.result.lock().expect("lock poisoned") = Some(result);
        self.inner.ctx.set_completed();
        self.inner.ctx.signal_terminal();

        WorkOutcome::Success { data: None }
    }

    fn fail(&self, error: Error) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            %error,
            "upload failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        self.inner.ctx.set_failed(error);
        self.inner.ctx.signal_terminal();
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
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    fn create_test_transfer(s3_client: aws_sdk_s3::Client, content: Vec<u8>) -> UploadTransfer {
        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder().client(s3_client).build(),
        );

        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let stream = InputStream::from(content);

        let (ctx, _completion_rx) = TransferContext::new(handle);
        UploadTransfer::new(ctx, BucketType::Standard, input, stream)
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

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_poll_work_initial_state_returns_create_mpu() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::CreateMPU));
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_poll_work_pending_while_init_in_flight() {
        let s3_client = mock_client!(aws_sdk_s3, []);
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_poll_work_generates_parts_after_create_mpu() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());
        transfer.execute(&mut work).await;

        let mut work1 = assert_ready(transfer.poll_work());
        let data1 = work1.data_mut::<UploadWork>();
        assert!(matches!(data1, UploadWork::UploadPart));

        let mut work2 = assert_ready(transfer.poll_work());
        let data2 = work2.data_mut::<UploadWork>();
        assert!(matches!(data2, UploadWork::UploadPart));

        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_execute_create_mpu_transitions_to_transferring() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());

        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Success { .. }));

        let mut next = assert_ready(transfer.poll_work());
        let data = next.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::UploadPart));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_execute_full_mpu_flow() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        // 1. CreateMPU
        let mut work = assert_ready(transfer.poll_work());
        transfer.execute(&mut work).await;

        // 2. UploadPart 1 (read+send in one call)
        let mut work = assert_ready(transfer.poll_work());
        transfer.execute(&mut work).await;

        // 3. UploadPart 2 (read+send in one call)
        let mut work = assert_ready(transfer.poll_work());
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
        let result = transfer.take_result();
        assert!(result.is_some());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_basic_upload_object() {
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        // Small content below MPU threshold (default 8MB)
        let content = vec![0u8; 1024];
        let transfer = create_test_transfer(s3_client, content);

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::PutObject { .. }));

        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Success { .. }));

        assert!(transfer.take_result().is_some());

        // PutObject must record network_tx so the adaptive controller sees throughput
        assert!(
            !transfer.ctx().handle.telemetry.io_counters.is_idle(),
            "PutObject should record network_tx to IOCounters"
        );
    }

    /// Regression: when the upload source is a file (`InputStream::from_path`),
    /// the PutObject code path must record `disk_read` equal to the payload
    /// size. Previously only `network_tx` was recorded, leaving the plural
    /// `upload_objects` metrics' `disk_read` at zero for all small-file
    /// children.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_put_object_records_disk_read_for_file_source() {
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        use std::io::Write;

        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let payload = vec![0u8; 1024];
        tmp.write_all(&payload).unwrap();
        tmp.flush().unwrap();

        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder().client(s3_client).build(),
        );
        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let stream = InputStream::from_path(tmp.path()).unwrap();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = UploadTransfer::new(ctx, BucketType::Standard, input, stream);

        let mut work = assert_ready(transfer.poll_work());
        assert!(matches!(
            work.data_mut::<UploadWork>(),
            UploadWork::PutObject { .. }
        ));
        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Success { .. }));

        let metrics = transfer.ctx().metrics();
        assert_eq!(
            payload.len() as u64,
            metrics.network_tx,
            "network_tx should equal the file size"
        );
        assert_eq!(
            payload.len() as u64,
            metrics.disk_read,
            "disk_read should equal the file size for a file-backed PutObject"
        );
    }

    /// Complement to the file-backed test: in-memory (`RawInputStream::Buf`)
    /// uploads must NOT inflate `disk_read`, since no bytes were read from
    /// disk.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_put_object_does_not_record_disk_read_for_memory_source() {
        use aws_sdk_s3::operation::put_object::PutObjectOutput;

        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]);
        let content = vec![0u8; 1024];
        let transfer = create_test_transfer(s3_client, content.clone());

        let mut work = assert_ready(transfer.poll_work());
        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Success { .. }));

        let metrics = transfer.ctx().metrics();
        assert_eq!(content.len() as u64, metrics.network_tx);
        assert_eq!(
            0, metrics.disk_read,
            "in-memory source must not report disk_read"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_basic_mpu() {
        let s3_client = mock_s3_client_for_mpu();
        let content = vec![0u8; 16 * 1024 * 1024];
        let transfer = create_test_transfer(s3_client, content);

        // CreateMPU
        let mut work = assert_ready(transfer.poll_work());
        assert!(matches!(
            work.data_mut::<UploadWork>(),
            UploadWork::CreateMPU
        ));
        transfer.execute(&mut work).await;

        // Upload parts
        let mut work1 = assert_ready(transfer.poll_work());
        transfer.execute(&mut work1).await;
        let mut work2 = assert_ready(transfer.poll_work());
        transfer.execute(&mut work2).await;

        // CompleteMPU
        let mut work = assert_ready(transfer.poll_work());
        assert!(matches!(
            work.data_mut::<UploadWork>(),
            UploadWork::CompleteMPU
        ));
        transfer.execute(&mut work).await;

        assert!(transfer.take_result().is_some());
    }
}
