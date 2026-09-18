/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload transfer implementation for scheduler integration.
//!
//! # Failure handling
//!
//! Upload requests carry no adaptive latency deadline (timing the whole
//! part-send is size-blind and false-cancels large parts on slow links). The
//! bounds on a failing or stuck upload are:
//!
//! - **Transient transport** (connection IO error, client-side timeout,
//!   ENOBUFS-style dispatch failure): the SDK retries, and this module's outer
//!   [`retry`](crate::retry::retry) loop re-issues past a drained shared retry
//!   token bucket with a fast backoff.
//! - **Throttling** (503 `SlowDown`): the SDK's retry token bucket handles it
//!   first; when that bucket drains under a high fan-out the outer
//!   [`retry`](crate::retry::retry) loop re-issues with a hard throttle backoff,
//!   bounded by the loop's attempt budget. The per-bucket token bucket
//!   ([`bucket_retry_partition`](crate::retry::bucket_retry_partition)) is given
//!   a time-based refill so a drained budget recovers.
//! - **Mid-upload-body stall** (the peer stops making progress on the request
//!   body): stalled-stream protection ([`Handle::upload_override`](crate::client::Handle::upload_override)).
//! - **Response never arrives after the body is fully sent**: NOT bounded. This
//!   is the response-first-byte gap — stalled-stream protection watches
//!   request-body throughput, which has already completed, and the SDK sets no
//!   response/operation timeout. Bounding it needs a response-first-byte timeout
//!   measured from send completion (a signal the SDK does not currently expose).

use std::cmp;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use tracing::Instrument;

use crate::error::{Error, ErrorKind};
use crate::io::part_reader::{Builder as PartReaderBuilder, PartReadStart, PartReader};
use crate::io::{InputStream, PartData};
use crate::operation::upload::context::{
    PartPlan, PartReadWake, PartTransferState, PendingPartRead, UploadPartWork, UploadState,
};
use crate::operation::upload::diagnostics::{
    self, PartTransferSnapshot, PartTransferTransition, UploadDiagnosticTimer,
    UploadTransferDiagnostics,
};
use crate::operation::upload::input::convert::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::part_body;
use crate::operation::upload::{UploadInput, UploadOutput, UploadOutputBuilder};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::BucketType;

/// Upload-specific work data.
#[derive(Debug)]
pub(crate) enum UploadWork {
    CreateMPU,
    UploadPart(UploadPartWork),
    CompleteMPU,
    PutObject { stream: Option<InputStream> },
}

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

/// Initial capacity for the completed-part list when the content length is unknown.
///
/// There is no part count to size it by, so this only avoids a few early reallocations; the list
/// grows as parts complete. This matches the CRT reference's default.
const UNKNOWN_LENGTH_DEFAULT_NUM_PARTS: usize = 32;

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
        let size_hint = stream.size_hint();

        // Only equal bounds are a known total. A bounded stream reports no total while in progress;
        // its actual size is published after EOF.
        if size_hint.upper() == Some(size_hint.lower()) {
            ctx.set_total_bytes(size_hint.lower());
        }

        let inner = Arc::new(UploadTransferInner {
            ctx,
            state: Mutex::new(UploadState::PendingInit {
                stream: Some(stream),
                size_hint,
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

    fn emit_part_pipeline(
        &self,
        transition: PartTransferTransition,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        diagnostics::emit_pipeline_transition(self.inner.ctx.id, transition, snapshot);
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
    /// - `PollWork::Ready { .. }` - work available to execute
    /// - `PollWork::Pending` - waiting for in-flight work to complete
    /// - `PollWork::Done` - transfer complete
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().expect("lock poisoned");

        loop {
            match &mut *state {
                UploadState::PendingInit {
                    init_in_flight,
                    size_hint,
                    stream,
                } => {
                    if *init_in_flight {
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }

                    let use_mpu = match size_hint.upper() {
                        None => true,
                        Some(upper) => {
                            stream.as_ref().is_some_and(|stream| stream.is_mpu_only())
                                || upper >= self.inner.ctx.handle.mpu_threshold_bytes()
                        }
                    };
                    return if use_mpu {
                        *init_in_flight = true;
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::CreateMPU)),
                        })
                    } else {
                        let stream = stream.take().expect("stream already taken");
                        *state = UploadState::PutObjectInFlight;
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::PutObject {
                                stream: Some(stream),
                            })),
                        })
                    };
                }
                UploadState::Transferring { parts, .. } => {
                    if let Some(work) = parts.schedule_part() {
                        let transition = if work.is_resumed() {
                            PartTransferTransition::SourceWakeScheduled
                        } else {
                            PartTransferTransition::NewPartScheduled
                        };
                        self.emit_part_pipeline(transition, parts.transition_snapshot());
                        return PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::UploadPart(work))),
                        });
                    }
                    let snapshot = parts.transition_snapshot();
                    let pending_reason = parts.pending_reason();
                    if parts.is_complete() {
                        self.emit_part_pipeline(PartTransferTransition::CompletionReady, snapshot);
                    }
                    if try_begin_completing(&mut state) {
                        continue;
                    }
                    self.emit_part_pipeline(
                        PartTransferTransition::Pending(pending_reason),
                        snapshot,
                    );
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }
                UploadState::Completing {
                    complete_in_flight, ..
                } => {
                    if *complete_in_flight {
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }
                    *complete_in_flight = true;
                    return PollWork::ready(IoRequest {
                        data: Some(Box::new(UploadWork::CompleteMPU)),
                    });
                }
                UploadState::PutObjectInFlight => {
                    self.inner.ctx.set_pending();
                    return PollWork::Pending;
                }
                UploadState::Done => return PollWork::Done,
            }
        }
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<UploadWork>();
        match data {
            UploadWork::CreateMPU => self.execute_create_mpu().await,
            UploadWork::UploadPart(work) => self.execute_upload_part(work).await,
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

        let transfer_diagnostics = self.inner.ctx.handle.config.diagnostics().transfer();
        let create_timer = UploadDiagnosticTimer::start(transfer_diagnostics);
        let resp = match mpu_req
            .customize()
            .config_override(
                self.inner
                    .ctx
                    .handle
                    .bucket_partition_override(self.inner.request.bucket()),
            )
            .send()
            .instrument(tracing::debug_span!("send-create-multipart-upload"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };
        let create_elapsed = create_timer.elapsed();

        let upload_id = resp.upload_id().expect("upload_id present").to_string();
        let response_builder = UploadOutputBuilder::from(resp);

        let (stream, size_hint) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::PendingInit {
                    stream, size_hint, ..
                } => (stream.take().expect("stream already taken"), *size_hint),
                _ => panic!("unexpected state for create_mpu"),
            }
        };
        let streaming = stream.is_mpu_only();

        // An upper bound can size parts so a manager-produced body fits S3's part limit. A stream
        // without one uses the configured size and enforces the limit as parts are produced.
        let part_size = match size_hint.upper() {
            Some(upper) => cmp::max(
                self.inner.ctx.handle.upload_part_size_bytes(),
                upper.div_ceil(MAX_PARTS),
            ),
            None => self.inner.ctx.handle.upload_part_size_bytes(),
        };
        let plan = if streaming {
            PartPlan::Streaming {
                lower: size_hint.lower(),
                upper: size_hint.upper(),
            }
        } else {
            let expected_size = size_hint
                .upper()
                .expect("byte and file streams have an exact size");
            debug_assert_eq!(size_hint.lower(), expected_size);
            PartPlan::Fixed {
                total_parts: expected_size.div_ceil(part_size),
                expected_size,
            }
        };

        tracing::trace!("upload request using multipart upload with part size: {part_size} bytes");

        let part_reader = Arc::new(
            match PartReaderBuilder::new()
                .stream(stream)
                .part_size(part_size.try_into().expect("valid part size"))
                .direct_io(self.inner.ctx.handle.runtime.components().direct_io())
                .buffer_pool(self.inner.ctx.handle.buffer_pool.clone())
                .metrics(std::sync::Arc::clone(&self.inner.ctx.metrics))
                .telemetry(std::sync::Arc::clone(&self.inner.ctx.handle.telemetry))
                .build()
            {
                Ok(reader) => reader,
                Err(e) => return self.fail(e.into()),
            },
        );

        let completed_parts_capacity = match &plan {
            PartPlan::Fixed { total_parts, .. } => *total_parts as usize,
            PartPlan::Streaming {
                upper: Some(upper), ..
            } => upper.div_ceil(part_size) as usize,
            PartPlan::Streaming { upper: None, .. } => UNKNOWN_LENGTH_DEFAULT_NUM_PARTS,
        };
        {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            *state = UploadState::Transferring {
                upload_id,
                parts: PartTransferState::new(
                    part_reader,
                    plan,
                    completed_parts_capacity,
                    UploadTransferDiagnostics::new(transfer_diagnostics, create_elapsed),
                ),
                response_builder,
            };
        }

        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            total_parts = size_hint.upper().map(|upper| upper.div_ceil(part_size)),
            part_size,
            "MPU created, transferring",
        );

        WorkOutcome::Success { data: None }
    }

    async fn execute_upload_part(&self, work: &mut UploadPartWork) -> WorkOutcome {
        let (part_reader, streaming) = {
            let state = self.inner.state.lock().expect("lock poisoned");
            match &*state {
                UploadState::Transferring { parts, .. } => {
                    (Arc::clone(&parts.part_reader), parts.is_streaming())
                }
                _ => panic!("unexpected state for read_part"),
            }
        };

        let (mut future, wake, mut timing) = match work.take_pending_read() {
            Some(read) => (read.future, read.wake, read.timing),
            None => {
                let future = match part_reader
                    .start_part_read()
                    .instrument(tracing::debug_span!("read-upload-body"))
                    .await
                {
                    PartReadStart::Ready(future) => future,
                    PartReadStart::Blocked => {
                        let mut state = self.inner.state.lock().expect("lock poisoned");
                        let UploadState::Transferring { parts, .. } = &mut *state else {
                            panic!("unexpected state while retracting upload part");
                        };
                        parts.retract_scheduled_part();
                        let snapshot = parts.transition_snapshot();
                        drop(state);
                        self.emit_part_pipeline(
                            PartTransferTransition::CustomSourceBlocked,
                            snapshot,
                        );
                        return WorkOutcome::Yielded;
                    }
                    PartReadStart::Finished => {
                        self.finish_read_without_part();
                        return WorkOutcome::Yielded;
                    }
                };
                (
                    future,
                    PartReadWake::new(self.inner.ctx.scheduler_waker()),
                    work.take_timing(),
                )
            }
        };

        let result = {
            let task_waker = std::task::Waker::from(Arc::clone(&wake));
            let mut task_context = Context::from_waker(&task_waker);
            Pin::new(&mut future).poll(&mut task_context)
        };
        let data = match result {
            Poll::Pending => {
                let mut state = self.inner.state.lock().expect("lock poisoned");
                let UploadState::Transferring { parts, .. } = &mut *state else {
                    panic!("unexpected state while parking upload part read");
                };
                parts.record_read_pending(&mut timing);
                let parked_wake = Arc::clone(&wake);
                let observation = timing.observation();
                parts.park_read(PendingPartRead {
                    future,
                    wake,
                    timing,
                });
                let snapshot = parts.transition_snapshot();
                drop(state);
                diagnostics::emit_source_pending(self.inner.ctx.id, observation, snapshot);
                parked_wake.requeue_if_notified();
                return WorkOutcome::Yielded;
            }
            Poll::Ready(Ok(Some(data))) => data,
            Poll::Ready(Ok(None)) => {
                tracing::trace!("part_reader exhausted");
                return self.on_end_of_stream(&part_reader, timing).await;
            }
            Poll::Ready(Err(error)) => return self.fail(error.into()),
        };

        {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &mut *state else {
                panic!("unexpected state while recording upload part");
            };
            if let Err(error) = parts.observe_part(data.data.len() as u64) {
                drop(state);
                return self.fail(crate::error::invalid_input(error));
            }
        }

        // A custom source that was blocked is available again. Refill the work withheld while the
        // retained source operation was pending so reading the next part can overlap this upload.
        self.inner.ctx.try_wake();

        // Guard S3's part-count ceiling by parts the source actually yielded, not speculative
        // dispatches. Fail before sending part 10,001 so the error can name the remedy.
        if streaming && part_reader.parts_yielded() > MAX_PARTS {
            return self.fail(Error::new(
                ErrorKind::InputInvalid,
                format!(
                    "stream exceeds the maximum of {MAX_PARTS} parts at the current part size; \
                     configure a larger part size to upload an object this large without a known \
                     content length"
                ),
            ));
        }

        self.send_part(data, timing).await
    }

    /// Records source end-of-stream and synthesizes the empty part S3 requires when necessary.
    ///
    /// Exactly one caller owns empty-stream synthesis because [`PartTransferState`] records the
    /// first end-of-stream observation. A declared length never takes this path: inventing a part
    /// would contradict the source's independent size declaration.
    async fn on_end_of_stream(
        &self,
        part_reader: &PartReader,
        timing: diagnostics::UploadPartTiming,
    ) -> WorkOutcome {
        let empty_and_owned = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &mut *state else {
                panic!("unexpected state at upload end-of-stream");
            };
            match parts.observe_end_of_stream(part_reader.parts_yielded()) {
                Ok(empty_and_owned) => empty_and_owned,
                Err(error) => {
                    drop(state);
                    return self.fail(crate::error::invalid_input(error));
                }
            }
        };
        // End-of-stream may close dispatch while `poll_work` is parked behind the source gate.
        self.inner.ctx.try_wake();

        if empty_and_owned {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                "empty unknown-length stream; uploading a single empty part",
            );
            return self.send_part(PartData::new(1, Bytes::new()), timing).await;
        }

        self.finish_read_without_part();
        WorkOutcome::Success { data: None }
    }

    /// Uploads one part and records it for CompleteMultipartUpload.
    ///
    /// This is shared by ordinary source output and the synthesized empty part, keeping request
    /// retries, accounting, and completion transitions identical.
    async fn send_part(
        &self,
        data: PartData,
        timing: diagnostics::UploadPartTiming,
    ) -> WorkOutcome {
        let part_number = data.part_number;
        let presentation_segments = data.data.segment_count();
        let content_length = data.data.len() as i64;
        let bytes_sent = content_length as u64;

        let (upload_id, source_observation, request_start_snapshot) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::Transferring {
                    upload_id, parts, ..
                } => {
                    let source_observation = parts.begin_upload(presentation_segments, timing);
                    (
                        upload_id.clone(),
                        source_observation,
                        parts.transition_snapshot(),
                    )
                }
                _ => panic!("unexpected state for send_part"),
            }
        };

        let part_num_i32 = part_number as i32;
        diagnostics::emit_part_started(
            self.inner.ctx.id,
            part_number,
            bytes_sent,
            presentation_segments,
            source_observation,
            request_start_snapshot,
        );

        let sdk_body = part_body::sdk_body(data.data);
        let checksum = data.checksum;

        // Retry transient transport errors and throttles (the classifier picks
        // the backoff: fast for transient, hard for a throttle). The SDK normally
        // retries an UploadPart dispatch over the rewindable in-memory body, but a
        // concurrent ENOBUFS-style burst or a throttle storm can exhaust its
        // shared retry token bucket and surface parts un-recovered; this outer
        // loop re-issues those, landing after in-flight parts refill the quota.
        //
        // No adaptive latency deadline on the upload path: timing the whole
        // part-send (body-push + response) is size-blind and false-cancels large
        // parts on slow links. Stalled-stream protection (`upload_override`)
        // bounds a mid-upload-body stall; a response that never arrives after the
        // body is fully sent is not bounded here (see the module docs on the
        // response-first-byte gap).
        let request_timer =
            UploadDiagnosticTimer::start(self.inner.ctx.handle.config.diagnostics().transfer());
        let result = crate::retry::retry(crate::retry::classify_upload_part_retry, |_hedge| {
            let body = sdk_body
                .try_clone()
                .expect("UploadPart SdkBody must be retryable");
            let req = copy_fields_to_upload_part_request(
                &self.inner.request,
                self.inner
                    .ctx
                    .s3_client()
                    .upload_part()
                    .upload_id(&upload_id)
                    .part_number(part_num_i32)
                    .content_length(content_length)
                    .body(ByteStream::new(body)),
                checksum.as_ref(),
            );
            async move {
                req.customize()
                    .config_override(
                        self.inner
                            .ctx
                            .handle
                            .upload_override(self.inner.request.bucket()),
                    )
                    .disable_payload_signing()
                    .send()
                    .instrument(tracing::debug_span!("send-upload-part", part_number))
                    .await
                    .map_err(|e| crate::retry::GuardError::Inner(crate::error::Error::from(e)))
            }
        })
        .instrument(tracing::debug_span!(
            target: crate::telemetry::TARGET_TRANSFER,
            "upload-part",
            tid = %self.inner.ctx.id,
            part_number
        ))
        .await;
        let request_elapsed = request_timer.elapsed();
        let resp = match result {
            Ok(resp) => resp,
            Err(e) => {
                let snapshot = {
                    let state = self.inner.state.lock().expect("lock poisoned");
                    let UploadState::Transferring { parts, .. } = &*state else {
                        panic!("unexpected state while reporting failed upload part");
                    };
                    parts.transition_snapshot()
                };
                diagnostics::emit_part_failed(
                    self.inner.ctx.id,
                    part_number,
                    bytes_sent,
                    presentation_segments,
                    request_elapsed,
                    snapshot,
                );
                return self.fail(e);
            }
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

        let (should_wake, completion_snapshot) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &mut *state else {
                panic!("unexpected state while completing upload part");
            };
            parts.complete_part(completed, bytes_sent, request_elapsed);
            let snapshot = parts.transition_snapshot();
            (try_begin_completing(&mut state), snapshot)
        };

        diagnostics::emit_part_completed(
            self.inner.ctx.id,
            part_number,
            bytes_sent,
            presentation_segments,
            request_elapsed,
            completion_snapshot,
        );

        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_tx: bytes_sent,
            ..Default::default()
        });

        if should_wake {
            self.inner.ctx.try_wake();
        }

        WorkOutcome::Success { data: None }
    }

    /// Retires scheduled source work that produced no UploadPart request.
    fn finish_read_without_part(&self) {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        let UploadState::Transferring { parts, .. } = &mut *state else {
            panic!("unexpected state while completing empty upload read");
        };
        parts.finish_read_without_part();
        if try_begin_completing(&mut state) {
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
        // in-memory sources ride `SdkBody::from(Bytes)`'s built-in rebuild path;
        // file-backed sources go through `DirectFileBody` / `OffloadedFileBody`
        // (one stable file identity, a fresh cursor per retry, and pooled
        // bounded chunks).
        // The body must stay a native `SdkBody` (not a custom wrapper) so the SDK
        // keeps its in-memory checksum path — wrapping would force aws-chunked
        // trailer encoding and change the checksum framing.
        let sdk_body =
            match stream.into_sdk_body(direct_io, self.inner.ctx.handle.buffer_pool.clone()) {
                Ok(body) => body,
                Err(error) => return self.fail(error.into()),
            };

        let transfer_id = self.inner.ctx.id;
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            content_length,
            is_file_backed,
            "put_object.send_enter",
        );

        // Retry transient transport and throttles (same rationale as UploadPart).
        // No adaptive latency deadline; a mid-upload-body stall is bounded by
        // stalled-stream protection (`upload_override`), a post-send response-wait
        // is not (see the module docs on the response-first-byte gap).
        let result = crate::retry::retry(crate::retry::classify_upload_part_retry, |_hedge| {
            let body = sdk_body
                .try_clone()
                .expect("PutObject SdkBody must be retryable");
            let put_req = copy_fields_to_put_object_request(
                &self.inner.request,
                self.inner
                    .ctx
                    .s3_client()
                    .put_object()
                    .body(ByteStream::new(body)),
            );
            async move {
                put_req
                    .customize()
                    .config_override(
                        self.inner
                            .ctx
                            .handle
                            .upload_override(self.inner.request.bucket()),
                    )
                    .disable_payload_signing()
                    .send()
                    .instrument(tracing::debug_span!("send-put-object"))
                    .await
                    .map_err(|e| crate::retry::GuardError::Inner(crate::error::Error::from(e)))
            }
        })
        .instrument(tracing::debug_span!(
            target: crate::telemetry::TARGET_TRANSFER,
            "put-object",
            tid = %transfer_id
        ))
        .await;
        let resp = match result {
            Ok(resp) => {
                tracing::debug!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %transfer_id,
                    "put_object.send_exit_ok",
                );
                resp
            }
            Err(e) => {
                tracing::debug!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %transfer_id,
                    error = %e,
                    "put_object.send_exit_err",
                );
                return self.fail(e);
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
        let transfer_diagnostics = self.inner.ctx.handle.config.diagnostics().transfer();
        let completion_timer = UploadDiagnosticTimer::start(transfer_diagnostics);
        let (upload_id, response_builder, parts) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::Completing {
                    upload_id,
                    response_builder,
                    parts,
                    ..
                } => (
                    upload_id.take().expect("upload_id already taken"),
                    response_builder
                        .take()
                        .expect("response_builder already taken"),
                    parts.take().expect("part transfer state already taken"),
                ),
                _ => panic!("unexpected state for complete_mpu"),
            }
        };

        let (part_reader, plan, mut completed_parts, bytes_uploaded, final_snapshot, diagnostics) =
            parts.into_completion();
        completed_parts.sort_by_key(|p| p.part_number);

        if let Err(error) = plan.validate_complete(bytes_uploaded) {
            return self.fail(crate::error::invalid_input(error));
        }
        let object_size = bytes_uploaded;
        self.inner.ctx.set_total_bytes(object_size);

        let base_req = self
            .inner
            .ctx
            .s3_client()
            .complete_multipart_upload()
            .upload_id(&upload_id)
            .mpu_object_size(object_size as i64)
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

        let request_timer = UploadDiagnosticTimer::start(transfer_diagnostics);
        let resp = match complete_req
            .customize()
            .config_override(
                self.inner
                    .ctx
                    .handle
                    .bucket_partition_override(self.inner.request.bucket()),
            )
            .send()
            .instrument(tracing::debug_span!("send-complete-multipart-upload"))
            .await
        {
            Ok(resp) => resp,
            Err(e) => return self.fail(e.into()),
        };
        let summary = diagnostics.finish(
            final_snapshot,
            request_timer.elapsed(),
            completion_timer.elapsed(),
        );
        diagnostics::emit_summary(self.inner.ctx.id, summary);

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

/// Moves a fully drained multipart transfer into its completion phase.
///
/// The state replacement elects exactly one caller even when source EOF and a network completion
/// race to retire the final work item.
fn try_begin_completing(state: &mut UploadState) -> bool {
    let ready = matches!(
        state,
        UploadState::Transferring { parts, .. } if parts.is_complete()
    );
    if !ready {
        return false;
    }

    let UploadState::Transferring {
        upload_id,
        parts,
        response_builder,
    } = std::mem::replace(state, UploadState::Done)
    else {
        unreachable!("completion readiness changed under lock");
    };
    *state = UploadState::Completing {
        upload_id: Some(upload_id),
        parts: Some(parts),
        response_builder: Some(response_builder),
        complete_in_flight: false,
    };
    true
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
        create_test_transfer_with_stream(s3_client, InputStream::from(content))
    }

    fn create_test_transfer_with_stream(
        s3_client: aws_sdk_s3::Client,
        stream: InputStream,
    ) -> UploadTransfer {
        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder()
                .client(s3_client)
                .diagnostics_for_test(crate::config::MemoryDiagnosticsConfig::default(), 2)
                .build(),
        );

        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

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

    #[derive(Debug)]
    struct BlockingPartStream {
        ready: Arc<std::sync::atomic::AtomicBool>,
        source_waker: Arc<Mutex<Option<std::task::Waker>>>,
        yielded: bool,
    }

    impl crate::io::PartStream for BlockingPartStream {
        fn poll_part(
            mut self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            _stream_cx: &crate::io::StreamContext,
        ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
            if self.yielded {
                return std::task::Poll::Ready(None);
            }
            if self.ready.load(std::sync::atomic::Ordering::Acquire) {
                self.yielded = true;
                return std::task::Poll::Ready(Some(Ok(PartData::new(
                    1,
                    Bytes::from_static(b"ready"),
                ))));
            }
            *self.source_waker.lock().expect("source waker poisoned") = Some(cx.waker().clone());
            std::task::Poll::Pending
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::exact(32 * 1024 * 1024)
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_poll_work_unknown_length_routes_to_multipart() {
        #[derive(Debug)]
        struct EmptyUnknown;

        impl crate::io::PartStream for EmptyUnknown {
            fn poll_part(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _stream_cx: &crate::io::StreamContext,
            ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
                std::task::Poll::Ready(None)
            }

            fn size_hint(&self) -> crate::io::SizeHint {
                crate::io::SizeHint::default()
            }
        }

        let transfer = create_test_transfer_with_stream(
            mock_client!(aws_sdk_s3, []),
            InputStream::from_part_stream(EmptyUnknown),
        );
        let mut work = assert_ready(transfer.poll_work());
        assert!(matches!(
            work.data_mut::<UploadWork>(),
            UploadWork::CreateMPU
        ));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn empty_unknown_length_stream_uploads_one_empty_part() {
        #[derive(Debug)]
        struct EmptyUnknown;

        impl crate::io::PartStream for EmptyUnknown {
            fn poll_part(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _stream_cx: &crate::io::StreamContext,
            ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
                std::task::Poll::Ready(None)
            }

            fn size_hint(&self) -> crate::io::SizeHint {
                crate::io::SizeHint::default()
            }
        }

        let transfer = create_test_transfer_with_stream(
            mock_s3_client_for_mpu(),
            InputStream::from_part_stream(EmptyUnknown),
        );

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));

        let mut upload = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut upload).await,
            WorkOutcome::Success { .. }
        ));

        let mut complete = assert_ready(transfer.poll_work());
        assert!(matches!(
            complete.data_mut::<UploadWork>(),
            UploadWork::CompleteMPU
        ));
        assert!(matches!(
            transfer.execute(&mut complete).await,
            WorkOutcome::Success { .. }
        ));

        crate::scheduler::test_util::assert_done(transfer.poll_work());
        assert!(transfer.take_result().is_some());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn blocked_custom_source_retains_one_read_and_retracts_waiters() {
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let source_waker = Arc::new(Mutex::new(None));
        let stream = BlockingPartStream {
            ready: Arc::clone(&ready),
            source_waker: Arc::clone(&source_waker),
            yielded: false,
        };
        let transfer = create_test_transfer_with_stream(
            mock_s3_client_for_mpu(),
            InputStream::from_part_stream(stream),
        );

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));

        let mut first = assert_ready(transfer.poll_work());
        let mut second = assert_ready(transfer.poll_work());
        let mut third = assert_ready(transfer.poll_work());

        assert!(matches!(
            transfer.execute(&mut first).await,
            WorkOutcome::Yielded
        ));
        {
            let state = transfer.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &*state else {
                panic!("upload left transferring state");
            };
            assert_eq!(parts.test_counts(), (3, 3, 1));
            assert_eq!(
                parts.snapshot(),
                PartTransferSnapshot {
                    parts_dispatched: 3,
                    parts_in_flight: 3,
                    uploads_in_flight: 0,
                    pending_reads: 1,
                    completed_parts: 0,
                    bytes_uploaded: 0,
                    eof: false,
                    dispatch_closed: false,
                }
            );
        }

        assert!(matches!(
            transfer.execute(&mut second).await,
            WorkOutcome::Yielded
        ));
        assert!(matches!(
            transfer.execute(&mut third).await,
            WorkOutcome::Yielded
        ));
        {
            let state = transfer.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &*state else {
                panic!("upload left transferring state");
            };
            assert_eq!(parts.test_counts(), (1, 1, 1));
            let summary = parts.test_summary();
            assert_eq!(summary.read_pending_polls, 1);
            assert_eq!(summary.read_pending_parts, 0);
        }
        assert_pending(transfer.poll_work());

        ready.store(true, std::sync::atomic::Ordering::Release);
        source_waker
            .lock()
            .expect("source waker poisoned")
            .take()
            .expect("source did not register a waker")
            .wake();

        let mut resumed = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut resumed).await,
            WorkOutcome::Success { .. }
        ));
        {
            let state = transfer.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &*state else {
                panic!("upload left transferring state");
            };
            assert_eq!(parts.test_counts(), (1, 0, 0));
            let summary = parts.test_summary();
            assert_eq!(summary.snapshot.completed_parts, 1);
            assert_eq!(summary.snapshot.bytes_uploaded, 5);
            assert_eq!(summary.read_pending_polls, 1);
            assert_eq!(summary.read_pending_parts, 1);
            assert_eq!(summary.presentation_segments, 1);
            assert_eq!(summary.max_presentation_segments, 1);
        }

        let mut next = assert_ready(transfer.poll_work());
        assert!(matches!(
            next.data_mut::<UploadWork>(),
            UploadWork::UploadPart(_)
        ));
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
        assert!(matches!(data1, UploadWork::UploadPart(_)));

        let mut work2 = assert_ready(transfer.poll_work());
        let data2 = work2.data_mut::<UploadWork>();
        assert!(matches!(data2, UploadWork::UploadPart(_)));

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
        assert!(matches!(data, UploadWork::UploadPart(_)));
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

        {
            let state = transfer.inner.state.lock().expect("lock poisoned");
            let UploadState::Completing {
                parts: Some(parts), ..
            } = &*state
            else {
                panic!("upload did not enter completing state");
            };
            let summary = parts.test_summary();
            assert_eq!(summary.snapshot.completed_parts, 2);
            assert_eq!(summary.snapshot.parts_in_flight, 0);
            assert_eq!(summary.snapshot.uploads_in_flight, 0);
            assert_eq!(summary.snapshot.pending_reads, 0);
            assert_eq!(summary.segmented_parts, 0);
            assert_eq!(summary.presentation_segments, 2);
            assert_eq!(summary.max_presentation_segments, 1);
            assert_eq!(summary.read_pending_polls, 0);
            assert_eq!(summary.read_pending_parts, 0);
        }

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
