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
use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::{Buf, Bytes};
use tracing::Instrument;

use std::future::Future;
use std::pin::Pin;

use crate::error::{Error, ErrorKind};
use crate::io::part_reader::{Builder as PartReaderBuilder, PartReader};
use crate::io::{InputStream, PartData};
use crate::operation::upload::context::{PartPlan, UploadState};
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

/// Initial capacity for the completed-part list when the content length is unknown.
///
/// There is no part count to size it by, so this only avoids a few early reallocations; the list
/// grows as parts complete. Matches the CRT reference's default, which is likewise "arbitrary
/// picked to avoid using allocations and using too much memory".
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
        // `None` for a `PartStream` whose total size is not known up front. Such a source is always
        // `is_mpu_only`, so it routes through the multipart path and its parts are dispatched
        // speculatively until the reader reports end-of-stream (see `PartPlan`).
        let content_length = stream.size_hint().upper();

        // Only meaningful when the length is known; an unknown-length transfer reports
        // `total_bytes: None` while in progress and sets it at end-of-stream.
        if let Some(content_length) = content_length {
            ctx.set_total_bytes(content_length);
        }

        let inner = Arc::new(UploadTransferInner {
            ctx,
            state: Mutex::new(UploadState::PendingInit {
                stream: Some(stream),
                content_length,
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
                    content_length,
                    stream,
                } => {
                    if *init_in_flight {
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }

                    // The single-request path requires a content length, so a source without one has
                    // to go multipart regardless of what the threshold comparison would say.
                    let use_mpu = match *content_length {
                        None => true,
                        Some(len) => {
                            stream.as_ref().is_some_and(|s| s.is_mpu_only())
                                || len >= self.inner.ctx.handle.mpu_threshold_bytes()
                        }
                    };
                    return if use_mpu {
                        *init_in_flight = true;
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::CreateMPU)),
                        })
                    } else {
                        let taken_stream = stream.take().expect("stream already taken");
                        *state = UploadState::PutObjectInFlight;
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::PutObject {
                                stream: Some(taken_stream),
                            })),
                        })
                    };
                }
                UploadState::Transferring {
                    parts_dispatched,
                    plan,
                    eof,
                    parts_in_flight,
                    ..
                } => {
                    if plan.all_dispatched(*parts_dispatched, *eof) {
                        // Last one out: transition now and serve the `CompleteMPU` it unlocks on
                        // this same poll, rather than parking for a re-poll.
                        if try_begin_completing(&mut state) {
                            continue;
                        }
                        // Parts are still in flight; `on_part_completed` wakes this poll.
                        self.inner.ctx.set_pending();
                        return PollWork::Pending;
                    }

                    *parts_dispatched += 1;
                    *parts_in_flight += 1;
                    return PollWork::ready(IoRequest {
                        data: Some(Box::new(UploadWork::UploadPart)),
                    });
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
                    content_length.take(),
                ),
                _ => panic!("unexpected state for create_mpu"),
            }
        };

        // With a known length the part size is bumped so the part count fits within `MAX_PARTS`.
        // With an unknown length there is no total to bump against, so the configured size is used
        // as-is and the object is bounded by `part_size * MAX_PARTS` (guarded in `poll_work`).
        let part_size = match content_length {
            Some(content_length) => cmp::max(
                self.inner.ctx.handle.upload_part_size_bytes(),
                content_length.div_ceil(MAX_PARTS),
            ),
            None => self.inner.ctx.handle.upload_part_size_bytes(),
        };

        let plan = match content_length {
            Some(content_length) => PartPlan::Known {
                total_parts: content_length.div_ceil(part_size),
                declared_object_size: content_length,
            },
            None => PartPlan::Unknown,
        };

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

        let completed_parts_capacity = match &plan {
            PartPlan::Known { total_parts, .. } => *total_parts as usize,
            PartPlan::Unknown => UNKNOWN_LENGTH_DEFAULT_NUM_PARTS,
        };

        {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            *state = UploadState::Transferring {
                upload_id,
                part_reader,
                parts_dispatched: 0,
                plan,
                eof: false,
                parts_in_flight: 0,
                completed_parts: Vec::with_capacity(completed_parts_capacity),
                response_builder,
                bytes_uploaded: 0,
            };
        }

        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            total_parts = content_length.map(|len| len.div_ceil(part_size)),
            part_size,
            "MPU created, transferring",
        );

        WorkOutcome::Success { data: None }
    }

    async fn execute_upload_part(&self) -> WorkOutcome {
        let (part_reader, is_unknown) = {
            let state = self.inner.state.lock().expect("lock poisoned");
            match &*state {
                UploadState::Transferring {
                    part_reader, plan, ..
                } => (part_reader.clone(), matches!(plan, PartPlan::Unknown)),
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
                return self.on_end_of_stream(&part_reader).await;
            }
            Err(e) => return self.fail(e.into()),
        };

        // Guard the S3 part-count ceiling by the number of parts the reader has actually yielded,
        // not by speculative dispatches. Fail before sending the (MAX_PARTS + 1)-th so the caller
        // gets an error naming the remedy instead of S3's opaque `InvalidArgument` on part 10001.
        if is_unknown && part_reader.parts_yielded() > MAX_PARTS {
            return self.fail(Error::new(
                ErrorKind::InputInvalid,
                format!(
                    "stream exceeds the maximum of {MAX_PARTS} parts at the current part size; \
                     configure a larger part size to upload an object this large without a known \
                     content length"
                ),
            ));
        }

        self.send_part(data).await
    }

    /// Handle an unknown-length reader reaching end-of-stream: mark `eof`, and if the stream turned
    /// out empty, synthesize the one zero-length part S3 requires (a multipart upload must list at
    /// least one part).
    ///
    /// `parts_yielded()` is final at end-of-stream (see its docs for the ordering guarantee), so
    /// `== 0` is an exact emptiness test. Exactly one caller synthesizes: only the one that flips
    /// `eof` false→true takes that branch.
    async fn on_end_of_stream(&self, part_reader: &PartReader) -> WorkOutcome {
        let empty_and_owned = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::Transferring { eof, plan, .. } if !*eof => {
                    *eof = true;
                    // Gated on `Unknown`: a declared length already carries a part count, so a
                    // source that declares a size and then delivers nothing must not have a part
                    // invented for it — that would contradict the size it declared.
                    matches!(plan, PartPlan::Unknown) && part_reader.parts_yielded() == 0
                }
                _ => false,
            }
        };
        // `eof` may unblock a `poll_work` parked on in-flight parts, and it is set here in `execute`
        // while the poller may already be parked — so signal the edge or the transfer hangs (see the
        // wake protocol on `TransferContext::set_pending`).
        self.inner.ctx.try_wake();

        if empty_and_owned {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                "empty unknown-length stream; uploading a single empty part",
            );
            return self.send_part(PartData::new(1, Bytes::new())).await;
        }

        self.on_part_completed();
        WorkOutcome::Success { data: None }
    }

    /// Upload one part and record it, then advance the state machine.
    ///
    /// Shared by the ordinary read-a-part path and the synthesized empty part 1 (see
    /// [`Self::on_end_of_stream`]), so both go through the same retry classification, metrics, and
    /// completion handoff.
    async fn send_part(&self, data: PartData) -> WorkOutcome {
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
        let result = crate::retry::retry(crate::retry::classify_upload_part_retry, |_hedge| {
            let req = copy_fields_to_upload_part_request(
                &self.inner.request,
                self.inner
                    .ctx
                    .s3_client()
                    .upload_part()
                    .upload_id(&upload_id)
                    .part_number(part_num_i32)
                    .content_length(content_length)
                    .body(ByteStream::from(data_bytes.clone())),
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
                completed_parts,
                bytes_uploaded,
                ..
            } = &mut *state
            {
                completed_parts.push(completed);
                *bytes_uploaded += bytes_sent;
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

        self.on_part_completed();

        WorkOutcome::Success { data: None }
    }

    /// Release the slot a dispatched `UploadPart` held, and hand off to `Completing` if it was the
    /// last one out.
    ///
    /// Fires for every completed `UploadPart` dispatch, including one whose read only found
    /// end-of-stream. This is the mutator side of the wake protocol: the state that could unblock a
    /// parked `poll_work` is mutated under the lock, then the lock is dropped before signalling.
    fn on_part_completed(&self) {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        if let UploadState::Transferring {
            parts_in_flight, ..
        } = &mut *state
        {
            *parts_in_flight -= 1;
        }
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

        // `poll_work` sends every source without a declared length to the multipart path, so reaching
        // here means the length is known.
        let content_length = stream
            .size_hint()
            .upper()
            .expect("content length must be known for PutObject");

        let is_file_backed = stream.is_file_backed();
        let direct_io = self.inner.ctx.handle.runtime.components().direct_io();

        // Hand the request body off to the SDK as a retryable `SdkBody`:
        // in-memory sources ride `SdkBody::from(Bytes)`'s built-in rebuild path;
        // file-backed sources go through `DirectFileBody` / `OffloadedFileBody`
        // (fresh fd + cursor per retry, TM I/O machinery, bounded peak memory).
        // The body must stay a native `SdkBody` (not a custom wrapper) so the SDK
        // keeps its in-memory checksum path — wrapping would force aws-chunked
        // trailer encoding and change the checksum framing.
        let sdk_body = stream.into_sdk_body(direct_io);

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
                    .map_err(|e| crate::retry::GuardError::Inner(e.into()))
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
        let (upload_id, mut completed_parts, response_builder, part_reader, plan, bytes_uploaded) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::Completing {
                    upload_id,
                    completed_parts,
                    response_builder,
                    part_reader,
                    plan,
                    bytes_uploaded,
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
                    std::mem::replace(plan, PartPlan::Unknown),
                    *bytes_uploaded,
                ),
                _ => panic!("unexpected state for complete_mpu"),
            }
        };

        completed_parts.sort_by_key(|p| p.part_number);

        // S3 sums the parts it assembled and rejects CompleteMPU when the total doesn't match the
        // `MpuObjectSize` sent, so a dropped or duplicated part fails the complete instead of
        // silently producing a wrong-sized object (SEP step 7). Sending the source's declared size on
        // the known path keeps that check independent of our own part accounting; the unknown path has
        // no declared size, so its check can only catch an S3-side assembly mismatch.
        let object_size = match plan {
            PartPlan::Known {
                declared_object_size,
                ..
            } => {
                // `declared_object_size` is `size_hint().upper()` — a bound, so under-delivery is
                // legal input and only over-delivery implies we over-counted.
                debug_assert!(
                    bytes_uploaded <= declared_object_size,
                    "uploaded {bytes_uploaded} bytes, more than the declared upper bound of \
                     {declared_object_size}; part accounting over-counted"
                );
                declared_object_size
            }
            PartPlan::Unknown => bytes_uploaded,
        };

        // An unknown-length transfer could not report a total while in flight (progress is genuinely
        // unknowable there). By now the stream has ended and `object_size` is exact, so final
        // metrics report the true size. `set_total_bytes` is a `OnceLock` set, so this is a no-op
        // when the length was known up front.
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

/// If every part has been dispatched and none remain in flight, move `Transferring` → `Completing`
/// and report that it did.
///
/// Returns `true` to exactly one caller: the `mem::replace` below makes that call the sole owner of
/// the moved-out fields, so completion is set up once even when a poll and a part completion race to
/// be last one out.
fn try_begin_completing(state: &mut UploadState) -> bool {
    let ready = matches!(
        state,
        UploadState::Transferring { parts_dispatched, plan, eof, parts_in_flight, .. }
            if plan.all_dispatched(*parts_dispatched, *eof) && *parts_in_flight == 0
    );
    if !ready {
        return false;
    }

    if let UploadState::Transferring {
        upload_id,
        part_reader,
        completed_parts,
        response_builder,
        plan,
        bytes_uploaded,
        ..
    } = std::mem::replace(state, UploadState::Done)
    {
        *state = UploadState::Completing {
            upload_id: Some(upload_id),
            part_reader: Some(part_reader),
            completed_parts: Some(completed_parts),
            response_builder: Some(response_builder),
            complete_in_flight: false,
            plan,
            bytes_uploaded,
        };
    }
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

    /// A source with no declared length is routed to the multipart path, which is the only one that
    /// can serve it. The stream is below the multipart threshold, so a threshold comparison against a
    /// defaulted-to-zero length would pick the single-request path instead.
    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_poll_work_unknown_length_routes_to_multipart() {
        struct NoUpperBound;
        impl crate::io::PartStream for NoUpperBound {
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

        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder()
                .client(mock_client!(aws_sdk_s3, []))
                .build(),
        );
        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = UploadTransfer::new(
            ctx,
            BucketType::Standard,
            input,
            InputStream::from_part_stream(NoUpperBound),
        );

        let mut work = assert_ready(transfer.poll_work());
        assert!(
            matches!(work.data_mut::<UploadWork>(), UploadWork::CreateMPU),
            "an unknown-length source must not be routed to the single-request path"
        );
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
