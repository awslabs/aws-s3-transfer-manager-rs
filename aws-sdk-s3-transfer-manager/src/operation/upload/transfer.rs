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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::error::{Error, ErrorKind};
use crate::io::part_reader::{Builder as PartReaderBuilder, PartReadStart};
use crate::io::{InputStream, PartData};
use crate::operation::upload::context::{
    apply_content_length, validate_distinct_part_numbers, validate_part_number, validate_size_hint,
    AbortMpuProgress, MultipartCompletion, PartPlan, PartReadWake, PartTransferState,
    PendingPartRead, UploadFailure, UploadPartWork, UploadState,
};
#[cfg(test)]
use crate::operation::upload::observability::UploadTerminalOutcome;
use crate::operation::upload::observability::{
    UploadDiagnosticTimer, UploadEvent, UploadExecutionState, UploadMode, UploadObservability,
    UploadPartTiming, UploadRequestKind, UploadStateSnapshot, UploadTerminalReport,
};
use crate::operation::upload::part_body;
use crate::operation::upload::request::{
    copy_fields_to_mpu_request, copy_fields_to_upload_part_request,
};
use crate::operation::upload::{UploadInput, UploadOutput, UploadOutputBuilder};
use crate::transfer::{IoRequest, PendingCause, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::{AbortOutcome, AbortedUpload, BucketType, FailedMultipartUploadPolicy};

/// Upload-specific work data.
///
/// Every variant carries an `InFlight` guard, which counts the item in
/// `UploadTransferInner::in_flight` until it is dropped.
#[derive(Debug)]
pub(crate) enum UploadWork {
    CreateMPU(InFlight),
    UploadPart(UploadPartWork, InFlight),
    CompleteMPU(Box<CompleteMpuWork>, InFlight),
    PutObject { stream: InputStream, live: InFlight },
    AbortMPU { upload_id: String, live: InFlight },
}

/// Payload for CompleteMultipartUpload, moved out of `UploadState::Completing` when the request is
/// dispatched.
#[derive(Debug)]
pub(crate) struct CompleteMpuWork {
    parts: PartTransferState,
    response_builder: UploadOutputBuilder,
}

/// Counts one emitted work item in `UploadTransferInner::in_flight` until the
/// item is dropped: after `execute` finishes it, when a cancellation drops it
/// mid-request, or when the scheduler discards it unexecuted.
///
/// The transfer does not signal terminal while the count is above zero (see
/// `UploadTransfer::signal_terminal_when_drained`), so `join()`/`abort()` never
/// return while one of our requests is still on the wire or still holds its
/// source read or buffer.
pub(crate) struct InFlight {
    inner: Arc<UploadTransferInner>,
}

impl InFlight {
    /// Counts one work item. Must be called under the state lock, so a
    /// `poll_work` that reads the count under the lock never misses the item.
    fn new(inner: &Arc<UploadTransferInner>) -> Self {
        inner.in_flight.fetch_add(1, Ordering::SeqCst);
        Self {
            inner: Arc::clone(inner),
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let prev = self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
        if prev != 1 {
            return;
        }
        // The last item to go signals terminal if a request handler has
        // already ended the transfer.
        if !self.inner.ctx.is_active() {
            self.inner.ctx.signal_terminal();
        } else if self.inner.abort_requested.load(Ordering::SeqCst) {
            // Aborting state waits for the count to reach zero before it
            // ends the transfer. Singal the context to continue as the
            // count dropped to zero
            self.inner.ctx.wake();
        }
    }
}

impl std::fmt::Debug for InFlight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InFlight")
    }
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
    /// Optional upload-wide event and summary observation.
    observability: UploadObservability,
    /// The original request (body taken for processing)
    request: Arc<UploadInput>,
    /// Type of S3 bucket targeted by this operation.
    // TODO(vnext): unify bucket representation (name + kind) across operations.
    #[allow(dead_code)]
    bucket_type: BucketType,
    /// What S3 reported, for the handle to read once the transfer has ended.
    /// Taken alone, never while holding the state lock.
    result: Mutex<Option<UploadResult>>,
    /// Whether an abort has been requested, by the caller or by a failure.
    /// Set once and never cleared.
    abort_requested: AtomicBool,
    /// Interrupts in-flight UploadPart (with its part reads) and PutObject.
    ///
    /// TODO: this should be moved away and let the scheduler to know which
    /// work can be interrupt or not.
    abort_parts_token: CancellationToken,
    /// Work items emitted and not yet dropped (see `InFlight`).
    in_flight: AtomicUsize,
}

/// What S3 reported for an upload.
///
/// When Complete commits while AbortMultipartUpload is on the wire, both can
/// succeed. `Completed` then wins, in either order: the object exists.
#[derive(Debug)]
pub(crate) enum UploadResult {
    /// CompleteMultipartUpload or PutObject committed the object.
    Completed(UploadOutput),
    /// AbortMultipartUpload succeeded (upload ID and request charged).
    Aborted(AbortedUpload),
}

impl UploadTransfer {
    pub(crate) fn try_new(
        ctx: TransferContext,
        bucket_type: BucketType,
        request: UploadInput,
        stream: InputStream,
    ) -> Result<Self, Error> {
        let size_hint = stream.size_hint();
        validate_size_hint(size_hint).map_err(crate::error::invalid_input)?;
        // A declared content length replaces the body's bounds, so the size checks while reading
        // and at completion hold the body to exactly that many bytes.
        let size_hint = apply_content_length(size_hint, request.content_length())
            .map_err(crate::error::invalid_input)?;
        let observability =
            UploadObservability::new(ctx.handle.config.diagnostics().transfer(), size_hint);

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
            observability,
            request: Arc::new(request),
            bucket_type,
            result: Mutex::new(None),
            abort_requested: AtomicBool::new(false),
            abort_parts_token: CancellationToken::new(),
            in_flight: AtomicUsize::new(0),
        });

        Ok(Self { inner })
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    fn observe_event(&self, event: UploadEvent, state: &UploadState) {
        let snapshot = snapshot_state(state);
        self.inner
            .observability
            .observe_event(self.inner.ctx.id, event, snapshot);
    }

    /// Emits the upload terminal summary before notifying the owning handle.
    ///
    /// `state_snapshot` is the state the transfer ended in. Must be called
    /// without the state lock held: the report takes the observability lock
    /// and emits events.
    fn report_terminal(&self, state_snapshot: UploadStateSnapshot) {
        self.inner.ctx.finalize_terminal_metrics();
        if !self.inner.ctx.claim_terminal_report() {
            return;
        }
        let pending = self.inner.ctx.pending_stats().unwrap_or_default();
        let _ = self
            .inner
            .observability
            .report_terminal(UploadTerminalReport {
                transfer_id: self.inner.ctx.id,
                status: self.inner.ctx.transfer_status(),
                error_kind: self.inner.ctx.error_kind(),
                metrics: self.inner.ctx.metrics(),
                request_total: self.inner.ctx.metrics.request_metrics(),
                pending,
                state_snapshot,
            });
    }

    /// Take what S3 reported (used by the handle once the transfer has ended).
    /// `None` if nothing committed and no AbortMultipartUpload succeeded.
    pub(crate) fn take_result(&self) -> Option<UploadResult> {
        self.inner.result.lock().expect("lock poisoned").take()
    }

    /// Starts a soft abort for the caller's `abort()`. Idempotent.
    pub(crate) fn request_abort(&self) {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.enter_aborting(state, None);
        self.signal_abort();
    }

    /// Interrupts parts and PutObject and wakes the transfer so `poll_work`
    /// drives `Aborting` to an end. Called after `enter_aborting`.
    fn signal_abort(&self) {
        self.inner.abort_requested.store(true, Ordering::SeqCst);
        self.inner.abort_parts_token.cancel();
        self.inner.ctx.wake();
    }

    /// Logs a multipart upload that the transfer ended without completing or
    /// aborting, so the caller can abort it.
    fn warn_upload_left_open(&self, upload_id: &str) {
        tracing::warn!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %self.inner.ctx.id,
            upload_id = %upload_id,
            "upload ended before completing; the multipart upload may still be open"
        );
    }

    /// Whether `Aborting` should send AbortMultipartUpload once it has an ID.
    fn abort_mpu_policy(&self) -> AbortMpuProgress {
        match self
            .inner
            .request
            .failed_multipart_upload_policy
            .clone()
            .unwrap_or_default()
        {
            FailedMultipartUploadPolicy::Retain => AbortMpuProgress::NotNeeded,
            // `_` also covers variants added later (the enum is non_exhaustive).
            _ => AbortMpuProgress::ToSend,
        }
    }

    /// Signals terminal if no work item is left; otherwise the last
    /// `InFlight` guard to drop signals it.
    fn signal_terminal_when_drained(&self) {
        if self.inner.in_flight.load(Ordering::SeqCst) == 0 {
            self.inner.ctx.signal_terminal();
        }
    }

    /// Sets the state to `Done` and returns its snapshot, in one lock.
    fn set_done(&self) -> UploadStateSnapshot {
        let mut state = self.inner.state.lock().expect("lock poisoned");
        *state = UploadState::Done;
        snapshot_state(&state)
    }

    /// Ends a transfer in `Aborting`: as failed with the recorded failure if
    /// there is one, otherwise as cancelled.
    ///
    /// `state` is the held state lock; it is set to `Done` and released
    /// before the terminal report.
    fn end_aborting(&self, mut state: MutexGuard<'_, UploadState>) -> PollWork {
        let UploadState::Aborting {
            upload_id,
            abort_mpu,
            failure,
            ..
        } = std::mem::replace(&mut *state, UploadState::Done)
        else {
            unreachable!("end_aborting called outside Aborting");
        };
        let snapshot = match &failure {
            Some(failure) => failure.snapshot,
            None => snapshot_state(&state),
        };
        drop(state);
        match failure {
            Some(failure) => {
                self.inner.ctx.set_failed(failure.error);
            }
            None => {
                // An upload ID with no AbortMultipartUpload to send means the
                // policy is `Retain`: report the open upload to the caller.
                // Stored before the terminal signal that `abort()` waits on.
                if let (Some(upload_id), AbortMpuProgress::NotNeeded) = (upload_id, abort_mpu) {
                    self.inner
                        .result
                        .lock()
                        .expect("lock poisoned")
                        .get_or_insert(UploadResult::Aborted(AbortedUpload {
                            upload_id: Some(upload_id),
                            outcome: AbortOutcome::Retained,
                            ..Default::default()
                        }));
                }
                self.inner.ctx.set_cancelled();
            }
        }
        self.report_terminal(snapshot);
        self.inner.ctx.signal_terminal();
        PollWork::Done
    }

    /// Moves the upload to `Aborting`, keeping the upload ID and which of
    /// CreateMultipartUpload and CompleteMultipartUpload are on the wire. The
    /// body and part state are dropped.
    ///
    /// `failure` becomes the transfer's outcome unless an earlier failure was
    /// recorded, and its request is no longer waited for. An upload already
    /// `Aborting` only records the failure. `Done`, or a transfer that has
    /// ended, is left as it is and the failure is dropped, logging an upload
    /// ID the failed request carries.
    ///
    /// The previous state is dropped after `state` is unlocked. It can own the
    /// caller's `PartStream`, whose `Drop` must not run under the lock.
    fn enter_aborting(
        &self,
        mut state: MutexGuard<'_, UploadState>,
        failure: Option<(UploadFailure, FailedRequest)>,
    ) {
        if !self.inner.ctx.is_active() {
            drop(state);
            // `on_terminal` drops the state. A CreateMultipartUpload that
            // failed after the transfer ended can carry the only copy of the
            // upload ID.
            if let Some((
                _,
                FailedRequest::CreateMpu {
                    upload_id: Some(upload_id),
                },
            )) = &failure
            {
                self.warn_upload_left_open(upload_id);
            }
            return;
        }
        let aborting = |upload_id, create_in_flight, complete_mpu_in_flight, abort_mpu| {
            UploadState::Aborting {
                upload_id,
                create_in_flight,
                complete_mpu_in_flight,
                abort_mpu,
                failure: None,
            }
        };
        let next = match &*state {
            // Create is on the wire and is not interrupted: if S3 accepted it,
            // there is an upload to abort, and only its response carries the
            // ID. The Create handler fills `upload_id` in when it returns.
            UploadState::PendingInit {
                init_in_flight: true,
                ..
            } => Some(aborting(None, true, false, self.abort_mpu_policy())),
            // Nothing was sent, so nothing exists on S3.
            UploadState::PendingInit {
                init_in_flight: false,
                ..
            } => Some(aborting(None, false, false, AbortMpuProgress::NotNeeded)),
            // Parts were interrupted by `abort_parts_token`. Their results no
            // longer matter, so the part state (reader, parked reads,
            // completed list) is dropped. From `Completing`, Complete has not
            // been sent and now never will be: the object cannot exist.
            UploadState::Transferring { upload_id, .. }
            | UploadState::Completing { upload_id, .. } => Some(aborting(
                Some(upload_id.clone()),
                false,
                false,
                self.abort_mpu_policy(),
            )),
            // Complete is (or is about to be) on the wire and is not
            // interrupted. Abort the upload right away, so that if S3 has not
            // committed yet the abort wins, and let Complete's response
            // decide the outcome. S3 accepts the abort whether or not Complete
            // committed.
            UploadState::CompleteInFlight { upload_id } => Some(aborting(
                Some(upload_id.clone()),
                false,
                true,
                self.abort_mpu_policy(),
            )),
            // PutObject is interrupted like a part (see `execute`), not waited
            // for: there is no upload to abort, and the caller gets `Cancelled`
            // even though S3 may already have stored the object. The
            // `Aborting` arm still waits for the dropped request to be freed
            // (its `InFlight` count).
            UploadState::PutObjectInFlight => {
                Some(aborting(None, false, false, AbortMpuProgress::NotNeeded))
            }
            UploadState::Aborting { .. } | UploadState::Done => None,
        };
        // Dropped once `state` is unlocked.
        let previous = next.map(|next| std::mem::replace(&mut *state, next));
        if let (
            Some((failure, failed)),
            UploadState::Aborting {
                upload_id,
                create_in_flight,
                complete_mpu_in_flight,
                abort_mpu,
                failure: recorded,
            },
        ) = (failure, &mut *state)
        {
            // The first failure is the one reported.
            if recorded.is_none() {
                *recorded = Some(failure);
            }
            // The failed request is still marked as on the wire, whether
            // `Aborting` was entered above or earlier.
            match failed {
                FailedRequest::Other => {}
                FailedRequest::CreateMpu { upload_id: id } => {
                    *create_in_flight = false;
                    if id.is_some() {
                        *upload_id = id;
                    }
                }
                FailedRequest::CompleteMpu => *complete_mpu_in_flight = false,
                FailedRequest::AbortMpu => *abort_mpu = AbortMpuProgress::Failed,
            }
        }
        drop(state);
        drop(previous);
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
                        self.inner
                            .ctx
                            .set_pending(PendingCause::in_flight_work("create_multipart_upload"));
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
                        self.inner.observability.select_mode(UploadMode::Multipart);
                        self.inner.observability.observe_event(
                            self.inner.ctx.id,
                            UploadEvent::MultipartUploadScheduled,
                            UploadStateSnapshot::inactive(
                                UploadExecutionState::CreateMultipartUploadInFlight,
                            ),
                        );
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::CreateMPU(InFlight::new(&self.inner)))),
                        })
                    } else {
                        let stream = stream.take().expect("stream already taken");
                        *state = UploadState::PutObjectInFlight;
                        self.inner.observability.select_mode(UploadMode::PutObject);
                        self.observe_event(UploadEvent::PutObjectScheduled, &state);
                        PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::PutObject {
                                stream,
                                live: InFlight::new(&self.inner),
                            })),
                        })
                    };
                }
                UploadState::Transferring { parts, .. } => {
                    if let Some(work) = parts.schedule_part() {
                        let event = if work.is_resumed() {
                            UploadEvent::SourceWakeScheduled
                        } else {
                            UploadEvent::NewPartScheduled
                        };
                        self.inner.observability.observe_event(
                            self.inner.ctx.id,
                            event,
                            parts.snapshot(),
                        );
                        return PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::UploadPart(
                                work,
                                InFlight::new(&self.inner),
                            ))),
                        });
                    }
                    if let Some(work) = parts.maybe_start_empty_object_part() {
                        self.inner.observability.observe_event(
                            self.inner.ctx.id,
                            UploadEvent::EmptyObjectPartScheduled,
                            parts.snapshot(),
                        );
                        return PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::UploadPart(
                                work,
                                InFlight::new(&self.inner),
                            ))),
                        });
                    }
                    let snapshot = parts.snapshot();
                    let pending_reason = parts.pending_reason();
                    if parts.is_complete() {
                        self.inner.observability.observe_event(
                            self.inner.ctx.id,
                            UploadEvent::CompletionReady,
                            snapshot,
                        );
                    }
                    if try_begin_completing(&mut state) {
                        continue;
                    }
                    self.inner.observability.observe_state(snapshot);
                    self.inner.ctx.set_pending(pending_reason);
                    return PollWork::Pending;
                }
                UploadState::Completing { .. } => {
                    // Move the fields out by value; the placeholder is overwritten before the
                    // state lock is released, so no reader observes it.
                    let UploadState::Completing {
                        upload_id,
                        parts,
                        response_builder,
                    } = std::mem::replace(&mut *state, UploadState::Done)
                    else {
                        unreachable!("state changed under lock");
                    };
                    *state = UploadState::CompleteInFlight { upload_id };
                    return PollWork::ready(IoRequest {
                        data: Some(Box::new(UploadWork::CompleteMPU(
                            Box::new(CompleteMpuWork {
                                parts,
                                response_builder,
                            }),
                            InFlight::new(&self.inner),
                        ))),
                    });
                }
                UploadState::CompleteInFlight { .. } => {
                    self.inner
                        .ctx
                        .set_pending(PendingCause::in_flight_work("complete_multipart_upload"));
                    return PollWork::Pending;
                }
                UploadState::PutObjectInFlight => {
                    self.inner
                        .ctx
                        .set_pending(PendingCause::in_flight_work("put_object"));
                    return PollWork::Pending;
                }
                UploadState::Aborting {
                    upload_id,
                    create_in_flight,
                    complete_mpu_in_flight,
                    abort_mpu,
                    ..
                } => {
                    // An upload exists and AbortMultipartUpload is still to be sent.
                    if let (Some(id), AbortMpuProgress::ToSend) = (upload_id.as_ref(), *abort_mpu) {
                        let upload_id = id.clone();
                        *abort_mpu = AbortMpuProgress::InFlight;
                        return PollWork::ready(IoRequest {
                            data: Some(Box::new(UploadWork::AbortMPU {
                                upload_id,
                                live: InFlight::new(&self.inner),
                            })),
                        });
                    }
                    // Wait while a request whose response decides the outcome,
                    // or any other emitted item, is outstanding.
                    let pending_inflight = match (
                        *create_in_flight,
                        *complete_mpu_in_flight,
                        *abort_mpu == AbortMpuProgress::InFlight,
                    ) {
                        (true, _, _) => Some("create_multipart_upload"),
                        (_, true, _) => Some("complete_multipart_upload_during_abort"),
                        (_, _, true) => Some("abort_multipart_upload"),
                        _ => {
                            if self.inner.in_flight.load(Ordering::SeqCst) > 0 {
                                Some("upload_parts")
                            } else {
                                None
                            }
                        }
                    };
                    if let Some(pending_inflight) = pending_inflight {
                        self.inner
                            .ctx
                            .set_pending(PendingCause::in_flight_work(pending_inflight));
                        return PollWork::Pending;
                    }
                    // Nothing is outstanding, and no commit succeeded (a commit
                    // moves the state to `Done` itself): the upload ends as
                    // failed if a failure was recorded, otherwise as cancelled.
                    return self.end_aborting(state);
                }
                UploadState::Done => return PollWork::Done,
            }
        }
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        // A work item executes once, so execute takes its payload by value.
        match work.take_data::<UploadWork>() {
            // Each arm keeps its `InFlight` guard until the request future is
            // gone, so the count is released only after that.
            UploadWork::CreateMPU(_live) => self.execute_create_mpu().await,
            UploadWork::UploadPart(part, live) => {
                // Race the part against the parts-only abort token; the loser
                // is dropped at its current `.await`. `biased` checks the
                // token first on every wake-up, so once the abort fires a part
                // never acts on a response it got later.
                let token = self.inner.abort_parts_token.clone();
                let outcome = tokio::select! {
                    biased;
                    _ = token.cancelled() => WorkOutcome::Cancelled,
                    outcome = self.execute_upload_part(part) => outcome,
                };
                // The part future is gone by the end of the `let` above, so
                // release the count only now.
                drop(live);
                outcome
            }
            UploadWork::CompleteMPU(completion, _live) => {
                self.execute_complete_mpu(*completion).await
            }
            UploadWork::PutObject { stream, live } => {
                // Same race as UploadPart: an abort drops PutObject wherever
                // it is (body upload, retry backoff or response wait) and the
                // upload ends as `Cancelled`. The object may still have been
                // stored if S3 had the whole body.
                let token = self.inner.abort_parts_token.clone();
                let outcome = tokio::select! {
                    biased;
                    _ = token.cancelled() => WorkOutcome::Cancelled,
                    outcome = self.execute_put_object(stream) => outcome,
                };
                drop(live);
                outcome
            }
            UploadWork::AbortMPU {
                upload_id,
                live: _live,
            } => self.execute_abort_mpu(upload_id).await,
        }
    }

    async fn execute_create_mpu(&self) -> WorkOutcome {
        let outcome = self.do_execute_create_mpu().await;
        // state changed - try to wake if we were pending
        self.inner.ctx.try_wake();
        outcome
    }

    async fn do_execute_create_mpu(&self) -> WorkOutcome {
        let client = self.inner.ctx.s3_client();

        let mpu_req =
            copy_fields_to_mpu_request(&self.inner.request, client.create_multipart_upload());

        let req_metrics = self
            .inner
            .observability
            .start_request(&self.inner.ctx, UploadRequestKind::CreateMultipartUpload);
        let response = mpu_req
            .customize()
            .config_override(
                self.inner
                    .ctx
                    .bucket_partition_override(self.inner.request.bucket()),
            )
            .send()
            .instrument(tracing::debug_span!("send-create-multipart-upload"))
            .await;
        let _ = req_metrics.finish();
        let resp = match response {
            Ok(resp) => resp,
            // Create's response is in, so it is no longer on the wire. No
            // upload was created, so there is nothing to abort.
            Err(e) => {
                let state = self.inner.state.lock().expect("lock poisoned");
                return self.fail(
                    state,
                    e.into(),
                    FailedRequest::CreateMpu { upload_id: None },
                );
            }
        };

        let upload_id = resp.upload_id().expect("upload_id present").to_string();
        let response_builder = crate::sdk_v1::upload_output_from_create_multipart_upload(&resp);

        let (stream, size_hint) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::PendingInit {
                    stream, size_hint, ..
                } => (stream.take().expect("stream already taken"), *size_hint),
                // An abort landed while Create was on the wire. Hand the ID to
                // the abort instead of starting the parts; `execute_create_mpu`
                // wakes the transfer after we return.
                UploadState::Aborting {
                    upload_id: slot,
                    create_in_flight,
                    ..
                } => {
                    *slot = Some(upload_id);
                    *create_in_flight = false;
                    return WorkOutcome::Success { data: None };
                }
                // The transfer ended while Create was on the wire. Nothing
                // aborts the multipart upload it opened.
                UploadState::Done => {
                    drop(state);
                    self.warn_upload_left_open(&upload_id);
                    return WorkOutcome::Cancelled;
                }
                _ => panic!("unexpected state for create_mpu"),
            }
        };
        let reads_until_eof = stream.is_mpu_only();

        // An upper bound can size parts so a manager-produced body fits S3's part limit. A stream
        // without one uses the configured size and enforces the limit as parts are produced.
        let part_size = match size_hint.upper() {
            Some(upper) => cmp::max(
                self.inner.ctx.handle.part_size_bytes(),
                upper.div_ceil(MAX_PARTS),
            ),
            None => self.inner.ctx.handle.part_size_bytes(),
        };
        let plan = if reads_until_eof {
            PartPlan::UntilEof { size_hint }
        } else {
            let declared_object_size = size_hint
                .upper()
                .expect("byte and file streams have an exact size");
            debug_assert_eq!(size_hint.lower(), declared_object_size);
            PartPlan::Known {
                total_parts: declared_object_size.div_ceil(part_size),
                declared_object_size,
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
                // The upload exists but the state does not have its ID yet.
                // Hand it to the abort so AbortMultipartUpload is sent.
                Err(e) => {
                    let state = self.inner.state.lock().expect("lock poisoned");
                    return self.fail(
                        state,
                        e.into(),
                        FailedRequest::CreateMpu {
                            upload_id: Some(upload_id),
                        },
                    );
                }
            },
        );

        let completed_parts_capacity = match &plan {
            PartPlan::Known { total_parts, .. } => *total_parts as usize,
            PartPlan::UntilEof { size_hint } => size_hint
                .upper()
                .map_or(UNKNOWN_LENGTH_DEFAULT_NUM_PARTS, |upper| {
                    upper.div_ceil(part_size) as usize
                }),
        };
        let snapshot = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            // The abort, or the end of the transfer, can also land after the
            // first lock above, while the part reader was being built with the
            // lock released. Same handling; the reader is dropped (closing the
            // source) after the lock is released when we return.
            match &mut *state {
                UploadState::Aborting {
                    upload_id: slot,
                    create_in_flight,
                    ..
                } => {
                    *slot = Some(upload_id);
                    *create_in_flight = false;
                    return WorkOutcome::Success { data: None };
                }
                UploadState::Done => {
                    drop(state);
                    self.warn_upload_left_open(&upload_id);
                    return WorkOutcome::Cancelled;
                }
                _ => {}
            }
            *state = UploadState::Transferring {
                upload_id,
                parts: PartTransferState::new(
                    part_reader,
                    plan,
                    completed_parts_capacity,
                    self.inner.observability.clone(),
                ),
                response_builder,
            };
            snapshot_state(&state)
        };
        self.inner.observability.multipart_created();
        self.inner.observability.observe_event(
            self.inner.ctx.id,
            UploadEvent::MultipartUploadCreated,
            snapshot,
        );

        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            total_parts = size_hint.upper().map(|upper| upper.div_ceil(part_size)),
            part_size,
            "MPU created, transferring",
        );

        WorkOutcome::Success { data: None }
    }

    async fn execute_upload_part(&self, mut work: UploadPartWork) -> WorkOutcome {
        if work.is_empty_object() {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.inner.ctx.id,
                "empty source satisfied its size hint; uploading required zero-byte part",
            );
            return self
                .send_part(PartData::new(1, Bytes::new()), work.take_timing())
                .await;
        }

        let (part_reader, reads_until_eof) = {
            let state = self.inner.state.lock().expect("lock poisoned");
            match &*state {
                UploadState::Transferring { parts, .. } => {
                    (Arc::clone(&parts.part_reader), parts.reads_until_eof())
                }
                other => return abandoned_part(other, "for read_part"),
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
                            return abandoned_part(&state, "while retracting upload part");
                        };
                        parts.retract_scheduled_part();
                        let snapshot = parts.snapshot();
                        drop(state);
                        self.inner.observability.observe_event(
                            self.inner.ctx.id,
                            UploadEvent::CustomSourceBlocked,
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
                    PartReadWake::new(self.inner.ctx.waker()),
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
                    return abandoned_part(&state, "while parking upload part read");
                };
                parts.record_read_pending(&mut timing);
                let parked_wake = Arc::clone(&wake);
                let observation = timing.observation();
                parts.park_read(PendingPartRead {
                    future,
                    wake,
                    timing,
                });
                let snapshot = parts.snapshot();
                drop(state);
                self.inner.observability.observe_source_pending(
                    self.inner.ctx.id,
                    observation,
                    snapshot,
                );
                parked_wake.requeue_if_notified();
                return WorkOutcome::Yielded;
            }
            Poll::Ready(Ok(Some(data))) => data,
            Poll::Ready(Ok(None)) => {
                return self.on_end_of_stream();
            }
            Poll::Ready(Err(error)) => {
                let state = self.inner.state.lock().expect("lock poisoned");
                return self.fail(state, error.into(), FailedRequest::Other);
            }
        };

        {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &mut *state else {
                return abandoned_part(&state, "while recording upload part");
            };
            let parts_read = match parts.observe_part(data.data.len() as u64) {
                Ok(parts_read) => parts_read,
                Err(error) => {
                    return self.fail(
                        state,
                        crate::error::invalid_input(error),
                        FailedRequest::Other,
                    );
                }
            };
            if reads_until_eof && parts_read > MAX_PARTS {
                return self.fail(
                    state,
                    Error::new(
                        ErrorKind::InputInvalid,
                        format!(
                            "stream produced more than the maximum of {MAX_PARTS} parts; combine \
                             source data into fewer, larger parts"
                        ),
                    ),
                    FailedRequest::Other,
                );
            }
            if let Err(error) = validate_part_number(data.part_number) {
                return self.fail(
                    state,
                    crate::error::invalid_input(error),
                    FailedRequest::Other,
                );
            }
        }

        // A custom source that was blocked is available again. Refill the work withheld while the
        // retained source operation was pending so reading the next part can overlap this upload.
        self.inner.ctx.try_wake();

        self.send_part(data, timing).await
    }

    /// Records source end-of-stream.
    ///
    /// Empty-part synthesis is deferred until every source read and UploadPart operation drains.
    /// At that point transfer state can distinguish no source parts from an explicit zero-byte part.
    fn on_end_of_stream(&self) -> WorkOutcome {
        let (transitioned, snapshot) = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            let UploadState::Transferring { parts, .. } = &mut *state else {
                return abandoned_part(&state, "at upload end-of-stream");
            };
            let transitioned = parts.record_end_of_stream();
            (transitioned, parts.snapshot())
        };
        if transitioned {
            self.inner.observability.observe_event(
                self.inner.ctx.id,
                UploadEvent::SourceExhausted,
                snapshot,
            );
        }

        // End-of-stream may close dispatch while `poll_work` is parked behind the source gate.
        self.inner.ctx.try_wake();

        self.finish_read_without_part();
        WorkOutcome::Success { data: None }
    }

    /// Uploads one part and records it for CompleteMultipartUpload.
    ///
    /// This is shared by ordinary source output and the empty-object part, keeping request
    /// retries, accounting, and completion behavior identical.
    async fn send_part(&self, data: PartData, timing: UploadPartTiming) -> WorkOutcome {
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
                    (upload_id.clone(), source_observation, parts.snapshot())
                }
                other => return abandoned_part(other, "for send_part"),
            }
        };

        let part_num_i32 = i32::try_from(part_number)
            .expect("part numbers are checked against 1..=10,000 before UploadPart");
        self.inner.observability.observe_part_started(
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
        let mut req_metrics = self
            .inner
            .observability
            .start_request(&self.inner.ctx, UploadRequestKind::UploadPart);
        let retry_classify = crate::retry::classify_upload_part_retry;
        let result = crate::retry::retry(req_metrics.metrics_mut(), retry_classify, |_hedge| {
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
                    .config_override(self.inner.ctx.upload_override(self.inner.request.bucket()))
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
        let request_elapsed = req_metrics.finish().elapsed;
        // The original retry body retains every immutable payload owner even
        // after the successful request clone has been consumed. Release it
        // before publishing part completion: the final part can wake
        // CompleteMultipartUpload on another worker, and that operation may
        // signal `join()` before this work item otherwise unwinds.
        drop(sdk_body);
        // Validate the response after the retry loop, so a rejected response is not re-sent.
        let result = result.and_then(|resp| validate_upload_part_response(resp, part_number));
        let resp = match result {
            Ok(resp) => resp,
            Err(e) => {
                self.inner.observability.complete_upload();
                // Outside `Transferring` (an abort started) there is no part
                // snapshot to report, but the error is still recorded. The
                // part event is emitted after the lock is released; the
                // terminal report cannot come first, because it waits for this
                // work item to drop.
                let state = self.inner.state.lock().expect("lock poisoned");
                let snapshot = match &*state {
                    UploadState::Transferring { parts, .. } => Some(parts.snapshot()),
                    _ => None,
                };
                let outcome = self.fail(state, e, FailedRequest::Other);
                if let Some(snapshot) = snapshot {
                    self.inner.observability.observe_part_failed(
                        self.inner.ctx.id,
                        part_number,
                        bytes_sent,
                        presentation_segments,
                        request_elapsed,
                        snapshot,
                    );
                }
                return outcome;
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
                return abandoned_part(&state, "while completing upload part");
            };
            parts.complete_part(completed, bytes_sent);
            let should_wake = try_begin_completing(&mut state);
            let snapshot = snapshot_state(&state);
            (should_wake, snapshot)
        };

        self.inner.observability.observe_part_completed(
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
            abandoned_part(&state, "while completing empty upload read");
            return;
        };
        let empty_object_ready = parts.finish_read_without_part();
        if empty_object_ready || try_begin_completing(&mut state) {
            drop(state);
            self.inner.ctx.try_wake();
        }
    }

    async fn execute_put_object(&self, stream: InputStream) -> WorkOutcome {
        use crate::operation::upload::request::copy_fields_to_put_object_request;

        {
            let state = self.inner.state.lock().expect("lock poisoned");
            // The upload was aborted or ended after dispatch: never send
            // PutObject. `execute` normally catches this first (the token is
            // checked before this future is polled); this is the backstop.
            if matches!(&*state, UploadState::Aborting { .. } | UploadState::Done) {
                return WorkOutcome::Cancelled;
            }
        }
        let content_length = stream
            .size_hint()
            .upper()
            .expect("content length must be known for PutObject");

        let is_file_backed = stream.is_file_backed();
        let direct_io = self.inner.ctx.handle.runtime.components().direct_body_io();

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
                Err(error) => {
                    let state = self.inner.state.lock().expect("lock poisoned");
                    return self.fail(state, error.into(), FailedRequest::Other);
                }
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
        let mut req_metrics = self
            .inner
            .observability
            .start_request(&self.inner.ctx, UploadRequestKind::PutObject);
        let retry_classify = crate::retry::classify_upload_part_retry;
        let result = crate::retry::retry(req_metrics.metrics_mut(), retry_classify, |_hedge| {
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
                    .config_override(self.inner.ctx.upload_override(self.inner.request.bucket()))
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
        let _ = req_metrics.finish();
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
                let state = self.inner.state.lock().expect("lock poisoned");
                return self.fail(state, e, FailedRequest::Other);
            }
        };

        let result = crate::sdk_v1::upload_output_from_put_object(&resp)
            .metrics(self.inner.ctx.metrics())
            .build()
            .expect("valid response");

        *self.inner.result.lock().expect("lock poisoned") = Some(UploadResult::Completed(result));

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

        let snapshot = self.set_done();
        self.inner.ctx.set_completed();
        self.report_terminal(snapshot);
        self.signal_terminal_when_drained();

        WorkOutcome::Success { data: None }
    }

    async fn execute_complete_mpu(&self, completion: CompleteMpuWork) -> WorkOutcome {
        let CompleteMpuWork {
            parts,
            response_builder,
        } = completion;
        let transfer_diagnostics = self.inner.ctx.handle.config.diagnostics().transfer();
        let completion_timer = UploadDiagnosticTimer::start(transfer_diagnostics);
        let upload_id = {
            let mut state = self.inner.state.lock().expect("lock poisoned");
            match &mut *state {
                UploadState::CompleteInFlight { upload_id } => upload_id.clone(),
                // The abort landed after `poll_work` dispatched us but before we
                // started. Never send Complete, so the object cannot exist, and
                // tell the abort it no longer has to wait for us.
                UploadState::Aborting {
                    complete_mpu_in_flight,
                    ..
                } => {
                    *complete_mpu_in_flight = false;
                    drop(state); // unlock before waking
                    self.inner.ctx.try_wake();
                    return WorkOutcome::Cancelled;
                }
                // The transfer has ended: never send Complete.
                UploadState::Done => return WorkOutcome::Cancelled,
                _ => panic!("unexpected state for complete_mpu"),
            }
        };
        // The part state is held by this work item rather than by `CompleteInFlight`,
        // so its measurements come from here.
        self.inner.observability.observe_event(
            self.inner.ctx.id,
            UploadEvent::MultipartCompletionStarted,
            parts
                .snapshot()
                .with_state(UploadExecutionState::CompleteMultipartUploadInFlight),
        );

        let MultipartCompletion {
            part_reader,
            plan,
            mut completed_parts,
            bytes_read,
            final_snapshot,
        } = parts.into_completion();
        completed_parts.sort_by_key(|p| p.part_number);
        // Sorted, so a number used twice is adjacent. Checked here rather than per part: the
        // assembled list is the first place a repeat is visible without tracking every number.
        // Complete is not sent on these errors, but the upload is still open.
        if let Err(error) = validate_distinct_part_numbers(&completed_parts) {
            return self.complete_mpu_failed(crate::error::invalid_input(error));
        }

        let object_size = match plan.mpu_object_size(bytes_read) {
            Ok(object_size) => object_size,
            Err(error) => return self.complete_mpu_failed(crate::error::invalid_input(error)),
        };
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

        let complete_req = super::request::copy_fields_to_complete_mpu_request(
            &self.inner.request,
            base_req,
            || async { part_reader.full_object_checksum().await },
        )
        .await;
        // The transfer may have ended while the request was prepared.
        if !self.inner.ctx.is_active() {
            return WorkOutcome::Cancelled;
        }

        let req_metrics = self
            .inner
            .observability
            .start_request(&self.inner.ctx, UploadRequestKind::CompleteMultipartUpload);
        let response = complete_req
            .customize()
            .config_override(
                self.inner
                    .ctx
                    .bucket_partition_override(self.inner.request.bucket()),
            )
            .send()
            .instrument(tracing::debug_span!("send-complete-multipart-upload"))
            .await;
        let _ = req_metrics.finish();
        let resp = match response {
            Ok(resp) => resp,
            Err(e) => {
                let error: Error = e.into();
                // The TM SEP requires a warning with the upload ID and cause.
                tracing::warn!(
                    upload_id = %upload_id,
                    error = %error,
                    "CompleteMultipartUpload failed"
                );
                return self.complete_mpu_failed(error);
            }
        };
        self.inner.observability.finish_multipart(
            final_snapshot.with_state(UploadExecutionState::CompleteMultipartUploadInFlight),
            completion_timer.elapsed(),
        );

        let result = crate::sdk_v1::update_upload_output_from_complete_multipart_upload(
            response_builder,
            &resp,
        )
        .metrics(self.inner.ctx.metrics())
        .build()
        .expect("valid response");

        *self.inner.result.lock().expect("lock poisoned") = Some(UploadResult::Completed(result));
        let snapshot = self.set_done();
        self.inner.ctx.set_completed();
        self.report_terminal(snapshot);
        self.signal_terminal_when_drained();

        WorkOutcome::Success { data: None }
    }

    /// Handles a CompleteMultipartUpload error, or an error that stopped
    /// Complete from being sent.
    ///
    /// During a caller's abort, `NoSuchUpload` means the abort won and no
    /// object exists: S3 returns `NoSuchUpload` to a Complete sent after
    /// AbortMultipartUpload, and 200 to a Complete retried after an earlier
    /// one committed. Every other error, and any error outside an abort, fails
    /// the transfer, which aborts the upload.
    fn complete_mpu_failed(&self, error: Error) -> WorkOutcome {
        let no_such_upload = error.code() == Some("NoSuchUpload");
        let mut state = self.inner.state.lock().expect("lock poisoned");
        if no_such_upload {
            if let UploadState::Aborting {
                complete_mpu_in_flight,
                ..
            } = &mut *state
            {
                *complete_mpu_in_flight = false;
                drop(state); // unlock before waking
                self.inner.ctx.try_wake(); // the `Aborting` arm ends the transfer
                return WorkOutcome::Success { data: None };
            }
        }
        // Complete's response is in (or it was never sent), so the abort does
        // not wait for it.
        self.fail(state, error, FailedRequest::CompleteMpu)
    }

    /// Sends AbortMultipartUpload for an upload in `UploadState::Aborting`.
    async fn execute_abort_mpu(&self, upload_id: String) -> WorkOutcome {
        let ctx = &self.inner.ctx;
        // Complete committed while this request waited for a slot (always the
        // case at concurrency 1, where Complete holds the only slot). The
        // object exists and `abort()` reports `AlreadyCompleted`, so there is
        // no upload left to abort.
        if ctx.is_completed() {
            return WorkOutcome::Success { data: None };
        }
        let response = crate::sdk_v1::copy_upload_input_fields_to_abort_multipart_upload(
            &self.inner.request,
            ctx.s3_client()
                .abort_multipart_upload()
                .upload_id(&upload_id),
        )
        .customize()
        .config_override(
            ctx.handle
                .bucket_partition_override(self.inner.request.bucket()),
        )
        .send()
        .instrument(tracing::debug_span!("send-abort-multipart-upload"))
        .await;

        match response {
            Ok(resp) => {
                // Taken alone, never while holding the state lock, so the two
                // mutexes have no ordering to get wrong. A `Completed` result
                // already stored is kept: Complete committed first.
                self.inner
                    .result
                    .lock()
                    .expect("lock poisoned")
                    .get_or_insert(UploadResult::Aborted(AbortedUpload {
                        upload_id: Some(upload_id),
                        request_charged: resp
                            .request_charged
                            .as_ref()
                            .map(crate::sdk_v1::request_charged_from_sdk),
                        ..Default::default()
                    }));
                let mut state = self.inner.state.lock().expect("lock poisoned");
                // `Done` here means the upload ended first; nothing to record.
                if let UploadState::Aborting { abort_mpu, .. } = &mut *state {
                    *abort_mpu = AbortMpuProgress::Done;
                }
                drop(state);
                ctx.try_wake();
                WorkOutcome::Success { data: None }
            }
            Err(e) => {
                if ctx.is_completed() {
                    // Complete committed first, so the abort had nothing to do
                    // and its error says nothing about the object.
                    return WorkOutcome::Success { data: None };
                }
                let error: Error = e.into();
                tracing::warn!(
                    upload_id = %upload_id,
                    error = %error,
                    "AbortMultipartUpload failed; the multipart upload may still be open"
                );
                let state = self.inner.state.lock().expect("lock poisoned");
                // Not sent again. The transfer ends as failed, with this
                // error unless an earlier failure started the abort.
                self.fail(state, error, FailedRequest::AbortMpu)
            }
        }
    }

    /// Handles an error from any request or source read: records `error` and
    /// starts the abort.
    ///
    /// The transfer moves to `Aborting`, which interrupts parts and PutObject
    /// still running, sends AbortMultipartUpload for an open upload unless
    /// the policy is to retain it, and waits for every request still on the
    /// wire. Then it ends as failed with the first recorded failure. An error
    /// that arrives after the upload ended (`Done`) is dropped.
    ///
    /// Returns `WorkOutcome::Failed` while the transfer is still active, so
    /// the scheduler sees the error classification.
    ///
    /// `state` is the held state lock; it is released before the abort is
    /// signalled. `failed` names the request that failed, so `Aborting` stops
    /// waiting for it.
    fn fail(
        &self,
        state: MutexGuard<'_, UploadState>,
        error: Error,
        failed: FailedRequest,
    ) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            %error,
            "upload failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        let snapshot = snapshot_state(&state);
        self.enter_aborting(state, Some((UploadFailure { error, snapshot }, failed)));
        self.signal_abort();
        WorkOutcome::Failed { classification }
    }
}

/// The request that failed, so that `Aborting` stops waiting for it.
enum FailedRequest {
    /// A part, a source read or PutObject. Each is counted by its own
    /// `InFlight`, so `Aborting` tracks nothing else for it.
    Other,
    /// CreateMultipartUpload returned. `upload_id` is set when S3 created the
    /// upload before the failure, so that upload is aborted.
    CreateMpu { upload_id: Option<String> },
    /// CompleteMultipartUpload returned, or was never sent.
    CompleteMpu,
    /// AbortMultipartUpload returned. It is not sent again.
    AbortMpu,
}

/// Returns a copyable view of upload execution while the state lock is held.
fn snapshot_state(state: &UploadState) -> UploadStateSnapshot {
    match state {
        UploadState::PendingInit {
            init_in_flight: false,
            ..
        } => UploadStateSnapshot::inactive(UploadExecutionState::PendingInitialization),
        UploadState::PendingInit {
            init_in_flight: true,
            ..
        } => UploadStateSnapshot::inactive(UploadExecutionState::CreateMultipartUploadInFlight),
        UploadState::Transferring { parts, .. } => parts.snapshot(),
        UploadState::Completing { parts, .. } => parts
            .snapshot()
            .with_state(UploadExecutionState::MultipartCompletionPending),
        // The part state moved into the dispatched work item, so no part
        // measurements are available here.
        UploadState::CompleteInFlight { .. } => {
            UploadStateSnapshot::inactive(UploadExecutionState::CompleteMultipartUploadInFlight)
        }
        UploadState::PutObjectInFlight => {
            UploadStateSnapshot::inactive(UploadExecutionState::PutObjectInFlight)
        }
        UploadState::Aborting { .. } => {
            UploadStateSnapshot::inactive(UploadExecutionState::Aborting)
        }
        UploadState::Done => UploadStateSnapshot::inactive(UploadExecutionState::Done),
    }
}

/// Moves a fully drained multipart transfer into its completion phase.
///
/// The state replacement elects exactly one caller even when source EOF and a network completion
/// race to retire the final work item.
/// Checks that a successful `UploadPart` response carries what completing the upload needs.
///
/// `CompleteMultipartUpload` identifies each part by its ETag, so a response without one, or with
/// an empty one, is a failed part: the completed list could not identify it, and the object would
/// be stored without that evidence. aws-c-s3 treats it the same way
/// (`AWS_ERROR_S3_MISSING_ETAG`). The error carries the operation name and the response's request
/// ids, as an error from a failed request does.
fn validate_upload_part_response(
    resp: aws_sdk_s3::operation::upload_part::UploadPartOutput,
    part_number: u64,
) -> Result<aws_sdk_s3::operation::upload_part::UploadPartOutput, Error> {
    if resp.e_tag().is_some_and(|e_tag| !e_tag.is_empty()) {
        return Ok(resp);
    }
    Err(crate::error::invalid_service_response(
        "UploadPart",
        &resp,
        format!("UploadPart returned no ETag for part {part_number}"),
    ))
}

/// Returns `WorkOutcome::Cancelled` for part work that succeeded, or produced
/// nothing, after the state left `Transferring`.
///
/// After an abort (by the caller or by a failure), a part already past its
/// last `.await` can still reach one of its state locks. There is no part
/// state left to record its result in, so the result is dropped. Part errors
/// never reach here; they go to `fail()`.
///
/// # Panics
///
/// Panics unless `state` is `Aborting` or `Done`; any other state is a bug.
fn abandoned_part(state: &UploadState, step: &str) -> WorkOutcome {
    assert!(
        matches!(state, UploadState::Aborting { .. } | UploadState::Done),
        "unexpected state {step}"
    );
    WorkOutcome::Cancelled
}

/// Returns the ID of the multipart upload `state` leaves open if the upload
/// ends now: one created, not committed, and not closed by
/// AbortMultipartUpload.
fn open_upload_id(state: &UploadState) -> Option<String> {
    match state {
        UploadState::Transferring { upload_id, .. }
        | UploadState::Completing { upload_id, .. }
        | UploadState::CompleteInFlight { upload_id } => Some(upload_id.clone()),
        UploadState::Aborting {
            upload_id,
            abort_mpu,
            ..
        } if *abort_mpu != AbortMpuProgress::Done => upload_id.clone(),
        _ => None,
    }
}

fn try_begin_completing(state: &mut UploadState) -> bool {
    let ready = matches!(
        state,
        UploadState::Transferring { parts, .. } if parts.is_complete()
    );
    if !ready {
        return false;
    }

    // Move the fields out by value; the placeholder is overwritten before the state lock is
    // released, so no reader observes it.
    let UploadState::Transferring {
        upload_id,
        parts,
        response_builder,
    } = std::mem::replace(state, UploadState::Done)
    else {
        unreachable!("completion readiness changed under lock");
    };
    *state = UploadState::Completing {
        upload_id,
        parts,
        response_builder,
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

    fn on_terminal(&self) {
        // Terminal cleanup runs even if a panic poisoned the state lock.
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The summary reports the state the transfer ended in.
        let snapshot = snapshot_state(&state);
        // An ended transfer is never polled again, so nothing sends
        // AbortMultipartUpload for it.
        let left_open = open_upload_id(&state);
        // Nothing is left to drive. Work still executing finds `Done` and
        // stands down.
        let previous = std::mem::replace(&mut *state, UploadState::Done);
        drop(state);
        // Interrupted parts drop the part readers they hold.
        self.inner.abort_parts_token.cancel();
        if let Some(upload_id) = left_open {
            self.warn_upload_left_open(&upload_id);
        }
        self.report_terminal(snapshot);
        // Last, after the lock and the transfer's own cleanup: it can own the
        // caller's `PartStream`, whose `Drop` is caller code.
        drop(previous);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::InputStream;
    use crate::scheduler::test_util::{assert_done, assert_pending, assert_ready};
    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;

    fn create_test_transfer(s3_client: aws_sdk_s3::Client, content: Vec<u8>) -> UploadTransfer {
        create_test_transfer_with_stream(s3_client, InputStream::from(content))
    }

    fn create_test_transfer_with_stream(
        s3_client: aws_sdk_s3::Client,
        stream: InputStream,
    ) -> UploadTransfer {
        create_test_transfer_with_detail(s3_client, stream, 2)
    }

    fn create_test_transfer_with_detail(
        s3_client: aws_sdk_s3::Client,
        stream: InputStream,
        detail: u64,
    ) -> UploadTransfer {
        create_test_transfer_with_policy(s3_client, stream, detail, None)
    }

    fn create_test_transfer_with_policy(
        s3_client: aws_sdk_s3::Client,
        stream: InputStream,
        detail: u64,
        policy: Option<FailedMultipartUploadPolicy>,
    ) -> UploadTransfer {
        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder()
                .sdk_client(s3_client)
                .diagnostics_for_test(crate::config::MemoryDiagnosticsConfig::default(), detail)
                .build(),
        );

        let mut input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream)
            .set_failed_multipart_upload_policy(policy)
            .build()
            .unwrap();
        let stream = input.take_body();

        let (ctx, _completion_rx) = TransferContext::new(handle);
        UploadTransfer::try_new(ctx, BucketType::Standard, input, stream).unwrap()
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

    fn abort_mpu_ok() -> aws_smithy_mocks::Rule {
        mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .then_output(|| AbortMultipartUploadOutput::builder().build())
    }

    /// Runs the AbortMultipartUpload a failure emits, then checks the
    /// transfer has ended.
    async fn finish_failure_abort(transfer: &UploadTransfer) {
        let mut abort = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut abort).await,
            WorkOutcome::Success { .. }
        ));
        assert_done(transfer.poll_work());
    }

    fn service_error_response() -> HttpResponse {
        HttpResponse::new(
            StatusCode::try_from(412).unwrap(),
            SdkBody::from("<Error><Code>PreconditionFailed</Code></Error>"),
        )
    }

    /// A 403 `AccessDenied`, so a test can tell it apart from the 412 in
    /// `service_error_response`.
    fn access_denied_response() -> HttpResponse {
        HttpResponse::new(
            StatusCode::try_from(403).unwrap(),
            SdkBody::from("<Error><Code>AccessDenied</Code></Error>"),
        )
    }

    fn create_mpu_ok() -> aws_smithy_mocks::Rule {
        mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        })
    }

    /// AbortMultipartUpload that succeeds only for `test-upload-id`.
    fn abort_mpu_for_test_upload() -> aws_smithy_mocks::Rule {
        mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .match_requests(|req| req.upload_id() == Some("test-upload-id"))
            .then_output(|| AbortMultipartUploadOutput::builder().build())
    }

    /// The `Aborting` fields that decide when and how the abort ends.
    #[derive(Debug, PartialEq)]
    struct AbortingView {
        upload_id: Option<String>,
        create_in_flight: bool,
        complete_mpu_in_flight: bool,
        abort_mpu: AbortMpuProgress,
        failure_recorded: bool,
    }

    /// Panics if the transfer is not in `Aborting`.
    fn aborting_view(transfer: &UploadTransfer) -> AbortingView {
        let state = transfer.inner.state.lock().expect("lock poisoned");
        let UploadState::Aborting {
            upload_id,
            create_in_flight,
            complete_mpu_in_flight,
            abort_mpu,
            failure,
        } = &*state
        else {
            panic!("expected Aborting, got {:?}", snapshot_state(&state).state);
        };
        AbortingView {
            upload_id: upload_id.clone(),
            create_in_flight: *create_in_flight,
            complete_mpu_in_flight: *complete_mpu_in_flight,
            abort_mpu: *abort_mpu,
            failure_recorded: failure.is_some(),
        }
    }

    /// The error the transfer ended with.
    fn final_error(transfer: &UploadTransfer) -> crate::error::Error {
        transfer
            .ctx()
            .take_error()
            .expect("transfer should end as failed")
    }

    #[test]
    fn upload_state_snapshot_classifies_non_multipart_execution() {
        let pending = UploadState::PendingInit {
            stream: None,
            size_hint: crate::io::SizeHint::exact(0),
            init_in_flight: false,
        };
        assert_eq!(
            snapshot_state(&pending),
            UploadStateSnapshot::inactive(UploadExecutionState::PendingInitialization)
        );

        let creating = UploadState::PendingInit {
            stream: None,
            size_hint: crate::io::SizeHint::exact(0),
            init_in_flight: true,
        };
        assert_eq!(
            snapshot_state(&creating),
            UploadStateSnapshot::inactive(UploadExecutionState::CreateMultipartUploadInFlight)
        );

        let completion_in_flight = UploadState::CompleteInFlight {
            upload_id: "test-upload-id".to_string(),
        };
        assert_eq!(
            snapshot_state(&completion_in_flight),
            UploadStateSnapshot::inactive(UploadExecutionState::CompleteMultipartUploadInFlight)
        );
        assert_eq!(
            snapshot_state(&UploadState::PutObjectInFlight),
            UploadStateSnapshot::inactive(UploadExecutionState::PutObjectInFlight)
        );
        assert_eq!(
            snapshot_state(&UploadState::Done),
            UploadStateSnapshot::inactive(UploadExecutionState::Done)
        );
    }

    #[derive(Debug)]
    struct BlockingPartStream {
        ready: Arc<std::sync::atomic::AtomicBool>,
        source_waker: Arc<Mutex<Option<std::task::Waker>>>,
        yielded: bool,
        /// Dropped with the stream, so a `Weak` to it reports whether the stream is still owned.
        _probe: Option<Arc<()>>,
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

    #[derive(Debug)]
    struct FailingPartStream;

    impl crate::io::PartStream for FailingPartStream {
        fn poll_part(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _stream_cx: &crate::io::StreamContext,
        ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
            std::task::Poll::Ready(Some(Err(std::io::Error::other("injected source failure"))))
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::exact(5)
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
            UploadWork::CreateMPU(_)
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

        let mut empty_object = assert_ready(transfer.poll_work());
        let UploadWork::UploadPart(work, _) = empty_object.data_mut::<UploadWork>() else {
            panic!("drained empty source did not schedule its required zero-byte part");
        };
        assert!(work.is_empty_object());
        assert!(matches!(
            transfer.execute(&mut empty_object).await,
            WorkOutcome::Success { .. }
        ));

        let mut complete = assert_ready(transfer.poll_work());
        assert!(matches!(
            complete.data_mut::<UploadWork>(),
            UploadWork::CompleteMPU(..)
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
    async fn end_of_stream_waits_for_an_earlier_source_part_to_retire() {
        #[derive(Debug)]
        struct UnknownSource;

        impl crate::io::PartStream for UnknownSource {
            fn poll_part(
                self: Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                _stream_cx: &crate::io::StreamContext,
            ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
                unreachable!("this test drives transfer accounting directly")
            }

            fn size_hint(&self) -> crate::io::SizeHint {
                crate::io::SizeHint::default()
            }
        }

        let transfer = create_test_transfer_with_stream(
            mock_s3_client_for_mpu(),
            InputStream::from_part_stream(UnknownSource),
        );
        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));

        let mut state = transfer.inner.state.lock().expect("lock poisoned");
        let UploadState::Transferring { parts, .. } = &mut *state else {
            panic!("upload did not enter transferring state");
        };

        // Model the ordering that exposed the race:
        //
        //   part N returns data ── gate opens ── part N+1 observes EOF
        //          │                                  │
        //          └──── accounting is delayed ───────┘
        //
        // EOF retires its own work first. The earlier source part remains in flight, so the
        // transfer must not mistake the temporarily empty accounting state for an empty source.
        let mut earlier_part = parts.schedule_part().expect("earlier source part");
        let _end_of_stream = parts.schedule_part().expect("end-of-stream read");
        assert!(parts.record_end_of_stream());
        assert!(!parts.record_end_of_stream());
        let empty_object_ready = parts.finish_read_without_part();

        assert!(!empty_object_ready);
        assert!(!parts.is_complete());

        parts.observe_part(5).unwrap();
        parts.begin_upload(1, earlier_part.take_timing());
        parts.complete_part(CompletedPart::builder().part_number(1).build(), 5);

        assert!(parts.maybe_start_empty_object_part().is_none());
        assert!(parts.is_complete());
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
            _probe: None,
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
                UploadStateSnapshot {
                    state: UploadExecutionState::Transferring,
                    parts_dispatched: 3,
                    parts_in_flight: 3,
                    uploads_in_flight: 0,
                    pending_reads: 1,
                    completed_parts: 0,
                    bytes_read: 0,
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
            assert_eq!(summary.parts_completed, 1);
            assert_eq!(summary.bytes_uploaded, 5);
            assert_eq!(summary.read_pending_polls, 1);
            assert_eq!(summary.read_pending_parts, 1);
            assert_eq!(summary.presentation_segments, 1);
            assert_eq!(summary.max_presentation_segments, 1);
        }

        let mut next = assert_ready(transfer.poll_work());
        assert!(matches!(
            next.data_mut::<UploadWork>(),
            UploadWork::UploadPart(..)
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
        assert!(matches!(data, UploadWork::CreateMPU(_)));
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
        assert!(matches!(data1, UploadWork::UploadPart(..)));

        let mut work2 = assert_ready(transfer.poll_work());
        let data2 = work2.data_mut::<UploadWork>();
        assert!(matches!(data2, UploadWork::UploadPart(..)));

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
        assert!(matches!(data, UploadWork::UploadPart(..)));
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
            let UploadState::Completing { parts, .. } = &*state else {
                panic!("upload did not enter completing state");
            };
            assert_eq!(
                snapshot_state(&state).state,
                UploadExecutionState::MultipartCompletionPending
            );
            let summary = parts.test_summary();
            assert_eq!(summary.parts_completed, 2);
            assert_eq!(summary.last_active_snapshot.parts_in_flight, 0);
            assert_eq!(summary.last_active_snapshot.uploads_in_flight, 0);
            assert_eq!(summary.last_active_snapshot.pending_reads, 0);
            assert_eq!(summary.segmented_parts, 0);
            assert_eq!(summary.presentation_segments, 2);
            assert_eq!(summary.max_presentation_segments, 1);
            assert_eq!(summary.read_pending_polls, 0);
            assert_eq!(summary.read_pending_parts, 0);
        }

        // 4. CompleteMPU
        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<UploadWork>();
        assert!(matches!(data, UploadWork::CompleteMPU(..)));
        transfer.execute(&mut work).await;

        // 5. Should be Done
        use crate::scheduler::test_util::assert_done;
        assert_done(transfer.poll_work());

        // 6. Result should be available
        let result = transfer.take_result();
        assert!(result.is_some());

        let request_metrics = transfer.ctx().metrics.request_metrics();
        assert_eq!(request_metrics.requests, 4);
        assert_eq!(request_metrics.retry_reissues, 0);
        assert_eq!(request_metrics.throttle_reissues, 0);
        assert_eq!(request_metrics.hedge_reissues, 0);

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("completed MPU should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Completed);
        assert_eq!(summary.mode, UploadMode::Multipart);
        assert_eq!(summary.request_total.requests, 4);
        assert_eq!(summary.requests.create_multipart_upload.requests, 1);
        assert_eq!(summary.requests.upload_part.requests, 2);
        assert_eq!(summary.requests.complete_multipart_upload.requests, 1);
        assert_eq!(
            summary
                .multipart
                .expect("multipart summary")
                .parts_completed,
            2
        );
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
        assert_eq!(transfer.ctx().metrics.request_metrics().requests, 1);

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("completed PutObject should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Completed);
        assert_eq!(summary.mode, UploadMode::PutObject);
        assert_eq!(summary.request_total.requests, 1);
        assert_eq!(summary.requests.put_object.requests, 1);
        assert_eq!(summary.multipart, None);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn diagnostic_levels_preserve_put_object_outcome() {
        for detail in 0..=2 {
            let put_object = mock!(aws_sdk_s3::Client::put_object).then_output(|| {
                aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build()
            });
            let transfer = create_test_transfer_with_detail(
                mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]),
                InputStream::from(vec![0u8; 1024]),
                detail,
            );

            let mut work = assert_ready(transfer.poll_work());
            assert!(matches!(
                transfer.execute(&mut work).await,
                WorkOutcome::Success { .. }
            ));
            assert_eq!(
                transfer.ctx().transfer_status(),
                crate::types::TransferStatus::Completed
            );
            assert_eq!(
                transfer
                    .inner
                    .observability
                    .test_terminal_summary()
                    .is_some(),
                detail != 0
            );
        }
    }

    // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn external_terminal_paths_emit_one_upload_summary() {
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[]),
            vec![0u8; 1024],
        );
        let _work = assert_ready(transfer.poll_work());
        transfer.ctx().set_cancelled();

        Transfer::on_terminal(&transfer);
        Transfer::on_terminal(&transfer);

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("external cancellation should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Cancelled);
        assert_eq!(summary.mode, UploadMode::PutObject);
        assert_eq!(summary.request_total.requests, 0);
    }

    // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn on_terminal_tolerates_poisoned_state() {
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[]),
            vec![0u8; 1024],
        );
        let _work = assert_ready(transfer.poll_work());
        std::thread::scope(|scope| {
            let poisoner = scope.spawn(|| {
                let _guard = transfer.inner.state.lock().expect("lock poisoned");
                panic!("unexpected state");
            });
            assert!(poisoner.join().is_err());
        });
        assert!(
            transfer.inner.state.lock().is_err(),
            "state should be poisoned"
        );
        transfer.ctx().set_failed(Error::new(
            ErrorKind::RuntimeError,
            "worker panic during execute",
        ));

        let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Transfer::on_terminal(&transfer)
        }));
        assert!(
            cleanup.is_ok(),
            "on_terminal must tolerate a poisoned state"
        );

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("poisoned state should still emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        let state = transfer
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            matches!(*state, UploadState::Done),
            "on_terminal must replace a poisoned state"
        );
    }

    // Builds a client handle: dropping the scheduler's ready set is not miri-clean.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn dropped_request_measurement_reaches_upload_request_kind_summary() {
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[]),
            vec![0u8; 1024],
        );
        {
            let mut measurement = transfer
                .inner
                .observability
                .start_request(&transfer.inner.ctx, UploadRequestKind::PutObject);
            measurement
                .metrics_mut()
                .record_retry_reissue(std::time::Duration::from_micros(3));
        }
        transfer.ctx().set_failed(Error::new(
            ErrorKind::RuntimeError,
            "simulated worker failure",
        ));
        Transfer::on_terminal(&transfer);

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("worker failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.request_total.requests, 1);
        assert_eq!(summary.requests.put_object.requests, 1);
        assert_eq!(summary.requests.put_object.retry_reissues, 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn create_multipart_failure_emits_terminal_summary() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload)
            .then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[create_mpu]),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut work = assert_ready(transfer.poll_work());
        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Failed { .. }));
        // No upload was created, so there is nothing to abort.
        assert_done(transfer.poll_work());

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("CreateMultipartUpload failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.mode, UploadMode::Multipart);
        assert_eq!(summary.requests.create_multipart_upload.requests, 1);
        assert_eq!(summary.requests.upload_part.requests, 0);
        assert_eq!(summary.multipart, None);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn put_object_failure_emits_terminal_summary() {
        let put_object =
            mock!(aws_sdk_s3::Client::put_object).then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put_object]),
            vec![0u8; 1024],
        );

        let mut work = assert_ready(transfer.poll_work());
        let outcome = transfer.execute(&mut work).await;
        assert!(matches!(outcome, WorkOutcome::Failed { .. }));
        assert_done(transfer.poll_work());

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("PutObject failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.mode, UploadMode::PutObject);
        assert_eq!(summary.requests.put_object.requests, 1);
        assert_eq!(summary.multipart, None);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn source_failure_emits_terminal_summary() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        });
        let transfer = create_test_transfer_with_stream(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu, abort_mpu_ok()]
            ),
            InputStream::from_part_stream(FailingPartStream),
        );

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        let mut part = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut part).await,
            WorkOutcome::Failed { .. }
        ));
        finish_failure_abort(&transfer).await;

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("source failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.requests.create_multipart_upload.requests, 1);
        assert_eq!(summary.requests.upload_part.requests, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn upload_part_failure_emits_terminal_summary() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        });
        let upload_part =
            mock!(aws_sdk_s3::Client::upload_part).then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu, upload_part, abort_mpu_ok()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        let mut part = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut part).await,
            WorkOutcome::Failed { .. }
        ));
        finish_failure_abort(&transfer).await;

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("UploadPart failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.requests.create_multipart_upload.requests, 1);
        assert_eq!(summary.requests.upload_part.requests, 1);
        let multipart = summary.multipart.expect("multipart state");
        assert_eq!(multipart.parts_completed, 0);
        assert_eq!(multipart.last_active_snapshot.uploads_in_flight, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn complete_multipart_failure_emits_terminal_summary() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        });
        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu, upload_part, complete_mpu, abort_mpu_ok()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        for _ in 0..2 {
            let mut part = assert_ready(transfer.poll_work());
            transfer.execute(&mut part).await;
        }
        let mut complete = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut complete).await,
            WorkOutcome::Failed { .. }
        ));
        finish_failure_abort(&transfer).await;

        let summary = transfer
            .inner
            .observability
            .test_terminal_summary()
            .expect("CompleteMultipartUpload failure should emit a terminal summary");
        assert_eq!(summary.outcome, UploadTerminalOutcome::Failed);
        assert_eq!(summary.requests.create_multipart_upload.requests, 1);
        assert_eq!(summary.requests.upload_part.requests, 2);
        assert_eq!(summary.requests.complete_multipart_upload.requests, 1);
        assert_eq!(
            summary.multipart.expect("multipart state").parts_completed,
            2
        );
    }

    /// A failed Create leaves no upload to abort: the transfer ends as failed
    /// without AbortMultipartUpload.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn create_failure_ends_without_abort_mpu() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload)
            .then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[create_mpu]),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: None,
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: true,
            }
        );
        assert_done(transfer.poll_work());
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// A caller's abort while Create is on the wire waits for Create; when
    /// Create then fails, the transfer ends as failed, not cancelled.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn create_failure_during_caller_abort_ends_as_failed() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload)
            .then_http_response(service_error_response);
        let transfer = create_test_transfer(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[create_mpu]),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.request_abort();
        assert_pending(transfer.poll_work());
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: None,
                create_in_flight: true,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: false,
            }
        );

        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: None,
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: true,
            }
        );
        assert_done(transfer.poll_work());
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// When the part reader cannot be built after Create succeeded, the
    /// upload ID is handed to the abort and the upload is aborted.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn part_reader_failure_after_create_aborts_the_upload() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&vec![0u8; 16 * 1024 * 1024]).unwrap();
        let abort_mpu = abort_mpu_for_test_upload();
        let transfer = create_test_transfer_with_stream(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), abort_mpu.clone()]
            ),
            InputStream::from_path(tmp.path()).unwrap(),
        );
        // The reader opens the file after Create returns; removing it makes
        // that open fail.
        tmp.close().unwrap();

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: true,
            }
        );
        finish_failure_abort(&transfer).await;
        assert_eq!(abort_mpu.num_calls(), 1);
        assert_eq!(
            final_error(&transfer).kind(),
            &crate::error::ErrorKind::IOError
        );
    }

    /// A failed UploadPart aborts the upload and reports the part's error.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn upload_part_failure_aborts_the_upload() {
        let upload_part =
            mock!(aws_sdk_s3::Client::upload_part).then_http_response(service_error_response);
        let abort_mpu = abort_mpu_for_test_upload();
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), upload_part, abort_mpu.clone()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        let mut part = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut part).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: true,
            }
        );
        finish_failure_abort(&transfer).await;
        assert_eq!(abort_mpu.num_calls(), 1);
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// Under `Retain`, a failed UploadPart leaves the upload open.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn upload_part_failure_with_retain_sends_no_abort_mpu() {
        let upload_part =
            mock!(aws_sdk_s3::Client::upload_part).then_http_response(service_error_response);
        let abort_mpu = abort_mpu_for_test_upload();
        let transfer = create_test_transfer_with_policy(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), upload_part, abort_mpu.clone()]
            ),
            InputStream::from(vec![0u8; 16 * 1024 * 1024]),
            2,
            Some(FailedMultipartUploadPolicy::Retain),
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        let mut part = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut part).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::NotNeeded,
                failure_recorded: true,
            }
        );
        assert_done(transfer.poll_work());
        assert_eq!(abort_mpu.num_calls(), 0);
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// Under `Retain`, an abort before CreateMultipartUpload is sent leaves no
    /// upload to retain, so it stores no result: `abort()` reports
    /// `Cancelled` without an upload ID.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn abort_before_create_with_retain_stores_no_result() {
        let transfer = create_test_transfer_with_policy(
            mock_client!(aws_sdk_s3, []),
            InputStream::from(vec![0u8; 16 * 1024 * 1024]),
            2,
            Some(FailedMultipartUploadPolicy::Retain),
        );

        transfer.request_abort();
        assert_done(transfer.poll_work());
        assert!(transfer.take_result().is_none());
    }

    /// A failed Complete aborts the upload; the abort does not wait for
    /// Complete, whose response is already in.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn complete_failure_aborts_the_upload() {
        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .then_http_response(service_error_response);
        let abort_mpu = abort_mpu_for_test_upload();
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[
                    create_mpu_ok(),
                    upload_part,
                    complete_mpu,
                    abort_mpu.clone()
                ]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        for _ in 0..2 {
            let mut part = assert_ready(transfer.poll_work());
            transfer.execute(&mut part).await;
        }
        let mut complete = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut complete).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::ToSend,
                failure_recorded: true,
            }
        );
        finish_failure_abort(&transfer).await;
        assert_eq!(abort_mpu.num_calls(), 1);
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// When the AbortMPU a failure started also fails, it is not sent again
    /// and the transfer reports the first failure.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_mpu_failure_after_part_failure_keeps_part_error() {
        let upload_part =
            mock!(aws_sdk_s3::Client::upload_part).then_http_response(service_error_response);
        let abort_mpu = mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .then_http_response(access_denied_response);
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), upload_part, abort_mpu.clone()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        let mut part = assert_ready(transfer.poll_work());
        transfer.execute(&mut part).await;
        let mut abort = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut abort).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::Failed,
                failure_recorded: true,
            }
        );
        assert_done(transfer.poll_work());
        assert_eq!(abort_mpu.num_calls(), 1);
        assert_eq!(final_error(&transfer).code(), Some("PreconditionFailed"));
    }

    /// When the AbortMPU of a caller's abort fails, the transfer ends as
    /// failed with the AbortMPU error instead of cancelled.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_mpu_failure_during_caller_abort_ends_as_failed() {
        let abort_mpu = mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .then_http_response(access_denied_response);
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), abort_mpu.clone()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );

        let mut create = assert_ready(transfer.poll_work());
        transfer.execute(&mut create).await;
        transfer.request_abort();
        let mut abort = assert_ready(transfer.poll_work());
        assert_eq!(
            aborting_view(&transfer),
            AbortingView {
                upload_id: Some("test-upload-id".to_owned()),
                create_in_flight: false,
                complete_mpu_in_flight: false,
                abort_mpu: AbortMpuProgress::InFlight,
                failure_recorded: false,
            }
        );
        assert!(matches!(
            transfer.execute(&mut abort).await,
            WorkOutcome::Failed { .. }
        ));
        assert_eq!(aborting_view(&transfer).abort_mpu, AbortMpuProgress::Failed);
        assert_done(transfer.poll_work());
        assert_eq!(abort_mpu.num_calls(), 1);
        assert_eq!(final_error(&transfer).code(), Some("AccessDenied"));
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
            crate::Config::builder().sdk_client(s3_client).build(),
        );
        let mut input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from_path(tmp.path()).unwrap())
            .build()
            .unwrap();
        let stream = input.take_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = UploadTransfer::try_new(ctx, BucketType::Standard, input, stream).unwrap();

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
            UploadWork::CreateMPU(_)
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
            UploadWork::CompleteMPU(..)
        ));
        transfer.execute(&mut work).await;

        assert!(transfer.take_result().is_some());
    }

    /// Returns a body over a [`BlockingPartStream`], and a `Weak` that upgrades only while the
    /// stream has not been dropped.
    ///
    /// With `ready`, the stream yields one five-byte part and then ends. Otherwise it stays
    /// pending, as a producer waiting for the upload to take its next part does.
    fn probed_stream(ready: bool) -> (InputStream, std::sync::Weak<()>) {
        let probe = Arc::new(());
        let weak = Arc::downgrade(&probe);
        let stream = BlockingPartStream {
            ready: Arc::new(std::sync::atomic::AtomicBool::new(ready)),
            source_waker: Arc::new(Mutex::new(None)),
            yielded: false,
            _probe: Some(probe),
        };
        (InputStream::from_part_stream(stream), weak)
    }

    /// Creates the multipart upload, then executes one source read that parks on the pending
    /// source.
    async fn park_one_source_read(transfer: &UploadTransfer) {
        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        let mut read = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut read).await,
            WorkOutcome::Yielded
        ));
        let state = transfer.inner.state.lock().expect("lock poisoned");
        assert_eq!(snapshot_state(&state).pending_reads, 1);
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A source read parked on a pending source is released when another work item fails the
    /// upload, rather than retained for as long as the transfer lives.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn failure_releases_parked_source_read() {
        let (stream, probe) = probed_stream(false);
        let transfer = create_test_transfer_with_stream(mock_s3_client_for_mpu(), stream);
        park_one_source_read(&transfer).await;

        assert!(matches!(
            transfer.fail(
                transfer.inner.state.lock().expect("lock poisoned"),
                Error::new(ErrorKind::RuntimeError, "sibling failure"),
                FailedRequest::Other,
            ),
            WorkOutcome::Failed { .. }
        ));

        assert!(
            probe.upgrade().is_none(),
            "failed upload retained its parked source read"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A failed UploadPart releases a source that no read holds. Between reads the stream waits
    /// in the part reader, so the failure must release the reader, not only the parked reads.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn upload_part_failure_releases_idle_source() {
        let create_mpu = mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(|| {
            CreateMultipartUploadOutput::builder()
                .upload_id("test-upload-id")
                .build()
        });
        let upload_part =
            mock!(aws_sdk_s3::Client::upload_part).then_http_response(service_error_response);
        let (stream, probe) = probed_stream(true);
        let transfer = create_test_transfer_with_stream(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[create_mpu, upload_part]),
            stream,
        );

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        let mut part = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut part).await,
            WorkOutcome::Failed { .. }
        ));

        assert!(
            probe.upgrade().is_none(),
            "failed upload retained its idle source"
        );
    }

    /// A source whose first poll fails the upload and then returns `Pending`: another work item
    /// failing while a read is in progress.
    #[derive(Debug)]
    struct FailsUploadWhilePolledStream {
        /// Taken on the first poll, which also breaks the transfer-to-stream cycle.
        transfer: Arc<Mutex<Option<UploadTransfer>>>,
        _probe: Arc<()>,
    }

    impl crate::io::PartStream for FailsUploadWhilePolledStream {
        fn poll_part(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _stream_cx: &crate::io::StreamContext,
        ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
            let transfer = self.transfer.lock().expect("transfer slot poisoned").take();
            if let Some(transfer) = transfer {
                assert!(matches!(
                    transfer.fail(
                        transfer.inner.state.lock().expect("lock poisoned"),
                        Error::new(ErrorKind::RuntimeError, "sibling failure"),
                        FailedRequest::Other,
                    ),
                    WorkOutcome::Failed { .. }
                ));
            }
            std::task::Poll::Pending
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::default()
        }
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A read whose source returns `Pending` after another work item failed the upload is not
    /// parked: the read reports `Cancelled`, and its stream is dropped with the part reader rather
    /// than retained.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn read_pending_after_failure_is_not_parked() {
        let probe = Arc::new(());
        let weak = Arc::downgrade(&probe);
        let slot = Arc::new(Mutex::new(None));
        let stream = FailsUploadWhilePolledStream {
            transfer: Arc::clone(&slot),
            _probe: probe,
        };
        let transfer = create_test_transfer_with_stream(
            mock_s3_client_for_mpu(),
            InputStream::from_part_stream(stream),
        );
        *slot.lock().expect("transfer slot poisoned") = Some(transfer.clone());

        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        let mut read = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut read).await,
            WorkOutcome::Cancelled
        ));

        let state = transfer.inner.state.lock().expect("lock poisoned");
        assert_eq!(snapshot_state(&state).pending_reads, 0);
        drop(state);
        assert!(
            weak.upgrade().is_none(),
            "a read pending after the failure retained its source"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// Ending the transfer from outside (cancellation or a worker panic) releases a source read
    /// parked on a pending source, rather than retaining it for as long as the handle is held.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn external_termination_releases_parked_source_read() {
        let (stream, probe) = probed_stream(false);
        let transfer = create_test_transfer_with_stream(mock_s3_client_for_mpu(), stream);
        park_one_source_read(&transfer).await;

        transfer.ctx().set_cancelled();
        Transfer::on_terminal(&transfer);

        assert!(
            probe.upgrade().is_none(),
            "cancelled upload retained its parked source read"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A read dispatched before the transfer ended from outside, and executed after, starts no
    /// source operation.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn read_started_after_termination_is_cancelled() {
        let (stream, probe) = probed_stream(false);
        let transfer = create_test_transfer_with_stream(mock_s3_client_for_mpu(), stream);
        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        let mut read = assert_ready(transfer.poll_work());

        transfer.ctx().set_cancelled();
        Transfer::on_terminal(&transfer);
        assert!(probe.upgrade().is_none());

        assert!(matches!(
            transfer.execute(&mut read).await,
            WorkOutcome::Cancelled
        ));
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// Ending the transfer from outside while CreateMultipartUpload is in flight drops the body.
    /// When Create then succeeds, it stands down without installing a part reader.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn create_multipart_after_termination_is_cancelled() {
        let (stream, probe) = probed_stream(false);
        let transfer = create_test_transfer_with_stream(mock_s3_client_for_mpu(), stream);
        let mut create = assert_ready(transfer.poll_work());

        transfer.ctx().set_cancelled();
        Transfer::on_terminal(&transfer);
        assert!(probe.upgrade().is_none());

        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Cancelled
        ));
        let state = transfer.inner.state.lock().expect("lock poisoned");
        assert!(matches!(&*state, UploadState::Done));
    }

    /// `open_upload_id` reports a multipart upload that was created, has not committed, and has
    /// not been closed by AbortMultipartUpload.
    #[test]
    fn open_upload_id_reports_only_uploads_left_open() {
        let id = || Some("test-upload-id".to_owned());
        let aborting = |upload_id, abort_mpu| UploadState::Aborting {
            upload_id,
            create_in_flight: false,
            complete_mpu_in_flight: false,
            abort_mpu,
            failure: None,
        };
        let complete_in_flight = UploadState::CompleteInFlight {
            upload_id: "test-upload-id".to_owned(),
        };
        assert_eq!(open_upload_id(&complete_in_flight), id());
        for abort_mpu in [
            AbortMpuProgress::NotNeeded,
            AbortMpuProgress::ToSend,
            AbortMpuProgress::InFlight,
            AbortMpuProgress::Failed,
        ] {
            assert_eq!(
                open_upload_id(&aborting(id(), abort_mpu)),
                id(),
                "{abort_mpu:?}"
            );
        }
        assert_eq!(
            open_upload_id(&aborting(id(), AbortMpuProgress::Done)),
            None
        );
        assert_eq!(
            open_upload_id(&aborting(None, AbortMpuProgress::ToSend)),
            None
        );
        let creating = UploadState::PendingInit {
            stream: None,
            size_hint: crate::io::SizeHint::default(),
            init_in_flight: true,
        };
        assert_eq!(open_upload_id(&creating), None);
        assert_eq!(open_upload_id(&UploadState::PutObjectInFlight), None);
        assert_eq!(open_upload_id(&UploadState::Done), None);
    }

    /// `open_upload_id` reports the upload while parts are transferring, and once they have all
    /// completed and CompleteMultipartUpload has not been dispatched.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn open_upload_id_reports_an_upload_with_parts() {
        let (stream, _probe) = probed_stream(false);
        let transferring = create_test_transfer_with_stream(mock_s3_client_for_mpu(), stream);
        park_one_source_read(&transferring).await;
        {
            let state = transferring.inner.state.lock().expect("lock poisoned");
            assert!(matches!(&*state, UploadState::Transferring { .. }));
            assert_eq!(open_upload_id(&state).as_deref(), Some("test-upload-id"));
        }

        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());
        let completing = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), upload_part]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );
        let mut create = assert_ready(completing.poll_work());
        assert!(matches!(
            completing.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        for _ in 0..2 {
            let mut part = assert_ready(completing.poll_work());
            assert!(matches!(
                completing.execute(&mut part).await,
                WorkOutcome::Success { .. }
            ));
        }
        let state = completing.inner.state.lock().expect("lock poisoned");
        assert!(matches!(&*state, UploadState::Completing { .. }));
        assert_eq!(open_upload_id(&state).as_deref(), Some("test-upload-id"));
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A CompleteMultipartUpload dispatched before the transfer ended from outside, and executed
    /// after, is never sent.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn complete_multipart_after_termination_is_not_sent() {
        let upload_part = mock!(aws_sdk_s3::Client::upload_part)
            .then_output(|| UploadPartOutput::builder().e_tag("test-etag").build());
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .then_output(|| CompleteMultipartUploadOutput::builder().build());
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                &[create_mpu_ok(), upload_part, complete_mpu.clone()]
            ),
            vec![0u8; 16 * 1024 * 1024],
        );
        let mut create = assert_ready(transfer.poll_work());
        assert!(matches!(
            transfer.execute(&mut create).await,
            WorkOutcome::Success { .. }
        ));
        for _ in 0..2 {
            let mut part = assert_ready(transfer.poll_work());
            assert!(matches!(
                transfer.execute(&mut part).await,
                WorkOutcome::Success { .. }
            ));
        }
        let mut complete = assert_ready(transfer.poll_work());

        transfer.ctx().set_cancelled();
        Transfer::on_terminal(&transfer);

        assert!(matches!(
            transfer.execute(&mut complete).await,
            WorkOutcome::Cancelled
        ));
        assert_eq!(complete_mpu.num_calls(), 0);
        assert!(transfer.take_result().is_none());
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// A PutObject that starts after the transfer ended from outside, before `abort_parts_token`
    /// interrupts it, is never sent.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn put_object_after_termination_is_not_sent() {
        use aws_sdk_s3::operation::put_object::PutObjectOutput;
        let put_object = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        let transfer = create_test_transfer(
            mock_client!(
                aws_sdk_s3,
                RuleMode::MatchAny,
                std::slice::from_ref(&put_object)
            ),
            vec![0u8; 1024],
        );
        let mut work = assert_ready(transfer.poll_work());
        let UploadWork::PutObject {
            stream,
            live: _live,
        } = work.take_data::<UploadWork>()
        else {
            panic!("expected PutObject");
        };

        transfer.ctx().set_cancelled();
        Transfer::on_terminal(&transfer);

        // Called past the token race in `execute`, as work that starts before `on_terminal`
        // cancels the token is.
        assert!(matches!(
            transfer.execute_put_object(stream).await,
            WorkOutcome::Cancelled
        ));
        assert_eq!(put_object.num_calls(), 0);
    }

    /// A source that stays pending and, when dropped, records whether the upload-state lock was
    /// free.
    #[derive(Debug)]
    struct ChecksStateLockOnDrop {
        /// Taken when dropped, which also breaks the transfer-to-stream cycle.
        transfer: Arc<Mutex<Option<UploadTransfer>>>,
        lock_was_free: Arc<Mutex<Option<bool>>>,
    }

    impl crate::io::PartStream for ChecksStateLockOnDrop {
        fn poll_part(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _stream_cx: &crate::io::StreamContext,
        ) -> std::task::Poll<Option<std::io::Result<PartData>>> {
            std::task::Poll::Pending
        }

        fn size_hint(&self) -> crate::io::SizeHint {
            crate::io::SizeHint::default()
        }
    }

    impl Drop for ChecksStateLockOnDrop {
        fn drop(&mut self) {
            let transfer = self.transfer.lock().expect("transfer slot poisoned").take();
            if let Some(transfer) = transfer {
                let free = transfer.inner.state.try_lock().is_ok();
                *self.lock_was_free.lock().expect("result poisoned") = Some(free);
            }
        }
    }

    /// Parks the only read of an upload, ends the upload with `end`, and asserts that the
    /// caller's stream was dropped with the upload-state lock free.
    ///
    /// No other work holds the part reader, so the state that `end` replaces holds the last owner
    /// of the stream.
    async fn assert_stream_dropped_outside_state_lock(end: impl FnOnce(&UploadTransfer)) {
        let slot = Arc::new(Mutex::new(None));
        let lock_was_free = Arc::new(Mutex::new(None));
        let stream = ChecksStateLockOnDrop {
            transfer: Arc::clone(&slot),
            lock_was_free: Arc::clone(&lock_was_free),
        };
        let transfer = create_test_transfer_with_stream(
            mock_s3_client_for_mpu(),
            InputStream::from_part_stream(stream),
        );
        *slot.lock().expect("transfer slot poisoned") = Some(transfer.clone());
        park_one_source_read(&transfer).await;

        end(&transfer);

        assert_eq!(
            *lock_was_free.lock().expect("result poisoned"),
            Some(true),
            "the stream was not dropped, or was dropped under the upload-state lock"
        );
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// Aborting an upload drops the caller's stream after the upload-state lock is released. The
    /// stream's `Drop` is caller code, and must not run under the lock.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn abort_drops_the_stream_outside_the_state_lock() {
        assert_stream_dropped_outside_state_lock(UploadTransfer::request_abort).await;
    }

    // ref: awslabs/aws-s3-transfer-manager-rs#226
    /// Ending the transfer from outside drops the caller's stream after the upload-state lock is
    /// released.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn termination_drops_the_stream_outside_the_state_lock() {
        assert_stream_dropped_outside_state_lock(|transfer| {
            transfer.ctx().set_cancelled();
            Transfer::on_terminal(transfer);
        })
        .await;
    }
}
