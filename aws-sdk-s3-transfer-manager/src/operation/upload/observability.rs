/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload state-machine observation and diagnostic emission.
//!
//! [`UploadObservability`] receives state-machine transitions and request
//! measurements from PutObject and multipart uploads. The disabled variant
//! avoids allocation, clocks, counters, and transition snapshots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::TransferDiagnosticsConfig;
use crate::error::ErrorKind;
use crate::io::SizeHint;
use crate::metrics::RequestMetrics;
use crate::operation::upload::context::PartTransferPendingReason;
use crate::transfer::{
    PendingCategory, RequestMeasurement, TransferContext, TransferId, TransferPendingStats,
};
use crate::types::{TransferMetrics, TransferStatus};

/// Multipart state sampled while the upload-state lock is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartTransferSnapshot {
    pub(crate) parts_dispatched: u64,
    pub(crate) parts_in_flight: usize,
    pub(crate) uploads_in_flight: usize,
    pub(crate) pending_reads: usize,
    pub(crate) completed_parts: usize,
    pub(crate) bytes_uploaded: u64,
    pub(crate) eof: bool,
    pub(crate) dispatch_closed: bool,
}

/// Aggregate multipart state included in an upload terminal summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartTransferSummary {
    pub(crate) snapshot: PartTransferSnapshot,
    pub(crate) segmented_parts: u64,
    pub(crate) presentation_segments: u64,
    pub(crate) max_presentation_segments: usize,
    pub(crate) max_parts_in_flight: usize,
    pub(crate) max_uploads_in_flight: usize,
    pub(crate) create_mpu_duration: Duration,
    pub(crate) source_read_duration: Duration,
    pub(crate) max_source_read_duration: Duration,
    pub(crate) read_pending_polls: u64,
    pub(crate) read_pending_parts: u64,
    pub(crate) read_pending_duration: Duration,
    pub(crate) upload_request_duration: Duration,
    pub(crate) max_upload_request_duration: Duration,
    pub(crate) body_duration: Duration,
    pub(crate) drain_duration: Duration,
    pub(crate) complete_mpu_request_duration: Duration,
    pub(crate) complete_mpu_duration: Duration,
}

/// Logical S3 request issued by one upload transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadRequestKind {
    /// Single-request object upload.
    PutObject,
    /// Multipart upload creation.
    CreateMultipartUpload,
    /// One multipart object part.
    UploadPart,
    /// Multipart upload completion.
    CompleteMultipartUpload,
}

/// Upload execution strategy selected from source size and stream behavior.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum UploadMode {
    /// The state machine has not selected an S3 upload operation.
    #[default]
    Undecided,
    /// The object is sent with one PutObject request.
    PutObject,
    /// The object is assembled with multipart upload requests.
    Multipart,
}

/// Shape of the source byte declaration captured before polling begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadLengthKind {
    /// Lower and upper bounds are equal.
    Exact,
    /// The source has an upper bound but not an exact length.
    Bounded,
    /// The source has no upper bound.
    Unbounded,
}

impl UploadLengthKind {
    fn from_size_hint(size_hint: SizeHint) -> Self {
        match size_hint.upper() {
            Some(upper) if upper == size_hint.lower() => Self::Exact,
            Some(_) => Self::Bounded,
            None => Self::Unbounded,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Bounded => "bounded",
            Self::Unbounded => "unbounded",
        }
    }
}

impl UploadMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Undecided => "undecided",
            Self::PutObject => "put_object",
            Self::Multipart => "multipart",
        }
    }
}

/// Terminal status emitted once for an upload transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadTerminalOutcome {
    /// The upload completed successfully.
    Completed,
    /// The upload failed.
    Failed,
    /// The upload was cancelled.
    Cancelled,
}

impl UploadTerminalOutcome {
    fn from_status(status: TransferStatus) -> Option<Self> {
        match status {
            TransferStatus::Completed => Some(Self::Completed),
            TransferStatus::Failed => Some(Self::Failed),
            TransferStatus::Cancelled => Some(Self::Cancelled),
            TransferStatus::Active => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Closed diagnostic vocabulary for upload state-machine transitions.
///
/// Each variant maps to one stable `transition` and `reason` field pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadTransition {
    /// PutObject work was selected for the source.
    PutObjectScheduled,
    /// CreateMultipartUpload work was selected for the source.
    MultipartUploadScheduled,
    /// CreateMultipartUpload completed and part transfer can begin.
    MultipartUploadCreated,
    /// A new source operation was scheduled.
    NewPartScheduled,
    /// A retained source operation was rescheduled after notification.
    SourceWakeScheduled,
    /// The source reported end-of-stream and closed further dispatch.
    SourceExhausted,
    /// The zero-byte part required for an empty MPU object was scheduled.
    EmptyObjectPartScheduled,
    /// Dispatch is closed and every scheduled operation has retired.
    CompletionReady,
    /// No upload-part work can be dispatched until multipart state changes.
    Pending(PartTransferPendingReason),
    /// A speculative dispatch was retracted after a custom source blocked.
    CustomSourceBlocked,
    /// CompleteMultipartUpload is ready to run.
    MultipartCompletionStarted,
    /// The upload reached one terminal outcome.
    Terminal(UploadTerminalOutcome),
}

impl UploadTransition {
    fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::PutObjectScheduled => ("work_scheduled", "put_object"),
            Self::MultipartUploadScheduled => ("work_scheduled", "create_multipart_upload"),
            Self::MultipartUploadCreated => ("state_changed", "multipart_upload_created"),
            Self::NewPartScheduled => ("work_scheduled", "new_part"),
            Self::SourceWakeScheduled => ("work_scheduled", "source_wake"),
            Self::SourceExhausted => ("dispatch_closed", "source_eof"),
            Self::EmptyObjectPartScheduled => ("work_scheduled", "empty_object"),
            Self::CompletionReady => ("completion_ready", "multipart_drained"),
            Self::Pending(PartTransferPendingReason::SourceUnavailable) => {
                ("poll_pending", "source_unavailable")
            }
            Self::Pending(PartTransferPendingReason::DispatchClosed) => {
                ("poll_pending", "dispatch_closed")
            }
            Self::Pending(PartTransferPendingReason::NoReadyPart) => {
                ("poll_pending", "no_ready_part")
            }
            Self::CustomSourceBlocked => ("work_retracted", "custom_source_blocked"),
            Self::MultipartCompletionStarted => ("request_started", "complete_multipart_upload"),
            Self::Terminal(UploadTerminalOutcome::Completed) => ("terminal", "completed"),
            Self::Terminal(UploadTerminalOutcome::Failed) => ("terminal", "failed"),
            Self::Terminal(UploadTerminalOutcome::Cancelled) => ("terminal", "cancelled"),
        }
    }
}

/// Optional clock sample whose construction is controlled by diagnostic policy.
#[derive(Debug)]
pub(crate) struct UploadDiagnosticTimer(Option<Instant>);

impl UploadDiagnosticTimer {
    /// Starts a timer only when summary collection is enabled.
    pub(crate) fn start(config: TransferDiagnosticsConfig) -> Self {
        Self(config.enable_summaries().then(Instant::now))
    }

    /// Returns elapsed time when this timer was enabled.
    pub(crate) fn elapsed(self) -> Option<Duration> {
        self.0.map(|started_at| started_at.elapsed())
    }
}

/// Timing retained with one scheduled source operation.
#[derive(Debug)]
pub(crate) struct UploadPartTiming {
    scheduled_at: Option<Instant>,
    pending_polls: u32,
}

impl UploadPartTiming {
    fn disabled() -> Self {
        Self {
            scheduled_at: None,
            pending_polls: 0,
        }
    }

    fn started() -> Self {
        Self {
            scheduled_at: Some(Instant::now()),
            pending_polls: 0,
        }
    }

    /// Records one source poll that returned `Pending`.
    pub(crate) fn record_pending(&mut self) {
        if self.scheduled_at.is_some() {
            self.pending_polls = self.pending_polls.saturating_add(1);
        }
    }

    fn finish(self) -> SourceReadObservation {
        self.observation()
    }

    /// Returns the current observation without consuming retained timing state.
    pub(crate) fn observation(&self) -> SourceReadObservation {
        SourceReadObservation {
            elapsed: self.scheduled_at.map(|scheduled_at| scheduled_at.elapsed()),
            pending_polls: self.pending_polls,
        }
    }
}

/// Source-read measurements transferred into aggregate upload observation.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SourceReadObservation {
    elapsed: Option<Duration>,
    pending_polls: u32,
}

impl SourceReadObservation {
    /// Returns elapsed source-read time, or zero when diagnostics are disabled.
    pub(crate) fn elapsed(self) -> Duration {
        self.elapsed.unwrap_or(Duration::ZERO)
    }

    /// Returns the number of source polls that returned `Pending`.
    pub(crate) fn pending_polls(self) -> u32 {
        self.pending_polls
    }
}

/// Per-request-kind metrics retained for an upload terminal summary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct UploadRequestMetrics {
    pub(crate) put_object: RequestMetrics,
    pub(crate) create_multipart_upload: RequestMetrics,
    pub(crate) upload_part: RequestMetrics,
    pub(crate) complete_multipart_upload: RequestMetrics,
}

impl UploadRequestMetrics {
    fn record(&mut self, kind: UploadRequestKind, metrics: &RequestMetrics) {
        let target = match kind {
            UploadRequestKind::PutObject => &mut self.put_object,
            UploadRequestKind::CreateMultipartUpload => &mut self.create_multipart_upload,
            UploadRequestKind::UploadPart => &mut self.upload_part,
            UploadRequestKind::CompleteMultipartUpload => &mut self.complete_multipart_upload,
        };
        *target += metrics;
    }
}

/// Complete diagnostic report for one upload transfer.
#[derive(Clone, Debug)]
pub(crate) struct UploadTransferSummary {
    pub(crate) outcome: UploadTerminalOutcome,
    pub(crate) mode: UploadMode,
    pub(crate) length_kind: UploadLengthKind,
    pub(crate) error_kind: Option<ErrorKind>,
    pub(crate) elapsed: Duration,
    pub(crate) metrics: TransferMetrics,
    pub(crate) requests: UploadRequestMetrics,
    pub(crate) request_total: RequestMetrics,
    pub(crate) pending: TransferPendingStats,
    pub(crate) multipart: Option<PartTransferSummary>,
}

/// Optional upload observation kept separate from correctness state.
#[derive(Clone, Debug)]
pub(crate) enum UploadObservability {
    Disabled,
    Enabled(Arc<EnabledUploadObservability>),
}

#[derive(Debug)]
pub(crate) struct EnabledUploadObservability {
    transitions: bool,
    terminal_reported: AtomicBool,
    state: Mutex<UploadObservabilityState>,
    #[cfg(test)]
    last_summary: Mutex<Option<UploadTransferSummary>>,
}

#[derive(Clone, Debug)]
struct UploadObservabilityState {
    mode: UploadMode,
    length_kind: UploadLengthKind,
    multipart_started_at: Option<Instant>,
    dispatch_closed_at: Option<Instant>,
    body_duration: Option<Duration>,
    drain_duration: Duration,
    uploads_in_flight: usize,
    segmented_parts: u64,
    presentation_segments: u64,
    max_presentation_segments: usize,
    max_parts_in_flight: usize,
    max_uploads_in_flight: usize,
    source_read_duration: Duration,
    max_source_read_duration: Duration,
    read_pending_polls: u64,
    read_pending_parts: u64,
    read_pending_duration: Duration,
    complete_mpu_duration: Duration,
    latest_part_snapshot: Option<PartTransferSnapshot>,
    requests: UploadRequestMetrics,
}

impl UploadObservability {
    /// Creates upload observation using the client-level diagnostic policy.
    pub(crate) fn new(config: TransferDiagnosticsConfig, size_hint: SizeHint) -> Self {
        if !config.enable_summaries() {
            return Self::Disabled;
        }
        Self::Enabled(Arc::new(EnabledUploadObservability {
            transitions: config.enable_transitions(),
            terminal_reported: AtomicBool::new(false),
            state: Mutex::new(UploadObservabilityState {
                mode: UploadMode::Undecided,
                length_kind: UploadLengthKind::from_size_hint(size_hint),
                multipart_started_at: None,
                dispatch_closed_at: None,
                body_duration: None,
                drain_duration: Duration::ZERO,
                uploads_in_flight: 0,
                segmented_parts: 0,
                presentation_segments: 0,
                max_presentation_segments: 0,
                max_parts_in_flight: 0,
                max_uploads_in_flight: 0,
                source_read_duration: Duration::ZERO,
                max_source_read_duration: Duration::ZERO,
                read_pending_polls: 0,
                read_pending_parts: 0,
                read_pending_duration: Duration::ZERO,
                complete_mpu_duration: Duration::ZERO,
                latest_part_snapshot: None,
                requests: UploadRequestMetrics::default(),
            }),
            #[cfg(test)]
            last_summary: Mutex::new(None),
        }))
    }

    /// Selects the request strategy used by this upload.
    pub(crate) fn select_mode(&self, mode: UploadMode) {
        if let Self::Enabled(enabled) = self {
            enabled.state.lock().expect("lock poisoned").mode = mode;
        }
    }

    /// Records the start of multipart source and UploadPart processing.
    pub(crate) fn multipart_created(&self) {
        if let Self::Enabled(enabled) = self {
            enabled
                .state
                .lock()
                .expect("lock poisoned")
                .multipart_started_at
                .get_or_insert_with(Instant::now);
        }
    }

    /// Starts a request measurement attributed to one upload request kind.
    pub(crate) fn start_request<'a>(
        &'a self,
        ctx: &TransferContext,
        kind: UploadRequestKind,
    ) -> UploadRequestMeasurement<'a> {
        UploadRequestMeasurement {
            measurement: Some(ctx.start_request_metrics()),
            observability: self,
            kind,
        }
    }

    fn record_request(&self, kind: UploadRequestKind, metrics: &RequestMetrics) {
        if let Self::Enabled(enabled) = self {
            enabled
                .state
                .lock()
                .expect("lock poisoned")
                .requests
                .record(kind, metrics);
        }
    }

    /// Creates timing state for one newly scheduled source operation.
    pub(crate) fn part_scheduled(&self) -> UploadPartTiming {
        match self {
            Self::Disabled => UploadPartTiming::disabled(),
            Self::Enabled(_) => UploadPartTiming::started(),
        }
    }

    /// Records one source poll that returned `Pending`.
    pub(crate) fn record_read_pending(&self, timing: &mut UploadPartTiming) {
        timing.record_pending();
        if let Self::Enabled(enabled) = self {
            let mut state = enabled.state.lock().expect("lock poisoned");
            state.read_pending_polls = state.read_pending_polls.saturating_add(1);
        }
    }

    /// Records the point at which multipart dispatch closes or reopens.
    pub(crate) fn set_dispatch_closed(&self, closed: bool) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        if closed {
            if state.dispatch_closed_at.is_none() {
                state.dispatch_closed_at = Some(Instant::now());
            }
        } else {
            state.dispatch_closed_at = None;
        }
    }

    /// Moves one source operation into an UploadPart request.
    pub(crate) fn begin_upload(
        &self,
        presentation_segments: usize,
        timing: UploadPartTiming,
    ) -> SourceReadObservation {
        let observation = timing.finish();
        let Self::Enabled(enabled) = self else {
            return observation;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");

        state.uploads_in_flight = state
            .uploads_in_flight
            .checked_add(1)
            .expect("diagnostic upload count overflow");
        state.max_uploads_in_flight = state.max_uploads_in_flight.max(state.uploads_in_flight);
        if presentation_segments > 1 {
            state.segmented_parts = state.segmented_parts.saturating_add(1);
        }
        state.presentation_segments = state
            .presentation_segments
            .saturating_add(presentation_segments as u64);
        state.max_presentation_segments =
            state.max_presentation_segments.max(presentation_segments);
        if let Some(elapsed) = observation.elapsed {
            state.source_read_duration = state.source_read_duration.saturating_add(elapsed);
            state.max_source_read_duration = state.max_source_read_duration.max(elapsed);
            if observation.pending_polls != 0 {
                state.read_pending_parts = state.read_pending_parts.saturating_add(1);
                state.read_pending_duration = state.read_pending_duration.saturating_add(elapsed);
            }
        }
        observation
    }

    /// Records one retired UploadPart request.
    pub(crate) fn complete_upload(&self) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.uploads_in_flight = state
            .uploads_in_flight
            .checked_sub(1)
            .expect("diagnostic upload completion underflow");
    }

    /// Captures multipart body and drain durations before completion begins.
    pub(crate) fn finish_body(&self) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        let now = Instant::now();
        state.body_duration = state
            .multipart_started_at
            .map(|started_at| now.saturating_duration_since(started_at));
        state.drain_duration = state
            .dispatch_closed_at
            .map(|closed_at| now.saturating_duration_since(closed_at))
            .unwrap_or(Duration::ZERO);
    }

    /// Returns the observed UploadPart request count.
    pub(crate) fn uploads_in_flight(&self) -> usize {
        match self {
            Self::Disabled => 0,
            Self::Enabled(enabled) => {
                enabled
                    .state
                    .lock()
                    .expect("lock poisoned")
                    .uploads_in_flight
            }
        }
    }

    /// Records the latest multipart state and returns it at detail level two.
    pub(crate) fn transition_snapshot(
        &self,
        snapshot: PartTransferSnapshot,
    ) -> Option<PartTransferSnapshot> {
        match self {
            Self::Disabled => None,
            Self::Enabled(enabled) => {
                let mut state = enabled.state.lock().expect("lock poisoned");
                state.latest_part_snapshot = Some(snapshot);
                state.max_parts_in_flight = state.max_parts_in_flight.max(snapshot.parts_in_flight);
                enabled.transitions.then_some(snapshot)
            }
        }
    }

    /// Records the completed multipart state.
    pub(crate) fn finish_multipart(
        &self,
        snapshot: PartTransferSnapshot,
        complete_duration: Option<Duration>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.latest_part_snapshot = Some(snapshot);
        state.complete_mpu_duration = complete_duration.unwrap_or(Duration::ZERO);
    }

    fn multipart_summary(state: &UploadObservabilityState) -> Option<PartTransferSummary> {
        let snapshot = state.latest_part_snapshot?;
        Some(PartTransferSummary {
            snapshot,
            segmented_parts: state.segmented_parts,
            presentation_segments: state.presentation_segments,
            max_presentation_segments: state.max_presentation_segments,
            max_parts_in_flight: state.max_parts_in_flight,
            max_uploads_in_flight: state.max_uploads_in_flight,
            create_mpu_duration: state.requests.create_multipart_upload.elapsed,
            source_read_duration: state.source_read_duration,
            max_source_read_duration: state.max_source_read_duration,
            read_pending_polls: state.read_pending_polls,
            read_pending_parts: state.read_pending_parts,
            read_pending_duration: state.read_pending_duration,
            upload_request_duration: state.requests.upload_part.elapsed,
            max_upload_request_duration: state.requests.upload_part.max_elapsed,
            body_duration: state.body_duration.unwrap_or(Duration::ZERO),
            drain_duration: state.drain_duration,
            complete_mpu_request_duration: state.requests.complete_multipart_upload.elapsed,
            complete_mpu_duration: state.complete_mpu_duration,
        })
    }

    /// Emits one upload state transition at diagnostic detail level two.
    pub(crate) fn publish_transition(
        &self,
        transfer_id: TransferId,
        transition: UploadTransition,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_transition(transfer_id, transition, snapshot);
        }
    }

    /// Emits one retained source operation at diagnostic detail level two.
    pub(crate) fn publish_source_pending(
        &self,
        transfer_id: TransferId,
        observation: SourceReadObservation,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_source_pending(transfer_id, observation, snapshot);
        }
    }

    /// Emits one UploadPart request start at diagnostic detail level two.
    pub(crate) fn publish_part_started(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        observation: SourceReadObservation,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_part_started(
                transfer_id,
                part_number,
                bytes_sent,
                presentation_segments,
                observation,
                snapshot,
            );
        }
    }

    /// Emits one failed UploadPart request at diagnostic detail level two.
    pub(crate) fn publish_part_failed(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        request_duration: Duration,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_part_failed(
                transfer_id,
                part_number,
                bytes_sent,
                presentation_segments,
                request_duration,
                snapshot,
            );
        }
    }

    /// Emits one completed UploadPart request at diagnostic detail level two.
    pub(crate) fn publish_part_completed(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        request_duration: Duration,
        snapshot: Option<PartTransferSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_part_completed(
                transfer_id,
                part_number,
                bytes_sent,
                presentation_segments,
                request_duration,
                snapshot,
            );
        }
    }

    /// Finalizes and emits the upload terminal summary once.
    pub(crate) fn report_terminal(
        &self,
        transfer_id: TransferId,
        status: TransferStatus,
        error_kind: Option<ErrorKind>,
        metrics: TransferMetrics,
        request_total: RequestMetrics,
        pending: TransferPendingStats,
    ) -> Option<UploadTransferSummary> {
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let outcome = UploadTerminalOutcome::from_status(status)?;
        if enabled.terminal_reported.swap(true, Ordering::AcqRel) {
            return None;
        }

        let state = enabled.state.lock().expect("lock poisoned");
        let summary = UploadTransferSummary {
            outcome,
            mode: state.mode,
            length_kind: state.length_kind,
            error_kind,
            elapsed: metrics
                .finished_at
                .map(|finished_at| finished_at.saturating_duration_since(metrics.started_at))
                .unwrap_or_default(),
            metrics,
            requests: state.requests,
            request_total,
            pending,
            multipart: Self::multipart_summary(&state),
        };
        drop(state);

        if enabled.transitions {
            emit_transition(
                transfer_id,
                UploadTransition::Terminal(outcome),
                summary.multipart.map(|multipart| multipart.snapshot),
            );
        }
        #[cfg(test)]
        {
            *enabled.last_summary.lock().expect("lock poisoned") = Some(summary.clone());
        }
        emit_terminal_summary(transfer_id, &summary);
        Some(summary)
    }

    /// Returns a point-in-time multipart summary for state-machine tests.
    #[cfg(test)]
    pub(crate) fn test_summary(
        &self,
        snapshot: PartTransferSnapshot,
    ) -> Option<PartTransferSummary> {
        self.finish_body();
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.latest_part_snapshot = Some(snapshot);
        Self::multipart_summary(&state)
    }

    /// Returns the terminal report emitted by this observer.
    #[cfg(test)]
    pub(crate) fn test_terminal_summary(&self) -> Option<UploadTransferSummary> {
        match self {
            Self::Disabled => None,
            Self::Enabled(enabled) => enabled.last_summary.lock().expect("lock poisoned").clone(),
        }
    }
}

/// In-progress request measurement attributed to one upload request kind.
pub(crate) struct UploadRequestMeasurement<'a> {
    measurement: Option<RequestMeasurement>,
    observability: &'a UploadObservability,
    kind: UploadRequestKind,
}

impl UploadRequestMeasurement<'_> {
    /// Returns the request metrics updated by the retry loop.
    pub(crate) fn metrics_mut(&mut self) -> &mut RequestMetrics {
        self.measurement
            .as_mut()
            .expect("request measurement already finished")
            .metrics_mut()
    }

    /// Publishes the request measurement to transfer and upload aggregates.
    pub(crate) fn finish(mut self) -> RequestMetrics {
        self.publish()
    }

    fn publish(&mut self) -> RequestMetrics {
        let measurement = self
            .measurement
            .take()
            .expect("request measurement already finished");
        let metrics = measurement.finish();
        self.observability.record_request(self.kind, &metrics);
        metrics
    }
}

impl Drop for UploadRequestMeasurement<'_> {
    fn drop(&mut self) {
        if self.measurement.is_some() {
            let _ = self.publish();
        }
    }
}

fn emit_transition(
    transfer_id: TransferId,
    transition: UploadTransition,
    snapshot: Option<PartTransferSnapshot>,
) {
    let (transition, reason) = transition.fields();
    if let Some(snapshot) = snapshot {
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            transition,
            reason,
            parts_dispatched = snapshot.parts_dispatched,
            parts_in_flight = snapshot.parts_in_flight,
            uploads_in_flight = snapshot.uploads_in_flight,
            pending_reads = snapshot.pending_reads,
            completed_parts = snapshot.completed_parts,
            bytes_uploaded = snapshot.bytes_uploaded,
            eof = snapshot.eof,
            dispatch_closed = snapshot.dispatch_closed,
            "upload state transition",
        );
    } else {
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            transition,
            reason,
            "upload state transition",
        );
    }
}

fn emit_source_pending(
    transfer_id: TransferId,
    observation: SourceReadObservation,
    snapshot: Option<PartTransferSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        pending_polls = observation.pending_polls(),
        scheduled_elapsed_us = duration_micros(observation.elapsed()),
        parts_dispatched = snapshot.parts_dispatched,
        parts_in_flight = snapshot.parts_in_flight,
        uploads_in_flight = snapshot.uploads_in_flight,
        pending_reads = snapshot.pending_reads,
        completed_parts = snapshot.completed_parts,
        dispatch_closed = snapshot.dispatch_closed,
        "upload source read pending",
    );
}

fn emit_part_started(
    transfer_id: TransferId,
    part_number: u64,
    bytes_sent: u64,
    presentation_segments: usize,
    observation: SourceReadObservation,
    snapshot: Option<PartTransferSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        part_number,
        bytes_sent,
        source_read_elapsed_us = duration_micros(observation.elapsed()),
        pending_polls = observation.pending_polls(),
        presentation_segments,
        body_mode = body_mode(presentation_segments),
        parts_in_flight = snapshot.parts_in_flight,
        uploads_in_flight = snapshot.uploads_in_flight,
        pending_reads = snapshot.pending_reads,
        completed_parts = snapshot.completed_parts,
        dispatch_closed = snapshot.dispatch_closed,
        "upload part request started",
    );
}

fn emit_part_failed(
    transfer_id: TransferId,
    part_number: u64,
    bytes_sent: u64,
    presentation_segments: usize,
    request_duration: Duration,
    snapshot: Option<PartTransferSnapshot>,
) {
    let Some(_snapshot) = snapshot else {
        return;
    };
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        part_number,
        bytes_sent,
        presentation_segments,
        body_mode = body_mode(presentation_segments),
        request_elapsed_us = duration_micros(request_duration),
        "upload part request failed",
    );
}

fn emit_part_completed(
    transfer_id: TransferId,
    part_number: u64,
    bytes_sent: u64,
    presentation_segments: usize,
    request_duration: Duration,
    snapshot: Option<PartTransferSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        part_number,
        bytes_sent,
        presentation_segments,
        body_mode = body_mode(presentation_segments),
        request_elapsed_us = duration_micros(request_duration),
        parts_in_flight = snapshot.parts_in_flight,
        uploads_in_flight = snapshot.uploads_in_flight,
        pending_reads = snapshot.pending_reads,
        completed_parts = snapshot.completed_parts,
        dispatch_closed = snapshot.dispatch_closed,
        "upload part request completed",
    );
}

fn emit_terminal_summary(transfer_id: TransferId, summary: &UploadTransferSummary) {
    let source_pending = summary.pending.category(PendingCategory::Source);
    let memory_pending = summary.pending.category(PendingCategory::Memory);
    let consumer_pending = summary.pending.category(PendingCategory::Consumer);
    let work_pending = summary.pending.category(PendingCategory::InFlightWork);
    let other_pending = summary.pending.category(PendingCategory::Other);
    let terminal_category = summary
        .pending
        .terminal_cause
        .map(|cause| cause.category.as_str());
    let terminal_reason = summary.pending.terminal_cause.map(|cause| cause.reason);

    tracing::debug!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        outcome = summary.outcome.as_str(),
        mode = summary.mode.as_str(),
        source_length = summary.length_kind.as_str(),
        error_kind = ?summary.error_kind,
        elapsed_us = duration_micros(summary.elapsed),
        expected_bytes = summary.metrics.total_bytes,
        network_tx = summary.metrics.network_tx,
        disk_read = summary.metrics.disk_read,
        requests = summary.request_total.requests,
        request_elapsed_us = duration_micros(summary.request_total.elapsed),
        max_request_elapsed_us = duration_micros(summary.request_total.max_elapsed),
        retry_reissues = summary.request_total.retry_reissues,
        throttle_reissues = summary.request_total.throttle_reissues,
        hedge_reissues = summary.request_total.hedge_reissues,
        retry_exhaustions = summary.request_total.retry_exhaustions,
        backoff_duration_us = duration_micros(summary.request_total.backoff_duration),
        put_object_requests = summary.requests.put_object.requests,
        put_object_elapsed_us = duration_micros(summary.requests.put_object.elapsed),
        put_object_max_elapsed_us = duration_micros(summary.requests.put_object.max_elapsed),
        put_object_retry_reissues = summary.requests.put_object.retry_reissues,
        put_object_throttle_reissues = summary.requests.put_object.throttle_reissues,
        put_object_hedge_reissues = summary.requests.put_object.hedge_reissues,
        put_object_retry_exhaustions = summary.requests.put_object.retry_exhaustions,
        put_object_backoff_us = duration_micros(summary.requests.put_object.backoff_duration),
        create_mpu_requests = summary.requests.create_multipart_upload.requests,
        create_mpu_elapsed_us = duration_micros(summary.requests.create_multipart_upload.elapsed),
        create_mpu_max_elapsed_us =
            duration_micros(summary.requests.create_multipart_upload.max_elapsed),
        create_mpu_retry_reissues = summary.requests.create_multipart_upload.retry_reissues,
        create_mpu_throttle_reissues =
            summary.requests.create_multipart_upload.throttle_reissues,
        create_mpu_hedge_reissues = summary.requests.create_multipart_upload.hedge_reissues,
        create_mpu_retry_exhaustions =
            summary.requests.create_multipart_upload.retry_exhaustions,
        create_mpu_backoff_us =
            duration_micros(summary.requests.create_multipart_upload.backoff_duration),
        upload_part_requests = summary.requests.upload_part.requests,
        upload_part_elapsed_us = duration_micros(summary.requests.upload_part.elapsed),
        upload_part_max_elapsed_us = duration_micros(summary.requests.upload_part.max_elapsed),
        upload_part_retry_reissues = summary.requests.upload_part.retry_reissues,
        upload_part_throttle_reissues = summary.requests.upload_part.throttle_reissues,
        upload_part_hedge_reissues = summary.requests.upload_part.hedge_reissues,
        upload_part_retry_exhaustions = summary.requests.upload_part.retry_exhaustions,
        upload_part_backoff_us = duration_micros(summary.requests.upload_part.backoff_duration),
        complete_mpu_requests = summary.requests.complete_multipart_upload.requests,
        complete_mpu_elapsed_us =
            duration_micros(summary.requests.complete_multipart_upload.elapsed),
        complete_mpu_max_elapsed_us =
            duration_micros(summary.requests.complete_multipart_upload.max_elapsed),
        complete_mpu_retry_reissues =
            summary.requests.complete_multipart_upload.retry_reissues,
        complete_mpu_throttle_reissues =
            summary.requests.complete_multipart_upload.throttle_reissues,
        complete_mpu_hedge_reissues =
            summary.requests.complete_multipart_upload.hedge_reissues,
        complete_mpu_retry_exhaustions =
            summary.requests.complete_multipart_upload.retry_exhaustions,
        complete_mpu_backoff_us =
            duration_micros(summary.requests.complete_multipart_upload.backoff_duration),
        source_pending = source_pending.count,
        source_pending_us = duration_micros(source_pending.pending_to_repoll),
        source_max_pending_us = duration_micros(source_pending.max_pending_to_repoll),
        memory_pending = memory_pending.count,
        memory_pending_us = duration_micros(memory_pending.pending_to_repoll),
        memory_max_pending_us = duration_micros(memory_pending.max_pending_to_repoll),
        consumer_pending = consumer_pending.count,
        consumer_pending_us = duration_micros(consumer_pending.pending_to_repoll),
        consumer_max_pending_us = duration_micros(consumer_pending.max_pending_to_repoll),
        in_flight_work_pending = work_pending.count,
        in_flight_work_pending_us = duration_micros(work_pending.pending_to_repoll),
        in_flight_work_max_pending_us = duration_micros(work_pending.max_pending_to_repoll),
        other_pending = other_pending.count,
        other_pending_us = duration_micros(other_pending.pending_to_repoll),
        other_max_pending_us = duration_micros(other_pending.max_pending_to_repoll),
        terminal_pending_category = terminal_category,
        terminal_pending_reason = terminal_reason,
        multipart_parts = summary.multipart.map(|part| part.snapshot.completed_parts),
        multipart_bytes = summary.multipart.map(|part| part.snapshot.bytes_uploaded),
        segmented_parts = summary.multipart.map(|part| part.segmented_parts),
        presentation_segments = summary.multipart.map(|part| part.presentation_segments),
        max_presentation_segments =
            summary.multipart.map(|part| part.max_presentation_segments),
        max_parts_in_flight = summary.multipart.map(|part| part.max_parts_in_flight),
        max_uploads_in_flight = summary.multipart.map(|part| part.max_uploads_in_flight),
        create_mpu_duration_us =
            summary.multipart.map(|part| duration_micros(part.create_mpu_duration)),
        source_read_duration_us =
            summary.multipart.map(|part| duration_micros(part.source_read_duration)),
        max_source_read_duration_us =
            summary.multipart.map(|part| duration_micros(part.max_source_read_duration)),
        read_pending_polls = summary.multipart.map(|part| part.read_pending_polls),
        read_pending_parts = summary.multipart.map(|part| part.read_pending_parts),
        read_pending_duration_us =
            summary.multipart.map(|part| duration_micros(part.read_pending_duration)),
        upload_request_duration_us =
            summary.multipart.map(|part| duration_micros(part.upload_request_duration)),
        max_upload_request_duration_us =
            summary.multipart.map(|part| duration_micros(part.max_upload_request_duration)),
        body_duration_us = summary.multipart.map(|part| duration_micros(part.body_duration)),
        drain_duration_us = summary.multipart.map(|part| duration_micros(part.drain_duration)),
        complete_mpu_request_duration_us =
            summary.multipart.map(|part| duration_micros(part.complete_mpu_request_duration)),
        complete_mpu_duration_us =
            summary.multipart.map(|part| duration_micros(part.complete_mpu_duration)),
        "upload transfer terminal",
    );
}

/// Converts a duration to the integer unit used by diagnostic events.
pub(crate) fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn body_mode(presentation_segments: usize) -> &'static str {
    if presentation_segments <= 1 {
        "contiguous"
    } else {
        "segmented"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(detail: u64) -> TransferDiagnosticsConfig {
        crate::config::DiagnosticsConfig::for_test(
            crate::config::MemoryDiagnosticsConfig::default(),
            detail,
        )
        .transfer()
    }

    fn exact_size_hint() -> SizeHint {
        SizeHint::exact(8 * 1024 * 1024)
    }

    fn test_transfer_metrics() -> TransferMetrics {
        let now = Instant::now();
        TransferMetrics {
            network_tx: 0,
            network_rx: 0,
            disk_read: 0,
            disk_write: 0,
            total_bytes: Some(8 * 1024 * 1024),
            started_at: now,
            finished_at: Some(now),
        }
    }

    fn snapshot() -> PartTransferSnapshot {
        PartTransferSnapshot {
            parts_dispatched: 1,
            parts_in_flight: 0,
            uploads_in_flight: 0,
            pending_reads: 0,
            completed_parts: 1,
            bytes_uploaded: 8 * 1024 * 1024,
            eof: false,
            dispatch_closed: true,
        }
    }

    #[test]
    fn disabled_collection_does_not_start_timers_or_emit_snapshots() {
        let observability = UploadObservability::new(config(0), exact_size_hint());
        let timing = observability.part_scheduled();
        assert!(matches!(observability, UploadObservability::Disabled));
        assert_eq!(UploadDiagnosticTimer::start(config(0)).elapsed(), None);
        assert_eq!(timing.finish().elapsed, None);
        assert_eq!(observability.transition_snapshot(snapshot()), None);
    }

    #[test]
    fn summary_and_transition_levels_are_distinct() {
        let summary = UploadObservability::new(config(1), exact_size_hint());
        assert_eq!(summary.transition_snapshot(snapshot()), None);

        let transitions = UploadObservability::new(config(2), exact_size_hint());
        assert_eq!(
            transitions.transition_snapshot(snapshot()),
            Some(snapshot())
        );
    }

    #[test]
    fn summary_level_aggregates_without_transition_snapshots() {
        let observability = UploadObservability::new(config(1), exact_size_hint());
        observability.select_mode(UploadMode::Multipart);
        let mut timing = observability.part_scheduled();
        observability.record_read_pending(&mut timing);
        let observation = observability.begin_upload(3, timing);
        assert_eq!(observation.pending_polls(), 1);
        observability.complete_upload();
        observability.set_dispatch_closed(true);

        let mut create = RequestMetrics::default();
        create.record_request(Duration::from_micros(11));
        observability.record_request(UploadRequestKind::CreateMultipartUpload, &create);
        let mut upload = RequestMetrics::default();
        upload.record_request(Duration::from_micros(17));
        observability.record_request(UploadRequestKind::UploadPart, &upload);
        let mut complete = RequestMetrics::default();
        complete.record_request(Duration::from_micros(19));
        observability.record_request(UploadRequestKind::CompleteMultipartUpload, &complete);

        observability.finish_body();
        observability.finish_multipart(snapshot(), Some(Duration::from_micros(23)));
        let summary = observability
            .test_summary(snapshot())
            .expect("summary detail should produce a report");
        assert_eq!(summary.snapshot.uploads_in_flight, 0);
        assert_eq!(summary.segmented_parts, 1);
        assert_eq!(summary.presentation_segments, 3);
        assert_eq!(summary.max_presentation_segments, 3);
        assert_eq!(summary.create_mpu_duration, Duration::from_micros(11));
        assert_eq!(summary.read_pending_polls, 1);
        assert_eq!(summary.read_pending_parts, 1);
        assert_eq!(summary.upload_request_duration, Duration::from_micros(17));
        assert_eq!(
            summary.max_upload_request_duration,
            Duration::from_micros(17)
        );
        assert_eq!(
            summary.complete_mpu_request_duration,
            Duration::from_micros(19)
        );
        assert_eq!(summary.complete_mpu_duration, Duration::from_micros(23));
    }

    #[test]
    fn upload_transition_vocabulary_is_closed_and_stable() {
        assert_eq!(
            UploadTransition::PutObjectScheduled.fields(),
            ("work_scheduled", "put_object")
        );
        assert_eq!(
            UploadTransition::MultipartUploadScheduled.fields(),
            ("work_scheduled", "create_multipart_upload")
        );
        assert_eq!(
            UploadTransition::MultipartUploadCreated.fields(),
            ("state_changed", "multipart_upload_created")
        );
        assert_eq!(
            UploadTransition::NewPartScheduled.fields(),
            ("work_scheduled", "new_part")
        );
        assert_eq!(
            UploadTransition::SourceWakeScheduled.fields(),
            ("work_scheduled", "source_wake")
        );
        assert_eq!(
            UploadTransition::SourceExhausted.fields(),
            ("dispatch_closed", "source_eof")
        );
        assert_eq!(
            UploadTransition::EmptyObjectPartScheduled.fields(),
            ("work_scheduled", "empty_object")
        );
        assert_eq!(
            UploadTransition::CompletionReady.fields(),
            ("completion_ready", "multipart_drained")
        );
        assert_eq!(
            UploadTransition::Pending(PartTransferPendingReason::SourceUnavailable).fields(),
            ("poll_pending", "source_unavailable")
        );
        assert_eq!(
            UploadTransition::Pending(PartTransferPendingReason::DispatchClosed).fields(),
            ("poll_pending", "dispatch_closed")
        );
        assert_eq!(
            UploadTransition::Pending(PartTransferPendingReason::NoReadyPart).fields(),
            ("poll_pending", "no_ready_part")
        );
        assert_eq!(
            UploadTransition::CustomSourceBlocked.fields(),
            ("work_retracted", "custom_source_blocked")
        );
        assert_eq!(
            UploadTransition::MultipartCompletionStarted.fields(),
            ("request_started", "complete_multipart_upload")
        );
        assert_eq!(
            UploadTransition::Terminal(UploadTerminalOutcome::Completed).fields(),
            ("terminal", "completed")
        );
    }

    #[test]
    fn terminal_summary_is_reported_once() {
        let observability = UploadObservability::new(config(1), exact_size_hint());
        observability.select_mode(UploadMode::PutObject);
        let first = observability.report_terminal(
            TransferId {
                id: 1,
                parent: None,
            },
            TransferStatus::Completed,
            None,
            test_transfer_metrics(),
            RequestMetrics::default(),
            TransferPendingStats::default(),
        );
        let second = observability.report_terminal(
            TransferId {
                id: 1,
                parent: None,
            },
            TransferStatus::Completed,
            None,
            test_transfer_metrics(),
            RequestMetrics::default(),
            TransferPendingStats::default(),
        );
        assert_eq!(first.expect("first report").mode, UploadMode::PutObject);
        assert!(second.is_none());
    }

    #[test]
    fn request_kind_metrics_remain_separate() {
        let observability = UploadObservability::new(config(1), exact_size_hint());
        let mut upload_part = RequestMetrics::default();
        upload_part.record_request(Duration::from_micros(7));
        upload_part.record_retry_reissue(Duration::from_micros(3));
        observability.record_request(UploadRequestKind::UploadPart, &upload_part);

        let summary = observability
            .report_terminal(
                TransferId {
                    id: 2,
                    parent: None,
                },
                TransferStatus::Failed,
                Some(ErrorKind::RuntimeError),
                test_transfer_metrics(),
                upload_part,
                TransferPendingStats::default(),
            )
            .expect("terminal report");
        assert_eq!(summary.requests.upload_part, upload_part);
        assert_eq!(summary.requests.put_object, RequestMetrics::default());
    }
}
