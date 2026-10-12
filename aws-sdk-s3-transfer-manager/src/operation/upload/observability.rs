/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload state-machine observation and diagnostic emission.
//!
//! [`UploadObservability`] receives state-machine events and request
//! measurements from PutObject and multipart uploads. The disabled variant
//! avoids allocation, clocks, counters, and event snapshots.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::TransferDiagnosticsConfig;
use crate::error::ErrorKind;
use crate::io::SizeHint;
use crate::metrics::RequestMetrics;
use crate::transfer::{
    emit_pending_details, AttributedRequestMeasurement, RequestMetricsAttribution, TransferContext,
    TransferId, TransferPendingStats,
};
use crate::types::{TransferMetrics, TransferStatus};

/// Copyable execution-state projection of
/// [`UploadState`](super::context::UploadState).
///
/// `UploadState` remains authoritative for source ownership, multipart
/// bookkeeping, request futures, and terminal response construction. This enum
/// retains only its current state-machine state for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadExecutionState {
    /// The state machine has not selected PutObject or multipart upload.
    PendingInitialization,
    /// CreateMultipartUpload is in flight.
    CreateMultipartUploadInFlight,
    /// Source parts are being read and uploaded.
    Transferring,
    /// Multipart source work has drained and completion can be scheduled.
    MultipartCompletionPending,
    /// CompleteMultipartUpload is in flight.
    CompleteMultipartUploadInFlight,
    /// PutObject is in flight.
    PutObjectInFlight,
    /// No more upload work can be produced.
    Done,
}

impl UploadExecutionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PendingInitialization => "pending_initialization",
            Self::CreateMultipartUploadInFlight => "create_multipart_upload_in_flight",
            Self::Transferring => "transferring",
            Self::MultipartCompletionPending => "multipart_completion_pending",
            Self::CompleteMultipartUploadInFlight => "complete_multipart_upload_in_flight",
            Self::PutObjectInFlight => "put_object_in_flight",
            Self::Done => "done",
        }
    }
}

/// Diagnostic projection of [`UploadState`](super::context::UploadState)
/// sampled while the upload-state lock is held.
///
/// The snapshot combines the current execution state with multipart counters
/// owned by the authoritative upload state. Multipart counters are zero while
/// the upload is outside multipart execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UploadStateSnapshot {
    /// Current state-machine state projected from
    /// [`UploadState`](super::context::UploadState).
    pub(crate) state: UploadExecutionState,
    /// Multipart work admitted by the scheduler.
    pub(crate) parts_dispatched: u64,
    /// Source reads or UploadPart requests that have not retired.
    pub(crate) parts_in_flight: usize,
    /// UploadPart requests currently executing.
    pub(crate) uploads_in_flight: usize,
    /// Source reads retained after returning `Poll::Pending`.
    pub(crate) pending_reads: usize,
    /// UploadPart requests accepted by S3.
    pub(crate) completed_parts: usize,
    /// Bytes returned by source reads.
    pub(crate) bytes_read: u64,
    /// Bytes accepted by completed UploadPart requests.
    pub(crate) bytes_uploaded: u64,
    /// Whether a caller-provided source reported end-of-stream.
    pub(crate) eof: bool,
    /// Whether no additional source reads may be dispatched.
    pub(crate) dispatch_closed: bool,
}

impl UploadStateSnapshot {
    /// Returns a snapshot for an execution state without multipart counters.
    pub(crate) const fn inactive(state: UploadExecutionState) -> Self {
        Self {
            state,
            parts_dispatched: 0,
            parts_in_flight: 0,
            uploads_in_flight: 0,
            pending_reads: 0,
            completed_parts: 0,
            bytes_read: 0,
            bytes_uploaded: 0,
            eof: false,
            dispatch_closed: false,
        }
    }

    /// Reclassifies multipart counters after the state machine advances.
    pub(crate) const fn with_state(mut self, state: UploadExecutionState) -> Self {
        self.state = state;
        self
    }
}

/// Aggregate multipart state included in an upload terminal summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MultipartTransferSummary {
    /// Last active multipart state observed before terminal reporting.
    pub(crate) last_active_snapshot: UploadStateSnapshot,
    /// UploadPart requests accepted by S3.
    pub(crate) parts_completed: usize,
    /// Bytes returned by multipart source reads.
    pub(crate) bytes_read: u64,
    /// Bytes accepted by completed UploadPart requests.
    pub(crate) bytes_uploaded: u64,
    /// Parts whose request body used more than one presentation segment.
    pub(crate) segmented_parts: u64,
    /// Total presentation segments across all UploadPart requests.
    pub(crate) presentation_segments: u64,
    /// Largest presentation-segment count for one UploadPart request.
    pub(crate) max_presentation_segments: usize,
    /// Highest source-read or UploadPart work count.
    pub(crate) max_parts_in_flight: usize,
    /// Highest concurrently executing UploadPart request count.
    pub(crate) max_uploads_in_flight: usize,
    /// Total CreateMultipartUpload request time.
    pub(crate) create_mpu_duration: Duration,
    /// Total elapsed source-read time.
    pub(crate) source_read_duration: Duration,
    /// Longest elapsed source read.
    pub(crate) max_source_read_duration: Duration,
    /// Source-future polls that returned `Pending`.
    pub(crate) read_pending_polls: u64,
    /// Source parts that returned `Pending` at least once.
    pub(crate) read_pending_parts: u64,
    /// Elapsed source-read time for parts that returned `Pending`.
    pub(crate) read_pending_duration: Duration,
    /// Total UploadPart request time.
    pub(crate) upload_request_duration: Duration,
    /// Longest UploadPart request.
    pub(crate) max_upload_request_duration: Duration,
    /// Time from multipart creation through source and UploadPart drain.
    pub(crate) body_duration: Duration,
    /// Time from source dispatch closure through multipart body drain.
    pub(crate) drain_duration: Duration,
    /// Total CompleteMultipartUpload request time.
    pub(crate) complete_mpu_request_duration: Duration,
    /// Elapsed completion work including checksum finalization.
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

impl UploadRequestKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PutObject => "put_object",
            Self::CreateMultipartUpload => "create_multipart_upload",
            Self::UploadPart => "upload_part",
            Self::CompleteMultipartUpload => "complete_multipart_upload",
        }
    }
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

/// Closed diagnostic vocabulary for upload state-machine events.
///
/// Each variant maps to one stable `event` and `reason` field pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadEvent {
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
    /// A speculative dispatch was retracted after a custom source blocked.
    CustomSourceBlocked,
    /// CompleteMultipartUpload is ready to run.
    MultipartCompletionStarted,
}

impl UploadEvent {
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
            Self::CustomSourceBlocked => ("work_retracted", "custom_source_blocked"),
            Self::MultipartCompletionStarted => ("request_started", "complete_multipart_upload"),
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
    /// Aggregate for PutObject requests.
    pub(crate) put_object: RequestMetrics,
    /// Aggregate for CreateMultipartUpload requests.
    pub(crate) create_multipart_upload: RequestMetrics,
    /// Aggregate for UploadPart requests.
    pub(crate) upload_part: RequestMetrics,
    /// Aggregate for CompleteMultipartUpload requests.
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
    /// Terminal outcome reported by the common transfer context.
    pub(crate) outcome: UploadTerminalOutcome,
    /// S3 operation strategy selected for the source.
    pub(crate) mode: UploadMode,
    /// Bounds declared by the upload source.
    pub(crate) length_kind: UploadLengthKind,
    /// Transfer-manager error classification for a failed upload.
    pub(crate) error_kind: Option<ErrorKind>,
    /// Time from transfer creation through terminal reporting.
    pub(crate) elapsed: Duration,
    /// Common byte and I/O metrics for the transfer.
    pub(crate) metrics: TransferMetrics,
    /// Request metrics separated by upload request kind.
    pub(crate) requests: UploadRequestMetrics,
    /// Common aggregate across every request issued by the upload.
    pub(crate) request_total: RequestMetrics,
    /// Scheduler pending intervals attributed by common category.
    pub(crate) pending: TransferPendingStats,
    /// Last active upload execution state observed before terminal reporting.
    pub(crate) last_active_state: UploadExecutionState,
    /// Multipart-only work and timing, absent for PutObject.
    pub(crate) multipart: Option<MultipartTransferSummary>,
}

/// Common and direction-specific facts captured at one terminal boundary.
pub(crate) struct UploadTerminalReport {
    /// Transfer receiving the terminal report.
    pub(crate) transfer_id: TransferId,
    /// Common transfer status at the terminal boundary.
    pub(crate) status: TransferStatus,
    /// Transfer-manager error classification for a failed upload.
    pub(crate) error_kind: Option<ErrorKind>,
    /// Common byte and I/O metrics for the transfer.
    pub(crate) metrics: TransferMetrics,
    /// Common aggregate across every request issued by the upload.
    pub(crate) request_total: RequestMetrics,
    /// Scheduler pending intervals attributed by common category.
    pub(crate) pending: TransferPendingStats,
    /// State captured at the terminal boundary.
    pub(crate) state_snapshot: UploadStateSnapshot,
}

/// Optional upload observation kept separate from correctness state.
#[derive(Clone, Debug)]
pub(crate) enum UploadObservability {
    Disabled,
    Enabled(Arc<EnabledUploadObservability>),
}

#[derive(Debug)]
pub(crate) struct EnabledUploadObservability {
    emit_events: bool,
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
    last_active_snapshot: Option<UploadStateSnapshot>,
    parts_completed: usize,
    bytes_read: u64,
    bytes_uploaded: u64,
    requests: UploadRequestMetrics,
}

impl UploadObservability {
    /// Creates upload observation using the client-level diagnostic policy.
    pub(crate) fn new(config: TransferDiagnosticsConfig, size_hint: SizeHint) -> Self {
        if !config.enable_summaries() {
            return Self::Disabled;
        }
        Self::Enabled(Arc::new(EnabledUploadObservability {
            emit_events: config.events_enabled(),
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
                last_active_snapshot: None,
                parts_completed: 0,
                bytes_read: 0,
                bytes_uploaded: 0,
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
    pub(crate) fn start_request(
        &self,
        ctx: &TransferContext,
        kind: UploadRequestKind,
    ) -> UploadRequestMeasurement {
        UploadRequestMeasurement::new(ctx, self.clone(), kind)
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

    /// Records the completed multipart state.
    pub(crate) fn finish_multipart(
        &self,
        snapshot: UploadStateSnapshot,
        complete_duration: Option<Duration>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        update_snapshot(&mut state, snapshot);
        state.complete_mpu_duration = complete_duration.unwrap_or(Duration::ZERO);
    }

    fn multipart_summary(state: &UploadObservabilityState) -> Option<MultipartTransferSummary> {
        if state.mode != UploadMode::Multipart {
            return None;
        }
        let last_active_snapshot = state.last_active_snapshot?;
        if !matches!(
            last_active_snapshot.state,
            UploadExecutionState::Transferring
                | UploadExecutionState::MultipartCompletionPending
                | UploadExecutionState::CompleteMultipartUploadInFlight
        ) {
            return None;
        }
        Some(MultipartTransferSummary {
            last_active_snapshot,
            parts_completed: state.parts_completed,
            bytes_read: state.bytes_read,
            bytes_uploaded: state.bytes_uploaded,
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

    /// Observes one state-machine event and its coherent state snapshot.
    pub(crate) fn observe_event(
        &self,
        transfer_id: TransferId,
        event: UploadEvent,
        snapshot: UploadStateSnapshot,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        if enabled.emit_events {
            emit_event(transfer_id, event, snapshot);
        }
    }

    /// Retains coherent state without emitting a direction-specific event.
    pub(crate) fn observe_state(&self, snapshot: UploadStateSnapshot) {
        if let Self::Enabled(enabled) = self {
            update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        }
    }

    /// Observes one retained source operation.
    pub(crate) fn observe_source_pending(
        &self,
        transfer_id: TransferId,
        observation: SourceReadObservation,
        snapshot: UploadStateSnapshot,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        if enabled.emit_events {
            emit_source_pending(transfer_id, observation, snapshot);
        }
    }

    /// Observes one UploadPart request start.
    pub(crate) fn observe_part_started(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        observation: SourceReadObservation,
        snapshot: UploadStateSnapshot,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        if enabled.emit_events {
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

    /// Observes one failed UploadPart request.
    pub(crate) fn observe_part_failed(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        request_duration: Duration,
        snapshot: UploadStateSnapshot,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        if enabled.emit_events {
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

    /// Observes one completed UploadPart request.
    pub(crate) fn observe_part_completed(
        &self,
        transfer_id: TransferId,
        part_number: u64,
        bytes_sent: u64,
        presentation_segments: usize,
        request_duration: Duration,
        snapshot: UploadStateSnapshot,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        update_snapshot(&mut enabled.state.lock().expect("lock poisoned"), snapshot);
        if enabled.emit_events {
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

    /// Finalizes and emits the upload terminal projection.
    pub(crate) fn report_terminal(
        &self,
        report: UploadTerminalReport,
    ) -> Option<UploadTransferSummary> {
        let outcome = UploadTerminalOutcome::from_status(report.status)?;
        let Self::Enabled(enabled) = self else {
            emit_terminal_lifecycle(&report, outcome);
            return None;
        };

        let mut state = enabled.state.lock().expect("lock poisoned");
        update_snapshot(&mut state, report.state_snapshot);
        let last_active_snapshot = state
            .last_active_snapshot
            .expect("terminal upload summary must retain a state snapshot");
        let summary = UploadTransferSummary {
            outcome,
            mode: state.mode,
            length_kind: state.length_kind,
            error_kind: report.error_kind,
            elapsed: report
                .metrics
                .finished_at
                .map(|finished_at| finished_at.saturating_duration_since(report.metrics.started_at))
                .unwrap_or_default(),
            metrics: report.metrics,
            requests: state.requests,
            request_total: report.request_total,
            pending: report.pending,
            last_active_state: last_active_snapshot.state,
            multipart: Self::multipart_summary(&state),
        };
        drop(state);

        if enabled.emit_events {
            emit_terminal_details(report.transfer_id, &summary);
        }
        #[cfg(test)]
        {
            *enabled.last_summary.lock().expect("lock poisoned") = Some(summary.clone());
        }
        emit_terminal_summary(report.transfer_id, &summary);
        Some(summary)
    }

    /// Returns a point-in-time multipart summary for state-machine tests.
    #[cfg(test)]
    pub(crate) fn test_summary(
        &self,
        snapshot: UploadStateSnapshot,
    ) -> Option<MultipartTransferSummary> {
        self.finish_body();
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        update_snapshot(&mut state, snapshot);
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

impl RequestMetricsAttribution for UploadObservability {
    type Kind = UploadRequestKind;

    fn record_request_metrics(&self, kind: Self::Kind, metrics: &RequestMetrics) {
        self.record_request(kind, metrics);
    }
}

/// In-progress request measurement attributed to one upload request kind.
pub(crate) type UploadRequestMeasurement = AttributedRequestMeasurement<UploadObservability>;

fn update_snapshot(state: &mut UploadObservabilityState, snapshot: UploadStateSnapshot) {
    if snapshot.state != UploadExecutionState::Done || state.last_active_snapshot.is_none() {
        state.last_active_snapshot = Some(snapshot);
    }
    state.max_parts_in_flight = state.max_parts_in_flight.max(snapshot.parts_in_flight);
    state.parts_completed = state.parts_completed.max(snapshot.completed_parts);
    state.bytes_read = state.bytes_read.max(snapshot.bytes_read);
    state.bytes_uploaded = state.bytes_uploaded.max(snapshot.bytes_uploaded);
}

fn emit_event(transfer_id: TransferId, event: UploadEvent, snapshot: UploadStateSnapshot) {
    let (event_name, reason) = event.fields();
    match event {
        UploadEvent::PutObjectScheduled
        | UploadEvent::MultipartUploadScheduled
        | UploadEvent::MultipartUploadCreated => {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %transfer_id,
                event = event_name,
                reason,
                state = snapshot.state.as_str(),
                "upload state-machine event",
            );
        }
        UploadEvent::NewPartScheduled
        | UploadEvent::SourceWakeScheduled
        | UploadEvent::EmptyObjectPartScheduled
        | UploadEvent::CustomSourceBlocked => {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %transfer_id,
                event = event_name,
                reason,
                state = snapshot.state.as_str(),
                parts_dispatched = snapshot.parts_dispatched,
                parts_in_flight = snapshot.parts_in_flight,
                pending_reads = snapshot.pending_reads,
                "upload state-machine event",
            );
        }
        UploadEvent::SourceExhausted => {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %transfer_id,
                event = event_name,
                reason,
                state = snapshot.state.as_str(),
                parts_in_flight = snapshot.parts_in_flight,
                pending_reads = snapshot.pending_reads,
                completed_parts = snapshot.completed_parts,
                bytes_read = snapshot.bytes_read,
                dispatch_closed = snapshot.dispatch_closed,
                "upload state-machine event",
            );
        }
        UploadEvent::CompletionReady | UploadEvent::MultipartCompletionStarted => {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %transfer_id,
                event = event_name,
                reason,
                state = snapshot.state.as_str(),
                parts_in_flight = snapshot.parts_in_flight,
                uploads_in_flight = snapshot.uploads_in_flight,
                completed_parts = snapshot.completed_parts,
                bytes_read = snapshot.bytes_read,
                bytes_uploaded = snapshot.bytes_uploaded,
                "upload state-machine event",
            );
        }
    }
}

fn emit_source_pending(
    transfer_id: TransferId,
    observation: SourceReadObservation,
    snapshot: UploadStateSnapshot,
) {
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
    snapshot: UploadStateSnapshot,
) {
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
    _snapshot: UploadStateSnapshot,
) {
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
    snapshot: UploadStateSnapshot,
) {
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

fn emit_terminal_lifecycle(report: &UploadTerminalReport, outcome: UploadTerminalOutcome) {
    let elapsed = report
        .metrics
        .finished_at
        .map(|finished_at| finished_at.saturating_duration_since(report.metrics.started_at))
        .unwrap_or_default();
    tracing::debug!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %report.transfer_id,
        outcome = outcome.as_str(),
        error_kind = ?report.error_kind,
        elapsed = ?elapsed,
        expected_bytes = report.metrics.total_bytes,
        network_tx = report.metrics.network_tx,
        disk_read = report.metrics.disk_read,
        "upload transfer terminal",
    );
}

fn emit_terminal_summary(transfer_id: TransferId, summary: &UploadTransferSummary) {
    if let Some(multipart) = summary.multipart {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            outcome = summary.outcome.as_str(),
            mode = summary.mode.as_str(),
            source_length = summary.length_kind.as_str(),
            error_kind = ?summary.error_kind,
            elapsed = ?summary.elapsed,
            expected_bytes = summary.metrics.total_bytes,
            network_tx = summary.metrics.network_tx,
            disk_read = summary.metrics.disk_read,
            requests = summary.request_total.requests,
            request_time_sum = ?summary.request_total.elapsed,
            request_time_max = ?summary.request_total.max_elapsed,
            retry_reissues = summary.request_total.retry_reissues,
            throttle_reissues = summary.request_total.throttle_reissues,
            hedge_reissues = summary.request_total.hedge_reissues,
            retry_exhaustions = summary.request_total.retry_exhaustions,
            backoff_time_sum = ?summary.request_total.backoff_duration,
            pending_intervals = summary.pending.interval_count(),
            pending_time_sum = ?summary.pending.pending_duration(),
            pending_time_max = ?summary.pending.max_pending_duration(),
            parts_completed = multipart.parts_completed,
            bytes_read = multipart.bytes_read,
            bytes_uploaded = multipart.bytes_uploaded,
            max_parts_in_flight = multipart.max_parts_in_flight,
            max_uploads_in_flight = multipart.max_uploads_in_flight,
            "upload transfer terminal",
        );
    } else {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            outcome = summary.outcome.as_str(),
            mode = summary.mode.as_str(),
            source_length = summary.length_kind.as_str(),
            error_kind = ?summary.error_kind,
            elapsed = ?summary.elapsed,
            expected_bytes = summary.metrics.total_bytes,
            network_tx = summary.metrics.network_tx,
            disk_read = summary.metrics.disk_read,
            requests = summary.request_total.requests,
            request_time_sum = ?summary.request_total.elapsed,
            request_time_max = ?summary.request_total.max_elapsed,
            retry_reissues = summary.request_total.retry_reissues,
            throttle_reissues = summary.request_total.throttle_reissues,
            hedge_reissues = summary.request_total.hedge_reissues,
            retry_exhaustions = summary.request_total.retry_exhaustions,
            backoff_time_sum = ?summary.request_total.backoff_duration,
            pending_intervals = summary.pending.interval_count(),
            pending_time_sum = ?summary.pending.pending_duration(),
            pending_time_max = ?summary.pending.max_pending_duration(),
            "upload transfer terminal",
        );
    }
}

fn emit_terminal_details(transfer_id: TransferId, summary: &UploadTransferSummary) {
    emit_request_detail(
        transfer_id,
        UploadRequestKind::PutObject,
        summary.requests.put_object,
    );
    emit_request_detail(
        transfer_id,
        UploadRequestKind::CreateMultipartUpload,
        summary.requests.create_multipart_upload,
    );
    emit_request_detail(
        transfer_id,
        UploadRequestKind::UploadPart,
        summary.requests.upload_part,
    );
    emit_request_detail(
        transfer_id,
        UploadRequestKind::CompleteMultipartUpload,
        summary.requests.complete_multipart_upload,
    );
    emit_pending_details(transfer_id, &summary.pending);

    if let Some(multipart) = summary.multipart {
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            last_active_state = summary.last_active_state.as_str(),
            parts_completed = multipart.parts_completed,
            bytes_read = multipart.bytes_read,
            bytes_uploaded = multipart.bytes_uploaded,
            segmented_parts = multipart.segmented_parts,
            presentation_segments = multipart.presentation_segments,
            max_presentation_segments = multipart.max_presentation_segments,
            max_parts_in_flight = multipart.max_parts_in_flight,
            max_uploads_in_flight = multipart.max_uploads_in_flight,
            create_mpu_time_sum = ?multipart.create_mpu_duration,
            source_read_time_sum = ?multipart.source_read_duration,
            source_read_time_max = ?multipart.max_source_read_duration,
            read_pending_polls = multipart.read_pending_polls,
            read_pending_parts = multipart.read_pending_parts,
            read_pending_time_sum = ?multipart.read_pending_duration,
            upload_part_time_sum = ?multipart.upload_request_duration,
            upload_part_time_max = ?multipart.max_upload_request_duration,
            body_time = ?multipart.body_duration,
            drain_time = ?multipart.drain_duration,
            complete_mpu_request_time_sum = ?multipart.complete_mpu_request_duration,
            complete_mpu_time = ?multipart.complete_mpu_duration,
            "upload multipart detail",
        );
    }
}

fn emit_request_detail(transfer_id: TransferId, kind: UploadRequestKind, metrics: RequestMetrics) {
    if metrics.requests == 0 {
        return;
    }
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        request_kind = kind.as_str(),
        requests = metrics.requests,
        request_time_sum = ?metrics.elapsed,
        request_time_max = ?metrics.max_elapsed,
        retry_reissues = metrics.retry_reissues,
        throttle_reissues = metrics.throttle_reissues,
        hedge_reissues = metrics.hedge_reissues,
        retry_exhaustions = metrics.retry_exhaustions,
        backoff_time_sum = ?metrics.backoff_duration,
        "upload request detail",
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

    fn terminal_report(
        transfer_id: u64,
        status: TransferStatus,
        error_kind: Option<ErrorKind>,
        request_total: RequestMetrics,
        state_snapshot: UploadStateSnapshot,
    ) -> UploadTerminalReport {
        UploadTerminalReport {
            transfer_id: TransferId {
                id: transfer_id,
                parent: None,
            },
            status,
            error_kind,
            metrics: test_transfer_metrics(),
            request_total,
            pending: TransferPendingStats::default(),
            state_snapshot,
        }
    }

    fn snapshot() -> UploadStateSnapshot {
        UploadStateSnapshot {
            state: UploadExecutionState::Transferring,
            parts_dispatched: 1,
            parts_in_flight: 0,
            uploads_in_flight: 0,
            pending_reads: 0,
            completed_parts: 1,
            bytes_read: 8 * 1024 * 1024,
            bytes_uploaded: 8 * 1024 * 1024,
            eof: false,
            dispatch_closed: true,
        }
    }

    #[test]
    fn disabled_collection_does_not_start_timers_or_retain_events() {
        let observability = UploadObservability::new(config(0), exact_size_hint());
        let timing = observability.part_scheduled();
        assert!(matches!(observability, UploadObservability::Disabled));
        assert_eq!(UploadDiagnosticTimer::start(config(0)).elapsed(), None);
        assert_eq!(timing.finish().elapsed, None);
        observability.observe_event(
            TransferId {
                id: 1,
                parent: None,
            },
            UploadEvent::NewPartScheduled,
            snapshot(),
        );
    }

    #[test]
    fn summary_and_event_levels_are_distinct() {
        let summary = UploadObservability::new(config(1), exact_size_hint());
        let events = UploadObservability::new(config(2), exact_size_hint());
        assert!(matches!(
            summary,
            UploadObservability::Enabled(ref enabled) if !enabled.emit_events
        ));
        assert!(matches!(
            events,
            UploadObservability::Enabled(ref enabled) if enabled.emit_events
        ));
    }

    #[test]
    fn summary_level_aggregates_without_event_emission() {
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
        assert_eq!(summary.last_active_snapshot.uploads_in_flight, 0);
        assert_eq!(summary.parts_completed, 1);
        assert_eq!(summary.bytes_read, 8 * 1024 * 1024);
        assert_eq!(summary.bytes_uploaded, 8 * 1024 * 1024);
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
    fn upload_event_vocabulary_is_closed_and_stable() {
        assert_eq!(
            UploadEvent::PutObjectScheduled.fields(),
            ("work_scheduled", "put_object")
        );
        assert_eq!(
            UploadEvent::MultipartUploadScheduled.fields(),
            ("work_scheduled", "create_multipart_upload")
        );
        assert_eq!(
            UploadEvent::MultipartUploadCreated.fields(),
            ("state_changed", "multipart_upload_created")
        );
        assert_eq!(
            UploadEvent::NewPartScheduled.fields(),
            ("work_scheduled", "new_part")
        );
        assert_eq!(
            UploadEvent::SourceWakeScheduled.fields(),
            ("work_scheduled", "source_wake")
        );
        assert_eq!(
            UploadEvent::SourceExhausted.fields(),
            ("dispatch_closed", "source_eof")
        );
        assert_eq!(
            UploadEvent::EmptyObjectPartScheduled.fields(),
            ("work_scheduled", "empty_object")
        );
        assert_eq!(
            UploadEvent::CompletionReady.fields(),
            ("completion_ready", "multipart_drained")
        );
        assert_eq!(
            UploadEvent::CustomSourceBlocked.fields(),
            ("work_retracted", "custom_source_blocked")
        );
        assert_eq!(
            UploadEvent::MultipartCompletionStarted.fields(),
            ("request_started", "complete_multipart_upload")
        );
    }

    #[test]
    fn terminal_summary_records_mode_and_last_active_state() {
        let observability = UploadObservability::new(config(1), exact_size_hint());
        observability.select_mode(UploadMode::PutObject);
        let first = observability.report_terminal(terminal_report(
            1,
            TransferStatus::Completed,
            None,
            RequestMetrics::default(),
            UploadStateSnapshot::inactive(UploadExecutionState::PutObjectInFlight),
        ));
        let first = first.expect("first report");
        assert_eq!(first.mode, UploadMode::PutObject);
        assert_eq!(
            first.last_active_state,
            UploadExecutionState::PutObjectInFlight
        );
    }

    #[test]
    fn request_kind_metrics_remain_separate() {
        let observability = UploadObservability::new(config(1), exact_size_hint());
        let mut upload_part = RequestMetrics::default();
        upload_part.record_request(Duration::from_micros(7));
        upload_part.record_retry_reissue(Duration::from_micros(3));
        observability.record_request(UploadRequestKind::UploadPart, &upload_part);

        let summary = observability
            .report_terminal(terminal_report(
                2,
                TransferStatus::Failed,
                Some(ErrorKind::RuntimeError),
                upload_part,
                UploadStateSnapshot::inactive(UploadExecutionState::PutObjectInFlight),
            ))
            .expect("terminal report");
        assert_eq!(summary.requests.upload_part, upload_part);
        assert_eq!(summary.requests.put_object, RequestMetrics::default());
    }
}
