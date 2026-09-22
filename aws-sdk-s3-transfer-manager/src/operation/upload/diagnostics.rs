/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Optional diagnostics for one upload transfer.
//!
//! [`UploadTransferDiagnostics`] owns collection policy and aggregate state.
//! Upload scheduling invokes it unconditionally; the disabled variant avoids
//! clocks, counters, and transition snapshots.

use std::time::{Duration, Instant};

use crate::config::TransferDiagnosticsConfig;
use crate::operation::upload::context::PartTransferPendingReason;

/// One multipart-pipeline state sampled while the upload-state lock is held.
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

/// Aggregate diagnostics emitted after one multipart upload completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartTransferSummary {
    pub(crate) snapshot: PartTransferSnapshot,
    pub(crate) segmented_parts: u64,
    pub(crate) presentation_segments: u64,
    pub(crate) max_presentation_segments: usize,
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

/// Closed diagnostic vocabulary for multipart scheduling transitions.
///
/// Each variant maps to one stable `transition` and `reason` field pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartTransferTransition {
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
    /// No upload-part work can be dispatched until pipeline state changes.
    Pending(PartTransferPendingReason),
    /// A speculative dispatch was retracted after a custom source blocked.
    CustomSourceBlocked,
}

impl PartTransferTransition {
    fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::NewPartScheduled => ("work_scheduled", "new_part"),
            Self::SourceWakeScheduled => ("work_scheduled", "source_wake"),
            Self::SourceExhausted => ("dispatch_closed", "source_eof"),
            Self::EmptyObjectPartScheduled => ("work_scheduled", "empty_object"),
            Self::CompletionReady => ("completion_ready", "pipeline_drained"),
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
        }
    }
}

/// Optional clock sample whose construction is controlled by diagnostic policy.
#[derive(Debug)]
pub(crate) struct UploadDiagnosticTimer(Option<Instant>);

impl UploadDiagnosticTimer {
    /// Starts a timer only when collection is enabled.
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

/// Source-read measurements transferred into aggregate upload diagnostics.
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

/// Optional upload diagnostics kept separate from multipart correctness state.
#[derive(Clone, Debug)]
pub(crate) enum UploadTransferDiagnostics {
    Disabled,
    Enabled(Box<UploadDiagnosticsState>),
}

#[derive(Clone, Debug)]
pub(crate) struct UploadDiagnosticsState {
    transitions: bool,
    started_at: Instant,
    dispatch_closed_at: Option<Instant>,
    body_duration: Option<Duration>,
    drain_duration: Duration,
    uploads_in_flight: usize,
    segmented_parts: u64,
    presentation_segments: u64,
    max_presentation_segments: usize,
    create_mpu_duration: Duration,
    source_read_duration: Duration,
    max_source_read_duration: Duration,
    read_pending_polls: u64,
    read_pending_parts: u64,
    read_pending_duration: Duration,
    upload_request_duration: Duration,
    max_upload_request_duration: Duration,
}

impl UploadTransferDiagnostics {
    /// Starts multipart-body collection using the client-level transfer policy.
    pub(crate) fn new(
        config: TransferDiagnosticsConfig,
        create_mpu_duration: Option<Duration>,
    ) -> Self {
        if !config.enable_summaries() {
            return Self::Disabled;
        }
        Self::Enabled(Box::new(UploadDiagnosticsState {
            transitions: config.enable_transitions(),
            started_at: Instant::now(),
            dispatch_closed_at: None,
            body_duration: None,
            drain_duration: Duration::ZERO,
            uploads_in_flight: 0,
            segmented_parts: 0,
            presentation_segments: 0,
            max_presentation_segments: 0,
            create_mpu_duration: create_mpu_duration.unwrap_or(Duration::ZERO),
            source_read_duration: Duration::ZERO,
            max_source_read_duration: Duration::ZERO,
            read_pending_polls: 0,
            read_pending_parts: 0,
            read_pending_duration: Duration::ZERO,
            upload_request_duration: Duration::ZERO,
            max_upload_request_duration: Duration::ZERO,
        }))
    }

    /// Creates timing state for one newly scheduled source operation.
    pub(crate) fn part_scheduled(&self) -> UploadPartTiming {
        match self {
            Self::Disabled => UploadPartTiming::disabled(),
            Self::Enabled(_) => UploadPartTiming::started(),
        }
    }

    /// Records one source poll that returned `Pending`.
    pub(crate) fn record_read_pending(&mut self, timing: &mut UploadPartTiming) {
        timing.record_pending();
        if let Self::Enabled(state) = self {
            state.read_pending_polls = state.read_pending_polls.saturating_add(1);
        }
    }

    /// Records the point at which dispatch closes or reopens.
    pub(crate) fn set_dispatch_closed(&mut self, closed: bool) {
        let Self::Enabled(state) = self else {
            return;
        };
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
        &mut self,
        presentation_segments: usize,
        timing: UploadPartTiming,
    ) -> SourceReadObservation {
        let observation = timing.finish();
        let Self::Enabled(state) = self else {
            return observation;
        };

        state.uploads_in_flight = state
            .uploads_in_flight
            .checked_add(1)
            .expect("diagnostic upload count overflow");
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

    /// Records one completed UploadPart request.
    pub(crate) fn complete_upload(&mut self, request_duration: Option<Duration>) {
        let Self::Enabled(state) = self else {
            return;
        };
        state.uploads_in_flight = state
            .uploads_in_flight
            .checked_sub(1)
            .expect("diagnostic upload completion underflow");
        if let Some(duration) = request_duration {
            state.upload_request_duration = state.upload_request_duration.saturating_add(duration);
            state.max_upload_request_duration = state.max_upload_request_duration.max(duration);
        }
    }

    /// Captures the multipart body and drain durations before completion begins.
    pub(crate) fn finish_body(&mut self) {
        let Self::Enabled(state) = self else {
            return;
        };
        let now = Instant::now();
        state.body_duration = Some(now.saturating_duration_since(state.started_at));
        state.drain_duration = state
            .dispatch_closed_at
            .map(|closed_at| now.saturating_duration_since(closed_at))
            .unwrap_or(Duration::ZERO);
    }

    /// Returns the diagnostic upload-request count.
    pub(crate) fn uploads_in_flight(&self) -> usize {
        match self {
            Self::Disabled => 0,
            Self::Enabled(state) => state.uploads_in_flight,
        }
    }

    /// Returns a transition snapshot only at the detailed diagnostic level.
    pub(crate) fn transition_snapshot(
        &self,
        snapshot: PartTransferSnapshot,
    ) -> Option<PartTransferSnapshot> {
        match self {
            Self::Enabled(state) if state.transitions => Some(snapshot),
            _ => None,
        }
    }

    /// Finalizes the aggregate report after CompleteMultipartUpload succeeds.
    pub(crate) fn finish(
        self,
        snapshot: PartTransferSnapshot,
        complete_request_duration: Option<Duration>,
        complete_duration: Option<Duration>,
    ) -> Option<PartTransferSummary> {
        let Self::Enabled(state) = self else {
            return None;
        };
        Some(PartTransferSummary {
            snapshot,
            segmented_parts: state.segmented_parts,
            presentation_segments: state.presentation_segments,
            max_presentation_segments: state.max_presentation_segments,
            create_mpu_duration: state.create_mpu_duration,
            source_read_duration: state.source_read_duration,
            max_source_read_duration: state.max_source_read_duration,
            read_pending_polls: state.read_pending_polls,
            read_pending_parts: state.read_pending_parts,
            read_pending_duration: state.read_pending_duration,
            upload_request_duration: state.upload_request_duration,
            max_upload_request_duration: state.max_upload_request_duration,
            body_duration: state.body_duration.unwrap_or(Duration::ZERO),
            drain_duration: state.drain_duration,
            complete_mpu_request_duration: complete_request_duration.unwrap_or(Duration::ZERO),
            complete_mpu_duration: complete_duration.unwrap_or(Duration::ZERO),
        })
    }

    /// Returns a point-in-time summary for internal state-machine tests.
    #[cfg(test)]
    pub(crate) fn test_summary(
        &self,
        snapshot: PartTransferSnapshot,
    ) -> Option<PartTransferSummary> {
        let mut diagnostics = self.clone();
        diagnostics.finish_body();
        diagnostics.finish(snapshot, None, None)
    }
}

/// Emits one optional multipart state transition.
pub(crate) fn emit_pipeline_transition(
    transfer_id: crate::transfer::TransferId,
    transition: PartTransferTransition,
    snapshot: Option<PartTransferSnapshot>,
) {
    let Some(snapshot) = snapshot else {
        return;
    };
    let (transition, reason) = transition.fields();
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
        "upload multipart pipeline",
    );
}

/// Emits one source operation that retained its future after `Pending`.
pub(crate) fn emit_source_pending(
    transfer_id: crate::transfer::TransferId,
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

/// Emits the start of one UploadPart request.
pub(crate) fn emit_part_started(
    transfer_id: crate::transfer::TransferId,
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

/// Emits one failed UploadPart request.
pub(crate) fn emit_part_failed(
    transfer_id: crate::transfer::TransferId,
    part_number: u64,
    bytes_sent: u64,
    presentation_segments: usize,
    request_duration: Option<Duration>,
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
        request_elapsed_us = duration_micros(request_duration.unwrap_or(Duration::ZERO)),
        "upload part request failed",
    );
}

/// Emits one successfully completed UploadPart request.
pub(crate) fn emit_part_completed(
    transfer_id: crate::transfer::TransferId,
    part_number: u64,
    bytes_sent: u64,
    presentation_segments: usize,
    request_duration: Option<Duration>,
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
        request_elapsed_us = duration_micros(request_duration.unwrap_or(Duration::ZERO)),
        parts_in_flight = snapshot.parts_in_flight,
        uploads_in_flight = snapshot.uploads_in_flight,
        pending_reads = snapshot.pending_reads,
        completed_parts = snapshot.completed_parts,
        dispatch_closed = snapshot.dispatch_closed,
        "upload part request completed",
    );
}

/// Emits the completed multipart summary when summary collection was enabled.
pub(crate) fn emit_summary(
    transfer_id: crate::transfer::TransferId,
    summary: Option<PartTransferSummary>,
) {
    let Some(summary) = summary else {
        return;
    };
    tracing::debug!(
        target: crate::telemetry::TARGET_TRANSFER,
        tid = %transfer_id,
        parts = summary.snapshot.completed_parts,
        bytes_uploaded = summary.snapshot.bytes_uploaded,
        segmented_parts = summary.segmented_parts,
        presentation_segments = summary.presentation_segments,
        max_presentation_segments = summary.max_presentation_segments,
        create_mpu_duration_us = duration_micros(summary.create_mpu_duration),
        source_read_duration_us = duration_micros(summary.source_read_duration),
        max_source_read_duration_us = duration_micros(summary.max_source_read_duration),
        read_pending_polls = summary.read_pending_polls,
        read_pending_parts = summary.read_pending_parts,
        read_pending_duration_us = duration_micros(summary.read_pending_duration),
        upload_request_duration_us = duration_micros(summary.upload_request_duration),
        max_upload_request_duration_us = duration_micros(summary.max_upload_request_duration),
        body_duration_us = duration_micros(summary.body_duration),
        drain_duration_us = duration_micros(summary.drain_duration),
        complete_mpu_request_duration_us =
            duration_micros(summary.complete_mpu_request_duration),
        complete_mpu_duration_us = duration_micros(summary.complete_mpu_duration),
        "multipart upload pipeline completed",
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

    #[test]
    fn disabled_collection_does_not_start_timers_or_emit_snapshots() {
        let diagnostics = UploadTransferDiagnostics::new(config(0), None);
        let timing = diagnostics.part_scheduled();
        assert!(matches!(diagnostics, UploadTransferDiagnostics::Disabled));
        assert_eq!(UploadDiagnosticTimer::start(config(0)).elapsed(), None);
        assert_eq!(timing.finish().elapsed, None);
        assert_eq!(
            diagnostics.transition_snapshot(PartTransferSnapshot {
                parts_dispatched: 1,
                parts_in_flight: 1,
                uploads_in_flight: 0,
                pending_reads: 0,
                completed_parts: 0,
                bytes_uploaded: 0,
                eof: false,
                dispatch_closed: false,
            }),
            None
        );
    }

    #[test]
    fn summary_and_transition_levels_are_distinct() {
        let snapshot = PartTransferSnapshot {
            parts_dispatched: 1,
            parts_in_flight: 1,
            uploads_in_flight: 0,
            pending_reads: 0,
            completed_parts: 0,
            bytes_uploaded: 0,
            eof: false,
            dispatch_closed: false,
        };
        let summary = UploadTransferDiagnostics::new(config(1), None);
        assert_eq!(summary.transition_snapshot(snapshot), None);

        let transitions = UploadTransferDiagnostics::new(config(2), None);
        assert_eq!(transitions.transition_snapshot(snapshot), Some(snapshot));
    }

    #[test]
    fn summary_level_aggregates_without_transition_snapshots() {
        let mut diagnostics =
            UploadTransferDiagnostics::new(config(1), Some(Duration::from_micros(11)));
        let mut timing = diagnostics.part_scheduled();
        diagnostics.record_read_pending(&mut timing);
        let observation = diagnostics.begin_upload(3, timing);
        assert_eq!(observation.pending_polls(), 1);
        diagnostics.complete_upload(Some(Duration::from_micros(17)));
        diagnostics.set_dispatch_closed(true);
        diagnostics.finish_body();

        let snapshot = PartTransferSnapshot {
            parts_dispatched: 1,
            parts_in_flight: 0,
            uploads_in_flight: diagnostics.uploads_in_flight(),
            pending_reads: 0,
            completed_parts: 1,
            bytes_uploaded: 8 * 1024 * 1024,
            eof: false,
            dispatch_closed: true,
        };
        assert_eq!(diagnostics.transition_snapshot(snapshot), None);

        let summary = diagnostics
            .finish(
                snapshot,
                Some(Duration::from_micros(19)),
                Some(Duration::from_micros(23)),
            )
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
    fn pipeline_transition_vocabulary_is_closed_and_stable() {
        assert_eq!(
            PartTransferTransition::NewPartScheduled.fields(),
            ("work_scheduled", "new_part")
        );
        assert_eq!(
            PartTransferTransition::SourceWakeScheduled.fields(),
            ("work_scheduled", "source_wake")
        );
        assert_eq!(
            PartTransferTransition::SourceExhausted.fields(),
            ("dispatch_closed", "source_eof")
        );
        assert_eq!(
            PartTransferTransition::EmptyObjectPartScheduled.fields(),
            ("work_scheduled", "empty_object")
        );
        assert_eq!(
            PartTransferTransition::CompletionReady.fields(),
            ("completion_ready", "pipeline_drained")
        );
        assert_eq!(
            PartTransferTransition::Pending(PartTransferPendingReason::SourceUnavailable).fields(),
            ("poll_pending", "source_unavailable")
        );
        assert_eq!(
            PartTransferTransition::Pending(PartTransferPendingReason::DispatchClosed).fields(),
            ("poll_pending", "dispatch_closed")
        );
        assert_eq!(
            PartTransferTransition::Pending(PartTransferPendingReason::NoReadyPart).fields(),
            ("poll_pending", "no_ready_part")
        );
        assert_eq!(
            PartTransferTransition::CustomSourceBlocked.fields(),
            ("work_retracted", "custom_source_blocked")
        );
    }
}
