/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download state-machine observation and diagnostic emission.
//!
//! [`DownloadObservability`] receives state-machine transitions and request
//! measurements from discovery and range transfer. The disabled variant avoids
//! optional allocation, clocks, counters, and transition snapshots.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::config::TransferDiagnosticsConfig;
use crate::error::ErrorKind;
use crate::metrics::RequestMetrics;
use crate::operation::download::context::DownloadPendingReason;
use crate::transfer::{
    PendingCategory, RequestMeasurement, TransferContext, TransferId, TransferPendingStats,
};
use crate::types::{ChecksumValidation, TransferMetrics, TransferStatus};

/// Delivery surface owned by one download.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadDestination {
    /// Completed chunks remain available through the download body.
    Stream,
    /// Completed chunks are written to a positioned file sink.
    File,
}

impl DownloadDestination {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::File => "file",
        }
    }
}

/// Logical S3 request issued by one download transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadRequestKind {
    /// HeadObject request used for object discovery.
    DiscoveryHead,
    /// Ranged GetObject request used for object discovery.
    DiscoveryRange,
    /// Part-number GetObject request used for object discovery.
    DiscoveryPart,
    /// GetObject request for one post-discovery byte range.
    Range,
}

/// Coarse download state retained in a diagnostic snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadStateKind {
    /// Discovery has not been scheduled.
    PendingDiscovery,
    /// Discovery work is executing.
    DiscoveryInFlight,
    /// Object ranges are being issued or retired.
    Transferring,
    /// The state machine has claimed its terminal transition.
    Terminal,
}

impl DownloadStateKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PendingDiscovery => "pending_discovery",
            Self::DiscoveryInFlight => "discovery_in_flight",
            Self::Transferring => "transferring",
            Self::Terminal => "terminal",
        }
    }
}

/// Download state sampled while the download-state lock is held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownloadStateSnapshot {
    pub(crate) state: DownloadStateKind,
    pub(crate) remaining_bytes: Option<u64>,
    pub(crate) ranges_in_flight: usize,
    pub(crate) ranges_issued: u64,
    pub(crate) ranges_released: u64,
    pub(crate) resident_parts: u64,
    pub(crate) read_ahead_window: u64,
    pub(crate) memory_claim_pending: bool,
}

impl DownloadStateSnapshot {
    /// Returns a snapshot for a state without range-transfer counters.
    pub(crate) const fn inactive(state: DownloadStateKind, read_ahead_window: u64) -> Self {
        Self {
            state,
            remaining_bytes: None,
            ranges_in_flight: 0,
            ranges_issued: 0,
            ranges_released: 0,
            resident_parts: 0,
            read_ahead_window,
            memory_claim_pending: false,
        }
    }
}

/// Terminal status emitted once for a download transfer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadTerminalOutcome {
    /// The download completed successfully.
    Completed,
    /// The download failed.
    Failed,
    /// The download was cancelled.
    Cancelled,
}

impl DownloadTerminalOutcome {
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

/// Closed diagnostic vocabulary for download state-machine transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadTransition {
    /// Discovery work was scheduled.
    DiscoveryScheduled,
    /// Discovery established the object and transfer geometry.
    DiscoveryCompleted,
    /// One object range was scheduled.
    RangeScheduled,
    /// One object range retired.
    RangeCompleted,
    /// Every object range has been issued.
    AllRangesIssued,
    /// No download work can be produced until local state changes.
    Pending(DownloadPendingReason),
    /// A disk drain was scheduled to release resident memory.
    MemoryReliefScheduled,
    /// A disk drain released resident memory.
    MemoryReliefCompleted,
    /// The destination was finalized for successful completion.
    DestinationFinalized,
    /// The download reached one terminal outcome.
    Terminal(DownloadTerminalOutcome),
}

impl DownloadTransition {
    fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::DiscoveryScheduled => ("work_scheduled", "discovery"),
            Self::DiscoveryCompleted => ("state_changed", "discovery_completed"),
            Self::RangeScheduled => ("work_scheduled", "range"),
            Self::RangeCompleted => ("work_completed", "range"),
            Self::AllRangesIssued => ("dispatch_closed", "all_ranges_issued"),
            Self::Pending(DownloadPendingReason::Discovery) => ("poll_pending", "discovery"),
            Self::Pending(DownloadPendingReason::ReadAhead) => ("poll_pending", "read_ahead"),
            Self::Pending(DownloadPendingReason::MemoryAdmission) => {
                ("poll_pending", "memory_admission")
            }
            Self::Pending(DownloadPendingReason::RangeCompletion) => {
                ("poll_pending", "range_completion")
            }
            Self::MemoryReliefScheduled => ("work_scheduled", "memory_relief"),
            Self::MemoryReliefCompleted => ("work_completed", "memory_relief"),
            Self::DestinationFinalized => ("state_changed", "destination_finalized"),
            Self::Terminal(DownloadTerminalOutcome::Completed) => ("terminal", "completed"),
            Self::Terminal(DownloadTerminalOutcome::Failed) => ("terminal", "failed"),
            Self::Terminal(DownloadTerminalOutcome::Cancelled) => ("terminal", "cancelled"),
        }
    }
}

/// Per-request-kind metrics retained for a download terminal summary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DownloadRequestMetrics {
    pub(crate) discovery_head: RequestMetrics,
    pub(crate) discovery_range: RequestMetrics,
    pub(crate) discovery_part: RequestMetrics,
    pub(crate) range: RequestMetrics,
}

impl DownloadRequestMetrics {
    fn record(&mut self, kind: DownloadRequestKind, metrics: &RequestMetrics) {
        let target = match kind {
            DownloadRequestKind::DiscoveryHead => &mut self.discovery_head,
            DownloadRequestKind::DiscoveryRange => &mut self.discovery_range,
            DownloadRequestKind::DiscoveryPart => &mut self.discovery_part,
            DownloadRequestKind::Range => &mut self.range,
        };
        *target += metrics;
    }
}

/// Destination work included in a download terminal summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DownloadDestinationSummary {
    pub(crate) prepared: bool,
    pub(crate) file_finalized: bool,
    pub(crate) finalization_failed: bool,
    pub(crate) batched_drain_parts: u64,
    pub(crate) memory_relief_drains: u64,
    pub(crate) memory_relief_parts: u64,
    pub(crate) terminal_drain_parts: u64,
    pub(crate) terminal_drain_failed: bool,
}

/// Complete diagnostic report for one download transfer.
#[derive(Clone, Debug)]
pub(crate) struct DownloadTransferSummary {
    pub(crate) outcome: DownloadTerminalOutcome,
    pub(crate) destination: DownloadDestination,
    pub(crate) error_kind: Option<ErrorKind>,
    pub(crate) elapsed: Duration,
    pub(crate) metrics: TransferMetrics,
    pub(crate) requests: DownloadRequestMetrics,
    pub(crate) request_total: RequestMetrics,
    pub(crate) pending: TransferPendingStats,
    pub(crate) state: Option<DownloadStateSnapshot>,
    pub(crate) ranges_scheduled: u64,
    pub(crate) ranges_completed: u64,
    pub(crate) max_ranges_in_flight: usize,
    pub(crate) max_resident_parts: u64,
    pub(crate) destination_work: DownloadDestinationSummary,
    pub(crate) checksum_validation: Option<ChecksumValidation>,
}

/// Common and direction-specific facts captured at one terminal boundary.
pub(crate) struct DownloadTerminalReport {
    pub(crate) transfer_id: TransferId,
    pub(crate) status: TransferStatus,
    pub(crate) error_kind: Option<ErrorKind>,
    pub(crate) metrics: TransferMetrics,
    pub(crate) request_total: RequestMetrics,
    pub(crate) pending: TransferPendingStats,
    pub(crate) checksum_validation: Option<ChecksumValidation>,
}

/// Optional download observation kept separate from correctness state.
#[derive(Clone, Debug)]
pub(crate) enum DownloadObservability {
    Disabled,
    Enabled(Arc<EnabledDownloadObservability>),
}

#[derive(Debug)]
pub(crate) struct EnabledDownloadObservability {
    transitions: bool,
    terminal_reported: AtomicBool,
    state: Mutex<DownloadObservabilityState>,
    #[cfg(test)]
    last_summary: Mutex<Option<DownloadTransferSummary>>,
}

#[derive(Clone, Debug)]
struct DownloadObservabilityState {
    destination: DownloadDestination,
    latest_snapshot: Option<DownloadStateSnapshot>,
    ranges_scheduled: u64,
    ranges_completed: u64,
    max_ranges_in_flight: usize,
    max_resident_parts: u64,
    destination_work: DownloadDestinationSummary,
    requests: DownloadRequestMetrics,
}

impl DownloadObservability {
    /// Creates download observation using the client-level diagnostic policy.
    pub(crate) fn new(config: TransferDiagnosticsConfig, destination: DownloadDestination) -> Self {
        if !config.enable_summaries() {
            return Self::Disabled;
        }
        Self::Enabled(Arc::new(EnabledDownloadObservability {
            transitions: config.enable_transitions(),
            terminal_reported: AtomicBool::new(false),
            state: Mutex::new(DownloadObservabilityState {
                destination,
                latest_snapshot: None,
                ranges_scheduled: 0,
                ranges_completed: 0,
                max_ranges_in_flight: 0,
                max_resident_parts: 0,
                destination_work: DownloadDestinationSummary::default(),
                requests: DownloadRequestMetrics::default(),
            }),
            #[cfg(test)]
            last_summary: Mutex::new(None),
        }))
    }

    /// Starts a request measurement attributed to one download request kind.
    pub(crate) fn start_request(
        &self,
        ctx: &TransferContext,
        kind: DownloadRequestKind,
    ) -> DownloadRequestMeasurement {
        DownloadRequestMeasurement {
            measurement: Some(ctx.start_request_metrics()),
            observability: self.clone(),
            kind,
        }
    }

    fn record_request(&self, kind: DownloadRequestKind, metrics: &RequestMetrics) {
        if let Self::Enabled(enabled) = self {
            enabled
                .state
                .lock()
                .expect("lock poisoned")
                .requests
                .record(kind, metrics);
        }
    }

    /// Records the destination preparation boundary.
    pub(crate) fn destination_prepared(&self) {
        if let Self::Enabled(enabled) = self {
            let mut state = enabled.state.lock().expect("lock poisoned");
            if state.destination == DownloadDestination::File {
                state.destination_work.prepared = true;
            }
        }
    }

    /// Records a successfully issued range and updates state high-water marks.
    pub(crate) fn range_scheduled(
        &self,
        snapshot: DownloadStateSnapshot,
    ) -> Option<DownloadStateSnapshot> {
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.ranges_scheduled = state.ranges_scheduled.saturating_add(1);
        update_snapshot(&mut state, snapshot);
        enabled.transitions.then_some(snapshot)
    }

    /// Records a retired range and any batched disk drain it triggered.
    pub(crate) fn range_completed(
        &self,
        snapshot: DownloadStateSnapshot,
        drained_parts: u64,
    ) -> Option<DownloadStateSnapshot> {
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.ranges_completed = state.ranges_completed.saturating_add(1);
        state.destination_work.batched_drain_parts = state
            .destination_work
            .batched_drain_parts
            .saturating_add(drained_parts);
        update_snapshot(&mut state, snapshot);
        enabled.transitions.then_some(snapshot)
    }

    /// Records one eager disk drain used to relieve memory pressure.
    pub(crate) fn memory_relief_completed(
        &self,
        snapshot: DownloadStateSnapshot,
        drained_parts: u64,
    ) -> Option<DownloadStateSnapshot> {
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let mut state = enabled.state.lock().expect("lock poisoned");
        state.destination_work.memory_relief_drains = state
            .destination_work
            .memory_relief_drains
            .saturating_add(1);
        state.destination_work.memory_relief_parts = state
            .destination_work
            .memory_relief_parts
            .saturating_add(drained_parts);
        update_snapshot(&mut state, snapshot);
        enabled.transitions.then_some(snapshot)
    }

    /// Records terminal disk draining without changing its error policy.
    pub(crate) fn terminal_drain_completed(&self, result: &Result<u64, std::io::Error>) {
        if let Self::Enabled(enabled) = self {
            let mut state = enabled.state.lock().expect("lock poisoned");
            match result {
                Ok(parts) => {
                    state.destination_work.terminal_drain_parts = state
                        .destination_work
                        .terminal_drain_parts
                        .saturating_add(*parts);
                }
                Err(_) => state.destination_work.terminal_drain_failed = true,
            }
        }
    }

    /// Records whether successful destination finalization completed.
    pub(crate) fn destination_finalized(&self, result: &Result<u64, std::io::Error>) {
        if let Self::Enabled(enabled) = self {
            let mut state = enabled.state.lock().expect("lock poisoned");
            match result {
                Ok(parts) => {
                    state.destination_work.terminal_drain_parts = state
                        .destination_work
                        .terminal_drain_parts
                        .saturating_add(*parts);
                    if state.destination == DownloadDestination::File {
                        state.destination_work.file_finalized = true;
                    }
                }
                Err(_) => state.destination_work.finalization_failed = true,
            }
        }
    }

    /// Records the latest state and returns it at diagnostic detail level two.
    pub(crate) fn transition_snapshot(
        &self,
        snapshot: DownloadStateSnapshot,
    ) -> Option<DownloadStateSnapshot> {
        match self {
            Self::Disabled => None,
            Self::Enabled(enabled) => {
                let mut state = enabled.state.lock().expect("lock poisoned");
                update_snapshot(&mut state, snapshot);
                enabled.transitions.then_some(snapshot)
            }
        }
    }

    /// Emits one download state transition at diagnostic detail level two.
    pub(crate) fn publish_transition(
        &self,
        transfer_id: TransferId,
        transition: DownloadTransition,
        snapshot: Option<DownloadStateSnapshot>,
    ) {
        let Self::Enabled(enabled) = self else {
            return;
        };
        if enabled.transitions {
            emit_transition(transfer_id, transition, snapshot);
        }
    }

    /// Finalizes and emits the download terminal summary once.
    pub(crate) fn report_terminal(
        &self,
        report: DownloadTerminalReport,
    ) -> Option<DownloadTransferSummary> {
        let Self::Enabled(enabled) = self else {
            return None;
        };
        let outcome = DownloadTerminalOutcome::from_status(report.status)?;
        if enabled.terminal_reported.swap(true, Ordering::AcqRel) {
            return None;
        }

        let state = enabled.state.lock().expect("lock poisoned");
        let summary = DownloadTransferSummary {
            outcome,
            destination: state.destination,
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
            state: state.latest_snapshot,
            ranges_scheduled: state.ranges_scheduled,
            ranges_completed: state.ranges_completed,
            max_ranges_in_flight: state.max_ranges_in_flight,
            max_resident_parts: state.max_resident_parts,
            destination_work: state.destination_work,
            checksum_validation: report.checksum_validation,
        };
        drop(state);

        if enabled.transitions {
            emit_transition(
                report.transfer_id,
                DownloadTransition::Terminal(outcome),
                summary.state,
            );
        }
        #[cfg(test)]
        {
            *enabled.last_summary.lock().expect("lock poisoned") = Some(summary.clone());
        }
        emit_terminal_summary(report.transfer_id, &summary);
        Some(summary)
    }

    /// Returns the terminal report emitted by this observer.
    #[cfg(test)]
    pub(crate) fn test_terminal_summary(&self) -> Option<DownloadTransferSummary> {
        match self {
            Self::Disabled => None,
            Self::Enabled(enabled) => enabled.last_summary.lock().expect("lock poisoned").clone(),
        }
    }
}

fn update_snapshot(state: &mut DownloadObservabilityState, snapshot: DownloadStateSnapshot) {
    // Preserve the last state that carried download progress. A finalization
    // failure or duplicate terminal callback can observe `Terminal` after the
    // range-election winner already recorded the useful preterminal counters.
    if snapshot.state != DownloadStateKind::Terminal || state.latest_snapshot.is_none() {
        state.latest_snapshot = Some(snapshot);
    }
    state.max_ranges_in_flight = state.max_ranges_in_flight.max(snapshot.ranges_in_flight);
    state.max_resident_parts = state.max_resident_parts.max(snapshot.resident_parts);
}

/// In-progress request measurement attributed to one download request kind.
pub(crate) struct DownloadRequestMeasurement {
    measurement: Option<RequestMeasurement>,
    observability: DownloadObservability,
    kind: DownloadRequestKind,
}

impl std::fmt::Debug for DownloadRequestMeasurement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadRequestMeasurement")
            .field("kind", &self.kind)
            .field("finished", &self.measurement.is_none())
            .finish()
    }
}

impl DownloadRequestMeasurement {
    /// Returns the request metrics updated by the retry loop.
    pub(crate) fn metrics_mut(&mut self) -> &mut RequestMetrics {
        self.measurement
            .as_mut()
            .expect("request measurement already finished")
            .metrics_mut()
    }

    /// Publishes the request measurement to transfer and download aggregates.
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

impl Drop for DownloadRequestMeasurement {
    fn drop(&mut self) {
        if self.measurement.is_some() {
            let _ = self.publish();
        }
    }
}

fn emit_transition(
    transfer_id: TransferId,
    transition: DownloadTransition,
    snapshot: Option<DownloadStateSnapshot>,
) {
    let (transition, reason) = transition.fields();
    if let Some(snapshot) = snapshot {
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            transition,
            reason,
            state = snapshot.state.as_str(),
            remaining_bytes = snapshot.remaining_bytes,
            ranges_in_flight = snapshot.ranges_in_flight,
            ranges_issued = snapshot.ranges_issued,
            ranges_released = snapshot.ranges_released,
            resident_parts = snapshot.resident_parts,
            read_ahead_window = snapshot.read_ahead_window,
            memory_claim_pending = snapshot.memory_claim_pending,
            "download state transition",
        );
    } else {
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %transfer_id,
            transition,
            reason,
            "download state transition",
        );
    }
}

fn emit_terminal_summary(transfer_id: TransferId, summary: &DownloadTransferSummary) {
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
        destination = summary.destination.as_str(),
        error_kind = ?summary.error_kind,
        elapsed_us = duration_micros(summary.elapsed),
        expected_bytes = summary.metrics.total_bytes,
        network_rx = summary.metrics.network_rx,
        disk_write = summary.metrics.disk_write,
        requests = summary.request_total.requests,
        request_elapsed_us = duration_micros(summary.request_total.elapsed),
        max_request_elapsed_us = duration_micros(summary.request_total.max_elapsed),
        retry_reissues = summary.request_total.retry_reissues,
        throttle_reissues = summary.request_total.throttle_reissues,
        hedge_reissues = summary.request_total.hedge_reissues,
        retry_exhaustions = summary.request_total.retry_exhaustions,
        backoff_duration_us = duration_micros(summary.request_total.backoff_duration),
        discovery_head_requests = summary.requests.discovery_head.requests,
        discovery_head_elapsed_us = duration_micros(summary.requests.discovery_head.elapsed),
        discovery_head_retry_reissues = summary.requests.discovery_head.retry_reissues,
        discovery_range_requests = summary.requests.discovery_range.requests,
        discovery_range_elapsed_us = duration_micros(summary.requests.discovery_range.elapsed),
        discovery_range_retry_reissues = summary.requests.discovery_range.retry_reissues,
        discovery_range_hedge_reissues = summary.requests.discovery_range.hedge_reissues,
        discovery_part_requests = summary.requests.discovery_part.requests,
        discovery_part_elapsed_us = duration_micros(summary.requests.discovery_part.elapsed),
        discovery_part_retry_reissues = summary.requests.discovery_part.retry_reissues,
        range_requests = summary.requests.range.requests,
        range_elapsed_us = duration_micros(summary.requests.range.elapsed),
        range_max_elapsed_us = duration_micros(summary.requests.range.max_elapsed),
        range_retry_reissues = summary.requests.range.retry_reissues,
        range_throttle_reissues = summary.requests.range.throttle_reissues,
        range_hedge_reissues = summary.requests.range.hedge_reissues,
        range_retry_exhaustions = summary.requests.range.retry_exhaustions,
        range_backoff_us = duration_micros(summary.requests.range.backoff_duration),
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
        state = summary.state.map(|snapshot| snapshot.state.as_str()),
        remaining_bytes = summary.state.and_then(|snapshot| snapshot.remaining_bytes),
        ranges_in_flight = summary.state.map(|snapshot| snapshot.ranges_in_flight),
        ranges_issued = summary.state.map(|snapshot| snapshot.ranges_issued),
        ranges_released = summary.state.map(|snapshot| snapshot.ranges_released),
        resident_parts = summary.state.map(|snapshot| snapshot.resident_parts),
        read_ahead_window = summary.state.map(|snapshot| snapshot.read_ahead_window),
        memory_claim_pending = summary.state.map(|snapshot| snapshot.memory_claim_pending),
        ranges_scheduled = summary.ranges_scheduled,
        ranges_completed = summary.ranges_completed,
        max_ranges_in_flight = summary.max_ranges_in_flight,
        max_resident_parts = summary.max_resident_parts,
        destination_prepared = summary.destination_work.prepared,
        file_finalized = summary.destination_work.file_finalized,
        finalization_failed = summary.destination_work.finalization_failed,
        batched_drain_parts = summary.destination_work.batched_drain_parts,
        memory_relief_drains = summary.destination_work.memory_relief_drains,
        memory_relief_parts = summary.destination_work.memory_relief_parts,
        terminal_drain_parts = summary.destination_work.terminal_drain_parts,
        terminal_drain_failed = summary.destination_work.terminal_drain_failed,
        checksum_validation = ?summary.checksum_validation,
        "download transfer terminal",
    );
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    fn config(detail: u64) -> TransferDiagnosticsConfig {
        crate::config::DiagnosticsConfig::for_test(
            crate::config::MemoryDiagnosticsConfig::default(),
            detail,
        )
        .transfer()
    }

    fn snapshot() -> DownloadStateSnapshot {
        DownloadStateSnapshot {
            state: DownloadStateKind::Transferring,
            remaining_bytes: Some(8 * 1024 * 1024),
            ranges_in_flight: 2,
            ranges_issued: 3,
            ranges_released: 1,
            resident_parts: 2,
            read_ahead_window: 4,
            memory_claim_pending: false,
        }
    }

    fn transfer_metrics() -> TransferMetrics {
        let now = Instant::now();
        TransferMetrics {
            network_tx: 0,
            network_rx: 8 * 1024 * 1024,
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
        request_total: RequestMetrics,
    ) -> DownloadTerminalReport {
        DownloadTerminalReport {
            transfer_id: TransferId {
                id: transfer_id,
                parent: None,
            },
            status,
            error_kind: None,
            metrics: transfer_metrics(),
            request_total,
            pending: TransferPendingStats::default(),
            checksum_validation: None,
        }
    }

    #[test]
    fn disabled_collection_does_not_emit_snapshots() {
        let observability = DownloadObservability::new(config(0), DownloadDestination::Stream);
        assert!(matches!(observability, DownloadObservability::Disabled));
        assert_eq!(observability.transition_snapshot(snapshot()), None);
    }

    #[test]
    fn summary_and_transition_levels_are_distinct() {
        let summary = DownloadObservability::new(config(1), DownloadDestination::Stream);
        assert_eq!(summary.transition_snapshot(snapshot()), None);

        let transitions = DownloadObservability::new(config(2), DownloadDestination::Stream);
        assert_eq!(
            transitions.transition_snapshot(snapshot()),
            Some(snapshot())
        );
    }

    #[test]
    fn request_kind_metrics_remain_separate() {
        let observability = DownloadObservability::new(config(1), DownloadDestination::Stream);
        let mut range = RequestMetrics::default();
        range.record_request(Duration::from_micros(7));
        range.record_retry_reissue(Duration::from_micros(3));
        range.record_throttle_reissue(Duration::from_micros(5));
        range.record_hedge_reissue(Duration::from_micros(2));
        range.record_retry_exhaustion();
        observability.record_request(DownloadRequestKind::Range, &range);

        let summary = observability
            .report_terminal(terminal_report(1, TransferStatus::Completed, range))
            .expect("terminal report");
        assert_eq!(summary.requests.range, range);
        assert_eq!(summary.requests.discovery_head, RequestMetrics::default());
    }

    #[test]
    fn terminal_summary_is_reported_once() {
        let observability = DownloadObservability::new(config(1), DownloadDestination::File);
        let first = observability.report_terminal(terminal_report(
            2,
            TransferStatus::Completed,
            RequestMetrics::default(),
        ));
        let second = observability.report_terminal(terminal_report(
            2,
            TransferStatus::Completed,
            RequestMetrics::default(),
        ));
        assert_eq!(
            first.expect("first report").destination,
            DownloadDestination::File
        );
        assert!(second.is_none());
    }

    #[test]
    fn terminal_state_does_not_erase_the_last_progress_snapshot() {
        let observability = DownloadObservability::new(config(1), DownloadDestination::Stream);
        let _ = observability.transition_snapshot(snapshot());
        let _ = observability.transition_snapshot(DownloadStateSnapshot::inactive(
            DownloadStateKind::Terminal,
            4,
        ));

        let summary = observability
            .report_terminal(terminal_report(
                4,
                TransferStatus::Failed,
                RequestMetrics::default(),
            ))
            .expect("terminal report");
        assert_eq!(summary.state, Some(snapshot()));
    }

    #[test]
    fn destination_and_range_work_are_aggregated() {
        let observability = DownloadObservability::new(config(1), DownloadDestination::File);
        observability.destination_prepared();
        observability.range_scheduled(snapshot());
        observability.range_completed(snapshot(), 2);
        observability.memory_relief_completed(snapshot(), 1);
        observability.destination_finalized(&Ok(3));

        let summary = observability
            .report_terminal(terminal_report(
                3,
                TransferStatus::Completed,
                RequestMetrics::default(),
            ))
            .expect("terminal report");
        assert_eq!(summary.ranges_scheduled, 1);
        assert_eq!(summary.ranges_completed, 1);
        assert_eq!(summary.destination_work.batched_drain_parts, 2);
        assert_eq!(summary.destination_work.memory_relief_parts, 1);
        assert_eq!(summary.destination_work.terminal_drain_parts, 3);
        assert!(summary.destination_work.file_finalized);
    }

    #[test]
    fn download_transition_vocabulary_is_closed_and_stable() {
        assert_eq!(
            DownloadTransition::DiscoveryScheduled.fields(),
            ("work_scheduled", "discovery")
        );
        assert_eq!(
            DownloadTransition::Pending(DownloadPendingReason::ReadAhead).fields(),
            ("poll_pending", "read_ahead")
        );
        assert_eq!(
            DownloadTransition::MemoryReliefCompleted.fields(),
            ("work_completed", "memory_relief")
        );
        assert_eq!(
            DownloadTransition::Terminal(DownloadTerminalOutcome::Completed).fields(),
            ("terminal", "completed")
        );
    }
}
