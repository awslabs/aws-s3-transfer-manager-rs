/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `download_objects` state-machine event and terminal summary emission.
//!
//! State-machine events are emitted at TRACE only when transfer diagnostics
//! report state-machine transitions. The terminal summary is emitted once per
//! transfer at DEBUG.

use std::time::Duration;

use crate::error::ErrorKind;
use crate::operation::download::DownloadTerminalOutcome;
use crate::transfer::{emit_pending_details, TransferContext, TransferId, TransferPendingStats};
use crate::types::TransferMetrics;

/// Closed diagnostic vocabulary for `download_objects` state-machine events.
///
/// Each variant maps to one stable `event` and `reason` field pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadObjectsEvent {
    /// A listing page fetch was scheduled.
    WalkDispatched,
    /// The listing is exhausted, and no further entries will be listed.
    ListingExhausted,
    /// A child download was orchestrated for one entry.
    ChildSpawned,
    /// Terminal children were removed and scheduled for reaping.
    ReapScheduled,
    /// Reaped children's results were recorded.
    ReapCompleted,
}

impl DownloadObjectsEvent {
    fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::WalkDispatched => ("work_scheduled", "advance_walker"),
            Self::ListingExhausted => ("dispatch_closed", "listing_exhausted"),
            Self::ChildSpawned => ("child_spawned", "orchestrated"),
            Self::ReapScheduled => ("work_scheduled", "join_children"),
            Self::ReapCompleted => ("state_changed", "children_reaped"),
        }
    }
}

/// Copyable counters read from `download_objects` state under its lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownloadObjectsStateSnapshot {
    pub(crate) pending_entries: usize,
    pub(crate) walk_in_flight: bool,
    pub(crate) children: usize,
    pub(crate) children_reserved: usize,
    pub(crate) reaping_in_flight: usize,
    pub(crate) successful: u64,
    pub(crate) failed: usize,
}

fn events_enabled(ctx: &TransferContext) -> bool {
    ctx.handle.config.diagnostics().transfer().events_enabled()
}

/// Emits one parent state-machine event with the parent's counters.
pub(crate) fn emit_event(
    ctx: &TransferContext,
    event: DownloadObjectsEvent,
    snapshot: DownloadObjectsStateSnapshot,
) {
    if !events_enabled(ctx) {
        return;
    }
    let (event_name, reason) = event.fields();
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER_DOWNLOAD_OBJECTS,
        tid = %ctx.id,
        event = event_name,
        reason,
        pending_entries = snapshot.pending_entries,
        walk_in_flight = snapshot.walk_in_flight,
        children = snapshot.children,
        children_reserved = snapshot.children_reserved,
        reaping_in_flight = snapshot.reaping_in_flight,
        successful = snapshot.successful,
        failed = snapshot.failed,
        "download_objects state-machine event",
    );
}

/// Emits one event about a single child, naming its transfer ID and key.
pub(crate) fn emit_child_event(
    ctx: &TransferContext,
    event: DownloadObjectsEvent,
    child: TransferId,
    key: &str,
) {
    if !events_enabled(ctx) {
        return;
    }
    let (event_name, reason) = event.fields();
    tracing::trace!(
        target: crate::telemetry::TARGET_TRANSFER_DOWNLOAD_OBJECTS,
        tid = %ctx.id,
        event = event_name,
        reason,
        child_id = ?child,
        key,
        "download_objects state-machine event",
    );
}

/// Terminal report for one `download_objects` transfer.
#[derive(Clone, Debug)]
pub(crate) struct DownloadObjectsTransferSummary {
    /// Terminal outcome reported by the common transfer context.
    pub(crate) outcome: DownloadTerminalOutcome,
    /// Transfer-manager error classification for a failed transfer.
    pub(crate) error_kind: Option<ErrorKind>,
    /// Time from transfer creation through terminal reporting.
    pub(crate) elapsed: Duration,
    /// Common byte and I/O metrics. Bytes cover successful children only.
    pub(crate) metrics: TransferMetrics,
    /// Children that completed successfully.
    pub(crate) successful: u64,
    /// Children and entries recorded as failed.
    pub(crate) failed: usize,
    /// Scheduler pending intervals, present when summaries are enabled.
    pub(crate) pending: Option<TransferPendingStats>,
}

/// Emits the `download_objects` terminal summary and returns it.
///
/// Returns `None` and emits nothing while the transfer is still active.
/// `snapshot` is the parent state the transfer ended in. Pending totals are
/// included when summaries are enabled, and per-category pending detail when
/// events are enabled.
pub(crate) fn emit_terminal_summary(
    ctx: &TransferContext,
    snapshot: DownloadObjectsStateSnapshot,
) -> Option<DownloadObjectsTransferSummary> {
    let outcome = DownloadTerminalOutcome::from_status(ctx.transfer_status())?;
    let metrics = ctx.metrics();
    let summary = DownloadObjectsTransferSummary {
        outcome,
        error_kind: ctx.error_kind(),
        elapsed: metrics
            .finished_at
            .map(|finished_at| finished_at.saturating_duration_since(metrics.started_at))
            .unwrap_or_default(),
        metrics,
        successful: snapshot.successful,
        failed: snapshot.failed,
        pending: ctx.pending_stats(),
    };
    let Some(pending) = summary.pending else {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER_DOWNLOAD_OBJECTS,
            tid = %ctx.id,
            outcome = summary.outcome.as_str(),
            error_kind = ?summary.error_kind,
            elapsed = ?summary.elapsed,
            successful = summary.successful,
            failed = summary.failed,
            network_rx = summary.metrics.network_rx,
            disk_write = summary.metrics.disk_write,
            "download_objects transfer terminal",
        );
        return Some(summary);
    };
    if events_enabled(ctx) {
        emit_pending_details(ctx.id, &pending);
    }
    tracing::debug!(
        target: crate::telemetry::TARGET_TRANSFER_DOWNLOAD_OBJECTS,
        tid = %ctx.id,
        outcome = summary.outcome.as_str(),
        error_kind = ?summary.error_kind,
        elapsed = ?summary.elapsed,
        successful = summary.successful,
        failed = summary.failed,
        network_rx = summary.metrics.network_rx,
        disk_write = summary.metrics.disk_write,
        pending_intervals = pending.interval_count(),
        pending_time_sum = ?pending.pending_duration(),
        pending_time_max = ?pending.max_pending_duration(),
        "download_objects transfer terminal",
    );
    Some(summary)
}
