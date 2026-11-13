/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `download_objects` state-machine event and terminal summary emission.
//!
//! State-machine events are emitted at TRACE only when transfer diagnostics
//! report state-machine transitions. The terminal summary is emitted once per
//! transfer at DEBUG.

use crate::transfer::{emit_pending_details, CompositeTransferSummary, TransferContext};

/// Closed diagnostic vocabulary for `download_objects` state-machine events.
///
/// Each event is one TRACE record, `download_objects state-machine event`, on
/// the `transfer::download_objects` target. It is written only when transfer
/// diagnostics report state-machine transitions (`transfer.detail=2`). The
/// record carries the variant's `event` and `reason` fields, shown on each
/// variant, and the parent's counters from [`DownloadObjectsStateSnapshot`].
/// The field values are stable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadObjectsEvent {
    /// A listing page fetch was scheduled.
    ///
    /// `event="work_scheduled"`, `reason="advance_walker"`.
    WalkDispatched,
    /// The listing is exhausted, and no further entries will be listed.
    ///
    /// `event="dispatch_closed"`, `reason="listing_exhausted"`.
    ListingExhausted,
    /// Terminal children were removed from the running set, and a reap was
    /// scheduled to join them.
    ///
    /// `event="work_scheduled"`, `reason="join_children"`.
    ReapScheduled,
    /// A reap joined its children and recorded their results.
    ///
    /// `event="state_changed"`, `reason="children_reaped"`.
    ReapCompleted,
}

impl DownloadObjectsEvent {
    /// Returns the event's stable `event` and `reason` field values.
    fn fields(self) -> (&'static str, &'static str) {
        match self {
            Self::WalkDispatched => ("work_scheduled", "advance_walker"),
            Self::ListingExhausted => ("dispatch_closed", "listing_exhausted"),
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

/// Emits the `download_objects` terminal summary and returns it.
///
/// Returns `None` and emits nothing while the transfer is still active.
/// `snapshot` is the parent state the transfer ended in. Pending totals are
/// included when summaries are enabled, and per-category pending detail when
/// events are enabled.
pub(crate) fn emit_terminal_summary(
    ctx: &TransferContext,
    snapshot: DownloadObjectsStateSnapshot,
) -> Option<CompositeTransferSummary> {
    let summary = CompositeTransferSummary::new(ctx, snapshot.successful, snapshot.failed)?;
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
