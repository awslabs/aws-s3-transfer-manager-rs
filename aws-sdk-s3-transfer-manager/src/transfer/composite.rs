/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Diagnostic types shared by composite transfers.
//!
//! A composite transfer spawns one child transfer per entry it lists. Each
//! composite operation keeps its own events, snapshot, and records, because a
//! record's target must be a constant at its callsite. The pending reasons and
//! the terminal summary they describe do not depend on the operation.

use std::time::Duration;

use crate::error::ErrorKind;
use crate::types::TransferMetrics;

use super::{
    PendingCategory, PendingCause, TerminalOutcome, TransferContext, TransferPendingStats,
};

/// Why a composite transfer cannot produce another work item.
///
/// Each reason becomes the [`PendingCause`] recorded with the transfer's
/// pending interval, shown on each variant as its category and reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompositePendingReason {
    /// The walk has not produced an entry to spawn.
    ///
    /// `source`, `listing`.
    Listing,
    /// Entries are waiting, and the child concurrency limit is reached.
    ///
    /// `in_flight_work`, `child_capacity`.
    ChildCapacity,
    /// The walk is exhausted, and running children must finish.
    ///
    /// `in_flight_work`, `child_completion`.
    ChildCompletion,
    /// Reaped children are still being recorded.
    ///
    /// `in_flight_work`, `reap_completion`.
    Reaping,
    /// The transfer is cancelled or failed, and in-flight work must drain.
    ///
    /// `in_flight_work`, `draining`.
    Draining,
}

impl From<CompositePendingReason> for PendingCause {
    fn from(reason: CompositePendingReason) -> Self {
        match reason {
            CompositePendingReason::Listing => Self::new(PendingCategory::Source, "listing"),
            CompositePendingReason::ChildCapacity => Self::in_flight_work("child_capacity"),
            CompositePendingReason::ChildCompletion => Self::in_flight_work("child_completion"),
            CompositePendingReason::Reaping => Self::in_flight_work("reap_completion"),
            CompositePendingReason::Draining => Self::in_flight_work("draining"),
        }
    }
}

/// Terminal report for one composite transfer.
#[derive(Clone, Debug)]
pub(crate) struct CompositeTransferSummary {
    /// Terminal outcome reported by the common transfer context.
    pub(crate) outcome: TerminalOutcome,
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

impl CompositeTransferSummary {
    /// Builds the summary from `ctx` and the parent's child counts.
    ///
    /// Returns `None` while the transfer is active.
    pub(crate) fn new(ctx: &TransferContext, successful: u64, failed: usize) -> Option<Self> {
        let outcome = TerminalOutcome::from_status(ctx.transfer_status())?;
        let metrics = ctx.metrics();
        Some(Self {
            outcome,
            error_kind: ctx.error_kind(),
            elapsed: metrics
                .finished_at
                .map(|finished_at| finished_at.saturating_duration_since(metrics.started_at))
                .unwrap_or_default(),
            metrics,
            successful,
            failed,
            pending: ctx.pending_stats(),
        })
    }
}
