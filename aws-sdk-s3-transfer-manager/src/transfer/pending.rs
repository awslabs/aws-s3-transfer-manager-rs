/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Scheduler-visible pending state for one transfer.
//!
//! Transfer state machines record why a poll cannot produce work. The
//! scheduler records the first wake and the next poll. This module joins those
//! boundaries for diagnostics without participating in scheduling decisions.
//!
//! ```text
//! set_pending()             first wake                 begin_poll()
//!      |-------------------------|--------------------------|
//!           pending_to_wake             wake_to_repoll
//!      |----------------------------------------------------|
//!                         pending_to_repoll
//! ```

use std::time::{Duration, Instant};

use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};
use crate::runtime::sync::sync::Mutex;

use super::TransferId;

const PENDING_CATEGORY_COUNT: usize = 5;

/// Coarse resource or work class preventing a transfer from producing work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingCategory {
    /// Caller-provided source data or source work is not ready.
    Source,
    /// Shared buffer-pool admission is pending.
    Memory,
    /// Downstream delivery or consumption must release capacity.
    Consumer,
    /// Dispatched transfer work must retire.
    InFlightWork,
    /// Direction-specific state without a common category.
    #[allow(dead_code)] // Reserved escape hatch for future transfer kinds.
    Other,
}

impl PendingCategory {
    const fn index(self) -> usize {
        match self {
            Self::Source => 0,
            Self::Memory => 1,
            Self::Consumer => 2,
            Self::InFlightWork => 3,
            Self::Other => 4,
        }
    }

    /// Returns the stable diagnostic value for this category.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Memory => "memory",
            Self::Consumer => "consumer",
            Self::InFlightWork => "in_flight_work",
            Self::Other => "other",
        }
    }
}

/// Common pending category paired with direction-owned detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PendingCause {
    /// Common category used for aggregation across transfer kinds.
    pub(crate) category: PendingCategory,
    /// Static direction-specific reason used in diagnostic records.
    pub(crate) reason: &'static str,
}

impl PendingCause {
    /// Creates a cause from a common category and direction-specific reason.
    pub(crate) const fn new(category: PendingCategory, reason: &'static str) -> Self {
        Self { category, reason }
    }

    /// Creates a cause for dispatched work that must retire before repolling.
    pub(crate) const fn in_flight_work(reason: &'static str) -> Self {
        Self::new(PendingCategory::InFlightWork, reason)
    }

    /// Creates a cause that has no current common category.
    #[allow(dead_code)] // Used by test transfers; reserved for future production causes.
    pub(crate) const fn other(reason: &'static str) -> Self {
        Self::new(PendingCategory::Other, reason)
    }
}

/// Aggregate timings for one common pending category.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingCategoryStats {
    /// Number of pending intervals entered.
    pub(crate) count: u64,
    /// Sum of pending-to-first-wake durations.
    pub(crate) pending_to_wake: Duration,
    /// Longest pending-to-first-wake duration.
    pub(crate) max_pending_to_wake: Duration,
    /// Sum of first-wake-to-repoll durations.
    pub(crate) wake_to_repoll: Duration,
    /// Longest first-wake-to-repoll duration.
    pub(crate) max_wake_to_repoll: Duration,
    /// Sum of pending-to-repoll durations.
    pub(crate) pending_to_repoll: Duration,
    /// Longest pending-to-repoll duration.
    pub(crate) max_pending_to_repoll: Duration,
}

/// Accumulated pending intervals for one transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransferPendingStats {
    categories: [PendingCategoryStats; PENDING_CATEGORY_COUNT],
    /// Cause that remained pending when the transfer became terminal.
    pub(crate) terminal_cause: Option<PendingCause>,
}

impl Default for TransferPendingStats {
    fn default() -> Self {
        Self {
            categories: [PendingCategoryStats::default(); PENDING_CATEGORY_COUNT],
            terminal_cause: None,
        }
    }
}

impl TransferPendingStats {
    /// Returns the accumulated values for one common category.
    pub(crate) fn category(&self, category: PendingCategory) -> PendingCategoryStats {
        self.categories[category.index()]
    }
}

/// Current scheduler-visible interval.
#[derive(Clone, Copy, Debug)]
enum PendingIntervalState {
    /// No pending interval is open.
    Idle,
    /// The transfer returned `Pending` and has not observed a wake.
    Pending {
        cause: PendingCause,
        pending_at: Instant,
    },
    /// The first wake was observed and the scheduler has not repolled.
    Woken {
        cause: PendingCause,
        pending_at: Instant,
        woken_at: Instant,
    },
}

/// State updated together under the pending-state lock.
#[derive(Debug)]
struct PendingData {
    interval: PendingIntervalState,
    stats: TransferPendingStats,
}

impl Default for PendingData {
    fn default() -> Self {
        Self {
            interval: PendingIntervalState::Idle,
            stats: TransferPendingStats::default(),
        }
    }
}

/// Live pending accounting for one transfer.
///
/// `TransferContext` allocates this state only when transfer diagnostic
/// summaries are enabled. `interval_open` avoids the lock and clock on ordinary
/// ready polls and wakes unrelated to a pending interval.
pub(crate) struct TransferPendingState {
    interval_open: AtomicBool,
    emit_events: bool,
    data: Mutex<PendingData>,
}

impl TransferPendingState {
    /// Creates enabled state with optional per-event diagnostic records.
    pub(crate) fn new(emit_events: bool) -> Self {
        Self {
            interval_open: AtomicBool::new(false),
            emit_events,
            data: Mutex::new(PendingData::default()),
        }
    }

    /// Opens one pending interval if no interval is currently active.
    ///
    /// Repeated calls before a wake and repoll are ignored.
    pub(crate) fn record_pending(&self, id: TransferId, cause: PendingCause) {
        if self.interval_open.load(Ordering::Acquire) {
            return;
        }

        if self.record_pending_at(cause, Instant::now()) && self.emit_events {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %id,
                category = cause.category.as_str(),
                reason = cause.reason,
                "transfer pending",
            );
        }
    }

    /// Records the first scheduler wake for the current pending interval.
    ///
    /// Wakes without an open interval and repeated wakes are ignored. The
    /// scheduler backfills this operation when a wake races before
    /// `set_pending`.
    pub(crate) fn record_wake(&self, id: TransferId) {
        if !self.interval_open.load(Ordering::Acquire) {
            return;
        }

        if let Some((cause, elapsed)) = self.record_wake_at(Instant::now()) {
            if self.emit_events {
                tracing::trace!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %id,
                    category = cause.category.as_str(),
                    reason = cause.reason,
                    pending_to_wake = ?elapsed,
                    "pending transfer woke",
                );
            }
        }
    }

    /// Records that the scheduler is beginning another poll.
    ///
    /// An open interval is closed at this boundary and contributes its
    /// pending-to-wake, wake-to-repoll, and pending-to-repoll measurements.
    /// Calls without an open interval avoid the lock and clock.
    pub(crate) fn begin_poll(&self, id: TransferId) {
        if !self.interval_open.load(Ordering::Acquire) {
            return;
        }

        if let Some(interval) = self.record_repoll_at(Instant::now()) {
            if self.emit_events {
                tracing::trace!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %id,
                    category = interval.cause.category.as_str(),
                    reason = interval.cause.reason,
                    pending_to_wake = ?interval.pending_to_wake,
                    wake_to_repoll = ?interval.wake_to_repoll,
                    pending_to_repoll = ?interval.pending_to_repoll,
                    "pending transfer repolled",
                );
            }
        }
    }

    /// Closes an outstanding interval when the transfer becomes terminal.
    ///
    /// The first terminal closure retains the pending cause. Repeated terminal
    /// notifications do not alter the accumulated statistics.
    pub(crate) fn record_terminal(&self, id: TransferId) {
        if !self.interval_open.load(Ordering::Acquire) {
            return;
        }

        if let Some((cause, elapsed)) = self.record_terminal_at(Instant::now()) {
            if self.emit_events {
                tracing::trace!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %id,
                    category = cause.category.as_str(),
                    reason = cause.reason,
                    pending_to_terminal = ?elapsed,
                    "pending transfer became terminal",
                );
            }
        }
    }

    fn record_pending_at(&self, cause: PendingCause, now: Instant) -> bool {
        let mut data = self.data.lock();
        if !matches!(data.interval, PendingIntervalState::Idle) {
            return false;
        }

        let category = &mut data.stats.categories[cause.category.index()];
        category.count = category.count.saturating_add(1);
        data.interval = PendingIntervalState::Pending {
            cause,
            pending_at: now,
        };
        self.interval_open.store(true, Ordering::Release);
        true
    }

    fn record_wake_at(&self, now: Instant) -> Option<(PendingCause, Duration)> {
        if !self.interval_open.load(Ordering::Acquire) {
            return None;
        }

        let mut data = self.data.lock();
        let PendingIntervalState::Pending { cause, pending_at } = data.interval else {
            return None;
        };

        let elapsed = now.saturating_duration_since(pending_at);
        let category = &mut data.stats.categories[cause.category.index()];
        category.pending_to_wake = category.pending_to_wake.saturating_add(elapsed);
        category.max_pending_to_wake = category.max_pending_to_wake.max(elapsed);
        data.interval = PendingIntervalState::Woken {
            cause,
            pending_at,
            woken_at: now,
        };
        Some((cause, elapsed))
    }

    fn record_repoll_at(&self, now: Instant) -> Option<PendingInterval> {
        if !self.interval_open.swap(false, Ordering::AcqRel) {
            return None;
        }

        let mut data = self.data.lock();
        let current = std::mem::replace(&mut data.interval, PendingIntervalState::Idle);
        let interval = match current {
            PendingIntervalState::Idle => return None,
            PendingIntervalState::Pending { cause, pending_at } => PendingInterval {
                cause,
                pending_to_wake: None,
                wake_to_repoll: None,
                pending_to_repoll: now.saturating_duration_since(pending_at),
            },
            PendingIntervalState::Woken {
                cause,
                pending_at,
                woken_at,
            } => PendingInterval {
                cause,
                pending_to_wake: Some(woken_at.saturating_duration_since(pending_at)),
                wake_to_repoll: Some(now.saturating_duration_since(woken_at)),
                pending_to_repoll: now.saturating_duration_since(pending_at),
            },
        };

        let category = &mut data.stats.categories[interval.cause.category.index()];
        category.pending_to_repoll = category
            .pending_to_repoll
            .saturating_add(interval.pending_to_repoll);
        category.max_pending_to_repoll = category
            .max_pending_to_repoll
            .max(interval.pending_to_repoll);
        if let Some(elapsed) = interval.wake_to_repoll {
            category.wake_to_repoll = category.wake_to_repoll.saturating_add(elapsed);
            category.max_wake_to_repoll = category.max_wake_to_repoll.max(elapsed);
        }
        Some(interval)
    }

    fn record_terminal_at(&self, now: Instant) -> Option<(PendingCause, Duration)> {
        if !self.interval_open.swap(false, Ordering::AcqRel) {
            return None;
        }

        let mut data = self.data.lock();
        let current = std::mem::replace(&mut data.interval, PendingIntervalState::Idle);
        let (cause, pending_at) = match current {
            PendingIntervalState::Idle => return None,
            PendingIntervalState::Pending { cause, pending_at }
            | PendingIntervalState::Woken {
                cause, pending_at, ..
            } => (cause, pending_at),
        };
        data.stats.terminal_cause = Some(cause);
        Some((cause, now.saturating_duration_since(pending_at)))
    }

    /// Returns a copy of the accumulated statistics.
    pub(crate) fn snapshot(&self) -> TransferPendingStats {
        self.data.lock().stats
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingInterval {
    cause: PendingCause,
    pending_to_wake: Option<Duration>,
    wake_to_repoll: Option<Duration>,
    pending_to_repoll: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cause(category: PendingCategory) -> PendingCause {
        PendingCause::new(category, "test")
    }

    #[test]
    fn pending_wake_repoll_records_one_interval() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();
        let wake = start + Duration::from_millis(7);
        let repoll = wake + Duration::from_millis(3);

        assert!(pending.record_pending_at(cause(PendingCategory::Memory), start));
        assert_eq!(
            pending.record_wake_at(wake),
            Some((cause(PendingCategory::Memory), Duration::from_millis(7)))
        );
        let interval = pending.record_repoll_at(repoll).expect("active interval");
        assert_eq!(interval.pending_to_repoll, Duration::from_millis(10));

        let stats = pending.snapshot().category(PendingCategory::Memory);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.pending_to_wake, Duration::from_millis(7));
        assert_eq!(stats.max_pending_to_wake, Duration::from_millis(7));
        assert_eq!(stats.wake_to_repoll, Duration::from_millis(3));
        assert_eq!(stats.max_wake_to_repoll, Duration::from_millis(3));
        assert_eq!(stats.pending_to_repoll, Duration::from_millis(10));
        assert_eq!(stats.max_pending_to_repoll, Duration::from_millis(10));
    }

    #[test]
    fn repeated_wakes_keep_the_first_endpoint() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();

        assert!(pending.record_pending_at(cause(PendingCategory::Source), start));
        assert!(pending
            .record_wake_at(start + Duration::from_millis(2))
            .is_some());
        assert_eq!(
            pending.record_wake_at(start + Duration::from_millis(9)),
            None
        );
        pending
            .record_repoll_at(start + Duration::from_millis(10))
            .expect("active interval");

        let stats = pending.snapshot().category(PendingCategory::Source);
        assert_eq!(stats.pending_to_wake, Duration::from_millis(2));
        assert_eq!(stats.wake_to_repoll, Duration::from_millis(8));
    }

    #[test]
    fn wake_before_pending_is_backfilled_once() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();

        assert_eq!(pending.record_wake_at(start), None);
        assert!(pending.record_pending_at(cause(PendingCategory::Consumer), start));
        assert!(pending
            .record_wake_at(start + Duration::from_millis(1))
            .is_some());
        assert_eq!(
            pending.record_wake_at(start + Duration::from_millis(2)),
            None
        );
        pending
            .record_repoll_at(start + Duration::from_millis(3))
            .expect("active interval");

        let stats = pending.snapshot().category(PendingCategory::Consumer);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.pending_to_wake, Duration::from_millis(1));
    }

    #[test]
    fn terminal_closure_is_idempotent_and_retains_cause() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();
        let cause = cause(PendingCategory::InFlightWork);

        assert!(pending.record_pending_at(cause, start));
        assert!(pending
            .record_terminal_at(start + Duration::from_millis(5))
            .is_some());
        assert_eq!(
            pending.record_terminal_at(start + Duration::from_millis(6)),
            None
        );
        assert_eq!(pending.snapshot().terminal_cause, Some(cause));
    }

    #[test]
    fn idle_repolls_do_not_create_intervals() {
        let pending = TransferPendingState::new(false);
        assert!(pending.record_repoll_at(Instant::now()).is_none());
        assert_eq!(
            pending.snapshot(),
            TransferPendingStats::default(),
            "ready polls must not affect pending accounting"
        );
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use std::sync::Arc;

    use loom::thread;

    #[test]
    fn wake_racing_pending_is_reconciled_by_backfill() {
        loom::model(|| {
            let pending = Arc::new(TransferPendingState::new(false));
            let now = Instant::now();

            let poll_state = Arc::clone(&pending);
            let poller = thread::spawn(move || {
                poll_state
                    .record_pending_at(PendingCause::new(PendingCategory::Memory, "loom"), now);
            });

            let wake_state = Arc::clone(&pending);
            let waker = thread::spawn(move || {
                wake_state.record_wake_at(now);
            });

            poller.join().unwrap();
            waker.join().unwrap();

            // The descriptor release-and-recheck path performs this idempotent
            // backfill after `poll_work` has published Pending.
            pending.record_wake_at(now);
            pending.record_repoll_at(now);

            let stats = pending.snapshot().category(PendingCategory::Memory);
            assert_eq!(stats.count, 1);
            assert_eq!(stats.pending_to_wake, Duration::ZERO);
        });
    }
}
