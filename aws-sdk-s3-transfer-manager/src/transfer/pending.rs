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
//! set_pending()             first wake              next poll starts
//!      |-------------------------|--------------------------|
//!           pending_to_wake             scheduler_delay
//!      |----------------------------------------------------|
//!                         pending_duration
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
    /// Common categories in stable diagnostic order.
    pub(crate) const ALL: [Self; PENDING_CATEGORY_COUNT] = [
        Self::Source,
        Self::Memory,
        Self::Consumer,
        Self::InFlightWork,
        Self::Other,
    ];

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

    /// Creates a cause for dispatched work that must retire before the next poll.
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
    /// Sum of first-wake-to-next-poll scheduler delays.
    pub(crate) scheduler_delay: Duration,
    /// Longest first-wake-to-next-poll scheduler delay.
    pub(crate) max_scheduler_delay: Duration,
    /// Sum of pending intervals closed by a later poll.
    pub(crate) pending_duration: Duration,
    /// Longest pending interval closed by a later poll.
    pub(crate) max_pending_duration: Duration,
}

/// Accumulated pending intervals for one transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransferPendingStats {
    categories: [PendingCategoryStats; PENDING_CATEGORY_COUNT],
    /// Cause that remained pending when the transfer became terminal.
    pub(crate) terminal_cause: Option<PendingCause>,
    /// Duration of the interval closed by terminal transfer state.
    pub(crate) terminal_duration: Option<Duration>,
}

impl Default for TransferPendingStats {
    fn default() -> Self {
        Self {
            categories: [PendingCategoryStats::default(); PENDING_CATEGORY_COUNT],
            terminal_cause: None,
            terminal_duration: None,
        }
    }
}

impl TransferPendingStats {
    /// Returns the accumulated values for one common category.
    pub(crate) fn category(&self, category: PendingCategory) -> PendingCategoryStats {
        self.categories[category.index()]
    }

    /// Returns the number of pending intervals entered by this transfer.
    pub(crate) fn interval_count(&self) -> u64 {
        self.categories
            .iter()
            .fold(0, |total, category| total.saturating_add(category.count))
    }

    /// Returns the total duration of all pending intervals.
    ///
    /// An interval ends when the next poll begins or the transfer becomes
    /// terminal.
    pub(crate) fn pending_duration(&self) -> Duration {
        self.categories
            .iter()
            .fold(Duration::ZERO, |total, category| {
                total.saturating_add(category.pending_duration)
            })
            .saturating_add(self.terminal_duration.unwrap_or(Duration::ZERO))
    }

    /// Returns the longest completed or terminally closed pending interval.
    pub(crate) fn max_pending_duration(&self) -> Duration {
        self.categories
            .iter()
            .fold(Duration::ZERO, |longest, category| {
                longest.max(category.max_pending_duration)
            })
            .max(self.terminal_duration.unwrap_or(Duration::ZERO))
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
    /// The first wake was observed and the scheduler has not started the next poll.
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
    /// Repeated calls before a wake and the next poll are ignored.
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
                    wait = ?elapsed,
                    "pending transfer woke",
                );
            }
        }
    }

    /// Records that the scheduler is beginning the next poll.
    ///
    /// An open interval is closed at this boundary and contributes its
    /// pending-to-wake, scheduler-delay, and pending-duration measurements.
    /// Calls without an open interval avoid the lock and clock.
    pub(crate) fn record_poll_started(&self, id: TransferId) {
        if !self.interval_open.load(Ordering::Acquire) {
            return;
        }

        if let Some(interval) = self.record_poll_started_at(Instant::now()) {
            if self.emit_events {
                if let (Some(wait), Some(scheduler_delay)) =
                    (interval.pending_to_wake, interval.scheduler_delay)
                {
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        tid = %id,
                        category = interval.cause.category.as_str(),
                        reason = interval.cause.reason,
                        wait = ?wait,
                        scheduler_delay = ?scheduler_delay,
                        pending_time = ?interval.pending_duration,
                        "pending transfer poll started",
                    );
                } else {
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        tid = %id,
                        category = interval.cause.category.as_str(),
                        reason = interval.cause.reason,
                        wake_observed = false,
                        pending_time = ?interval.pending_duration,
                        "pending transfer poll started",
                    );
                }
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
                    pending_time = ?elapsed,
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

    fn record_poll_started_at(&self, now: Instant) -> Option<PendingInterval> {
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
                scheduler_delay: None,
                pending_duration: now.saturating_duration_since(pending_at),
            },
            PendingIntervalState::Woken {
                cause,
                pending_at,
                woken_at,
            } => PendingInterval {
                cause,
                pending_to_wake: Some(woken_at.saturating_duration_since(pending_at)),
                scheduler_delay: Some(now.saturating_duration_since(woken_at)),
                pending_duration: now.saturating_duration_since(pending_at),
            },
        };

        let category = &mut data.stats.categories[interval.cause.category.index()];
        category.pending_duration = category
            .pending_duration
            .saturating_add(interval.pending_duration);
        category.max_pending_duration =
            category.max_pending_duration.max(interval.pending_duration);
        if let Some(elapsed) = interval.scheduler_delay {
            category.scheduler_delay = category.scheduler_delay.saturating_add(elapsed);
            category.max_scheduler_delay = category.max_scheduler_delay.max(elapsed);
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
        let elapsed = now.saturating_duration_since(pending_at);
        data.stats.terminal_cause = Some(cause);
        data.stats.terminal_duration = Some(elapsed);
        Some((cause, elapsed))
    }

    /// Returns a copy of the accumulated statistics.
    pub(crate) fn snapshot(&self) -> TransferPendingStats {
        self.data.lock().stats
    }
}

/// Emits detail for pending categories entered by one transfer.
pub(crate) fn emit_pending_details(id: TransferId, stats: &TransferPendingStats) {
    for category in PendingCategory::ALL {
        let category_stats = stats.category(category);
        if category_stats.count == 0 {
            continue;
        }
        let terminal_duration = stats
            .terminal_cause
            .filter(|cause| cause.category == category)
            .and(stats.terminal_duration);
        let terminal_reason = stats
            .terminal_cause
            .filter(|cause| cause.category == category)
            .map(|cause| cause.reason);
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %id,
            category = category.as_str(),
            intervals = category_stats.count,
            wait_to_wake_sum = ?category_stats.pending_to_wake,
            wait_to_wake_max = ?category_stats.max_pending_to_wake,
            scheduler_delay_sum = ?category_stats.scheduler_delay,
            scheduler_delay_max = ?category_stats.max_scheduler_delay,
            pending_time_sum = ?category_stats.pending_duration,
            pending_time_max = ?category_stats.max_pending_duration,
            terminal_time = ?terminal_duration,
            terminal_reason,
            "transfer pending detail",
        );
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingInterval {
    cause: PendingCause,
    pending_to_wake: Option<Duration>,
    scheduler_delay: Option<Duration>,
    pending_duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cause(category: PendingCategory) -> PendingCause {
        PendingCause::new(category, "test")
    }

    #[test]
    fn pending_wake_and_next_poll_record_one_interval() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();
        let wake = start + Duration::from_millis(7);
        let next_poll = wake + Duration::from_millis(3);

        assert!(pending.record_pending_at(cause(PendingCategory::Memory), start));
        assert_eq!(
            pending.record_wake_at(wake),
            Some((cause(PendingCategory::Memory), Duration::from_millis(7)))
        );
        let interval = pending
            .record_poll_started_at(next_poll)
            .expect("active interval");
        assert_eq!(interval.pending_duration, Duration::from_millis(10));

        let stats = pending.snapshot().category(PendingCategory::Memory);
        assert_eq!(stats.count, 1);
        assert_eq!(stats.pending_to_wake, Duration::from_millis(7));
        assert_eq!(stats.max_pending_to_wake, Duration::from_millis(7));
        assert_eq!(stats.scheduler_delay, Duration::from_millis(3));
        assert_eq!(stats.max_scheduler_delay, Duration::from_millis(3));
        assert_eq!(stats.pending_duration, Duration::from_millis(10));
        assert_eq!(stats.max_pending_duration, Duration::from_millis(10));
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
            .record_poll_started_at(start + Duration::from_millis(10))
            .expect("active interval");

        let stats = pending.snapshot().category(PendingCategory::Source);
        assert_eq!(stats.pending_to_wake, Duration::from_millis(2));
        assert_eq!(stats.scheduler_delay, Duration::from_millis(8));
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
            .record_poll_started_at(start + Duration::from_millis(3))
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
        let stats = pending.snapshot();
        assert_eq!(stats.terminal_cause, Some(cause));
        assert_eq!(stats.terminal_duration, Some(Duration::from_millis(5)));
        assert_eq!(stats.interval_count(), 1);
        assert_eq!(stats.pending_duration(), Duration::from_millis(5));
        assert_eq!(stats.max_pending_duration(), Duration::from_millis(5));
    }

    #[test]
    fn aggregate_pending_time_includes_polled_and_terminal_intervals() {
        let pending = TransferPendingState::new(false);
        let start = Instant::now();

        assert!(pending.record_pending_at(cause(PendingCategory::Memory), start));
        pending
            .record_wake_at(start + Duration::from_millis(2))
            .expect("memory wake");
        pending
            .record_poll_started_at(start + Duration::from_millis(5))
            .expect("memory poll");

        let source_start = start + Duration::from_millis(10);
        assert!(pending.record_pending_at(cause(PendingCategory::Source), source_start));
        pending
            .record_terminal_at(source_start + Duration::from_millis(7))
            .expect("source terminal");

        let stats = pending.snapshot();
        assert_eq!(stats.interval_count(), 2);
        assert_eq!(stats.pending_duration(), Duration::from_millis(12));
        assert_eq!(stats.max_pending_duration(), Duration::from_millis(7));
    }

    #[test]
    fn idle_polls_do_not_create_intervals() {
        let pending = TransferPendingState::new(false);
        assert!(pending.record_poll_started_at(Instant::now()).is_none());
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
            pending.record_poll_started_at(now);

            let stats = pending.snapshot().category(PendingCategory::Memory);
            assert_eq!(stats.count, 1);
            assert_eq!(stats.pending_to_wake, Duration::ZERO);
        });
    }
}
