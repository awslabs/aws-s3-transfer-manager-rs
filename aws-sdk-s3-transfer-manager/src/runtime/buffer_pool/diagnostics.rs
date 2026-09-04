/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Periodic diagnostic reporting for one buffer-pool domain.
//!
//! Configuration is resolved before pool construction. This module owns only
//! snapshot collection, interval deltas, and tracing output. The maintenance
//! worker supplies the reporting deadline.

use std::time::{Duration, Instant};

use super::admission::ReservationQueueDiagnosticSample;
use super::arena::ArenaDiagnosticSnapshot;
use super::maintenance::MaintenanceDiagnosticSnapshot;
use super::metrics::MemoryMetrics;
use super::PoolInner;

/// Periodic reporting state owned exclusively by the maintenance worker.
pub(super) struct PeriodicDiagnosticReporter {
    interval: Duration,
    next_snapshot: Instant,
    previous: PoolDiagnosticSnapshot,
}

impl PeriodicDiagnosticReporter {
    /// Captures the baseline and arms the first complete interval.
    pub(super) fn new(pool: &PoolInner, interval: Duration) -> Option<Self> {
        let previous = PoolDiagnosticSnapshot::capture(pool);
        let Some(next_snapshot) = next_snapshot_after(previous.captured_at, interval) else {
            warn_unrepresentable_deadline(interval);
            return None;
        };
        Some(Self {
            interval,
            next_snapshot,
            previous,
        })
    }

    /// Returns the next reporting deadline.
    pub(super) fn next_snapshot(&self) -> Instant {
        self.next_snapshot
    }

    /// Emits one current-state sample and advances the delta baseline.
    pub(super) fn emit(&mut self, pool: &PoolInner) {
        let current = PoolDiagnosticSnapshot::capture(pool);
        emit_snapshot(&current, &self.previous, self.interval);
        self.previous = current;
    }

    /// Schedules the next complete interval from reporting completion.
    ///
    /// This deliberately skips missed ticks rather than emitting a burst
    /// of catch-up snapshots. `false` disables reporting when the clock
    /// cannot represent the next deadline.
    pub(super) fn schedule_next(&mut self, completed_at: Instant) -> bool {
        let Some(next_snapshot) = next_snapshot_after(completed_at, self.interval) else {
            warn_unrepresentable_deadline(self.interval);
            return false;
        };
        self.next_snapshot = next_snapshot;
        true
    }
}

/// Reports a deadline that cannot be represented by the monotonic clock.
fn warn_unrepresentable_deadline(interval: Duration) {
    tracing::warn!(
        target: crate::telemetry::TARGET_MEMORY,
        configured_interval_ms = interval.as_millis(),
        "disabled buffer-pool diagnostic snapshots with an unrepresentable deadline"
    );
}

/// Adds one configured interval, returning `None` outside the clock range.
fn next_snapshot_after(now: Instant, interval: Duration) -> Option<Instant> {
    now.checked_add(interval)
}

/// Accounting and counter sample captured outside allocator-exclusive work.
struct PoolDiagnosticSnapshot {
    captured_at: Instant,
    memory: MemoryMetrics,
    queue: ReservationQueueDiagnosticSample,
    arena: ArenaDiagnosticSnapshot,
    maintenance: MaintenanceDiagnosticSnapshot,
}

impl PoolDiagnosticSnapshot {
    /// Captures independent subsystem samples without nesting their locks.
    fn capture(pool: &PoolInner) -> Self {
        let captured_at = Instant::now();
        let (memory, queue) = pool.diagnostic_memory_sample(captured_at);
        let arena = pool.arena.diagnostics();
        let maintenance = pool.maintenance.diagnostics();
        Self {
            captured_at,
            memory,
            queue,
            arena,
            maintenance,
        }
    }
}

/// Counter changes during one diagnostic interval.
struct PoolDiagnosticDelta {
    reservation_enqueues: u64,
    optimistic_scans: u64,
    optimistic_scan_words: u64,
    optimistic_misses: u64,
    serialized_fallbacks: u64,
    serialized_slots_inspected: u64,
    blocks_prepared: u64,
    block_ranges_reserved: u64,
    rolled_back_carriers: u64,
    trim_slots_scanned: u64,
    blocks_reclaimed: u64,
    cleanup_retries: u64,
    cleanup_failures: u64,
    idle_deadlines: u64,
    reclaim_passes: u64,
    reclaim_retries: u64,
    cleanup_requests: u64,
}

impl PoolDiagnosticDelta {
    /// Computes saturating changes between consecutive cumulative samples.
    fn between(current: &PoolDiagnosticSnapshot, previous: &PoolDiagnosticSnapshot) -> Self {
        let (optimistic_scans, optimistic_scan_words) =
            match (current.arena.detailed, previous.arena.detailed) {
                (Some(current), Some(previous)) => (
                    counter_delta(current.optimistic_scans, previous.optimistic_scans),
                    counter_delta(
                        current.optimistic_scan_words,
                        previous.optimistic_scan_words,
                    ),
                ),
                _ => (0, 0),
            };

        Self {
            reservation_enqueues: counter_delta(
                current.memory.reservation_enqueues_total(),
                previous.memory.reservation_enqueues_total(),
            ),
            optimistic_scans,
            optimistic_scan_words,
            optimistic_misses: counter_delta(
                current.arena.optimistic_misses,
                previous.arena.optimistic_misses,
            ),
            serialized_fallbacks: counter_delta(
                current.arena.serialized_fallbacks,
                previous.arena.serialized_fallbacks,
            ),
            serialized_slots_inspected: counter_delta(
                current.arena.serialized_slots_inspected,
                previous.arena.serialized_slots_inspected,
            ),
            blocks_prepared: counter_delta(
                current.arena.blocks_prepared,
                previous.arena.blocks_prepared,
            ),
            block_ranges_reserved: counter_delta(
                current.arena.block_ranges_reserved,
                previous.arena.block_ranges_reserved,
            ),
            rolled_back_carriers: counter_delta(
                current.arena.rolled_back_carriers,
                previous.arena.rolled_back_carriers,
            ),
            trim_slots_scanned: counter_delta(
                current.arena.trim_slots_scanned,
                previous.arena.trim_slots_scanned,
            ),
            blocks_reclaimed: counter_delta(
                current.arena.blocks_reclaimed,
                previous.arena.blocks_reclaimed,
            ),
            cleanup_retries: counter_delta(
                current.arena.cleanup_retries,
                previous.arena.cleanup_retries,
            ),
            cleanup_failures: counter_delta(
                current.arena.cleanup_failures,
                previous.arena.cleanup_failures,
            ),
            idle_deadlines: counter_delta(
                current.maintenance.idle_deadlines,
                previous.maintenance.idle_deadlines,
            ),
            reclaim_passes: counter_delta(
                current.maintenance.reclaim_passes,
                previous.maintenance.reclaim_passes,
            ),
            reclaim_retries: counter_delta(
                current.maintenance.reclaim_retries,
                previous.maintenance.reclaim_retries,
            ),
            cleanup_requests: counter_delta(
                current.maintenance.cleanup_requests,
                previous.maintenance.cleanup_requests,
            ),
        }
    }
}

/// Returns a saturating cumulative-counter change.
fn counter_delta(current: u64, previous: u64) -> u64 {
    current.saturating_sub(previous)
}

/// Emits one current-state sample and interval counter changes.
fn emit_snapshot(
    current: &PoolDiagnosticSnapshot,
    previous: &PoolDiagnosticSnapshot,
    interval: Duration,
) {
    let delta = PoolDiagnosticDelta::between(current, previous);
    let elapsed = current
        .captured_at
        .saturating_duration_since(previous.captured_at);
    let detailed_allocator_enabled = current.arena.detailed.is_some();
    let (optimistic_scans, optimistic_scan_words) = current
        .arena
        .detailed
        .map(|detailed| (detailed.optimistic_scans, detailed.optimistic_scan_words))
        .unwrap_or((0, 0));

    tracing::debug!(
        target: crate::telemetry::TARGET_MEMORY,
        configured_interval_ms = interval.as_millis(),
        elapsed_ms = elapsed.as_millis(),
        configured_capacity_bytes = current.memory.configured_capacity_bytes(),
        admission_used_bytes = current.memory.admission_used_bytes(),
        admission_overage_bytes = current.memory.admission_overage_bytes(),
        active_planned_demand_bytes = current.memory.active_planned_demand_bytes(),
        charged_capacity_bytes = current.memory.charged_capacity_bytes(),
        prepared_capacity_bytes = current.memory.prepared_capacity_bytes(),
        queued_reservations = current.memory.queued_reservations(),
        queued_reservations_peak = current.queue.peak_depth,
        reservation_enqueues_total = current.memory.reservation_enqueues_total(),
        reservation_enqueues_delta = delta.reservation_enqueues,
        reservation_queue_nonempty = current.queue.current_depth != 0,
        reservation_queue_nonempty_duration_ms =
            current.queue.nonempty_duration.as_millis(),
        reservation_queue_continuous_nonempty_ms = current
            .queue
            .continuous_nonempty_duration
            .map(|duration| duration.as_millis())
            .unwrap_or(0),
        detailed_allocator_enabled,
        optimistic_scans_total = optimistic_scans,
        optimistic_scans_delta = delta.optimistic_scans,
        optimistic_scan_words_total = optimistic_scan_words,
        optimistic_scan_words_delta = delta.optimistic_scan_words,
        optimistic_misses_total = current.arena.optimistic_misses,
        optimistic_misses_delta = delta.optimistic_misses,
        serialized_fallbacks_total = current.arena.serialized_fallbacks,
        serialized_fallbacks_delta = delta.serialized_fallbacks,
        serialized_slots_inspected_total = current.arena.serialized_slots_inspected,
        serialized_slots_inspected_delta = delta.serialized_slots_inspected,
        blocks_prepared_total = current.arena.blocks_prepared,
        blocks_prepared_delta = delta.blocks_prepared,
        block_ranges_reserved_total = current.arena.block_ranges_reserved,
        block_ranges_reserved_delta = delta.block_ranges_reserved,
        rolled_back_carriers_total = current.arena.rolled_back_carriers,
        rolled_back_carriers_delta = delta.rolled_back_carriers,
        trim_slots_scanned_total = current.arena.trim_slots_scanned,
        trim_slots_scanned_delta = delta.trim_slots_scanned,
        blocks_reclaimed_total = current.arena.blocks_reclaimed,
        blocks_reclaimed_delta = delta.blocks_reclaimed,
        cleanup_retries_total = current.arena.cleanup_retries,
        cleanup_retries_delta = delta.cleanup_retries,
        cleanup_failures_total = current.arena.cleanup_failures,
        cleanup_failures_delta = delta.cleanup_failures,
        idle_deadlines_total = current.maintenance.idle_deadlines,
        idle_deadlines_delta = delta.idle_deadlines,
        reclaim_passes_total = current.maintenance.reclaim_passes,
        reclaim_passes_delta = delta.reclaim_passes,
        reclaim_retries_total = current.maintenance.reclaim_retries,
        reclaim_retries_delta = delta.reclaim_retries,
        cleanup_requests_total = current.maintenance.cleanup_requests,
        cleanup_requests_delta = delta.cleanup_requests,
        maintenance_disabled = current.maintenance.maintenance_disabled,
        "buffer-pool diagnostic snapshot"
    );
}

#[cfg(test)]
mod tests {
    use futures_test::task::new_count_waker;

    use super::super::test_util::{poll_reserve, test_pool, test_pool_with_scan_and_diagnostics};
    use super::*;
    use crate::config::MemoryDiagnosticsConfig;

    #[test]
    fn default_pool_omits_detailed_scans_but_keeps_pressure_counters() {
        let (pool, carrier_size) =
            test_pool_with_scan_and_diagnostics(1, 3, 1, MemoryDiagnosticsConfig::default());
        let reservation = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("reservation");
        let acquired = pool.acquire(&reservation, carrier_size * 3).unwrap();

        let diagnostics = pool.diagnostics();
        assert_eq!(diagnostics.detailed_allocator, None);
        assert!(diagnostics.optimistic_misses > 0);
        assert!(diagnostics.serialized_fallbacks > 0);
        drop(acquired);
    }

    #[test]
    fn baseline_reporter_does_not_require_detailed_counters() {
        let (pool, _) =
            test_pool_with_scan_and_diagnostics(1, 1, 1, MemoryDiagnosticsConfig::default());
        let mut reporter = PeriodicDiagnosticReporter::new(&pool.inner, Duration::from_secs(1))
            .expect("baseline diagnostic reporter");

        reporter.emit(&pool.inner);
        assert!(reporter.previous.arena.detailed.is_none());
    }

    #[test]
    fn reporter_arms_one_complete_interval_from_its_baseline() {
        let (pool, _) = test_pool(1, 1);
        let interval = Duration::from_secs(1);
        let reporter =
            PeriodicDiagnosticReporter::new(&pool.inner, interval).expect("diagnostic reporter");

        assert_eq!(
            reporter
                .next_snapshot()
                .saturating_duration_since(reporter.previous.captured_at),
            interval
        );
    }

    #[test]
    fn queue_snapshots_follow_enqueue_cancel_and_grant() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool.try_reserve(carrier_size).unwrap().unwrap();
        let (waker, _) = new_count_waker();
        let mut cancelled = pool.reserve(carrier_size);
        let mut granted = pool.reserve(carrier_size);
        assert!(poll_reserve(&mut cancelled, &waker).is_pending());
        assert!(poll_reserve(&mut granted, &waker).is_pending());

        let (_, queued) = pool.inner.diagnostic_memory_sample(Instant::now());
        assert_eq!(queued.current_depth, 2);
        assert_eq!(queued.peak_depth, 2);
        assert!(queued.continuous_nonempty_duration.is_some());

        drop(cancelled);
        drop(holder);

        let (_, cleared) = pool.inner.diagnostic_memory_sample(Instant::now());
        assert_eq!(cleared.current_depth, 0);
        assert_eq!(cleared.peak_depth, 2);
        assert_eq!(cleared.continuous_nonempty_duration, None);
        drop(granted);
    }

    #[test]
    fn cumulative_counter_delta_never_wraps() {
        assert_eq!(counter_delta(12, 5), 7);
        assert_eq!(counter_delta(5, 12), 0);
    }
}
