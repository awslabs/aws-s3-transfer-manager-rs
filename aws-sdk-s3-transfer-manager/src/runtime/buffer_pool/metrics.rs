/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Memory state and operational diagnostics for one pool.
//!
//! [`MemoryMetrics`] is one admission-serialized accounting sample suitable
//! for capacity decisions. [`MemoryDiagnostics`] combines cumulative counters
//! with a best-effort arena scan for tests and diagnostic explanation.

use super::arena::{ArenaDiagnosticSnapshot, ArenaLifecycleSample};
use super::maintenance::MaintenanceDiagnosticSnapshot;
use super::CarrierCount;

/// Point-in-time memory state for one shared pool domain.
///
/// All values come from one coherent sample. Byte values may be rounded to the
/// pool's allocation unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryMetrics {
    capacity_bytes: u64,
    admitted_bytes: u64,
    active_planned_demand_bytes: u64,
    in_use_bytes: u64,
    admission_overage_bytes: u64,
    prepared_capacity_bytes: u64,
    waiting_requests: usize,
    waits_total: u64,
}

/// Carrier-count inputs captured under admission serialization.
pub(super) struct MemoryMetricState {
    /// Normal admission ceiling.
    pub(super) configured_capacity: CarrierCount,
    /// Active planned demand plus uncovered charges.
    pub(super) admission_used: CarrierCount,
    /// Complete open envelopes.
    pub(super) active_planned_demand: CarrierCount,
    /// Aggregate owner and in-flight acquisition charges.
    pub(super) charged_capacity: CarrierCount,
    /// Capacity whose physical preparation completed.
    pub(super) prepared_capacity: CarrierCount,
    /// Requests currently retained in the FIFO.
    pub(super) queued_reservations: usize,
    /// Cumulative requests that entered the FIFO.
    pub(super) reservation_enqueues_total: u64,
}

/// Operational counters and lifecycle state for one shared pool domain.
///
/// This crate-private snapshot is intentionally separate from stable memory
/// metrics. It may scan block state and is intended for diagnostics, tests,
/// and rare tracing rather than allocator decisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryDiagnostics {
    /// Carrier bitmap bits owned by provisional claims or live guards.
    ///
    /// Concurrent claims and returns may change this value during the arena
    /// scan. It is exact when the pool is externally quiescent.
    pub(crate) live_carriers: usize,
    /// Claimable carriers backed by prepared active block incarnations.
    pub(crate) prepared_carriers: usize,
    /// Blocks unavailable until trim or mapping recovery completes.
    pub(crate) cleanup_pending_blocks: usize,
    /// Per-acquisition allocator work, when detailed diagnostics were enabled.
    pub(crate) detailed_allocator: Option<DetailedAllocatorDiagnostics>,
    /// Cumulative optimistic acquisitions that needed more carriers.
    pub(crate) optimistic_misses: u64,
    /// Cumulative partial claims completed through serialized fallback.
    pub(crate) serialized_fallbacks: u64,
    /// Cumulative existing slots inspected by serialized fallback.
    pub(crate) serialized_slots_inspected: u64,
    /// Cumulative blocks made claimable.
    pub(crate) blocks_prepared: u64,
    /// Cumulative stable virtual ranges reserved for new block slots.
    pub(crate) block_ranges_reserved: u64,
    /// Cumulative provisional carriers rolled back before ownership transfer.
    pub(crate) rolled_back_carriers: u64,
    /// Cumulative slots inspected by trim selection.
    pub(crate) trim_slots_scanned: u64,
    /// Cumulative blocks removed from prepared capacity.
    pub(crate) blocks_reclaimed: u64,
    /// Cumulative pending mapping transitions retried.
    pub(crate) cleanup_retries: u64,
    /// Cumulative cleanup retries that still failed.
    pub(crate) cleanup_failures: u64,
    /// Cumulative scheduler-global idle intervals that armed a deadline.
    pub(crate) idle_deadlines: u64,
    /// Whether worker creation failed and permanently disabled maintenance.
    pub(crate) maintenance_disabled: bool,
    /// Cumulative reclaim passes executed by maintenance.
    pub(crate) reclaim_passes: u64,
    /// Cumulative reclaim passes that remained blocked.
    pub(crate) reclaim_retries: u64,
    /// Cumulative cleanup generations requested after mapping failures.
    pub(crate) cleanup_requests: u64,
}

/// Per-acquisition allocator work collected only in detailed diagnostic mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetailedAllocatorDiagnostics {
    /// Cumulative optimistic acquisition attempts.
    pub(crate) optimistic_scans: u64,
    /// Cumulative bitmap words inspected by optimistic acquisition.
    pub(crate) optimistic_scan_words: u64,
}

impl MemoryDiagnostics {
    /// Composes independent arena and maintenance diagnostic samples.
    pub(super) fn from_samples(
        arena: ArenaDiagnosticSnapshot,
        lifecycle: ArenaLifecycleSample,
        maintenance: MaintenanceDiagnosticSnapshot,
    ) -> Self {
        Self {
            live_carriers: lifecycle.live_carriers.get(),
            prepared_carriers: lifecycle.prepared_capacity.get(),
            cleanup_pending_blocks: lifecycle.cleanup_pending_blocks,
            detailed_allocator: arena.detailed.map(|detailed| DetailedAllocatorDiagnostics {
                optimistic_scans: detailed.optimistic_scans,
                optimistic_scan_words: detailed.optimistic_scan_words,
            }),
            optimistic_misses: arena.optimistic_misses,
            serialized_fallbacks: arena.serialized_fallbacks,
            serialized_slots_inspected: arena.serialized_slots_inspected,
            blocks_prepared: arena.blocks_prepared,
            block_ranges_reserved: arena.block_ranges_reserved,
            rolled_back_carriers: arena.rolled_back_carriers,
            trim_slots_scanned: arena.trim_slots_scanned,
            blocks_reclaimed: arena.blocks_reclaimed,
            cleanup_retries: arena.cleanup_retries,
            cleanup_failures: arena.cleanup_failures,
            idle_deadlines: maintenance.idle_deadlines,
            maintenance_disabled: maintenance.maintenance_disabled,
            reclaim_passes: maintenance.reclaim_passes,
            reclaim_retries: maintenance.reclaim_retries,
            cleanup_requests: maintenance.cleanup_requests,
        }
    }
}

impl MemoryMetrics {
    /// Constructs one sample from validated carrier counts.
    pub(super) fn from_carriers(carrier_size: usize, state: MemoryMetricState) -> Self {
        let capacity_bytes = carriers_to_bytes(state.configured_capacity, carrier_size);
        let admitted_bytes = carriers_to_bytes(state.admission_used, carrier_size);
        Self {
            capacity_bytes,
            admitted_bytes,
            active_planned_demand_bytes: carriers_to_bytes(
                state.active_planned_demand,
                carrier_size,
            ),
            in_use_bytes: carriers_to_bytes(state.charged_capacity, carrier_size),
            admission_overage_bytes: admitted_bytes.saturating_sub(capacity_bytes),
            prepared_capacity_bytes: carriers_to_bytes(state.prepared_capacity, carrier_size),
            waiting_requests: state.queued_reservations,
            waits_total: state.reservation_enqueues_total,
        }
    }

    /// Returns the pool's admission ceiling.
    ///
    /// This is the configured memory budget, rounded down to the pool's
    /// allocation unit. Requests wait while admitting them would exceed it.
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns memory currently admitted against [`Self::capacity_bytes`].
    ///
    /// Admitted memory includes memory in use and memory admitted for requests
    /// that have not yet filled it, so requests can wait at the ceiling while
    /// [`Self::in_use_bytes`] is lower.
    pub fn admitted_bytes(&self) -> u64 {
        self.admitted_bytes
    }

    /// Returns memory held by live buffers and the immutable bytes built from
    /// them.
    ///
    /// Memory remains in use until the last value referring to it is dropped.
    pub fn in_use_bytes(&self) -> u64 {
        self.in_use_bytes
    }

    /// Returns memory the pool holds ready for use, whether in use or kept for
    /// reuse.
    ///
    /// The value may exceed [`Self::capacity_bytes`]. `None` means the pool
    /// cannot tell; this pool always returns `Some`.
    pub fn committed_bytes(&self) -> Option<u64> {
        Some(self.prepared_capacity_bytes)
    }

    /// Returns requests currently waiting for admission.
    pub fn waiting_requests(&self) -> usize {
        self.waiting_requests
    }

    /// Returns how many requests have waited for admission.
    ///
    /// This pool-lifetime counter increments once per request that waits and
    /// saturates at `u64::MAX`. Unlike [`Self::waiting_requests`], it does not
    /// decrease when a request is admitted or cancelled.
    pub fn waits_total(&self) -> u64 {
        self.waits_total
    }

    /// Returns complete envelopes whose acquisition authority remains open.
    pub(crate) fn active_planned_demand_bytes(&self) -> u64 {
        self.active_planned_demand_bytes
    }

    /// Returns admission use above the normal configured ceiling.
    pub(crate) fn admission_overage_bytes(&self) -> u64 {
        self.admission_overage_bytes
    }

    /// Returns capacity whose physical preparation completed.
    ///
    /// Preparation rounds its current floor up to whole blocks and may exceed
    /// that floor by less than one block. Idle-only admission overage may also
    /// place prepared capacity above the normal configured ceiling.
    pub(crate) fn prepared_capacity_bytes(&self) -> u64 {
        self.prepared_capacity_bytes
    }
}

/// Converts a validated carrier count to its public byte representation.
fn carriers_to_bytes(count: CarrierCount, carrier_size: usize) -> u64 {
    let count = u64::try_from(count.get()).expect("carrier count must fit metric representation");
    let carrier_size =
        u64::try_from(carrier_size).expect("carrier size must fit metric representation");
    count
        .checked_mul(carrier_size)
        .expect("carrier capacity must fit metric representation")
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use futures_test::task::new_count_waker;

    use super::super::test_util::{poll_reserve, test_pool};

    #[test]
    fn test_metrics_report_admission_ownership_and_queue_state() {
        let (pool, carrier_size) = test_pool(1, 1);
        let initial = pool.metrics();
        assert_eq!(initial.capacity_bytes(), carrier_size as u64);
        assert_eq!(initial.admitted_bytes(), 0);
        assert_eq!(initial.active_planned_demand_bytes(), 0);
        assert_eq!(initial.in_use_bytes(), 0);
        assert_eq!(initial.admission_overage_bytes(), 0);
        assert_eq!(initial.prepared_capacity_bytes(), 0);
        assert_eq!(initial.waiting_requests(), 0);
        assert_eq!(initial.waits_total(), 0);

        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let (waker, _) = new_count_waker();
        let mut queued = pool.reserve(carrier_size);
        assert!(poll_reserve(&mut queued, &waker).is_pending());

        let queued_metrics = pool.metrics();
        assert_eq!(queued_metrics.admitted_bytes(), carrier_size as u64);
        assert_eq!(
            queued_metrics.active_planned_demand_bytes(),
            carrier_size as u64
        );
        assert_eq!(queued_metrics.in_use_bytes(), 0);
        assert_eq!(
            queued_metrics.prepared_capacity_bytes(),
            carrier_size as u64
        );
        assert_eq!(queued_metrics.waiting_requests(), 1);
        assert_eq!(queued_metrics.waits_total(), 1);

        drop(queued);
        let acquired = pool.acquire(&reservation, carrier_size).unwrap();
        let charged = pool.metrics();
        assert_eq!(charged.in_use_bytes(), carrier_size as u64);
        assert_eq!(charged.waiting_requests(), 0);
        assert_eq!(charged.waits_total(), 1);

        reservation.close_acquisition();
        let closed = pool.metrics();
        assert_eq!(closed.active_planned_demand_bytes(), 0);
        assert_eq!(closed.admitted_bytes(), carrier_size as u64);
        assert_eq!(closed.in_use_bytes(), carrier_size as u64);
        drop(acquired);
    }

    #[test]
    fn test_metrics_report_unreserved_overage_without_exposing_ledger_fields() {
        let (pool, carrier_size) = test_pool(2, 1);

        let acquired = pool.acquire_unreserved(carrier_size * 2).unwrap();
        let metrics = pool.metrics();

        assert_eq!(metrics.capacity_bytes(), carrier_size as u64);
        assert_eq!(metrics.admitted_bytes(), (carrier_size * 2) as u64);
        assert_eq!(metrics.active_planned_demand_bytes(), 0);
        assert_eq!(metrics.in_use_bytes(), (carrier_size * 2) as u64);
        assert_eq!(metrics.admission_overage_bytes(), carrier_size as u64);
        assert_eq!(metrics.prepared_capacity_bytes(), (carrier_size * 2) as u64);
        drop(acquired);
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::test_util::test_single_carrier_pool;
    use crate::runtime::sync::thread;

    #[test]
    fn test_metrics_sample_close_and_return_as_complete_states() {
        loom::model(|| {
            let (pool, carrier_size) = test_single_carrier_pool(1);
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let acquired = pool.acquire(&reservation, carrier_size).unwrap();

            let closing = thread::spawn(move || reservation.close_acquisition());
            let returning = thread::spawn(move || drop(acquired));
            let sample = pool.metrics();
            closing.join().unwrap();
            returning.join().unwrap();

            let carrier_size = carrier_size as u64;
            assert_eq!(sample.capacity_bytes(), carrier_size);
            assert!([0, carrier_size].contains(&sample.admitted_bytes()));
            assert!([0, carrier_size].contains(&sample.active_planned_demand_bytes()));
            assert!([0, carrier_size].contains(&sample.in_use_bytes()));
            assert!(sample.in_use_bytes() <= sample.admitted_bytes());
            assert_eq!(sample.admission_overage_bytes(), 0);
            assert_eq!(sample.prepared_capacity_bytes(), carrier_size);

            let final_sample = pool.metrics();
            assert_eq!(final_sample.admitted_bytes(), 0);
            assert_eq!(final_sample.active_planned_demand_bytes(), 0);
            assert_eq!(final_sample.in_use_bytes(), 0);
        });
    }
}
