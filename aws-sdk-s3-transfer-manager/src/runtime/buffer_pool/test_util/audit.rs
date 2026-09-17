/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Global accounting and ownership reconciliation for tests and fuzzing.

use super::super::{BufferPool, CarrierCount};

/// Result of reconciling externally stabilized pool state.
///
/// The pool may retain live reservations and owners. Quiescence means only
/// that no operation mutates the state while the audit reconstructs it.
#[derive(Debug, Eq, PartialEq)]
pub(in crate::runtime::buffer_pool) struct PoolAuditReport {
    /// Complete open reservation envelopes.
    pub(in crate::runtime::buffer_pool) active_planned_demand: CarrierCount,
    /// Open-envelope capacity not occupied by an acquisition.
    pub(in crate::runtime::buffer_pool) available_coverage: CarrierCount,
    /// Charges outside open-envelope coverage.
    pub(in crate::runtime::buffer_pool) uncovered_charges: CarrierCount,
    /// Aggregate acquisition charges reconstructed from accounting.
    pub(in crate::runtime::buffer_pool) charged_capacity: CarrierCount,
    /// Prepared capacity serialized by admission.
    pub(in crate::runtime::buffer_pool) prepared_capacity: CarrierCount,
    /// Valid live bits reconstructed from block incarnations.
    pub(in crate::runtime::buffer_pool) live_carriers: CarrierCount,
    /// Reservation futures linked in FIFO order.
    pub(in crate::runtime::buffer_pool) queued_reservations: usize,
    /// Blocks unavailable while trim or mapping recovery remains pending.
    pub(in crate::runtime::buffer_pool) cleanup_pending_blocks: usize,
}

impl BufferPool {
    /// Reconstructs and validates all load-bearing pool accounting.
    ///
    /// No claim, return, reservation, or maintenance operation may overlap
    /// this audit. Holding admission stabilizes planned demand and prepared
    /// capacity; external quiescence stabilizes lock-free bitmap ownership.
    pub(in crate::runtime::buffer_pool) fn audit_quiescent(&self) -> PoolAuditReport {
        let (
            active_planned_demand,
            available_coverage,
            uncovered_charges,
            charged_capacity,
            prepared_capacity,
            queued_reservations,
            lifecycle,
        ) = {
            let admission = self.inner.admission.lock();
            let coverage = self.inner.coverage.snapshot();
            admission.ledger.assert_invariants(coverage);
            let lifecycle = self.inner.arena.sample_lifecycle();

            let covered_charges = admission
                .ledger
                .active_planned_demand
                .checked_sub(coverage.available)
                .expect("available coverage exceeds active planned demand");
            let charged_capacity = covered_charges
                .checked_add(coverage.uncovered)
                .expect("quiescent charged capacity overflowed");
            (
                admission.ledger.active_planned_demand,
                coverage.available,
                coverage.uncovered,
                charged_capacity,
                admission.ledger.prepared_capacity,
                admission.waiter_count(),
                lifecycle,
            )
        };

        assert_eq!(
            lifecycle.prepared_capacity, prepared_capacity,
            "prepared accounting disagrees with active block incarnations"
        );
        assert_eq!(
            lifecycle.live_carriers, charged_capacity,
            "aggregate charges disagree with live carrier bits"
        );

        PoolAuditReport {
            active_planned_demand,
            available_coverage,
            uncovered_charges,
            charged_capacity,
            prepared_capacity,
            live_carriers: lifecycle.live_carriers,
            queued_reservations,
            cleanup_pending_blocks: lifecycle.cleanup_pending_blocks,
        }
    }

    /// Reports whether a complete aligned carrier-word run is currently free.
    ///
    /// No claim, return, reservation, or maintenance operation may overlap
    /// this query. The result is used before a generated acquisition to assert
    /// the allocator's contiguous-first contract without duplicating its
    /// cursor or claim algorithm in the reference model.
    pub(in crate::runtime::buffer_pool) fn has_free_word_run_quiescent(
        &self,
        word_count: usize,
    ) -> bool {
        let _admission = self.inner.admission.lock();
        self.inner.arena.has_free_word_run_quiescent(word_count)
    }

    /// Runs the internal accounting and ownership reconciliation.
    #[cfg(test)]
    pub(crate) fn validate_quiescent_for_test(&self) {
        let _ = self.audit_quiescent();
    }

    /// Asserts that no accounting, physical ownership, or cleanup remains.
    #[cfg(test)]
    pub(in crate::runtime::buffer_pool) fn assert_quiescent_zero(&self) {
        assert_eq!(
            self.audit_quiescent(),
            PoolAuditReport {
                active_planned_demand: CarrierCount::ZERO,
                available_coverage: CarrierCount::ZERO,
                uncovered_charges: CarrierCount::ZERO,
                charged_capacity: CarrierCount::ZERO,
                prepared_capacity: CarrierCount::ZERO,
                live_carriers: CarrierCount::ZERO,
                queued_reservations: 0,
                cleanup_pending_blocks: 0,
            }
        );
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::super::super::admission::MAX_PACKED_CARRIERS;
    use super::super::super::maintenance::{
        execute_maintenance, MaintenanceAction, MaintenanceOutcome,
    };
    use super::super::super::virtual_memory::VirtualMemoryOperation;
    use super::super::{counting_waker, poll_reserve, test_pool};
    use super::*;

    #[test]
    fn test_audit_reconciles_nonzero_reservation_and_ownership() {
        let (pool, carrier_size) = test_pool(2, 8);
        let reservation = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("reservation");
        let owned = pool.acquire(&reservation, carrier_size * 2).unwrap();

        assert_eq!(
            pool.audit_quiescent(),
            PoolAuditReport {
                active_planned_demand: CarrierCount::new(3),
                available_coverage: CarrierCount::new(1),
                uncovered_charges: CarrierCount::ZERO,
                charged_capacity: CarrierCount::new(2),
                prepared_capacity: CarrierCount::new(4),
                live_carriers: CarrierCount::new(2),
                queued_reservations: 0,
                cleanup_pending_blocks: 0,
            }
        );

        drop(owned);
        drop(reservation);
    }

    #[test]
    fn test_audit_rejects_prepared_accounting_mismatch() {
        let (pool, carrier_size) = test_pool(2, 4);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        let prepared = pool.audit_quiescent().prepared_capacity;
        {
            let mut admission = pool.inner.admission.lock();
            admission.ledger.prepared_capacity = CarrierCount::new(1);
        }

        let result = catch_unwind(AssertUnwindSafe(|| pool.audit_quiescent()));

        pool.inner.admission.lock().ledger.prepared_capacity = prepared;
        assert!(
            result.is_err(),
            "audit accepted inconsistent prepared state"
        );
        drop(owned);
    }

    #[test]
    fn test_audit_rejects_live_ownership_mismatch() {
        let (pool, carrier_size) = test_pool(2, 4);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        let count = CarrierCount::new(1);
        let returned = pool.inner.coverage.release(count);
        assert_eq!(returned.uncovered_removed, count);

        let result = catch_unwind(AssertUnwindSafe(|| pool.audit_quiescent()));

        pool.inner
            .coverage
            .debit(count, MAX_PACKED_CARRIERS)
            .unwrap();
        assert!(
            result.is_err(),
            "audit accepted inconsistent ownership accounting"
        );
        drop(owned);
    }

    #[test]
    fn test_audit_reports_a_queued_reservation() {
        let (pool, carrier_size) = test_pool(1, 1);
        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let mut queued = pool.reserve(carrier_size);
        let (waker, _) = counting_waker();
        assert!(poll_reserve(&mut queued, &waker).is_pending());

        assert_eq!(
            pool.audit_quiescent(),
            PoolAuditReport {
                active_planned_demand: CarrierCount::new(1),
                available_coverage: CarrierCount::new(1),
                uncovered_charges: CarrierCount::ZERO,
                charged_capacity: CarrierCount::ZERO,
                prepared_capacity: CarrierCount::new(1),
                live_carriers: CarrierCount::ZERO,
                queued_reservations: 1,
                cleanup_pending_blocks: 0,
            }
        );

        drop(queued);
        drop(reservation);
    }

    #[test]
    fn test_placement_audit_distinguishes_partial_and_complete_word_runs() {
        /*
         * Two 128-carrier blocks begin completely free:
         *
         *     block 0            block 1
         *     [free][free]       [free][free]
         *
         * A one-carrier owner damages one word. A 128-carrier part consumes
         * the other block, leaving one complete word but no two-word run.
         */
        let (pool, carrier_size) = test_pool(128, 192);
        let reservation = pool
            .try_reserve(carrier_size * 192)
            .unwrap()
            .expect("placement-audit reservation");
        assert!(pool.has_free_word_run_quiescent(2));

        let small = pool.acquire(&reservation, carrier_size).unwrap();
        assert!(pool.has_free_word_run_quiescent(2));

        let part = pool.acquire(&reservation, carrier_size * 128).unwrap();
        assert!(pool.has_free_word_run_quiescent(1));
        assert!(!pool.has_free_word_run_quiescent(2));

        drop(part);
        assert!(pool.has_free_word_run_quiescent(2));
        drop(small);
        drop(reservation);
    }

    #[test]
    fn test_audit_reports_pending_mapping_cleanup() {
        let (pool, carrier_size) = test_pool(1, 1);
        let owned = pool.acquire_unreserved(carrier_size).unwrap();
        drop(owned);
        let slot = pool
            .inner
            .arena
            .select_trim_candidate()
            .expect("free prepared slot");
        slot.inject_failure_once(VirtualMemoryOperation::Deactivate);

        let pass = execute_maintenance(
            &pool.inner,
            MaintenanceAction::Reclaim {
                epoch: 0,
                target: CarrierCount::ZERO,
            },
        );
        assert_eq!(pass.outcome, MaintenanceOutcome::Complete);
        assert!(pass.cleanup_pending);
        assert_eq!(pool.audit_quiescent().cleanup_pending_blocks, 1);

        let retry = execute_maintenance(
            &pool.inner,
            MaintenanceAction::RetryCleanup { generation: 1 },
        );
        assert_eq!(retry.outcome, MaintenanceOutcome::Complete);
        pool.assert_quiescent_zero();
    }
}
