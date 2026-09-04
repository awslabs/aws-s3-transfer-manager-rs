/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared construction, polling, observation, and failure injection for tests.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc as StdArc;
use std::task::Wake;
use std::task::{Context, Poll, Waker};

use bytes::BufMut;

use crate::config::MemoryDiagnosticsConfig;
use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};
use crate::runtime::sync::thread;

use super::admission::{Reservation, ReserveError, ReserveFuture};
use super::geometry::PoolGeometry;
use super::virtual_memory::page_size;
use super::{BufferPool, CarrierCount, PoolInner, PooledBufMut};

/// Result of reconciling externally stabilized pool state.
///
/// The pool may retain live reservations and owners. Quiescence means only
/// that no operation mutates the state while the audit reconstructs it.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct PoolAuditReport {
    /// Complete open reservation envelopes.
    pub(super) active_planned_demand: CarrierCount,
    /// Open-envelope capacity not occupied by an acquisition.
    pub(super) available_coverage: CarrierCount,
    /// Charges outside open-envelope coverage.
    pub(super) uncovered_charges: CarrierCount,
    /// Aggregate acquisition charges reconstructed from accounting.
    pub(super) charged_capacity: CarrierCount,
    /// Prepared capacity serialized by admission.
    pub(super) prepared_capacity: CarrierCount,
    /// Valid live bits reconstructed from block incarnations.
    pub(super) live_carriers: CarrierCount,
    /// Reservation futures linked in FIFO order.
    pub(super) queued_reservations: usize,
    /// Blocks unavailable while trim or mapping recovery remains pending.
    pub(super) cleanup_pending_blocks: usize,
}

/// Waker state that records each wake through the active atomic backend.
struct CountingWake {
    count: Arc<AtomicUsize>,
}

impl Wake for CountingWake {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }
}

/// Per-pool hooks that keep parallel and Loom tests isolated.
pub(super) struct TestHooks {
    /// Acquisition calls that reached the physical allocator.
    acquisition_attempts: AtomicUsize,
    /// Remaining metadata boundaries before one acquisition failure.
    acquisition_allocation_failure: AtomicUsize,
    /// One terminal reservation failure returned before admission work.
    reservation_failure: Mutex<Option<ReserveError>>,
    /// Fails the next maintenance worker creation when set.
    maintenance_spawn_failure: AtomicBool,
    /// Number of maintenance worker creation attempts.
    maintenance_spawn_attempts: AtomicUsize,
    /// Optional one-shot pause after the worker upgrades its weak pool owner.
    maintenance_pause: Mutex<Option<Arc<MaintenancePause>>>,
}

impl TestHooks {
    /// Creates a pool with every test hook disabled.
    pub(super) fn new() -> Self {
        Self {
            acquisition_attempts: AtomicUsize::new(0),
            acquisition_allocation_failure: AtomicUsize::new(0),
            reservation_failure: Mutex::new(None),
            maintenance_spawn_failure: AtomicBool::new(false),
            maintenance_spawn_attempts: AtomicUsize::new(0),
            maintenance_pause: Mutex::new(None),
        }
    }

    /// Records one physical acquisition attempt.
    pub(super) fn record_acquisition_attempt(&self) {
        self.acquisition_attempts.fetch_add(1, Ordering::AcqRel);
    }

    /// Fails the `boundary`th subsequent acquisition metadata reservation.
    fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        assert!(boundary != 0, "failure boundary must be nonzero");
        self.acquisition_allocation_failure
            .compare_exchange(0, boundary, Ordering::AcqRel, Ordering::Acquire)
            .expect("an acquisition allocation failure is already pending");
    }

    /// Returns whether this metadata boundary consumes the injected failure.
    pub(super) fn take_acquisition_allocation_failure(&self) -> bool {
        self.acquisition_allocation_failure
            .fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |remaining| match remaining {
                    0 => None,
                    1 => Some(0),
                    remaining => Some(remaining - 1),
                },
            )
            .is_ok_and(|previous| previous == 1)
    }

    /// Installs one terminal reservation failure.
    fn inject_reservation_failure(&self, error: ReserveError) {
        let previous = self.reservation_failure.lock().replace(error);
        assert!(
            previous.is_none(),
            "a reservation failure is already pending"
        );
    }

    /// Consumes one terminal reservation failure.
    pub(super) fn take_reservation_failure(&self) -> Option<ReserveError> {
        self.reservation_failure.lock().take()
    }

    /// Records one serialized maintenance worker creation attempt.
    pub(super) fn record_maintenance_spawn_attempt(&self) {
        self.maintenance_spawn_attempts
            .fetch_add(1, Ordering::AcqRel);
    }

    /// Consumes one injected maintenance worker creation failure.
    pub(super) fn take_maintenance_spawn_failure(&self) -> bool {
        self.maintenance_spawn_failure.swap(false, Ordering::AcqRel)
    }

    /// Pauses once after the worker obtains temporary strong pool ownership.
    pub(super) fn wait_after_maintenance_upgrade(&self) {
        let pause = self.maintenance_pause.lock().take();
        if let Some(pause) = pause {
            pause.wait();
        }
    }
}

/// Test-controlled pause in the worker's temporary ownership window.
pub(super) struct MaintenancePause {
    /// Set after the worker upgrades its weak pool owner.
    entered: AtomicBool,
    /// Set by the test to permit maintenance execution.
    released: AtomicBool,
}

impl MaintenancePause {
    /// Creates a closed pause gate.
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
        }
    }

    /// Blocks the worker until the test releases the gate.
    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        while !self.released.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    /// Returns whether the worker entered the temporary ownership window.
    pub(super) fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }

    /// Releases the worker.
    pub(super) fn release(&self) {
        self.released.store(true, Ordering::Release);
    }
}

impl BufferPool {
    /// Returns physical acquisition attempts observed by this pool.
    pub(crate) fn acquisition_attempts(&self) -> usize {
        self.inner
            .test_hooks
            .acquisition_attempts
            .load(Ordering::Acquire)
    }

    /// Injects one acquisition metadata allocation failure.
    pub(super) fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        self.inner
            .test_hooks
            .inject_acquisition_allocation_failure(boundary);
    }

    /// Returns whether an acquisition metadata failure remains pending.
    pub(super) fn acquisition_allocation_failure_pending(&self) -> bool {
        self.inner
            .test_hooks
            .acquisition_allocation_failure
            .load(Ordering::Acquire)
            != 0
    }

    /// Makes the next reservation future resolve to `error`.
    pub(crate) fn inject_reservation_failure(&self, error: ReserveError) {
        self.inner.test_hooks.inject_reservation_failure(error);
    }

    /// Fails the next maintenance worker creation.
    pub(super) fn inject_maintenance_spawn_failure(&self) {
        assert!(
            !self
                .inner
                .test_hooks
                .maintenance_spawn_failure
                .swap(true, Ordering::AcqRel),
            "a maintenance spawn failure is already pending"
        );
    }

    /// Installs a one-shot pause after weak pool ownership is upgraded.
    pub(super) fn pause_maintenance_after_upgrade(&self) -> Arc<MaintenancePause> {
        let pause = Arc::new(MaintenancePause::new());
        let previous = self
            .inner
            .test_hooks
            .maintenance_pause
            .lock()
            .replace(Arc::clone(&pause));
        assert!(
            previous.is_none(),
            "a maintenance pause is already installed"
        );
        pause
    }

    /// Returns maintenance worker creation attempts.
    pub(super) fn maintenance_spawn_attempts(&self) -> usize {
        self.inner
            .test_hooks
            .maintenance_spawn_attempts
            .load(Ordering::Acquire)
    }

    /// Reconstructs and validates all load-bearing pool accounting.
    ///
    /// No claim, return, reservation, or maintenance operation may overlap
    /// this audit. Holding admission stabilizes planned demand and prepared
    /// capacity; external quiescence stabilizes lock-free bitmap ownership.
    pub(super) fn audit_quiescent(&self) -> PoolAuditReport {
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

    /// Asserts that no accounting, physical ownership, or cleanup remains.
    pub(super) fn assert_quiescent_zero(&self) {
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

impl PoolInner {
    /// Returns one coherent admission and aggregate sample.
    pub(super) fn test_accounting_state(
        &self,
    ) -> (
        CarrierCount,
        CarrierCount,
        CarrierCount,
        CarrierCount,
        usize,
    ) {
        let admission = self.admission.lock();
        let coverage = self.coverage.snapshot();
        (
            admission.ledger.prepared_capacity,
            admission.ledger.active_planned_demand,
            coverage.available,
            coverage.uncovered,
            admission.waiter_count(),
        )
    }
}

/// Constructs a waker and its shared wake counter.
pub(super) fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let state = CountingWake {
        count: Arc::clone(&count),
    };
    (Waker::from(StdArc::new(state)), count)
}

/// Loads a counting waker's observed wake count.
pub(super) fn wake_count(count: &AtomicUsize) -> usize {
    count.load(Ordering::Acquire)
}

/// Constructs a pool with a one-word optimistic scan budget.
pub(super) fn test_pool(block_carriers: usize, configured: usize) -> (BufferPool, usize) {
    test_pool_with_scan(block_carriers, configured, 1)
}

/// Constructs a pool with explicit block and optimistic-scan geometry.
pub(super) fn test_pool_with_scan(
    block_carriers: usize,
    configured: usize,
    optimistic_scan_words: usize,
) -> (BufferPool, usize) {
    test_pool_with_scan_and_diagnostics(
        block_carriers,
        configured,
        optimistic_scan_words,
        MemoryDiagnosticsConfig::for_test(None, 1),
    )
}

/// Constructs a pool with explicit scan geometry and diagnostic policy.
pub(super) fn test_pool_with_scan_and_diagnostics(
    block_carriers: usize,
    configured: usize,
    optimistic_scan_words: usize,
    diagnostics: MemoryDiagnosticsConfig,
) -> (BufferPool, usize) {
    let page_size = page_size().unwrap().get();
    let geometry = PoolGeometry::new(
        page_size,
        page_size.checked_mul(block_carriers).unwrap(),
        page_size,
    )
    .unwrap();
    let pool = BufferPool::from_parts_with_diagnostics(
        geometry,
        CarrierCount::new(configured),
        optimistic_scan_words,
        diagnostics,
    )
    .unwrap();
    (pool, page_size)
}

/// Constructs a pool whose block and carrier are one runtime page.
pub(super) fn test_single_carrier_pool(configured: usize) -> (BufferPool, usize) {
    test_pool(1, configured)
}

/// Initializes bytes through the pooled buffer's `BufMut` boundary.
pub(super) fn write_pooled(buffer: &mut PooledBufMut, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let written = {
            let chunk = buffer.chunk_mut();
            let written = chunk.len().min(bytes.len());
            chunk[..written].copy_from_slice(&bytes[..written]);
            written
        };
        // SAFETY: the preceding copy initialized exactly `written` bytes.
        unsafe { buffer.advance_mut(written) };
        bytes = &bytes[written..];
    }
}

/// Polls one reservation future with an explicit waker.
pub(super) fn poll_reserve(
    future: &mut ReserveFuture,
    waker: &Waker,
) -> Poll<Result<Reservation, ReserveError>> {
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}

#[cfg(not(s3_tm_loom))]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::super::admission::MAX_PACKED_CARRIERS;
    use super::super::maintenance::{execute_maintenance, MaintenanceAction, MaintenanceOutcome};
    use super::super::virtual_memory::VirtualMemoryOperation;
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
        assert!(matches!(poll_reserve(&mut queued, &waker), Poll::Pending));

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
