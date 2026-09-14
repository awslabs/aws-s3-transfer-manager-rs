/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-pool observations and failure injection for tests.

use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};
use crate::runtime::sync::thread;

use super::super::admission::ReserveError;
use super::super::{BufferPool, CarrierCount, PoolInner};

/// Per-pool hooks that keep parallel and Loom tests isolated.
pub(in crate::runtime::buffer_pool) struct TestHooks {
    acquisition_attempts: AtomicUsize,
    return_admission_entries: AtomicUsize,
    acquisition_allocation_failure: AtomicUsize,
    reservation_queue_allocation_failure: AtomicBool,
    reservation_failure: Mutex<Option<ReserveError>>,
    maintenance_spawn_failure: AtomicBool,
    maintenance_spawn_attempts: AtomicUsize,
    maintenance_pause: Mutex<Option<Arc<MaintenancePause>>>,
}

impl TestHooks {
    pub(in crate::runtime::buffer_pool) fn new() -> Self {
        Self {
            acquisition_attempts: AtomicUsize::new(0),
            return_admission_entries: AtomicUsize::new(0),
            acquisition_allocation_failure: AtomicUsize::new(0),
            reservation_queue_allocation_failure: AtomicBool::new(false),
            reservation_failure: Mutex::new(None),
            maintenance_spawn_failure: AtomicBool::new(false),
            maintenance_spawn_attempts: AtomicUsize::new(0),
            maintenance_pause: Mutex::new(None),
        }
    }

    pub(in crate::runtime::buffer_pool) fn record_acquisition_attempt(&self) {
        self.acquisition_attempts.fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime::buffer_pool) fn record_return_admission_entry(&self) {
        self.return_admission_entries.fetch_add(1, Ordering::AcqRel);
    }

    fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        assert!(boundary != 0, "failure boundary must be nonzero");
        self.acquisition_allocation_failure
            .compare_exchange(0, boundary, Ordering::AcqRel, Ordering::Acquire)
            .expect("an acquisition allocation failure is already pending");
    }

    pub(in crate::runtime::buffer_pool) fn take_acquisition_allocation_failure(&self) -> bool {
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

    fn inject_reservation_queue_allocation_failure(&self) {
        assert!(
            !self
                .reservation_queue_allocation_failure
                .swap(true, Ordering::AcqRel),
            "a reservation queue allocation failure is already pending"
        );
    }

    pub(in crate::runtime::buffer_pool) fn take_reservation_queue_allocation_failure(
        &self,
    ) -> bool {
        self.reservation_queue_allocation_failure
            .swap(false, Ordering::AcqRel)
    }

    fn inject_reservation_failure(&self, error: ReserveError) {
        let previous = self.reservation_failure.lock().replace(error);
        assert!(
            previous.is_none(),
            "a reservation failure is already pending"
        );
    }

    pub(in crate::runtime::buffer_pool) fn take_reservation_failure(&self) -> Option<ReserveError> {
        self.reservation_failure.lock().take()
    }

    pub(in crate::runtime::buffer_pool) fn record_maintenance_spawn_attempt(&self) {
        self.maintenance_spawn_attempts
            .fetch_add(1, Ordering::AcqRel);
    }

    pub(in crate::runtime::buffer_pool) fn take_maintenance_spawn_failure(&self) -> bool {
        self.maintenance_spawn_failure.swap(false, Ordering::AcqRel)
    }

    pub(in crate::runtime::buffer_pool) fn wait_after_maintenance_upgrade(&self) {
        let pause = self.maintenance_pause.lock().take();
        if let Some(pause) = pause {
            pause.wait();
        }
    }
}

/// Test-controlled pause in the worker's temporary ownership window.
pub(in crate::runtime::buffer_pool) struct MaintenancePause {
    entered: AtomicBool,
    released: AtomicBool,
}

impl MaintenancePause {
    fn new() -> Self {
        Self {
            entered: AtomicBool::new(false),
            released: AtomicBool::new(false),
        }
    }

    fn wait(&self) {
        self.entered.store(true, Ordering::Release);
        while !self.released.load(Ordering::Acquire) {
            thread::yield_now();
        }
    }

    pub(in crate::runtime::buffer_pool) fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }

    pub(in crate::runtime::buffer_pool) fn release(&self) {
        self.released.store(true, Ordering::Release);
    }
}

impl BufferPool {
    pub(crate) fn acquisition_attempts(&self) -> usize {
        self.inner
            .test_hooks
            .acquisition_attempts
            .load(Ordering::Acquire)
    }

    pub(in crate::runtime::buffer_pool) fn return_admission_entries(&self) -> usize {
        self.inner
            .test_hooks
            .return_admission_entries
            .load(Ordering::Acquire)
    }

    pub(in crate::runtime::buffer_pool) fn inject_acquisition_allocation_failure(
        &self,
        boundary: usize,
    ) {
        self.inner
            .test_hooks
            .inject_acquisition_allocation_failure(boundary);
    }

    pub(in crate::runtime::buffer_pool) fn acquisition_allocation_failure_pending(&self) -> bool {
        self.inner
            .test_hooks
            .acquisition_allocation_failure
            .load(Ordering::Acquire)
            != 0
    }

    pub(in crate::runtime::buffer_pool) fn inject_reservation_queue_allocation_failure(&self) {
        self.inner
            .test_hooks
            .inject_reservation_queue_allocation_failure();
    }

    pub(crate) fn inject_reservation_failure(&self, error: ReserveError) {
        self.inner.test_hooks.inject_reservation_failure(error);
    }

    pub(in crate::runtime::buffer_pool) fn inject_maintenance_spawn_failure(&self) {
        assert!(
            !self
                .inner
                .test_hooks
                .maintenance_spawn_failure
                .swap(true, Ordering::AcqRel),
            "a maintenance spawn failure is already pending"
        );
    }

    pub(in crate::runtime::buffer_pool) fn pause_maintenance_after_upgrade(
        &self,
    ) -> Arc<MaintenancePause> {
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

    pub(in crate::runtime::buffer_pool) fn maintenance_spawn_attempts(&self) -> usize {
        self.inner
            .test_hooks
            .maintenance_spawn_attempts
            .load(Ordering::Acquire)
    }
}

impl PoolInner {
    pub(in crate::runtime::buffer_pool) fn test_accounting_state(
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
