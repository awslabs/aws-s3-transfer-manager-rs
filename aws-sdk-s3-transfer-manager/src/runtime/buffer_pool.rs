/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#![cfg_attr(all(test, s3_tm_loom), allow(dead_code))]

//! Elastic pooled storage for transfer payloads.
//!
//! Admission and physical acquisition are separate operations. A reservation
//! charges planned demand before dispatch. Execution then acquires carriers
//! against that reservation, or without one for transport traffic that cannot
//! carry request-scoped authority.
//!
//! # Accounting
//!
//! While acquisition is open, a reservation charges its complete envelope.
//! Closing consumes the acquisition authority and replaces that envelope with
//! the exact number of direct carriers still owned by consumers. Unreserved
//! carriers not covered by open reservations become debt and suppress later
//! admission until they return.
//!
//! # Ownership
//!
//! Each carrier checkout creates one [`CarrierGuard`]. Mutable tails and every
//! immutable `Bytes` view over that carrier share the guard. Its final drop
//! returns the carrier and applies exactly one accounting inverse.
//!
//! # Synchronization
//!
//! [`PoolInner::admission`] serializes planned-demand, FIFO, and debt-producing
//! transitions. A packed unreserved state lets covered transport acquisition
//! and debt-free return avoid that lock while remaining atomic with coverage
//! grant and withdrawal. Reservation acquisition counts use a second packed
//! atomic so direct return before close also avoids the lock. Close holds the
//! admission lock while publishing the closed bit and installing exact
//! retiring charges. Final carrier return makes the carrier reusable before
//! lowering admission pressure.
//!
//! Reservation admission is strict FIFO. A grant is physically prepared and
//! transferred to its waiter before notification. Notifications run after the
//! admission lock is released.
//!
//! Queue operations may take the admission lock and then a waiter-slot lock.
//! Ticket drop extracts its slot state and releases that lock before taking the
//! admission lock. This ordering must not be reversed or nested.
//!
//! This implementation does not provide topology or a real mmap backend.

use crate::runtime::sync::sync::{Arc, Mutex};

mod admission;
use admission::{
    AdmissionState, Charge, DirectDebit, NotifyFn, Reservation, ReservationPlan, Reserve,
    UnreservedDebit, UnreservedState,
};

mod arena;
use arena::{Arena, CarrierAllocation};

mod buffer;
use buffer::{PooledBufMut, WritableCarrier};

#[cfg(all(test, not(s3_tm_loom)))]
mod tests;

/// Cloneable handle to the pool's admission and physical storage state.
#[derive(Clone)]
struct BufferPool {
    inner: Arc<PoolInner>,
}

/// Shared lifetime retained by reservations and checked-out carriers.
struct PoolInner {
    admission: Mutex<AdmissionState>,
    /// Aggregate counters changed at transport acquisition frequency.
    unreserved: UnreservedState,
    arena: Arena,
}

/// Final-return token shared by every view over one carrier checkout.
struct CarrierGuard {
    pool: Arc<PoolInner>,
    allocation: Option<CarrierAllocation>,
    charge: Charge,
}

/// Minimum writable capacity requested from the pool.
#[derive(Clone, Copy)]
struct AcquireRequest {
    minimum_capacity: usize,
}

impl AcquireRequest {
    /// Request at least `minimum_capacity` writable bytes.
    fn new(minimum_capacity: usize) -> Self {
        assert!(
            minimum_capacity > 0,
            "an acquisition must request nonzero capacity"
        );
        Self { minimum_capacity }
    }
}

/// Failure to create a new reservation immediately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReserveError {
    Closed,
    AtCapacity,
    PhysicalPreparationFailed,
    CapacityOverflow,
}

/// Failure to acquire physical carriers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AllocError {
    ForeignReservation,
    ReservationEnvelopeExceeded,
    PhysicalAllocationFailed,
    CapacityOverflow,
}

/// Diagnostic snapshot of logical and physical pool state.
///
/// Admission and arena counters are sampled under their respective locks but
/// not in one atomic transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PoolSnapshot {
    /// Soft admission and retained-capacity target.
    configured: usize,
    /// Capacity whose current preparation steps have completed.
    prepared: usize,
    /// Full envelopes with direct acquisition still open.
    active_planned_demand: usize,
    /// Direct owners surviving after reservation close.
    retiring_direct_live: usize,
    /// Active-plan capacity available to unreserved acquisition.
    unreserved_coverage: usize,
    /// Live ownership acquired without reservation authority.
    unreserved_live: usize,
    /// Unreserved ownership not covered by active plans.
    unreserved_debt: usize,
    /// Planned, retiring, and sticky-debt pressure on admission.
    admission_used: usize,
    /// Prepared carriers retained within the configured target.
    retained: usize,
    /// Retained carriers currently available for acquisition.
    free_retained: usize,
    /// Prepared carriers above the configured target.
    overflow: usize,
    /// Mapped carriers excluded from acquisition after cleanup failure.
    quarantined: usize,
    /// Carriers currently held by mutable or immutable owners.
    physical_live: usize,
    /// Reservation requests still held in FIFO order.
    waiters: usize,
    /// Whether new reservations and queued grants are closed.
    admission_closed: bool,
}

impl BufferPool {
    /// Create an empty pool with fixed-size carriers.
    ///
    /// `configured_carriers` is both the admission ceiling and the retained
    /// carrier target. Temporary overflow may exceed it while admitted work
    /// drains.
    fn new(carrier_size: usize, configured_carriers: usize) -> Self {
        assert!(carrier_size > 0, "carrier size must be nonzero");
        assert!(
            configured_carriers > 0,
            "configured capacity must be nonzero"
        );

        Self {
            inner: Arc::new(PoolInner {
                admission: Mutex::new(AdmissionState::new(configured_carriers)),
                unreserved: UnreservedState::new(),
                arena: Arena::new(carrier_size, configured_carriers),
            }),
        }
    }

    /// Admit `plan` immediately or report current capacity pressure.
    ///
    /// A successful call charges the complete envelope. This method does not
    /// park.
    fn try_reserve(&self, plan: ReservationPlan) -> Result<Reservation, ReserveError> {
        PoolInner::try_reserve(&self.inner, plan)
    }

    /// Reserve `plan` immediately or enqueue it in strict arrival order.
    ///
    /// A pending waiter receives an already-charged reservation before its
    /// notification runs. Preparation and shutdown failure are delivered
    /// through the same waiter.
    fn reserve(&self, plan: ReservationPlan, notify: NotifyFn) -> Result<Reserve, ReserveError> {
        PoolInner::reserve(&self.inner, plan, notify)
    }

    /// Close admission and fail every queued waiter.
    ///
    /// Existing reservations, unreserved acquisition, and carrier return
    /// remain valid until their owners drain.
    fn close_admission(&self) {
        PoolInner::close_admission(&self.inner);
    }

    /// Acquire writable storage against direct reservation authority.
    ///
    /// The complete carrier count is debited before physical acquisition.
    /// [`DirectDebit`] rolls back any uncommitted count if allocation fails.
    fn acquire(
        &self,
        reservation: &Reservation,
        request: AcquireRequest,
    ) -> Result<PooledBufMut, AllocError> {
        let carrier_count = self.carriers_for(request.minimum_capacity)?;
        let mut debit = reservation.try_debit(&self.inner, carrier_count)?;
        let mut carriers = Vec::with_capacity(carrier_count);

        for _ in 0..carrier_count {
            let allocation = match self.inner.acquire_one() {
                Ok(allocation) => allocation,
                Err(error) => {
                    drop(carriers);
                    return Err(error);
                }
            };
            carriers.push(debit.commit(allocation));
        }

        debug_assert_eq!(debit.uncommitted, 0);
        Ok(PooledBufMut::new(carriers))
    }

    /// Acquire writable storage without request-scoped authority.
    ///
    /// Covered acquisition consumes active aggregate coverage. Acquisition
    /// beyond coverage creates sticky debt that suppresses later reservation
    /// grants until an unreserved carrier returns.
    fn acquire_unreserved(&self, request: AcquireRequest) -> Result<PooledBufMut, AllocError> {
        let carrier_count = self.carriers_for(request.minimum_capacity)?;
        let mut debit = PoolInner::debit_unreserved(&self.inner, carrier_count)?;
        let mut carriers = Vec::with_capacity(carrier_count);

        for _ in 0..carrier_count {
            let allocation = match self.inner.acquire_one() {
                Ok(allocation) => allocation,
                Err(error) => {
                    drop(carriers);
                    return Err(error);
                }
            };
            carriers.push(debit.commit(allocation));
        }

        Ok(PooledBufMut::new(carriers))
    }

    /// Convert byte capacity to whole carriers without overflow.
    fn carriers_for(&self, minimum_capacity: usize) -> Result<usize, AllocError> {
        let carrier_size = self.inner.arena.carrier_size();
        minimum_capacity
            .checked_add(carrier_size - 1)
            .map(|rounded| rounded / carrier_size)
            .ok_or(AllocError::CapacityOverflow)
    }

    /// Capture diagnostic logical and physical counters.
    ///
    /// The two stores are sampled in lock order, but ownership may transition
    /// between samples.
    fn snapshot(&self) -> PoolSnapshot {
        let admission = self.inner.admission.lock();
        let unreserved = self.inner.unreserved.snapshot();
        let arena = self.inner.arena.snapshot();
        PoolSnapshot {
            configured: admission.ledger.configured,
            prepared: admission.ledger.prepared,
            active_planned_demand: admission.ledger.active_planned_demand,
            retiring_direct_live: admission.ledger.retiring_direct_live,
            unreserved_coverage: admission.ledger.unreserved_coverage,
            unreserved_live: admission.ledger.unreserved_live(unreserved),
            unreserved_debt: unreserved.debt,
            admission_used: admission.ledger.admission_used(unreserved.debt),
            retained: arena.retained,
            free_retained: arena.free_retained,
            overflow: arena.overflow,
            quarantined: arena.quarantined,
            physical_live: arena.physical_live,
            waiters: admission.waiter_count(),
            admission_closed: admission.is_closed(),
        }
    }

    /// Inject a physical acquisition failure after `successes` allocations.
    fn fail_after_successes(&self, successes: usize) {
        self.inner.arena.fail_after_successes(successes);
    }

    /// Fail the next operation that must prepare additional capacity.
    fn fail_next_preparation(&self) {
        self.inner.arena.fail_next_preparation();
    }

    /// Trim free overflow while retaining configured reusable capacity.
    fn trim_excess(&self) -> usize {
        self.inner.trim_excess()
    }
}

impl DirectDebit {
    /// Transfer one debited unit into a carrier's final-return guard.
    fn commit(&mut self, allocation: CarrierAllocation) -> WritableCarrier {
        assert!(self.uncommitted > 0, "direct debit is exhausted");
        self.uncommitted -= 1;
        let ptr = allocation.ptr();
        let capacity = allocation.capacity();
        let source = allocation.source;
        let guard = Arc::new(CarrierGuard {
            pool: Arc::clone(&self.reservation.pool),
            allocation: Some(allocation),
            charge: Charge::Direct(Arc::clone(&self.reservation)),
        });
        WritableCarrier::new(ptr, capacity, source, guard)
    }
}

impl UnreservedDebit {
    /// Transfer one installed charge into a carrier's final-return guard.
    fn commit(&mut self, allocation: CarrierAllocation) -> WritableCarrier {
        assert!(self.uncommitted > 0, "unreserved debit is exhausted");
        self.uncommitted -= 1;
        let ptr = allocation.ptr();
        let capacity = allocation.capacity();
        let source = allocation.source;
        let guard = Arc::new(CarrierGuard {
            pool: Arc::clone(&self.pool),
            allocation: Some(allocation),
            charge: Charge::Unreserved,
        });
        WritableCarrier::new(ptr, capacity, source, guard)
    }
}

impl Drop for CarrierGuard {
    fn drop(&mut self) {
        // Publish physical availability before reducing admission pressure.
        // A newly admitted operation may acquire this carrier immediately.
        drop(self.allocation.take());
        match &self.charge {
            Charge::Direct(reservation) => reservation.release_direct(1),
            Charge::Unreserved => PoolInner::release_unreserved(&self.pool, 1),
        }
    }
}

impl PoolInner {
    /// Acquire from prepared capacity, then serialize an exhaustive recheck and
    /// compatible growth on miss.
    fn acquire_one(&self) -> Result<CarrierAllocation, AllocError> {
        if let Some(allocation) = self.arena.try_acquire_one()? {
            return Ok(allocation);
        }

        let mut admission = self.admission.lock();
        self.arena
            .acquire_or_grow_one(&mut admission.ledger.prepared)
    }

    /// Remove free overflow without crossing admission's prepared floor.
    fn trim_excess(&self) -> usize {
        let mut trimmed = 0;
        loop {
            let cleanup = {
                let mut admission = self.admission.lock();
                let floor = admission.ledger.configured.max(
                    admission
                        .ledger
                        .admission_used(self.unreserved.snapshot().debt),
                );
                self.arena
                    .start_trim_excess(&mut admission.ledger.prepared, floor)
            };
            let Some(cleanup) = cleanup else {
                return trimmed;
            };
            let carrier_count = cleanup.carrier_count();
            if cleanup.finish() {
                trimmed += carrier_count;
            }
        }
    }
}
