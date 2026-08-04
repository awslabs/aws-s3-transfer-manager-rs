/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

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
//! [`PoolInner::admission`] serializes global ledger transitions. Reservation
//! acquisition counts use a packed atomic so direct return before close avoids
//! that lock. Close holds the admission lock while publishing the closed bit
//! and installing the retiring count. Final carrier return makes the carrier
//! reusable before lowering admission pressure.
//!
//! [`BufferPool::try_reserve`] performs immediate admission and never parks.
//! This implementation does not provide FIFO parking, forced oversized
//! admission, topology, or block lifecycle.

use std::sync::{Arc, Mutex};

mod admission;
use admission::{AdmissionState, Charge, DirectDebit, Reservation, ReservationPlan};

mod arena;
use arena::{Arena, CarrierAllocation, CarrierId};

mod buffer;
use buffer::{PooledBufMut, WritableCarrier};

#[cfg(test)]
mod tests;

/// Cloneable handle to the pool's admission and physical storage state.
#[derive(Clone)]
struct BufferPool {
    inner: Arc<PoolInner>,
}

/// Shared lifetime retained by reservations and checked-out carriers.
struct PoolInner {
    admission: Mutex<AdmissionState>,
    arena: Arena,
}

/// Final-return token shared by every view over one carrier checkout.
struct CarrierGuard {
    pool: Arc<PoolInner>,
    id: CarrierId,
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
    configured: usize,
    active_planned_demand: usize,
    retiring_direct_live: usize,
    unreserved_coverage: usize,
    unreserved_live: usize,
    unreserved_debt: usize,
    admission_used: usize,
    retained: usize,
    free_retained: usize,
    overflow: usize,
    physical_live: usize,
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
                arena: Arena::new(carrier_size, configured_carriers),
            }),
        }
    }

    /// Admit `plan` immediately or report current capacity pressure.
    ///
    /// A successful call charges the complete envelope. This method does not
    /// park.
    fn try_reserve(&self, plan: ReservationPlan) -> Result<Reservation, ReserveError> {
        let mut admission = self.inner.admission.lock().unwrap();
        admission.try_admit(plan)?;
        drop(admission);
        Ok(Reservation::new(Arc::clone(&self.inner), plan))
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
            let allocation = match self.inner.arena.acquire_one() {
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
        let mut carriers = Vec::with_capacity(carrier_count);

        for _ in 0..carrier_count {
            let allocation = match self.inner.arena.acquire_one() {
                Ok(allocation) => allocation,
                Err(error) => {
                    drop(carriers);
                    return Err(error);
                }
            };
            let id = allocation.id;
            self.inner.record_unreserved_acquire();
            carriers.push(WritableCarrier::new(
                allocation,
                Arc::new(CarrierGuard {
                    pool: Arc::clone(&self.inner),
                    id,
                    charge: Charge::Unreserved,
                }),
            ));
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
        let admission = self.inner.admission.lock().unwrap();
        let arena = self.inner.arena.snapshot();
        PoolSnapshot {
            configured: admission.ledger.configured,
            active_planned_demand: admission.ledger.active_planned_demand,
            retiring_direct_live: admission.ledger.retiring_direct_live,
            unreserved_coverage: admission.ledger.unreserved_coverage,
            unreserved_live: admission.ledger.unreserved_live,
            unreserved_debt: admission.ledger.unreserved_debt,
            admission_used: admission.ledger.admission_used(),
            retained: arena.retained,
            free_retained: arena.free_retained,
            overflow: arena.overflow,
            physical_live: arena.physical_live,
        }
    }

    /// Inject a physical acquisition failure after `successes` allocations.
    fn fail_after_successes(&self, successes: usize) {
        self.inner.arena.fail_after_successes(successes);
    }
}

impl DirectDebit {
    /// Transfer one debited unit into a carrier's final-return guard.
    fn commit(&mut self, allocation: CarrierAllocation) -> WritableCarrier {
        assert!(self.uncommitted > 0, "direct debit is exhausted");
        self.uncommitted -= 1;
        let guard = Arc::new(CarrierGuard {
            pool: Arc::clone(&self.reservation.pool),
            id: allocation.id,
            charge: Charge::Direct(Arc::clone(&self.reservation)),
        });
        WritableCarrier::new(allocation, guard)
    }
}

impl Drop for CarrierGuard {
    fn drop(&mut self) {
        // Publish physical availability before reducing admission pressure.
        // A newly admitted operation may acquire this carrier immediately.
        self.pool.arena.return_carrier(self.id);
        match &self.charge {
            Charge::Direct(reservation) => reservation.release_direct(1),
            Charge::Unreserved => self.pool.release_unreserved(),
        }
    }
}
