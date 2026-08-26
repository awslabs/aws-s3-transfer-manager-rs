/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Complete carrier acquisition and physical-first final return.
//!
//! Accounting is installed before bitmap ownership. A pending transaction
//! retains both until every carrier has become a guard or rollback has
//! returned all physical ownership before releasing its charges.

use std::fmt;

use smallvec::SmallVec;

use super::admission::{
    AdmissionGuard, DirectDebitError, Reservation, ReservationState, ReserveError,
};
use super::arena::{ArenaError, ClaimBatch};
use super::block::{BlockError, CarrierAllocation};
use super::{BufferPool, CarrierCount, PoolInner};
#[cfg(test)]
use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
use crate::runtime::sync::sync::Arc;

/// Failure to acquire a complete mutable carrier batch.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AcquireError {
    /// The byte request was zero.
    InvalidSize,
    /// The reservation belongs to another pool.
    ForeignReservation,
    /// The reservation no longer permits carrier acquisition.
    ReservationClosed,
    /// The request exceeds the reservation's direct-acquisition authority.
    ReservationCapacityExceeded,
    /// Geometry or accounting cannot represent the request.
    CapacityOverflow,
    /// Physical storage or ownership metadata could not be allocated.
    PhysicalAllocationFailed,
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize => f.write_str("acquisition size must be nonzero"),
            Self::ForeignReservation => f.write_str("reservation belongs to another buffer pool"),
            Self::ReservationClosed => f.write_str("reservation acquisition is closed"),
            Self::ReservationCapacityExceeded => {
                f.write_str("acquisition exceeds reservation capacity")
            }
            Self::CapacityOverflow => {
                f.write_str("acquisition exceeds buffer-pool accounting capacity")
            }
            Self::PhysicalAllocationFailed => f.write_str("physical buffer-pool allocation failed"),
        }
    }
}

impl std::error::Error for AcquireError {}

/// One-shot failure injection for fallible acquisition metadata boundaries.
#[cfg(test)]
pub(super) struct AcquisitionAllocationFailures {
    /// Remaining boundaries before the injected failure.
    remaining: AtomicUsize,
}

#[cfg(test)]
impl AcquisitionAllocationFailures {
    /// Creates a disabled injector.
    pub(super) fn new() -> Self {
        Self {
            remaining: AtomicUsize::new(0),
        }
    }

    /// Fails the `boundary`th subsequent metadata reservation.
    fn inject_on_nth(&self, boundary: usize) {
        assert!(boundary != 0, "failure boundary must be nonzero");
        self.remaining
            .compare_exchange(0, boundary, Ordering::AcqRel, Ordering::Acquire)
            .expect("an acquisition allocation failure is already pending");
    }

    /// Returns whether this boundary consumes the injected failure.
    fn take(&self) -> bool {
        self.remaining
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
}

impl BufferPool {
    /// Acquires a complete carrier-rounded batch under one reservation.
    pub(crate) fn acquire(
        &self,
        reservation: &Reservation,
        min_bytes: usize,
    ) -> Result<AcquiredRuns, AcquireError> {
        let count = self.acquisition_count(min_bytes)?;
        let direct = reservation
            .acquisition_state()
            .ok_or(AcquireError::ReservationClosed)?;
        if !direct.belongs_to(&self.inner) {
            return Err(AcquireError::ForeignReservation);
        }
        acquire_count(&self.inner, Some(Arc::clone(direct)), count)
    }

    /// Acquires a complete carrier-rounded batch without a reservation.
    pub(crate) fn acquire_unreserved(
        &self,
        min_bytes: usize,
    ) -> Result<AcquiredRuns, AcquireError> {
        let count = self.acquisition_count(min_bytes)?;
        acquire_count(&self.inner, None, count)
    }

    /// Converts one acquisition request to its complete carrier count.
    fn acquisition_count(&self, min_bytes: usize) -> Result<CarrierCount, AcquireError> {
        self.inner
            .arena
            .carriers_for_bytes(min_bytes)
            .map_err(|error| match error {
                super::geometry::GeometryError::ZeroByteRequest => AcquireError::InvalidSize,
                _ => AcquireError::CapacityOverflow,
            })
    }

    /// Injects one acquisition metadata allocation failure for tests.
    #[cfg(test)]
    fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        self.inner
            .acquisition_allocation_failures
            .inject_on_nth(boundary);
    }
}

/// Aggregate and optional direct charges not yet owned by carrier guards.
struct AcquisitionDebit {
    /// Pool whose aggregate accounting owns every remaining charge.
    pool: Arc<PoolInner>,
    /// Reservation whose direct authority owns every remaining charge.
    direct: Option<Arc<ReservationState>>,
    /// Charges not yet transferred to carrier guards.
    untransferred: CarrierCount,
}

impl AcquisitionDebit {
    /// Installs a complete accounting debit before physical acquisition.
    fn install(
        pool: &Arc<PoolInner>,
        direct: Option<Arc<ReservationState>>,
        count: CarrierCount,
    ) -> Result<Self, AcquireError> {
        if let Some(direct) = direct.as_ref() {
            direct
                .precheck_debit(count)
                .map_err(map_direct_debit_error)?;
        }

        let aggregate = match pool.try_debit_covered(count) {
            Ok(true) => Ok(()),
            Ok(false) => {
                let mut admission = AdmissionGuard::new(pool.admission.lock());
                PoolInner::debit_and_prepare_locked(pool, &mut admission, count)
            }
            Err(error) => Err(error),
        };
        if let Err(error) = aggregate {
            return Err(map_reserve_error(error));
        }
        if let Some(direct) = direct.as_ref() {
            if let Err(error) = direct.try_debit(count) {
                PoolInner::release_acquisition_charges(pool, count);
                return Err(map_direct_debit_error(error));
            }
        }

        Ok(Self {
            pool: Arc::clone(pool),
            direct,
            untransferred: count,
        })
    }

    /// Transfers one physical allocation and its accounting provenance.
    fn transfer(&mut self, allocation: CarrierAllocation) -> Arc<CarrierGuard> {
        assert!(
            self.untransferred != CarrierCount::ZERO,
            "acquisition debit transferred too many charges"
        );
        let guard = Arc::new(CarrierGuard {
            pool: Arc::clone(&self.pool),
            allocation: Some(allocation),
            direct: self.direct.as_ref().map(Arc::clone),
        });
        self.untransferred = self
            .untransferred
            .checked_sub(CarrierCount::new(1))
            .expect("acquisition debit must retain a transferred charge");
        guard
    }
}

impl Drop for AcquisitionDebit {
    fn drop(&mut self) {
        if self.untransferred == CarrierCount::ZERO {
            return;
        }
        if let Some(direct) = self.direct.as_ref() {
            direct.release(self.untransferred);
        }
        PoolInner::release_acquisition_charges(&self.pool, self.untransferred);
    }
}

/// Physical and accounting ownership retained until acquisition commits.
struct PendingAcquisition {
    /// Completed carrier owners returned before the remaining transaction.
    guards: Vec<Arc<CarrierGuard>>,
    /// Provisional physical bits returned before accounting rollback.
    claim: Option<ClaimBatch>,
    /// Aggregate and direct charges released last.
    debit: Option<AcquisitionDebit>,
}

impl PendingAcquisition {
    /// Creates a pending transaction after its complete debit is installed.
    fn new(debit: AcquisitionDebit) -> Self {
        Self {
            guards: Vec::new(),
            claim: None,
            debit: Some(debit),
        }
    }

    /// Converts one complete physical batch into grouped carrier ownership.
    fn finish(mut self) -> Result<AcquiredRuns, AcquireError> {
        let claim = self
            .claim
            .take()
            .expect("pending acquisition must retain its physical claim");
        let allocations = claim.finish().map_err(map_arena_error)?;
        let pool = &self
            .debit
            .as_ref()
            .expect("pending acquisition must retain its accounting debit")
            .pool;
        if allocation_failure_injected(pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        self.guards
            .try_reserve_exact(allocations.len())
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;

        let debit = self
            .debit
            .as_mut()
            .expect("pending acquisition must retain its accounting debit");
        for allocation in allocations {
            self.guards.push(debit.transfer(allocation));
        }
        assert_eq!(
            debit.untransferred,
            CarrierCount::ZERO,
            "complete acquisition left untransferred charges"
        );

        AcquiredRuns::try_from_guards(std::mem::take(&mut self.guards))
    }
}

impl Drop for PendingAcquisition {
    fn drop(&mut self) {
        self.guards.clear();
        drop(self.claim.take());
        drop(self.debit.take());
    }
}

/// Complete carrier runs returned by one acquisition transaction.
pub(crate) struct AcquiredRuns {
    /// Contiguous physical runs in ascending slot and carrier order.
    runs: SmallVec<[CarrierRun; 4]>,
    /// Complete carrier-rounded capacity.
    capacity: usize,
}

impl AcquiredRuns {
    /// Groups physical owners into ascending adjacent carrier runs.
    fn try_from_guards(mut guards: Vec<Arc<CarrierGuard>>) -> Result<AcquiredRuns, AcquireError> {
        guards.sort_unstable_by_key(|guard| guard.identity());
        let pool = Arc::clone(
            &guards
                .first()
                .expect("complete acquisition must contain a carrier")
                .pool,
        );

        let mut runs = SmallVec::<[CarrierRun; 4]>::new();
        if allocation_failure_injected(&pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        runs.try_reserve(guards.len())
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        let mut capacity = 0usize;
        for guard in guards {
            capacity = capacity
                .checked_add(guard.capacity())
                .ok_or(AcquireError::CapacityOverflow)?;
            if let Some(run) = runs.last_mut() {
                if run.can_append(&guard) {
                    run.try_append(guard)?;
                    continue;
                }
            }
            runs.push(CarrierRun::try_new(guard)?);
        }

        Ok(Self { runs, capacity })
    }

    /// Returns the complete carrier-rounded byte capacity.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of contiguous carrier runs.
    pub(crate) fn run_count(&self) -> usize {
        self.runs.len()
    }

    /// Transfers the grouped carrier owners to the mutable-buffer layer.
    pub(super) fn into_runs(self) -> SmallVec<[CarrierRun; 4]> {
        self.runs
    }
}

/// Adjacent carriers within one stable block slot.
pub(super) struct CarrierRun {
    /// Stable block slot containing the run.
    slot_id: u32,
    /// First carrier index within the slot.
    first_carrier: u32,
    /// Total run capacity in bytes.
    capacity: usize,
    /// Per-carrier owners in ascending address order.
    carriers: Vec<Arc<CarrierGuard>>,
}

impl CarrierRun {
    /// Creates a run containing one carrier.
    fn try_new(guard: Arc<CarrierGuard>) -> Result<Self, AcquireError> {
        let (slot_id, first_carrier) = guard.identity();
        let capacity = guard.capacity();
        let mut carriers = Vec::new();
        if allocation_failure_injected(&guard.pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        carriers
            .try_reserve_exact(1)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        carriers.push(guard);
        Ok(Self {
            slot_id,
            first_carrier,
            capacity,
            carriers,
        })
    }

    /// Returns whether `guard` immediately follows this run.
    fn can_append(&self, guard: &CarrierGuard) -> bool {
        let run_len =
            u32::try_from(self.carriers.len()).expect("carrier run length must fit block geometry");
        let next = self
            .first_carrier
            .checked_add(run_len)
            .expect("carrier run end must fit block geometry");
        guard.identity() == (self.slot_id, next)
    }

    /// Appends one adjacent carrier without exposing a partial run on failure.
    fn try_append(&mut self, guard: Arc<CarrierGuard>) -> Result<(), AcquireError> {
        if allocation_failure_injected(&guard.pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        self.carriers
            .try_reserve(1)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        self.capacity = self
            .capacity
            .checked_add(guard.capacity())
            .ok_or(AcquireError::CapacityOverflow)?;
        self.carriers.push(guard);
        Ok(())
    }

    /// Returns the run's total byte capacity.
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the first carrier's checked writable pointer.
    pub(super) fn ptr(&self) -> std::ptr::NonNull<std::mem::MaybeUninit<u8>> {
        self.carriers
            .first()
            .expect("carrier run must be nonempty")
            .ptr()
    }

    /// Transfers per-carrier ownership in ascending address order.
    pub(super) fn into_carriers(self) -> Vec<Arc<CarrierGuard>> {
        self.carriers
    }
}

/// One physical carrier and its aggregate accounting charge.
pub(super) struct CarrierGuard {
    /// Pool retained through final physical and accounting return.
    pool: Arc<PoolInner>,
    /// Single-owner physical return capability.
    allocation: Option<CarrierAllocation>,
    /// Optional reservation-local return provenance.
    direct: Option<Arc<ReservationState>>,
}

impl CarrierGuard {
    /// Returns stable identity used only to group adjacent carriers.
    fn identity(&self) -> (u32, u32) {
        let allocation = self
            .allocation
            .as_ref()
            .expect("live carrier guard must retain its allocation");
        (allocation.slot_id(), allocation.carrier_index())
    }

    /// Returns this carrier's byte capacity.
    pub(super) fn capacity(&self) -> usize {
        self.allocation
            .as_ref()
            .expect("live carrier guard must retain its allocation")
            .capacity()
    }

    /// Returns the first byte of this exclusively owned carrier.
    pub(super) fn ptr(&self) -> std::ptr::NonNull<std::mem::MaybeUninit<u8>> {
        self.allocation
            .as_ref()
            .expect("live carrier guard must retain its allocation")
            .ptr()
    }
}

impl Drop for CarrierGuard {
    fn drop(&mut self) {
        let allocation = self
            .allocation
            .take()
            .expect("carrier guard returned its allocation more than once");
        drop(allocation);
        if let Some(direct) = self.direct.as_ref() {
            direct.release(CarrierCount::new(1));
        }
        PoolInner::release_acquisition_charges(&self.pool, CarrierCount::new(1));
    }
}

/// Installs accounting, acquires a complete physical batch, and groups it.
fn acquire_count(
    pool: &Arc<PoolInner>,
    direct: Option<Arc<ReservationState>>,
    count: CarrierCount,
) -> Result<AcquiredRuns, AcquireError> {
    let debit = AcquisitionDebit::install(pool, direct, count)?;
    let mut pending = PendingAcquisition::new(debit);
    pending.claim = Some(
        pool.arena
            .claim_optimistic(count)
            .map_err(map_arena_error)?,
    );

    if !pending
        .claim
        .as_ref()
        .expect("pending acquisition must retain its claim")
        .is_complete()
    {
        let fallback = {
            let mut admission = AdmissionGuard::new(pool.admission.lock());
            pool.arena.complete_claim_serialized(
                &mut admission,
                pending
                    .claim
                    .as_mut()
                    .expect("pending acquisition must retain its claim"),
            )
        };
        fallback.map_err(map_arena_error)?;
    }

    pending.finish()
}

/// Returns whether test injection fails this fallible allocation boundary.
fn allocation_failure_injected(pool: &PoolInner) -> bool {
    #[cfg(test)]
    {
        pool.acquisition_allocation_failures.take()
    }
    #[cfg(not(test))]
    {
        let _ = pool;
        false
    }
}

fn map_direct_debit_error(error: DirectDebitError) -> AcquireError {
    match error {
        DirectDebitError::Closed => AcquireError::ReservationClosed,
        DirectDebitError::CapacityExceeded => AcquireError::ReservationCapacityExceeded,
        DirectDebitError::CapacityOverflow => AcquireError::CapacityOverflow,
    }
}

fn map_reserve_error(error: ReserveError) -> AcquireError {
    match error {
        ReserveError::InvalidSize => AcquireError::InvalidSize,
        ReserveError::PhysicalPreparationFailed => AcquireError::PhysicalAllocationFailed,
        ReserveError::CapacityOverflow => AcquireError::CapacityOverflow,
    }
}

fn map_arena_error(error: ArenaError) -> AcquireError {
    match error {
        ArenaError::Block(BlockError::PreparedCapacityOverflow)
        | ArenaError::SlotIdExhausted
        | ArenaError::RegistryCapacityOverflow
        | ArenaError::AddressOverflow { .. }
        | ArenaError::ScanSpaceOverflow { .. } => AcquireError::CapacityOverflow,
        ArenaError::Block(_)
        | ArenaError::Allocation(_)
        | ArenaError::InvalidScanBudget
        | ArenaError::InvalidClaimCount
        | ArenaError::IncompleteClaim { .. }
        | ArenaError::AddressOverlap { .. } => AcquireError::PhysicalAllocationFailed,
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use super::super::block::{BlockSlot, TrimBlocked};
    use super::super::geometry::PoolGeometry;
    use super::super::virtual_memory::{page_size, VirtualMemoryOperation};
    use super::super::ReserveFuture;
    use super::*;

    struct ClaimingWake {
        pool: BufferPool,
        wakes: AtomicUsize,
        claimed: AtomicBool,
    }

    impl Wake for ClaimingWake {
        fn wake(self: StdArc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &StdArc<Self>) {
            let claim = self
                .pool
                .inner
                .arena
                .claim_optimistic(CarrierCount::new(1))
                .expect("wake-time claim");
            self.claimed.store(claim.is_complete(), Ordering::Release);
            self.wakes.fetch_add(1, Ordering::Release);
        }
    }

    fn test_pool(
        block_carriers: usize,
        configured: usize,
        optimistic_scan_words: usize,
    ) -> (BufferPool, usize) {
        let page_size = page_size().unwrap().get();
        let geometry = PoolGeometry::new(
            page_size,
            page_size.checked_mul(block_carriers).unwrap(),
            page_size,
        )
        .unwrap();
        let pool = BufferPool::from_validated_parts(
            geometry,
            CarrierCount::new(configured),
            optimistic_scan_words,
        )
        .unwrap();
        (pool, page_size)
    }

    fn poll_reserve(
        future: &mut ReserveFuture,
        waker: &Waker,
    ) -> Poll<Result<Reservation, ReserveError>> {
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn test_acquisition_rejects_zero_foreign_and_excess_requests() {
        let (first, carrier_size) = test_pool(1, 1, 1);
        let (second, _) = test_pool(1, 1, 1);
        let reservation = first
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");

        assert!(matches!(
            first.acquire_unreserved(0),
            Err(AcquireError::InvalidSize)
        ));
        assert!(matches!(
            second.acquire(&reservation, carrier_size),
            Err(AcquireError::ForeignReservation)
        ));
        assert!(matches!(
            first.acquire(&reservation, carrier_size * 2),
            Err(AcquireError::ReservationCapacityExceeded)
        ));
        assert_eq!(
            first.inner.test_accounting_state(),
            (
                CarrierCount::new(1),
                CarrierCount::new(1),
                CarrierCount::new(1),
                CarrierCount::ZERO,
                0,
            )
        );

        let direct = Arc::clone(reservation.acquisition_state().unwrap());
        reservation.close_acquisition();
        assert!(matches!(
            acquire_count(&first.inner, Some(direct), CarrierCount::new(1)),
            Err(AcquireError::ReservationClosed)
        ));
    }

    #[test]
    fn test_reserved_acquisition_returns_direct_and_aggregate_authority() {
        let (pool, carrier_size) = test_pool(2, 2, 1);
        let reservation = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("reservation");
        let direct = Arc::clone(reservation.acquisition_state().unwrap());

        let acquired = pool.acquire(&reservation, carrier_size + 1).unwrap();

        assert_eq!(acquired.capacity(), carrier_size * 2);
        assert_eq!(acquired.run_count(), 1);
        assert_eq!(direct.test_owner_state(), (false, CarrierCount::new(2)));
        assert_eq!(
            pool.inner.test_accounting_state(),
            (
                CarrierCount::new(2),
                CarrierCount::new(2),
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                0,
            )
        );

        drop(acquired);
        assert_eq!(direct.test_owner_state(), (false, CarrierCount::ZERO));
        assert_eq!(pool.inner.test_accounting_state().2, CarrierCount::new(2));
        reservation.close_acquisition();
        assert_eq!(
            pool.inner.test_accounting_state(),
            (
                CarrierCount::new(2),
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                0,
            )
        );
    }

    #[test]
    fn test_unreserved_shortfall_prepares_and_groups_complete_runs() {
        let (pool, carrier_size) = test_pool(2, 1, 1);

        let acquired = pool.acquire_unreserved(carrier_size * 3).unwrap();

        assert_eq!(acquired.capacity(), carrier_size * 3);
        assert_eq!(acquired.run_count(), 2);
        assert_eq!(acquired.runs[0].capacity(), carrier_size * 2);
        assert_eq!(acquired.runs[1].capacity(), carrier_size);
        assert_eq!(
            pool.inner.test_accounting_state(),
            (
                CarrierCount::new(4),
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                CarrierCount::new(3),
                0,
            )
        );

        drop(acquired);
        assert_eq!(
            pool.inner.test_accounting_state(),
            (
                CarrierCount::new(4),
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                0,
            )
        );
    }

    #[test]
    fn test_partial_optimistic_claim_completes_through_fallback() {
        let (pool, carrier_size) = test_pool(65, 65, 1);
        let reservation = pool
            .try_reserve(carrier_size * 65)
            .unwrap()
            .expect("reservation");

        let acquired = pool.acquire(&reservation, carrier_size * 65).unwrap();

        assert_eq!(acquired.capacity(), carrier_size * 65);
        assert_eq!(acquired.run_count(), 1);
        assert_eq!(pool.inner.arena.diagnostics().serialized_fallbacks, 1);
    }

    #[test]
    fn test_preparation_failure_restores_direct_and_aggregate_debits() {
        let (pool, carrier_size) = test_pool(1, 1, 1);
        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let direct = Arc::clone(reservation.acquisition_state().unwrap());
        let occupying = pool.acquire_unreserved(carrier_size).unwrap();
        let failed_slot = pool.inner.arena.reserve_slot().unwrap();
        failed_slot.inject_failure_once(VirtualMemoryOperation::Prepare);

        assert!(matches!(
            pool.acquire(&reservation, carrier_size),
            Err(AcquireError::PhysicalAllocationFailed)
        ));
        assert_eq!(direct.test_owner_state(), (false, CarrierCount::ZERO));
        assert_eq!(
            pool.inner.test_accounting_state(),
            (
                CarrierCount::new(1),
                CarrierCount::new(1),
                CarrierCount::ZERO,
                CarrierCount::ZERO,
                0,
            )
        );

        let retry = pool.acquire(&reservation, carrier_size).unwrap();
        assert_eq!(retry.capacity(), carrier_size);
        drop(retry);
        drop(occupying);
    }

    #[test]
    fn test_covered_acquisition_does_not_enter_admission() {
        let (pool, carrier_size) = test_pool(1, 1, 1);
        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let admission = pool.inner.admission.lock();
        let worker_pool = pool.clone();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            let acquired = worker_pool.acquire(&reservation, carrier_size);
            let result = match &acquired {
                Ok(runs) => Ok(runs.capacity()),
                Err(error) => Err(*error),
            };
            acquired_tx.send(result).unwrap();
            release_rx.recv().unwrap();
            drop(acquired);
            drop(reservation);
        });

        assert_eq!(
            acquired_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("covered acquisition blocked on admission")
                .unwrap(),
            carrier_size
        );
        drop(admission);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn test_pending_rollback_returns_bits_before_releasing_charges() {
        let (pool, _) = test_pool(2, 1, 1);
        let count = CarrierCount::new(2);
        let debit = AcquisitionDebit::install(&pool.inner, None, count).unwrap();
        let claim = pool.inner.arena.claim_optimistic(count).unwrap();
        assert!(claim.is_complete());
        let mut pending = PendingAcquisition::new(debit);
        pending.claim = Some(claim);

        drop(pending);

        assert_eq!(pool.inner.test_accounting_state().3, CarrierCount::ZERO);
        let reclaimed = pool.inner.arena.claim_optimistic(count).unwrap();
        assert!(reclaimed.is_complete());
        drop(reclaimed);
    }

    #[test]
    fn test_uncovered_return_drains_fifo_after_physical_return() {
        let (pool, carrier_size) = test_pool(1, 2, 1);
        let first = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("first reservation");
        let first_acquired = pool.acquire(&first, carrier_size).unwrap();
        first.close_acquisition();

        let second = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("idle-only reservation");
        let second_acquired = pool.acquire(&second, carrier_size).unwrap();
        let wake_state = StdArc::new(ClaimingWake {
            pool: pool.clone(),
            wakes: AtomicUsize::new(0),
            claimed: AtomicBool::new(false),
        });
        let waker = Waker::from(StdArc::clone(&wake_state));
        let mut queued = pool.reserve(carrier_size);
        assert!(poll_reserve(&mut queued, &waker).is_pending());

        drop(first_acquired);

        assert_eq!(wake_state.wakes.load(Ordering::Acquire), 1);
        assert!(
            wake_state.claimed.load(Ordering::Acquire),
            "waiter reentry ran before the returned physical bit was reusable"
        );
        let third = match poll_reserve(&mut queued, &waker) {
            Poll::Ready(Ok(reservation)) => reservation,
            Poll::Ready(Err(error)) => panic!("queued reservation failed: {error}"),
            Poll::Pending => panic!("uncovered repayment stranded an eligible waiter"),
        };
        drop(second_acquired);
        drop(second);
        drop(third);
    }

    #[test]
    fn test_return_and_close_orders_converge() {
        fn run(close_first: bool) -> (CarrierCount, CarrierCount, CarrierCount) {
            let (pool, carrier_size) = test_pool(1, 1, 1);
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let acquired = pool.acquire(&reservation, carrier_size).unwrap();
            if close_first {
                reservation.close_acquisition();
                drop(acquired);
            } else {
                drop(acquired);
                reservation.close_acquisition();
            }
            let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
            (active, available, uncovered)
        }

        assert_eq!(
            run(false),
            (CarrierCount::ZERO, CarrierCount::ZERO, CarrierCount::ZERO)
        );
        assert_eq!(run(true), run(false));
    }

    #[test]
    fn test_close_preserves_other_reservation_coverage_under_normal_ceiling() {
        let (pool, carrier_size) = test_pool(4, 8, 1);
        let first = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("first reservation");
        let first_acquired = pool.acquire(&first, carrier_size * 3).unwrap();
        let second = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("second reservation");

        first.close_acquisition();

        let closed = pool.metrics();
        assert_eq!(
            closed.active_planned_demand_bytes(),
            (carrier_size * 3) as u64
        );
        assert_eq!(closed.charged_capacity_bytes(), (carrier_size * 3) as u64);
        assert_eq!(closed.admission_used_bytes(), (carrier_size * 6) as u64);
        assert!(
            pool.try_reserve(carrier_size * 4).unwrap().is_none(),
            "close made another reservation's coverage available for a new grant"
        );

        let second_acquired = pool.acquire(&second, carrier_size * 3).unwrap();
        assert!(first_acquired.capacity() + second_acquired.capacity() <= carrier_size * 8);

        drop(first_acquired);
        let third = pool
            .try_reserve(carrier_size * 4)
            .unwrap()
            .expect("returned ownership restores normal headroom");
        let third_acquired = pool.acquire(&third, carrier_size * 4).unwrap();
        assert!(second_acquired.capacity() + third_acquired.capacity() <= carrier_size * 8);

        drop(second_acquired);
        drop(second);
        drop(third_acquired);
        drop(third);
    }

    #[test]
    fn test_carrier_owner_retains_pool_until_final_return() {
        let (pool, carrier_size) = test_pool(1, 1, 1);
        let weak = Arc::downgrade(&pool.inner);
        let acquired = pool.acquire_unreserved(carrier_size).unwrap();

        drop(pool);
        assert!(weak.upgrade().is_some());
        drop(acquired);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn test_acquisition_ownership_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send::<AcquiredRuns>();
        assert_send_sync::<CarrierGuard>();
    }

    #[test]
    fn test_metadata_allocation_failure_rolls_back_every_commit_boundary() {
        for boundary in 1..=4 {
            let (pool, carrier_size) = test_pool(2, 2, 1);
            let reservation = pool
                .try_reserve(carrier_size * 2)
                .unwrap()
                .expect("reservation");
            let direct = Arc::clone(reservation.acquisition_state().unwrap());
            pool.inject_acquisition_allocation_failure(boundary);

            assert!(
                matches!(
                    pool.acquire(&reservation, carrier_size * 2),
                    Err(AcquireError::PhysicalAllocationFailed)
                ),
                "metadata boundary {boundary} did not fail"
            );
            assert_eq!(
                direct.test_owner_state(),
                (false, CarrierCount::ZERO),
                "metadata boundary {boundary} leaked direct authority"
            );
            let metrics = pool.metrics();
            assert_eq!(
                metrics.charged_capacity_bytes(),
                0,
                "metadata boundary {boundary} leaked aggregate charges"
            );
            assert_eq!(
                metrics.active_planned_demand_bytes(),
                (carrier_size * 2) as u64
            );

            let retry = pool
                .acquire(&reservation, carrier_size * 2)
                .expect("pool remains usable after metadata failure");
            drop(retry);
            drop(reservation);
        }
    }

    #[test]
    fn test_in_flight_shortfall_charge_preserves_trim_floor() {
        let (pool, _) = test_pool(2, 1, 1);
        let debit = AcquisitionDebit::install(&pool.inner, None, CarrierCount::new(1)).unwrap();
        let candidate = pool
            .inner
            .arena
            .select_trim_candidate()
            .expect("prepared free block");

        {
            let mut admission = AdmissionGuard::new(pool.inner.admission.lock());
            let floor = admission.acquisition_floor(&pool.inner.coverage).unwrap();
            assert!(matches!(
                BlockSlot::start_trim(&candidate, admission.prepared_capacity_mut(), floor,),
                Err(TrimBlocked::FloorViolation)
            ));
        }

        drop(debit);
        let cleanup = {
            let mut admission = AdmissionGuard::new(pool.inner.admission.lock());
            let floor = admission.acquisition_floor(&pool.inner.coverage).unwrap();
            BlockSlot::start_trim(&candidate, admission.prepared_capacity_mut(), floor)
                .expect("returned charge permits trim")
        };
        cleanup.finish().unwrap();
        assert_eq!(pool.metrics().prepared_capacity_bytes(), 0);
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc as StdArc;
    use std::task::{Context, Poll, Wake, Waker};

    use super::super::block::{BlockSlot, TrimBlocked};
    use super::super::geometry::PoolGeometry;
    use super::super::virtual_memory::page_size;
    use super::super::ReserveFuture;
    use super::*;
    use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

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

    fn test_pool(configured: usize) -> (BufferPool, usize) {
        let page_size = page_size().unwrap().get();
        let geometry = PoolGeometry::new(page_size, page_size, page_size).unwrap();
        let pool =
            BufferPool::from_validated_parts(geometry, CarrierCount::new(configured), 1).unwrap();
        (pool, page_size)
    }

    fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let state = CountingWake {
            count: Arc::clone(&count),
        };
        (Waker::from(StdArc::new(state)), count)
    }

    fn poll_reserve(
        future: &mut ReserveFuture,
        waker: &Waker,
    ) -> Poll<Result<Reservation, ReserveError>> {
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn test_final_return_racing_close_converges() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let acquired = pool.acquire(&reservation, carrier_size).unwrap();

            let closing = thread::spawn(move || reservation.close_acquisition());
            let returning = thread::spawn(move || drop(acquired));
            closing.join().unwrap();
            returning.join().unwrap();

            let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::ZERO);
            assert_eq!(available, CarrierCount::ZERO);
            assert_eq!(uncovered, CarrierCount::ZERO);
        });
    }

    #[test]
    fn test_close_cannot_free_another_reservations_coverage_for_grant() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(2);
            let first = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("first reservation");
            let first_acquired = pool.acquire(&first, carrier_size).unwrap();
            let second = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("second reservation");

            let closing = thread::spawn(move || first.close_acquisition());
            let granting_pool = pool.clone();
            let granting = thread::spawn(move || granting_pool.try_reserve(carrier_size).unwrap());

            closing.join().unwrap();
            assert!(
                granting.join().unwrap().is_none(),
                "close admitted work against another reservation's coverage"
            );
            let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::new(1));
            assert_eq!(available, CarrierCount::new(1));
            assert_eq!(uncovered, CarrierCount::new(1));

            drop(first_acquired);
            drop(second);
        });
    }

    #[test]
    fn test_concurrent_acquisitions_consume_direct_authority_once() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let direct = Arc::clone(reservation.acquisition_state().unwrap());

            let first_pool = Arc::clone(&pool.inner);
            let first_direct = Arc::clone(&direct);
            let first = thread::spawn(move || {
                acquire_count(&first_pool, Some(first_direct), CarrierCount::new(1))
            });
            let second_pool = Arc::clone(&pool.inner);
            let second = thread::spawn(move || {
                acquire_count(&second_pool, Some(direct), CarrierCount::new(1))
            });

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
            assert!(
                matches!(first, Err(AcquireError::ReservationCapacityExceeded))
                    || matches!(second, Err(AcquireError::ReservationCapacityExceeded))
            );
            drop(first);
            drop(second);
            drop(reservation);
            let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::ZERO);
            assert_eq!(available, CarrierCount::ZERO);
            assert_eq!(uncovered, CarrierCount::ZERO);
        });
    }

    #[test]
    fn test_final_return_racing_waiter_enqueue_cannot_strand_grant() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(2);
            let returned = pool.acquire_unreserved(carrier_size).unwrap();
            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("idle-only reservation");
            let (waker, wake_count) = counting_waker();
            let mut future = pool.reserve(carrier_size);

            let returning = thread::spawn(move || drop(returned));
            let first_poll = poll_reserve(&mut future, &waker);
            returning.join().unwrap();
            let granted = match first_poll {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                Poll::Pending => match poll_reserve(&mut future, &waker) {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                    Poll::Pending => panic!("return stranded an eligible waiter"),
                },
            };

            assert!(wake_count.load(Ordering::Acquire) <= 1);
            drop(holder);
            drop(granted);
            let (_, active, available, uncovered, waiters) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::ZERO);
            assert_eq!(available, CarrierCount::ZERO);
            assert_eq!(uncovered, CarrierCount::ZERO);
            assert_eq!(waiters, 0);
        });
    }

    #[test]
    fn test_close_racing_direct_debit_rollback_cannot_reopen_authority() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let reservation = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation");
            let direct = Arc::clone(reservation.acquisition_state().unwrap());

            let debiting_pool = Arc::clone(&pool.inner);
            let debiting_direct = Arc::clone(&direct);
            let debiting = thread::spawn(move || {
                AcquisitionDebit::install(
                    &debiting_pool,
                    Some(debiting_direct),
                    CarrierCount::new(1),
                )
            });
            let closing = thread::spawn(move || reservation.close_acquisition());
            let debit = debiting.join().unwrap();
            closing.join().unwrap();

            match debit {
                Ok(debit) => {
                    assert_eq!(direct.test_owner_state(), (true, CarrierCount::new(1)));
                    let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
                    assert_eq!(active, CarrierCount::ZERO);
                    assert_eq!(available, CarrierCount::ZERO);
                    assert_eq!(uncovered, CarrierCount::new(1));
                    drop(debit);
                }
                Err(AcquireError::ReservationClosed) => {
                    assert_eq!(direct.test_owner_state(), (true, CarrierCount::ZERO));
                }
                Err(error) => panic!("unexpected acquisition debit error: {error}"),
            }

            assert_eq!(direct.test_owner_state(), (true, CarrierCount::ZERO));
            let (_, active, available, uncovered, _) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::ZERO);
            assert_eq!(available, CarrierCount::ZERO);
            assert_eq!(uncovered, CarrierCount::ZERO);
        });
    }

    #[test]
    fn test_shortfall_rollback_racing_waiter_enqueue_cannot_strand_grant() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(2);
            let debit = AcquisitionDebit::install(&pool.inner, None, CarrierCount::new(1)).unwrap();
            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("idle-only reservation");
            let (waker, wake_count) = counting_waker();
            let mut future = pool.reserve(carrier_size);

            let rolling_back = thread::spawn(move || drop(debit));
            let first_poll = poll_reserve(&mut future, &waker);
            rolling_back.join().unwrap();
            let granted = match first_poll {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                Poll::Pending => match poll_reserve(&mut future, &waker) {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                    Poll::Pending => panic!("rollback stranded an eligible waiter"),
                },
            };

            assert!(wake_count.load(Ordering::Acquire) <= 1);
            drop(holder);
            drop(granted);
            let (_, active, available, uncovered, waiters) = pool.inner.test_accounting_state();
            assert_eq!(active, CarrierCount::ZERO);
            assert_eq!(available, CarrierCount::ZERO);
            assert_eq!(uncovered, CarrierCount::ZERO);
            assert_eq!(waiters, 0);
        });
    }

    #[test]
    fn test_manager_local_teardown_preserves_escaped_owner_and_shared_pool() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(2);
            let manager_pool = pool.clone();
            let reservation = manager_pool
                .try_reserve(carrier_size * 2)
                .unwrap()
                .expect("manager reservation");
            let escaped = manager_pool.acquire(&reservation, carrier_size).unwrap();
            let (waker, _) = counting_waker();
            let mut queued = manager_pool.reserve(carrier_size);
            assert!(poll_reserve(&mut queued, &waker).is_pending());
            drop(manager_pool);

            let cancelling = thread::spawn(move || drop(queued));
            let closing = thread::spawn(move || reservation.close_acquisition());
            cancelling.join().unwrap();
            closing.join().unwrap();

            let retained = pool.metrics();
            assert_eq!(retained.active_planned_demand_bytes(), 0);
            assert_eq!(retained.charged_capacity_bytes(), carrier_size as u64);
            assert_eq!(retained.queued_reservations(), 0);

            let shared = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("shared pool remains operational");
            drop(shared);
            drop(escaped);
            let released = pool.metrics();
            assert_eq!(released.admission_used_bytes(), 0);
            assert_eq!(released.charged_capacity_bytes(), 0);
        });
    }

    #[test]
    fn test_published_shortfall_floor_blocks_trim_during_unlocked_claim() {
        loom::model(|| {
            let (pool, _) = test_pool(1);
            let debit = AcquisitionDebit::install(&pool.inner, None, CarrierCount::new(1)).unwrap();
            let candidate = pool
                .inner
                .arena
                .select_trim_candidate()
                .expect("prepared free block");

            let claiming_pool = pool.clone();
            let claiming = thread::spawn(move || {
                claiming_pool
                    .inner
                    .arena
                    .claim_optimistic(CarrierCount::new(1))
                    .unwrap()
            });
            let trimming_pool = pool.clone();
            let trimming = thread::spawn(move || {
                let mut admission = AdmissionGuard::new(trimming_pool.inner.admission.lock());
                let floor = admission
                    .acquisition_floor(&trimming_pool.inner.coverage)
                    .unwrap();
                assert!(matches!(
                    BlockSlot::start_trim(&candidate, admission.prepared_capacity_mut(), floor,),
                    Err(TrimBlocked::FloorViolation)
                ));
            });

            let claim = claiming.join().unwrap();
            trimming.join().unwrap();
            assert!(claim.is_complete());
            drop(claim);
            drop(debit);
            assert_eq!(pool.metrics().admission_used_bytes(), 0);
        });
    }
}
