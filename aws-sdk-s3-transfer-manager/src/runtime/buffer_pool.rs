/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Elastic admission and fixed-size pooled storage.

use std::task::Waker;

use crate::runtime::sync::sync::{Arc, Mutex};

mod acquisition;
mod admission;
mod arena;
mod block;
mod config;
mod geometry;
mod maintenance;
mod metrics;
mod pooled_buf;
mod segmented_bytes;
#[cfg(test)]
mod test_util;
mod virtual_memory;

use acquisition::acquire_count;
pub(crate) use acquisition::AcquireError;
use admission::{
    wake_all, AdmissionGuard, AdmissionState, CoverageState, ReservationPoll, WaitSlot, WaitState,
    Waiter, MAX_PACKED_CARRIERS,
};
pub(crate) use admission::{Reservation, ReserveError, ReserveFuture};
use arena::{Arena, ArenaError};
use block::BlockError;
use config::ResolvedPoolConfig;
pub(crate) use config::{BufferPoolBuildError, MemoryCapacity};
use geometry::{GeometryError, PoolGeometry};
use maintenance::MaintenanceCoordinator;
use metrics::{MemoryDiagnostics, MemoryMetricState, MemoryMetrics};
use pooled_buf::GrowthAuthority;
pub(crate) use pooled_buf::PooledBufMut;
pub(crate) use segmented_bytes::SegmentedBytes;

#[cfg(test)]
use test_util::TestHooks;

/// A cloneable handle to one admission, accounting, and storage domain.
#[derive(Clone)]
pub(crate) struct BufferPool {
    inner: Arc<PoolInner>,
}

impl BufferPool {
    /// Constructs an empty pool from capacity policy and detected memory.
    ///
    /// Capacity and geometry are fixed before publication. Construction does
    /// not reserve virtual ranges, prepare backing, or start maintenance.
    pub(crate) fn from_capacity(
        capacity: MemoryCapacity,
        detected_memory: Option<usize>,
    ) -> Result<Self, BufferPoolBuildError> {
        let resolved = ResolvedPoolConfig::resolve(capacity, detected_memory)?;
        Ok(Self::from_parts(
            resolved.geometry,
            resolved.configured_capacity,
            resolved.optimistic_scan_words,
        )
        .unwrap_or_else(|_| invariant_violation("validated pool configuration was rejected")))
    }

    /// Constructs a pool from checked geometry and internal configuration.
    ///
    /// Invalid capacity indicates an internal configuration defect. Public
    /// configuration validates these values before reaching this boundary.
    /// Construction reserves no blocks and prepares no physical capacity.
    fn from_parts(
        geometry: PoolGeometry,
        configured_capacity: CarrierCount,
        optimistic_scan_words: usize,
    ) -> Result<Self, ArenaError> {
        if configured_capacity == CarrierCount::ZERO {
            invariant_violation("configured capacity must be nonzero");
        }
        if configured_capacity > MAX_PACKED_CARRIERS {
            invariant_violation("configured capacity must fit packed accounting");
        }

        Ok(Self {
            inner: Arc::new(PoolInner::new(
                geometry,
                configured_capacity,
                optimistic_scan_words,
            )?),
        })
    }

    /// Attempts one immediate reservation grant.
    ///
    /// `Ok(None)` reports an older FIFO request or current admission pressure.
    /// A successful grant has already prepared storage through its complete
    /// admission floor.
    pub(crate) fn try_reserve(&self, bytes: usize) -> Result<Option<Reservation>, ReserveError> {
        let envelope = self.reservation_envelope(bytes)?;
        PoolInner::try_reserve_count(&self.inner, envelope)
    }

    /// Creates a lazy, cancellation-safe reservation request.
    ///
    /// The first poll either returns an immediate result or enters the
    /// pool-wide FIFO. Invalid requests and physical preparation failures
    /// resolve through the future's `ReserveError`.
    pub(crate) fn reserve(&self, bytes: usize) -> ReserveFuture {
        ReserveFuture::new(self.clone(), bytes)
    }

    /// Returns one coherent sample of this pool's memory state.
    pub(crate) fn metrics(&self) -> MemoryMetrics {
        self.inner.memory_metrics()
    }

    /// Returns operational counters and a scanned lifecycle sample.
    pub(crate) fn diagnostics(&self) -> MemoryDiagnostics {
        MemoryDiagnostics::from_snapshots(
            self.inner.arena.diagnostics(),
            self.inner.arena.lifecycle_snapshot(),
            self.inner.maintenance.diagnostics(),
        )
    }

    /// Invalidates pending idle reclamation when managed work starts.
    pub(crate) fn record_managed_activity(&self) {
        self.inner.maintenance.record_activity();
    }

    /// Arms idle reclamation after the scheduler becomes globally idle.
    pub(crate) fn record_global_idle(&self) {
        self.inner.maintenance.record_idle(&self.inner);
    }

    /// Returns the fixed writable allocation unit in bytes.
    pub(crate) fn carrier_size(&self) -> usize {
        self.inner.geometry.carrier_size()
    }

    /// Acquires at least `min_bytes` under one reservation.
    ///
    /// Capacity is rounded up to complete carriers. The reservation must
    /// belong to this pool, remain open, and retain enough direct-acquisition
    /// authority for the rounded request. Failure exposes no partial carrier
    /// ownership.
    pub(crate) fn acquire(
        &self,
        reservation: &Reservation,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError> {
        let count = self.acquisition_count(min_bytes)?;
        let direct = reservation
            .acquisition_state()
            .ok_or(AcquireError::ReservationClosed)?;
        if !direct.belongs_to(&self.inner) {
            return Err(AcquireError::ForeignReservation);
        }
        let direct = Arc::clone(direct);
        let guards = acquire_count(&self.inner, Some(Arc::clone(&direct)), count)?;
        PooledBufMut::try_new(GrowthAuthority::reserved(direct), guards)
    }

    /// Acquires at least `min_bytes` without reservation-local authority.
    ///
    /// Capacity is rounded up to complete carriers. The request still
    /// participates in aggregate accounting and may prepare physical capacity
    /// or raise admission above its normal configured ceiling. Failure exposes
    /// no partial carrier ownership.
    pub(crate) fn acquire_unreserved(
        &self,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError> {
        let count = self.acquisition_count(min_bytes)?;
        let guards = acquire_count(&self.inner, None, count)?;
        PooledBufMut::try_new(GrowthAuthority::unreserved(Arc::clone(&self.inner)), guards)
    }

    /// Converts one public byte request to its checked carrier envelope.
    fn reservation_envelope(&self, bytes: usize) -> Result<CarrierCount, ReserveError> {
        let envelope = self
            .inner
            .geometry
            .carriers_for_bytes(bytes)
            .map_err(map_reservation_geometry_error)?;
        if envelope > MAX_PACKED_CARRIERS {
            return Err(ReserveError::CapacityOverflow);
        }
        Ok(envelope)
    }

    /// Converts one acquisition request to its complete carrier count.
    fn acquisition_count(&self, min_bytes: usize) -> Result<CarrierCount, AcquireError> {
        self.inner
            .geometry
            .carriers_for_bytes(min_bytes)
            .map_err(|error| match error {
                GeometryError::ZeroByteRequest => AcquireError::InvalidSize,
                _ => AcquireError::CapacityOverflow,
            })
    }
}

/// Shared state retained by pool handles, reservations, and carrier owners.
struct PoolInner {
    /// Immutable page, block, carrier, and bitmap dimensions.
    geometry: PoolGeometry,
    /// Planned-demand policy and prepared-capacity serialization.
    admission: Mutex<AdmissionState>,
    /// Aggregate charges updated by carrier acquisition and final return.
    coverage: CoverageState,
    /// Stable virtual ranges and physical carrier ownership.
    arena: Arena,
    /// Lazy idle reclamation and mapping-cleanup worker.
    maintenance: MaintenanceCoordinator,
    /// Per-pool failure injection and test-only observations.
    #[cfg(test)]
    test_hooks: TestHooks,
}

impl PoolInner {
    /// Constructs an empty pool domain without reserving virtual ranges.
    fn new(
        geometry: PoolGeometry,
        configured_capacity: CarrierCount,
        optimistic_scan_words: usize,
    ) -> Result<Self, ArenaError> {
        Ok(Self {
            geometry,
            admission: Mutex::new(AdmissionState::new(configured_capacity)),
            coverage: CoverageState::new(),
            arena: Arena::new(geometry, optimistic_scan_words)?,
            maintenance: MaintenanceCoordinator::new(
                configured_capacity,
                geometry.carriers_per_block(),
            ),
            #[cfg(test)]
            test_hooks: TestHooks::new(),
        })
    }

    /// Returns one coherent admission and aggregate memory sample.
    fn memory_metrics(&self) -> MemoryMetrics {
        let admission = self.admission.lock();
        let coverage = self.coverage.snapshot();
        admission.ledger.assert_invariants(coverage);

        let admission_used = admission
            .ledger
            .admission_used(coverage)
            .unwrap_or_else(|_| invariant_violation("completed admission exceeds packed capacity"));
        let covered_charges = admission
            .ledger
            .active_planned_demand
            .checked_sub(coverage.available)
            .unwrap_or_else(|| {
                invariant_violation("available coverage exceeds active planned demand")
            });
        let charged_capacity = covered_charges
            .checked_add(coverage.uncovered)
            .unwrap_or_else(|| invariant_violation("completed charges exceed packed capacity"));

        MemoryMetrics::from_carriers(
            self.geometry.carrier_size(),
            MemoryMetricState {
                configured_capacity: admission.ledger.configured_capacity,
                admission_used,
                active_planned_demand: admission.ledger.active_planned_demand,
                charged_capacity,
                prepared_capacity: admission.ledger.prepared_capacity,
                queued_reservations: admission.waiters.len(),
                parked_reservations_total: admission.parked_reservations_total,
            },
        )
    }

    /// Attempts an aggregate debit without admission serialization.
    ///
    /// `Ok(false)` leaves accounting unchanged and requires the caller to use
    /// the serialized shortfall path.
    fn try_debit_covered(&self, count: CarrierCount) -> Result<bool, ReserveError> {
        self.coverage
            .try_debit_covered(count)
            .map_err(|_| ReserveError::CapacityOverflow)
    }

    /// Publishes a shortfall debit and prepares through its admission floor.
    ///
    /// Failure reverses the aggregate debit before returning. The caller
    /// remains responsible for rolling back reservation-local authority.
    fn debit_and_prepare_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionGuard<'_>,
        count: CarrierCount,
    ) -> Result<(), ReserveError> {
        pool.coverage
            .debit(count, admission.maximum_uncovered())
            .map_err(|_| ReserveError::CapacityOverflow)?;

        let floor = match admission.acquisition_floor(&pool.coverage) {
            Ok(floor) => floor,
            Err(error) => {
                admission.rollback_acquisition(&pool.coverage, count);
                return Err(error);
            }
        };
        if let Err(error) = pool.arena.prepare_to(admission, floor) {
            admission.rollback_acquisition(&pool.coverage, count);
            Self::request_cleanup_after_arena_error(pool, &error);
            return Err(map_preparation_error(error));
        }
        Ok(())
    }

    /// Attempts one immediate grant without bypassing the FIFO.
    fn try_reserve_count(
        pool: &Arc<Self>,
        envelope: CarrierCount,
    ) -> Result<Option<Reservation>, ReserveError> {
        if envelope == CarrierCount::ZERO {
            return Err(ReserveError::InvalidSize);
        }
        if envelope > MAX_PACKED_CARRIERS {
            return Err(ReserveError::CapacityOverflow);
        }

        let mut admission = AdmissionGuard::new(pool.admission.lock());
        if !admission.inner.waiters.is_empty() {
            return Ok(None);
        }
        let coverage = pool.coverage.snapshot();
        if !admission.can_grant(coverage, envelope) {
            return Ok(None);
        }

        Self::prepare_and_grant_locked(pool, &mut admission, envelope).map(Some)
    }

    /// Grants one first-poll request or links it behind existing work.
    fn reserve_or_enqueue(
        pool: &Arc<Self>,
        envelope: CarrierCount,
        waker: Waker,
    ) -> Result<ReservationPoll, ReserveError> {
        if envelope == CarrierCount::ZERO {
            return Err(ReserveError::InvalidSize);
        }
        if envelope > MAX_PACKED_CARRIERS {
            return Err(ReserveError::CapacityOverflow);
        }

        let mut admission = AdmissionGuard::new(pool.admission.lock());
        let coverage = pool.coverage.snapshot();
        if admission.inner.waiters.is_empty() && admission.can_grant(coverage, envelope) {
            return Self::prepare_and_grant_locked(pool, &mut admission, envelope)
                .map(ReservationPoll::Ready);
        }

        admission
            .inner
            .waiters
            .try_reserve(1)
            .map_err(|_| ReserveError::MetadataAllocationFailed)?;
        let slot = Arc::new(WaitSlot::new(waker));
        admission.inner.waiters.push_back(Waiter {
            envelope,
            slot: Arc::clone(&slot),
        });
        admission.inner.parked_reservations_total =
            admission.inner.parked_reservations_total.saturating_add(1);
        Ok(ReservationPoll::Queued(slot))
    }

    /// Removes one cancelled queue link and reconsiders the exposed head.
    fn cancel_waiter(pool: &Arc<Self>, slot: &Arc<WaitSlot>) -> Vec<Waker> {
        let mut admission = AdmissionGuard::new(pool.admission.lock());
        if let Some(index) = admission
            .inner
            .waiters
            .iter()
            .position(|waiter| Arc::ptr_eq(&waiter.slot, slot))
        {
            admission.inner.waiters.remove(index);
        }
        Self::drain_fifo_locked(pool, &mut admission)
    }

    /// Prepares and publishes one grant while admission is serialized.
    fn prepare_and_grant_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionGuard<'_>,
        envelope: CarrierCount,
    ) -> Result<Reservation, ReserveError> {
        let coverage = pool.coverage.snapshot();
        let target = admission.grant_target(coverage, envelope)?;
        if let Err(error) = pool.arena.prepare_to(admission, target) {
            Self::request_cleanup_after_arena_error(pool, &error);
            return Err(map_preparation_error(error));
        }
        admission.commit_grant(&pool.coverage, envelope)?;
        Ok(Reservation::new(Arc::clone(pool), envelope))
    }

    /// Transfers every eligible FIFO head and returns wakers for post-unlock use.
    fn drain_fifo_locked(pool: &Arc<Self>, admission: &mut AdmissionGuard<'_>) -> Vec<Waker> {
        let mut wakers = Vec::new();
        while let Some(front) = admission.inner.waiters.front() {
            let envelope = front.envelope;
            let slot = Arc::clone(&front.slot);
            let mut slot_state = slot.state.lock();

            match &*slot_state {
                WaitState::Taken => {
                    admission.inner.waiters.pop_front();
                    continue;
                }
                WaitState::Queued { .. } => {}
                WaitState::Granted(_) | WaitState::Failed(_) => {
                    invariant_violation("terminal reservation result remained linked in the FIFO");
                }
            }

            let coverage = pool.coverage.snapshot();
            if !admission.can_grant(coverage, envelope) {
                break;
            }

            let result = Self::prepare_and_grant_locked(pool, admission, envelope);
            let previous = std::mem::replace(&mut *slot_state, WaitState::Taken);
            let WaitState::Queued { waker } = previous else {
                invariant_violation("reservation head changed while its slot lock was held");
            };
            *slot_state = match result {
                Ok(reservation) => WaitState::Granted(reservation),
                Err(error) => WaitState::Failed(error),
            };

            let Some(removed) = admission.inner.waiters.pop_front() else {
                invariant_violation("reservation head disappeared while admission was held");
            };
            if !Arc::ptr_eq(&removed.slot, &slot) {
                invariant_violation("FIFO head changed while admission was held");
            }
            wakers.push(waker);
        }
        wakers
    }

    /// Retires aggregate charges and reconsiders work exposed by repayment.
    ///
    /// Physical ownership and direct provenance are returned before this
    /// boundary. Coverage-only returns stay lock-free. Repayment of uncovered
    /// charges enters admission and invokes wakers only after unlocking.
    fn release_acquisition_charges(pool: &Arc<Self>, count: CarrierCount) {
        let returned = pool.coverage.release(count);
        if returned.uncovered_removed == CarrierCount::ZERO {
            return;
        }

        let wakers = {
            let mut admission = AdmissionGuard::new(pool.admission.lock());
            let wakers = Self::drain_fifo_locked(pool, &mut admission);
            admission
                .inner
                .ledger
                .assert_invariants(pool.coverage.snapshot());
            wakers
        };
        wake_all(wakers);
    }

    /// Schedules recovery when block preparation leaves a slot nonclaimable.
    fn request_cleanup_after_arena_error(pool: &Arc<Self>, error: &ArenaError) {
        if !error.cleanup_required() {
            return;
        }
        tracing::warn!(
            target: crate::telemetry::TARGET_MEMORY,
            error = %error,
            "buffer-pool preparation failed; mapping cleanup was scheduled"
        );
        pool.maintenance.request_cleanup(pool);
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        self.maintenance.shutdown();
    }
}

fn map_reservation_geometry_error(error: GeometryError) -> ReserveError {
    match error {
        GeometryError::ZeroByteRequest => ReserveError::InvalidSize,
        _ => invariant_violation("checked geometry rejected byte-to-carrier conversion"),
    }
}

fn map_preparation_error(error: ArenaError) -> ReserveError {
    match error {
        ArenaError::Block(BlockError::PreparedCapacityOverflow)
        | ArenaError::SlotIdExhausted
        | ArenaError::RegistryCapacityOverflow
        | ArenaError::AddressOverflow { .. }
        | ArenaError::ScanSpaceOverflow { .. } => ReserveError::CapacityOverflow,
        ArenaError::Block(BlockError::Allocation(_)) | ArenaError::Allocation(_) => {
            ReserveError::MetadataAllocationFailed
        }
        ArenaError::Block(_)
        | ArenaError::InvalidScanBudget
        | ArenaError::InvalidClaimCount
        | ArenaError::IncompleteClaim { .. }
        | ArenaError::AddressOverlap { .. } => ReserveError::PhysicalPreparationFailed,
    }
}

/// A count of fixed-size carriers.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct CarrierCount(usize);

impl CarrierCount {
    /// No carriers.
    const ZERO: Self = Self(0);

    /// Wraps a carrier count.
    const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Returns the underlying count.
    fn get(self) -> usize {
        self.0
    }

    /// Adds two counts, returning `None` on overflow.
    fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    /// Subtracts two counts, returning `None` on underflow.
    fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    /// Converts this count to the packed accounting lane width.
    fn try_as_u32(self) -> Option<u32> {
        u32::try_from(self.0).ok()
    }
}

/// Stops execution after an internal buffer-pool invariant fails.
#[cold]
#[track_caller]
fn invariant_violation(message: &'static str) -> ! {
    tracing::error!(
        target: crate::telemetry::TARGET_MEMORY,
        reason = message,
        "buffer-pool invariant violated; aborting"
    );

    #[cfg(test)]
    panic!("buffer-pool invariant violated: {message}");

    #[cfg(not(test))]
    {
        let _ = message;
        std::process::abort()
    }
}
