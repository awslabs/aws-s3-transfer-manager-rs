/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Bounded transfer-memory admission and fixed-size pooled storage.
//!
//! [`BufferPool`] grants planned demand in FIFO order, accounts carrier
//! ownership independently of reservation lifetime, and prepares backing in
//! whole blocks. Mutable acquisitions retain exclusive carrier ownership;
//! immutable segmented values may present adjacent carriers as one byte range
//! while preserving each carrier's independent return boundary.
//!
//! Current memory gauges are always available through [`BufferPool::metrics`].
//! Pressure and lifecycle counters are updated only when exceptional allocator
//! work occurs. Per-acquisition scan accounting and periodic snapshots are
//! opt-in so the normal success path does not pay for diagnostic atomics.

use std::task::Waker;

use crate::config::MemoryDiagnosticsConfig;
use crate::runtime::sync::sync::{Arc, Mutex};

mod acquisition;
mod admission;
mod arena;
mod block;
mod config;
#[cfg(not(all(test, s3_tm_loom)))]
mod diagnostics;
mod geometry;
mod maintenance;
mod metrics;
mod pooled_buf;
mod segmented_bytes;
#[cfg(test)]
mod test_util;
mod virtual_memory;

use crate::types::MemoryBudgetConfig;
use acquisition::acquire_count;
pub use acquisition::AcquireError;
use admission::{
    wake_all, AdmissionGuard, AdmissionState, CoverageState, ReservationPoll, WaitSlot, WaitState,
    Waiter, MAX_PACKED_CARRIERS,
};
pub use admission::{Reservation, ReserveError, ReserveFuture};
use arena::{Arena, ArenaError, ArenaOptions};
use block::BlockError;
use config::PoolConfig;
pub use config::{BufferPoolBuildError, BufferPoolBuilder};
use geometry::{GeometryError, PoolGeometry};
use maintenance::MaintenanceCoordinator;
pub use metrics::MemoryMetrics;
use metrics::{MemoryDiagnostics, MemoryMetricState};
use pooled_buf::GrowthAuthority;
pub use pooled_buf::PooledBufMut;
pub use segmented_bytes::SegmentedBytes;

#[cfg(test)]
use test_util::TestHooks;

/// A cloneable handle to one admission, accounting, and storage domain.
///
/// Every clone shares the configured capacity, reservation queue, ownership
/// charges, prepared storage, maintenance state, and metrics. Constructing a
/// pool is lazy: no virtual ranges or physical memory are prepared until the
/// first admitted acquisition needs them.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool").finish_non_exhaustive()
    }
}

impl BufferPool {
    /// Returns a builder for an empty shared memory pool.
    pub fn builder() -> BufferPoolBuilder {
        BufferPoolBuilder::default()
    }

    /// Returns the fixed writable allocation unit used by this pool.
    ///
    /// Reservations and acquisitions accept arbitrary byte counts and round
    /// them to this page-aligned unit.
    pub fn carrier_size(&self) -> usize {
        self.inner.geometry.carrier_size()
    }

    /// Constructs an empty pool from capacity policy and detected memory.
    ///
    /// Capacity and geometry are fixed before publication. Construction does
    /// not reserve virtual ranges, prepare backing, or start maintenance.
    pub(crate) fn from_capacity(
        capacity: MemoryBudgetConfig,
        detected_memory: Option<usize>,
        diagnostics: MemoryDiagnosticsConfig,
    ) -> Result<Self, BufferPoolBuildError> {
        let resolved = PoolConfig::resolve(capacity, detected_memory)?;
        Ok(Self::from_parts_with_diagnostics(
            resolved.geometry,
            resolved.configured_capacity,
            resolved.optimistic_scan_words,
            diagnostics,
        )
        .unwrap_or_else(|_| invariant_violation("validated pool configuration was rejected")))
    }

    /// Constructs a pool with one already resolved diagnostic policy.
    fn from_parts_with_diagnostics(
        geometry: PoolGeometry,
        configured_capacity: CarrierCount,
        optimistic_scan_words: usize,
        diagnostics: MemoryDiagnosticsConfig,
    ) -> Result<Self, ArenaError> {
        if configured_capacity == CarrierCount::ZERO {
            invariant_violation("configured capacity must be nonzero");
        }
        if configured_capacity > MAX_PACKED_CARRIERS {
            invariant_violation("configured capacity must fit packed accounting");
        }

        let configured_capacity_bytes = configured_capacity
            .get()
            .checked_mul(geometry.carrier_size())
            .unwrap_or_else(|| invariant_violation("configured byte capacity overflowed"));
        let inner = Arc::new(PoolInner::new(
            geometry,
            configured_capacity,
            optimistic_scan_words,
            diagnostics,
        )?);
        inner.maintenance.start_periodic_diagnostics(&inner);
        tracing::debug!(
            target: crate::telemetry::TARGET_MEMORY,
            configured_capacity_bytes,
            page_size_bytes = geometry.page_size(),
            carrier_size_bytes = geometry.carrier_size(),
            block_size_bytes = geometry.block_size(),
            optimistic_scan_words,
            diagnostic_detail = diagnostics.detail_level(),
            diagnostic_snapshot_interval_ms =
                diagnostics.snapshot_interval().map(|interval| interval.as_millis()).unwrap_or(0),
            "constructed buffer pool"
        );
        Ok(Self { inner })
    }

    /// Attempts one immediate reservation grant.
    ///
    /// `Ok(None)` reports an older FIFO request or current admission pressure.
    /// A successful grant has already prepared storage through its complete
    /// admission floor.
    pub fn try_reserve(&self, bytes: usize) -> Result<Option<Reservation>, ReserveError> {
        let envelope = self.reservation_envelope(bytes)?;
        PoolInner::try_reserve_count(&self.inner, envelope)
    }

    /// Creates a lazy, cancellation-safe reservation request.
    ///
    /// The first poll either returns an immediate result or enters the
    /// pool-wide FIFO. Invalid requests and physical preparation failures
    /// resolve through the future's `ReserveError`.
    pub fn reserve(&self, bytes: usize) -> ReserveFuture {
        ReserveFuture::new(self.clone(), bytes)
    }

    /// Returns one coherent sample of this pool's memory state.
    pub fn metrics(&self) -> MemoryMetrics {
        self.inner.memory_metrics()
    }

    /// Returns operational counters and a scanned lifecycle sample.
    pub(crate) fn diagnostics(&self) -> MemoryDiagnostics {
        MemoryDiagnostics::from_samples(
            self.inner.arena.diagnostics(),
            self.inner.arena.sample_lifecycle(),
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

    /// Acquires at least `min_bytes` under one reservation.
    ///
    /// Capacity is rounded up to complete carriers. The reservation must
    /// belong to this pool, remain open, and retain enough direct-acquisition
    /// authority for the rounded request. Failure exposes no partial carrier
    /// ownership.
    pub fn acquire(
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
    pub fn acquire_unreserved(&self, min_bytes: usize) -> Result<PooledBufMut, AcquireError> {
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
    /// Lazy maintenance and optional diagnostic-reporting worker.
    maintenance: MaintenanceCoordinator,
    /// Per-pool failure injection and test-only observations.
    #[cfg(test)]
    test_hooks: TestHooks,
}

/// Reservation pressure captured while admission state is coherent.
#[derive(Clone, Copy)]
struct ReservationQueueSample {
    /// Empty-boundary transition and any data specific to it.
    event: ReservationQueueEvent,
    /// Requests retained in FIFO order at this transition.
    queued_reservations: usize,
    /// Pool-lifetime requests that have entered the FIFO.
    reservation_enqueues_total: u64,
    /// Planned demand and uncovered ownership after this enqueue.
    admission_used: CarrierCount,
    /// Normal admission ceiling.
    configured_capacity: CarrierCount,
}

/// A reservation FIFO crossing its empty boundary.
#[derive(Clone, Copy)]
enum ReservationQueueEvent {
    /// The FIFO became non-empty for this request.
    BecameNonempty { requested: CarrierCount },
    /// The FIFO became empty.
    BecameEmpty,
}

/// FIFO work and an optional empty-boundary transition captured under admission.
struct ReservationDrain {
    wakers: Vec<Waker>,
    queue_sample: Option<ReservationQueueSample>,
}

impl PoolInner {
    /// Constructs an empty pool domain without reserving virtual ranges.
    fn new(
        geometry: PoolGeometry,
        configured_capacity: CarrierCount,
        optimistic_scan_words: usize,
        diagnostics: MemoryDiagnosticsConfig,
    ) -> Result<Self, ArenaError> {
        Ok(Self {
            geometry,
            admission: Mutex::new(AdmissionState::new(configured_capacity)),
            coverage: CoverageState::new(),
            arena: Arena::new(
                geometry,
                ArenaOptions::new(
                    optimistic_scan_words,
                    diagnostics.enable_detailed_counters(),
                ),
            )?,
            maintenance: MaintenanceCoordinator::new(
                configured_capacity,
                geometry,
                diagnostics.snapshot_interval(),
            ),
            #[cfg(test)]
            test_hooks: TestHooks::new(),
        })
    }

    /// Returns one coherent admission and aggregate memory sample.
    fn memory_metrics(&self) -> MemoryMetrics {
        let admission = self.admission.lock();
        self.memory_metrics_locked(&admission)
    }

    /// Takes memory and reservation-queue state at one diagnostic boundary.
    fn diagnostic_memory_sample(
        &self,
        now: std::time::Instant,
    ) -> (MemoryMetrics, admission::ReservationQueueDiagnosticSample) {
        let mut admission = self.admission.lock();
        let queue = admission.take_queue_diagnostics(now);
        let metrics = self.memory_metrics_locked(&admission);
        (metrics, queue)
    }

    /// Builds public memory metrics while admission remains coherent.
    fn memory_metrics_locked(&self, admission: &AdmissionState) -> MemoryMetrics {
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
                queued_reservations: admission.waiter_count(),
                reservation_enqueues_total: admission.reservation_enqueues_total(),
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
        if !admission.inner.waiters_is_empty() {
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
        #[cfg(test)]
        if let Some(error) = pool.test_hooks.take_reservation_failure() {
            return Err(error);
        }
        if envelope == CarrierCount::ZERO {
            return Err(ReserveError::InvalidSize);
        }
        if envelope > MAX_PACKED_CARRIERS {
            return Err(ReserveError::CapacityOverflow);
        }

        let mut admission = AdmissionGuard::new(pool.admission.lock());
        let coverage = pool.coverage.snapshot();
        if admission.inner.waiters_is_empty() && admission.can_grant(coverage, envelope) {
            return Self::prepare_and_grant_locked(pool, &mut admission, envelope)
                .map(ReservationPoll::Ready);
        }

        let slot = Arc::new(WaitSlot::new(waker));
        let queue_became_nonempty = admission
            .inner
            .enqueue_waiter(
                Waiter {
                    envelope,
                    slot: Arc::clone(&slot),
                },
                std::time::Instant::now(),
            )
            .map_err(|_| ReserveError::MetadataAllocationFailed)?;
        let queue_sample = queue_became_nonempty
            .then(|| {
                Self::reservation_queue_sample(
                    &admission,
                    &pool.coverage,
                    ReservationQueueEvent::BecameNonempty {
                        requested: envelope,
                    },
                )
            })
            .flatten();
        drop(admission);
        if let Some(sample) = queue_sample {
            pool.log_reservation_queue_transition(sample);
        }
        Ok(ReservationPoll::Queued(slot))
    }

    /// Removes one cancelled queue link and reconsiders the exposed head.
    fn cancel_waiter(pool: &Arc<Self>, slot: &Arc<WaitSlot>) -> Vec<Waker> {
        let mut admission = AdmissionGuard::new(pool.admission.lock());
        let queue_became_empty = admission
            .inner
            .remove_waiter(slot, std::time::Instant::now());
        let drained = Self::drain_fifo_locked(pool, &mut admission, queue_became_empty);
        drop(admission);
        if let Some(sample) = drained.queue_sample {
            pool.log_reservation_queue_transition(sample);
        }
        drained.wakers
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
    fn drain_fifo_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionGuard<'_>,
        mut queue_became_empty: bool,
    ) -> ReservationDrain {
        let mut wakers = Vec::new();
        while let Some(front) = admission.inner.front_waiter() {
            let envelope = front.envelope;
            let slot = Arc::clone(&front.slot);
            let mut slot_state = slot.state.lock();

            match &*slot_state {
                WaitState::Taken => {
                    let (_, cleared) = admission.inner.pop_front_waiter(std::time::Instant::now());
                    queue_became_empty |= cleared;
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

            let (removed, cleared) = admission.inner.pop_front_waiter(std::time::Instant::now());
            queue_became_empty |= cleared;
            let Some(removed) = removed else {
                invariant_violation("reservation head disappeared while admission was held");
            };
            if !Arc::ptr_eq(&removed.slot, &slot) {
                invariant_violation("FIFO head changed while admission was held");
            }
            wakers.push(waker);
        }
        let queue_sample = queue_became_empty
            .then(|| {
                Self::reservation_queue_sample(
                    admission,
                    &pool.coverage,
                    ReservationQueueEvent::BecameEmpty,
                )
            })
            .flatten();
        ReservationDrain {
            wakers,
            queue_sample,
        }
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

        let drained = {
            let mut admission = AdmissionGuard::new(pool.admission.lock());
            let drained = Self::drain_fifo_locked(pool, &mut admission, false);
            admission
                .inner
                .ledger
                .assert_invariants(pool.coverage.snapshot());
            drained
        };
        if let Some(sample) = drained.queue_sample {
            pool.log_reservation_queue_transition(sample);
        }
        wake_all(drained.wakers);
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

    /// Captures one reservation-queue transition while admission is coherent.
    fn reservation_queue_sample(
        admission: &AdmissionGuard<'_>,
        coverage: &CoverageState,
        event: ReservationQueueEvent,
    ) -> Option<ReservationQueueSample> {
        let admission_used = admission
            .inner
            .ledger
            .admission_used(coverage.snapshot())
            .ok()?;
        Some(ReservationQueueSample {
            event,
            queued_reservations: admission.inner.waiter_count(),
            reservation_enqueues_total: admission.inner.reservation_enqueues_total(),
            admission_used,
            configured_capacity: admission.inner.ledger.configured_capacity,
        })
    }

    /// Logs one reservation FIFO empty-boundary transition.
    fn log_reservation_queue_transition(&self, sample: ReservationQueueSample) {
        if !tracing::enabled!(
            target: crate::telemetry::TARGET_MEMORY,
            tracing::Level::DEBUG
        ) {
            return;
        }

        match sample.event {
            ReservationQueueEvent::BecameNonempty { requested } => tracing::debug!(
                target: crate::telemetry::TARGET_MEMORY,
                requested_bytes = self.carriers_to_bytes(requested),
                queued_reservations = sample.queued_reservations,
                reservation_enqueues_total = sample.reservation_enqueues_total,
                admission_used_bytes = self.carriers_to_bytes(sample.admission_used),
                configured_capacity_bytes = self.carriers_to_bytes(sample.configured_capacity),
                "buffer-pool reservation queue became non-empty"
            ),
            ReservationQueueEvent::BecameEmpty => tracing::debug!(
                target: crate::telemetry::TARGET_MEMORY,
                queued_reservations = sample.queued_reservations,
                reservation_enqueues_total = sample.reservation_enqueues_total,
                admission_used_bytes = self.carriers_to_bytes(sample.admission_used),
                configured_capacity_bytes = self.carriers_to_bytes(sample.configured_capacity),
                "buffer-pool reservation queue became empty"
            ),
        }
    }

    /// Converts one checked carrier count to its byte representation.
    fn carriers_to_bytes(&self, count: CarrierCount) -> u64 {
        let count = u64::try_from(count.get()).unwrap_or(u64::MAX);
        let carrier_size = u64::try_from(self.geometry.carrier_size()).unwrap_or(u64::MAX);
        count.saturating_mul(carrier_size)
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
