/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Planned-demand admission, FIFO waiters, and acquisition accounting.
//!
//! Aggregate coverage and reservation-local authority are independent state
//! machines. Aggregate coverage determines how an acquisition is charged to
//! the pool. Reservation-local authority determines whether one admitted work
//! item may make that acquisition.

use std::collections::VecDeque;
use std::fmt;
use std::task::Waker;
use std::time::{Duration, Instant};

use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering};
use crate::runtime::sync::sync::{Arc, MutexGuard};

use super::{invariant_violation, CarrierCount, PoolInner};

/// Closed bit in [`ReservationOwnerState::packed`].
const RESERVATION_CLOSED: u64 = 1 << 63;

/// Direct owners and in-flight debits in [`ReservationOwnerState::packed`].
const DIRECT_OUTSTANDING_MASK: u64 = !RESERVATION_CLOSED;

mod coverage;
use coverage::CoverageSnapshot;
pub(super) use coverage::{CoverageState, MAX_PACKED_CARRIERS};

mod waiter;
pub use waiter::ReserveFuture;
pub(super) use waiter::{ReservationPoll, WaitSlot, WaitState, Waiter};

/// Planned-demand state protected by the pool admission mutex.
pub(super) struct AdmissionState {
    /// Counters participating in grant and preparation decisions.
    pub(super) ledger: AdmissionLedger,
    /// Reservation requests and queue observations retained in strict arrival order.
    queue: ReservationQueue,
}

impl AdmissionState {
    /// Creates empty admission state with one immutable normal ceiling.
    pub(super) fn new(configured_capacity: CarrierCount) -> Self {
        Self {
            ledger: AdmissionLedger {
                configured_capacity,
                prepared_capacity: CarrierCount::ZERO,
                active_planned_demand: CarrierCount::ZERO,
            },
            queue: ReservationQueue::new(Instant::now()),
        }
    }

    /// Returns whether no reservation request is queued.
    pub(super) fn waiters_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns the current reservation FIFO depth.
    pub(super) fn waiter_count(&self) -> usize {
        self.queue.len()
    }

    /// Returns the reservation request at the FIFO head.
    pub(super) fn front_waiter(&self) -> Option<&Waiter> {
        self.queue.front()
    }

    /// Appends one request after reserving its FIFO metadata.
    ///
    /// Returns whether the queue changed from empty to non-empty.
    pub(super) fn enqueue_waiter(
        &mut self,
        waiter: Waiter,
        now: Instant,
    ) -> Result<bool, std::collections::TryReserveError> {
        self.queue.enqueue(waiter, now)
    }

    /// Removes a reservation request and returns whether pressure cleared.
    pub(super) fn remove_waiter(&mut self, slot: &Arc<WaitSlot>, now: Instant) -> bool {
        self.queue.remove(slot, now)
    }

    /// Removes the FIFO head and returns whether pressure cleared.
    pub(super) fn pop_front_waiter(&mut self, now: Instant) -> (Option<Waiter>, bool) {
        self.queue.pop_front(now)
    }

    /// Takes interval queue statistics without changing the FIFO.
    pub(super) fn take_queue_diagnostics(
        &mut self,
        now: Instant,
    ) -> ReservationQueueDiagnosticSample {
        self.queue.take_diagnostics(now)
    }

    /// Returns the cumulative requests that entered the FIFO.
    pub(super) fn reservation_enqueues_total(&self) -> u64 {
        self.queue.reservation_enqueues_total
    }

    /// Sets the cumulative enqueue count for saturation testing.
    #[cfg(test)]
    pub(super) fn set_reservation_enqueues_total_for_test(&mut self, total: u64) {
        self.queue.reservation_enqueues_total = total;
    }
}

/// Reservation FIFO and observations derived from its mutation order.
struct ReservationQueue {
    waiters: VecDeque<Waiter>,
    diagnostics: ReservationQueueDiagnostics,
    reservation_enqueues_total: u64,
}

impl ReservationQueue {
    /// Creates an empty queue and diagnostic interval.
    fn new(now: Instant) -> Self {
        Self {
            waiters: VecDeque::new(),
            diagnostics: ReservationQueueDiagnostics::new(now),
            reservation_enqueues_total: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    fn len(&self) -> usize {
        self.waiters.len()
    }

    fn front(&self) -> Option<&Waiter> {
        self.waiters.front()
    }

    /// Reserves metadata, appends one request, and records its queue transition.
    fn enqueue(
        &mut self,
        waiter: Waiter,
        now: Instant,
    ) -> Result<bool, std::collections::TryReserveError> {
        self.waiters.try_reserve(1)?;
        self.waiters.push_back(waiter);
        self.reservation_enqueues_total = self.reservation_enqueues_total.saturating_add(1);
        Ok(matches!(
            self.diagnostics.record_depth(self.waiters.len(), now),
            Some(ReservationQueueTransition::BecameNonempty)
        ))
    }

    /// Removes a linked request and reports the nonempty-to-empty edge.
    fn remove(&mut self, slot: &Arc<WaitSlot>, now: Instant) -> bool {
        let Some(index) = self
            .waiters
            .iter()
            .position(|waiter| Arc::ptr_eq(&waiter.slot, slot))
        else {
            return false;
        };
        self.waiters.remove(index);
        matches!(
            self.diagnostics.record_depth(self.waiters.len(), now),
            Some(ReservationQueueTransition::BecameEmpty)
        )
    }

    /// Removes the oldest request and reports the nonempty-to-empty edge.
    fn pop_front(&mut self, now: Instant) -> (Option<Waiter>, bool) {
        let waiter = self.waiters.pop_front();
        let cleared = waiter.is_some()
            && matches!(
                self.diagnostics.record_depth(self.waiters.len(), now),
                Some(ReservationQueueTransition::BecameEmpty)
            );
        (waiter, cleared)
    }

    fn take_diagnostics(&mut self, now: Instant) -> ReservationQueueDiagnosticSample {
        self.diagnostics.take(self.waiters.len(), now)
    }
}

/// A reservation FIFO crossing its empty boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationQueueTransition {
    /// The FIFO changed from empty to non-empty.
    BecameNonempty,
    /// The FIFO changed from non-empty to empty.
    BecameEmpty,
}

/// Queue occupancy accumulated since the preceding diagnostic snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReservationQueueDiagnosticSample {
    /// Requests retained in FIFO order at the snapshot boundary.
    pub(super) current_depth: usize,
    /// Largest FIFO depth observed during the interval.
    pub(super) peak_depth: usize,
    /// Time during the interval for which the FIFO was non-empty.
    pub(super) nonempty_duration: Duration,
    /// Duration of the current uninterrupted non-empty interval.
    pub(super) continuous_nonempty_duration: Option<Duration>,
}

/// Queue timing and high-water state protected by admission serialization.
struct ReservationQueueDiagnostics {
    current_depth: usize,
    peak_depth: usize,
    nonempty_duration: Duration,
    became_nonempty_at: Option<Instant>,
    last_observed_at: Instant,
}

impl ReservationQueueDiagnostics {
    /// Creates an empty interval at pool construction.
    fn new(now: Instant) -> Self {
        Self {
            current_depth: 0,
            peak_depth: 0,
            nonempty_duration: Duration::ZERO,
            became_nonempty_at: None,
            last_observed_at: now,
        }
    }

    /// Accounts elapsed non-empty time before changing the observed depth.
    fn record_depth(&mut self, depth: usize, now: Instant) -> Option<ReservationQueueTransition> {
        self.accrue(now);
        let transition = match (self.current_depth == 0, depth == 0) {
            (true, false) => {
                self.became_nonempty_at = Some(now);
                Some(ReservationQueueTransition::BecameNonempty)
            }
            (false, true) => {
                self.became_nonempty_at = None;
                Some(ReservationQueueTransition::BecameEmpty)
            }
            _ => None,
        };
        self.current_depth = depth;
        self.peak_depth = self.peak_depth.max(depth);
        transition
    }

    /// Returns and resets interval statistics at one coherent queue depth.
    fn take(&mut self, depth: usize, now: Instant) -> ReservationQueueDiagnosticSample {
        let _ = self.record_depth(depth, now);
        let sample = ReservationQueueDiagnosticSample {
            current_depth: depth,
            peak_depth: self.peak_depth,
            nonempty_duration: self.nonempty_duration,
            continuous_nonempty_duration: self
                .became_nonempty_at
                .map(|started| now.saturating_duration_since(started)),
        };
        self.peak_depth = depth;
        self.nonempty_duration = Duration::ZERO;
        sample
    }

    /// Adds elapsed non-empty time through `now`.
    fn accrue(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_observed_at);
        if self.current_depth != 0 {
            self.nonempty_duration = self.nonempty_duration.saturating_add(elapsed);
        }
        self.last_observed_at = now;
    }
}

/// Proof that one operation owns admission serialization.
pub(super) struct AdmissionGuard<'a> {
    pub(super) inner: MutexGuard<'a, AdmissionState>,
}

impl<'a> AdmissionGuard<'a> {
    /// Wraps the mutex guard that establishes admission serialization.
    pub(super) fn new(inner: MutexGuard<'a, AdmissionState>) -> Self {
        Self { inner }
    }

    /// Returns prepared capacity.
    pub(super) fn prepared_capacity(&self) -> CarrierCount {
        self.inner.ledger.prepared_capacity
    }

    /// Returns exclusive prepared-capacity access under admission.
    pub(super) fn prepared_capacity_mut(&mut self) -> &mut CarrierCount {
        &mut self.inner.ledger.prepared_capacity
    }

    /// Returns whether one fresh request is immediately eligible.
    pub(super) fn can_grant(&self, coverage: CoverageSnapshot, envelope: CarrierCount) -> bool {
        self.inner.ledger.can_grant(coverage, envelope)
    }

    /// Computes and validates the post-grant admission floor.
    pub(super) fn grant_target(
        &self,
        coverage: CoverageSnapshot,
        envelope: CarrierCount,
    ) -> Result<CarrierCount, ReserveError> {
        self.inner.ledger.grant_target(coverage, envelope)
    }

    /// Publishes one prepared grant.
    pub(super) fn commit_grant(
        &mut self,
        coverage: &CoverageState,
        envelope: CarrierCount,
    ) -> Result<(), ReserveError> {
        self.inner.ledger.commit_grant(coverage, envelope)
    }

    /// Retires one open envelope without releasing its carrier owners.
    pub(super) fn close_envelope(
        &mut self,
        coverage: &CoverageState,
        envelope: CarrierCount,
        direct_outstanding: CarrierCount,
    ) {
        self.inner
            .ledger
            .close_envelope(coverage, envelope, direct_outstanding);
    }

    /// Returns the largest uncovered count compatible with packed admission.
    pub(super) fn maximum_uncovered(&self) -> CarrierCount {
        MAX_PACKED_CARRIERS
            .checked_sub(self.inner.ledger.active_planned_demand)
            .unwrap_or_else(|| {
                invariant_violation("active planned demand exceeds packed admission")
            })
    }

    /// Returns the floor required by the current admission state.
    pub(super) fn acquisition_floor(
        &self,
        coverage: &CoverageState,
    ) -> Result<CarrierCount, ReserveError> {
        self.inner.ledger.admission_used(coverage.snapshot())
    }

    /// Reverses an unexposed acquisition while admission remains held.
    pub(super) fn rollback_acquisition(&mut self, coverage: &CoverageState, count: CarrierCount) {
        coverage.release(count);
        self.inner.ledger.assert_invariants(coverage.snapshot());
    }
}

/// Carrier-granular values used to decide and back new admission.
pub(super) struct AdmissionLedger {
    /// Normal admission ceiling.
    pub(super) configured_capacity: CarrierCount,
    /// Capacity whose complete preparation steps have succeeded.
    pub(super) prepared_capacity: CarrierCount,
    /// Complete envelopes whose acquisition authority remains open.
    pub(super) active_planned_demand: CarrierCount,
}

impl AdmissionLedger {
    /// Returns active envelopes plus ownership outside their coverage.
    pub(super) fn admission_used(
        &self,
        coverage: CoverageSnapshot,
    ) -> Result<CarrierCount, ReserveError> {
        self.active_planned_demand
            .checked_add(coverage.uncovered)
            .filter(|count| *count <= MAX_PACKED_CARRIERS)
            .ok_or(ReserveError::CapacityOverflow)
    }

    /// Returns whether one envelope may be granted now.
    ///
    /// A request normally fits below the configured ceiling. If no reservation
    /// remains active, one request may exceed that ceiling so retained
    /// uncovered ownership cannot prevent all future progress.
    fn can_grant(&self, coverage: CoverageSnapshot, envelope: CarrierCount) -> bool {
        let normal = self
            .admission_used(coverage)
            .ok()
            .and_then(|used| used.checked_add(envelope))
            .is_some_and(|next| next <= self.configured_capacity);
        normal || self.active_planned_demand == CarrierCount::ZERO
    }

    /// Returns the prepared-capacity floor required after one grant.
    fn grant_target(
        &self,
        coverage: CoverageSnapshot,
        envelope: CarrierCount,
    ) -> Result<CarrierCount, ReserveError> {
        self.admission_used(coverage)?
            .checked_add(envelope)
            .filter(|target| *target <= MAX_PACKED_CARRIERS)
            .ok_or(ReserveError::CapacityOverflow)
    }

    /// Adds one complete envelope after its floor has been prepared.
    fn commit_grant(
        &mut self,
        coverage: &CoverageState,
        envelope: CarrierCount,
    ) -> Result<(), ReserveError> {
        let snapshot = coverage.snapshot();
        let next_active = self
            .active_planned_demand
            .checked_add(envelope)
            .ok_or(ReserveError::CapacityOverflow)?;
        let next_admission_used = next_active
            .checked_add(snapshot.uncovered)
            .filter(|total| *total <= MAX_PACKED_CARRIERS)
            .ok_or(ReserveError::CapacityOverflow)?;
        if self.prepared_capacity < next_admission_used {
            invariant_violation(
                "reservation grant did not prepare its admission floor before publication",
            );
        }

        coverage
            .add_coverage(envelope)
            .map_err(|_| ReserveError::CapacityOverflow)?;
        self.active_planned_demand = next_active;
        self.assert_invariants(coverage.snapshot());
        Ok(())
    }

    /// Withdraws one envelope and reclassifies occupied coverage.
    fn close_envelope(
        &mut self,
        coverage: &CoverageState,
        envelope: CarrierCount,
        direct_outstanding: CarrierCount,
    ) {
        let Some(remaining_active) = self.active_planned_demand.checked_sub(envelope) else {
            invariant_violation("reservation close exceeds active planned demand");
        };
        self.active_planned_demand = remaining_active;
        coverage.remove_coverage(envelope, direct_outstanding, self.active_planned_demand);
        self.assert_invariants(coverage.snapshot());
    }

    /// Checks identities required after each serialized transition.
    pub(super) fn assert_invariants(&self, coverage: CoverageSnapshot) {
        let admission_used = self
            .admission_used(coverage)
            .unwrap_or_else(|_| invariant_violation("completed admission exceeds packed capacity"));
        if self.prepared_capacity < admission_used {
            invariant_violation("prepared capacity does not cover admission used");
        }
        if coverage.available > self.active_planned_demand {
            invariant_violation("available coverage exceeds active planned demand");
        }
    }
}

/// Failure to create a new reservation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReserveError {
    /// The byte request was zero.
    InvalidSize,
    /// Physical storage could not be prepared.
    PhysicalPreparationFailed,
    /// Queue or ownership metadata could not reserve the required capacity.
    MetadataAllocationFailed,
    /// The request or resulting accounting state is not representable.
    CapacityOverflow,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize => f.write_str("reservation size must be nonzero"),
            Self::PhysicalPreparationFailed => {
                f.write_str("physical buffer-pool preparation failed")
            }
            Self::MetadataAllocationFailed => f.write_str("buffer-pool metadata allocation failed"),
            Self::CapacityOverflow => {
                f.write_str("reservation exceeds buffer-pool accounting capacity")
            }
        }
    }
}

impl std::error::Error for ReserveError {}

/// Non-cloneable acquisition authority for one admitted memory envelope.
///
/// Dropping the reservation closes it. Mutable buffers and immutable views
/// already acquired through it remain valid and keep their memory charged
/// until their final owners are dropped.
pub struct Reservation {
    state: Option<Arc<ReservationState>>,
}

/// Private reservation state retained by future direct carrier owners.
pub(super) struct ReservationState {
    /// Pool that granted the reservation.
    pool: Arc<PoolInner>,
    /// Complete admitted envelope.
    envelope: CarrierCount,
    /// Close state and direct owners or in-flight debits.
    owner_state: ReservationOwnerState,
}

impl Reservation {
    /// Creates one open reservation for an already published grant.
    pub(super) fn new(pool: Arc<PoolInner>, envelope: CarrierCount) -> Self {
        Self {
            state: Some(Arc::new(ReservationState {
                pool,
                envelope,
                owner_state: ReservationOwnerState::new(),
            })),
        }
    }

    /// Revokes new acquisition and retires this reservation's planned demand.
    ///
    /// Buffers acquired before this call keep their existing capacity, but
    /// attempts to grow them return
    /// [`AcquireError::ReservationClosed`](super::AcquireError::ReservationClosed).
    pub fn close_acquisition(mut self) {
        self.close();
    }

    fn close(&mut self) {
        if let Some(state) = self.state.take() {
            state.close_acquisition();
        }
    }

    /// Returns the private state retained by a direct acquisition.
    pub(super) fn acquisition_state(&self) -> Option<&Arc<ReservationState>> {
        self.state.as_ref()
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reservation")
            .field("open", &self.state.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.close();
    }
}

impl ReservationState {
    /// Returns the pool whose direct authority this state represents.
    pub(super) fn pool(&self) -> &Arc<PoolInner> {
        &self.pool
    }

    /// Returns whether this reservation belongs to `pool`.
    pub(super) fn belongs_to(&self, pool: &Arc<PoolInner>) -> bool {
        Arc::ptr_eq(&self.pool, pool)
    }

    /// Installs one complete direct-acquisition debit.
    pub(super) fn try_debit(&self, count: CarrierCount) -> Result<(), DirectDebitError> {
        self.owner_state.try_debit(self.envelope, count)
    }

    /// Rejects an already impossible debit before aggregate preparation.
    ///
    /// This load does not reserve authority. The caller must still use
    /// [`Self::try_debit`] after publishing its aggregate charge.
    pub(super) fn precheck_debit(&self, count: CarrierCount) -> Result<(), DirectDebitError> {
        self.owner_state.precheck_debit(self.envelope, count)
    }

    /// Retires direct owners or an in-flight debit.
    pub(super) fn release(&self, count: CarrierCount) {
        self.owner_state.release(count);
    }

    /// Returns direct-owner state for composed acquisition tests.
    #[cfg(test)]
    pub(super) fn test_owner_state(&self) -> (bool, CarrierCount) {
        let snapshot = self.owner_state.snapshot();
        (snapshot.closed, snapshot.direct_outstanding)
    }

    /// Closes direct authority and withdraws this reservation's envelope.
    fn close_acquisition(&self) {
        let drained = {
            let mut admission = AdmissionGuard::new(self.pool.admission.lock());
            let Some(direct_outstanding) = self.owner_state.close() else {
                return;
            };
            admission.close_envelope(&self.pool.coverage, self.envelope, direct_outstanding);
            PoolInner::drain_fifo_locked(&self.pool, &mut admission, false)
        };
        if let Some(sample) = drained.queue_sample {
            self.pool.log_reservation_queue_transition(sample);
        }
        wake_all(drained.wakers);
    }
}

/// Wakes terminal reservation futures after every pool lock is released.
pub(super) fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

/// Why a reservation-local debit could not be installed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DirectDebitError {
    /// The reservation no longer permits new carrier acquisition.
    Closed,
    /// The debit would exceed the admitted envelope.
    CapacityExceeded,
    /// The count cannot be represented in the packed owner state.
    CapacityOverflow,
}

/// One coherent reservation-owner sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReservationOwnerSnapshot {
    /// Whether acquisition of new carriers is closed.
    closed: bool,
    /// Direct owners and in-flight debits.
    direct_outstanding: CarrierCount,
}

/// Reservation-local acquisition state.
///
/// The high bit records close. The remaining bits count direct carrier owners
/// and acquisition debits that have not yet become owners.
struct ReservationOwnerState {
    packed: AtomicU64,
}

impl ReservationOwnerState {
    /// Creates open authority with no direct owners.
    fn new() -> Self {
        Self {
            packed: AtomicU64::new(0),
        }
    }

    /// Rejects a debit that the current state already makes impossible.
    ///
    /// This load does not reserve authority and may race close or another
    /// debit. [`Self::try_debit`] performs the authoritative atomic update.
    fn precheck_debit(
        &self,
        envelope: CarrierCount,
        count: CarrierCount,
    ) -> Result<(), DirectDebitError> {
        let envelope =
            u64::try_from(envelope.get()).map_err(|_| DirectDebitError::CapacityOverflow)?;
        let count = u64::try_from(count.get()).map_err(|_| DirectDebitError::CapacityOverflow)?;
        if envelope > DIRECT_OUTSTANDING_MASK || count > DIRECT_OUTSTANDING_MASK {
            return Err(DirectDebitError::CapacityOverflow);
        }

        let current = self.packed.load(Ordering::Acquire);
        if current & RESERVATION_CLOSED != 0 {
            return Err(DirectDebitError::Closed);
        }
        (current & DIRECT_OUTSTANDING_MASK)
            .checked_add(count)
            .filter(|next| *next <= envelope)
            .map(|_| ())
            .ok_or(DirectDebitError::CapacityExceeded)
    }

    /// Installs one complete direct-acquisition debit.
    ///
    /// Close and debit linearize on the same atomic word. A successful debit
    /// remains valid if close follows it. A debit that observes close fails.
    fn try_debit(
        &self,
        envelope: CarrierCount,
        count: CarrierCount,
    ) -> Result<(), DirectDebitError> {
        let envelope =
            u64::try_from(envelope.get()).map_err(|_| DirectDebitError::CapacityOverflow)?;
        let count = u64::try_from(count.get()).map_err(|_| DirectDebitError::CapacityOverflow)?;
        if envelope > DIRECT_OUTSTANDING_MASK || count > DIRECT_OUTSTANDING_MASK {
            return Err(DirectDebitError::CapacityOverflow);
        }

        let result = self
            .packed
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current & RESERVATION_CLOSED != 0 {
                    return None;
                }
                (current & DIRECT_OUTSTANDING_MASK)
                    .checked_add(count)
                    .filter(|next| *next <= envelope)
            });

        match result {
            Ok(_) => Ok(()),
            Err(current) if current & RESERVATION_CLOSED != 0 => Err(DirectDebitError::Closed),
            Err(_) => Err(DirectDebitError::CapacityExceeded),
        }
    }

    /// Closes direct acquisition and returns the outstanding count it observed.
    fn close(&self) -> Option<CarrierCount> {
        let previous = self.packed.fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
        if previous & RESERVATION_CLOSED != 0 {
            return None;
        }
        Some(CarrierCount::new(
            usize::try_from(previous & DIRECT_OUTSTANDING_MASK)
                .unwrap_or_else(|_| invariant_violation("direct owner count does not fit usize")),
        ))
    }

    /// Retires direct owners or rolls back an in-flight debit.
    ///
    /// Returns `true` when the reservation remains open. Release preserves the
    /// closed bit and cannot restore acquisition authority after close.
    fn release(&self, count: CarrierCount) -> bool {
        let count = u64::try_from(count.get()).unwrap_or_else(|_| {
            invariant_violation("direct release count does not fit owner state")
        });
        let previous = self
            .packed
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let outstanding = current & DIRECT_OUTSTANDING_MASK;
                outstanding
                    .checked_sub(count)
                    .map(|next| (current & RESERVATION_CLOSED) | next)
            })
            .unwrap_or_else(|_| {
                invariant_violation("direct release exceeds outstanding ownership")
            });
        previous & RESERVATION_CLOSED == 0
    }

    /// Loads one coherent owner-state sample.
    fn snapshot(&self) -> ReservationOwnerSnapshot {
        let packed = self.packed.load(Ordering::Acquire);
        ReservationOwnerSnapshot {
            closed: packed & RESERVATION_CLOSED != 0,
            direct_outstanding: CarrierCount::new(
                usize::try_from(packed & DIRECT_OUTSTANDING_MASK).unwrap_or_else(|_| {
                    invariant_violation("direct owner count does not fit usize")
                }),
            ),
        }
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::super::test_util::test_pool;
    use super::super::virtual_memory::VirtualMemoryOperation;
    use super::super::BufferPool;
    use super::*;

    #[test]
    fn test_queue_diagnostics_report_depth_peak_and_nonempty_durations() {
        let start = Instant::now();
        let mut diagnostics = ReservationQueueDiagnostics::new(start);

        assert_eq!(
            diagnostics.record_depth(1, start + Duration::from_millis(10)),
            Some(ReservationQueueTransition::BecameNonempty)
        );
        assert_eq!(
            diagnostics.record_depth(4, start + Duration::from_millis(20)),
            None
        );
        assert_eq!(
            diagnostics.take(4, start + Duration::from_millis(30)),
            ReservationQueueDiagnosticSample {
                current_depth: 4,
                peak_depth: 4,
                nonempty_duration: Duration::from_millis(20),
                continuous_nonempty_duration: Some(Duration::from_millis(20)),
            }
        );

        assert_eq!(
            diagnostics.record_depth(0, start + Duration::from_millis(50)),
            Some(ReservationQueueTransition::BecameEmpty)
        );
        assert_eq!(
            diagnostics.take(0, start + Duration::from_millis(60)),
            ReservationQueueDiagnosticSample {
                current_depth: 0,
                peak_depth: 4,
                nonempty_duration: Duration::from_millis(20),
                continuous_nonempty_duration: None,
            }
        );
    }

    #[test]
    fn test_immediate_grant_prepares_before_publication() {
        let (pool, carrier_size) = test_pool(2, 4);

        let reservation = pool.try_reserve(carrier_size).unwrap().unwrap();

        {
            let admission = pool.inner.admission.lock();
            let coverage = pool.inner.coverage.snapshot();
            assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(2));
            assert_eq!(admission.ledger.active_planned_demand, CarrierCount::new(1));
            assert_eq!(coverage.available, CarrierCount::new(1));
            assert_eq!(coverage.uncovered, CarrierCount::ZERO);
        }

        drop(reservation);
        let admission = pool.inner.admission.lock();
        assert_eq!(admission.ledger.active_planned_demand, CarrierCount::ZERO);
        assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(2));
        assert_eq!(
            pool.inner.coverage.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::ZERO,
            }
        );
    }

    #[test]
    fn test_grant_cannot_publish_before_preparing_its_floor() {
        let mut admission = AdmissionState::new(CarrierCount::new(1));
        let coverage = CoverageState::new();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            admission
                .ledger
                .commit_grant(&coverage, CarrierCount::new(1))
                .unwrap();
        }));

        assert!(result.is_err());
        assert_eq!(admission.ledger.active_planned_demand, CarrierCount::ZERO);
        assert_eq!(
            coverage.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::ZERO,
            }
        );
    }

    #[test]
    fn test_normal_grant_respects_configured_capacity() {
        let (pool, carrier_size) = test_pool(2, 2);
        let reservation = pool.try_reserve(carrier_size * 2).unwrap().unwrap();

        assert!(pool.try_reserve(carrier_size).unwrap().is_none());
        assert_eq!(pool.inner.arena.diagnostics().blocks_prepared, 1);

        drop(reservation);
    }

    #[test]
    fn test_idle_only_grant_can_exceed_configured_capacity() {
        let (pool, carrier_size) = test_pool(2, 1);

        let reservation = pool.try_reserve(carrier_size * 3).unwrap().unwrap();

        let admission = pool.inner.admission.lock();
        assert_eq!(admission.ledger.active_planned_demand, CarrierCount::new(3));
        assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(4));
        drop(admission);
        drop(reservation);
    }

    #[test]
    fn test_idle_only_grant_can_progress_past_uncovered_ownership() {
        let (pool, carrier_size) = test_pool(1, 1);
        {
            let mut admission = AdmissionGuard::new(pool.inner.admission.lock());
            pool.inner
                .coverage
                .debit(CarrierCount::new(1), MAX_PACKED_CARRIERS)
                .unwrap();
            pool.inner
                .arena
                .prepare_to(&mut admission, CarrierCount::new(1))
                .unwrap();
        }

        let reservation = pool.try_reserve(carrier_size).unwrap().unwrap();

        {
            let admission = pool.inner.admission.lock();
            assert_eq!(admission.ledger.active_planned_demand, CarrierCount::new(1));
            assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(2));
            assert_eq!(
                pool.inner.coverage.snapshot().uncovered,
                CarrierCount::new(1)
            );
        }
        drop(reservation);
        pool.inner.coverage.release(CarrierCount::new(1));
    }

    #[test]
    fn test_preparation_failure_publishes_no_grant() {
        let (pool, carrier_size) = test_pool(2, 4);
        let first = pool.inner.arena.reserve_slot().unwrap();
        let second = pool.inner.arena.reserve_slot().unwrap();
        second.inject_failure_once(VirtualMemoryOperation::Prepare);

        assert!(matches!(
            pool.try_reserve(carrier_size * 3),
            Err(ReserveError::PhysicalPreparationFailed)
        ));
        {
            let admission = pool.inner.admission.lock();
            assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(2));
            assert_eq!(admission.ledger.active_planned_demand, CarrierCount::ZERO);
            assert_eq!(
                pool.inner.coverage.snapshot(),
                CoverageSnapshot {
                    available: CarrierCount::ZERO,
                    uncovered: CarrierCount::ZERO,
                }
            );
        }

        let reservation = pool.try_reserve(carrier_size * 3).unwrap().unwrap();
        assert_eq!(
            pool.inner.admission.lock().ledger.prepared_capacity,
            CarrierCount::new(4)
        );
        drop(reservation);
        drop(first);
    }

    #[test]
    fn test_close_reclassifies_occupied_coverage() {
        let (pool, carrier_size) = test_pool(2, 2);
        let reservation = pool.try_reserve(carrier_size * 2).unwrap().unwrap();
        assert!(pool
            .inner
            .coverage
            .try_debit_covered(CarrierCount::new(1))
            .unwrap());

        reservation.close_acquisition();

        assert_eq!(
            pool.inner.coverage.snapshot(),
            CoverageSnapshot {
                available: CarrierCount::ZERO,
                uncovered: CarrierCount::new(1),
            }
        );
        assert_eq!(
            pool.inner.admission.lock().ledger.active_planned_demand,
            CarrierCount::ZERO
        );
        pool.inner.coverage.release(CarrierCount::new(1));
    }

    #[test]
    fn test_invalid_reservation_sizes_fail_before_preparation() {
        let (pool, carrier_size) = test_pool(1, 1);

        assert!(matches!(
            pool.try_reserve(0),
            Err(ReserveError::InvalidSize)
        ));
        if let Some(bytes) = (u32::MAX as usize)
            .checked_add(1)
            .and_then(|carriers| carriers.checked_mul(carrier_size))
        {
            assert!(matches!(
                pool.try_reserve(bytes),
                Err(ReserveError::CapacityOverflow)
            ));
        }
        assert_eq!(pool.inner.arena.diagnostics().blocks_prepared, 0);
    }

    #[test]
    fn test_pool_ownership_spine_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<BufferPool>();
        assert_send_sync::<Reservation>();
        assert_send_sync::<ReservationState>();
    }

    #[test]
    fn test_close_is_idempotent() {
        let state = ReservationOwnerState::new();

        assert_eq!(state.close(), Some(CarrierCount::ZERO));
        assert_eq!(state.close(), None);
        assert_eq!(
            state.snapshot(),
            ReservationOwnerSnapshot {
                closed: true,
                direct_outstanding: CarrierCount::ZERO,
            }
        );
    }

    #[test]
    fn test_direct_debit_cannot_exceed_envelope() {
        let state = ReservationOwnerState::new();
        state
            .try_debit(CarrierCount::new(3), CarrierCount::new(2))
            .unwrap();

        assert_eq!(
            state.try_debit(CarrierCount::new(3), CarrierCount::new(2)),
            Err(DirectDebitError::CapacityExceeded)
        );
        assert_eq!(state.snapshot().direct_outstanding, CarrierCount::new(2));
    }

    #[test]
    fn test_release_after_close_does_not_reopen_reservation() {
        let state = ReservationOwnerState::new();
        state
            .try_debit(CarrierCount::new(2), CarrierCount::new(2))
            .unwrap();
        assert_eq!(state.close(), Some(CarrierCount::new(2)));

        assert!(!state.release(CarrierCount::new(2)));
        assert_eq!(
            state.snapshot(),
            ReservationOwnerSnapshot {
                closed: true,
                direct_outstanding: CarrierCount::ZERO,
            }
        );
        assert_eq!(
            state.try_debit(CarrierCount::new(2), CarrierCount::new(1)),
            Err(DirectDebitError::Closed)
        );
    }

    #[test]
    fn test_invalid_release_leaves_owner_state_unchanged() {
        let state = ReservationOwnerState::new();

        let result = std::panic::catch_unwind(|| state.release(CarrierCount::new(1)));

        assert!(result.is_err());
        assert_eq!(
            state.snapshot(),
            ReservationOwnerSnapshot {
                closed: false,
                direct_outstanding: CarrierCount::ZERO,
            }
        );
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::test_util::test_single_carrier_pool as test_pool;
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

    use super::*;

    #[test]
    fn test_concurrent_immediate_grants_respect_normal_capacity() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);

            let first_pool = pool.clone();
            let first = thread::spawn(move || first_pool.try_reserve(carrier_size).unwrap());
            let second_pool = pool.clone();
            let second = thread::spawn(move || second_pool.try_reserve(carrier_size).unwrap());

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            assert_eq!(
                usize::from(first.is_some()) + usize::from(second.is_some()),
                1
            );
            drop(first);
            drop(second);
            assert_eq!(
                pool.inner.admission.lock().ledger.active_planned_demand,
                CarrierCount::ZERO
            );
        });
    }

    #[test]
    fn test_concurrent_idle_only_grants_do_not_compound_overage() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);

            let first_pool = pool.clone();
            let first = thread::spawn(move || first_pool.try_reserve(carrier_size * 2).unwrap());
            let second_pool = pool.clone();
            let second = thread::spawn(move || second_pool.try_reserve(carrier_size * 2).unwrap());

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            assert_eq!(
                usize::from(first.is_some()) + usize::from(second.is_some()),
                1
            );
            drop(first);
            drop(second);
            assert_eq!(
                pool.inner.admission.lock().ledger.active_planned_demand,
                CarrierCount::ZERO
            );
        });
    }

    #[test]
    fn test_direct_debit_racing_close_has_one_linearization() {
        loom::model(|| {
            let state = Arc::new(ReservationOwnerState::new());

            let debiting = Arc::clone(&state);
            let debit = thread::spawn(move || {
                debiting.try_debit(CarrierCount::new(1), CarrierCount::new(1))
            });
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || closing.close());

            let debit = debit.join().unwrap();
            let closed_outstanding = close.join().unwrap();
            let snapshot = state.snapshot();
            assert!(snapshot.closed);
            match debit {
                Ok(()) => {
                    assert_eq!(closed_outstanding, Some(CarrierCount::new(1)));
                    assert_eq!(snapshot.direct_outstanding, CarrierCount::new(1));
                }
                Err(DirectDebitError::Closed) => {
                    assert_eq!(closed_outstanding, Some(CarrierCount::ZERO));
                    assert_eq!(snapshot.direct_outstanding, CarrierCount::ZERO);
                }
                Err(error) => panic!("unexpected debit error: {error:?}"),
            }
        });
    }

    #[test]
    fn test_concurrent_direct_debits_consume_authority_once() {
        loom::model(|| {
            let state = Arc::new(ReservationOwnerState::new());

            let first_state = Arc::clone(&state);
            let first = thread::spawn(move || {
                first_state.try_debit(CarrierCount::new(1), CarrierCount::new(1))
            });
            let second_state = Arc::clone(&state);
            let second = thread::spawn(move || {
                second_state.try_debit(CarrierCount::new(1), CarrierCount::new(1))
            });

            let successes = usize::from(first.join().unwrap().is_ok())
                + usize::from(second.join().unwrap().is_ok());
            assert_eq!(successes, 1);
            assert_eq!(state.snapshot().direct_outstanding, CarrierCount::new(1));
        });
    }

    #[test]
    fn test_release_racing_close_cannot_reopen_reservation() {
        loom::model(|| {
            let state = Arc::new(ReservationOwnerState::new());
            state
                .try_debit(CarrierCount::new(1), CarrierCount::new(1))
                .unwrap();

            let releasing = Arc::clone(&state);
            let release = thread::spawn(move || releasing.release(CarrierCount::new(1)));
            let closing = Arc::clone(&state);
            let close = thread::spawn(move || closing.close());

            release.join().unwrap();
            close.join().unwrap();
            assert_eq!(
                state.snapshot(),
                ReservationOwnerSnapshot {
                    closed: true,
                    direct_outstanding: CarrierCount::ZERO,
                }
            );
        });
    }
}
