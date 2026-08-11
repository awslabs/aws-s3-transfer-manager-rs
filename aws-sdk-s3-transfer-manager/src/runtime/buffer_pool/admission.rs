/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Planned-demand admission, FIFO waiters, and reservation lifetime.
//!
//! A grant prepares fungible capacity before charging its complete envelope.
//! Waiters receive the charged reservation before notification and never
//! re-contend for released capacity. Closing replaces the envelope with exact
//! surviving direct ownership and newly uncovered unreserved debt.

use std::collections::VecDeque;
use std::sync::Arc as StdArc;
use std::task::Poll;

use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

use super::{AllocError, PoolInner, ReserveError};

mod unreserved;
pub(super) use unreserved::UnreservedState;
use unreserved::{UnreservedSnapshot, MAX_CARRIERS};

/// Callback invoked after a pending reservation reaches a terminal state.
pub(super) type NotifyFn = StdArc<dyn Fn() + Send + Sync>;

/// Phase bit in [`ReservationState::owner_state`].
const RESERVATION_CLOSED: u64 = 1 << 63;
/// Direct owner/debit bits in [`ReservationState::owner_state`].
const RESERVATION_COUNT_MASK: u64 = !RESERVATION_CLOSED;

/// Global admission state protected by `PoolInner::admission`.
pub(super) struct AdmissionState {
    /// Counters participating in preparation and grant decisions.
    pub(super) ledger: AdmissionLedger,
    waiters: VecDeque<Waiter>,
    closed: bool,
}

/// Carrier-granular quantities used to decide and back new admission.
///
/// Admission pressure is `active_planned_demand + retiring_direct_live` plus
/// debt sampled from [`UnreservedState`]. `prepared` covers that pressure
/// after every completed transition.
pub(super) struct AdmissionLedger {
    /// Soft admission and retained-capacity target.
    pub(super) configured: usize,
    /// Capacity whose mapping and current preparation steps are complete.
    pub(super) prepared: usize,
    /// Full envelopes whose direct acquisition remains open.
    pub(super) active_planned_demand: usize,
    /// Direct owners surviving after their reservation closed.
    pub(super) retiring_direct_live: usize,
    /// Active-plan capacity available to future unreserved ownership.
    pub(super) unreserved_coverage: usize,
}

/// Planned demand and direct-acquisition authority for one work item.
///
/// The difference between `envelope` and `direct_limit` is aggregate coverage
/// for future unreserved acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReservationPlan {
    /// Complete planned demand charged while direct acquisition remains open.
    pub(super) envelope: usize,
    /// Subset of the envelope available through reserved acquisition.
    pub(super) direct_limit: usize,
}

/// Result of a reservation request that may enter the FIFO.
pub(super) enum Reserve {
    Ready(Reservation),
    Pending(WaitTicket),
}

/// A reservation request held in arrival order.
struct Waiter {
    plan: ReservationPlan,
    slot: Arc<WaitSlot>,
    notify: NotifyFn,
}

/// Result slot shared by the FIFO and its caller.
struct WaitSlot {
    state: Mutex<WaitState>,
}

/// Lifecycle of one FIFO result slot.
enum WaitState {
    Queued,
    Granted(Reservation),
    Failed(ReserveError),
    Taken,
}

/// Handle to a reservation request parked in the FIFO.
///
/// Dropping this handle cancels a queued request. A grant dropped before
/// [`WaitTicket::take`] returns its reservation to admission.
pub(super) struct WaitTicket {
    slot: Arc<WaitSlot>,
    pool: Arc<PoolInner>,
}

/// Linear authority to acquire directly against one admitted plan.
///
/// This type is not cloneable. [`Reservation::close_acquisition`] consumes it,
/// making acquire-versus-close races unrepresentable through the safe API.
pub(super) struct Reservation {
    state: Option<Arc<ReservationState>>,
}

/// State retained by direct carrier guards after the public reservation closes.
pub(super) struct ReservationState {
    pub(super) pool: Arc<PoolInner>,
    plan: ReservationPlan,

    /// The high bit closes acquisition. Remaining bits count direct carrier
    /// guards and in-flight debits that have not become guards.
    owner_state: AtomicU64,
}

/// RAII rollback for direct authority debited before physical acquisition.
pub(super) struct DirectDebit {
    pub(super) reservation: Arc<ReservationState>,
    pub(super) uncommitted: usize,
}

/// RAII rollback for unreserved charges installed before physical acquisition.
pub(super) struct UnreservedDebit {
    pub(super) pool: Arc<PoolInner>,
    pub(super) uncommitted: usize,
}

/// Accounting inverse performed by a carrier's final owner.
pub(super) enum Charge {
    Direct(Arc<ReservationState>),
    Unreserved,
}

impl std::fmt::Debug for WaitTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitTicket").finish_non_exhaustive()
    }
}

impl AdmissionState {
    /// Create empty admission state with a fixed configured ceiling.
    pub(super) fn new(configured: usize) -> Self {
        Self {
            ledger: AdmissionLedger {
                configured,
                prepared: 0,
                active_planned_demand: 0,
                retiring_direct_live: 0,
                unreserved_coverage: 0,
            },
            waiters: VecDeque::new(),
            closed: false,
        }
    }

    /// Number of reservation requests still held in FIFO order.
    pub(super) fn waiter_count(&self) -> usize {
        self.waiters.len()
    }

    /// Whether new admission and queued grants have been closed.
    pub(super) fn is_closed(&self) -> bool {
        self.closed
    }

    /// Whether `plan` can be granted under the normal bound or idle escape.
    fn can_grant(&self, plan: ReservationPlan, unreserved_debt: usize) -> bool {
        let admission_used = self.ledger.admission_used(unreserved_debt);
        let normal = plan.envelope <= self.ledger.configured.saturating_sub(admission_used);
        let managed_demand = self
            .ledger
            .active_planned_demand
            .checked_add(self.ledger.retiring_direct_live)
            .expect("managed admission demand overflowed");

        normal || managed_demand == 0
    }
}

impl AdmissionLedger {
    /// Pressure charged against new planned demand.
    pub(super) fn admission_used(&self, unreserved_debt: usize) -> usize {
        self.active_planned_demand
            .checked_add(self.retiring_direct_live)
            .and_then(|used| used.checked_add(unreserved_debt))
            .expect("admission pressure overflowed")
    }

    /// Charge one plan after its capacity has been prepared.
    fn admit(
        &mut self,
        unreserved: &UnreservedState,
        plan: ReservationPlan,
    ) -> Result<(), ReserveError> {
        self.validate_admit(unreserved.snapshot(), plan)?;
        let active_planned_demand = self
            .active_planned_demand
            .checked_add(plan.envelope)
            .expect("validated active planned demand overflowed");
        let unreserved_coverage = self
            .unreserved_coverage
            .checked_add(plan.unreserved_coverage())
            .expect("validated unreserved coverage overflowed");

        unreserved
            .add_coverage(plan.unreserved_coverage())
            .expect("validated coverage must fit packed state");
        self.active_planned_demand = active_planned_demand;
        self.unreserved_coverage = unreserved_coverage;
        self.assert_invariants(unreserved.snapshot());
        Ok(())
    }

    /// Validate all fallible counters before physical preparation starts.
    fn validate_admit(
        &self,
        unreserved: UnreservedSnapshot,
        plan: ReservationPlan,
    ) -> Result<(), ReserveError> {
        self.active_planned_demand
            .checked_add(plan.envelope)
            .ok_or(ReserveError::CapacityOverflow)?;
        self.unreserved_coverage
            .checked_add(plan.unreserved_coverage())
            .and_then(|coverage| coverage.checked_add(unreserved.debt))
            .filter(|total| *total <= MAX_CARRIERS)
            .ok_or(ReserveError::CapacityOverflow)?;
        Ok(())
    }

    /// Check identities that must hold after every completed transition.
    fn assert_invariants(&self, unreserved: UnreservedSnapshot) {
        assert!(
            self.prepared >= self.admission_used(unreserved.debt),
            "prepared capacity must cover admitted work and debt"
        );
        assert!(
            unreserved.available_coverage <= self.unreserved_coverage,
            "available coverage exceeds active coverage"
        );
        assert!(
            self.unreserved_coverage + unreserved.debt <= MAX_CARRIERS,
            "unreserved live bound exceeds packed state"
        );
    }

    /// Derive live unreserved owners from one coherent packed sample.
    pub(super) fn unreserved_live(&self, unreserved: UnreservedSnapshot) -> usize {
        self.unreserved_coverage
            .checked_add(unreserved.debt)
            .and_then(|total| total.checked_sub(unreserved.available_coverage))
            .expect("unreserved state violates coverage identity")
    }
}

impl ReservationPlan {
    /// Construct an admitted envelope and its direct-acquisition subset.
    pub(super) fn new(envelope: usize, direct_limit: usize) -> Self {
        assert!(envelope > 0, "a reservation must have a nonzero envelope");
        assert!(
            direct_limit <= envelope,
            "direct authority must fit the reservation envelope"
        );
        Self {
            envelope,
            direct_limit,
        }
    }

    /// Capacity available to aggregate unreserved acquisition.
    fn unreserved_coverage(self) -> usize {
        self.envelope - self.direct_limit
    }
}

impl Reservation {
    /// Create direct acquisition authority for an already-admitted plan.
    fn new(pool: Arc<PoolInner>, plan: ReservationPlan) -> Self {
        Self {
            state: Some(Arc::new(ReservationState {
                pool,
                plan,
                owner_state: AtomicU64::new(0),
            })),
        }
    }

    /// Debit direct authority for one physical acquisition attempt.
    pub(super) fn try_debit(
        &self,
        pool: &Arc<PoolInner>,
        count: usize,
    ) -> Result<DirectDebit, AllocError> {
        let state = self
            .state
            .as_ref()
            .expect("an open reservation retains its state");
        if !Arc::ptr_eq(&state.pool, pool) {
            return Err(AllocError::ForeignReservation);
        }
        ReservationState::try_debit(state, count)
    }

    /// Consume all future direct-acquisition authority.
    ///
    /// Direct carrier guards retain the private state needed to retire their
    /// exact charges after this handle is gone.
    pub(super) fn close_acquisition(mut self) {
        self.close();
    }

    fn close(&mut self) {
        if let Some(state) = self.state.take() {
            state.close_acquisition();
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.close();
    }
}

impl WaitTicket {
    /// Take the terminal result installed by admission.
    ///
    /// Returns [`Poll::Pending`] while queued. Calling this method after it
    /// returned [`Poll::Ready`] is a caller error.
    pub(super) fn take(&mut self) -> Poll<Result<Reservation, ReserveError>> {
        let mut slot_state = self.slot.state.lock();
        let state = std::mem::replace(&mut *slot_state, WaitState::Taken);
        match state {
            WaitState::Queued => {
                *slot_state = WaitState::Queued;
                Poll::Pending
            }
            WaitState::Granted(reservation) => Poll::Ready(Ok(reservation)),
            WaitState::Failed(error) => Poll::Ready(Err(error)),
            WaitState::Taken => panic!("wait result was already taken"),
        }
    }
}

impl Drop for WaitTicket {
    fn drop(&mut self) {
        let state = {
            let mut slot_state = self.slot.state.lock();
            std::mem::replace(&mut *slot_state, WaitState::Taken)
        };

        match state {
            WaitState::Granted(reservation) => drop(reservation),
            WaitState::Queued => {
                let notifications = PoolInner::cancel_waiter(&self.pool, &self.slot);
                notify_all(notifications);
            }
            WaitState::Failed(_) | WaitState::Taken => {}
        }
    }
}

impl ReservationState {
    /// Debit direct authority for an entire physical acquisition attempt.
    ///
    /// The returned guard owns rollback until each unit is transferred to a
    /// carrier guard. Acquire ordering publishes the updated count before a
    /// concurrent carrier return or close observes it.
    fn try_debit(state: &Arc<Self>, count: usize) -> Result<DirectDebit, AllocError> {
        let count = u64::try_from(count).map_err(|_| AllocError::CapacityOverflow)?;
        let direct_limit =
            u64::try_from(state.plan.direct_limit).map_err(|_| AllocError::CapacityOverflow)?;

        let result =
            state
                .owner_state
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    if current & RESERVATION_CLOSED != 0 {
                        return None;
                    }
                    let outstanding = current & RESERVATION_COUNT_MASK;
                    outstanding
                        .checked_add(count)
                        .filter(|next| *next <= direct_limit)
                });

        match result {
            Ok(_) => Ok(DirectDebit {
                reservation: Arc::clone(state),
                uncommitted: usize::try_from(count).unwrap(),
            }),
            Err(current) => {
                assert_eq!(
                    current & RESERVATION_CLOSED,
                    0,
                    "structural reservation authority cannot outlive close"
                );
                Err(AllocError::ReservationEnvelopeExceeded)
            }
        }
    }

    /// Replace the active envelope with exact surviving ownership.
    ///
    /// The admission lock is held across the phase-bit update and ledger
    /// conversion. A return before the atomic observes an open reservation; a
    /// return after it waits for the lock before retiring its installed charge.
    fn close_acquisition(&self) {
        let notifications = {
            let mut admission = self.pool.admission.lock();
            let previous = self
                .owner_state
                .fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
            if previous & RESERVATION_CLOSED != 0 {
                return;
            }

            let direct_live = usize::try_from(previous & RESERVATION_COUNT_MASK).unwrap();
            admission.ledger.active_planned_demand = admission
                .ledger
                .active_planned_demand
                .checked_sub(self.plan.envelope)
                .expect("reservation close exceeded active planned demand");
            admission.ledger.retiring_direct_live = admission
                .ledger
                .retiring_direct_live
                .checked_add(direct_live)
                .expect("retiring direct ownership overflowed");
            self.pool
                .unreserved
                .remove_coverage(self.plan.unreserved_coverage());
            admission.ledger.unreserved_coverage = admission
                .ledger
                .unreserved_coverage
                .checked_sub(self.plan.unreserved_coverage())
                .expect("reservation close exceeded unreserved coverage");

            admission
                .ledger
                .assert_invariants(self.pool.unreserved.snapshot());
            PoolInner::drain_fifo_locked(&self.pool, &mut admission)
        };
        notify_all(notifications);
    }

    /// Release direct owners or uncommitted debits.
    ///
    /// Returns before close restore reservation authority. Returns after close
    /// also remove the corresponding global retiring charge.
    pub(super) fn release_direct(&self, count: usize) {
        let count = u64::try_from(count).expect("direct release count fits owner state");
        let previous = self.owner_state.fetch_sub(count, Ordering::AcqRel);
        assert!(
            previous & RESERVATION_COUNT_MASK >= count,
            "direct return exceeds outstanding ownership"
        );

        if previous & RESERVATION_CLOSED != 0 {
            let notifications = {
                let mut admission = self.pool.admission.lock();
                admission.ledger.retiring_direct_live = admission
                    .ledger
                    .retiring_direct_live
                    .checked_sub(usize::try_from(count).unwrap())
                    .expect("direct return exceeded retiring ownership");
                admission
                    .ledger
                    .assert_invariants(self.pool.unreserved.snapshot());
                PoolInner::drain_fifo_locked(&self.pool, &mut admission)
            };
            notify_all(notifications);
        }
    }
}

impl Drop for DirectDebit {
    fn drop(&mut self) {
        if self.uncommitted > 0 {
            self.reservation.release_direct(self.uncommitted);
        }
    }
}

impl Drop for UnreservedDebit {
    fn drop(&mut self) {
        if self.uncommitted > 0 {
            PoolInner::release_unreserved(&self.pool, self.uncommitted);
        }
    }
}

impl PoolInner {
    /// Grant `plan` immediately without bypassing an existing waiter.
    pub(super) fn try_reserve(
        pool: &Arc<Self>,
        plan: ReservationPlan,
    ) -> Result<Reservation, ReserveError> {
        let mut admission = pool.admission.lock();
        if admission.closed {
            return Err(ReserveError::Closed);
        }
        let unreserved_debt = pool.unreserved.snapshot().debt;
        if !admission.waiters.is_empty() || !admission.can_grant(plan, unreserved_debt) {
            return Err(ReserveError::AtCapacity);
        }
        PoolInner::prepare_and_grant_locked(pool, &mut admission, plan)
    }

    /// Grant `plan` immediately or append it to the reservation FIFO.
    pub(super) fn reserve(
        pool: &Arc<Self>,
        plan: ReservationPlan,
        notify: NotifyFn,
    ) -> Result<Reserve, ReserveError> {
        let mut admission = pool.admission.lock();
        if admission.closed {
            return Err(ReserveError::Closed);
        }
        let unreserved_debt = pool.unreserved.snapshot().debt;
        if admission.waiters.is_empty() && admission.can_grant(plan, unreserved_debt) {
            let reservation = PoolInner::prepare_and_grant_locked(pool, &mut admission, plan)?;
            return Ok(Reserve::Ready(reservation));
        }

        let slot = Arc::new(WaitSlot {
            state: Mutex::new(WaitState::Queued),
        });
        admission.waiters.push_back(Waiter {
            plan,
            slot: Arc::clone(&slot),
            notify,
        });
        Ok(Reserve::Pending(WaitTicket {
            slot,
            pool: Arc::clone(pool),
        }))
    }

    /// Close new admission and fail every queued waiter.
    pub(super) fn close_admission(pool: &Arc<Self>) {
        let notifications = {
            let mut admission = pool.admission.lock();
            if admission.closed {
                return;
            }
            admission.closed = true;

            let mut notifications = Vec::with_capacity(admission.waiters.len());
            while let Some(waiter) = admission.waiters.pop_front() {
                let mut state = waiter.slot.state.lock();
                if matches!(*state, WaitState::Queued) {
                    *state = WaitState::Failed(ReserveError::Closed);
                    notifications.push(waiter.notify);
                }
            }
            notifications
        };
        notify_all(notifications);
    }

    /// Charge an unreserved acquisition and prepare any newly exposed debt.
    ///
    /// Charging precedes physical claim so unreserved traffic cannot consume
    /// the only carrier backing an existing reservation without first growing
    /// replacement capacity.
    pub(super) fn debit_unreserved(
        pool: &Arc<Self>,
        count: usize,
    ) -> Result<UnreservedDebit, AllocError> {
        if pool
            .unreserved
            .try_debit_covered(count)
            .map_err(|_| AllocError::CapacityOverflow)?
        {
            return Ok(UnreservedDebit {
                pool: Arc::clone(pool),
                uncommitted: count,
            });
        }

        let mut admission = pool.admission.lock();
        let maximum_debt = MAX_CARRIERS
            .checked_sub(admission.ledger.unreserved_coverage)
            .ok_or(AllocError::CapacityOverflow)?;
        let debit = pool
            .unreserved
            .debit(count, maximum_debt)
            .map_err(|_| AllocError::CapacityOverflow)?;

        if debit.new_debt > 0 {
            let unreserved = pool.unreserved.snapshot();
            let target = admission.ledger.admission_used(unreserved.debt);
            if let Err(error) = pool
                .arena
                .prepare_to(target, &mut admission.ledger.prepared)
            {
                pool.unreserved.release(count);
                admission
                    .ledger
                    .assert_invariants(pool.unreserved.snapshot());
                return Err(error);
            }
        }
        admission
            .ledger
            .assert_invariants(pool.unreserved.snapshot());
        Ok(UnreservedDebit {
            pool: Arc::clone(pool),
            uncommitted: count,
        })
    }

    /// Retire unreserved charges, repaying sticky debt first.
    pub(super) fn release_unreserved(pool: &Arc<Self>, count: usize) {
        let release = pool.unreserved.release(count);
        if release.repaid_debt == 0 {
            return;
        }

        let notifications = {
            let mut admission = pool.admission.lock();
            admission
                .ledger
                .assert_invariants(pool.unreserved.snapshot());
            PoolInner::drain_fifo_locked(pool, &mut admission)
        };
        notify_all(notifications);
    }

    /// Remove a queued waiter and grant newly exposed requests.
    fn cancel_waiter(pool: &Arc<Self>, slot: &Arc<WaitSlot>) -> Vec<NotifyFn> {
        let mut admission = pool.admission.lock();
        admission
            .waiters
            .retain(|waiter| !Arc::ptr_eq(&waiter.slot, slot));
        PoolInner::drain_fifo_locked(pool, &mut admission)
    }

    /// Prepare and charge one plan while admission is serialized.
    fn prepare_and_grant_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionState,
        plan: ReservationPlan,
    ) -> Result<Reservation, ReserveError> {
        admission
            .ledger
            .validate_admit(pool.unreserved.snapshot(), plan)?;
        PoolInner::prepare_plan_locked(pool, admission, plan)?;
        admission.ledger.admit(&pool.unreserved, plan)?;
        Ok(Reservation::new(Arc::clone(pool), plan))
    }

    /// Prepare capacity for `plan` without changing admission pressure.
    fn prepare_plan_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionState,
        plan: ReservationPlan,
    ) -> Result<(), ReserveError> {
        let target = admission
            .ledger
            .admission_used(pool.unreserved.snapshot().debt)
            .checked_add(plan.envelope)
            .ok_or(ReserveError::CapacityOverflow)?;
        pool.arena
            .prepare_to(target, &mut admission.ledger.prepared)
            .map_err(map_preparation_error)
    }

    /// Grant every eligible FIFO head and return callbacks to run after unlock.
    fn drain_fifo_locked(pool: &Arc<Self>, admission: &mut AdmissionState) -> Vec<NotifyFn> {
        if admission.closed {
            return Vec::new();
        }

        let mut notifications = Vec::new();
        while let Some(front) = admission.waiters.front() {
            let plan = front.plan;
            let slot = Arc::clone(&front.slot);
            let notify = StdArc::clone(&front.notify);

            if !matches!(*slot.state.lock(), WaitState::Queued) {
                admission.waiters.pop_front();
                continue;
            }
            if !admission.can_grant(plan, pool.unreserved.snapshot().debt) {
                break;
            }

            if let Err(error) = admission
                .ledger
                .validate_admit(pool.unreserved.snapshot(), plan)
            {
                admission.waiters.pop_front();
                let mut state = slot.state.lock();
                if matches!(*state, WaitState::Queued) {
                    *state = WaitState::Failed(error);
                    notifications.push(notify);
                }
                continue;
            }

            if let Err(error) = PoolInner::prepare_plan_locked(pool, admission, plan) {
                admission.waiters.pop_front();
                let mut state = slot.state.lock();
                if matches!(*state, WaitState::Queued) {
                    *state = WaitState::Failed(error);
                    notifications.push(notify);
                }
                continue;
            }

            admission.waiters.pop_front();
            let mut state = slot.state.lock();
            if matches!(*state, WaitState::Queued) {
                match admission.ledger.admit(&pool.unreserved, plan) {
                    Ok(()) => {
                        *state = WaitState::Granted(Reservation::new(Arc::clone(pool), plan));
                        notifications.push(notify);
                    }
                    Err(error) => {
                        *state = WaitState::Failed(error);
                        notifications.push(notify);
                    }
                }
            }
        }
        notifications
    }
}

fn map_preparation_error(error: AllocError) -> ReserveError {
    match error {
        AllocError::CapacityOverflow => ReserveError::CapacityOverflow,
        _ => ReserveError::PhysicalPreparationFailed,
    }
}

fn notify_all(notifications: Vec<NotifyFn>) {
    for notify in notifications {
        notify();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use std::task::Poll;

    use crate::runtime::buffer_pool::{BufferPool, Reserve};
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

    use super::*;

    fn notify_flag() -> (NotifyFn, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        let notified = Arc::clone(&flag);
        (
            StdArc::new(move || notified.store(true, Ordering::Release)),
            flag,
        )
    }

    fn pending(pool: &BufferPool, plan: ReservationPlan, notify: NotifyFn) -> WaitTicket {
        match pool.reserve(plan, notify).unwrap() {
            Reserve::Ready(reservation) => {
                drop(reservation);
                panic!("reservation unexpectedly granted");
            }
            Reserve::Pending(ticket) => ticket,
        }
    }

    #[test]
    fn grant_racing_cancellation_never_leaks_admission() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let holder = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
            let (notify, _) = notify_flag();
            let ticket = pending(&pool, ReservationPlan::new(1, 1), notify);

            let releasing = thread::spawn(move || drop(holder));
            let cancelling = thread::spawn(move || drop(ticket));
            releasing.join().unwrap();
            cancelling.join().unwrap();

            let snapshot = pool.snapshot();
            assert_eq!(snapshot.admission_used, 0);
            assert_eq!(snapshot.waiters, 0);
        });
    }

    #[test]
    fn grant_is_visible_before_notification_and_take() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let holder = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
            let (notify, notified) = notify_flag();
            let mut ticket = pending(&pool, ReservationPlan::new(1, 1), notify);

            let releasing = thread::spawn(move || drop(holder));
            let taking = thread::spawn(move || {
                let result = ticket.take();
                (ticket, result)
            });

            releasing.join().unwrap();
            let (mut ticket, first) = taking.join().unwrap();
            let reservation = match first {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("grant failed: {error:?}"),
                Poll::Pending => match ticket.take() {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("grant failed: {error:?}"),
                    Poll::Pending => panic!("grant remained pending after release"),
                },
            };

            assert!(notified.load(Ordering::Acquire));
            drop(reservation);
            drop(ticket);
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn notification_observes_grant_without_admission_lock() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let holder = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
            let ticket_slot: Arc<Mutex<Option<WaitTicket>>> = Arc::new(Mutex::new(None));
            let notified_ticket = Arc::clone(&ticket_slot);
            let notify: NotifyFn = StdArc::new(move || {
                let mut ticket = notified_ticket
                    .lock()
                    .take()
                    .expect("ticket installed before notification");
                let reservation = match ticket.take() {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("grant failed: {error:?}"),
                    Poll::Pending => panic!("notification ran before grant publication"),
                };

                // Re-enters admission. This deadlocks if notification runs under its lock.
                drop(reservation);
            });
            let ticket = pending(&pool, ReservationPlan::new(1, 1), notify);
            *ticket_slot.lock() = Some(ticket);

            drop(holder);

            assert!(ticket_slot.lock().is_none());
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn close_and_final_direct_return_wake_waiter_once() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let reservation = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
            let debit = reservation.try_debit(&pool.inner, 1).unwrap();
            let (notify, notified) = notify_flag();
            let mut ticket = pending(&pool, ReservationPlan::new(1, 1), notify);

            let closing = thread::spawn(move || reservation.close_acquisition());
            let returning = thread::spawn(move || drop(debit));
            closing.join().unwrap();
            returning.join().unwrap();

            assert!(notified.load(Ordering::Acquire));
            let next = match ticket.take() {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("grant failed: {error:?}"),
                Poll::Pending => panic!("grant remained pending after pressure retired"),
            };
            drop(next);
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn unreserved_debit_racing_coverage_close_remains_charged() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let reservation = pool.try_reserve(ReservationPlan::new(1, 0)).unwrap();

            let acquiring = pool.clone();
            let debit =
                thread::spawn(move || PoolInner::debit_unreserved(&acquiring.inner, 1).unwrap());
            let closing = thread::spawn(move || reservation.close_acquisition());

            let debit = debit.join().unwrap();
            closing.join().unwrap();

            let snapshot = pool.snapshot();
            assert_eq!(snapshot.unreserved_coverage, 0);
            assert_eq!(snapshot.unreserved_live, 1);
            assert_eq!(snapshot.unreserved_debt, 1);
            assert_eq!(snapshot.admission_used, 1);

            drop(debit);
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn unreserved_return_racing_coverage_close_retires_charge() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let reservation = pool.try_reserve(ReservationPlan::new(1, 0)).unwrap();
            let debit = PoolInner::debit_unreserved(&pool.inner, 1).unwrap();

            let returning = thread::spawn(move || drop(debit));
            let closing = thread::spawn(move || reservation.close_acquisition());
            returning.join().unwrap();
            closing.join().unwrap();

            let snapshot = pool.snapshot();
            assert_eq!(snapshot.unreserved_coverage, 0);
            assert_eq!(snapshot.unreserved_live, 0);
            assert_eq!(snapshot.unreserved_debt, 0);
            assert_eq!(snapshot.admission_used, 0);
        });
    }
}
