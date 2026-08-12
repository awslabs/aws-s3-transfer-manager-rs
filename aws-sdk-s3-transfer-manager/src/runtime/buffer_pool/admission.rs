/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Planned-demand admission, FIFO waiters, and reservation lifetime.
//!
//! A grant prepares fungible capacity before charging its complete envelope.
//! Waiters receive the charged reservation before notification and never
//! re-contend for released capacity. Closing withdraws the envelope from
//! aggregate coverage; occupied coverage becomes debt until its owner returns.

use std::collections::VecDeque;
use std::sync::Arc as StdArc;
use std::task::Poll;

use crate::runtime::sync::sync::atomic::{AtomicU64, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

use super::{AllocError, PoolInner, ReserveError};

mod coverage;
pub(super) use coverage::CoverageState;
use coverage::{CoverageSnapshot, MAX_CARRIERS};

/// Callback invoked after a pending reservation reaches a terminal state.
pub(super) type NotifyFn = StdArc<dyn Fn() + Send + Sync>;

/// Phase bit in [`ReservationState::owner_state`].
const RESERVATION_CLOSED: u64 = 1 << 63;
/// Direct owner and debit bits in [`ReservationState::owner_state`].
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
/// Admission pressure is active planned demand plus debt sampled from
/// [`CoverageState`]. `prepared` covers that pressure after every completed
/// transition.
pub(super) struct AdmissionLedger {
    /// Soft admission and retained-capacity target.
    pub(super) configured: usize,
    /// Capacity whose mapping and current preparation steps are complete.
    pub(super) prepared: usize,
    /// Full envelopes whose acquisition authority remains open.
    pub(super) active_planned_demand: usize,
}

/// Result of a reservation request that may enter the FIFO.
pub(super) enum Reserve {
    Ready(Reservation),
    Pending(WaitTicket),
}

/// A reservation request held in arrival order.
struct Waiter {
    envelope: usize,
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
    envelope: usize,

    /// The high bit closes acquisition. Remaining bits count direct carrier
    /// guards and in-flight debits that have not become guards.
    owner_state: AtomicU64,
}

/// RAII rollback for accounting debited before physical acquisition.
pub(super) struct AcquisitionDebit {
    pub(super) pool: Arc<PoolInner>,
    pub(super) direct: Option<Arc<ReservationState>>,
    pub(super) uncommitted: usize,
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

    /// Whether `envelope` can be granted under the normal bound or idle escape.
    fn can_grant(&self, envelope: usize, debt: usize) -> bool {
        let admission_used = self.ledger.admission_used(debt);
        let normal = envelope <= self.ledger.configured.saturating_sub(admission_used);

        normal || self.ledger.active_planned_demand == 0
    }
}

impl AdmissionLedger {
    /// Pressure charged against new planned demand.
    pub(super) fn admission_used(&self, debt: usize) -> usize {
        self.active_planned_demand
            .checked_add(debt)
            .expect("admission pressure overflowed")
    }

    /// Charge one envelope after its capacity has been prepared.
    fn admit(&mut self, coverage: &CoverageState, envelope: usize) -> Result<(), ReserveError> {
        self.validate_admit(coverage.snapshot(), envelope)?;
        let active_planned_demand = self
            .active_planned_demand
            .checked_add(envelope)
            .expect("validated active planned demand overflowed");

        coverage
            .add_coverage(envelope)
            .expect("validated coverage must fit packed state");
        self.active_planned_demand = active_planned_demand;
        self.assert_invariants(coverage.snapshot());
        Ok(())
    }

    /// Validate all fallible counters before physical preparation starts.
    fn validate_admit(
        &self,
        coverage: CoverageSnapshot,
        envelope: usize,
    ) -> Result<(), ReserveError> {
        self.active_planned_demand
            .checked_add(envelope)
            .and_then(|active| active.checked_add(coverage.debt))
            .filter(|total| *total <= MAX_CARRIERS)
            .ok_or(ReserveError::CapacityOverflow)?;
        Ok(())
    }

    /// Check identities that must hold after every completed transition.
    fn assert_invariants(&self, coverage: CoverageSnapshot) {
        assert!(
            self.prepared >= self.admission_used(coverage.debt),
            "prepared capacity must cover admitted work and debt"
        );
        assert!(
            coverage.available_coverage <= self.active_planned_demand,
            "available coverage exceeds active coverage"
        );
        assert!(
            self.active_planned_demand + coverage.debt <= MAX_CARRIERS,
            "admission pressure exceeds packed state"
        );
    }

    /// Derive charged acquisitions from one coherent packed sample.
    pub(super) fn charged_live(&self, coverage: CoverageSnapshot) -> usize {
        self.active_planned_demand
            .checked_add(coverage.debt)
            .and_then(|total| total.checked_sub(coverage.available_coverage))
            .expect("aggregate state violates coverage identity")
    }
}

impl Reservation {
    /// Create direct acquisition authority for an admitted envelope.
    fn new(pool: Arc<PoolInner>, envelope: usize) -> Self {
        Self {
            state: Some(Arc::new(ReservationState {
                pool,
                envelope,
                owner_state: AtomicU64::new(0),
            })),
        }
    }

    /// Debit direct authority for one physical acquisition attempt.
    pub(super) fn try_debit(
        &self,
        pool: &Arc<PoolInner>,
        count: usize,
    ) -> Result<AcquisitionDebit, AllocError> {
        let state = self
            .state
            .as_ref()
            .expect("an open reservation retains its state");
        if !Arc::ptr_eq(&state.pool, pool) {
            return Err(AllocError::ForeignReservation);
        }
        ReservationState::try_debit(state, count)?;
        match PoolInner::debit_coverage(pool, count) {
            Ok(mut debit) => {
                debit.direct = Some(Arc::clone(state));
                Ok(debit)
            }
            Err(error) => {
                state.release_direct(count);
                Err(error)
            }
        }
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
    fn try_debit(state: &Arc<Self>, count: usize) -> Result<(), AllocError> {
        let count = u64::try_from(count).map_err(|_| AllocError::CapacityOverflow)?;
        let envelope = u64::try_from(state.envelope).map_err(|_| AllocError::CapacityOverflow)?;

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
                        .filter(|next| *next <= envelope)
                });

        match result {
            Ok(_) => Ok(()),
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

    /// Withdraw the active envelope and convert occupied coverage to debt.
    fn close_acquisition(&self) {
        let notifications = {
            let mut admission = self.pool.admission.lock();
            let previous = self
                .owner_state
                .fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
            if previous & RESERVATION_CLOSED != 0 {
                return;
            }

            admission.ledger.active_planned_demand = admission
                .ledger
                .active_planned_demand
                .checked_sub(self.envelope)
                .expect("reservation close exceeded active planned demand");
            self.pool.coverage.remove_coverage(self.envelope);

            admission
                .ledger
                .assert_invariants(self.pool.coverage.snapshot());
            PoolInner::drain_fifo_locked(&self.pool, &mut admission)
        };
        notify_all(notifications);
    }

    /// Restore reservation-local authority.
    pub(super) fn release_direct(&self, count: usize) {
        let count = u64::try_from(count).expect("direct release count fits owner state");
        let previous = self.owner_state.fetch_sub(count, Ordering::AcqRel);
        assert!(
            previous & RESERVATION_COUNT_MASK >= count,
            "direct return exceeds outstanding ownership"
        );
    }
}

impl Drop for AcquisitionDebit {
    fn drop(&mut self) {
        if self.uncommitted > 0 {
            if let Some(reservation) = &self.direct {
                reservation.release_direct(self.uncommitted);
            }
            PoolInner::release_coverage(&self.pool, self.uncommitted);
        }
    }
}

impl PoolInner {
    /// Grant `envelope` immediately without bypassing an existing waiter.
    pub(super) fn try_reserve(
        pool: &Arc<Self>,
        envelope: usize,
    ) -> Result<Reservation, ReserveError> {
        let mut admission = pool.admission.lock();
        if admission.closed {
            return Err(ReserveError::Closed);
        }
        let debt = pool.coverage.snapshot().debt;
        if !admission.waiters.is_empty() || !admission.can_grant(envelope, debt) {
            return Err(ReserveError::AtCapacity);
        }
        PoolInner::prepare_and_grant_locked(pool, &mut admission, envelope)
    }

    /// Grant `envelope` immediately or append it to the reservation FIFO.
    pub(super) fn reserve(
        pool: &Arc<Self>,
        envelope: usize,
        notify: NotifyFn,
    ) -> Result<Reserve, ReserveError> {
        let mut admission = pool.admission.lock();
        if admission.closed {
            return Err(ReserveError::Closed);
        }
        let debt = pool.coverage.snapshot().debt;
        if admission.waiters.is_empty() && admission.can_grant(envelope, debt) {
            let reservation = PoolInner::prepare_and_grant_locked(pool, &mut admission, envelope)?;
            return Ok(Reserve::Ready(reservation));
        }

        let slot = Arc::new(WaitSlot {
            state: Mutex::new(WaitState::Queued),
        });
        admission.waiters.push_back(Waiter {
            envelope,
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

    /// Charge an acquisition and prepare any newly exposed debt.
    ///
    /// Charging precedes physical claim so unreserved traffic cannot consume
    /// the only carrier backing an existing reservation without first growing
    /// replacement capacity.
    pub(super) fn debit_coverage(
        pool: &Arc<Self>,
        count: usize,
    ) -> Result<AcquisitionDebit, AllocError> {
        if pool
            .coverage
            .try_debit_covered(count)
            .map_err(|_| AllocError::CapacityOverflow)?
        {
            return Ok(AcquisitionDebit {
                pool: Arc::clone(pool),
                direct: None,
                uncommitted: count,
            });
        }

        let mut admission = pool.admission.lock();
        let maximum_debt = MAX_CARRIERS
            .checked_sub(admission.ledger.active_planned_demand)
            .ok_or(AllocError::CapacityOverflow)?;
        let debit = pool
            .coverage
            .debit(count, maximum_debt)
            .map_err(|_| AllocError::CapacityOverflow)?;

        if debit.new_debt > 0 {
            let coverage = pool.coverage.snapshot();
            let target = admission.ledger.admission_used(coverage.debt);
            if let Err(error) = pool
                .arena
                .prepare_to(target, &mut admission.ledger.prepared)
            {
                pool.coverage.release(count);
                admission.ledger.assert_invariants(pool.coverage.snapshot());
                return Err(error);
            }
        }
        admission.ledger.assert_invariants(pool.coverage.snapshot());
        Ok(AcquisitionDebit {
            pool: Arc::clone(pool),
            direct: None,
            uncommitted: count,
        })
    }

    /// Retire aggregate charges, repaying sticky debt first.
    pub(super) fn release_coverage(pool: &Arc<Self>, count: usize) {
        let release = pool.coverage.release(count);
        if release.repaid_debt == 0 {
            return;
        }

        let notifications = {
            let mut admission = pool.admission.lock();
            admission.ledger.assert_invariants(pool.coverage.snapshot());
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

    /// Prepare and charge one envelope while admission is serialized.
    fn prepare_and_grant_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionState,
        envelope: usize,
    ) -> Result<Reservation, ReserveError> {
        admission
            .ledger
            .validate_admit(pool.coverage.snapshot(), envelope)?;
        PoolInner::prepare_envelope_locked(pool, admission, envelope)?;
        admission.ledger.admit(&pool.coverage, envelope)?;
        Ok(Reservation::new(Arc::clone(pool), envelope))
    }

    /// Prepare capacity for `envelope` without changing admission pressure.
    fn prepare_envelope_locked(
        pool: &Arc<Self>,
        admission: &mut AdmissionState,
        envelope: usize,
    ) -> Result<(), ReserveError> {
        let target = admission
            .ledger
            .admission_used(pool.coverage.snapshot().debt)
            .checked_add(envelope)
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
            let envelope = front.envelope;
            let slot = Arc::clone(&front.slot);
            let notify = StdArc::clone(&front.notify);

            if !matches!(*slot.state.lock(), WaitState::Queued) {
                admission.waiters.pop_front();
                continue;
            }
            if !admission.can_grant(envelope, pool.coverage.snapshot().debt) {
                break;
            }

            if let Err(error) = admission
                .ledger
                .validate_admit(pool.coverage.snapshot(), envelope)
            {
                admission.waiters.pop_front();
                let mut state = slot.state.lock();
                if matches!(*state, WaitState::Queued) {
                    *state = WaitState::Failed(error);
                    notifications.push(notify);
                }
                continue;
            }

            if let Err(error) = PoolInner::prepare_envelope_locked(pool, admission, envelope) {
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
                match admission.ledger.admit(&pool.coverage, envelope) {
                    Ok(()) => {
                        *state = WaitState::Granted(Reservation::new(Arc::clone(pool), envelope));
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

    fn pending(pool: &BufferPool, envelope: usize, notify: NotifyFn) -> WaitTicket {
        match pool.reserve(envelope, notify).unwrap() {
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
            let holder = pool.try_reserve(1).unwrap();
            let (notify, _) = notify_flag();
            let ticket = pending(&pool, 1, notify);

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
            let holder = pool.try_reserve(1).unwrap();
            let (notify, notified) = notify_flag();
            let mut ticket = pending(&pool, 1, notify);

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
            let holder = pool.try_reserve(1).unwrap();
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
            let ticket = pending(&pool, 1, notify);
            *ticket_slot.lock() = Some(ticket);

            drop(holder);

            assert!(ticket_slot.lock().is_none());
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn close_and_final_direct_return_wake_waiter_once() {
        loom::model(|| {
            let pool = BufferPool::new(4, 2);
            let anchor = pool.try_reserve(1).unwrap();
            let reservation = pool.try_reserve(1).unwrap();
            let unreserved = PoolInner::debit_coverage(&pool.inner, 1).unwrap();
            let debit = reservation.try_debit(&pool.inner, 1).unwrap();
            let (notify, notified) = notify_flag();
            let mut ticket = pending(&pool, 1, notify);

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
            drop(anchor);
            drop(unreserved);
            assert_eq!(pool.snapshot().admission_used, 0);
        });
    }

    #[test]
    fn unreserved_debit_racing_coverage_close_remains_charged() {
        loom::model(|| {
            let pool = BufferPool::new(4, 1);
            let reservation = pool.try_reserve(1).unwrap();

            let acquiring = pool.clone();
            let debit =
                thread::spawn(move || PoolInner::debit_coverage(&acquiring.inner, 1).unwrap());
            let closing = thread::spawn(move || reservation.close_acquisition());

            let debit = debit.join().unwrap();
            closing.join().unwrap();

            let snapshot = pool.snapshot();
            assert_eq!(snapshot.available_coverage, 0);
            assert_eq!(snapshot.charged_live, 1);
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
            let reservation = pool.try_reserve(1).unwrap();
            let debit = PoolInner::debit_coverage(&pool.inner, 1).unwrap();

            let returning = thread::spawn(move || drop(debit));
            let closing = thread::spawn(move || reservation.close_acquisition());
            returning.join().unwrap();
            closing.join().unwrap();

            let snapshot = pool.snapshot();
            assert_eq!(snapshot.available_coverage, 0);
            assert_eq!(snapshot.charged_live, 0);
            assert_eq!(snapshot.unreserved_debt, 0);
            assert_eq!(snapshot.admission_used, 0);
        });
    }
}
