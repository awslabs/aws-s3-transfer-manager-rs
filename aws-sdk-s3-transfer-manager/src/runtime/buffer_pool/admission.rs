/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Planned-demand admission and reservation lifetime.
//!
//! An open reservation charges its complete envelope. Direct acquisition
//! debits reservation-local authority but does not change global pressure.
//! Closing consumes the public authority, replaces the envelope with exact
//! surviving direct ownership, and converts uncovered unreserved ownership to
//! debt.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::{AllocError, PoolInner, ReserveError};

/// Phase bit in [`ReservationState::owner_state`].
const RESERVATION_CLOSED: u64 = 1 << 63;
/// Direct owner/debit bits in [`ReservationState::owner_state`].
const RESERVATION_COUNT_MASK: u64 = !RESERVATION_CLOSED;

/// Global admission state protected by `PoolInner::admission`.
pub(super) struct AdmissionState {
    pub(super) ledger: AdmissionLedger,
    pub(super) closed: bool,
}

/// Carrier-granular quantities used to decide new admission.
///
/// Admission pressure is `active_planned_demand + retiring_direct_live +
/// unreserved_debt`.
pub(super) struct AdmissionLedger {
    pub(super) configured: usize,
    pub(super) active_planned_demand: usize,
    pub(super) retiring_direct_live: usize,
    pub(super) unreserved_coverage: usize,
    pub(super) unreserved_live: usize,
    pub(super) unreserved_debt: usize,
}

/// Planned demand and direct-acquisition authority for one work item.
///
/// The difference between `envelope` and `direct_limit` is aggregate coverage
/// for future unreserved acquisition.
#[derive(Clone, Copy)]
pub(super) struct ReservationPlan {
    pub(super) envelope: usize,
    pub(super) direct_limit: usize,
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

/// Accounting inverse performed by a carrier's final owner.
pub(super) enum Charge {
    Direct(Arc<ReservationState>),
    Unreserved,
}

impl AdmissionState {
    /// Create empty admission state with a fixed configured ceiling.
    pub(super) fn new(configured: usize) -> Self {
        Self {
            ledger: AdmissionLedger {
                configured,
                active_planned_demand: 0,
                retiring_direct_live: 0,
                unreserved_coverage: 0,
                unreserved_live: 0,
                unreserved_debt: 0,
            },
            closed: false,
        }
    }

    /// Charge one plan or reject it under current pressure.
    pub(super) fn try_admit(&mut self, plan: ReservationPlan) -> Result<(), ReserveError> {
        if self.closed {
            return Err(ReserveError::Closed);
        }

        if plan.envelope
            > self
                .ledger
                .configured
                .saturating_sub(self.ledger.admission_used())
        {
            return Err(ReserveError::AtCapacity);
        }

        self.ledger.active_planned_demand += plan.envelope;
        self.ledger.unreserved_coverage += plan.unreserved_coverage();
        self.ledger.assert_invariants();
        Ok(())
    }
}

impl AdmissionLedger {
    /// Pressure charged against new planned demand.
    pub(super) fn admission_used(&self) -> usize {
        self.active_planned_demand
            .saturating_add(self.retiring_direct_live)
            .saturating_add(self.unreserved_debt)
    }

    /// Check identities that must hold after every ledger transition.
    fn assert_invariants(&self) {
        assert!(self.unreserved_debt <= self.unreserved_live);
        assert!(
            self.unreserved_live - self.unreserved_debt <= self.unreserved_coverage,
            "covered unreserved ownership must fit active coverage"
        );
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
    pub(super) fn new(pool: Arc<PoolInner>, plan: ReservationPlan) -> Self {
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
        state.try_debit(count)
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

impl ReservationState {
    /// Debit direct authority for an entire physical acquisition attempt.
    ///
    /// The returned guard owns rollback until each unit is transferred to a
    /// carrier guard. Acquire ordering publishes the updated count before a
    /// concurrent carrier return or close observes it.
    fn try_debit(self: &Arc<Self>, count: usize) -> Result<DirectDebit, AllocError> {
        let count = u64::try_from(count).map_err(|_| AllocError::CapacityOverflow)?;
        let direct_limit =
            u64::try_from(self.plan.direct_limit).map_err(|_| AllocError::CapacityOverflow)?;

        let result =
            self.owner_state
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
                reservation: Arc::clone(self),
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
        let mut admission = self.pool.admission.lock().unwrap();
        let previous = self
            .owner_state
            .fetch_or(RESERVATION_CLOSED, Ordering::AcqRel);
        if previous & RESERVATION_CLOSED != 0 {
            return;
        }

        let direct_live = usize::try_from(previous & RESERVATION_COUNT_MASK).unwrap();
        admission.ledger.active_planned_demand -= self.plan.envelope;
        admission.ledger.retiring_direct_live += direct_live;
        admission.ledger.unreserved_coverage -= self.plan.unreserved_coverage();

        let minimum_debt = admission
            .ledger
            .unreserved_live
            .saturating_sub(admission.ledger.unreserved_coverage);
        admission.ledger.unreserved_debt = admission.ledger.unreserved_debt.max(minimum_debt);
        admission.ledger.assert_invariants();
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
            let mut admission = self.pool.admission.lock().unwrap();
            admission.ledger.retiring_direct_live -= usize::try_from(count).unwrap();
            admission.ledger.assert_invariants();
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

impl PoolInner {
    /// Record one unreserved checkout and create debt when coverage is full.
    pub(super) fn record_unreserved_acquire(&self) {
        let mut admission = self.admission.lock().unwrap();
        let covered_live = admission.ledger.unreserved_live - admission.ledger.unreserved_debt;
        if covered_live == admission.ledger.unreserved_coverage {
            admission.ledger.unreserved_debt += 1;
        }
        admission.ledger.unreserved_live += 1;
        admission.ledger.assert_invariants();
    }

    /// Retire one unreserved checkout, repaying sticky debt first.
    pub(super) fn release_unreserved(&self) {
        let mut admission = self.admission.lock().unwrap();
        assert!(
            admission.ledger.unreserved_live > 0,
            "unreserved return requires a live charge"
        );
        admission.ledger.unreserved_live -= 1;
        admission.ledger.unreserved_debt = admission.ledger.unreserved_debt.saturating_sub(1);
        admission.ledger.assert_invariants();
    }
}
