/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Deterministic model for elastic buffer-pool admission and acquisition.
//!
//! This is intentionally test-only. It separates planned-demand admission from
//! physical carrier ownership before production concurrency obscures the state
//! transitions.
//!
//! Admission usage is reserved demand plus sticky unreserved debt. New
//! reservations provide coverage only for future unreserved acquisitions; they
//! cannot retroactively absorb already-live debt.

use std::collections::{HashMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DomainId(u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DomainSet(u128);

impl DomainSet {
    fn from_domains(domains: impl IntoIterator<Item = DomainId>) -> Self {
        let mut bits = 0u128;
        for DomainId(domain) in domains {
            assert!(domain < 128, "model supports at most 128 domains");
            bits |= 1u128 << domain;
        }
        Self(bits)
    }

    fn contains(self, DomainId(domain): DomainId) -> bool {
        self.0 & (1u128 << domain) != 0
    }

    fn is_empty(self) -> bool {
        self.0 == 0
    }

    fn is_subset_of(self, other: Self) -> bool {
        self.0 & !other.0 == 0
    }

    fn first(self) -> Option<DomainId> {
        (!self.is_empty()).then(|| DomainId(self.0.trailing_zeros() as u8))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DispatchAffinity {
    eligible: DomainSet,
    preferred: DomainSet,
}

impl DispatchAffinity {
    fn new(eligible: DomainSet, preferred: DomainSet) -> Self {
        assert!(!eligible.is_empty(), "at least one domain must be eligible");
        assert!(
            preferred.is_subset_of(eligible),
            "preferred domains must be eligible"
        );
        Self {
            eligible,
            preferred,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct RingId(u8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapabilityRequirement {
    Ordinary,
    Fixed(RingId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CarrierCapability {
    Cpu,
    Fixed(RingId),
}

impl CapabilityRequirement {
    fn accepts(self, capability: CarrierCapability) -> bool {
        match (self, capability) {
            (Self::Ordinary, _) => true,
            (Self::Fixed(required), CarrierCapability::Fixed(actual)) => required == actual,
            (Self::Fixed(_), CarrierCapability::Cpu) => false,
        }
    }

    fn materialized(self) -> CarrierCapability {
        match self {
            Self::Ordinary => CarrierCapability::Cpu,
            Self::Fixed(ring) => CarrierCapability::Fixed(ring),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AcquireRequest {
    minimum_capacity: usize,
    alignment: usize,
    capability: CapabilityRequirement,
    affinity: DispatchAffinity,
}

impl AcquireRequest {
    fn new(
        minimum_capacity: usize,
        alignment: usize,
        capability: CapabilityRequirement,
        affinity: DispatchAffinity,
    ) -> Self {
        assert!(minimum_capacity > 0);
        assert!(alignment.is_power_of_two());
        Self {
            minimum_capacity,
            alignment,
            capability,
            affinity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ReservationId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WaiterId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CarrierId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReservationPlan {
    reservation_size: usize,
    direct_acquire_limit: usize,
    capability: CapabilityRequirement,
    affinity: DispatchAffinity,
}

impl ReservationPlan {
    fn new(
        reservation_size: usize,
        direct_acquire_limit: usize,
        capability: CapabilityRequirement,
        affinity: DispatchAffinity,
    ) -> Self {
        assert!(reservation_size > 0);
        assert!(direct_acquire_limit <= reservation_size);
        Self {
            reservation_size,
            direct_acquire_limit,
            capability,
            affinity,
        }
    }

    fn unreserved_coverage(self) -> usize {
        self.reservation_size - self.direct_acquire_limit
    }
}

#[derive(Debug)]
struct ReservationState {
    plan: ReservationPlan,
    direct_live: usize,
    closing: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmissionWaiter {
    id: WaiterId,
    plan: ReservationPlan,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReserveOutcome {
    Ready(ReservationId),
    Pending(WaiterId),
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GrantedWaiter {
    waiter: WaiterId,
    reservation: ReservationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimOwner {
    Reserved(ReservationId),
    Unreserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcquisitionSource {
    Reused,
    RetainedGrowth,
    Overflow,
}

#[derive(Debug)]
struct ReusableCarrier {
    id: CarrierId,
    home: DomainId,
    alignment: usize,
    capability: CarrierCapability,
    owner: Option<ClaimOwner>,
}

#[derive(Debug)]
struct OverflowCarrier {
    home: DomainId,
    alignment: usize,
    capability: CarrierCapability,
    owner: ClaimOwner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaimedCarrier {
    id: CarrierId,
    home: DomainId,
    alignment: usize,
    capability: CarrierCapability,
    source: AcquisitionSource,
    owner: ClaimOwner,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AcquisitionStats {
    reused: usize,
    retained_growth: usize,
    overflow: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelError {
    UnknownReservation,
    UnknownWaiter,
    ReservationClosing,
    ReservationEnvelopeExceeded,
    RequirementMismatch,
    UnsupportedLayout,
    AllocationFailed,
    WrongCarrierOwner,
}

#[derive(Debug)]
struct ElasticPoolModel {
    carrier_size: usize,

    configured_capacity: usize,
    reserved_demand: usize,
    live: usize,
    unreserved_live: usize,
    unreserved_debt: usize,
    reservations: HashMap<ReservationId, ReservationState>,
    waiters: VecDeque<AdmissionWaiter>,

    reusable_capacity_target: usize,
    retained: Vec<ReusableCarrier>,
    overflow: HashMap<CarrierId, OverflowCarrier>,

    next_reservation: u64,
    next_waiter: u64,
    next_carrier: u64,
    fail_next_allocation: bool,
    stats: AcquisitionStats,
}

impl ElasticPoolModel {
    fn new(carrier_size: usize, configured_capacity: usize) -> Self {
        assert!(carrier_size > 0);
        assert!(configured_capacity > 0);
        Self {
            carrier_size,
            configured_capacity,
            reserved_demand: 0,
            live: 0,
            unreserved_live: 0,
            unreserved_debt: 0,
            reservations: HashMap::new(),
            waiters: VecDeque::new(),
            reusable_capacity_target: configured_capacity,
            retained: Vec::new(),
            overflow: HashMap::new(),
            next_reservation: 0,
            next_waiter: 0,
            next_carrier: 0,
            fail_next_allocation: false,
            stats: AcquisitionStats::default(),
        }
    }

    fn reserve(&mut self, plan: ReservationPlan) -> ReserveOutcome {
        if plan.reservation_size > self.configured_capacity {
            return ReserveOutcome::Rejected;
        }
        if self.waiters.is_empty() && self.can_reserve(plan.reservation_size) {
            let reservation = self.grant(plan);
            self.assert_invariants();
            return ReserveOutcome::Ready(reservation);
        }

        let waiter = WaiterId(self.next_waiter);
        self.next_waiter += 1;
        self.waiters.push_back(AdmissionWaiter { id: waiter, plan });
        self.assert_invariants();
        ReserveOutcome::Pending(waiter)
    }

    fn grant(&mut self, plan: ReservationPlan) -> ReservationId {
        assert!(self.can_reserve(plan.reservation_size));
        let reservation = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        self.reserved_demand += plan.reservation_size;
        let previous = self.reservations.insert(
            reservation,
            ReservationState {
                plan,
                direct_live: 0,
                closing: false,
            },
        );
        assert!(previous.is_none());
        reservation
    }

    fn close(&mut self, reservation: ReservationId) -> Result<Vec<GrantedWaiter>, ModelError> {
        let release_now = {
            let state = self
                .reservations
                .get_mut(&reservation)
                .ok_or(ModelError::UnknownReservation)?;
            state.closing = true;
            state.direct_live == 0
        };

        if release_now {
            self.remove_reservation(reservation);
        }
        let grants = self.drain_waiters();
        self.assert_invariants();
        Ok(grants)
    }

    fn remove_reservation(&mut self, reservation: ReservationId) {
        let state = self
            .reservations
            .remove(&reservation)
            .expect("reservation must exist while releasing admission");
        assert!(state.closing);
        assert_eq!(state.direct_live, 0);
        self.reserved_demand -= state.plan.reservation_size;

        // Unreserved owners cannot be attributed to the reservation whose
        // coverage disappeared. Preserve them as sticky debt instead of
        // allowing a later reservation to absorb their existing demand.
        let minimum_debt = self
            .unreserved_live
            .saturating_sub(self.unreserved_coverage());
        self.unreserved_debt = self.unreserved_debt.max(minimum_debt);
    }

    fn drain_waiters(&mut self) -> Vec<GrantedWaiter> {
        let mut granted = Vec::new();
        while let Some(waiter) = self.waiters.front() {
            if !self.can_reserve(waiter.plan.reservation_size) {
                break;
            }
            let waiter = self.waiters.pop_front().unwrap();
            granted.push(GrantedWaiter {
                waiter: waiter.id,
                reservation: self.grant(waiter.plan),
            });
        }
        granted
    }

    fn cancel_waiter(&mut self, waiter: WaiterId) -> Result<Vec<GrantedWaiter>, ModelError> {
        let index = self
            .waiters
            .iter()
            .position(|candidate| candidate.id == waiter)
            .ok_or(ModelError::UnknownWaiter)?;
        self.waiters.remove(index);
        let grants = self.drain_waiters();
        self.assert_invariants();
        Ok(grants)
    }

    fn acquire(
        &mut self,
        reservation: ReservationId,
        request: AcquireRequest,
    ) -> Result<ClaimedCarrier, ModelError> {
        {
            let state = self
                .reservations
                .get_mut(&reservation)
                .ok_or(ModelError::UnknownReservation)?;
            if state.closing {
                return Err(ModelError::ReservationClosing);
            }
            if state.direct_live == state.plan.direct_acquire_limit {
                return Err(ModelError::ReservationEnvelopeExceeded);
            }
            if request.capability != state.plan.capability
                || !request
                    .affinity
                    .eligible
                    .is_subset_of(state.plan.affinity.eligible)
            {
                return Err(ModelError::RequirementMismatch);
            }
            state.direct_live += 1;
        }

        match self.acquire_physical(ClaimOwner::Reserved(reservation), request) {
            Ok(claim) => {
                self.live += 1;
                self.assert_invariants();
                Ok(claim)
            }
            Err(error) => {
                self.reservations
                    .get_mut(&reservation)
                    .expect("reservation remains live during acquisition")
                    .direct_live -= 1;
                self.assert_invariants();
                Err(error)
            }
        }
    }

    fn acquire_unreserved(
        &mut self,
        request: AcquireRequest,
    ) -> Result<ClaimedCarrier, ModelError> {
        let claim = self.acquire_physical(ClaimOwner::Unreserved, request)?;
        let covered_live = self.unreserved_live - self.unreserved_debt;
        if covered_live == self.unreserved_coverage() {
            self.unreserved_debt += 1;
        }
        self.unreserved_live += 1;
        self.live += 1;
        self.assert_invariants();
        Ok(claim)
    }

    fn acquire_physical(
        &mut self,
        owner: ClaimOwner,
        request: AcquireRequest,
    ) -> Result<ClaimedCarrier, ModelError> {
        self.validate_layout(request)?;

        if let Some(index) = self.find_reusable(request) {
            let carrier = &mut self.retained[index];
            carrier.owner = Some(owner);
            self.stats.reused += 1;
            return Ok(ClaimedCarrier {
                id: carrier.id,
                home: carrier.home,
                alignment: carrier.alignment,
                capability: carrier.capability,
                source: AcquisitionSource::Reused,
                owner,
            });
        }

        let home = request
            .affinity
            .preferred
            .first()
            .or_else(|| request.affinity.eligible.first())
            .expect("AcquireRequest affinity is validated at construction");
        let capability = request.capability.materialized();

        if self.retained.len() < self.reusable_capacity_target {
            self.allocate()?;
            let id = self.next_carrier_id();
            self.retained.push(ReusableCarrier {
                id,
                home,
                alignment: request.alignment,
                capability,
                owner: Some(owner),
            });
            self.stats.retained_growth += 1;
            return Ok(ClaimedCarrier {
                id,
                home,
                alignment: request.alignment,
                capability,
                source: AcquisitionSource::RetainedGrowth,
                owner,
            });
        }

        self.allocate()?;
        let id = self.next_carrier_id();
        let previous = self.overflow.insert(
            id,
            OverflowCarrier {
                home,
                alignment: request.alignment,
                capability,
                owner,
            },
        );
        assert!(previous.is_none());
        self.stats.overflow += 1;
        Ok(ClaimedCarrier {
            id,
            home,
            alignment: request.alignment,
            capability,
            source: AcquisitionSource::Overflow,
            owner,
        })
    }

    fn validate_layout(&self, request: AcquireRequest) -> Result<(), ModelError> {
        if request.minimum_capacity > self.carrier_size
            || !request.alignment.is_power_of_two()
            || request.affinity.eligible.is_empty()
            || !request
                .affinity
                .preferred
                .is_subset_of(request.affinity.eligible)
        {
            return Err(ModelError::UnsupportedLayout);
        }
        Ok(())
    }

    fn find_reusable(&self, request: AcquireRequest) -> Option<usize> {
        let compatible = |carrier: &&ReusableCarrier| {
            carrier.owner.is_none()
                && request.affinity.eligible.contains(carrier.home)
                && request.capability.accepts(carrier.capability)
                && carrier.alignment >= request.alignment
                && carrier.alignment.is_multiple_of(request.alignment)
        };

        self.retained
            .iter()
            .enumerate()
            .filter(|(_, carrier)| compatible(carrier))
            .find(|(_, carrier)| request.affinity.preferred.contains(carrier.home))
            .map(|(index, _)| index)
            .or_else(|| {
                self.retained
                    .iter()
                    .enumerate()
                    .find(|(_, carrier)| compatible(carrier))
                    .map(|(index, _)| index)
            })
    }

    fn allocate(&mut self) -> Result<(), ModelError> {
        if self.fail_next_allocation {
            self.fail_next_allocation = false;
            return Err(ModelError::AllocationFailed);
        }
        Ok(())
    }

    fn next_carrier_id(&mut self) -> CarrierId {
        let id = CarrierId(self.next_carrier);
        self.next_carrier += 1;
        id
    }

    fn release(&mut self, claim: ClaimedCarrier) -> Result<Vec<GrantedWaiter>, ModelError> {
        match claim.source {
            AcquisitionSource::Reused | AcquisitionSource::RetainedGrowth => {
                let index = self
                    .retained
                    .iter()
                    .position(|carrier| carrier.id == claim.id)
                    .ok_or(ModelError::WrongCarrierOwner)?;
                let carrier = &self.retained[index];
                if carrier.owner != Some(claim.owner)
                    || carrier.home != claim.home
                    || carrier.alignment != claim.alignment
                    || carrier.capability != claim.capability
                {
                    return Err(ModelError::WrongCarrierOwner);
                }
                self.retained[index].owner = None;

                // Lowering the reuse policy turns returned excess carriers
                // into trim-on-return storage.
                if self.retained.len() > self.reusable_capacity_target {
                    self.retained.remove(index);
                }
            }
            AcquisitionSource::Overflow => {
                let carrier = self
                    .overflow
                    .get(&claim.id)
                    .ok_or(ModelError::WrongCarrierOwner)?;
                if carrier.owner != claim.owner
                    || carrier.home != claim.home
                    || carrier.alignment != claim.alignment
                    || carrier.capability != claim.capability
                {
                    return Err(ModelError::WrongCarrierOwner);
                }
                self.overflow.remove(&claim.id);
            }
        }

        self.live -= 1;
        match claim.owner {
            ClaimOwner::Unreserved => {
                self.unreserved_live -= 1;
                self.unreserved_debt = self.unreserved_debt.saturating_sub(1);
            }
            ClaimOwner::Reserved(reservation) => {
                let release_admission = {
                    let state = self
                        .reservations
                        .get_mut(&reservation)
                        .ok_or(ModelError::UnknownReservation)?;
                    assert!(state.direct_live > 0);
                    state.direct_live -= 1;
                    state.closing && state.direct_live == 0
                };
                if release_admission {
                    self.remove_reservation(reservation);
                }
            }
        }
        let grants = self.drain_waiters();
        self.assert_invariants();
        Ok(grants)
    }

    fn set_reusable_capacity_target(&mut self, target: usize) {
        assert!(target <= self.configured_capacity);
        self.reusable_capacity_target = target;
        while self.retained.len() > target {
            let Some(index) = self
                .retained
                .iter()
                .rposition(|carrier| carrier.owner.is_none())
            else {
                break;
            };
            self.retained.remove(index);
        }
        self.assert_invariants();
    }

    fn fail_next_allocation(&mut self) {
        self.fail_next_allocation = true;
    }

    fn can_reserve(&self, amount: usize) -> bool {
        amount
            <= self
                .configured_capacity
                .saturating_sub(self.admission_used())
    }

    fn admission_used(&self) -> usize {
        self.reserved_demand.saturating_add(self.unreserved_debt)
    }

    fn unreserved_coverage(&self) -> usize {
        self.reservations
            .values()
            .map(|reservation| reservation.plan.unreserved_coverage())
            .sum()
    }

    fn reserved_demand(&self) -> usize {
        self.reserved_demand
    }

    fn live(&self) -> usize {
        self.live
    }

    fn reusable_capacity(&self) -> usize {
        self.retained.len()
    }

    fn reusable_live(&self) -> usize {
        self.retained
            .iter()
            .filter(|carrier| carrier.owner.is_some())
            .count()
    }

    fn overflow_live(&self) -> usize {
        self.overflow.len()
    }

    fn free_reusable(&self) -> usize {
        self.retained
            .iter()
            .filter(|carrier| carrier.owner.is_none())
            .count()
    }

    fn mapped(&self) -> usize {
        self.retained.len() + self.overflow.len()
    }

    fn unreserved_debt(&self) -> usize {
        self.unreserved_debt
    }

    fn reservation_live(&self, reservation: ReservationId) -> usize {
        self.reservations[&reservation].direct_live
    }

    fn stats(&self) -> AcquisitionStats {
        self.stats
    }

    fn assert_invariants(&self) {
        let reserved_from_reservations: usize = self
            .reservations
            .values()
            .map(|reservation| reservation.plan.reservation_size)
            .sum();
        assert_eq!(self.reserved_demand, reserved_from_reservations);
        assert!(self.reserved_demand <= self.configured_capacity);
        assert!(self.reusable_capacity_target <= self.configured_capacity);

        for (reservation_id, reservation) in &self.reservations {
            assert!(reservation.direct_live <= reservation.plan.direct_acquire_limit);
            let physical_live = self
                .retained
                .iter()
                .filter(|carrier| carrier.owner == Some(ClaimOwner::Reserved(*reservation_id)))
                .count()
                + self
                    .overflow
                    .values()
                    .filter(|carrier| carrier.owner == ClaimOwner::Reserved(*reservation_id))
                    .count();
            assert_eq!(reservation.direct_live, physical_live);
        }

        let physical_unreserved_live = self
            .retained
            .iter()
            .filter(|carrier| carrier.owner == Some(ClaimOwner::Unreserved))
            .count()
            + self
                .overflow
                .values()
                .filter(|carrier| carrier.owner == ClaimOwner::Unreserved)
                .count();
        assert_eq!(self.unreserved_live, physical_unreserved_live);
        assert!(self.unreserved_debt <= self.unreserved_live);
        assert!(
            self.unreserved_live - self.unreserved_debt <= self.unreserved_coverage(),
            "covered unreserved owners must fit active reservation coverage"
        );

        let physical_live = self.reusable_live() + self.overflow.len();
        assert_eq!(self.live, physical_live);
        assert!(
            self.live <= self.admission_used(),
            "reservation plus sticky debt must cover all live carriers"
        );
        assert_eq!(self.free_reusable() + self.live, self.mapped());

        let mut ids: Vec<_> = self.retained.iter().map(|carrier| carrier.id).collect();
        ids.extend(self.overflow.keys().copied());
        ids.sort_by_key(|id| id.0);
        ids.dedup();
        assert_eq!(ids.len(), self.retained.len() + self.overflow.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    fn domain(value: u8) -> DomainId {
        DomainId(value)
    }

    fn affinity(eligible: &[u8], preferred: &[u8]) -> DispatchAffinity {
        DispatchAffinity::new(
            DomainSet::from_domains(eligible.iter().copied().map(domain)),
            DomainSet::from_domains(preferred.iter().copied().map(domain)),
        )
    }

    fn ordinary_request() -> AcquireRequest {
        AcquireRequest::new(
            MIB,
            4096,
            CapabilityRequirement::Ordinary,
            affinity(&[0, 1], &[0]),
        )
    }

    fn upload_plan(carriers: usize) -> ReservationPlan {
        ReservationPlan::new(
            carriers,
            carriers,
            CapabilityRequirement::Ordinary,
            affinity(&[0, 1], &[0]),
        )
    }

    fn download_plan(carriers: usize) -> ReservationPlan {
        ReservationPlan::new(
            carriers,
            0,
            CapabilityRequirement::Ordinary,
            affinity(&[0, 1], &[0]),
        )
    }

    fn model(configured_capacity: usize, reusable_capacity_target: usize) -> ElasticPoolModel {
        let mut pool = ElasticPoolModel::new(MIB, configured_capacity);
        pool.set_reusable_capacity_target(reusable_capacity_target);
        pool
    }

    #[test]
    fn reusable_target_does_not_cap_live_acquisition() {
        let mut pool = model(8, 2);
        let ReserveOutcome::Ready(download) = pool.reserve(download_plan(8)) else {
            panic!("download should be admitted");
        };

        let first = pool.acquire_unreserved(ordinary_request()).unwrap();
        let second = pool.acquire_unreserved(ordinary_request()).unwrap();
        let third = pool.acquire_unreserved(ordinary_request()).unwrap();

        assert_eq!(pool.reserved_demand(), 8);
        assert_eq!(pool.reusable_capacity(), 2);
        assert_eq!(pool.overflow_live(), 1);
        assert_eq!(third.source, AcquisitionSource::Overflow);

        pool.release(first).unwrap();
        pool.release(second).unwrap();
        pool.release(third).unwrap();
        pool.close(download).unwrap();
    }

    #[test]
    fn unreserved_acquisition_requires_no_active_reservation() {
        let mut pool = model(8, 1);
        let claim = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(claim.source, AcquisitionSource::RetainedGrowth);
        assert_eq!(pool.reserved_demand(), 0);
        assert_eq!(pool.unreserved_debt(), 1);
        pool.release(claim).unwrap();
        assert_eq!(pool.live(), 0);
        assert_eq!(pool.unreserved_debt(), 0);
        assert_eq!(pool.free_reusable(), 1);
        assert_eq!(pool.mapped(), 1);
    }

    #[test]
    fn new_reservations_do_not_absorb_existing_unreserved_debt() {
        let mut pool = model(3, 3);
        let ReserveOutcome::Ready(active) = pool.reserve(download_plan(1)) else {
            panic!("first download should be admitted");
        };
        let first = pool.acquire_unreserved(ordinary_request()).unwrap();
        let second = pool.acquire_unreserved(ordinary_request()).unwrap();

        assert_eq!(pool.reserved_demand(), 1);
        assert_eq!(pool.live(), 2);
        assert_eq!(pool.unreserved_debt(), 1);
        assert_eq!(pool.admission_used(), 2);

        let ReserveOutcome::Ready(second_reservation) = pool.reserve(download_plan(1)) else {
            panic!("one unit of admission headroom should remain");
        };
        assert_eq!(pool.reserved_demand(), 2);
        assert_eq!(pool.unreserved_debt(), 1);
        assert_eq!(pool.admission_used(), 3);

        // The new reservation covers future unreserved demand without erasing
        // the debt that was already live when it was granted.
        let future = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(pool.live(), 3);
        assert_eq!(pool.unreserved_debt(), 1);

        let ReserveOutcome::Pending(waiter) = pool.reserve(download_plan(1)) else {
            panic!("sticky debt must prevent another reservation");
        };

        let grants = pool.release(first).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);
        pool.release(second).unwrap();
        pool.release(future).unwrap();
        pool.close(active).unwrap();
        pool.close(second_reservation).unwrap();
        pool.close(grants[0].reservation).unwrap();
    }

    #[test]
    fn reusable_capacity_is_preferred_before_growth_or_overflow() {
        let mut pool = model(8, 2);
        let first = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(first.source, AcquisitionSource::RetainedGrowth);
        pool.release(first).unwrap();

        let reused = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(reused.source, AcquisitionSource::Reused);
        assert_eq!(
            pool.stats(),
            AcquisitionStats {
                reused: 1,
                retained_growth: 1,
                overflow: 0,
            }
        );
        pool.release(reused).unwrap();
    }

    #[test]
    fn overflow_is_destroyed_on_return() {
        let mut pool = model(8, 1);
        let retained = pool.acquire_unreserved(ordinary_request()).unwrap();
        let overflow = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(overflow.source, AcquisitionSource::Overflow);
        assert_eq!(pool.overflow_live(), 1);

        pool.release(overflow).unwrap();
        assert_eq!(pool.overflow_live(), 0);
        assert_eq!(pool.reusable_capacity(), 1);
        pool.release(retained).unwrap();
    }

    #[test]
    fn unreserved_use_of_free_capacity_does_not_block_reserved_growth() {
        let mut pool = model(2, 1);
        let ReserveOutcome::Ready(upload) = pool.reserve(upload_plan(1)) else {
            panic!("upload should be admitted");
        };
        let unreserved = pool.acquire_unreserved(ordinary_request()).unwrap();
        let reserved = pool.acquire(upload, ordinary_request()).unwrap();

        assert_eq!(unreserved.source, AcquisitionSource::RetainedGrowth);
        assert_eq!(reserved.source, AcquisitionSource::Overflow);
        assert_eq!(pool.reservation_live(upload), 1);

        pool.release(unreserved).unwrap();
        pool.close(upload).unwrap();
        pool.release(reserved).unwrap();
    }

    #[test]
    fn admission_being_full_does_not_reject_unreserved_acquisition() {
        let mut pool = model(1, 0);
        let ReserveOutcome::Ready(reservation) = pool.reserve(download_plan(1)) else {
            panic!("download should be admitted");
        };

        let claim = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(claim.source, AcquisitionSource::Overflow);
        assert_eq!(pool.reserved_demand(), 1);

        pool.release(claim).unwrap();
        pool.close(reservation).unwrap();
    }

    #[test]
    fn reservation_envelope_limits_direct_live_ownership_only() {
        let mut pool = model(4, 4);
        let ReserveOutcome::Ready(upload) = pool.reserve(upload_plan(1)) else {
            panic!("upload should be admitted");
        };

        let reserved = pool.acquire(upload, ordinary_request()).unwrap();
        assert_eq!(
            pool.acquire(upload, ordinary_request()),
            Err(ModelError::ReservationEnvelopeExceeded)
        );

        let unreserved = pool.acquire_unreserved(ordinary_request()).unwrap();
        pool.release(unreserved).unwrap();
        pool.release(reserved).unwrap();
        pool.close(upload).unwrap();
    }

    #[test]
    fn allocation_failure_rolls_back_reservation_live_count() {
        let mut pool = model(2, 1);
        let ReserveOutcome::Ready(upload) = pool.reserve(upload_plan(1)) else {
            panic!("upload should be admitted");
        };
        pool.fail_next_allocation();

        assert_eq!(
            pool.acquire(upload, ordinary_request()),
            Err(ModelError::AllocationFailed)
        );
        assert_eq!(pool.reservation_live(upload), 0);

        let retry = pool.acquire(upload, ordinary_request()).unwrap();
        pool.release(retry).unwrap();
        pool.close(upload).unwrap();
    }

    #[test]
    fn closing_reserved_reservation_waits_for_final_owner() {
        let mut pool = model(1, 1);
        let ReserveOutcome::Ready(first) = pool.reserve(upload_plan(1)) else {
            panic!("first upload should be admitted");
        };
        let owner = pool.acquire(first, ordinary_request()).unwrap();
        let ReserveOutcome::Pending(waiter) = pool.reserve(upload_plan(1)) else {
            panic!("second upload should wait");
        };

        assert!(pool.close(first).unwrap().is_empty());
        assert_eq!(pool.reserved_demand(), 1);

        let grants = pool.release(owner).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);
        assert_eq!(pool.reserved_demand(), 1);
        pool.close(grants[0].reservation).unwrap();
    }

    #[test]
    fn closing_download_converts_live_unreserved_owners_to_debt() {
        let mut pool = model(1, 1);
        let ReserveOutcome::Ready(download) = pool.reserve(download_plan(1)) else {
            panic!("download should be admitted");
        };
        let owner = pool.acquire_unreserved(ordinary_request()).unwrap();
        let ReserveOutcome::Pending(waiter) = pool.reserve(download_plan(1)) else {
            panic!("next download should wait");
        };

        assert!(pool.close(download).unwrap().is_empty());
        assert_eq!(pool.reserved_demand(), 0);
        assert_eq!(pool.reusable_live(), 1);
        assert_eq!(pool.unreserved_debt(), 1);
        assert_eq!(pool.admission_used(), 1);

        let grants = pool.release(owner).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);
        pool.close(grants[0].reservation).unwrap();
    }

    #[test]
    fn reserved_carrier_return_does_not_repay_unreserved_debt() {
        let mut pool = model(2, 2);
        let ReserveOutcome::Ready(upload) = pool.reserve(upload_plan(1)) else {
            panic!("upload should be admitted");
        };
        let reserved = pool.acquire(upload, ordinary_request()).unwrap();
        let unreserved = pool.acquire_unreserved(ordinary_request()).unwrap();
        let ReserveOutcome::Pending(waiter) = pool.reserve(download_plan(1)) else {
            panic!("download should wait for admission headroom");
        };

        assert!(pool.release(reserved).unwrap().is_empty());
        assert_eq!(pool.unreserved_debt(), 1);
        assert_eq!(pool.admission_used(), 2);

        let grants = pool.close(upload).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);

        pool.release(unreserved).unwrap();
        pool.close(grants[0].reservation).unwrap();
    }

    #[test]
    fn strict_fifo_prevents_small_work_bypassing_large_waiter() {
        let mut pool = model(4, 1);
        let ReserveOutcome::Ready(active) = pool.reserve(download_plan(3)) else {
            panic!("first work should be admitted");
        };
        let ReserveOutcome::Pending(large) = pool.reserve(download_plan(3)) else {
            panic!("large work should wait");
        };
        let ReserveOutcome::Pending(small) = pool.reserve(download_plan(1)) else {
            panic!("small work should queue behind large work");
        };

        let grants = pool.close(active).unwrap();
        assert_eq!(grants.len(), 2);
        assert_eq!(grants[0].waiter, large);
        assert_eq!(grants[1].waiter, small);
        for grant in grants {
            pool.close(grant.reservation).unwrap();
        }
    }

    #[test]
    fn cancelling_head_waiter_allows_smaller_work_to_run() {
        let mut pool = model(4, 1);
        let ReserveOutcome::Ready(active) = pool.reserve(download_plan(3)) else {
            panic!("first work should be admitted");
        };
        let ReserveOutcome::Pending(large) = pool.reserve(download_plan(3)) else {
            panic!("large work should wait");
        };
        let ReserveOutcome::Pending(small) = pool.reserve(download_plan(1)) else {
            panic!("small work should queue behind large work");
        };

        let grants = pool.cancel_waiter(large).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, small);
        pool.close(active).unwrap();
        pool.close(grants[0].reservation).unwrap();
    }

    #[test]
    fn preferred_domain_wins_but_other_eligible_capacity_is_reusable() {
        let mut pool = model(4, 2);
        let on_one = AcquireRequest::new(
            MIB,
            4096,
            CapabilityRequirement::Ordinary,
            affinity(&[1], &[1]),
        );
        let carrier = pool.acquire_unreserved(on_one).unwrap();
        assert_eq!(carrier.home, domain(1));
        pool.release(carrier).unwrap();

        let broad = ordinary_request();
        let reused = pool.acquire_unreserved(broad).unwrap();
        assert_eq!(reused.source, AcquisitionSource::Reused);
        assert_eq!(reused.home, domain(1));
        pool.release(reused).unwrap();
    }

    #[test]
    fn ordinary_use_can_borrow_fixed_capacity_and_fixed_demand_can_overflow() {
        let mut pool = model(4, 1);
        let fixed_request = AcquireRequest::new(
            MIB,
            4096,
            CapabilityRequirement::Fixed(RingId(7)),
            affinity(&[0], &[0]),
        );
        let fixed = pool.acquire_unreserved(fixed_request).unwrap();
        pool.release(fixed).unwrap();

        let ordinary = pool.acquire_unreserved(ordinary_request()).unwrap();
        assert_eq!(ordinary.source, AcquisitionSource::Reused);
        assert_eq!(ordinary.capability, CarrierCapability::Fixed(RingId(7)));

        let fixed_overflow = pool.acquire_unreserved(fixed_request).unwrap();
        assert_eq!(fixed_overflow.source, AcquisitionSource::Overflow);

        pool.release(ordinary).unwrap();
        pool.release(fixed_overflow).unwrap();
    }

    #[test]
    fn incompatible_reusable_capacity_does_not_prevent_progress() {
        let mut pool = model(4, 1);
        let fixed_request = AcquireRequest::new(
            MIB,
            4096,
            CapabilityRequirement::Fixed(RingId(9)),
            affinity(&[0], &[0]),
        );
        let fixed = pool.acquire_unreserved(fixed_request).unwrap();
        pool.release(fixed).unwrap();

        let other_ring = AcquireRequest::new(
            MIB,
            4096,
            CapabilityRequirement::Fixed(RingId(10)),
            affinity(&[0], &[0]),
        );
        let overflow = pool.acquire_unreserved(other_ring).unwrap();
        assert_eq!(overflow.source, AcquisitionSource::Overflow);
        pool.release(overflow).unwrap();
    }

    #[test]
    fn lowering_reusable_target_trims_free_then_live_on_return() {
        let mut pool = model(4, 2);
        let first = pool.acquire_unreserved(ordinary_request()).unwrap();
        let second = pool.acquire_unreserved(ordinary_request()).unwrap();
        pool.release(first).unwrap();
        assert_eq!(pool.reusable_capacity(), 2);

        pool.set_reusable_capacity_target(0);
        assert_eq!(pool.reusable_capacity(), 1);
        assert_eq!(pool.reusable_live(), 1);

        pool.release(second).unwrap();
        assert_eq!(pool.reusable_capacity(), 0);
    }
}
