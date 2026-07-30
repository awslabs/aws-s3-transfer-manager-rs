/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Executable design model for buffer-pool admission and ownership.
//!
//! This is intentionally test-only. It turns the proposed invariants into state
//! transitions before production concurrency and allocation mechanisms obscure
//! them. Production code should not be copied from this model mechanically.

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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DispatchAffinity {
    eligible: DomainSet,
    preferred: DomainSet,
}

impl DispatchAffinity {
    fn new(eligible: DomainSet, preferred: DomainSet) -> Self {
        assert!(!eligible.is_empty(), "reservation needs an eligible domain");
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
struct PartitionId(usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ReservationId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WaiterId(u64);

#[derive(Debug)]
struct Carrier {
    home: DomainId,
    owner: Option<ReservationId>,
}

#[derive(Debug)]
struct Partition {
    affinity: DispatchAffinity,
    carriers: Vec<Carrier>,
    promised: usize,
}

impl Partition {
    fn capacity(&self) -> usize {
        self.carriers.len()
    }

    fn can_reserve(&self, carriers: usize) -> bool {
        carriers <= self.capacity().saturating_sub(self.promised)
    }
}

#[derive(Debug)]
struct ReservationState {
    partition: PartitionId,
    max_live: usize,
    live: usize,
}

#[derive(Debug)]
struct Waiter {
    id: WaiterId,
    partition: PartitionId,
    carriers: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReserveOutcome {
    Ready(ReservationId),
    Pending(WaiterId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GrantedWaiter {
    waiter: WaiterId,
    reservation: ReservationId,
}

/// A claimed carrier records concrete physical home, not the broad affinity
/// carried by the reservation before dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ClaimedCarrier {
    reservation: ReservationId,
    partition: PartitionId,
    carrier: usize,
    home: DomainId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelError {
    UnknownReservation,
    SelectedDomainNotEligible,
    ReservationEnvelopeExceeded,
    PhysicalPromiseBroken,
    WrongCarrierOwner,
    ReservationHasLiveClaims,
}

#[derive(Debug, Default)]
struct PoolModel {
    partitions: Vec<Partition>,
    reservations: HashMap<ReservationId, ReservationState>,
    waiters: VecDeque<Waiter>,
    next_reservation: u64,
    next_waiter: u64,
}

impl PoolModel {
    fn add_partition(
        &mut self,
        affinity: DispatchAffinity,
        carrier_homes: impl IntoIterator<Item = DomainId>,
    ) -> PartitionId {
        let carriers: Vec<_> = carrier_homes
            .into_iter()
            .map(|home| {
                assert!(
                    affinity.eligible.contains(home),
                    "carrier home must be eligible for its partition"
                );
                Carrier { home, owner: None }
            })
            .collect();
        assert!(!carriers.is_empty(), "partition needs physical capacity");
        let id = PartitionId(self.partitions.len());
        self.partitions.push(Partition {
            affinity,
            carriers,
            promised: 0,
        });
        id
    }

    /// Strict global FIFO: once any request waits, fresh reservations queue.
    fn reserve(&mut self, partition: PartitionId, carriers: usize) -> ReserveOutcome {
        assert!(carriers > 0, "zero-carrier work needs no reservation");
        if self.waiters.is_empty() && self.partitions[partition.0].can_reserve(carriers) {
            return ReserveOutcome::Ready(self.grant(partition, carriers));
        }

        let id = WaiterId(self.next_waiter);
        self.next_waiter += 1;
        self.waiters.push_back(Waiter {
            id,
            partition,
            carriers,
        });
        ReserveOutcome::Pending(id)
    }

    fn grant(&mut self, partition: PartitionId, carriers: usize) -> ReservationId {
        assert!(self.partitions[partition.0].can_reserve(carriers));
        let id = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        self.partitions[partition.0].promised += carriers;
        let old = self.reservations.insert(
            id,
            ReservationState {
                partition,
                max_live: carriers,
                live: 0,
            },
        );
        assert!(old.is_none());
        id
    }

    fn affinity(&self, reservation: ReservationId) -> Result<DispatchAffinity, ModelError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        Ok(self.partitions[state.partition.0].affinity)
    }

    fn claim(
        &mut self,
        reservation: ReservationId,
        selected_domain: DomainId,
    ) -> Result<ClaimedCarrier, ModelError> {
        let (partition_id, live, max_live) = {
            let state = self
                .reservations
                .get(&reservation)
                .ok_or(ModelError::UnknownReservation)?;
            (state.partition, state.live, state.max_live)
        };

        let partition = &mut self.partitions[partition_id.0];
        if !partition.affinity.eligible.contains(selected_domain) {
            return Err(ModelError::SelectedDomainNotEligible);
        }
        if live == max_live {
            return Err(ModelError::ReservationEnvelopeExceeded);
        }

        // Prefer a carrier physically homed on the selected domain, but every
        // carrier in the partition is valid for every eligible domain.
        let carrier = partition
            .carriers
            .iter()
            .position(|carrier| carrier.owner.is_none() && carrier.home == selected_domain)
            .or_else(|| {
                partition
                    .carriers
                    .iter()
                    .position(|carrier| carrier.owner.is_none())
            })
            .ok_or(ModelError::PhysicalPromiseBroken)?;

        let home = partition.carriers[carrier].home;
        partition.carriers[carrier].owner = Some(reservation);
        self.reservations
            .get_mut(&reservation)
            .expect("reservation checked above")
            .live += 1;

        Ok(ClaimedCarrier {
            reservation,
            partition: partition_id,
            carrier,
            home,
        })
    }

    fn release_carrier(&mut self, claimed: ClaimedCarrier) -> Result<(), ModelError> {
        let carrier = &mut self.partitions[claimed.partition.0].carriers[claimed.carrier];
        if carrier.owner != Some(claimed.reservation) {
            return Err(ModelError::WrongCarrierOwner);
        }
        carrier.owner = None;
        let reservation = self
            .reservations
            .get_mut(&claimed.reservation)
            .ok_or(ModelError::UnknownReservation)?;
        reservation.live -= 1;
        Ok(())
    }

    fn release_reservation(
        &mut self,
        reservation: ReservationId,
    ) -> Result<Vec<GrantedWaiter>, ModelError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        if state.live != 0 {
            return Err(ModelError::ReservationHasLiveClaims);
        }

        let state = self
            .reservations
            .remove(&reservation)
            .expect("reservation checked above");
        self.partitions[state.partition.0].promised -= state.max_live;
        Ok(self.drain())
    }

    fn drain(&mut self) -> Vec<GrantedWaiter> {
        let mut granted = Vec::new();
        while let Some(waiter) = self.waiters.front() {
            if !self.partitions[waiter.partition.0].can_reserve(waiter.carriers) {
                break;
            }
            let waiter = self.waiters.pop_front().unwrap();
            let reservation = self.grant(waiter.partition, waiter.carriers);
            granted.push(GrantedWaiter {
                waiter: waiter.id,
                reservation,
            });
        }
        granted
    }

    fn promised(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].promised
    }
}

/// Aggregate admission model for a client-global provider. The provider acquires
/// physical carriers without receiving a request's reservation ID.
#[derive(Debug)]
struct AggregatePartition {
    affinity: DispatchAffinity,
    carriers: Vec<AggregateCarrier>,
    ticket_reserved: usize,
    ticket_live: usize,
    provider_reserved: usize,
    provider_live: usize,
    provider_retiring: usize,
}

#[derive(Debug)]
struct AggregateCarrier {
    home: DomainId,
    owner: AggregateOwner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateOwner {
    Free,
    Provider,
    Ticketed(ReservationId),
}

#[derive(Debug)]
struct WorkMemoryReservation {
    partition: PartitionId,
    provider_envelope: usize,
    ticket_envelope: usize,
    ticket_live: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProviderClaim {
    partition: PartitionId,
    carrier: usize,
    home: DomainId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TicketClaim {
    reservation: ReservationId,
    partition: PartitionId,
    carrier: usize,
    home: DomainId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateError {
    UnknownReservation,
    SelectedDomainNotEligible,
    NoPromisedCapacity,
    ReservationEnvelopeExceeded,
    PhysicalPromiseBroken,
    WrongClaim,
    TicketClaimsPreventRelease,
}

#[derive(Debug, Default)]
struct AggregateProviderModel {
    partitions: Vec<AggregatePartition>,
    reservations: HashMap<ReservationId, WorkMemoryReservation>,
    next_reservation: u64,
}

impl AggregateProviderModel {
    fn add_partition(
        &mut self,
        affinity: DispatchAffinity,
        carrier_homes: impl IntoIterator<Item = DomainId>,
    ) -> PartitionId {
        let carriers: Vec<_> = carrier_homes
            .into_iter()
            .map(|home| AggregateCarrier {
                home,
                owner: AggregateOwner::Free,
            })
            .collect();
        assert!(!carriers.is_empty(), "partition needs physical capacity");
        assert!(
            carriers
                .iter()
                .all(|carrier| affinity.eligible.contains(carrier.home)),
            "carrier home must be eligible for its partition"
        );

        let id = PartitionId(self.partitions.len());
        self.partitions.push(AggregatePartition {
            affinity,
            carriers,
            ticket_reserved: 0,
            ticket_live: 0,
            provider_reserved: 0,
            provider_live: 0,
            provider_retiring: 0,
        });
        id
    }

    fn reserve(&mut self, partition: PartitionId, envelope: usize) -> Option<ReservationId> {
        self.reserve_components(partition, envelope, 0)
    }

    fn reserve_ticketed(
        &mut self,
        partition: PartitionId,
        envelope: usize,
    ) -> Option<ReservationId> {
        self.reserve_components(partition, 0, envelope)
    }

    fn reserve_compound(
        &mut self,
        partition: PartitionId,
        provider_envelope: usize,
        ticket_envelope: usize,
    ) -> Option<ReservationId> {
        assert!(provider_envelope > 0);
        assert!(ticket_envelope > 0);
        self.reserve_components(partition, provider_envelope, ticket_envelope)
    }

    fn reserve_components(
        &mut self,
        partition: PartitionId,
        provider_envelope: usize,
        ticket_envelope: usize,
    ) -> Option<ReservationId> {
        let envelope = provider_envelope + ticket_envelope;
        assert!(envelope > 0);
        let state = &mut self.partitions[partition.0];
        let reserved = state.ticket_reserved + state.provider_reserved;
        if envelope > state.carriers.len().saturating_sub(reserved) {
            return None;
        }

        let id = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        state.ticket_reserved += ticket_envelope;
        state.provider_reserved += provider_envelope;
        self.reservations.insert(
            id,
            WorkMemoryReservation {
                partition,
                provider_envelope,
                ticket_envelope,
                ticket_live: 0,
            },
        );
        self.assert_invariants();
        Some(id)
    }

    /// The provider has no reservation argument. Correctness therefore depends
    /// on every caller being covered by an active aggregate envelope.
    fn acquire(
        &mut self,
        partition: PartitionId,
        selected_domain: DomainId,
    ) -> Result<ProviderClaim, AggregateError> {
        let state = &mut self.partitions[partition.0];
        if !state.affinity.eligible.contains(selected_domain) {
            return Err(AggregateError::SelectedDomainNotEligible);
        }
        if state.provider_live == state.provider_reserved {
            return Err(AggregateError::NoPromisedCapacity);
        }

        let carrier = state
            .carriers
            .iter()
            .position(|carrier| {
                carrier.owner == AggregateOwner::Free && carrier.home == selected_domain
            })
            .or_else(|| {
                state
                    .carriers
                    .iter()
                    .position(|carrier| carrier.owner == AggregateOwner::Free)
            })
            .ok_or(AggregateError::PhysicalPromiseBroken)?;

        state.carriers[carrier].owner = AggregateOwner::Provider;
        state.provider_live += 1;
        let home = state.carriers[carrier].home;
        self.assert_invariants();
        Ok(ProviderClaim {
            partition,
            carrier,
            home,
        })
    }

    fn release(&mut self, claim: ProviderClaim) -> Result<(), AggregateError> {
        let state = &mut self.partitions[claim.partition.0];
        let carrier = &mut state.carriers[claim.carrier];
        if carrier.owner != AggregateOwner::Provider || carrier.home != claim.home {
            return Err(AggregateError::WrongClaim);
        }
        carrier.owner = AggregateOwner::Free;
        state.provider_live -= 1;
        Self::drain_provider_retirement(state);
        self.assert_invariants();
        Ok(())
    }

    fn claim_ticketed(
        &mut self,
        reservation: ReservationId,
        selected_domain: DomainId,
    ) -> Result<TicketClaim, AggregateError> {
        let (partition_id, envelope, live) = {
            let state = self
                .reservations
                .get(&reservation)
                .ok_or(AggregateError::UnknownReservation)?;
            if state.ticket_envelope == 0 {
                return Err(AggregateError::WrongClaim);
            }
            (state.partition, state.ticket_envelope, state.ticket_live)
        };
        if live == envelope {
            return Err(AggregateError::ReservationEnvelopeExceeded);
        }

        let partition = &mut self.partitions[partition_id.0];
        if !partition.affinity.eligible.contains(selected_domain) {
            return Err(AggregateError::SelectedDomainNotEligible);
        }
        let carrier = partition
            .carriers
            .iter()
            .position(|carrier| {
                carrier.owner == AggregateOwner::Free && carrier.home == selected_domain
            })
            .or_else(|| {
                partition
                    .carriers
                    .iter()
                    .position(|carrier| carrier.owner == AggregateOwner::Free)
            })
            .ok_or(AggregateError::PhysicalPromiseBroken)?;

        partition.carriers[carrier].owner = AggregateOwner::Ticketed(reservation);
        partition.ticket_live += 1;
        let state = self
            .reservations
            .get_mut(&reservation)
            .expect("reservation checked above");
        state.ticket_live += 1;
        let claim = TicketClaim {
            reservation,
            partition: partition_id,
            carrier,
            home: partition.carriers[carrier].home,
        };
        self.assert_invariants();
        Ok(claim)
    }

    fn release_ticketed(&mut self, claim: TicketClaim) -> Result<(), AggregateError> {
        let partition = &mut self.partitions[claim.partition.0];
        let carrier = &mut partition.carriers[claim.carrier];
        if carrier.owner != AggregateOwner::Ticketed(claim.reservation)
            || carrier.home != claim.home
        {
            return Err(AggregateError::WrongClaim);
        }
        carrier.owner = AggregateOwner::Free;
        partition.ticket_live -= 1;

        let state = self
            .reservations
            .get_mut(&claim.reservation)
            .ok_or(AggregateError::UnknownReservation)?;
        if state.ticket_envelope == 0 {
            return Err(AggregateError::WrongClaim);
        }
        state.ticket_live -= 1;
        self.assert_invariants();
        Ok(())
    }

    fn release_reservation(&mut self, reservation: ReservationId) -> Result<(), AggregateError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(AggregateError::UnknownReservation)?;
        if state.ticket_live != 0 {
            return Err(AggregateError::TicketClaimsPreventRelease);
        }

        let state = self
            .reservations
            .remove(&reservation)
            .expect("reservation checked above");
        let partition = &mut self.partitions[state.partition.0];
        partition.ticket_reserved -= state.ticket_envelope;

        // Anonymous owners cannot be attributed to this grant. Move its
        // allowance to retirement and release only the aggregate slack. Future
        // provider-owner drops drain the remainder before new claims can reuse it.
        partition.provider_retiring += state.provider_envelope;
        Self::drain_provider_retirement(partition);
        self.assert_invariants();
        Ok(())
    }

    fn drain_provider_retirement(partition: &mut AggregatePartition) {
        let slack = partition.provider_reserved - partition.provider_live;
        let released = partition.provider_retiring.min(slack);
        partition.provider_retiring -= released;
        partition.provider_reserved -= released;
    }

    fn promised(&self, partition: PartitionId) -> usize {
        let state = &self.partitions[partition.0];
        state.ticket_reserved + state.provider_reserved
    }

    fn live(&self, partition: PartitionId) -> usize {
        let state = &self.partitions[partition.0];
        state.ticket_live + state.provider_live
    }

    fn ticket_reserved(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].ticket_reserved
    }

    fn provider_reserved(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].provider_reserved
    }

    fn provider_retiring(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].provider_retiring
    }

    fn assert_invariants(&self) {
        for (partition_index, partition) in self.partitions.iter().enumerate() {
            let partition_id = PartitionId(partition_index);
            let ticket_reserved: usize = self
                .reservations
                .values()
                .filter(|reservation| reservation.partition == partition_id)
                .map(|reservation| reservation.ticket_envelope)
                .sum();
            let ticket_live: usize = self
                .reservations
                .values()
                .filter(|reservation| reservation.partition == partition_id)
                .map(|reservation| reservation.ticket_live)
                .sum();
            let active_provider_grants: usize = self
                .reservations
                .values()
                .filter(|reservation| reservation.partition == partition_id)
                .map(|reservation| reservation.provider_envelope)
                .sum();
            let physical_ticket_live = partition
                .carriers
                .iter()
                .filter(|carrier| matches!(carrier.owner, AggregateOwner::Ticketed(_)))
                .count();
            let physical_provider_live = partition
                .carriers
                .iter()
                .filter(|carrier| carrier.owner == AggregateOwner::Provider)
                .count();

            assert_eq!(partition.ticket_reserved, ticket_reserved);
            assert_eq!(partition.ticket_live, ticket_live);
            assert_eq!(partition.ticket_live, physical_ticket_live);
            assert_eq!(partition.provider_live, physical_provider_live);
            assert_eq!(
                partition.provider_reserved,
                active_provider_grants + partition.provider_retiring
            );
            assert!(partition.ticket_live <= partition.ticket_reserved);
            assert!(partition.provider_live <= partition.provider_reserved);
            assert!(
                partition.ticket_reserved + partition.provider_reserved <= partition.carriers.len()
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunCarrierState {
    Free,
    Escrowed(ReservationId),
    Live(ReservationId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Placement {
    Contiguous { first: usize, count: usize },
    Scatter { count: usize },
}

#[derive(Debug)]
struct RunReservation {
    count: usize,
    placement: Placement,
    claimed: Option<Vec<usize>>,
}

/// Uniform-carrier allocator with an optional contiguous fast path. Contiguity
/// is never required for admission: a fragmented reservation falls back to a
/// carrier manifest instead of waiting for a large shape.
#[derive(Debug)]
struct OpportunisticRunModel {
    carriers: Vec<RunCarrierState>,
    reservations: HashMap<ReservationId, RunReservation>,
    promised: usize,
    next_reservation: u64,
}

impl OpportunisticRunModel {
    fn new(carriers: usize) -> Self {
        assert!(carriers > 0);
        Self {
            carriers: vec![RunCarrierState::Free; carriers],
            reservations: HashMap::new(),
            promised: 0,
            next_reservation: 0,
        }
    }

    fn reserve(&mut self, count: usize) -> Option<ReservationId> {
        assert!(count > 0);
        if count > self.carriers.len().saturating_sub(self.promised) {
            return None;
        }

        let id = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        let placement = self
            .free_run(count)
            .map(|first| {
                for carrier in &mut self.carriers[first..first + count] {
                    *carrier = RunCarrierState::Escrowed(id);
                }
                Placement::Contiguous { first, count }
            })
            .unwrap_or(Placement::Scatter { count });

        self.promised += count;
        self.reservations.insert(
            id,
            RunReservation {
                count,
                placement,
                claimed: None,
            },
        );
        Some(id)
    }

    fn free_run(&self, count: usize) -> Option<usize> {
        self.carriers
            .windows(count)
            .position(|run| run.iter().all(|state| *state == RunCarrierState::Free))
    }

    fn placement(&self, reservation: ReservationId) -> Placement {
        self.reservations[&reservation].placement
    }

    fn claim(&mut self, reservation: ReservationId) -> Result<Vec<usize>, ModelError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        assert!(state.claimed.is_none(), "model reservation is one-shot");

        let carriers = match state.placement {
            Placement::Contiguous { first, count } => {
                let carriers: Vec<_> = (first..first + count).collect();
                assert!(carriers.iter().all(|index| {
                    self.carriers[*index] == RunCarrierState::Escrowed(reservation)
                }));
                carriers
            }
            Placement::Scatter { count } => {
                let carriers: Vec<_> = self
                    .carriers
                    .iter()
                    .enumerate()
                    .filter_map(|(index, state)| (*state == RunCarrierState::Free).then_some(index))
                    .take(count)
                    .collect();
                if carriers.len() != count {
                    return Err(ModelError::PhysicalPromiseBroken);
                }
                carriers
            }
        };

        for index in &carriers {
            self.carriers[*index] = RunCarrierState::Live(reservation);
        }
        self.reservations
            .get_mut(&reservation)
            .expect("reservation checked above")
            .claimed = Some(carriers.clone());
        Ok(carriers)
    }

    fn release_claim(&mut self, reservation: ReservationId) -> Result<(), ModelError> {
        let state = self
            .reservations
            .get_mut(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        let carriers = state.claimed.take().ok_or(ModelError::WrongCarrierOwner)?;
        for index in carriers {
            if self.carriers[index] != RunCarrierState::Live(reservation) {
                return Err(ModelError::WrongCarrierOwner);
            }
            self.carriers[index] = RunCarrierState::Free;
        }
        Ok(())
    }

    fn release_reservation(&mut self, reservation: ReservationId) -> Result<(), ModelError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        if state.claimed.is_some() {
            return Err(ModelError::ReservationHasLiveClaims);
        }

        if let Placement::Contiguous { first, count } = state.placement {
            for index in first..first + count {
                if self.carriers[index] == RunCarrierState::Escrowed(reservation) {
                    self.carriers[index] = RunCarrierState::Free;
                }
            }
        }
        let state = self
            .reservations
            .remove(&reservation)
            .expect("reservation checked above");
        self.promised -= state.count;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ModelBlockId(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SystemBlockState {
    Active,
    Draining,
    Dead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SystemCarrierOwner {
    Free,
    Live(ReservationId),
}

#[derive(Debug)]
struct SystemCarrier {
    home: DomainId,
    owner: SystemCarrierOwner,
}

#[derive(Debug)]
struct SystemBlock {
    state: SystemBlockState,
    carriers: Vec<SystemCarrier>,
}

#[derive(Debug)]
struct SystemWaiter {
    id: WaiterId,
    envelope: usize,
}

#[derive(Debug)]
struct SystemPartition {
    affinity: DispatchAffinity,
    configured_capacity: usize,
    active_capacity: usize,
    preparing: usize,
    draining: usize,
    reserved: usize,
    waiters: VecDeque<SystemWaiter>,
    blocks: Vec<SystemBlock>,
}

#[derive(Debug)]
struct SystemLeaseState {
    partition: PartitionId,
    envelope: usize,
    permits_in_use: usize,
    closed: bool,
    released: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModelLeaseCapability {
    reservation: ReservationId,
}

#[derive(Debug, Eq, PartialEq)]
struct ModelClaimPermit {
    reservation: ReservationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ModelCarrierOwner {
    reservation: ReservationId,
    partition: PartitionId,
    block: ModelBlockId,
    carrier: usize,
}

#[derive(Debug)]
struct ModelPreparation {
    partition: PartitionId,
    homes: Vec<DomainId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SystemReserveOutcome {
    Ready(ReservationId),
    Pending(WaiterId),
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SystemGrantedWaiter {
    waiter: WaiterId,
    reservation: ReservationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SystemModelError {
    UnknownReservation,
    LeaseClosed,
    EnvelopeExceeded,
    SelectedDomainNotEligible,
    PhysicalPromiseBroken,
    WrongCarrierOwner,
    TrimWouldBreakPromise,
    BlockNotActive,
    BlockNotDrained,
}

/// Composed state model for preparation, per-partition reservations, reusable
/// claim permits, owner lifetime, and trim. It deliberately omits concurrency;
/// Loom should refine the same transitions rather than inventing new states.
#[derive(Debug)]
struct SystemPoolModel {
    configured_capacity: usize,
    partitions: Vec<SystemPartition>,
    leases: HashMap<ReservationId, SystemLeaseState>,
    next_reservation: u64,
    next_waiter: u64,
}

impl Default for SystemPoolModel {
    fn default() -> Self {
        Self::with_global_capacity(usize::MAX)
    }
}

impl SystemPoolModel {
    fn with_global_capacity(configured_capacity: usize) -> Self {
        assert!(configured_capacity > 0);
        Self {
            configured_capacity,
            partitions: Vec::new(),
            leases: HashMap::new(),
            next_reservation: 0,
            next_waiter: 0,
        }
    }

    fn add_partition(
        &mut self,
        affinity: DispatchAffinity,
        configured_capacity: usize,
    ) -> PartitionId {
        assert!(configured_capacity > 0);
        let id = PartitionId(self.partitions.len());
        self.partitions.push(SystemPartition {
            affinity,
            configured_capacity,
            active_capacity: 0,
            preparing: 0,
            draining: 0,
            reserved: 0,
            waiters: VecDeque::new(),
            blocks: Vec::new(),
        });
        id
    }

    fn begin_preparation(
        &mut self,
        partition: PartitionId,
        homes: impl IntoIterator<Item = DomainId>,
    ) -> Option<ModelPreparation> {
        let homes: Vec<_> = homes.into_iter().collect();
        assert!(!homes.is_empty());
        let globally_mapped_or_pending: usize = self
            .partitions
            .iter()
            .map(|state| state.active_capacity + state.draining + state.preparing)
            .sum();
        if homes.len()
            > self
                .configured_capacity
                .saturating_sub(globally_mapped_or_pending)
        {
            return None;
        }
        let state = &mut self.partitions[partition.0];
        assert!(
            homes
                .iter()
                .all(|home| state.affinity.eligible.contains(*home)),
            "prepared carrier home must be eligible for its partition"
        );
        let mapped_or_pending = state.active_capacity + state.draining + state.preparing;
        if homes.len() > state.configured_capacity.saturating_sub(mapped_or_pending) {
            return None;
        }
        state.preparing += homes.len();
        Some(ModelPreparation { partition, homes })
    }

    fn fail_preparation(&mut self, preparation: ModelPreparation) {
        self.partitions[preparation.partition.0].preparing -= preparation.homes.len();
        self.assert_invariants();
    }

    fn complete_preparation(
        &mut self,
        preparation: ModelPreparation,
    ) -> (ModelBlockId, Vec<SystemGrantedWaiter>) {
        let partition = preparation.partition;
        let state = &mut self.partitions[partition.0];
        state.preparing -= preparation.homes.len();
        state.active_capacity += preparation.homes.len();
        let block = ModelBlockId(state.blocks.len());
        state.blocks.push(SystemBlock {
            state: SystemBlockState::Active,
            carriers: preparation
                .homes
                .into_iter()
                .map(|home| SystemCarrier {
                    home,
                    owner: SystemCarrierOwner::Free,
                })
                .collect(),
        });
        let grants = self.drain_partition(partition);
        self.assert_invariants();
        (block, grants)
    }

    fn reserve(&mut self, partition: PartitionId, envelope: usize) -> SystemReserveOutcome {
        assert!(envelope > 0);
        let state = &mut self.partitions[partition.0];
        if envelope > state.configured_capacity {
            return SystemReserveOutcome::Rejected;
        }
        if state.waiters.is_empty()
            && envelope <= state.active_capacity.saturating_sub(state.reserved)
        {
            let reservation = self.grant_system(partition, envelope);
            self.assert_invariants();
            return SystemReserveOutcome::Ready(reservation);
        }

        let waiter = WaiterId(self.next_waiter);
        self.next_waiter += 1;
        self.partitions[partition.0]
            .waiters
            .push_back(SystemWaiter {
                id: waiter,
                envelope,
            });
        self.assert_invariants();
        SystemReserveOutcome::Pending(waiter)
    }

    fn grant_system(&mut self, partition: PartitionId, envelope: usize) -> ReservationId {
        let state = &mut self.partitions[partition.0];
        assert!(envelope <= state.active_capacity.saturating_sub(state.reserved));
        state.reserved += envelope;
        let reservation = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        let previous = self.leases.insert(
            reservation,
            SystemLeaseState {
                partition,
                envelope,
                permits_in_use: 0,
                closed: false,
                released: false,
            },
        );
        assert!(previous.is_none());
        reservation
    }

    fn drain_partition(&mut self, partition: PartitionId) -> Vec<SystemGrantedWaiter> {
        let mut granted = Vec::new();
        loop {
            let Some((waiter, envelope)) = self.partitions[partition.0]
                .waiters
                .front()
                .map(|waiter| (waiter.id, waiter.envelope))
            else {
                break;
            };
            let state = &self.partitions[partition.0];
            if envelope > state.active_capacity.saturating_sub(state.reserved) {
                break;
            }
            self.partitions[partition.0].waiters.pop_front();
            granted.push(SystemGrantedWaiter {
                waiter,
                reservation: self.grant_system(partition, envelope),
            });
        }
        granted
    }

    fn capability(
        &self,
        reservation: ReservationId,
    ) -> Result<ModelLeaseCapability, SystemModelError> {
        let state = self
            .leases
            .get(&reservation)
            .ok_or(SystemModelError::UnknownReservation)?;
        if state.released {
            return Err(SystemModelError::LeaseClosed);
        }
        Ok(ModelLeaseCapability { reservation })
    }

    fn acquire_permit(
        &mut self,
        capability: ModelLeaseCapability,
    ) -> Result<ModelClaimPermit, SystemModelError> {
        let state = self
            .leases
            .get_mut(&capability.reservation)
            .ok_or(SystemModelError::UnknownReservation)?;
        if state.closed || state.released {
            return Err(SystemModelError::LeaseClosed);
        }
        if state.permits_in_use == state.envelope {
            return Err(SystemModelError::EnvelopeExceeded);
        }
        state.permits_in_use += 1;
        self.assert_invariants();
        Ok(ModelClaimPermit {
            reservation: capability.reservation,
        })
    }

    fn claim(
        &mut self,
        capability: ModelLeaseCapability,
        selected_domain: DomainId,
    ) -> Result<ModelCarrierOwner, SystemModelError> {
        let permit = self.acquire_permit(capability)?;
        match self.bind_permit(permit, selected_domain) {
            Ok(owner) => Ok(owner),
            Err((permit, error)) => {
                self.cancel_permit(permit)
                    .expect("fresh permit must remain cancellable");
                Err(error)
            }
        }
    }

    fn bind_permit(
        &mut self,
        permit: ModelClaimPermit,
        selected_domain: DomainId,
    ) -> Result<ModelCarrierOwner, (ModelClaimPermit, SystemModelError)> {
        let partition = match self.leases.get(&permit.reservation) {
            Some(state) => state.partition,
            None => return Err((permit, SystemModelError::UnknownReservation)),
        };
        let state = &self.partitions[partition.0];
        if !state.affinity.eligible.contains(selected_domain) {
            return Err((permit, SystemModelError::SelectedDomainNotEligible));
        }

        let matching_home = state.blocks.iter().enumerate().find_map(|(block, state)| {
            (state.state == SystemBlockState::Active).then(|| {
                state
                    .carriers
                    .iter()
                    .position(|carrier| {
                        carrier.owner == SystemCarrierOwner::Free && carrier.home == selected_domain
                    })
                    .map(|carrier| (ModelBlockId(block), carrier))
            })?
        });
        let any_home = || {
            self.partitions[partition.0]
                .blocks
                .iter()
                .enumerate()
                .find_map(|(block, state)| {
                    (state.state == SystemBlockState::Active).then(|| {
                        state
                            .carriers
                            .iter()
                            .position(|carrier| carrier.owner == SystemCarrierOwner::Free)
                            .map(|carrier| (ModelBlockId(block), carrier))
                    })?
                })
        };
        let Some((block, carrier)) = matching_home.or_else(any_home) else {
            return Err((permit, SystemModelError::PhysicalPromiseBroken));
        };

        self.partitions[partition.0].blocks[block.0].carriers[carrier].owner =
            SystemCarrierOwner::Live(permit.reservation);
        let owner = ModelCarrierOwner {
            reservation: permit.reservation,
            partition,
            block,
            carrier,
        };
        self.assert_invariants();
        Ok(owner)
    }

    fn cancel_permit(
        &mut self,
        permit: ModelClaimPermit,
    ) -> Result<Vec<SystemGrantedWaiter>, SystemModelError> {
        self.release_permit(permit.reservation)
    }

    fn release_owner(
        &mut self,
        owner: ModelCarrierOwner,
    ) -> Result<Vec<SystemGrantedWaiter>, SystemModelError> {
        let carrier =
            &mut self.partitions[owner.partition.0].blocks[owner.block.0].carriers[owner.carrier];
        if carrier.owner != SystemCarrierOwner::Live(owner.reservation) {
            return Err(SystemModelError::WrongCarrierOwner);
        }
        carrier.owner = SystemCarrierOwner::Free;
        self.release_permit(owner.reservation)
    }

    fn release_permit(
        &mut self,
        reservation: ReservationId,
    ) -> Result<Vec<SystemGrantedWaiter>, SystemModelError> {
        let should_release = {
            let state = self
                .leases
                .get_mut(&reservation)
                .ok_or(SystemModelError::UnknownReservation)?;
            assert!(state.permits_in_use > 0);
            state.permits_in_use -= 1;
            state.closed && state.permits_in_use == 0
        };
        let grants = if should_release {
            self.release_system_reservation(reservation)?
        } else {
            Vec::new()
        };
        self.assert_invariants();
        Ok(grants)
    }

    fn close(
        &mut self,
        reservation: ReservationId,
    ) -> Result<Vec<SystemGrantedWaiter>, SystemModelError> {
        let should_release = {
            let state = self
                .leases
                .get_mut(&reservation)
                .ok_or(SystemModelError::UnknownReservation)?;
            state.closed = true;
            !state.released && state.permits_in_use == 0
        };
        let grants = if should_release {
            self.release_system_reservation(reservation)?
        } else {
            Vec::new()
        };
        self.assert_invariants();
        Ok(grants)
    }

    fn release_system_reservation(
        &mut self,
        reservation: ReservationId,
    ) -> Result<Vec<SystemGrantedWaiter>, SystemModelError> {
        let (partition, envelope) = {
            let state = self
                .leases
                .get_mut(&reservation)
                .ok_or(SystemModelError::UnknownReservation)?;
            if state.released {
                return Ok(Vec::new());
            }
            assert!(state.closed);
            assert_eq!(state.permits_in_use, 0);
            state.released = true;
            (state.partition, state.envelope)
        };
        self.partitions[partition.0].reserved -= envelope;
        Ok(self.drain_partition(partition))
    }

    fn begin_trim(
        &mut self,
        partition: PartitionId,
        block: ModelBlockId,
    ) -> Result<(), SystemModelError> {
        let state = &mut self.partitions[partition.0];
        let block_capacity = state
            .blocks
            .get(block.0)
            .filter(|block| block.state == SystemBlockState::Active)
            .map(|block| block.carriers.len())
            .ok_or(SystemModelError::BlockNotActive)?;
        if state.active_capacity - block_capacity < state.reserved {
            return Err(SystemModelError::TrimWouldBreakPromise);
        }
        state.active_capacity -= block_capacity;
        state.draining += block_capacity;
        state.blocks[block.0].state = SystemBlockState::Draining;
        self.assert_invariants();
        Ok(())
    }

    fn finish_trim(
        &mut self,
        partition: PartitionId,
        block: ModelBlockId,
    ) -> Result<(), SystemModelError> {
        let state = &mut self.partitions[partition.0];
        let block = state
            .blocks
            .get_mut(block.0)
            .filter(|block| block.state == SystemBlockState::Draining)
            .ok_or(SystemModelError::BlockNotActive)?;
        if block
            .carriers
            .iter()
            .any(|carrier| carrier.owner != SystemCarrierOwner::Free)
        {
            return Err(SystemModelError::BlockNotDrained);
        }
        state.draining -= block.carriers.len();
        block.state = SystemBlockState::Dead;
        self.assert_invariants();
        Ok(())
    }

    fn active_capacity(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].active_capacity
    }

    fn reserved(&self, partition: PartitionId) -> usize {
        self.partitions[partition.0].reserved
    }

    fn permits_in_use(&self, reservation: ReservationId) -> usize {
        self.leases[&reservation].permits_in_use
    }

    fn released(&self, reservation: ReservationId) -> bool {
        self.leases[&reservation].released
    }

    fn assert_invariants(&self) {
        for (partition_index, partition) in self.partitions.iter().enumerate() {
            let partition_id = PartitionId(partition_index);
            let reserved_from_leases: usize = self
                .leases
                .values()
                .filter(|lease| lease.partition == partition_id && !lease.released)
                .map(|lease| lease.envelope)
                .sum();
            let permits: usize = self
                .leases
                .values()
                .filter(|lease| lease.partition == partition_id && !lease.released)
                .map(|lease| lease.permits_in_use)
                .sum();
            let physical_live: usize = partition
                .blocks
                .iter()
                .flat_map(|block| &block.carriers)
                .filter(|carrier| carrier.owner != SystemCarrierOwner::Free)
                .count();
            let mapped = partition.active_capacity + partition.draining + partition.preparing;

            assert_eq!(partition.reserved, reserved_from_leases);
            assert!(physical_live <= permits, "A <= U");
            assert!(permits <= partition.reserved, "U <= R");
            assert!(
                partition.reserved <= partition.active_capacity,
                "R <= active C"
            );
            assert!(mapped <= partition.configured_capacity);
        }
        let globally_mapped_or_pending: usize = self
            .partitions
            .iter()
            .map(|state| state.active_capacity + state.draining + state.preparing)
            .sum();
        assert!(globally_mapped_or_pending <= self.configured_capacity);
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
    CpuOnly,
    Fixed(RingId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapabilityOwner {
    Free,
    Live(ReservationId),
}

#[derive(Debug)]
struct CapabilityCarrier {
    capability: CarrierCapability,
    owner: CapabilityOwner,
}

#[derive(Debug)]
struct CapabilityReservation {
    requirement: CapabilityRequirement,
    envelope: usize,
    live: usize,
}

#[derive(Debug)]
struct FixedCapabilityWaiter {
    id: WaiterId,
    ring: RingId,
    envelope: usize,
    escrowed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CapabilityReserveOutcome {
    Ready(ReservationId),
    Pending(WaiterId),
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityGrantedWaiter {
    waiter: WaiterId,
    reservation: ReservationId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapabilityCarrierOwner {
    reservation: ReservationId,
    carrier: usize,
}

/// Models ordinary CPU claims borrowing fixed-ring carriers without creating a
/// static ordinary/ring split.
///
/// Pending fixed-ring demand escrows both global capacity and a ring floor.
/// Existing ordinary owners above that future floor may drain naturally, but
/// ordinary reclaims cannot reoccupy the protected capacity. This is what turns
/// borrowing into eventual fungibility rather than starvation of hard demand.
#[derive(Debug)]
struct CapabilityPoolModel {
    carriers: Vec<CapabilityCarrier>,
    reservations: HashMap<ReservationId, CapabilityReservation>,
    fixed_waiters: VecDeque<FixedCapabilityWaiter>,
    next_reservation: u64,
    next_waiter: u64,
}

impl CapabilityPoolModel {
    fn new(capabilities: impl IntoIterator<Item = CarrierCapability>) -> Self {
        let carriers: Vec<_> = capabilities
            .into_iter()
            .map(|capability| CapabilityCarrier {
                capability,
                owner: CapabilityOwner::Free,
            })
            .collect();
        assert!(!carriers.is_empty());
        Self {
            carriers,
            reservations: HashMap::new(),
            fixed_waiters: VecDeque::new(),
            next_reservation: 0,
            next_waiter: 0,
        }
    }

    fn reserve_ordinary(&mut self, envelope: usize) -> Option<ReservationId> {
        assert!(envelope > 0);
        if self.fixed_waiters.iter().any(|waiter| !waiter.escrowed)
            || envelope > self.carriers.len().saturating_sub(self.total_committed())
        {
            return None;
        }
        let reservation = self.grant(CapabilityRequirement::Ordinary, envelope);
        self.assert_invariants();
        Some(reservation)
    }

    fn reserve_fixed(&mut self, ring: RingId, envelope: usize) -> CapabilityReserveOutcome {
        assert!(envelope > 0);
        if envelope > self.fixed_capacity(ring) {
            return CapabilityReserveOutcome::Rejected;
        }
        let can_escrow = envelope <= self.carriers.len().saturating_sub(self.total_committed())
            && envelope
                <= self
                    .fixed_capacity(ring)
                    .saturating_sub(self.fixed_committed(ring));
        if can_escrow && self.fixed_claimable(ring, envelope) && self.fixed_waiters.is_empty() {
            let reservation = self.grant(CapabilityRequirement::Fixed(ring), envelope);
            self.assert_invariants();
            return CapabilityReserveOutcome::Ready(reservation);
        }

        let waiter = WaiterId(self.next_waiter);
        self.next_waiter += 1;
        self.fixed_waiters.push_back(FixedCapabilityWaiter {
            id: waiter,
            ring,
            envelope,
            escrowed: false,
        });
        let grants = self.drain_fixed_waiters();
        self.assert_invariants();
        grants
            .into_iter()
            .find(|grant| grant.waiter == waiter)
            .map(|grant| CapabilityReserveOutcome::Ready(grant.reservation))
            .unwrap_or(CapabilityReserveOutcome::Pending(waiter))
    }

    fn grant(&mut self, requirement: CapabilityRequirement, envelope: usize) -> ReservationId {
        let reservation = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        let previous = self.reservations.insert(
            reservation,
            CapabilityReservation {
                requirement,
                envelope,
                live: 0,
            },
        );
        assert!(previous.is_none());
        reservation
    }

    fn claim(&mut self, reservation: ReservationId) -> Result<CapabilityCarrierOwner, ModelError> {
        let (requirement, live, envelope) = self
            .reservations
            .get(&reservation)
            .map(|state| (state.requirement, state.live, state.envelope))
            .ok_or(ModelError::UnknownReservation)?;
        if live == envelope {
            return Err(ModelError::ReservationEnvelopeExceeded);
        }

        let carrier = match requirement {
            CapabilityRequirement::Ordinary => self
                .carriers
                .iter()
                .position(|carrier| {
                    carrier.owner == CapabilityOwner::Free
                        && carrier.capability == CarrierCapability::CpuOnly
                })
                .or_else(|| {
                    self.carriers.iter().position(|carrier| {
                        let CarrierCapability::Fixed(ring) = carrier.capability else {
                            return false;
                        };
                        carrier.owner == CapabilityOwner::Free
                            && self.ordinary_live_on_ring(ring)
                                < self
                                    .fixed_capacity(ring)
                                    .saturating_sub(self.fixed_committed(ring))
                    })
                }),
            CapabilityRequirement::Fixed(ring) => self.carriers.iter().position(|carrier| {
                carrier.owner == CapabilityOwner::Free
                    && carrier.capability == CarrierCapability::Fixed(ring)
            }),
        }
        .ok_or(ModelError::PhysicalPromiseBroken)?;

        self.carriers[carrier].owner = CapabilityOwner::Live(reservation);
        self.reservations
            .get_mut(&reservation)
            .expect("reservation checked above")
            .live += 1;
        self.assert_invariants();
        Ok(CapabilityCarrierOwner {
            reservation,
            carrier,
        })
    }

    fn release_owner(
        &mut self,
        owner: CapabilityCarrierOwner,
    ) -> Result<Vec<CapabilityGrantedWaiter>, ModelError> {
        let carrier = self
            .carriers
            .get_mut(owner.carrier)
            .ok_or(ModelError::WrongCarrierOwner)?;
        if carrier.owner != CapabilityOwner::Live(owner.reservation) {
            return Err(ModelError::WrongCarrierOwner);
        }
        carrier.owner = CapabilityOwner::Free;
        self.reservations
            .get_mut(&owner.reservation)
            .ok_or(ModelError::UnknownReservation)?
            .live -= 1;
        let grants = self.drain_fixed_waiters();
        self.assert_invariants();
        Ok(grants)
    }

    fn release_reservation(
        &mut self,
        reservation: ReservationId,
    ) -> Result<Vec<CapabilityGrantedWaiter>, ModelError> {
        let state = self
            .reservations
            .get(&reservation)
            .ok_or(ModelError::UnknownReservation)?;
        if state.live != 0 {
            return Err(ModelError::ReservationHasLiveClaims);
        }
        self.reservations.remove(&reservation);
        let grants = self.drain_fixed_waiters();
        self.assert_invariants();
        Ok(grants)
    }

    fn cancel_fixed_waiter(&mut self, waiter: WaiterId) -> Vec<CapabilityGrantedWaiter> {
        if let Some(index) = self
            .fixed_waiters
            .iter()
            .position(|candidate| candidate.id == waiter)
        {
            self.fixed_waiters.remove(index);
        }
        let grants = self.drain_fixed_waiters();
        self.assert_invariants();
        grants
    }

    fn drain_fixed_waiters(&mut self) -> Vec<CapabilityGrantedWaiter> {
        // Escrow in queue order. An unescrowed head blocks later admission,
        // preserving the original FIFO starvation guarantee while capacity is
        // shared across capability classes.
        for index in 0..self.fixed_waiters.len() {
            if self.fixed_waiters[index].escrowed {
                continue;
            }
            let waiter = &self.fixed_waiters[index];
            let can_escrow = waiter.envelope
                <= self.carriers.len().saturating_sub(self.total_committed())
                && waiter.envelope
                    <= self
                        .fixed_capacity(waiter.ring)
                        .saturating_sub(self.fixed_committed(waiter.ring));
            if !can_escrow {
                break;
            }
            self.fixed_waiters[index].escrowed = true;
        }

        let mut grants = Vec::new();
        let mut index = 0;
        while index < self.fixed_waiters.len() {
            let waiter = &self.fixed_waiters[index];
            let earlier_same_ring = self
                .fixed_waiters
                .iter()
                .take(index)
                .any(|earlier| earlier.ring == waiter.ring);
            if !waiter.escrowed
                || earlier_same_ring
                || !self.fixed_claimable(waiter.ring, waiter.envelope)
            {
                index += 1;
                continue;
            }

            let waiter = self.fixed_waiters.remove(index).unwrap();
            let reservation =
                self.grant(CapabilityRequirement::Fixed(waiter.ring), waiter.envelope);
            grants.push(CapabilityGrantedWaiter {
                waiter: waiter.id,
                reservation,
            });
        }
        grants
    }

    fn fixed_claimable(&self, ring: RingId, additional: usize) -> bool {
        self.ordinary_live_on_ring(ring) + self.fixed_reserved(ring) + additional
            <= self.fixed_capacity(ring)
    }

    fn fixed_capacity(&self, ring: RingId) -> usize {
        self.carriers
            .iter()
            .filter(|carrier| carrier.capability == CarrierCapability::Fixed(ring))
            .count()
    }

    fn ordinary_live_on_ring(&self, ring: RingId) -> usize {
        self.carriers
            .iter()
            .filter(|carrier| {
                if carrier.capability != CarrierCapability::Fixed(ring) {
                    return false;
                }
                let CapabilityOwner::Live(reservation) = carrier.owner else {
                    return false;
                };
                self.reservations
                    .get(&reservation)
                    .is_some_and(|state| state.requirement == CapabilityRequirement::Ordinary)
            })
            .count()
    }

    fn fixed_reserved(&self, ring: RingId) -> usize {
        self.reservations
            .values()
            .filter_map(|state| {
                (state.requirement == CapabilityRequirement::Fixed(ring)).then_some(state.envelope)
            })
            .sum()
    }

    fn fixed_escrowed(&self, ring: RingId) -> usize {
        self.fixed_waiters
            .iter()
            .filter(|waiter| waiter.escrowed && waiter.ring == ring)
            .map(|waiter| waiter.envelope)
            .sum()
    }

    fn fixed_committed(&self, ring: RingId) -> usize {
        self.fixed_reserved(ring) + self.fixed_escrowed(ring)
    }

    fn total_committed(&self) -> usize {
        self.reservations
            .values()
            .map(|state| state.envelope)
            .sum::<usize>()
            + self
                .fixed_waiters
                .iter()
                .filter(|waiter| waiter.escrowed)
                .map(|waiter| waiter.envelope)
                .sum::<usize>()
    }

    fn assert_invariants(&self) {
        assert!(self.total_committed() <= self.carriers.len());
        for reservation in self.reservations.values() {
            assert!(reservation.live <= reservation.envelope);
        }

        let physical_live = self
            .carriers
            .iter()
            .filter(|carrier| carrier.owner != CapabilityOwner::Free)
            .count();
        let recorded_live: usize = self
            .reservations
            .values()
            .map(|reservation| reservation.live)
            .sum();
        assert_eq!(physical_live, recorded_live);

        for ring in self.carriers.iter().filter_map(|carrier| {
            let CarrierCapability::Fixed(ring) = carrier.capability else {
                return None;
            };
            Some(ring)
        }) {
            assert!(self.fixed_committed(ring) <= self.fixed_capacity(ring));
            assert!(
                self.ordinary_live_on_ring(ring) + self.fixed_reserved(ring)
                    <= self.fixed_capacity(ring),
                "granted fixed reservations must remain physically claimable"
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtentState {
    Free,
    Large,
    Small(u64),
}

/// Minimal two-shape allocator model used to construct fragmentation states.
#[derive(Debug)]
struct ExtentModel {
    extents: Vec<ExtentState>,
    slots_per_extent: usize,
}

impl ExtentModel {
    fn new(extents: usize, slots_per_extent: usize) -> Self {
        assert!(slots_per_extent <= 64);
        Self {
            extents: vec![ExtentState::Free; extents],
            slots_per_extent,
        }
    }

    fn claim_small_at(&mut self, extent: usize, slot: usize) {
        assert!(slot < self.slots_per_extent);
        let bit = 1u64 << slot;
        match &mut self.extents[extent] {
            state @ ExtentState::Free => *state = ExtentState::Small(bit),
            ExtentState::Small(bits) => {
                assert_eq!(*bits & bit, 0, "slot already live");
                *bits |= bit;
            }
            ExtentState::Large => panic!("large extent cannot serve a small slot"),
        }
    }

    fn claim_large(&mut self) -> Option<usize> {
        let extent = self
            .extents
            .iter()
            .position(|state| *state == ExtentState::Free)?;
        self.extents[extent] = ExtentState::Large;
        Some(extent)
    }

    fn free_small_slots(&self) -> usize {
        let all_slots = self.slots_per_extent;
        self.extents
            .iter()
            .map(|state| match state {
                ExtentState::Free => all_slots,
                ExtentState::Large => 0,
                ExtentState::Small(bits) => all_slots - bits.count_ones() as usize,
            })
            .sum()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShortReadPolicy {
    RetainTail,
    AbandonTail,
}

/// Counts physical carriers consumed by a sequence of successful short reads.
#[derive(Debug)]
struct ShortReadModel {
    capacity: usize,
    remaining_tail: usize,
    carriers: usize,
    policy: ShortReadPolicy,
}

impl ShortReadModel {
    fn new(capacity: usize, policy: ShortReadPolicy) -> Self {
        assert!(capacity > 0);
        Self {
            capacity,
            remaining_tail: 0,
            carriers: 0,
            policy,
        }
    }

    fn record_read(&mut self, bytes: usize) {
        assert!(bytes > 0, "EOF is not a successful body read");
        if self.remaining_tail == 0 {
            self.carriers += 1;
            self.remaining_tail = self.capacity;
        }
        assert!(
            bytes <= self.remaining_tail,
            "transport cannot initialize beyond supplied writable tail"
        );
        self.remaining_tail -= bytes;
        if self.policy == ShortReadPolicy::AbandonTail {
            self.remaining_tail = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use aws_sdk_s3::config::Region;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::ChecksumAlgorithm;
    use aws_smithy_http_client::test_util::{capture_request, ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use bytes::Bytes;
    use futures_util::future::poll_fn;
    use http_body_1x::{Body, Frame, SizeHint};

    const D0: DomainId = DomainId(0);
    const D1: DomainId = DomainId(1);

    fn two_domain_affinity() -> DispatchAffinity {
        DispatchAffinity::new(
            DomainSet::from_domains([D0, D1]),
            DomainSet::from_domains([D0]),
        )
    }

    fn prepare_system_block(
        pool: &mut SystemPoolModel,
        partition: PartitionId,
        homes: impl IntoIterator<Item = DomainId>,
    ) -> (ModelBlockId, Vec<SystemGrantedWaiter>) {
        let preparation = pool
            .begin_preparation(partition, homes)
            .expect("configured capacity should permit preparation");
        pool.complete_preparation(preparation)
    }

    #[test]
    fn system_reservation_is_granted_only_after_preparation_completes() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 2);
        let waiter = match pool.reserve(partition, 2) {
            SystemReserveOutcome::Pending(waiter) => waiter,
            other => panic!("unprepared capacity must not grant: {other:?}"),
        };

        let failed = pool
            .begin_preparation(partition, [D0, D1])
            .expect("preparation fits configured capacity");
        assert_eq!(pool.active_capacity(partition), 0);
        pool.fail_preparation(failed);
        assert_eq!(pool.active_capacity(partition), 0);
        assert_eq!(pool.reserved(partition), 0);

        let (_, grants) = prepare_system_block(&mut pool, partition, [D0, D1]);
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);
        assert_eq!(pool.reserved(partition), 2);
    }

    #[test]
    fn preparation_honors_global_capacity_across_partitions_and_trim_releases_it() {
        let mut pool = SystemPoolModel::with_global_capacity(3);
        let p0 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D0]), DomainSet::from_domains([D0])),
            3,
        );
        let p1 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D1]), DomainSet::from_domains([D1])),
            3,
        );

        let pending = pool
            .begin_preparation(p0, [D0, D0])
            .expect("first partition fits the process-wide cap");
        assert!(
            pool.begin_preparation(p1, [D1, D1]).is_none(),
            "in-flight preparation must consume process-wide capacity"
        );
        let (p0_block, _) = pool.complete_preparation(pending);
        prepare_system_block(&mut pool, p1, [D1]);
        assert!(
            pool.begin_preparation(p1, [D1]).is_none(),
            "active blocks must consume process-wide capacity"
        );

        pool.begin_trim(p0, p0_block).unwrap();
        assert!(
            pool.begin_preparation(p1, [D1]).is_none(),
            "draining blocks retain capacity until unregister and unmap complete"
        );
        pool.finish_trim(p0, p0_block).unwrap();

        let replacement = pool
            .begin_preparation(p1, [D1, D1])
            .expect("fully trimmed capacity may be prepared in another partition");
        pool.complete_preparation(replacement);
        assert_eq!(pool.active_capacity(p0), 0);
        assert_eq!(pool.active_capacity(p1), 3);
    }

    #[test]
    fn lease_permits_are_reusable_and_close_waits_for_final_owner() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 1);
        prepare_system_block(&mut pool, partition, [D0]);
        let lease = match pool.reserve(partition, 1) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("prepared capacity should grant: {other:?}"),
        };
        let capability = pool.capability(lease).unwrap();

        let first = pool.claim(capability, D0).unwrap();
        assert_eq!(pool.permits_in_use(lease), 1);
        pool.release_owner(first).unwrap();
        assert_eq!(pool.permits_in_use(lease), 0);

        let retry = pool.claim(capability, D0).unwrap();
        pool.close(lease).unwrap();
        assert!(
            !pool.released(lease),
            "live backing keeps reservation charged"
        );
        assert_eq!(
            pool.acquire_permit(capability),
            Err(SystemModelError::LeaseClosed)
        );

        pool.release_owner(retry).unwrap();
        assert!(pool.released(lease));
        assert_eq!(pool.reserved(partition), 0);
    }

    #[test]
    fn close_cancels_new_claims_but_unbound_permit_releases_normally() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 1);
        prepare_system_block(&mut pool, partition, [D0]);
        let lease = match pool.reserve(partition, 1) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("prepared capacity should grant: {other:?}"),
        };
        let capability = pool.capability(lease).unwrap();
        let fallback_permit = pool.acquire_permit(capability).unwrap();

        pool.close(lease).unwrap();
        assert!(!pool.released(lease));
        pool.cancel_permit(fallback_permit).unwrap();
        assert!(pool.released(lease));
    }

    #[test]
    fn request_scoped_capability_cannot_spend_another_lease_envelope() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 2);
        prepare_system_block(&mut pool, partition, [D0, D0]);
        let first = match pool.reserve(partition, 1) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("first lease should fit: {other:?}"),
        };
        let second = match pool.reserve(partition, 1) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("second lease should fit: {other:?}"),
        };
        let first_capability = pool.capability(first).unwrap();
        let second_capability = pool.capability(second).unwrap();

        let first_owner = pool.claim(first_capability, D0).unwrap();
        assert_eq!(
            pool.acquire_permit(first_capability),
            Err(SystemModelError::EnvelopeExceeded)
        );
        let second_owner = pool.claim(second_capability, D0).unwrap();

        pool.close(first).unwrap();
        pool.close(second).unwrap();
        pool.release_owner(first_owner).unwrap();
        pool.release_owner(second_owner).unwrap();
        assert!(pool.released(first));
        assert!(pool.released(second));
    }

    #[test]
    fn trim_escrow_preserves_claimability_while_old_owner_drains() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 6);
        let (old_block, _) = prepare_system_block(&mut pool, partition, [D0, D0, D0]);
        prepare_system_block(&mut pool, partition, [D1, D1, D1]);
        let lease = match pool.reserve(partition, 3) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("lease should fit: {other:?}"),
        };
        let capability = pool.capability(lease).unwrap();
        let old_owner = pool.claim(capability, D0).unwrap();
        assert_eq!(old_owner.block, old_block);

        pool.begin_trim(partition, old_block).unwrap();
        assert_eq!(pool.active_capacity(partition), 3);
        assert_eq!(pool.reserved(partition), 3);
        assert_eq!(
            pool.finish_trim(partition, old_block),
            Err(SystemModelError::BlockNotDrained)
        );

        let active_a = pool.claim(capability, D1).unwrap();
        let active_b = pool.claim(capability, D1).unwrap();
        assert_ne!(active_a.block, old_block);
        assert_ne!(active_b.block, old_block);

        pool.close(lease).unwrap();
        pool.release_owner(active_a).unwrap();
        pool.release_owner(active_b).unwrap();
        assert!(!pool.released(lease));
        pool.release_owner(old_owner).unwrap();
        assert!(pool.released(lease));
        pool.finish_trim(partition, old_block).unwrap();
    }

    #[test]
    fn trim_rejects_removing_capacity_promised_to_reservations() {
        let mut pool = SystemPoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), 6);
        let (first_block, _) = prepare_system_block(&mut pool, partition, [D0, D0, D0]);
        prepare_system_block(&mut pool, partition, [D1, D1, D1]);
        let lease = match pool.reserve(partition, 4) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("lease should fit: {other:?}"),
        };

        assert_eq!(
            pool.begin_trim(partition, first_block),
            Err(SystemModelError::TrimWouldBreakPromise)
        );
        pool.close(lease).unwrap();
    }

    #[test]
    fn ordinary_claims_borrow_only_above_a_fixed_ring_floor() {
        let ring = RingId(0);
        let mut pool = CapabilityPoolModel::new([
            CarrierCapability::CpuOnly,
            CarrierCapability::CpuOnly,
            CarrierCapability::Fixed(ring),
            CarrierCapability::Fixed(ring),
        ]);
        let fixed = match pool.reserve_fixed(ring, 1) {
            CapabilityReserveOutcome::Ready(reservation) => reservation,
            other => panic!("fixed floor should be immediately claimable: {other:?}"),
        };
        let ordinary = pool.reserve_ordinary(3).unwrap();

        let ordinary_owners = [
            pool.claim(ordinary).unwrap(),
            pool.claim(ordinary).unwrap(),
            pool.claim(ordinary).unwrap(),
        ];
        assert_eq!(pool.ordinary_live_on_ring(ring), 1);
        let fixed_owner = pool.claim(fixed).unwrap();
        assert_eq!(
            pool.carriers[fixed_owner.carrier].capability,
            CarrierCapability::Fixed(ring)
        );

        for owner in ordinary_owners {
            pool.release_owner(owner).unwrap();
        }
        pool.release_owner(fixed_owner).unwrap();
        pool.release_reservation(ordinary).unwrap();
        pool.release_reservation(fixed).unwrap();
    }

    #[test]
    fn pending_fixed_demand_migrates_reusable_ordinary_claims_off_ring() {
        let ring = RingId(0);
        let mut pool = CapabilityPoolModel::new([
            CarrierCapability::CpuOnly,
            CarrierCapability::CpuOnly,
            CarrierCapability::Fixed(ring),
            CarrierCapability::Fixed(ring),
        ]);

        // Fill the least-constrained carriers first so a second ordinary lease
        // legitimately borrows both ring-capable carriers.
        let filler = pool.reserve_ordinary(2).unwrap();
        let filler_owners = [pool.claim(filler).unwrap(), pool.claim(filler).unwrap()];
        let borrower = pool.reserve_ordinary(2).unwrap();
        let borrowed = [pool.claim(borrower).unwrap(), pool.claim(borrower).unwrap()];
        assert_eq!(pool.ordinary_live_on_ring(ring), 2);

        // Global capacity becomes available, but its physical placement is
        // temporarily wrong for a two-carrier fixed-ring promise.
        for owner in filler_owners {
            pool.release_owner(owner).unwrap();
        }
        pool.release_reservation(filler).unwrap();
        let waiter = match pool.reserve_fixed(ring, 2) {
            CapabilityReserveOutcome::Pending(waiter) => waiter,
            other => panic!("borrowed ring capacity must delay the grant: {other:?}"),
        };
        assert_eq!(pool.fixed_escrowed(ring), 2);

        // The pending floor prevents reusable ordinary permits from taking the
        // ring carriers again; they migrate to the now-free CPU-only carriers.
        assert!(pool.release_owner(borrowed[0]).unwrap().is_empty());
        let migrated_a = pool.claim(borrower).unwrap();
        assert_eq!(
            pool.carriers[migrated_a.carrier].capability,
            CarrierCapability::CpuOnly
        );

        let grants = pool.release_owner(borrowed[1]).unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0].waiter, waiter);
        let fixed = grants[0].reservation;
        let migrated_b = pool.claim(borrower).unwrap();
        assert_eq!(
            pool.carriers[migrated_b.carrier].capability,
            CarrierCapability::CpuOnly
        );
        let fixed_a = pool.claim(fixed).unwrap();
        let fixed_b = pool.claim(fixed).unwrap();
        assert_eq!(pool.ordinary_live_on_ring(ring), 0);

        for owner in [migrated_a, migrated_b] {
            pool.release_owner(owner).unwrap();
        }
        for owner in [fixed_a, fixed_b] {
            pool.release_owner(owner).unwrap();
        }
        pool.release_reservation(borrower).unwrap();
        pool.release_reservation(fixed).unwrap();
    }

    #[test]
    fn cancelling_unescrowed_fixed_waiter_unblocks_admission() {
        let ring = RingId(0);
        let mut pool = CapabilityPoolModel::new([
            CarrierCapability::CpuOnly,
            CarrierCapability::CpuOnly,
            CarrierCapability::Fixed(ring),
            CarrierCapability::Fixed(ring),
        ]);
        let holder = pool.reserve_ordinary(3).unwrap();
        let waiter = match pool.reserve_fixed(ring, 2) {
            CapabilityReserveOutcome::Pending(waiter) => waiter,
            other => panic!("global headroom is insufficient: {other:?}"),
        };
        assert!(
            pool.reserve_ordinary(1).is_none(),
            "an unescrowed FIFO head must not be bypassed"
        );

        assert!(pool.cancel_fixed_waiter(waiter).is_empty());
        let follower = pool
            .reserve_ordinary(1)
            .expect("cancellation releases the admission queue");
        pool.release_reservation(holder).unwrap();
        pool.release_reservation(follower).unwrap();
    }

    #[test]
    fn independent_partitions_have_independent_fifo_queues() {
        let mut pool = SystemPoolModel::default();
        let p0 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D0]), DomainSet::from_domains([D0])),
            1,
        );
        let p1 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D1]), DomainSet::from_domains([D1])),
            1,
        );
        prepare_system_block(&mut pool, p0, [D0]);
        prepare_system_block(&mut pool, p1, [D1]);
        let holder = match pool.reserve(p0, 1) {
            SystemReserveOutcome::Ready(lease) => lease,
            other => panic!("p0 holder should fit: {other:?}"),
        };
        assert!(matches!(
            pool.reserve(p0, 1),
            SystemReserveOutcome::Pending(_)
        ));
        assert!(matches!(
            pool.reserve(p1, 1),
            SystemReserveOutcome::Ready(_)
        ));
        pool.close(holder).unwrap();
    }

    #[test]
    fn uniform_reservations_make_every_covered_claim_physical() {
        let mut pool = PoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D1, D0, D1, D0]);

        let first = match pool.reserve(partition, 3) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("first reservation should fit"),
        };
        let second = match pool.reserve(partition, 2) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("second reservation should fit"),
        };

        let a = pool.claim(first, D0).unwrap();
        let b = pool.claim(second, D1).unwrap();
        let c = pool.claim(first, D1).unwrap();
        let d = pool.claim(second, D0).unwrap();
        let e = pool.claim(first, D0).unwrap();

        assert_eq!(
            pool.claim(first, D0),
            Err(ModelError::ReservationEnvelopeExceeded)
        );
        for carrier in [a, b, c, d, e] {
            pool.release_carrier(carrier).unwrap();
        }
        pool.release_reservation(first).unwrap();
        pool.release_reservation(second).unwrap();
        assert_eq!(pool.promised(partition), 0);
    }

    #[test]
    fn global_provider_can_acquire_without_a_reservation_handle() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let first = pool.reserve(partition, 2).unwrap();
        let second = pool.reserve(partition, 2).unwrap();

        // These calls model deep hyper code: no ReservationId is available.
        let a = pool.acquire(partition, D0).unwrap();
        let b = pool.acquire(partition, D0).unwrap();
        let c = pool.acquire(partition, D0).unwrap();
        let d = pool.acquire(partition, D0).unwrap();
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity)
        );

        // Aggregate credits are fungible. Closing either grant is safe once
        // enough aggregate slack exists, regardless of claim attribution.
        pool.release(a).unwrap();
        pool.release(b).unwrap();
        pool.release_reservation(first).unwrap();
        assert_eq!(pool.promised(partition), 2);
        assert_eq!(pool.live(partition), 2);

        pool.release(c).unwrap();
        pool.release(d).unwrap();
        pool.release_reservation(second).unwrap();
        assert_eq!(pool.promised(partition), 0);
        assert_eq!(pool.live(partition), 0);
    }

    #[test]
    fn provider_rejects_acquisition_without_any_admitted_envelope() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0]);

        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity)
        );
    }

    #[test]
    fn anonymous_provider_cannot_spend_ticketed_upload_capacity() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let upload = pool.reserve_ticketed(partition, 2).unwrap();
        let download = pool.reserve(partition, 2).unwrap();

        let network_a = pool.acquire(partition, D0).unwrap();
        let network_b = pool.acquire(partition, D0).unwrap();
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity),
            "the anonymous allowance excludes ticketed upload promises"
        );
        let upload_a = pool.claim_ticketed(upload, D0).unwrap();
        let upload_b = pool.claim_ticketed(upload, D0).unwrap();
        assert_eq!(pool.ticket_reserved(partition), 2);
        assert_eq!(pool.provider_reserved(partition), 2);

        pool.release(network_a).unwrap();
        pool.release(network_b).unwrap();
        pool.release_reservation(download).unwrap();
        pool.release_ticketed(upload_a).unwrap();
        pool.release_ticketed(upload_b).unwrap();
        pool.release_reservation(upload).unwrap();
    }

    #[test]
    fn compound_upload_envelope_separates_request_and_response_capacity() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let upload = pool.reserve_compound(partition, 1, 3).unwrap();

        let request_a = pool.claim_ticketed(upload, D0).unwrap();
        let request_b = pool.claim_ticketed(upload, D0).unwrap();
        let request_c = pool.claim_ticketed(upload, D0).unwrap();
        assert_eq!(
            pool.claim_ticketed(upload, D0),
            Err(AggregateError::ReservationEnvelopeExceeded)
        );

        let response = pool.acquire(partition, D0).unwrap();
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity),
            "hyper cannot consume the three carriers retained by the request body"
        );

        pool.release(response).unwrap();
        for claim in [request_a, request_b, request_c] {
            pool.release_ticketed(claim).unwrap();
        }
        pool.release_reservation(upload).unwrap();
    }

    #[test]
    fn anonymous_provider_must_acquire_from_the_promised_partition() {
        let mut pool = AggregateProviderModel::default();
        let p0 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D0]), DomainSet::from_domains([D0])),
            [D0],
        );
        let p1 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D1]), DomainSet::from_domains([D1])),
            [D1],
        );
        let reservation = pool.reserve(p0, 1).unwrap();

        assert_eq!(
            pool.acquire(p1, D1),
            Err(AggregateError::NoPromisedCapacity),
            "a global provider cannot spend a promise made in another partition"
        );
        let claim = pool.acquire(p0, D0).unwrap();
        pool.release(claim).unwrap();
        pool.release_reservation(reservation).unwrap();
    }

    #[test]
    fn unreserved_shared_client_can_steal_an_aggregate_promise() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let reservation = pool.reserve(partition, 4).unwrap();

        // The API cannot distinguish this unrelated SDK request from covered TM
        // traffic. It consumes one of TM's anonymous promised credits.
        let stolen = pool.acquire(partition, D0).unwrap();
        let covered: Vec<_> = (0..3)
            .map(|_| pool.acquire(partition, D0).unwrap())
            .collect();
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity),
            "the fourth covered acquisition is no longer claimable"
        );

        pool.release(stolen).unwrap();
        for claim in covered {
            pool.release(claim).unwrap();
        }
        pool.release_reservation(reservation).unwrap();
    }

    #[test]
    fn blind_provider_has_no_per_request_claimability_guarantee() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let first = pool.reserve(partition, 2).unwrap();
        let second = pool.reserve(partition, 2).unwrap();
        let leaked_from_first = pool.acquire(partition, D0).unwrap();

        // Global counts still balance, so the pool cannot know that this live
        // anonymous claim belongs to `first`.
        pool.release_reservation(first).unwrap();
        let only_claim_for_second = pool.acquire(partition, D0).unwrap();
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity),
            "aggregate authority cannot preserve a per-request promise"
        );

        pool.release(leaked_from_first).unwrap();
        pool.release(only_claim_for_second).unwrap();
        pool.release_reservation(second).unwrap();
    }

    #[test]
    fn dropped_provider_grant_retires_after_anonymous_owner_returns() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0]);
        let request = pool.reserve(partition, 1).unwrap();
        let connection_read_buffer = pool.acquire(partition, D0).unwrap();

        pool.release_reservation(request).unwrap();
        assert_eq!(pool.provider_reserved(partition), 1);
        assert_eq!(pool.provider_retiring(partition), 1);
        assert_eq!(
            pool.acquire(partition, D0),
            Err(AggregateError::NoPromisedCapacity),
            "retiring credit cannot authorize another anonymous claim"
        );

        pool.release(connection_read_buffer).unwrap();
        assert_eq!(pool.provider_reserved(partition), 0);
        assert_eq!(pool.provider_retiring(partition), 0);
        assert_eq!(pool.promised(partition), 0);
    }

    #[test]
    fn aggregate_retirement_drains_one_credit_per_owner_release() {
        let mut pool = AggregateProviderModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0]);
        let request = pool.reserve(partition, 2).unwrap();
        let first = pool.acquire(partition, D0).unwrap();
        let second = pool.acquire(partition, D0).unwrap();

        pool.release_reservation(request).unwrap();
        assert_eq!(pool.provider_reserved(partition), 2);
        assert_eq!(pool.provider_retiring(partition), 2);

        pool.release(first).unwrap();
        assert_eq!(pool.provider_reserved(partition), 1);
        assert_eq!(pool.provider_retiring(partition), 1);
        pool.release(second).unwrap();
        assert_eq!(pool.provider_reserved(partition), 0);
        assert_eq!(pool.provider_retiring(partition), 0);
    }

    #[test]
    fn reservation_lifetime_must_cover_every_claimed_carrier() {
        let mut pool = PoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0]);
        let reservation = match pool.reserve(partition, 1) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("reservation should fit"),
        };
        let carrier = pool.claim(reservation, D0).unwrap();

        assert_eq!(
            pool.release_reservation(reservation),
            Err(ModelError::ReservationHasLiveClaims)
        );
        pool.release_carrier(carrier).unwrap();
        pool.release_reservation(reservation).unwrap();
    }

    #[test]
    fn reservation_affinity_is_broad_but_claimed_carrier_home_is_concrete() {
        let mut pool = PoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D1]);
        let reservation = match pool.reserve(partition, 1) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("reservation should fit"),
        };

        assert_eq!(pool.affinity(reservation).unwrap(), two_domain_affinity());
        let carrier = pool.claim(reservation, D1).unwrap();
        assert_eq!(carrier.home, D1);
        pool.release_carrier(carrier).unwrap();
        pool.release_reservation(reservation).unwrap();
    }

    #[test]
    fn strict_fifo_prevents_a_small_request_from_bypassing_a_large_waiter() {
        let mut pool = PoolModel::default();
        let partition = pool.add_partition(two_domain_affinity(), [D0, D0, D0, D0]);
        let holder = match pool.reserve(partition, 3) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("holder should fit"),
        };
        let large_waiter = match pool.reserve(partition, 2) {
            ReserveOutcome::Pending(id) => id,
            ReserveOutcome::Ready(_) => panic!("large request should wait"),
        };
        let small_waiter = match pool.reserve(partition, 1) {
            ReserveOutcome::Pending(id) => id,
            ReserveOutcome::Ready(_) => panic!("small request must not bypass"),
        };

        let grants = pool.release_reservation(holder).unwrap();
        assert_eq!(
            grants.iter().map(|grant| grant.waiter).collect::<Vec<_>>(),
            vec![large_waiter, small_waiter]
        );
    }

    #[test]
    fn global_fifo_can_idle_an_independent_partition() {
        let mut pool = PoolModel::default();
        let p0 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D0]), DomainSet::from_domains([D0])),
            [D0],
        );
        let p1 = pool.add_partition(
            DispatchAffinity::new(DomainSet::from_domains([D1]), DomainSet::from_domains([D1])),
            [D1],
        );
        let holder = match pool.reserve(p0, 1) {
            ReserveOutcome::Ready(id) => id,
            ReserveOutcome::Pending(_) => panic!("holder should fit"),
        };
        let p0_waiter = match pool.reserve(p0, 1) {
            ReserveOutcome::Pending(id) => id,
            ReserveOutcome::Ready(_) => panic!("p0 is full"),
        };
        let p1_waiter = match pool.reserve(p1, 1) {
            ReserveOutcome::Pending(id) => id,
            ReserveOutcome::Ready(_) => panic!("global FIFO queues behind p0"),
        };

        // p1 is physically idle, but strict global FIFO preserves scheduler order.
        assert_eq!(pool.promised(p1), 0);
        let grants = pool.release_reservation(holder).unwrap();
        assert_eq!(
            grants.iter().map(|grant| grant.waiter).collect::<Vec<_>>(),
            vec![p0_waiter, p1_waiter]
        );
    }

    #[test]
    fn two_shape_pool_can_have_free_bytes_but_no_large_extent() {
        let mut pool = ExtentModel::new(4, 4);
        for extent in 0..4 {
            pool.claim_small_at(extent, 0);
        }

        assert_eq!(pool.free_small_slots(), 12);
        assert_eq!(
            pool.claim_large(),
            None,
            "one live small slot in every extent strands every large shape"
        );
    }

    #[test]
    fn opportunistic_run_is_escrowed_when_contiguous_carriers_exist() {
        let mut pool = OpportunisticRunModel::new(8);
        let reservation = pool.reserve(4).unwrap();

        assert_eq!(
            pool.placement(reservation),
            Placement::Contiguous { first: 0, count: 4 }
        );
        assert_eq!(pool.claim(reservation).unwrap(), vec![0, 1, 2, 3]);
        pool.release_claim(reservation).unwrap();
        pool.release_reservation(reservation).unwrap();
    }

    #[test]
    fn fragmented_run_falls_back_to_scatter_without_losing_liveness() {
        let mut pool = OpportunisticRunModel::new(8);
        let mut holders = Vec::new();
        for expected in 0..8 {
            let reservation = pool.reserve(1).unwrap();
            assert_eq!(pool.claim(reservation).unwrap(), vec![expected]);
            holders.push(reservation);
        }

        // Leave every even carrier live and every odd carrier free. Four units
        // are available, but no contiguous run is longer than one carrier.
        for index in [1usize, 3, 5, 7] {
            pool.release_claim(holders[index]).unwrap();
            pool.release_reservation(holders[index]).unwrap();
        }

        let part = pool.reserve(4).expect("aggregate capacity is available");
        assert_eq!(pool.placement(part), Placement::Scatter { count: 4 });
        assert_eq!(
            pool.claim(part).unwrap(),
            vec![1, 3, 5, 7],
            "fragmentation changes placement, not admission"
        );
    }

    #[test]
    fn cancelled_contiguous_escrow_returns_every_carrier() {
        let mut pool = OpportunisticRunModel::new(8);
        let cancelled = pool.reserve(4).unwrap();
        pool.release_reservation(cancelled).unwrap();

        let whole_pool = pool.reserve(8).unwrap();
        assert_eq!(
            pool.placement(whole_pool),
            Placement::Contiguous { first: 0, count: 8 }
        );
    }

    #[test]
    fn retaining_short_read_tail_makes_carrier_demand_exact() {
        let reads = [1usize; 8];
        let mut retained = ShortReadModel::new(4, ShortReadPolicy::RetainTail);
        let mut abandoned = ShortReadModel::new(4, ShortReadPolicy::AbandonTail);
        for read in reads {
            retained.record_read(read);
            abandoned.record_read(read);
        }

        assert_eq!(retained.carriers, 2);
        assert_eq!(abandoned.carriers, 8);
    }

    #[derive(Clone, Debug)]
    struct SegmentedBody {
        segments: Arc<[Bytes]>,
        next: usize,
        remaining: u64,
    }

    impl SegmentedBody {
        fn new(segments: Arc<[Bytes]>) -> Self {
            let remaining = segments.iter().map(|segment| segment.len() as u64).sum();
            Self {
                segments,
                next: 0,
                remaining,
            }
        }
    }

    impl Body for SegmentedBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let Some(segment) = self.segments.get(self.next).cloned() else {
                return Poll::Ready(None);
            };
            self.next += 1;
            self.remaining -= segment.len() as u64;
            Poll::Ready(Some(Ok(Frame::data(segment))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.remaining)
        }

        fn is_end_stream(&self) -> bool {
            self.next == self.segments.len()
        }
    }

    fn retryable_segmented_sdk_body(segments: Arc<[Bytes]>) -> SdkBody {
        SdkBody::retryable(move || {
            SdkBody::from_body_1_x(SegmentedBody::new(Arc::clone(&segments)))
        })
    }

    async fn collect_body(mut body: SdkBody) -> Vec<Bytes> {
        let mut frames = Vec::new();
        while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
            frames.push(
                frame
                    .expect("segmented body is infallible")
                    .into_data()
                    .expect("segmented body emits only data"),
            );
        }
        frames
    }

    #[tokio::test]
    async fn segmented_sdk_body_is_retryable_known_length_and_streaming() {
        let segments: Arc<[Bytes]> = vec![
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"defg"),
            Bytes::from_static(b"h"),
        ]
        .into();
        let body = retryable_segmented_sdk_body(segments.clone());

        assert_eq!(Body::size_hint(&body).exact(), Some(8));
        assert!(
            body.bytes().is_none(),
            "multi-frame body cannot use the SDK's in-memory checksum path"
        );
        let retry = body
            .try_clone()
            .expect("rebuild closure makes body retryable");

        assert_eq!(collect_body(body).await, segments.as_ref());
        assert_eq!(collect_body(retry).await, segments.as_ref());
    }

    #[derive(Debug)]
    struct DropTrackedOwner {
        data: Box<[u8]>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for DropTrackedOwner {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for DropTrackedOwner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn retryable_segmented_body_pins_carriers_across_retry_window() {
        let drops = Arc::new(AtomicUsize::new(0));
        let segment = Bytes::from_owner(DropTrackedOwner {
            data: Box::from(&b"abc"[..]),
            drops: Arc::clone(&drops),
        });
        let body = retryable_segmented_sdk_body(vec![segment].into());
        let retry = body
            .try_clone()
            .expect("segmented body has a rebuild closure");

        assert_eq!(collect_body(body).await, [Bytes::from_static(b"abc")]);
        assert_eq!(
            drops.load(Ordering::Relaxed),
            0,
            "the retry checkpoint still owns the physical carrier"
        );

        drop(retry);
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "carrier returns only after the retryable body is discarded"
        );
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct BodyStorageMarker(u64);

    #[tokio::test]
    async fn smithy_request_extension_reaches_the_http_connector() {
        let (client, captured) = sdk_client_capturing_one_request();
        client
            .head_object()
            .bucket("bucket")
            .key("key")
            .customize()
            .mutate_request(|request| {
                request.add_extension(BodyStorageMarker(41));
            })
            .send()
            .await
            .expect("capture client returns a successful HeadObject response");

        let request = captured
            .expect_request()
            .try_into_http1x()
            .expect("captured request converts to http 1.x");
        assert_eq!(
            request.extensions().get::<BodyStorageMarker>(),
            Some(&BodyStorageMarker(41))
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn mutate_request_reinserts_context_on_every_sdk_attempt() {
        fn empty_request() -> http::Request<SdkBody> {
            http::Request::builder().body(SdkBody::empty()).unwrap()
        }

        let http_client = StaticReplayClient::new(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(500)
                    .body(SdkBody::from(
                        r#"<Error><Code>InternalError</Code></Error>"#,
                    ))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);
        let config = aws_sdk_s3::Config::builder()
            .with_test_defaults()
            .http_client(http_client.clone())
            .region(Region::new("us-east-1"))
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(2))
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        let interceptor_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&interceptor_calls);

        client
            .head_object()
            .bucket("bucket")
            .key("key")
            .customize()
            .mutate_request(move |request| {
                let attempt = calls.fetch_add(1, Ordering::Relaxed) + 1;
                request.add_extension(BodyStorageMarker(attempt as u64));
            })
            .send()
            .await
            .expect("second attempt succeeds");

        assert_eq!(
            interceptor_calls.load(Ordering::Relaxed),
            2,
            "request rewind clears extensions, then mutate_request must run again"
        );
        assert_eq!(http_client.actual_requests().count(), 2);
    }

    fn sdk_client_capturing_one_request() -> (
        aws_sdk_s3::Client,
        aws_smithy_http_client::test_util::CaptureRequestReceiver,
    ) {
        let (http_client, captured) = capture_request(None);
        let config = aws_sdk_s3::Config::builder()
            .with_test_defaults()
            .http_client(http_client)
            .region(Region::new("us-east-1"))
            .build();
        (aws_sdk_s3::Client::from_conf(config), captured)
    }

    async fn collect_body_bytes(body: SdkBody) -> Vec<u8> {
        collect_body(body)
            .await
            .into_iter()
            .flat_map(|frame| frame.to_vec())
            .collect()
    }

    fn segmented_upload_body() -> ByteStream {
        let segments: Arc<[Bytes]> = vec![
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"defg"),
            Bytes::from_static(b"h"),
        ]
        .into();
        ByteStream::new(retryable_segmented_sdk_body(segments))
    }

    #[tokio::test]
    async fn sdk_owned_checksum_encodes_segmented_body_as_aws_chunked() {
        let (client, captured) = sdk_client_capturing_one_request();
        client
            .upload_part()
            .bucket("bucket")
            .key("key")
            .upload_id("upload-id")
            .part_number(1)
            .content_length(8)
            .body(segmented_upload_body())
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .send()
            .await
            .expect("capture client returns a successful UploadPart response");

        let mut request = captured.expect_request();
        assert_eq!(
            request.headers().get("content-encoding"),
            Some("aws-chunked")
        );
        assert_eq!(
            request.headers().get("x-amz-trailer"),
            Some("x-amz-checksum-crc32")
        );
        assert_eq!(
            request.headers().get("x-amz-decoded-content-length"),
            Some("8")
        );
        assert!(
            request.headers().get("x-amz-checksum-crc32").is_none(),
            "SDK-owned streaming checksum is emitted as a trailer"
        );

        let encoded = collect_body_bytes(request.take_body()).await;
        let encoded = String::from_utf8(encoded).expect("aws-chunked framing is ASCII");
        assert!(encoded.contains("abc"));
        assert!(encoded.contains("defg"));
        assert!(encoded.contains("h"));
        assert!(encoded.contains("\r\n0\r\nx-amz-checksum-crc32:"));
    }

    #[tokio::test]
    async fn supplied_checksum_keeps_segmented_body_plain_and_retryable() {
        let (client, captured) = sdk_client_capturing_one_request();
        client
            .upload_part()
            .bucket("bucket")
            .key("key")
            .upload_id("upload-id")
            .part_number(1)
            .content_length(8)
            .body(segmented_upload_body())
            .checksum_crc32("ru8qUA==")
            .send()
            .await
            .expect("capture client returns a successful UploadPart response");

        let mut request = captured.expect_request();
        assert_eq!(
            request.headers().get("x-amz-checksum-crc32"),
            Some("ru8qUA==")
        );
        assert!(request.headers().get("content-encoding").is_none());
        assert!(request.headers().get("x-amz-trailer").is_none());
        assert!(request
            .headers()
            .get("x-amz-decoded-content-length")
            .is_none());
        assert_eq!(request.headers().get("content-length"), Some("8"));
        assert_eq!(collect_body_bytes(request.take_body()).await, b"abcdefgh");
    }
}

#[cfg(all(test, s3_tm_loom))]
mod pressure_loom_tests {
    use std::collections::VecDeque;

    use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    const FREE: usize = 0;

    #[derive(Clone, Copy, Debug)]
    enum PressureJob {
        Claim(usize),
        Barrier(usize),
    }

    /// Concurrency model for the pressure epoch and serialized fallback runner.
    ///
    /// Even epochs admit optimistic bitmap claims. An odd epoch diverts new
    /// claims to the FIFO. A fast claimant that raced the close must validate
    /// its observed epoch after the bitmap CAS or return the carrier and wake
    /// the runner.
    struct PressureHarness {
        epoch: AtomicUsize,
        runner: AtomicBool,
        wake_epoch: AtomicUsize,
        carrier: AtomicUsize,
        queue: Mutex<VecDeque<PressureJob>>,
        completed: AtomicUsize,
    }

    impl PressureHarness {
        fn new(carrier_owner: usize) -> Self {
            Self {
                epoch: AtomicUsize::new(0),
                runner: AtomicBool::new(false),
                wake_epoch: AtomicUsize::new(0),
                carrier: AtomicUsize::new(carrier_owner),
                queue: Mutex::new(VecDeque::new()),
                completed: AtomicUsize::new(0),
            }
        }

        fn fast_claim(&self, owner: usize) {
            let observed = self.epoch.load(Ordering::SeqCst);
            if observed & 1 != 0 {
                self.enqueue(PressureJob::Claim(owner));
                return;
            }

            thread::yield_now();
            if self
                .carrier
                .compare_exchange(FREE, owner, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                self.enqueue(PressureJob::Claim(owner));
                return;
            }

            thread::yield_now();
            if self.epoch.load(Ordering::SeqCst) == observed {
                self.complete(owner);
                return;
            }

            // Pressure closed after the optimistic read. The provisional owner
            // is not committed; returning it is a wake-producing free edge.
            self.carrier
                .compare_exchange(owner, FREE, Ordering::SeqCst, Ordering::SeqCst)
                .expect("only this provisional claimant can own its carrier");
            self.signal();
            self.enqueue(PressureJob::Claim(owner));
        }

        fn enqueue_barrier(&self, id: usize) {
            self.enqueue(PressureJob::Barrier(id));
        }

        fn enqueue(&self, job: PressureJob) {
            {
                let mut queue = self.queue.lock().unwrap();
                queue.push_back(job);
                // The queue lock composes publication with close/reopen. A
                // runner cannot reopen between this push and the odd epoch.
                self.epoch.fetch_or(1, Ordering::SeqCst);
                self.wake_epoch.fetch_add(1, Ordering::SeqCst);
            }
            self.drive();
        }

        fn release(&self, owner: usize) {
            self.carrier
                .compare_exchange(owner, FREE, Ordering::SeqCst, Ordering::SeqCst)
                .expect("release must match the physical owner");
            self.signal();
        }

        fn signal(&self) {
            self.wake_epoch.fetch_add(1, Ordering::SeqCst);
            if self.epoch.load(Ordering::SeqCst) & 1 != 0 {
                self.drive();
            }
        }

        fn drive(&self) {
            if self.epoch.load(Ordering::SeqCst) & 1 == 0
                || self
                    .runner
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return;
            }

            let observed_wake = self.wake_epoch.load(Ordering::SeqCst);
            let mut queue = self.queue.lock().unwrap();
            if self.epoch.load(Ordering::SeqCst) & 1 == 0 {
                // The close observed before runner election may already have
                // been drained and reopened by another runner.
                self.runner.store(false, Ordering::SeqCst);
                return;
            }

            loop {
                let Some(job) = queue.front().copied() else {
                    break;
                };
                match job {
                    PressureJob::Barrier(id) => {
                        queue.pop_front();
                        thread::yield_now();
                        self.complete(id);
                    }
                    PressureJob::Claim(owner) => {
                        if self
                            .carrier
                            .compare_exchange(FREE, owner, Ordering::SeqCst, Ordering::SeqCst)
                            .is_err()
                        {
                            break;
                        }
                        queue.pop_front();
                        self.complete(owner);
                    }
                }
            }

            if queue.is_empty() {
                self.epoch.fetch_add(1, Ordering::SeqCst);
                // Clear RUNNER while holding the queue lock. A producer either
                // published before reopen and was observed, or publishes after
                // this edge and can elect itself.
                self.runner.store(false, Ordering::SeqCst);
                return;
            }

            self.runner.store(false, Ordering::SeqCst);
            drop(queue);
            // A free edge can race the failed scan while RUNNER is still true.
            // The event counter hands that wake to the retiring runner.
            if self.wake_epoch.load(Ordering::SeqCst) != observed_wake {
                self.drive();
            }
        }

        fn complete(&self, id: usize) {
            let bit = 1usize << id;
            let previous = self.completed.fetch_or(bit, Ordering::SeqCst);
            assert_eq!(previous & bit, 0, "job completed more than once");
        }

        fn completed(&self, id: usize) -> bool {
            self.completed.load(Ordering::SeqCst) & (1usize << id) != 0
        }
    }

    const BLOCK_ACTIVE: usize = 0;
    const BLOCK_DRAINING: usize = 1;

    struct TrimBlock {
        generation: u64,
        state: AtomicUsize,
        carrier: AtomicUsize,
        retired: Arc<AtomicUsize>,
    }

    impl TrimBlock {
        fn new(generation: u64, retired: Arc<AtomicUsize>) -> Self {
            Self {
                generation,
                state: AtomicUsize::new(BLOCK_ACTIVE),
                carrier: AtomicUsize::new(FREE),
                retired,
            }
        }
    }

    impl Drop for TrimBlock {
        fn drop(&mut self) {
            assert_eq!(
                self.carrier.load(Ordering::SeqCst),
                FREE,
                "mapping retired while a committed or provisional owner remained"
            );
            self.retired.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TrimCarrierOwner {
        owner: usize,
        block: Arc<TrimBlock>,
    }

    impl Drop for TrimCarrierOwner {
        fn drop(&mut self) {
            self.block
                .carrier
                .compare_exchange(self.owner, FREE, Ordering::SeqCst, Ordering::SeqCst)
                .expect("owner returns directly through its retained block Arc");
        }
    }

    /// Logical ArcSwap/generational-registry model. The mutex represents
    /// atomic snapshot publication, not a proposed hot-path lock.
    struct TrimRegistryHarness {
        epoch: AtomicUsize,
        current: Mutex<Option<Arc<TrimBlock>>>,
    }

    impl TrimRegistryHarness {
        fn new(block: Arc<TrimBlock>) -> Self {
            Self {
                epoch: AtomicUsize::new(0),
                current: Mutex::new(Some(block)),
            }
        }

        fn claim(&self, generation: u64, owner: usize) -> Option<TrimCarrierOwner> {
            let observed = self.epoch.load(Ordering::Acquire);
            if observed & 1 != 0 {
                return None;
            }

            let block = self.current.lock().unwrap().as_ref()?.clone();
            if block.generation != generation || block.state.load(Ordering::Acquire) != BLOCK_ACTIVE
            {
                return None;
            }
            if block
                .carrier
                .compare_exchange(FREE, owner, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return None;
            }

            thread::yield_now();
            let active = block.state.load(Ordering::Acquire) == BLOCK_ACTIVE;
            // Epoch validation is the final commit check. If trim closed after
            // the state check, this load still rejects the provisional owner.
            if active && self.epoch.load(Ordering::Acquire) == observed {
                return Some(TrimCarrierOwner { owner, block });
            }

            block
                .carrier
                .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
                .expect("provisional owner remains private until epoch validation");
            None
        }

        fn trim_and_replace(&self, replacement: Arc<TrimBlock>) {
            let previous = self.epoch.fetch_or(1, Ordering::AcqRel);
            assert_eq!(previous & 1, 0, "model permits only one trim runner");

            let old = {
                let mut current = self.current.lock().unwrap();
                let old = current.take().expect("active block exists");
                old.state.store(BLOCK_DRAINING, Ordering::Release);
                *current = Some(replacement);
                old
            };

            // Registry replacement happens-before a claimant that observes the
            // reopened even epoch.
            self.epoch.fetch_add(1, Ordering::Release);
            drop(old);
        }
    }

    #[test]
    fn fast_claim_racing_pressure_close_is_committed_or_requeued_once() {
        loom::model(|| {
            let pool = Arc::new(PressureHarness::new(FREE));
            let fast = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || pool.fast_claim(1))
            };
            let close = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || pool.enqueue_barrier(2))
            };

            fast.join().unwrap();
            close.join().unwrap();
            assert!(pool.completed(1));
            assert!(pool.completed(2));
            assert_eq!(pool.carrier.load(Ordering::SeqCst), 1);
            assert_eq!(pool.epoch.load(Ordering::SeqCst) & 1, 0);
            assert!(!pool.runner.load(Ordering::SeqCst));
            assert!(pool.queue.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn owner_return_racing_runner_retirement_cannot_lose_wake() {
        loom::model(|| {
            let pool = Arc::new(PressureHarness::new(9));
            let waiter = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || pool.enqueue(PressureJob::Claim(1)))
            };
            let release = {
                let pool = Arc::clone(&pool);
                thread::spawn(move || pool.release(9))
            };

            waiter.join().unwrap();
            release.join().unwrap();
            assert!(pool.completed(1));
            assert_eq!(pool.carrier.load(Ordering::SeqCst), 1);
            assert_eq!(pool.epoch.load(Ordering::SeqCst) & 1, 0);
            assert!(!pool.runner.load(Ordering::SeqCst));
            assert!(pool.queue.lock().unwrap().is_empty());
        });
    }

    #[test]
    fn old_snapshot_claim_cannot_outlive_mapping_or_alias_reused_generation() {
        loom::model(|| {
            let old_retired = Arc::new(AtomicUsize::new(0));
            let new_retired = Arc::new(AtomicUsize::new(0));
            let old = Arc::new(TrimBlock::new(7, Arc::clone(&old_retired)));
            let registry = Arc::new(TrimRegistryHarness::new(old));
            let retained_owner = Arc::new(Mutex::new(None));

            let claim = {
                let registry = Arc::clone(&registry);
                let retained_owner = Arc::clone(&retained_owner);
                thread::spawn(move || {
                    *retained_owner.lock().unwrap() = registry.claim(7, 1);
                })
            };
            let trim = {
                let registry = Arc::clone(&registry);
                let new_retired = Arc::clone(&new_retired);
                thread::spawn(move || {
                    registry.trim_and_replace(Arc::new(TrimBlock::new(8, new_retired)));
                })
            };

            claim.join().unwrap();
            trim.join().unwrap();
            assert!(
                registry.claim(7, 2).is_none(),
                "a stale hint cannot name the replacement mapping"
            );

            let owner = retained_owner.lock().unwrap().take();
            if owner.is_some() {
                assert_eq!(
                    old_retired.load(Ordering::SeqCst),
                    0,
                    "committed owner must retain the old mapping"
                );
            }
            drop(owner);
            assert_eq!(old_retired.load(Ordering::SeqCst), 1);
            assert_eq!(new_retired.load(Ordering::SeqCst), 0);
        });
    }
}
