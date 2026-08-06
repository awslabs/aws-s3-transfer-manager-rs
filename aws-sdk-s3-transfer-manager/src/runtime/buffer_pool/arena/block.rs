/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Claim, drain, and revival lifecycle for one stable arena block.
//!
//! [`BlockSlot`] owns stable geometry and a reserved address range. Each
//! activation publishes a fresh [`BlockIncarnation`] and occupancy bitmap.
//! Claim attempts retain the incarnation they first observed until their
//! Active/Draining gate completes. A failed gate therefore rolls back only
//! into retired metadata, never into a revived bitmap.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

#[cfg(all(test, s3_tm_loom))]
use crate::runtime::sync::sync::atomic::fence;
#[cfg(not(all(test, s3_tm_loom)))]
use arc_swap::{ArcSwapOption, Guard};

const ACTIVE: u8 = 0;
const DRAINING: u8 = 1;
const DEAD: u8 = 2;

/// Stable location of one carrier within the arena.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CarrierId {
    pub(super) slot: u32,
    pub(super) index: u32,
    identity: IncarnationIdentity,
}

/// Result of attempting to remove one block from prepared capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TrimResult {
    /// The slot has no active incarnation.
    NotPrepared,
    /// Removing the block would violate the caller's prepared-capacity floor.
    Required,
    /// A carrier claim won the drain race, so the block returned to Active.
    Busy,
    /// Cleanup completed and the slot may be revived.
    Trimmed,
    /// Cleanup failed. The block remains Draining and cannot be revived.
    Quarantined,
}

/// Linear cleanup work created after successful trim confirmation.
#[must_use = "confirmed trim cleanup must be completed or quarantined"]
pub(super) struct TrimCleanup {
    slot: Arc<BlockSlot>,
    incarnation: Arc<BlockIncarnation>,
    complete: bool,
}

/// Failure to activate a block slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReviveError {
    AlreadyPrepared,
    CapacityOverflow,
}

/// One reserved virtual range whose address does not change across revivals.
///
/// Heap storage is a deterministic substitute for mmap in this executable
/// design. `commit` and `decommit` model physical preparation while preserving
/// the address and ownership rules used by the production mapping backend.
struct RegionReservation {
    data: Box<[UnsafeCell<MaybeUninit<u8>>]>,
    committed: AtomicBool,
    fail_next_decommit: AtomicBool,
}

// SAFETY: a region address is exposed only after a carrier bit passes the
// Active gate. That bit grants exclusive mutable ownership and remains set
// until every mutable or immutable carrier owner has dropped.
unsafe impl Send for RegionReservation {}
unsafe impl Sync for RegionReservation {}

/// Occupancy and lifecycle state for one committed use of a block slot.
struct BlockIncarnation {
    region: Arc<RegionReservation>,
    state: AtomicU8,
    in_use: Box<[AtomicU64]>,
}

/// Stable block geometry and replaceable occupancy metadata.
pub(super) struct BlockSlot {
    slot: u32,
    carrier_size: usize,
    carrier_count: usize,
    region: Arc<RegionReservation>,
    valid_masks: Box<[u64]>,
    current: IncarnationCell,
    lifecycle: Mutex<()>,
}

/// Temporary protection for the incarnation loaded by a claim attempt.
///
/// Production uses ArcSwap's debt-based guard. Loom uses an owned `Arc`
/// loaded through a mutex because ArcSwap is not Loom-instrumented.
struct IncarnationGuard {
    #[cfg(not(all(test, s3_tm_loom)))]
    inner: Guard<Option<Arc<BlockIncarnation>>>,
    #[cfg(all(test, s3_tm_loom))]
    inner: Option<Arc<BlockIncarnation>>,
}

/// Replaceable incarnation pointer.
struct IncarnationCell {
    #[cfg(not(all(test, s3_tm_loom)))]
    inner: ArcSwapOption<BlockIncarnation>,
    #[cfg(all(test, s3_tm_loom))]
    inner: Mutex<Option<Arc<BlockIncarnation>>>,
}

/// Diagnostic identity for a gate-passed carrier owner.
///
/// The live bitmap bit, not this value, prevents incarnation replacement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct IncarnationIdentity(usize);

/// Bits won in one bitmap word.
#[derive(Clone, Copy, Debug)]
struct WonWord {
    word: usize,
    mask: u64,
}

/// Claim state retained from the first bitmap mutation through the state gate.
pub(super) struct BlockClaimAttempt {
    slot: Arc<BlockSlot>,
    incarnation: IncarnationGuard,
    won: Vec<WonWord>,
}

/// Linear owner of gate-passed bits not yet split into carrier allocations.
pub(super) struct ProvisionalBits {
    slot: Arc<BlockSlot>,
    identity: IncarnationIdentity,
    won: Vec<WonWord>,
}

/// Linear physical ownership of one claimed carrier.
pub(super) struct ClaimedCarrier {
    slot: Arc<BlockSlot>,
    id: CarrierId,
    ptr: NonNull<MaybeUninit<u8>>,
    capacity: usize,
}

// SAFETY: the carrier's live bit grants exclusive mutable access. Moving the
// linear owner transfers that authority without duplicating it.
unsafe impl Send for ClaimedCarrier {}
unsafe impl Sync for ClaimedCarrier {}

impl RegionReservation {
    fn new(capacity: usize) -> Self {
        let data = std::iter::repeat_with(|| UnsafeCell::new(MaybeUninit::uninit()))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            data,
            committed: AtomicBool::new(false),
            fail_next_decommit: AtomicBool::new(false),
        }
    }

    fn commit(&self) {
        self.committed.store(true, Ordering::Release);
    }

    fn decommit(&self) -> Result<(), ()> {
        if self.fail_next_decommit.swap(false, Ordering::AcqRel) {
            return Err(());
        }
        self.committed.store(false, Ordering::Release);
        Ok(())
    }

    fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    fn carrier_ptr(&self, index: usize, carrier_size: usize) -> NonNull<MaybeUninit<u8>> {
        assert!(
            self.is_committed(),
            "a carrier address requires committed storage"
        );
        let offset = index
            .checked_mul(carrier_size)
            .expect("carrier offset fits the reserved region");
        let ptr = self
            .data
            .get(offset)
            .expect("carrier offset is within the reserved region")
            .get();
        NonNull::new(ptr).expect("a nonempty region has a non-null base")
    }

    #[cfg(test)]
    fn fail_next_decommit(&self) {
        self.fail_next_decommit.store(true, Ordering::Release);
    }
}

impl BlockIncarnation {
    fn new(region: Arc<RegionReservation>, words: usize) -> Self {
        let in_use = std::iter::repeat_with(|| AtomicU64::new(0))
            .take(words)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            region,
            state: AtomicU8::new(ACTIVE),
            in_use,
        }
    }

    fn identity(this: &Arc<Self>) -> IncarnationIdentity {
        IncarnationIdentity(Arc::as_ptr(this) as usize)
    }
}

impl IncarnationCell {
    fn new() -> Self {
        Self {
            #[cfg(not(all(test, s3_tm_loom)))]
            inner: ArcSwapOption::empty(),
            #[cfg(all(test, s3_tm_loom))]
            inner: Mutex::new(None),
        }
    }

    fn load(&self) -> IncarnationGuard {
        IncarnationGuard {
            #[cfg(not(all(test, s3_tm_loom)))]
            inner: self.inner.load(),
            #[cfg(all(test, s3_tm_loom))]
            inner: self.inner.lock().clone(),
        }
    }

    fn swap(&self, next: Option<Arc<BlockIncarnation>>) -> Option<Arc<BlockIncarnation>> {
        #[cfg(not(all(test, s3_tm_loom)))]
        {
            self.inner.swap(next)
        }
        #[cfg(all(test, s3_tm_loom))]
        {
            std::mem::replace(&mut *self.inner.lock(), next)
        }
    }
}

impl IncarnationGuard {
    fn as_ref(&self) -> Option<&Arc<BlockIncarnation>> {
        self.inner.as_ref()
    }
}

impl BlockSlot {
    /// Reserve one stable block geometry without preparing physical capacity.
    pub(super) fn new(
        slot: u32,
        carrier_size: usize,
        carrier_count: usize,
    ) -> Result<Self, ReviveError> {
        assert!(carrier_size > 0, "carrier size must be nonzero");
        assert!(carrier_count > 0, "a block must contain carriers");
        u32::try_from(carrier_count - 1).map_err(|_| ReviveError::CapacityOverflow)?;
        let capacity = carrier_size
            .checked_mul(carrier_count)
            .ok_or(ReviveError::CapacityOverflow)?;
        let word_count = carrier_count.div_ceil(u64::BITS as usize);
        let mut valid_masks = vec![u64::MAX; word_count];
        let final_bits = carrier_count % u64::BITS as usize;
        if final_bits != 0 {
            *valid_masks.last_mut().unwrap() = (1u64 << final_bits) - 1;
        }

        Ok(Self {
            slot,
            carrier_size,
            carrier_count,
            region: Arc::new(RegionReservation::new(capacity)),
            valid_masks: valid_masks.into_boxed_slice(),
            current: IncarnationCell::new(),
            lifecycle: Mutex::new(()),
        })
    }

    pub(super) fn carrier_count(&self) -> usize {
        self.carrier_count
    }

    /// Prepare a fresh incarnation for this stable slot.
    pub(super) fn revive(&self, prepared: &mut usize) -> Result<(), ReviveError> {
        let _lifecycle = self.lifecycle.lock();
        if self.current.load().as_ref().is_some() {
            return Err(ReviveError::AlreadyPrepared);
        }
        let next_prepared = prepared
            .checked_add(self.carrier_count)
            .ok_or(ReviveError::CapacityOverflow)?;

        self.region.commit();
        let incarnation = Arc::new(BlockIncarnation::new(
            Arc::clone(&self.region),
            self.valid_masks.len(),
        ));
        let previous = self.current.swap(Some(incarnation));
        assert!(previous.is_none(), "lifecycle lock serializes revival");
        *prepared = next_prepared;
        Ok(())
    }

    /// Load the current incarnation before making any bitmap mutation.
    pub(super) fn begin_claim(slot: &Arc<Self>) -> Option<BlockClaimAttempt> {
        let incarnation = slot.current.load();
        incarnation.as_ref()?;
        Some(BlockClaimAttempt {
            slot: Arc::clone(slot),
            incarnation,
            won: Vec::new(),
        })
    }

    /// Claim up to `count` carriers from this block.
    pub(super) fn try_claim(slot: &Arc<Self>, count: usize) -> Option<ProvisionalBits> {
        assert!(count > 0, "a block claim must request carriers");
        let mut attempt = Self::begin_claim(slot)?;
        attempt.take(count);
        attempt.finish()
    }

    /// Confirm that this block may be removed from prepared capacity.
    ///
    /// The caller holds admission serialization through this method. The
    /// prepared count changes only after the confirming bitmap scan succeeds.
    /// Returned cleanup runs after the caller releases admission serialization.
    pub(super) fn start_trim(
        slot: &Arc<Self>,
        prepared: &mut usize,
        floor: usize,
    ) -> Result<TrimCleanup, TrimResult> {
        let _lifecycle = slot.lifecycle.lock();
        let current = slot.current.load();
        let Some(incarnation) = current.as_ref() else {
            return Err(TrimResult::NotPrepared);
        };
        match incarnation.state.load(Ordering::Acquire) {
            ACTIVE => {}
            DRAINING => return Err(TrimResult::Quarantined),
            _ => return Err(TrimResult::NotPrepared),
        }
        assert!(
            *prepared >= slot.carrier_count,
            "prepared capacity includes every Active block"
        );
        if *prepared - slot.carrier_count < floor {
            return Err(TrimResult::Required);
        }
        if incarnation
            .state
            .compare_exchange(ACTIVE, DRAINING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(TrimResult::NotPrepared);
        }
        model_seq_cst_store_load();

        let busy = incarnation
            .in_use
            .iter()
            .zip(&slot.valid_masks)
            .any(|(word, valid)| word.load(Ordering::SeqCst) & valid != 0);
        if busy {
            incarnation.state.store(ACTIVE, Ordering::SeqCst);
            return Err(TrimResult::Busy);
        }

        *prepared -= slot.carrier_count;
        Ok(TrimCleanup {
            slot: Arc::clone(slot),
            incarnation: Arc::clone(incarnation),
            complete: false,
        })
    }

    fn release_won(&self, identity: IncarnationIdentity, won: &[WonWord]) {
        if won.iter().all(|won| won.mask == 0) {
            return;
        }
        let current = self.current.load();
        let Some(incarnation) = current.as_ref() else {
            invariant_violation();
        };
        if BlockIncarnation::identity(incarnation) != identity {
            invariant_violation();
        }

        for won in won {
            let previous = incarnation.in_use[won.word].fetch_and(!won.mask, Ordering::SeqCst);
            if previous & won.mask != won.mask {
                invariant_violation();
            }
        }
    }

    fn release_one(&self, id: CarrierId) {
        self.release_won(
            id.identity,
            &[WonWord {
                word: id.index as usize / u64::BITS as usize,
                mask: 1u64 << (id.index as usize % u64::BITS as usize),
            }],
        );
    }

    pub(super) fn live_carriers(&self) -> usize {
        self.current
            .load()
            .as_ref()
            .map(|incarnation| {
                incarnation
                    .in_use
                    .iter()
                    .zip(&self.valid_masks)
                    .map(|(word, valid)| {
                        (word.load(Ordering::Acquire) & valid).count_ones() as usize
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    pub(super) fn is_prepared(&self) -> bool {
        self.current
            .load()
            .as_ref()
            .map(|incarnation| incarnation.state.load(Ordering::Acquire) == ACTIVE)
            .unwrap_or(false)
    }

    pub(super) fn is_mapped(&self) -> bool {
        self.region.is_committed()
    }

    pub(super) fn is_quarantined(&self) -> bool {
        self.current
            .load()
            .as_ref()
            .map(|incarnation| incarnation.state.load(Ordering::Acquire) == DRAINING)
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn fail_next_decommit(&self) {
        self.region.fail_next_decommit();
    }

    #[cfg(test)]
    fn trim(slot: &Arc<Self>, prepared: &mut usize, floor: usize) -> TrimResult {
        match Self::start_trim(slot, prepared, floor) {
            Ok(cleanup) => {
                if cleanup.finish() {
                    TrimResult::Trimmed
                } else {
                    TrimResult::Quarantined
                }
            }
            Err(result) => result,
        }
    }
}

impl TrimCleanup {
    pub(super) fn carrier_count(&self) -> usize {
        self.slot.carrier_count
    }

    /// Decommit the confirmed block and permit a later fresh incarnation.
    pub(super) fn finish(mut self) -> bool {
        self.complete = true;
        self.finish_inner() == TrimResult::Trimmed
    }

    fn finish_inner(&self) -> TrimResult {
        let _lifecycle = self.slot.lifecycle.lock();
        let current = self.slot.current.load();
        let Some(incarnation) = current.as_ref() else {
            invariant_violation();
        };
        if !Arc::ptr_eq(incarnation, &self.incarnation)
            || incarnation.state.load(Ordering::Acquire) != DRAINING
        {
            invariant_violation();
        }

        if incarnation.region.decommit().is_err() {
            return TrimResult::Quarantined;
        }

        incarnation.state.store(DEAD, Ordering::Release);
        let removed = self
            .slot
            .current
            .swap(None)
            .expect("a draining incarnation remains published through cleanup");
        if !Arc::ptr_eq(&removed, incarnation) {
            invariant_violation();
        }
        TrimResult::Trimmed
    }
}

impl Drop for TrimCleanup {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.finish_inner();
        }
    }
}

impl BlockClaimAttempt {
    /// Set candidate bits in the protected incarnation.
    pub(super) fn take(&mut self, mut count: usize) -> usize {
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        let mut taken = 0;

        for (word_index, (word, valid)) in incarnation
            .in_use
            .iter()
            .zip(&self.slot.valid_masks)
            .enumerate()
        {
            if count == 0 {
                break;
            }
            let observed = word.load(Ordering::Relaxed);
            let candidate = take_lowest(!observed & valid, count);
            if candidate == 0 {
                continue;
            }
            let previous = word.fetch_or(candidate, Ordering::SeqCst);
            let won = candidate & !previous;
            if won != 0 {
                let won_count = won.count_ones() as usize;
                self.won.push(WonWord {
                    word: word_index,
                    mask: won,
                });
                count -= won_count;
                taken += won_count;
            }
        }
        taken
    }

    /// Complete the Active/Draining gate.
    ///
    /// Failed-gate rollback writes through this attempt's original guard. A
    /// successful result drops that guard and transfers the live bits to a
    /// linear owner.
    pub(super) fn finish(mut self) -> Option<ProvisionalBits> {
        if self.won.is_empty() {
            return None;
        }
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        model_seq_cst_store_load();
        if incarnation.state.load(Ordering::SeqCst) != ACTIVE {
            self.rollback_original();
            return None;
        }

        let identity = BlockIncarnation::identity(incarnation);
        let won = std::mem::take(&mut self.won);
        Some(ProvisionalBits {
            slot: Arc::clone(&self.slot),
            identity,
            won,
        })
    }

    fn rollback_original(&mut self) {
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        for won in self.won.drain(..) {
            let previous = incarnation.in_use[won.word].fetch_and(!won.mask, Ordering::SeqCst);
            assert_eq!(
                previous & won.mask,
                won.mask,
                "failed-gate rollback clears only bits won by this attempt"
            );
        }
    }
}

impl Drop for BlockClaimAttempt {
    fn drop(&mut self) {
        self.rollback_original();
    }
}

impl ProvisionalBits {
    pub(super) fn len(&self) -> usize {
        self.won
            .iter()
            .map(|won| won.mask.count_ones() as usize)
            .sum()
    }

    /// Split this batch owner into one linear owner per carrier.
    pub(super) fn into_carriers(mut self) -> Vec<ClaimedCarrier> {
        let count = self.len();
        let mut carriers = Vec::with_capacity(count);

        for won_index in 0..self.won.len() {
            while self.won[won_index].mask != 0 {
                let bit = self.won[won_index].mask.trailing_zeros() as usize;
                let mask = 1u64 << bit;
                let index = self.won[won_index].word * u64::BITS as usize + bit;
                assert!(index < self.slot.carrier_count);
                let id = CarrierId {
                    slot: self.slot.slot,
                    index: u32::try_from(index).expect("carrier index fits its stable ID"),
                    identity: self.identity,
                };
                let carrier = ClaimedCarrier {
                    ptr: self.slot.region.carrier_ptr(index, self.slot.carrier_size),
                    capacity: self.slot.carrier_size,
                    slot: Arc::clone(&self.slot),
                    id,
                };

                // Ownership moves before `push`: if `push` unwinds, the local
                // carrier returns this bit and ProvisionalBits owns the rest.
                self.won[won_index].mask &= !mask;
                carriers.push(carrier);
            }
        }
        carriers
    }
}

impl Drop for ProvisionalBits {
    fn drop(&mut self) {
        self.slot.release_won(self.identity, &self.won);
    }
}

impl ClaimedCarrier {
    pub(super) fn ptr(&self) -> NonNull<MaybeUninit<u8>> {
        self.ptr
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for ClaimedCarrier {
    fn drop(&mut self) {
        self.slot.release_one(self.id);
    }
}

fn take_lowest(mut available: u64, count: usize) -> u64 {
    let mut selected = 0;
    for _ in 0..count.min(u64::BITS as usize) {
        if available == 0 {
            break;
        }
        let bit = 1u64 << available.trailing_zeros();
        selected |= bit;
        available &= !bit;
    }
    selected
}

/// Model the global store-load order supplied by production `SeqCst`.
///
/// Loom 0.7 treats `SeqCst` accesses as `AcqRel`, but it models a `SeqCst`
/// fence. The test-only fence prevents a documented Loom false positive
/// without adding an extra fence to the production claim path.
#[inline]
fn model_seq_cst_store_load() {
    #[cfg(all(test, s3_tm_loom))]
    fence(Ordering::SeqCst);
}

#[cold]
fn invariant_violation() -> ! {
    #[cfg(test)]
    panic!("live carrier no longer names its claimed block incarnation");

    #[cfg(not(test))]
    std::process::abort()
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::*;

    fn prepared_slot(carriers: usize) -> (Arc<BlockSlot>, usize) {
        let slot = Arc::new(BlockSlot::new(7, 1, carriers).unwrap());
        let mut prepared = 0;
        slot.revive(&mut prepared).unwrap();
        (slot, prepared)
    }

    #[test]
    fn stale_failed_gate_rolls_back_only_retired_bitmap() {
        let (slot, mut prepared) = prepared_slot(3);
        let mut stale = BlockSlot::begin_claim(&slot).unwrap();

        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 0),
            TrimResult::Trimmed
        );
        slot.revive(&mut prepared).unwrap();
        let current = BlockSlot::try_claim(&slot, 1).unwrap().into_carriers();
        assert_eq!(current[0].id.index, 0);

        assert_eq!(stale.take(1), 1);
        assert!(stale.finish().is_none());
        assert_eq!(slot.live_carriers(), 1);

        drop(current);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn trim_abandons_when_a_gate_passed_bit_is_live() {
        let (slot, mut prepared) = prepared_slot(1);
        let carrier = BlockSlot::try_claim(&slot, 1).unwrap().into_carriers();

        assert_eq!(BlockSlot::trim(&slot, &mut prepared, 0), TrimResult::Busy);
        assert!(slot.is_prepared());
        assert_eq!(prepared, 1);

        drop(carrier);
        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 0),
            TrimResult::Trimmed
        );
        assert_eq!(prepared, 0);
    }

    #[test]
    fn trim_does_not_start_below_prepared_floor() {
        let (slot, mut prepared) = prepared_slot(3);

        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 1),
            TrimResult::Required
        );
        assert_eq!(prepared, 3);
        assert!(slot.is_prepared());
        assert!(slot.is_mapped());
    }

    #[test]
    fn confirmed_trim_rejects_claims_before_cleanup_finishes() {
        let (slot, mut prepared) = prepared_slot(1);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, 0).unwrap();

        assert_eq!(prepared, 0);
        assert!(slot.is_mapped());
        assert!(BlockSlot::try_claim(&slot, 1).is_none());

        assert!(cleanup.finish());
        assert!(!slot.is_mapped());
    }

    #[test]
    fn dropped_cleanup_finishes_confirmed_trim() {
        let (slot, mut prepared) = prepared_slot(1);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, 0).unwrap();

        drop(cleanup);

        assert_eq!(prepared, 0);
        assert!(!slot.is_mapped());
        slot.revive(&mut prepared).unwrap();
        assert_eq!(prepared, 1);
    }

    #[test]
    fn dropped_cleanup_quarantines_decommit_failure() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.fail_next_decommit();
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, 0).unwrap();

        drop(cleanup);

        assert_eq!(prepared, 0);
        assert!(slot.is_quarantined());
        assert!(slot.is_mapped());
        assert!(BlockSlot::try_claim(&slot, 1).is_none());
    }

    #[test]
    fn padding_bits_are_never_claimed_or_counted_live() {
        let (slot, mut prepared) = prepared_slot(65);
        let carriers = BlockSlot::try_claim(&slot, 65).unwrap().into_carriers();

        assert_eq!(carriers.len(), 65);
        assert_eq!(carriers.last().unwrap().id.index, 64);
        assert!(BlockSlot::try_claim(&slot, 1).is_none());
        assert_eq!(slot.live_carriers(), 65);

        drop(carriers);
        assert_eq!(slot.live_carriers(), 0);
        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 0),
            TrimResult::Trimmed
        );
    }

    #[test]
    fn failed_cleanup_quarantines_unprepared_block() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.fail_next_decommit();

        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 0),
            TrimResult::Quarantined
        );
        assert_eq!(prepared, 0);
        assert!(slot.is_quarantined());
        assert!(slot.is_mapped());
        assert!(BlockSlot::try_claim(&slot, 1).is_none());
        assert_eq!(
            slot.revive(&mut prepared),
            Err(ReviveError::AlreadyPrepared)
        );
    }

    #[test]
    fn provisional_drop_rolls_back_every_word_it_owns() {
        let (slot, mut prepared) = prepared_slot(65);
        let provisional = BlockSlot::try_claim(&slot, 65).unwrap();
        assert_eq!(provisional.len(), 65);

        drop(provisional);
        assert_eq!(slot.live_carriers(), 0);
        assert_eq!(
            BlockSlot::trim(&slot, &mut prepared, 0),
            TrimResult::Trimmed
        );
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};
    use crate::runtime::sync::thread;

    fn finish_trim(start: Result<TrimCleanup, TrimResult>) -> TrimResult {
        match start {
            Ok(cleanup) => {
                if cleanup.finish() {
                    TrimResult::Trimmed
                } else {
                    TrimResult::Quarantined
                }
            }
            Err(result) => result,
        }
    }

    #[test]
    fn stale_claim_cannot_clear_revived_claim() {
        loom::model(|| {
            let slot = Arc::new(BlockSlot::new(0, 1, 1).unwrap());
            let mut prepared = 0;
            slot.revive(&mut prepared).unwrap();
            let loaded = Arc::new(AtomicBool::new(false));
            let revived = Arc::new(AtomicBool::new(false));

            let stale_slot = Arc::clone(&slot);
            let stale_loaded = Arc::clone(&loaded);
            let stale_revived = Arc::clone(&revived);
            let stale = thread::spawn(move || {
                let mut attempt = BlockSlot::begin_claim(&stale_slot).unwrap();
                stale_loaded.store(true, Ordering::Release);
                while !stale_revived.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }
                assert_eq!(attempt.take(1), 1);
                assert!(attempt.finish().is_none());
            });

            let current_slot = Arc::clone(&slot);
            let current_loaded = Arc::clone(&loaded);
            let current_revived = Arc::clone(&revived);
            let current = thread::spawn(move || {
                while !current_loaded.load(Ordering::Acquire) {
                    loom::thread::yield_now();
                }
                let mut prepared = 1;
                assert_eq!(
                    BlockSlot::trim(&current_slot, &mut prepared, 0),
                    TrimResult::Trimmed
                );
                current_slot.revive(&mut prepared).unwrap();
                let carrier = BlockSlot::try_claim(&current_slot, 1)
                    .unwrap()
                    .into_carriers()
                    .pop()
                    .unwrap();
                current_revived.store(true, Ordering::Release);
                carrier
            });

            stale.join().unwrap();
            let carrier = current.join().unwrap();
            assert_eq!(slot.live_carriers(), 1);
            drop(carrier);
            assert_eq!(slot.live_carriers(), 0);
        });
    }

    #[test]
    fn claim_and_trim_cannot_both_win() {
        loom::model(|| {
            let slot = Arc::new(BlockSlot::new(0, 1, 1).unwrap());
            let mut initial = 0;
            slot.revive(&mut initial).unwrap();
            let prepared = Arc::new(Mutex::new(initial));

            let claim_slot = Arc::clone(&slot);
            let claim = thread::spawn(move || {
                BlockSlot::try_claim(&claim_slot, 1).map(ProvisionalBits::into_carriers)
            });

            let trim_slot = Arc::clone(&slot);
            let trim_prepared = Arc::clone(&prepared);
            let trim = thread::spawn(move || {
                let start = {
                    let mut prepared = trim_prepared.lock();
                    BlockSlot::start_trim(&trim_slot, &mut prepared, 0)
                };
                finish_trim(start)
            });

            let carriers = claim.join().unwrap();
            let result = trim.join().unwrap();
            if let Some(carriers) = carriers {
                assert_ne!(result, TrimResult::Trimmed);
                drop(carriers);
            }

            if slot.is_prepared() {
                let mut prepared = prepared.lock();
                assert_eq!(
                    BlockSlot::trim(&slot, &mut prepared, 0),
                    TrimResult::Trimmed
                );
            }
            assert!(!slot.is_mapped());
        });
    }

    #[test]
    fn final_return_prevents_or_precedes_trim() {
        loom::model(|| {
            let slot = Arc::new(BlockSlot::new(0, 1, 1).unwrap());
            let mut initial = 0;
            slot.revive(&mut initial).unwrap();
            let carrier = BlockSlot::try_claim(&slot, 1)
                .unwrap()
                .into_carriers()
                .pop()
                .unwrap();
            let prepared = Arc::new(Mutex::new(initial));

            let returning = thread::spawn(move || drop(carrier));
            let trim_slot = Arc::clone(&slot);
            let trim_prepared = Arc::clone(&prepared);
            let trimming = thread::spawn(move || {
                let start = {
                    let mut prepared = trim_prepared.lock();
                    BlockSlot::start_trim(&trim_slot, &mut prepared, 0)
                };
                finish_trim(start)
            });

            returning.join().unwrap();
            let result = trimming.join().unwrap();
            assert!(matches!(result, TrimResult::Busy | TrimResult::Trimmed));
            if slot.is_prepared() {
                let mut prepared = prepared.lock();
                assert_eq!(
                    BlockSlot::trim(&slot, &mut prepared, 0),
                    TrimResult::Trimmed
                );
            }
            assert_eq!(slot.live_carriers(), 0);
        });
    }
}
