/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Stable block geometry and carrier ownership.

use std::collections::TryReserveError;
use std::fmt;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use super::geometry::PoolGeometry;
use super::virtual_memory::{VirtualMemoryError, VirtualRange};
use super::CarrierCount;
use crate::runtime::sync::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

/// Incarnation state that permits carrier claims.
const ACTIVE: u8 = 0;
/// Incarnation state that rejects claims while trim confirms ownership.
const DRAINING: u8 = 1;
/// Incarnation state whose mapping is no longer accessible.
const DEAD: u8 = 2;

/// Failure to construct, prepare, or claim from one block.
#[derive(Debug)]
pub(super) enum BlockError {
    /// A virtual-memory operation failed.
    VirtualMemory(VirtualMemoryError),
    /// Ownership metadata could not be allocated.
    Allocation(TryReserveError),
    /// The slot already contains prepared capacity.
    AlreadyPrepared,
    /// Adding the block would overflow prepared-capacity accounting.
    PreparedCapacityOverflow,
    /// A claim requested no carriers.
    InvalidClaimCount,
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VirtualMemory(error) => error.fmt(f),
            Self::Allocation(error) => write!(f, "block metadata allocation failed: {error}"),
            Self::AlreadyPrepared => f.write_str("block already has a prepared incarnation"),
            Self::PreparedCapacityOverflow => f.write_str("prepared carrier count would overflow"),
            Self::InvalidClaimCount => f.write_str("a carrier claim must be nonzero"),
        }
    }
}

impl std::error::Error for BlockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::VirtualMemory(error) => Some(error),
            Self::Allocation(error) => Some(error),
            Self::AlreadyPrepared | Self::PreparedCapacityOverflow | Self::InvalidClaimCount => {
                None
            }
        }
    }
}

impl From<VirtualMemoryError> for BlockError {
    fn from(error: VirtualMemoryError) -> Self {
        Self::VirtualMemory(error)
    }
}

impl From<TryReserveError> for BlockError {
    fn from(error: TryReserveError) -> Self {
        Self::Allocation(error)
    }
}

/// Reason one block cannot begin a trim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TrimBlocked {
    /// The slot has no prepared incarnation.
    NotPrepared,
    /// Removing the block would violate the caller's prepared-capacity floor.
    Required,
    /// A carrier claim won the claim-trim gate.
    Busy,
    /// A prior trim or failed protection transition still owns the slot.
    CleanupPending,
}

/// Failed cleanup work that remains safe to retry while the slot is inactive.
#[derive(Debug)]
pub(super) enum CleanupRetry {
    /// Whole-range protection is unknown and must be recovered.
    ProtectionPending(VirtualMemoryError),
    /// The range is inaccessible, but backing may remain resident.
    ReclaimPending(VirtualMemoryError),
}

/// Accessibility state for one stable virtual range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MappingState {
    /// The range is inaccessible and may need another discard attempt.
    Reserved {
        /// `true` when backing reclaim has not completed.
        reclaim_pending: bool,
    },
    /// The complete range is readable and writable.
    Prepared,
    /// A protection call failed and the complete range must be recovered.
    ProtectionPending,
}

/// Stable identity of one carrier within an incarnation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CarrierId {
    /// Stable block-slot index.
    slot: u32,
    /// Carrier index within the slot.
    index: u32,
    /// Incarnation that owns the carrier bit.
    incarnation: IncarnationIdentity,
}

/// Comparison-only identity for one block activation.
///
/// The live carrier bit, not this value, prevents incarnation replacement.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct IncarnationIdentity(NonZeroUsize);

/// Bits won by one bitmap operation.
#[derive(Clone, Copy, Debug)]
struct WonWord {
    /// Index into the incarnation bitmap.
    word_index: usize,
    /// Bits changed from clear to set by the operation.
    mask: u64,
}

/// Occupancy metadata for one activation of a stable block slot.
struct BlockIncarnation {
    /// Claim-trim gate state.
    state: AtomicU8,
    /// Fixed bitmap with one ownership bit per carrier.
    ///
    /// Each atomic word covers 64 carriers. Padding bits in the final word
    /// remain clear.
    in_use: Box<[AtomicU64]>,
}

impl BlockIncarnation {
    /// Allocates an active incarnation with every valid carrier free.
    fn try_new(bitmap_words: usize) -> Result<Self, TryReserveError> {
        let mut in_use = Vec::new();
        in_use.try_reserve_exact(bitmap_words)?;
        in_use.extend((0..bitmap_words).map(|_| AtomicU64::new(0)));
        Ok(Self {
            state: AtomicU8::new(ACTIVE),
            in_use: in_use.into_boxed_slice(),
        })
    }

    /// Returns an opaque identity for diagnostic comparisons.
    fn identity(this: &Arc<Self>) -> IncarnationIdentity {
        let address = Arc::as_ptr(this).addr();
        IncarnationIdentity(
            NonZeroUsize::new(address).expect("an allocated incarnation has a nonzero address"),
        )
    }
}

#[cfg(not(all(test, s3_tm_loom)))]
mod incarnation_cell {
    use arc_swap::{ArcSwapOption, Guard};

    use super::BlockIncarnation;
    use crate::runtime::sync::sync::Arc;

    /// Atomic publication for the current block incarnation.
    pub(super) struct IncarnationCell {
        inner: ArcSwapOption<BlockIncarnation>,
    }

    impl IncarnationCell {
        /// Creates an empty publication cell.
        pub(super) fn new() -> Self {
            Self {
                inner: ArcSwapOption::empty(),
            }
        }

        /// Protects the current incarnation from metadata reclamation.
        pub(super) fn load(&self) -> IncarnationGuard {
            IncarnationGuard {
                inner: self.inner.load(),
            }
        }

        /// Replaces the current incarnation and returns the previous value.
        pub(super) fn swap(
            &self,
            next: Option<Arc<BlockIncarnation>>,
        ) -> Option<Arc<BlockIncarnation>> {
            self.inner.swap(next)
        }
    }

    /// Protection for the incarnation loaded before a bitmap mutation.
    pub(super) struct IncarnationGuard {
        inner: Guard<Option<Arc<BlockIncarnation>>>,
    }

    impl IncarnationGuard {
        /// Returns the protected incarnation, or `None` for an inactive slot.
        pub(super) fn as_ref(&self) -> Option<&Arc<BlockIncarnation>> {
            self.inner.as_ref()
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod incarnation_cell {
    use super::BlockIncarnation;
    use crate::runtime::sync::sync::{Arc, Mutex};

    /// Loom-instrumented publication for the current block incarnation.
    pub(super) struct IncarnationCell {
        inner: Mutex<Option<Arc<BlockIncarnation>>>,
    }

    impl IncarnationCell {
        /// Creates an empty publication cell.
        pub(super) fn new() -> Self {
            Self {
                inner: Mutex::new(None),
            }
        }

        /// Loads an owned reference to the current incarnation.
        pub(super) fn load(&self) -> IncarnationGuard {
            IncarnationGuard {
                inner: self.inner.lock().clone(),
            }
        }

        /// Replaces the current incarnation and returns the previous value.
        pub(super) fn swap(
            &self,
            next: Option<Arc<BlockIncarnation>>,
        ) -> Option<Arc<BlockIncarnation>> {
            std::mem::replace(&mut *self.inner.lock(), next)
        }
    }

    /// Owned protection for a Loom-loaded incarnation.
    pub(super) struct IncarnationGuard {
        inner: Option<Arc<BlockIncarnation>>,
    }

    impl IncarnationGuard {
        /// Returns the protected incarnation, or `None` for an inactive slot.
        pub(super) fn as_ref(&self) -> Option<&Arc<BlockIncarnation>> {
            self.inner.as_ref()
        }
    }
}

use incarnation_cell::{IncarnationCell, IncarnationGuard};

/// Stable block geometry, mapping, and replaceable ownership metadata.
pub(super) struct BlockSlot {
    /// Stable slot index.
    id: u32,
    /// Virtual range retained for the slot lifetime.
    range: VirtualRange,
    /// Checked carrier and bitmap dimensions.
    geometry: PoolGeometry,
    /// Fixed mask for the carrier bits represented by each bitmap word.
    valid_masks: Box<[u64]>,
    /// Serialized mapping and cleanup state.
    mapping: Mutex<MappingState>,
    /// Published claimable incarnation.
    current: IncarnationCell,
}

impl BlockSlot {
    /// Reserves an inaccessible virtual range and allocates immutable masks.
    ///
    /// Returns [`BlockError::Allocation`] if mask allocation fails or
    /// [`BlockError::VirtualMemory`] if the address range cannot be reserved.
    pub(super) fn new(id: u32, geometry: PoolGeometry) -> Result<Self, BlockError> {
        let mut valid_masks = Vec::new();
        valid_masks.try_reserve_exact(geometry.bitmap_words())?;
        valid_masks.extend(
            (0..geometry.bitmap_words())
                .map(|word| geometry.valid_mask(word).expect("word belongs to geometry")),
        );

        let range = VirtualRange::reserve(
            geometry.block_size(),
            NonZeroUsize::new(geometry.page_size()).expect("geometry has a nonzero page size"),
        )?;
        Ok(Self {
            id,
            range,
            geometry,
            valid_masks: valid_masks.into_boxed_slice(),
            mapping: Mutex::new(MappingState::Reserved {
                reclaim_pending: false,
            }),
            current: IncarnationCell::new(),
        })
    }

    /// Returns the number of carriers in this slot.
    pub(super) fn carrier_count(&self) -> CarrierCount {
        self.geometry.carriers_per_block()
    }

    /// Prepares the range and makes one incarnation active.
    ///
    /// `prepared` must be serialized with every capacity transition. The count
    /// is incremented before `Active` becomes visible. Protection failure
    /// leaves the slot nonclaimable and does not change `prepared`.
    pub(super) fn prepare(&self, prepared: &mut CarrierCount) -> Result<(), BlockError> {
        let mut mapping = self.mapping.lock();
        let current = self.current.load();
        if matches!(*mapping, MappingState::Prepared) {
            return Err(BlockError::AlreadyPrepared);
        }

        let next_prepared = prepared
            .checked_add(self.carrier_count())
            .ok_or(BlockError::PreparedCapacityOverflow)?;
        let fresh = if current.as_ref().is_none() {
            Some(Arc::new(BlockIncarnation::try_new(
                self.geometry.bitmap_words(),
            )?))
        } else {
            None
        };

        if let Some(incarnation) = current.as_ref() {
            if !matches!(*mapping, MappingState::ProtectionPending)
                || incarnation.state.load(Ordering::Acquire) != DRAINING
            {
                invariant_violation("only protection recovery may retain a draining incarnation");
            }
        }

        if let Err(error) = self.range.prepare() {
            *mapping = MappingState::ProtectionPending;
            return Err(error.into());
        }

        *mapping = MappingState::Prepared;
        *prepared = next_prepared;
        if let Some(incarnation) = current.as_ref() {
            incarnation.state.store(ACTIVE, Ordering::SeqCst);
        } else if self.current.swap(fresh).is_some() {
            invariant_violation("preparation replaced a current incarnation");
        }
        Ok(())
    }

    /// Confirms that this block may be removed from prepared capacity.
    ///
    /// `prepared` must be serialized with every capacity transition. Success
    /// subtracts the complete block and returns cleanup authority. A live bit
    /// restores `Active` without changing `prepared`.
    pub(super) fn start_trim(
        slot: &Arc<Self>,
        prepared: &mut CarrierCount,
        floor: CarrierCount,
    ) -> Result<TrimCleanup, TrimBlocked> {
        let mapping = slot.mapping.lock();
        match *mapping {
            MappingState::Prepared => {}
            MappingState::Reserved { .. } => return Err(TrimBlocked::NotPrepared),
            MappingState::ProtectionPending => return Err(TrimBlocked::CleanupPending),
        }

        let current = slot.current.load();
        let Some(incarnation) = current.as_ref() else {
            invariant_violation("prepared mapping has no current incarnation");
        };
        match incarnation.state.load(Ordering::Acquire) {
            ACTIVE => {}
            DRAINING => return Err(TrimBlocked::CleanupPending),
            DEAD => invariant_violation("prepared mapping has a dead current incarnation"),
            _ => invariant_violation("prepared mapping has an invalid incarnation state"),
        }

        let Some(next_prepared) = prepared.checked_sub(slot.carrier_count()) else {
            invariant_violation("prepared capacity excludes an active block");
        };
        if next_prepared < floor {
            return Err(TrimBlocked::Required);
        }

        if incarnation
            .state
            .compare_exchange(ACTIVE, DRAINING, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(TrimBlocked::CleanupPending);
        }
        model_seq_cst_store_load();

        let busy = incarnation
            .in_use
            .iter()
            .zip(&slot.valid_masks)
            .any(|(word, valid)| word.load(Ordering::SeqCst) & valid != 0);
        if busy {
            incarnation.state.store(ACTIVE, Ordering::SeqCst);
            return Err(TrimBlocked::Busy);
        }

        *prepared = next_prepared;
        Ok(TrimCleanup {
            slot: Arc::clone(slot),
            incarnation: Arc::clone(incarnation),
            consumed: false,
        })
    }

    /// Retries pending whole-range protection or backing reclaim.
    ///
    /// A slot with no pending work returns success without changing state.
    /// Failure leaves the slot nonclaimable and identifies the required retry.
    pub(super) fn retry_cleanup(&self) -> Result<(), CleanupRetry> {
        let mut mapping = self.mapping.lock();
        match *mapping {
            MappingState::Prepared => {
                let current = self.current.load();
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("prepared mapping has no current incarnation");
                };
                match incarnation.state.load(Ordering::Acquire) {
                    ACTIVE | DRAINING => Ok(()),
                    DEAD => invariant_violation("prepared mapping has a dead current incarnation"),
                    _ => invariant_violation("prepared mapping has an invalid incarnation state"),
                }
            }
            MappingState::Reserved {
                reclaim_pending: false,
            } => {
                if self.current.load().as_ref().is_some() {
                    invariant_violation("inactive mapping has a current incarnation");
                }
                Ok(())
            }
            MappingState::Reserved {
                reclaim_pending: true,
            } => {
                if self.current.load().as_ref().is_some() {
                    invariant_violation("inactive reclaim has a current incarnation");
                }
                match self.range.discard() {
                    Ok(()) => {
                        *mapping = MappingState::Reserved {
                            reclaim_pending: false,
                        };
                        Ok(())
                    }
                    Err(error) => Err(CleanupRetry::ReclaimPending(error)),
                }
            }
            MappingState::ProtectionPending => {
                if let Some(incarnation) = self.current.load().as_ref() {
                    if incarnation.state.load(Ordering::Acquire) != DRAINING {
                        invariant_violation(
                            "protection-pending mapping has a non-draining incarnation",
                        );
                    }
                }
                if let Err(error) = self.range.deactivate() {
                    return Err(CleanupRetry::ProtectionPending(error));
                }
                *mapping = MappingState::Reserved {
                    reclaim_pending: false,
                };
                self.finish_inactive_cleanup(&mut mapping)
            }
        }
    }

    /// Retires a draining incarnation after the range becomes inaccessible.
    ///
    /// Discard failure still retires the incarnation and records pending
    /// reclaim in `mapping`.
    fn finish_inactive_cleanup(&self, mapping: &mut MappingState) -> Result<(), CleanupRetry> {
        let reclaim = self.range.discard();
        let reclaim_pending = reclaim.is_err();
        *mapping = MappingState::Reserved { reclaim_pending };

        if let Some(incarnation) = self.current.load().as_ref() {
            if incarnation.state.load(Ordering::Acquire) != DRAINING {
                invariant_violation("cleanup found a non-draining current incarnation");
            }
            incarnation.state.store(DEAD, Ordering::Release);
            let removed = self
                .current
                .swap(None)
                .unwrap_or_else(|| invariant_violation("cleanup lost its current incarnation"));
            if !Arc::ptr_eq(&removed, incarnation) {
                invariant_violation("cleanup removed a different incarnation");
            }
        }

        match reclaim {
            Ok(()) => Ok(()),
            Err(error) => Err(CleanupRetry::ReclaimPending(error)),
        }
    }

    /// Claims up to `count` carriers from the current active incarnation.
    ///
    /// Returns `None` when no incarnation is published, no bit is free, or
    /// trim wins the state gate. Zero returns [`BlockError::InvalidClaimCount`].
    pub(super) fn try_claim(
        slot: &Arc<Self>,
        count: CarrierCount,
    ) -> Result<Option<ProvisionalBits>, BlockError> {
        if count == CarrierCount::ZERO {
            return Err(BlockError::InvalidClaimCount);
        }
        let Some(mut attempt) = BlockClaimAttempt::begin(slot)? else {
            return Ok(None);
        };
        attempt.take(count.get());
        Ok(attempt.finish())
    }

    /// Returns a set of bits from a gate-passed owner.
    ///
    /// Identity or ownership mismatch is fail-stop and clears no bits.
    fn release_won(&self, incarnation: IncarnationIdentity, won: &[WonWord]) {
        if won.iter().all(|word| word.mask == 0) {
            return;
        }
        let current = self.current.load();
        let Some(current) = current.as_ref() else {
            invariant_violation("owned carriers have no current incarnation");
        };
        if BlockIncarnation::identity(current) != incarnation {
            invariant_violation("owned carriers name a different incarnation");
        }
        match current.state.load(Ordering::Acquire) {
            ACTIVE | DRAINING => {}
            DEAD => invariant_violation("owned carriers name a dead incarnation"),
            _ => invariant_violation("owned carriers name an invalid incarnation state"),
        }

        for word in won {
            clear_owned_bits(&current.in_use[word.word_index], word.mask);
        }
    }

    /// Returns one carrier identified by slot, index, and incarnation.
    fn release_one(&self, id: CarrierId) {
        if id.slot != self.id {
            invariant_violation("carrier returned to a different block slot");
        }
        let index = id.index as usize;
        self.release_won(
            id.incarnation,
            &[WonWord {
                word_index: index / u64::BITS as usize,
                mask: 1u64 << (index % u64::BITS as usize),
            }],
        );
    }

    /// Counts set valid bits in the current incarnation.
    #[cfg(test)]
    fn live_carriers(&self) -> usize {
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
}

/// Linear cleanup authority for a confirmed trim.
///
/// Dropping an unfinished token performs the same cleanup attempt as
/// [`TrimCleanup::finish`].
#[must_use = "confirmed trim cleanup must be completed or dropped"]
pub(super) struct TrimCleanup {
    /// Slot whose prepared capacity was removed.
    slot: Arc<BlockSlot>,
    /// Draining incarnation confirmed free by trim.
    incarnation: Arc<BlockIncarnation>,
    /// `true` after cleanup has been attempted.
    consumed: bool,
}

impl TrimCleanup {
    /// Returns the number of carriers already removed from prepared capacity.
    pub(super) fn carrier_count(&self) -> CarrierCount {
        self.slot.carrier_count()
    }

    /// Makes the confirmed block inaccessible and reclaims its backing.
    ///
    /// Protection failure leaves the draining incarnation published.
    /// Discard failure retires it and records pending reclaim.
    pub(super) fn finish(mut self) -> Result<(), CleanupRetry> {
        self.consumed = true;
        self.finish_inner()
    }

    /// Performs the cleanup attempt after marking the token consumed.
    fn finish_inner(&self) -> Result<(), CleanupRetry> {
        let mut mapping = self.slot.mapping.lock();
        if !matches!(*mapping, MappingState::Prepared) {
            invariant_violation("trim cleanup started from a non-prepared mapping");
        }

        let current = self.slot.current.load();
        let Some(incarnation) = current.as_ref() else {
            invariant_violation("trim cleanup lost its current incarnation");
        };
        if !Arc::ptr_eq(incarnation, &self.incarnation)
            || incarnation.state.load(Ordering::Acquire) != DRAINING
        {
            invariant_violation("trim cleanup no longer owns the draining incarnation");
        }

        if let Err(error) = self.slot.range.deactivate() {
            *mapping = MappingState::ProtectionPending;
            return Err(CleanupRetry::ProtectionPending(error));
        }

        *mapping = MappingState::Reserved {
            reclaim_pending: false,
        };
        self.slot.finish_inactive_cleanup(&mut mapping)
    }
}

impl Drop for TrimCleanup {
    fn drop(&mut self) {
        if !self.consumed {
            self.consumed = true;
            let _ = self.finish_inner();
        }
    }
}

/// Claim state retained through the `Active` gate.
struct BlockClaimAttempt {
    /// Slot whose bitmap is being claimed.
    slot: Arc<BlockSlot>,
    /// Incarnation protected before the first bitmap mutation.
    incarnation: IncarnationGuard,
    /// Bits won by this attempt.
    won: Vec<WonWord>,
}

impl BlockClaimAttempt {
    /// Starts a claim against the current incarnation.
    ///
    /// Returns `None` when the slot has no published incarnation.
    fn begin(slot: &Arc<BlockSlot>) -> Result<Option<Self>, TryReserveError> {
        let incarnation = slot.current.load();
        if incarnation.as_ref().is_none() {
            return Ok(None);
        }

        let mut won = Vec::new();
        won.try_reserve_exact(slot.geometry.bitmap_words())?;
        Ok(Some(Self {
            slot: Arc::clone(slot),
            incarnation,
            won,
        }))
    }

    /// Claims up to `count` free bits and returns the number won.
    ///
    /// A claim may lose candidate bits after its relaxed load. For example:
    ///
    /// ```text
    /// observed  = 0b0010
    /// candidate = 0b0101
    /// previous  = 0b0110  // another claim took bit 2
    /// won       = 0b0001  // candidate & !previous
    /// ```
    fn take(&mut self, mut count: usize) -> usize {
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
                    word_index,
                    mask: won,
                });
                count -= won_count;
                taken += won_count;
            }
        }
        taken
    }

    /// Passes the state gate or rolls back every bit won by this attempt.
    fn finish(mut self) -> Option<ProvisionalBits> {
        if self.won.is_empty() {
            return None;
        }
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        model_seq_cst_store_load();
        match incarnation.state.load(Ordering::SeqCst) {
            ACTIVE => {}
            DRAINING | DEAD => {
                self.rollback_original();
                return None;
            }
            _ => invariant_violation("claim observed an invalid incarnation state"),
        }

        let incarnation = BlockIncarnation::identity(incarnation);
        let won = std::mem::take(&mut self.won);
        Some(ProvisionalBits {
            slot: Arc::clone(&self.slot),
            incarnation,
            won,
        })
    }

    /// Clears remaining bits through the originally protected incarnation.
    fn rollback_original(&mut self) {
        let Some(incarnation) = self.incarnation.as_ref() else {
            return;
        };
        for word in self.won.drain(..) {
            clear_owned_bits(&incarnation.in_use[word.word_index], word.mask);
        }
    }
}

impl Drop for BlockClaimAttempt {
    fn drop(&mut self) {
        self.rollback_original();
    }
}

/// Linear ownership of gate-passed bits not yet converted to carriers.
pub(super) struct ProvisionalBits {
    /// Slot containing the owned bits.
    slot: Arc<BlockSlot>,
    /// Incarnation containing the owned bits.
    incarnation: IncarnationIdentity,
    /// Bits returned if this owner is dropped.
    won: Vec<WonWord>,
}

impl ProvisionalBits {
    /// Returns the number of provisionally owned carriers.
    pub(super) fn len(&self) -> CarrierCount {
        CarrierCount::new(
            self.won
                .iter()
                .map(|word| word.mask.count_ones() as usize)
                .sum(),
        )
    }

    /// Converts every provisional bit into one carrier owner.
    ///
    /// Allocation failure returns every provisional bit on drop.
    /// A carrier index is `word_index * 64 + bit_index`. Bitmap word 1 bits 1
    /// and 3 therefore become carrier indices 65 and 67.
    pub(super) fn into_carriers(mut self) -> Result<Vec<CarrierAllocation>, BlockError> {
        let mut carriers = Vec::new();
        carriers.try_reserve_exact(self.len().get())?;

        for word in &mut self.won {
            while word.mask != 0 {
                let bit = word.mask.trailing_zeros() as usize;
                let mask = 1u64 << bit;
                let index = word.word_index * u64::BITS as usize + bit;
                let index = u32::try_from(index)
                    .unwrap_or_else(|_| invariant_violation("carrier index exceeds geometry"));
                let carrier = CarrierAllocation {
                    slot: Arc::clone(&self.slot),
                    id: CarrierId {
                        slot: self.slot.id,
                        index,
                        incarnation: self.incarnation,
                    },
                };

                // Move ownership before push so unwinding leaves each bit with
                // exactly one drop path.
                word.mask &= !mask;
                carriers.push(carrier);
            }
        }
        Ok(carriers)
    }
}

impl Drop for ProvisionalBits {
    fn drop(&mut self) {
        self.slot.release_won(self.incarnation, &self.won);
    }
}

/// Linear physical ownership of one carrier.
pub(super) struct CarrierAllocation {
    /// Slot that retains the carrier address.
    slot: Arc<BlockSlot>,
    /// Stable carrier and incarnation identity.
    id: CarrierId,
}

impl CarrierAllocation {
    /// Returns the carrier capacity in bytes.
    pub(super) fn capacity(&self) -> usize {
        self.slot.geometry.carrier_size()
    }

    /// Returns the first byte of this carrier.
    ///
    /// The range remains prepared and exclusively owned while `self` lives.
    /// Dereferencing the pointer must still obey initialization rules.
    pub(super) fn ptr(&self) -> NonNull<MaybeUninit<u8>> {
        let index = self.id.index as usize;
        let offset = self
            .slot
            .geometry
            .carrier_offset(index)
            .unwrap_or_else(|| invariant_violation("carrier index is outside block geometry"));
        // SAFETY: this allocation owns the carrier bit after passing the
        // `Active` gate, and its Arc retains the slot for the pointer lifetime.
        unsafe {
            self.slot
                .range
                .ptr_for_range(offset, self.capacity())
                .unwrap_or_else(|| invariant_violation("carrier range is outside its reservation"))
        }
    }
}

impl Drop for CarrierAllocation {
    fn drop(&mut self) {
        self.slot.release_one(self.id);
    }
}

/// Clears `mask` after verifying that every bit is owned.
fn clear_owned_bits(word: &AtomicU64, mask: u64) {
    if mask == 0 {
        return;
    }
    let mut observed = word.load(Ordering::Acquire);
    loop {
        if observed & mask != mask {
            invariant_violation("bitmap release does not own every cleared bit");
        }
        match word.compare_exchange_weak(
            observed,
            observed & !mask,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(actual) => observed = actual,
        }
    }
}

/// Selects at most `count` least-significant bits from `available`.
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

/// Models the global store-load order supplied by production `SeqCst`.
///
/// Loom 0.7 treats `SeqCst` accesses as `AcqRel`, but models a `SeqCst` fence.
#[inline]
fn model_seq_cst_store_load() {
    #[cfg(all(test, s3_tm_loom))]
    loom::sync::atomic::fence(Ordering::SeqCst);
}

/// Stops execution after an ownership or lifecycle invariant fails.
#[cold]
fn invariant_violation(message: &'static str) -> ! {
    #[cfg(test)]
    panic!("buffer-pool ownership invariant violated: {message}");

    #[cfg(not(test))]
    {
        let _ = message;
        std::process::abort()
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::super::virtual_memory::{page_size, VirtualMemoryOperation};
    use super::*;

    fn geometry(carriers: usize) -> PoolGeometry {
        let page_size = page_size().unwrap().get();
        PoolGeometry::new(page_size, page_size, carriers).unwrap()
    }

    fn prepared_slot(carriers: usize) -> (Arc<BlockSlot>, CarrierCount) {
        let slot = Arc::new(BlockSlot::new(7, geometry(carriers)).unwrap());
        let mut prepared = CarrierCount::ZERO;
        slot.prepare(&mut prepared).unwrap();
        (slot, prepared)
    }

    fn current_identity(slot: &BlockSlot) -> Option<IncarnationIdentity> {
        slot.current.load().as_ref().map(BlockIncarnation::identity)
    }

    fn assert_mapping(slot: &BlockSlot, expected: MappingState) {
        assert_eq!(*slot.mapping.lock(), expected);
    }

    #[test]
    fn preparation_publishes_claimable_capacity() {
        let (slot, prepared) = prepared_slot(3);

        assert_eq!(prepared, CarrierCount::new(3));
        let claimed = BlockSlot::try_claim(&slot, CarrierCount::new(3))
            .unwrap()
            .unwrap();
        assert_eq!(claimed.len(), CarrierCount::new(3));
        assert_eq!(slot.live_carriers(), 3);

        drop(claimed);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn preparation_rejects_a_current_incarnation() {
        let (slot, mut prepared) = prepared_slot(1);

        assert!(matches!(
            slot.prepare(&mut prepared),
            Err(BlockError::AlreadyPrepared)
        ));
        assert_eq!(prepared, CarrierCount::new(1));
    }

    #[test]
    fn failed_prepare_stays_nonclaimable_until_recovery() {
        let slot = Arc::new(BlockSlot::new(7, geometry(1)).unwrap());
        let mut prepared = CarrierCount::ZERO;
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Prepare);

        let error = slot.prepare(&mut prepared).unwrap_err();

        assert!(matches!(
            error,
            BlockError::VirtualMemory(ref error)
                if error.operation() == VirtualMemoryOperation::Prepare
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(&slot, MappingState::ProtectionPending);
        assert!(current_identity(&slot).is_none());
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());

        slot.prepare(&mut prepared).unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        assert_mapping(&slot, MappingState::Prepared);
        assert!(current_identity(&slot).is_some());
    }

    #[test]
    fn cleanup_retry_restores_reserved_state_after_initial_prepare_failure() {
        let slot = Arc::new(BlockSlot::new(7, geometry(1)).unwrap());
        let mut prepared = CarrierCount::ZERO;
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Prepare);
        assert!(slot.prepare(&mut prepared).is_err());
        assert_mapping(&slot, MappingState::ProtectionPending);
        assert!(current_identity(&slot).is_none());

        slot.retry_cleanup().unwrap();

        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
        assert!(current_identity(&slot).is_none());

        slot.prepare(&mut prepared).unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        let carrier = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .expect("cleaned slot is claimable");
        drop(carrier);
    }

    #[test]
    fn carrier_pointer_is_derived_from_live_ownership() {
        let (slot, _) = prepared_slot(1);
        let mut carriers = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        let carrier = carriers.pop().unwrap();

        assert_eq!(carrier.capacity(), geometry(1).carrier_size());
        // SAFETY: the carrier allocation owns this complete writable byte.
        unsafe { carrier.ptr().as_ptr().write(MaybeUninit::new(0x5a)) };
        // SAFETY: the preceding write initialized the byte.
        assert_eq!(unsafe { carrier.ptr().as_ptr().read().assume_init() }, 0x5a);

        drop(carrier);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn claim_never_wins_padding_bits() {
        let (slot, _) = prepared_slot(65);
        let carriers = BlockSlot::try_claim(&slot, CarrierCount::new(65))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();

        assert_eq!(carriers.len(), 65);
        assert_eq!(carriers.last().unwrap().id.index, 64);
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());

        drop(carriers);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn dropping_provisional_bits_returns_every_word() {
        let (slot, _) = prepared_slot(65);
        let provisional = BlockSlot::try_claim(&slot, CarrierCount::new(65))
            .unwrap()
            .unwrap();

        assert_eq!(slot.live_carriers(), 65);
        drop(provisional);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn concurrent_attempts_clear_only_their_won_bits() {
        let (slot, _) = prepared_slot(3);
        let mut first = BlockClaimAttempt::begin(&slot).unwrap().unwrap();
        let mut second = BlockClaimAttempt::begin(&slot).unwrap().unwrap();

        assert_eq!(first.take(1), 1);
        assert_eq!(second.take(2), 2);
        let first = first.finish().unwrap();
        let second = second.finish().unwrap();
        assert_eq!(slot.live_carriers(), 3);

        drop(first);
        assert_eq!(slot.live_carriers(), 2);
        drop(second);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn provisional_word_bits_map_to_carrier_indices() {
        let (slot, _) = prepared_slot(68);
        let incarnation = {
            let current = slot.current.load();
            let incarnation = current.as_ref().expect("slot is prepared");
            incarnation.in_use[1].store(0b1010, Ordering::Release);
            BlockIncarnation::identity(incarnation)
        };
        let provisional = ProvisionalBits {
            slot: Arc::clone(&slot),
            incarnation,
            won: vec![WonWord {
                word_index: 1,
                mask: 0b1010,
            }],
        };

        let carriers = provisional.into_carriers().unwrap();

        assert_eq!(
            carriers
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![65, 67]
        );
        drop(carriers);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn dropping_unfinished_attempt_returns_original_bits() {
        let (slot, _) = prepared_slot(2);
        let mut attempt = BlockClaimAttempt::begin(&slot).unwrap().unwrap();

        assert_eq!(attempt.take(2), 2);
        assert_eq!(slot.live_carriers(), 2);
        drop(attempt);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn trim_preserves_floor_and_abandons_for_live_ownership() {
        let (slot, mut prepared) = prepared_slot(2);

        assert!(matches!(
            BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::new(1)),
            Err(TrimBlocked::Required)
        ));
        assert_eq!(prepared, CarrierCount::new(2));

        let carriers = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        assert!(matches!(
            BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO),
            Err(TrimBlocked::Busy)
        ));
        assert_eq!(prepared, CarrierCount::new(2));
        assert_mapping(&slot, MappingState::Prepared);

        drop(carriers);
    }

    #[test]
    fn successful_trim_revives_with_fresh_metadata_at_the_same_address() {
        let (slot, mut prepared) = prepared_slot(1);
        let original_incarnation =
            Arc::clone(slot.current.load().as_ref().expect("slot is prepared"));
        let original_address = slot.range.base_address();

        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();
        assert_eq!(cleanup.carrier_count(), CarrierCount::new(1));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());
        cleanup.finish().unwrap();

        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
        assert!(current_identity(&slot).is_none());

        slot.prepare(&mut prepared).unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        assert_eq!(slot.range.base_address(), original_address);
        let revived = slot.current.load();
        assert!(!Arc::ptr_eq(
            revived.as_ref().expect("slot was revived"),
            &original_incarnation
        ));
    }

    #[test]
    fn stale_failed_gate_rolls_back_only_retired_metadata() {
        let (slot, mut prepared) = prepared_slot(1);
        let mut stale = BlockClaimAttempt::begin(&slot).unwrap().unwrap();

        BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO)
            .unwrap()
            .finish()
            .unwrap();
        slot.prepare(&mut prepared).unwrap();
        let current = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();

        assert_eq!(stale.take(1), 1);
        assert!(stale.finish().is_none());
        assert_eq!(slot.live_carriers(), 1);

        drop(current);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn failed_deactivation_can_restore_the_draining_incarnation() {
        let (slot, mut prepared) = prepared_slot(1);
        let original_identity = current_identity(&slot).unwrap();
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Deactivate);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();

        let error = cleanup.finish().unwrap_err();

        assert!(matches!(
            error,
            CleanupRetry::ProtectionPending(ref error)
                if error.operation() == VirtualMemoryOperation::Deactivate
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(&slot, MappingState::ProtectionPending);
        assert_eq!(current_identity(&slot), Some(original_identity));
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());

        slot.prepare(&mut prepared).unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        assert_mapping(&slot, MappingState::Prepared);
        assert_eq!(current_identity(&slot), Some(original_identity));
        let carrier = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap();
        drop(carrier);
    }

    #[test]
    fn protection_retry_finishes_inactive_cleanup() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Deactivate);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();
        assert!(matches!(
            cleanup.finish(),
            Err(CleanupRetry::ProtectionPending(_))
        ));

        slot.retry_cleanup().unwrap();

        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
        assert!(current_identity(&slot).is_none());
    }

    #[test]
    fn failed_discard_retires_the_incarnation_and_retries_inactive() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Discard);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();

        let error = cleanup.finish().unwrap_err();

        assert!(matches!(
            error,
            CleanupRetry::ReclaimPending(ref error)
                if error.operation() == VirtualMemoryOperation::Discard
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: true,
            },
        );
        assert!(current_identity(&slot).is_none());

        slot.retry_cleanup().unwrap();
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
    }

    #[test]
    fn revival_clears_pending_reclaim_before_publication() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Discard);
        BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO)
            .unwrap()
            .finish()
            .unwrap_err();

        slot.prepare(&mut prepared).unwrap();
        slot.retry_cleanup().unwrap();

        assert_eq!(prepared, CarrierCount::new(1));
        assert_mapping(&slot, MappingState::Prepared);
        let carrier = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap();
        drop(carrier);
    }

    #[test]
    fn dropped_cleanup_completes_the_confirmed_trim() {
        let (slot, mut prepared) = prepared_slot(1);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();

        drop(cleanup);

        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
        assert!(current_identity(&slot).is_none());
    }

    #[test]
    fn dropped_cleanup_leaves_failed_protection_nonclaimable() {
        let (slot, mut prepared) = prepared_slot(1);
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Deactivate);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();

        drop(cleanup);

        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(&slot, MappingState::ProtectionPending);
        assert!(current_identity(&slot).is_some());
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());
    }

    #[test]
    fn identity_mismatch_stops_before_clearing() {
        let (slot, _) = prepared_slot(1);
        let mut carriers = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        let carrier = carriers.pop().unwrap();
        let wrong = CarrierId {
            incarnation: IncarnationIdentity(
                NonZeroUsize::new(carrier.id.incarnation.0.get().wrapping_add(1))
                    .unwrap_or(NonZeroUsize::MIN),
            ),
            ..carrier.id
        };

        let result = catch_unwind(AssertUnwindSafe(|| slot.release_one(wrong)));

        assert!(result.is_err());
        assert_eq!(slot.live_carriers(), 1);
        drop(carrier);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn zero_claim_is_rejected_without_mutation() {
        let (slot, _) = prepared_slot(1);

        assert!(matches!(
            BlockSlot::try_claim(&slot, CarrierCount::ZERO),
            Err(BlockError::InvalidClaimCount)
        ));
        assert_eq!(slot.live_carriers(), 0);
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn ownership_spine_is_send_and_sync() {
        assert_send_sync::<BlockSlot>();
        assert_send_sync::<CarrierAllocation>();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::virtual_memory::VirtualMemoryOperation;
    use super::*;
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};
    use crate::runtime::sync::thread;

    fn prepared_slot() -> (Arc<BlockSlot>, CarrierCount) {
        let geometry = PoolGeometry::new(4096, 4096, 1).unwrap();
        let slot = Arc::new(BlockSlot::new(0, geometry).unwrap());
        let mut prepared = CarrierCount::ZERO;
        slot.prepare(&mut prepared).unwrap();
        (slot, prepared)
    }

    #[test]
    fn stale_claim_cannot_clear_a_revived_claim() {
        loom::model(|| {
            let (slot, initial_prepared) = prepared_slot();
            let loaded = Arc::new(AtomicBool::new(false));
            let revived = Arc::new(AtomicBool::new(false));

            let stale_slot = Arc::clone(&slot);
            let stale_loaded = Arc::clone(&loaded);
            let stale_revived = Arc::clone(&revived);
            let stale = thread::spawn(move || {
                let mut attempt = BlockClaimAttempt::begin(&stale_slot).unwrap().unwrap();
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
                let mut prepared = initial_prepared;
                BlockSlot::start_trim(&current_slot, &mut prepared, CarrierCount::ZERO)
                    .unwrap()
                    .finish()
                    .unwrap();
                current_slot.prepare(&mut prepared).unwrap();
                let carrier = BlockSlot::try_claim(&current_slot, CarrierCount::new(1))
                    .unwrap()
                    .unwrap()
                    .into_carriers()
                    .unwrap()
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
    fn claim_and_protection_recovery_preserve_single_ownership() {
        loom::model(|| {
            let (slot, mut prepared) = prepared_slot();
            let original = Arc::clone(slot.current.load().as_ref().expect("slot is prepared"));

            let mut attempt = BlockClaimAttempt::begin(&slot).unwrap().unwrap();
            slot.range
                .inject_failure_once(VirtualMemoryOperation::Deactivate);
            let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();
            assert!(matches!(
                cleanup.finish(),
                Err(CleanupRetry::ProtectionPending(_))
            ));
            assert_eq!(attempt.take(1), 1);

            let stale = thread::spawn(move || attempt.finish());
            let recovery_slot = Arc::clone(&slot);
            let recovery = thread::spawn(move || {
                recovery_slot.prepare(&mut prepared).unwrap();
                let current = BlockSlot::try_claim(&recovery_slot, CarrierCount::new(1)).unwrap();
                (prepared, current)
            });

            let stale = stale.join().unwrap();
            let (prepared, current) = recovery.join().unwrap();
            let owned = stale
                .as_ref()
                .map_or(0, |claim| claim.len().get())
                .checked_add(current.as_ref().map_or(0, |claim| claim.len().get()))
                .unwrap();

            assert_eq!(prepared, CarrierCount::new(1));
            assert!(matches!(*slot.mapping.lock(), MappingState::Prepared));
            let restored = slot.current.load();
            assert!(Arc::ptr_eq(
                restored.as_ref().expect("incarnation was restored"),
                &original
            ));
            assert!(owned <= 1);
            assert_eq!(slot.live_carriers(), owned);

            drop(stale);
            drop(current);
            assert_eq!(slot.live_carriers(), 0);
        });
    }

    #[test]
    fn claim_and_trim_cannot_both_pass_the_gate() {
        loom::model(|| {
            let (slot, initial_prepared) = prepared_slot();
            let prepared = Arc::new(Mutex::new(initial_prepared));

            let claim_slot = Arc::clone(&slot);
            let claim = thread::spawn(move || {
                BlockSlot::try_claim(&claim_slot, CarrierCount::new(1))
                    .unwrap()
                    .map(ProvisionalBits::into_carriers)
                    .transpose()
                    .unwrap()
            });

            let trim_slot = Arc::clone(&slot);
            let trim_prepared = Arc::clone(&prepared);
            let trim = thread::spawn(move || {
                let start = {
                    let mut prepared = trim_prepared.lock();
                    BlockSlot::start_trim(&trim_slot, &mut prepared, CarrierCount::ZERO)
                };
                match start {
                    Ok(cleanup) => {
                        cleanup.finish().unwrap();
                        true
                    }
                    Err(TrimBlocked::Busy) => false,
                    Err(other) => panic!("unexpected trim result: {other:?}"),
                }
            });

            let carriers = claim.join().unwrap();
            let trimmed = trim.join().unwrap();
            if let Some(carriers) = carriers {
                assert!(!trimmed);
                drop(carriers);
            }

            let active = slot
                .current
                .load()
                .as_ref()
                .map(|incarnation| incarnation.state.load(Ordering::Acquire) == ACTIVE)
                .unwrap_or(false);
            if active {
                let mut prepared = prepared.lock();
                BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO)
                    .unwrap()
                    .finish()
                    .unwrap();
            }

            assert!(slot.current.load().as_ref().is_none());
            assert_eq!(
                *slot.mapping.lock(),
                MappingState::Reserved {
                    reclaim_pending: false
                }
            );
        });
    }
}
