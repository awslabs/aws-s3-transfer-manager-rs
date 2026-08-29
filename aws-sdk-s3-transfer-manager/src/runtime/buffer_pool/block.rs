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
use super::{invariant_violation, CarrierCount};
use crate::runtime::sync::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

#[cfg(test)]
use {super::virtual_memory::VirtualMemoryOperation, std::ops::Range};

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
    /// A claim exceeds one block's carrier capacity.
    ClaimExceedsBlock {
        /// Requested carriers.
        requested: usize,
        /// Carriers in the block.
        capacity: usize,
    },
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VirtualMemory(error) => error.fmt(f),
            Self::Allocation(error) => write!(f, "block metadata allocation failed: {error}"),
            Self::AlreadyPrepared => f.write_str("block already has a prepared incarnation"),
            Self::PreparedCapacityOverflow => f.write_str("prepared carrier count would overflow"),
            Self::InvalidClaimCount => f.write_str("a carrier claim must be nonzero"),
            Self::ClaimExceedsBlock {
                requested,
                capacity,
            } => write!(
                f,
                "carrier claim {requested} exceeds block capacity {capacity}"
            ),
        }
    }
}

impl std::error::Error for BlockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::VirtualMemory(error) => Some(error),
            Self::Allocation(error) => Some(error),
            Self::AlreadyPrepared
            | Self::PreparedCapacityOverflow
            | Self::InvalidClaimCount
            | Self::ClaimExceedsBlock { .. } => None,
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
    FloorViolation,
    /// A carrier claim won the claim-trim gate.
    Busy,
    /// A prior trim or failed deactivation still owns the slot.
    CleanupPending,
}

/// Failed cleanup work that remains safe to retry while the slot is inactive.
#[derive(Debug)]
pub(super) enum CleanupRetry {
    /// Whole-range deactivation failed and remains safe to retry.
    DeactivationPending(VirtualMemoryError),
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
    /// Mapping preparation failed before an incarnation was published.
    ActivationRecoveryPending,
    /// Mapping deactivation failed while a draining incarnation remained.
    DeactivationRecoveryPending,
}

/// Claim-trim gate state for one block incarnation.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncarnationState {
    /// Carrier claims may proceed.
    Active = 0,
    /// Trim is confirming that no carrier is owned.
    Draining = 1,
    /// The incarnation's mapping is inaccessible.
    Dead = 2,
}

/// Atomic storage for [`IncarnationState`].
struct AtomicIncarnationState(AtomicU8);

impl AtomicIncarnationState {
    /// Creates an atomic state with `value`.
    fn new(value: IncarnationState) -> Self {
        Self(AtomicU8::new(value as u8))
    }

    /// Loads and validates the current state.
    fn load(&self, order: Ordering) -> IncarnationState {
        Self::decode(self.0.load(order))
    }

    /// Starts the claim-trim gate transition.
    fn try_start_trim(&self) -> Result<(), IncarnationState> {
        self.0
            .compare_exchange(
                IncarnationState::Active as u8,
                IncarnationState::Draining as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(Self::decode)
    }

    /// Restores a draining incarnation after trim is abandoned or reversed.
    fn restore_active(&self) {
        if self
            .0
            .compare_exchange(
                IncarnationState::Draining as u8,
                IncarnationState::Active as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            invariant_violation("only a draining incarnation may return to active");
        }
    }

    /// Retires a draining incarnation after its mapping becomes inaccessible.
    fn retire(&self) {
        if self
            .0
            .compare_exchange(
                IncarnationState::Draining as u8,
                IncarnationState::Dead as u8,
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_err()
        {
            invariant_violation("only a draining incarnation may retire");
        }
    }

    /// Converts a stored representation into a state.
    fn decode(value: u8) -> IncarnationState {
        match value {
            value if value == IncarnationState::Active as u8 => IncarnationState::Active,
            value if value == IncarnationState::Draining as u8 => IncarnationState::Draining,
            value if value == IncarnationState::Dead as u8 => IncarnationState::Dead,
            _ => invariant_violation("incarnation contains an invalid state"),
        }
    }
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

/// Stable physical location of one carrier within an arena.
///
/// The location orders carriers for run construction. It does not identify the
/// block incarnation that owns a live carrier bit.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CarrierLocation {
    /// Pool-local block-slot index.
    slot_id: u32,
    /// Carrier index within the slot.
    carrier_index: u32,
}

impl CarrierLocation {
    /// Creates one arena location.
    pub(super) const fn new(slot_id: u32, carrier_index: u32) -> Self {
        Self {
            slot_id,
            carrier_index,
        }
    }

    /// Returns the pool-local block-slot index.
    pub(super) const fn slot_id(self) -> u32 {
        self.slot_id
    }

    /// Returns the carrier index within the slot.
    pub(super) const fn carrier_index(self) -> u32 {
        self.carrier_index
    }
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
    state: AtomicIncarnationState,
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
            state: AtomicIncarnationState::new(IncarnationState::Active),
            in_use: in_use.into_boxed_slice(),
        })
    }

    /// Allocates an active incarnation with `count` low carriers owned.
    fn try_new_preclaimed(
        geometry: PoolGeometry,
        count: CarrierCount,
    ) -> Result<(Self, Vec<WonWord>), TryReserveError> {
        let incarnation = Self::try_new(geometry.bitmap_words())?;
        let mut won = Vec::new();
        won.try_reserve_exact(geometry.bitmap_words())?;

        let mut remaining = count.get();
        for (word_index, word) in incarnation.in_use.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let valid = bitmap_word_mask(geometry, word_index);
            let mask = take_lowest(valid, remaining);
            let taken = mask.count_ones() as usize;
            word.store(mask, Ordering::Relaxed);
            won.push(WonWord { word_index, mask });
            remaining -= taken;
        }
        if remaining != 0 {
            invariant_violation("preclaim exceeds valid block geometry");
        }
        Ok((incarnation, won))
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
    //! Lock-free production publication for block incarnations.

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
    //! Loom-instrumented publication with owned incarnation guards.

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
///
/// Mapping state and the current incarnation remain separately synchronized
/// so carrier claims never take `mapping`. Their legal combinations are:
///
/// | Mapping state                 | Current incarnation                    |
/// |-------------------------------|----------------------------------------|
/// | `Reserved`                    | none                                   |
/// | `Prepared`                    | `Active` or trim-owned `Draining`      |
/// | `ActivationRecoveryPending`   | none                                   |
/// | `DeactivationRecoveryPending` | `Draining`                             |
pub(super) struct BlockSlot {
    /// Stable slot index.
    id: u32,
    /// Virtual range retained for the slot lifetime.
    range: VirtualRange,
    /// Checked carrier and bitmap dimensions.
    geometry: PoolGeometry,
    /// Serialized mapping and cleanup state.
    mapping: Mutex<MappingState>,
    /// Published claimable incarnation.
    current: IncarnationCell,
}

impl BlockSlot {
    /// Reserves an inaccessible virtual range.
    ///
    /// Returns [`BlockError::VirtualMemory`] if the address range cannot be
    /// reserved.
    pub(super) fn new(id: u32, geometry: PoolGeometry) -> Result<Self, BlockError> {
        let range = VirtualRange::reserve(
            geometry.block_size(),
            NonZeroUsize::new(geometry.page_size()).expect("geometry has a nonzero page size"),
        )?;
        Ok(Self {
            id,
            range,
            geometry,
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

    /// Returns this slot's stable identifier.
    pub(super) fn id(&self) -> u32 {
        self.id
    }

    /// Returns the reservation base for integer comparison.
    pub(super) fn base_address(&self) -> usize {
        self.range.base_address()
    }

    /// Returns the stable virtual reservation length.
    pub(super) fn reserved_len(&self) -> usize {
        self.range.len()
    }

    /// Derives a checked immutable pointer from this slot's provenance root.
    ///
    /// # Safety
    ///
    /// The caller must retain initialized immutable ownership over the
    /// complete subrange. That ownership must keep its carrier bits live so
    /// trim cannot deactivate the slot while the pointer is used.
    pub(super) unsafe fn ptr_for_immutable_range(
        &self,
        offset: usize,
        len: usize,
    ) -> Option<NonNull<u8>> {
        // SAFETY: the caller supplies the initialized owner and deactivation
        // exclusion required by `VirtualRange::ptr_for_range`.
        unsafe {
            self.range
                .ptr_for_range(offset, len)
                .map(NonNull::cast::<u8>)
        }
    }

    /// Debug-checks that every carrier intersecting `offset..offset + len`
    /// remains live.
    ///
    /// This detects violations of the immutable-owner contract. A set bit
    /// alone does not prove that a particular owner controls that bit.
    #[cfg(debug_assertions)]
    pub(super) fn debug_assert_immutable_range_live(&self, offset: usize, len: usize) {
        let end = offset.checked_add(len);
        debug_assert!(
            len != 0 && end.is_some_and(|end| end <= self.geometry.block_size()),
            "immutable range is outside its block slot"
        );
        let Some(end) = end.filter(|end| len != 0 && *end <= self.geometry.block_size()) else {
            return;
        };
        let current = self.current.load();
        let Some(incarnation) = current.as_ref() else {
            debug_assert!(false, "immutable range has no current incarnation");
            return;
        };
        let first = offset / self.geometry.carrier_size();
        let last = (end - 1) / self.geometry.carrier_size();
        for index in first..=last {
            let word_index = index / u64::BITS as usize;
            let mask = 1u64 << (index % u64::BITS as usize);
            let live = incarnation
                .in_use
                .get(word_index)
                .is_some_and(|word| word.load(Ordering::Acquire) & mask != 0);
            debug_assert!(
                live,
                "immutable range includes a carrier without a live bit"
            );
        }
    }

    /// Returns the reserved half-open address range.
    ///
    /// The integer range grants no access to the backing memory. `None`
    /// reports address arithmetic overflow.
    #[cfg(test)]
    pub(super) fn address_range(&self) -> Option<Range<usize>> {
        let start = self.base_address();
        start.checked_add(self.reserved_len()).map(|end| start..end)
    }

    /// Returns the number of ownership bitmap words.
    pub(super) fn bitmap_words(&self) -> usize {
        self.geometry.bitmap_words()
    }

    /// Prepares the range and makes one incarnation active.
    ///
    /// The caller holds the admission lock that protects the pool-wide
    /// `prepared` count and admission floor. The count is incremented before
    /// `Active` becomes visible. Protection failure leaves the slot
    /// nonclaimable and does not change `prepared`.
    pub(super) fn prepare(&self, prepared: &mut CarrierCount) -> Result<(), BlockError> {
        let mut mapping = self.mapping.lock();
        let current = self.current.load();
        let create_fresh = match *mapping {
            MappingState::Prepared => {
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("prepared mapping has no current incarnation");
                };
                if incarnation.state.load(Ordering::Acquire) == IncarnationState::Dead {
                    invariant_violation("prepared mapping has a dead current incarnation");
                }
                return Err(BlockError::AlreadyPrepared);
            }
            MappingState::Reserved { .. } | MappingState::ActivationRecoveryPending => {
                if current.as_ref().is_some() {
                    invariant_violation("inactive mapping retains a current incarnation");
                }
                true
            }
            MappingState::DeactivationRecoveryPending => {
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("deactivation recovery lost its current incarnation");
                };
                if incarnation.state.load(Ordering::Acquire) != IncarnationState::Draining {
                    invariant_violation(
                        "deactivation recovery retained a non-draining incarnation",
                    );
                }
                false
            }
        };

        let next_prepared = prepared
            .checked_add(self.carrier_count())
            .ok_or(BlockError::PreparedCapacityOverflow)?;
        let fresh = if create_fresh {
            Some(Arc::new(BlockIncarnation::try_new(
                self.geometry.bitmap_words(),
            )?))
        } else {
            None
        };

        if let Err(error) = self.range.prepare() {
            *mapping = if current.as_ref().is_some() {
                MappingState::DeactivationRecoveryPending
            } else {
                MappingState::ActivationRecoveryPending
            };
            return Err(error.into());
        }

        *mapping = MappingState::Prepared;
        *prepared = next_prepared;
        if let Some(incarnation) = current.as_ref() {
            incarnation.state.restore_active();
        } else if self.current.swap(fresh).is_some() {
            invariant_violation("preparation replaced a current incarnation");
        }
        Ok(())
    }

    /// Prepares a reusable inactive slot with `count` carriers already owned.
    ///
    /// Serialized arena fallback calls this after exhausting active capacity.
    /// The caller also serializes the pool-wide `prepared` count and admission
    /// floor.
    ///
    /// `None` reports an active slot or cleanup-pending incarnation. Success
    /// increments `prepared` before publishing `Active` and returns ownership
    /// of exactly `count` carrier bits. Failure publishes no new incarnation
    /// and leaves `prepared` unchanged. The private incarnation owns its bits
    /// before publication, so no claim-trim gate is required.
    pub(super) fn prepare_and_claim_if_inactive(
        slot: &Arc<Self>,
        prepared: &mut CarrierCount,
        count: CarrierCount,
    ) -> Result<Option<ProvisionalBits>, BlockError> {
        if count == CarrierCount::ZERO {
            return Err(BlockError::InvalidClaimCount);
        }
        if count > slot.carrier_count() {
            return Err(BlockError::ClaimExceedsBlock {
                requested: count.get(),
                capacity: slot.carrier_count().get(),
            });
        }

        let mut mapping = slot.mapping.lock();
        let current = slot.current.load();
        match *mapping {
            MappingState::Prepared => {
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("prepared mapping has no current incarnation");
                };
                if incarnation.state.load(Ordering::Acquire) == IncarnationState::Dead {
                    invariant_violation("prepared mapping has a dead current incarnation");
                }
                return Ok(None);
            }
            MappingState::DeactivationRecoveryPending => {
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("deactivation recovery lost its current incarnation");
                };
                if incarnation.state.load(Ordering::Acquire) != IncarnationState::Draining {
                    invariant_violation(
                        "deactivation recovery retained a non-draining incarnation",
                    );
                }
                return Ok(None);
            }
            MappingState::Reserved { .. } | MappingState::ActivationRecoveryPending => {
                if current.as_ref().is_some() {
                    invariant_violation("inactive mapping retains a current incarnation");
                }
            }
        }

        let next_prepared = prepared
            .checked_add(slot.carrier_count())
            .ok_or(BlockError::PreparedCapacityOverflow)?;
        let (fresh, won) = BlockIncarnation::try_new_preclaimed(slot.geometry, count)?;
        let fresh = Arc::new(fresh);
        let incarnation = BlockIncarnation::identity(&fresh);

        if let Err(error) = slot.range.prepare() {
            *mapping = MappingState::ActivationRecoveryPending;
            return Err(error.into());
        }

        *mapping = MappingState::Prepared;
        *prepared = next_prepared;
        if slot.current.swap(Some(fresh)).is_some() {
            invariant_violation("preclaimed activation replaced a current incarnation");
        }
        Ok(Some(ProvisionalBits {
            slot: Arc::clone(slot),
            incarnation,
            won,
        }))
    }

    /// Confirms that this block may be removed from prepared capacity.
    ///
    /// The caller holds the admission lock that protects the pool-wide
    /// `prepared` count and admission floor. Success subtracts the complete
    /// block and returns cleanup authority. A live bit restores `Active`
    /// without changing `prepared`.
    pub(super) fn start_trim(
        slot: &Arc<Self>,
        prepared: &mut CarrierCount,
        floor: CarrierCount,
    ) -> Result<TrimCleanup, TrimBlocked> {
        let mapping = slot.mapping.lock();
        match *mapping {
            MappingState::Prepared => {}
            MappingState::Reserved { .. } => return Err(TrimBlocked::NotPrepared),
            MappingState::ActivationRecoveryPending | MappingState::DeactivationRecoveryPending => {
                return Err(TrimBlocked::CleanupPending)
            }
        }

        let current = slot.current.load();
        let Some(incarnation) = current.as_ref() else {
            invariant_violation("prepared mapping has no current incarnation");
        };
        match incarnation.state.load(Ordering::Acquire) {
            IncarnationState::Active => {}
            IncarnationState::Draining => return Err(TrimBlocked::CleanupPending),
            IncarnationState::Dead => {
                invariant_violation("prepared mapping has a dead current incarnation")
            }
        }

        let Some(next_prepared) = prepared.checked_sub(slot.carrier_count()) else {
            invariant_violation("prepared capacity excludes an active block");
        };
        if next_prepared < floor {
            return Err(TrimBlocked::FloorViolation);
        }

        if incarnation.state.try_start_trim().is_err() {
            return Err(TrimBlocked::CleanupPending);
        }
        loom_seq_cst_fence();

        let busy = incarnation
            .in_use
            .iter()
            .enumerate()
            .any(|(word_index, word)| {
                let valid = bitmap_word_mask(slot.geometry, word_index);
                word.load(Ordering::SeqCst) & valid != 0
            });
        if busy {
            incarnation.state.restore_active();
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
    /// Pool maintenance calls this after a cleanup operation records pending
    /// work. A slot with no pending work returns success without changing
    /// state. The result is `true` when this call attempted pending platform
    /// work. Failure leaves the slot nonclaimable and identifies the required
    /// retry.
    pub(super) fn retry_cleanup(&self) -> Result<bool, CleanupRetry> {
        let mut mapping = self.mapping.lock();
        match *mapping {
            MappingState::Prepared => {
                let current = self.current.load();
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("prepared mapping has no current incarnation");
                };
                match incarnation.state.load(Ordering::Acquire) {
                    IncarnationState::Active | IncarnationState::Draining => return Ok(false),
                    IncarnationState::Dead => {
                        invariant_violation("prepared mapping has a dead current incarnation")
                    }
                }
            }
            MappingState::Reserved {
                reclaim_pending: false,
            } => {
                if self.current.load().as_ref().is_some() {
                    invariant_violation("inactive mapping has a current incarnation");
                }
                return Ok(false);
            }
            MappingState::Reserved {
                reclaim_pending: true,
            } => {
                if self.current.load().as_ref().is_some() {
                    invariant_violation("inactive reclaim has a current incarnation");
                }
                return match self.range.discard() {
                    Ok(()) => {
                        *mapping = MappingState::Reserved {
                            reclaim_pending: false,
                        };
                        Ok(true)
                    }
                    Err(error) => Err(CleanupRetry::ReclaimPending(error)),
                };
            }
            MappingState::ActivationRecoveryPending => {
                if self.current.load().as_ref().is_some() {
                    invariant_violation("activation recovery retained a current incarnation");
                }
            }
            MappingState::DeactivationRecoveryPending => {
                let current = self.current.load();
                let Some(incarnation) = current.as_ref() else {
                    invariant_violation("deactivation recovery lost its current incarnation");
                };
                if incarnation.state.load(Ordering::Acquire) != IncarnationState::Draining {
                    invariant_violation(
                        "deactivation recovery retained a non-draining incarnation",
                    );
                }
            }
        }

        if let Err(error) = self.range.deactivate() {
            return Err(CleanupRetry::DeactivationPending(error));
        }
        *mapping = MappingState::Reserved {
            reclaim_pending: false,
        };
        self.finish_inactive_cleanup(&mut mapping).map(|()| true)
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
            incarnation.state.retire();
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
    #[cfg(test)]
    pub(super) fn try_claim(
        slot: &Arc<Self>,
        count: CarrierCount,
    ) -> Result<Option<ProvisionalBits>, BlockError> {
        Ok(Self::try_claim_words(slot, 0, slot.bitmap_words(), count)?.into_provisional())
    }

    /// Exhaustively claims from the current active incarnation.
    ///
    /// Unlike the bounded optimistic path, this retries a bitmap word after a
    /// competing claimant takes selected candidates. It stops only after the
    /// request is complete or atomic observations find every remaining word
    /// full.
    pub(super) fn try_claim_exhaustive(
        slot: &Arc<Self>,
        count: CarrierCount,
    ) -> Result<Option<ProvisionalBits>, BlockError> {
        if count == CarrierCount::ZERO {
            return Err(BlockError::InvalidClaimCount);
        }

        let Some(mut attempt) = BlockClaimAttempt::begin(slot)? else {
            return Ok(None);
        };
        attempt.take_exhaustive(count.get());
        Ok(attempt.finish())
    }

    /// Claims from a bounded contiguous bitmap-word window.
    ///
    /// `start_word` beyond the bitmap and a zero `word_limit` inspect no
    /// words. `inspected_words` counts candidate positions even when the slot
    /// has no active incarnation.
    pub(super) fn try_claim_words(
        slot: &Arc<Self>,
        start_word: usize,
        word_limit: usize,
        count: CarrierCount,
    ) -> Result<BlockClaim, BlockError> {
        if count == CarrierCount::ZERO {
            return Err(BlockError::InvalidClaimCount);
        }

        let inspected_words = slot
            .bitmap_words()
            .saturating_sub(start_word)
            .min(word_limit);
        if inspected_words == 0 {
            return Ok(BlockClaim::empty());
        }

        let Some(mut attempt) = BlockClaimAttempt::begin_with_capacity(slot, inspected_words)?
        else {
            return Ok(BlockClaim {
                provisional: None,
                inspected_words,
            });
        };
        let (_, inspected_words) = attempt.take_words(start_word, inspected_words, count.get());
        Ok(BlockClaim {
            provisional: attempt.finish(),
            inspected_words,
        })
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
            IncarnationState::Active | IncarnationState::Draining => {}
            IncarnationState::Dead => invariant_violation("owned carriers name a dead incarnation"),
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

    /// Returns `true` when the current active incarnation appears all-free.
    ///
    /// This is only a trim-selection hint. A concurrent claim may change the
    /// result immediately; [`BlockSlot::start_trim`] performs the authoritative
    /// lifecycle, bitmap, and capacity checks.
    pub(super) fn appears_free(&self) -> bool {
        let current = self.current.load();
        let Some(incarnation) = current.as_ref() else {
            return false;
        };
        if incarnation.state.load(Ordering::Acquire) != IncarnationState::Active {
            return false;
        }
        incarnation
            .in_use
            .iter()
            .enumerate()
            .all(|(word_index, word)| {
                let valid = bitmap_word_mask(self.geometry, word_index);
                word.load(Ordering::Acquire) & valid == 0
            })
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
                    .enumerate()
                    .map(|(word_index, word)| {
                        let valid = bitmap_word_mask(self.geometry, word_index);
                        (word.load(Ordering::Acquire) & valid).count_ones() as usize
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    /// Injects one virtual-memory transition failure.
    #[cfg(test)]
    pub(super) fn inject_failure_once(&self, operation: VirtualMemoryOperation) {
        self.range.inject_failure_once(operation);
    }
}

/// Result of one bounded block claim.
pub(super) struct BlockClaim {
    /// Gate-passed bits won from the inspected window.
    provisional: Option<ProvisionalBits>,
    /// Candidate bitmap positions charged to the scan budget.
    inspected_words: usize,
}

impl BlockClaim {
    /// Returns an empty result that inspected no words.
    fn empty() -> Self {
        Self {
            provisional: None,
            inspected_words: 0,
        }
    }

    /// Returns the number of bitmap positions inspected.
    pub(super) fn inspected_words(&self) -> usize {
        self.inspected_words
    }

    /// Consumes the result and returns any won bits.
    pub(super) fn into_provisional(self) -> Option<ProvisionalBits> {
        self.provisional
    }
}

/// Single-owner cleanup token for a confirmed trim.
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
            || incarnation.state.load(Ordering::Acquire) != IncarnationState::Draining
        {
            invariant_violation("trim cleanup no longer owns the draining incarnation");
        }

        if let Err(error) = self.slot.range.deactivate() {
            *mapping = MappingState::DeactivationRecoveryPending;
            return Err(CleanupRetry::DeactivationPending(error));
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
        Self::begin_with_capacity(slot, slot.geometry.bitmap_words())
    }

    /// Starts a claim with storage for at most `won_capacity` bitmap words.
    fn begin_with_capacity(
        slot: &Arc<BlockSlot>,
        won_capacity: usize,
    ) -> Result<Option<Self>, TryReserveError> {
        let incarnation = slot.current.load();
        if incarnation.as_ref().is_none() {
            return Ok(None);
        }

        let mut won = Vec::new();
        won.try_reserve_exact(won_capacity)?;
        Ok(Some(Self {
            slot: Arc::clone(slot),
            incarnation,
            won,
        }))
    }

    /// Claims up to `count` free bits and returns the number won.
    ///
    /// A competing claim may take a candidate after the relaxed load:
    ///
    /// ```text
    /// this claim loads       0b0010  // bit 1 is occupied
    /// this claim selects     0b0101  // free bits 0 and 2
    /// another claim sets     0b0100  // competitor wins bit 2
    /// fetch_or returns       0b0110  // occupancy before this fetch_or
    /// this claim wins        0b0001  // candidate & !previous
    /// ```
    #[cfg(test)]
    fn take(&mut self, count: usize) -> usize {
        self.take_words(0, self.slot.geometry.bitmap_words(), count)
            .0
    }

    /// Claims from `word_count` words beginning at `start_word`.
    ///
    /// Returns carriers won and bitmap words inspected.
    fn take_words(
        &mut self,
        start_word: usize,
        word_count: usize,
        mut count: usize,
    ) -> (usize, usize) {
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        let mut taken = 0;
        let mut inspected = 0;
        let end_word = start_word
            .saturating_add(word_count)
            .min(incarnation.in_use.len());

        let start_word = start_word.min(end_word);
        for word_index in start_word..end_word {
            if count == 0 {
                break;
            }
            inspected += 1;
            let word = &incarnation.in_use[word_index];
            let valid = bitmap_word_mask(self.slot.geometry, word_index);
            let observed = word.load(Ordering::Relaxed);
            let candidate = take_lowest(!observed & valid, count);
            if candidate == 0 {
                continue;
            }
            let previous = word.fetch_or(candidate, Ordering::SeqCst);
            let won = candidate & !previous;
            if won != 0 {
                let won_count = won.count_ones() as usize;
                record_won(&mut self.won, word_index, won);
                count -= won_count;
                taken += won_count;
            }
        }
        (taken, inspected)
    }

    /// Claims until each word is observed full or `count` carriers are won.
    fn take_exhaustive(&mut self, mut count: usize) -> usize {
        let incarnation = self
            .incarnation
            .as_ref()
            .expect("a claim attempt protects one incarnation");
        let mut taken = 0;

        for (word_index, word) in incarnation.in_use.iter().enumerate() {
            let mut observed = word.load(Ordering::Relaxed);
            while count != 0 {
                let valid = bitmap_word_mask(self.slot.geometry, word_index);
                let candidate = take_lowest(!observed & valid, count);
                if candidate == 0 {
                    break;
                }
                let previous = word.fetch_or(candidate, Ordering::SeqCst);
                // Each collision enters the local occupancy view. The loop
                // therefore retries at most once per valid carrier bit.
                observed = previous | candidate;
                let won = candidate & !previous;
                if won != 0 {
                    let won_count = won.count_ones() as usize;
                    record_won(&mut self.won, word_index, won);
                    count -= won_count;
                    taken += won_count;
                }
            }
            if count == 0 {
                break;
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
        loom_seq_cst_fence();
        match incarnation.state.load(Ordering::SeqCst) {
            IncarnationState::Active => {}
            IncarnationState::Draining | IncarnationState::Dead => {
                self.rollback_original();
                return None;
            }
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

/// Single-owner set of gate-passed bits not yet converted to carriers.
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

/// Single-owner physical allocation for one carrier.
pub(super) struct CarrierAllocation {
    /// Slot that retains the carrier address.
    slot: Arc<BlockSlot>,
    /// Stable carrier and incarnation identity.
    id: CarrierId,
}

impl CarrierAllocation {
    /// Returns the concrete slot that roots this carrier's pointer provenance.
    pub(super) fn slot(&self) -> &Arc<BlockSlot> {
        &self.slot
    }

    /// Returns this carrier's stable physical location within the arena.
    pub(super) fn location(&self) -> CarrierLocation {
        CarrierLocation::new(self.id.slot, self.id.index)
    }

    /// Returns the stable block-slot identifier.
    pub(super) fn slot_id(&self) -> u32 {
        self.location().slot_id()
    }

    /// Returns the carrier index within its block.
    pub(super) fn carrier_index(&self) -> u32 {
        self.location().carrier_index()
    }

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

/// Returns valid bits for a checked bitmap word.
fn bitmap_word_mask(geometry: PoolGeometry, word_index: usize) -> u64 {
    geometry
        .bitmap_word_mask(word_index)
        .unwrap_or_else(|| invariant_violation("incarnation bitmap exceeds block geometry"))
}

/// Selects at most `count` least-significant bits from `available`.
///
/// The loop executes at most `min(count, available.count_ones())` times and
/// never more than 64 times.
fn take_lowest(available: u64, count: usize) -> u64 {
    let mut remaining = available;
    for _ in 0..count.min(u64::BITS as usize) {
        if remaining == 0 {
            break;
        }
        remaining &= remaining - 1;
    }
    available ^ remaining
}

/// Records disjoint bits while retaining at most one entry per word.
fn record_won(won: &mut Vec<WonWord>, word_index: usize, mask: u64) {
    if let Some(word) = won.last_mut().filter(|word| word.word_index == word_index) {
        word.mask |= mask;
    } else {
        won.push(WonWord { word_index, mask });
    }
}

/// Supplies the store-load edge missing from Loom's atomic model.
///
/// Production `SeqCst` operations already provide the required global order.
/// Loom 0.7 treats those accesses as `AcqRel`, but models a `SeqCst` fence.
#[inline]
fn loom_seq_cst_fence() {
    #[cfg(all(test, s3_tm_loom))]
    loom::sync::atomic::fence(Ordering::SeqCst);
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::super::virtual_memory::{page_size, VirtualMemoryOperation};
    use super::*;

    fn geometry(carriers: usize) -> PoolGeometry {
        let page_size = page_size().unwrap().get();
        PoolGeometry::new(page_size, page_size * carriers, page_size).unwrap()
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
    fn incarnation_transitions_enforce_their_predecessors() {
        let state = AtomicIncarnationState::new(IncarnationState::Active);

        assert_eq!(state.try_start_trim(), Ok(()));
        assert_eq!(state.load(Ordering::Acquire), IncarnationState::Draining);
        assert_eq!(state.try_start_trim(), Err(IncarnationState::Draining));
        state.restore_active();
        assert_eq!(state.load(Ordering::Acquire), IncarnationState::Active);

        let invalid_retire = catch_unwind(AssertUnwindSafe(|| state.retire()));
        assert!(invalid_retire.is_err());
        assert_eq!(state.load(Ordering::Acquire), IncarnationState::Active);

        state.try_start_trim().unwrap();
        state.retire();
        assert_eq!(state.load(Ordering::Acquire), IncarnationState::Dead);

        let invalid_restore = catch_unwind(AssertUnwindSafe(|| state.restore_active()));
        assert!(invalid_restore.is_err());
        assert_eq!(state.load(Ordering::Acquire), IncarnationState::Dead);
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

    #[cfg(debug_assertions)]
    #[test]
    fn test_immutable_range_live_check_covers_each_intersecting_carrier() {
        let (slot, _) = prepared_slot(3);
        let carrier_size = slot.geometry.carrier_size();
        let claimed = BlockSlot::try_claim(&slot, CarrierCount::new(2))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();

        slot.debug_assert_immutable_range_live(carrier_size - 1, 2);
        drop(claimed);

        let missing_live_bit = catch_unwind(AssertUnwindSafe(|| {
            slot.debug_assert_immutable_range_live(carrier_size - 1, 2);
        }));
        assert!(missing_live_bit.is_err());
    }

    #[test]
    fn preclaimed_activation_publishes_owned_bits() {
        let slot = Arc::new(BlockSlot::new(7, geometry(4)).unwrap());
        let mut prepared = CarrierCount::ZERO;

        let preclaimed =
            BlockSlot::prepare_and_claim_if_inactive(&slot, &mut prepared, CarrierCount::new(2))
                .unwrap()
                .expect("fresh slot should activate");

        assert_eq!(prepared, CarrierCount::new(4));
        assert_eq!(preclaimed.len(), CarrierCount::new(2));
        assert_eq!(slot.live_carriers(), 2);
        let preclaimed = preclaimed.into_carriers().unwrap();
        assert_eq!(
            preclaimed
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let remaining = BlockSlot::try_claim(&slot, CarrierCount::new(4))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        assert_eq!(
            remaining
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        drop(preclaimed);
        drop(remaining);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn preclaimed_activation_crosses_bitmap_words_without_padding() {
        let slot = Arc::new(BlockSlot::new(7, geometry(70)).unwrap());
        let mut prepared = CarrierCount::ZERO;

        let preclaimed =
            BlockSlot::prepare_and_claim_if_inactive(&slot, &mut prepared, CarrierCount::new(67))
                .unwrap()
                .expect("fresh slot should activate")
                .into_carriers()
                .unwrap();

        assert_eq!(prepared, CarrierCount::new(70));
        assert_eq!(preclaimed.len(), 67);
        assert_eq!(preclaimed.first().unwrap().carrier_index(), 0);
        assert_eq!(preclaimed.last().unwrap().carrier_index(), 66);

        let remaining = BlockSlot::try_claim(&slot, CarrierCount::new(70))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        assert_eq!(
            remaining
                .iter()
                .map(CarrierAllocation::carrier_index)
                .collect::<Vec<_>>(),
            vec![67, 68, 69]
        );
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());

        drop(preclaimed);
        drop(remaining);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn failed_preclaimed_activation_publishes_no_ownership() {
        let slot = Arc::new(BlockSlot::new(7, geometry(2)).unwrap());
        let mut prepared = CarrierCount::ZERO;
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Prepare);

        let error = match BlockSlot::prepare_and_claim_if_inactive(
            &slot,
            &mut prepared,
            CarrierCount::new(1),
        ) {
            Ok(_) => panic!("injected preparation failure must reject activation"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            BlockError::VirtualMemory(ref error)
                if error.operation() == VirtualMemoryOperation::Prepare
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_eq!(slot.live_carriers(), 0);
        assert!(current_identity(&slot).is_none());
        assert_mapping(&slot, MappingState::ActivationRecoveryPending);
    }

    #[test]
    fn preclaimed_activation_rejects_invalid_counts_without_mutation() {
        let slot = Arc::new(BlockSlot::new(7, geometry(2)).unwrap());
        let mut prepared = CarrierCount::ZERO;

        assert!(matches!(
            BlockSlot::prepare_and_claim_if_inactive(&slot, &mut prepared, CarrierCount::ZERO),
            Err(BlockError::InvalidClaimCount)
        ));
        assert!(matches!(
            BlockSlot::prepare_and_claim_if_inactive(&slot, &mut prepared, CarrierCount::new(3)),
            Err(BlockError::ClaimExceedsBlock {
                requested: 3,
                capacity: 2
            })
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_eq!(slot.live_carriers(), 0);
        assert!(current_identity(&slot).is_none());
        assert_mapping(
            &slot,
            MappingState::Reserved {
                reclaim_pending: false,
            },
        );
    }

    #[test]
    fn preclaimed_activation_does_not_replace_retained_metadata() {
        let (slot, mut prepared) = prepared_slot(1);
        let identity = current_identity(&slot).unwrap();
        slot.range
            .inject_failure_once(VirtualMemoryOperation::Deactivate);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();
        assert!(matches!(
            cleanup.finish(),
            Err(CleanupRetry::DeactivationPending(_))
        ));

        assert!(BlockSlot::prepare_and_claim_if_inactive(
            &slot,
            &mut prepared,
            CarrierCount::new(1)
        )
        .unwrap()
        .is_none());
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_eq!(current_identity(&slot), Some(identity));
        assert_eq!(slot.live_carriers(), 0);

        slot.prepare(&mut prepared).unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        assert_eq!(current_identity(&slot), Some(identity));
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
        assert_mapping(&slot, MappingState::ActivationRecoveryPending);
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
        assert_mapping(&slot, MappingState::ActivationRecoveryPending);
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
    fn cleanup_retry_is_a_noop_without_pending_work() {
        let slot = Arc::new(BlockSlot::new(7, geometry(1)).unwrap());
        let mut prepared = CarrierCount::ZERO;

        slot.retry_cleanup().unwrap();
        assert!(matches!(
            BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO),
            Err(TrimBlocked::NotPrepared)
        ));

        slot.prepare(&mut prepared).unwrap();
        slot.retry_cleanup().unwrap();
        assert_eq!(prepared, CarrierCount::new(1));
        assert_mapping(&slot, MappingState::Prepared);
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
    fn bounded_claim_inspects_only_its_word_window() {
        let (slot, _) = prepared_slot(130);

        let claim = BlockSlot::try_claim_words(&slot, 1, 1, CarrierCount::new(2)).unwrap();
        assert_eq!(claim.inspected_words(), 1);
        let carriers = claim.into_provisional().unwrap().into_carriers().unwrap();
        assert_eq!(
            carriers
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![64, 65]
        );
        drop(carriers);

        let final_word =
            BlockSlot::try_claim_words(&slot, 2, usize::MAX, CarrierCount::new(8)).unwrap();
        assert_eq!(final_word.inspected_words(), 1);
        let carriers = final_word
            .into_provisional()
            .unwrap()
            .into_carriers()
            .unwrap();
        assert_eq!(
            carriers
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![128, 129]
        );
        drop(carriers);
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn empty_claim_windows_do_not_mutate_ownership() {
        let (slot, _) = prepared_slot(65);

        let zero_budget = BlockSlot::try_claim_words(&slot, 0, 0, CarrierCount::new(1)).unwrap();
        assert_eq!(zero_budget.inspected_words(), 0);
        assert!(zero_budget.into_provisional().is_none());

        let past_end =
            BlockSlot::try_claim_words(&slot, slot.bitmap_words(), 1, CarrierCount::new(1))
                .unwrap();
        assert_eq!(past_end.inspected_words(), 0);
        assert!(past_end.into_provisional().is_none());
        assert_eq!(slot.live_carriers(), 0);
    }

    #[test]
    fn inactive_window_consumes_scan_positions_without_claiming() {
        let slot = Arc::new(BlockSlot::new(7, geometry(65)).unwrap());

        let claim = BlockSlot::try_claim_words(&slot, 0, usize::MAX, CarrierCount::new(1)).unwrap();

        assert_eq!(claim.inspected_words(), 2);
        assert!(claim.into_provisional().is_none());
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
    fn partial_claim_crosses_a_bitmap_word_boundary() {
        let (slot, _) = prepared_slot(66);
        let first = BlockSlot::try_claim(&slot, CarrierCount::new(63))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();

        let second = BlockSlot::try_claim(&slot, CarrierCount::new(3))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();

        assert_eq!(first.last().unwrap().id.index, 62);
        assert_eq!(
            second
                .iter()
                .map(|carrier| carrier.id.index)
                .collect::<Vec<_>>(),
            vec![63, 64, 65]
        );
        drop(first);
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
            Err(TrimBlocked::FloorViolation)
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
            CleanupRetry::DeactivationPending(ref error)
                if error.operation() == VirtualMemoryOperation::Deactivate
        ));
        assert_eq!(prepared, CarrierCount::ZERO);
        assert_mapping(&slot, MappingState::DeactivationRecoveryPending);
        assert_eq!(current_identity(&slot), Some(original_identity));
        assert!(BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .is_none());
        assert!(matches!(
            BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO),
            Err(TrimBlocked::CleanupPending)
        ));

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
            Err(CleanupRetry::DeactivationPending(_))
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
        assert_mapping(&slot, MappingState::DeactivationRecoveryPending);
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
    fn slot_mismatch_stops_before_clearing() {
        let (slot, _) = prepared_slot(1);
        let mut carriers = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap()
            .into_carriers()
            .unwrap();
        let carrier = carriers.pop().unwrap();
        let wrong = CarrierId {
            slot: carrier.id.slot + 1,
            ..carrier.id
        };

        let result = catch_unwind(AssertUnwindSafe(|| slot.release_one(wrong)));

        assert!(result.is_err());
        assert_eq!(slot.live_carriers(), 1);
        drop(carrier);
        assert_eq!(slot.live_carriers(), 0);
    }

    /// Covers selection against a fixed occupancy pattern.
    ///
    /// A set bit is a free carrier, so `0b1011_0100` offers four at positions
    /// 2, 4, 5, and 7. Selection consumes them upward from position 2, and a
    /// request larger than the free count stops at every free bit.
    #[test]
    fn selects_only_the_requested_low_bits() {
        assert_eq!(take_lowest(0, 8), 0);
        assert_eq!(take_lowest(0b1011_0100, 0), 0);
        assert_eq!(take_lowest(0b1011_0100, 1), 0b0000_0100);
        assert_eq!(take_lowest(0b1011_0100, 3), 0b0011_0100);
        assert_eq!(take_lowest(0b1011_0100, usize::MAX), 0b1011_0100);
        assert_eq!(take_lowest(u64::MAX, usize::MAX), u64::MAX);
    }

    #[test]
    fn repeated_wins_in_one_word_share_one_record() {
        let mut won = Vec::new();

        record_won(&mut won, 1, 0b0001);
        record_won(&mut won, 1, 0b0100);
        record_won(&mut won, 2, 0b0010);

        assert_eq!(won.len(), 2);
        assert_eq!(won[0].word_index, 1);
        assert_eq!(won[0].mask, 0b0101);
        assert_eq!(won[1].word_index, 2);
        assert_eq!(won[1].mask, 0b0010);
    }

    /// Covers every occupancy pattern in the low bytes plus full-width cases.
    ///
    /// Selection must satisfy three properties: it selects only free bits, it
    /// selects the requested count or every free bit when fewer are available,
    /// and the bits it selects are the lowest free ones. The sweep covers bit
    /// arrangements; the listed patterns cover positions and counts a 16-bit
    /// sweep cannot reach.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "exhaustive pure-integer sweep adds no Miri-specific coverage"
    )]
    fn take_lowest_returns_exact_lowest_subset() {
        fn assert_lowest_subset(available: u64, count: usize) {
            let selected = take_lowest(available, count);
            assert_eq!(
                selected & !available,
                0,
                "selected unavailable bits from {available:#018x} with count {count}"
            );
            // A request larger than the free count saturates.
            assert_eq!(
                selected.count_ones() as usize,
                count.min(available.count_ones() as usize),
                "selected the wrong number of bits from {available:#018x} with count {count}"
            );

            // `leading_zeros` returns 64 for zero, which underflows the
            // position arithmetic below.
            let remaining = available & !selected;
            if selected != 0 && remaining != 0 {
                // A correctly sized subset is lowest iff no remaining bit is
                // below a selected bit.
                let highest_selected = u64::BITS - 1 - selected.leading_zeros();
                let lowest_remaining = remaining.trailing_zeros();
                assert!(
                    highest_selected < lowest_remaining,
                    "selected {selected:#018x} from {available:#018x} with count {count}"
                );
            }
        }

        for available in 0..=u16::MAX as u64 {
            for count in 0..=(u16::BITS as usize + 1) {
                assert_lowest_subset(available, count);
            }
        }

        // The top bit alone, the two most distant bits, a high bit above a
        // sparse low cluster, both alternating phases, and a fully free word.
        // Counts run past `u64::BITS` to cover the loop bound.
        for available in [
            1 << 63,
            (1 << 63) | 1,
            0x8000_0000_0000_00a5,
            0xaaaa_aaaa_aaaa_aaaa,
            0x5555_5555_5555_5555,
            u64::MAX,
        ] {
            for count in 0..=(u64::BITS as usize + 1) {
                assert_lowest_subset(available, count);
            }
        }
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
        let geometry = PoolGeometry::new(4096, 4096, 4096).unwrap();
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
                Err(CleanupRetry::DeactivationPending(_))
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
                .map(|incarnation| {
                    incarnation.state.load(Ordering::Acquire) == IncarnationState::Active
                })
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

    #[test]
    fn preclaimed_activation_publishes_owned_bits_before_fast_claim() {
        loom::model(|| {
            let geometry = PoolGeometry::new(4096, 8192, 4096).unwrap();
            let slot = Arc::new(BlockSlot::new(0, geometry).unwrap());

            let prepare_slot = Arc::clone(&slot);
            let prepare = thread::spawn(move || {
                let mut prepared = CarrierCount::ZERO;
                let carriers = BlockSlot::prepare_and_claim_if_inactive(
                    &prepare_slot,
                    &mut prepared,
                    CarrierCount::new(1),
                )
                .unwrap()
                .expect("fresh slot should activate")
                .into_carriers()
                .unwrap();
                (prepared, carriers)
            });

            let claim_slot = Arc::clone(&slot);
            let claim = thread::spawn(move || {
                BlockSlot::try_claim(&claim_slot, CarrierCount::new(1))
                    .unwrap()
                    .map(ProvisionalBits::into_carriers)
                    .transpose()
                    .unwrap()
            });

            let (prepared, preclaimed) = prepare.join().unwrap();
            let claimed = claim.join().unwrap();

            assert_eq!(prepared, CarrierCount::new(2));
            assert_eq!(preclaimed.len(), 1);
            assert_eq!(preclaimed[0].id.index, 0);
            if let Some(claimed) = &claimed {
                assert_eq!(claimed.len(), 1);
                assert_eq!(claimed[0].id.index, 1);
            }
            let expected_live = 1 + claimed.as_ref().map_or(0, Vec::len);
            assert_eq!(slot.live_carriers(), expected_live);

            drop(preclaimed);
            drop(claimed);
            assert_eq!(slot.live_carriers(), 0);
        });
    }

    #[test]
    fn exhaustive_claim_retries_after_an_optimistic_collision() {
        loom::model(|| {
            let geometry = PoolGeometry::new(4096, 8192, 4096).unwrap();
            let slot = Arc::new(BlockSlot::new(0, geometry).unwrap());
            let mut prepared = CarrierCount::ZERO;
            slot.prepare(&mut prepared).unwrap();

            let exhaustive_slot = Arc::clone(&slot);
            let exhaustive = thread::spawn(move || {
                BlockSlot::try_claim_exhaustive(&exhaustive_slot, CarrierCount::new(1))
                    .unwrap()
                    .expect("exhaustive claim must find one of two carriers")
                    .into_carriers()
                    .unwrap()
            });
            let optimistic_slot = Arc::clone(&slot);
            let optimistic = thread::spawn(move || {
                BlockSlot::try_claim(&optimistic_slot, CarrierCount::new(1))
                    .unwrap()
                    .map(ProvisionalBits::into_carriers)
                    .transpose()
                    .unwrap()
            });

            let exhaustive = exhaustive.join().unwrap();
            let optimistic = optimistic.join().unwrap();
            if let Some(optimistic) = &optimistic {
                assert_ne!(exhaustive[0].carrier_index(), optimistic[0].carrier_index());
            }
            assert_eq!(
                slot.live_carriers(),
                1 + optimistic.as_ref().map_or(0, Vec::len)
            );

            drop(exhaustive);
            drop(optimistic);
            assert_eq!(slot.live_carriers(), 0);
        });
    }

    #[test]
    fn two_claimants_cannot_own_one_carrier() {
        loom::model(|| {
            let (slot, _) = prepared_slot();

            let first_slot = Arc::clone(&slot);
            let first = thread::spawn(move || {
                BlockSlot::try_claim(&first_slot, CarrierCount::new(1)).unwrap()
            });
            let second_slot = Arc::clone(&slot);
            let second = thread::spawn(move || {
                BlockSlot::try_claim(&second_slot, CarrierCount::new(1)).unwrap()
            });

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            let owned = first.as_ref().map_or(0, |claim| claim.len().get())
                + second.as_ref().map_or(0, |claim| claim.len().get());

            assert_eq!(owned, 1);
            assert_eq!(slot.live_carriers(), 1);
            drop(first);
            drop(second);
            assert_eq!(slot.live_carriers(), 0);
        });
    }

    #[test]
    fn concurrent_returns_clear_disjoint_bits_in_one_word() {
        loom::model(|| {
            let geometry = PoolGeometry::new(4096, 8192, 4096).unwrap();
            let slot = Arc::new(BlockSlot::new(0, geometry).unwrap());
            let mut prepared = CarrierCount::ZERO;
            slot.prepare(&mut prepared).unwrap();
            let carriers = BlockSlot::try_claim(&slot, CarrierCount::new(2))
                .unwrap()
                .unwrap()
                .into_carriers()
                .unwrap();
            let mut carriers = carriers.into_iter();
            let first = carriers.next().unwrap();
            let second = carriers.next().unwrap();
            assert!(carriers.next().is_none());

            let first = thread::spawn(move || drop(first));
            let second = thread::spawn(move || drop(second));
            first.join().unwrap();
            second.join().unwrap();

            assert_eq!(slot.live_carriers(), 0);
        });
    }
}
