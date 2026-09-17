/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Multi-block carrier acquisition and stable-range lookup.
//!
//! [`Arena`] publishes immutable registry generations for lock-free optimistic
//! scans and address classification. A miss enters serialized fallback, which
//! exhausts reusable capacity before preparing another block. The arena owns
//! physical topology and claims; admission policy and byte presentation remain
//! in higher layers.

use std::collections::TryReserveError;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering as DiagnosticOrdering};

use super::admission::AdmissionGuard;
use super::block::{
    BlockError, BlockSlot, CarrierAllocation, ProvisionalBits, TrimBlocked, TrimCleanup,
};
use super::geometry::PoolGeometry;
use super::virtual_memory::VirtualMemoryOperation;
use super::{invariant_violation, CarrierCount};
use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

/// Stable slots and their atomically published lookup index.
pub(super) struct Arena {
    /// Shared block geometry.
    geometry: PoolGeometry,
    /// Maximum bitmap words inspected by one optimistic claim.
    optimistic_scan_words: usize,
    /// Rotating flattened bitmap-word origin.
    scan_origin: AtomicUsize,
    /// Internal scan, growth, and rollback counters.
    diagnostics: Arc<ArenaDiagnostics>,
    /// Lock-free registry publication.
    registry: BlockRegistry,
    /// Serialized slot creation and registry rebuilding.
    state: Mutex<ArenaState>,
}

impl Arena {
    /// Creates an empty arena with fixed block geometry.
    pub(super) fn new(
        geometry: PoolGeometry,
        optimistic_scan_words: usize,
    ) -> Result<Self, ArenaError> {
        if optimistic_scan_words == 0 {
            return Err(ArenaError::InvalidScanBudget);
        }
        Ok(Self {
            geometry,
            optimistic_scan_words,
            scan_origin: AtomicUsize::new(0),
            diagnostics: Arc::new(ArenaDiagnostics::default()),
            registry: BlockRegistry::new(),
            state: Mutex::new(ArenaState {
                slots: Vec::new(),
                next_slot: Some(0),
            }),
        })
    }

    /// Reserves and publishes one inactive stable block slot.
    ///
    /// Failure leaves the registry and slot identifier unchanged.
    #[cfg(test)]
    pub(super) fn reserve_slot(&self) -> Result<Arc<BlockSlot>, ArenaError> {
        let mut state = self.state.lock();
        self.reserve_slot_locked(&mut state)
    }

    /// Reserves and publishes one slot while holding arena serialization.
    ///
    /// The mutable state reference proves exclusive access without reacquiring
    /// the mutex. Failure leaves the state and registry generation unchanged.
    fn reserve_slot_locked(&self, state: &mut ArenaState) -> Result<Arc<BlockSlot>, ArenaError> {
        let slot_id = state.next_slot.ok_or(ArenaError::SlotIdExhausted)?;

        state.slots.try_reserve(1)?;
        let slot = Arc::new(BlockSlot::new(slot_id, self.geometry)?);
        let generation = RegistryGeneration::try_with_slot(&state.slots, &slot)?;

        state.slots.push(Arc::clone(&slot));
        state.next_slot = slot_id.checked_add(1);
        self.registry.publish(generation);
        self.diagnostics.record_block_range_reserved();
        Ok(slot)
    }

    /// Classifies a complete nonempty range within one stable block slot.
    ///
    /// Integer addresses are used only for comparison. This method does not
    /// construct a pointer or authorize access to the classified range.
    pub(super) fn classify_range(&self, start: usize, len: usize) -> Option<ClassifiedRange> {
        self.registry.load().classify_range(start, len)
    }

    /// Selects one block observed active and free.
    ///
    /// Selection is a lock-free hint. A claim may race this scan, so the
    /// caller must use [`BlockSlot::start_trim`] to recheck the lifecycle,
    /// bitmap, and prepared-capacity floor under admission serialization.
    pub(super) fn select_trim_candidate(&self) -> Option<Arc<BlockSlot>> {
        let generation = self.registry_generation();
        let mut scanned = 0;
        let candidate = generation.slots_in_claim_order().iter().find_map(|slot| {
            scanned += 1;
            slot.appears_free().then(|| Arc::clone(slot))
        });
        self.diagnostics.record_trim_slots_scanned(scanned);
        candidate
    }

    /// Starts cleanup for one free block without crossing `floor`.
    ///
    /// Selection remains a hint. Each candidate is rechecked through the
    /// claim-trim gate while admission owns prepared-capacity serialization.
    pub(super) fn start_trim(
        &self,
        admission: &mut AdmissionGuard<'_>,
        floor: CarrierCount,
    ) -> ArenaTrim {
        let generation = self.registry_generation();
        let mut scanned = 0;
        let mut result = ArenaTrim::Blocked;
        for slot in generation.slots_in_claim_order() {
            scanned += 1;
            if !slot.appears_free() {
                continue;
            }
            match BlockSlot::start_trim(slot, admission.prepared_capacity_mut(), floor) {
                Ok(cleanup) => {
                    result = ArenaTrim::Started(cleanup);
                    break;
                }
                Err(TrimBlocked::FloorViolation) => break,
                Err(TrimBlocked::NotPrepared | TrimBlocked::Busy | TrimBlocked::CleanupPending) => {
                }
            }
        }
        self.diagnostics.record_trim_slots_scanned(scanned);
        result
    }

    /// Retries pending cleanup in the current registry generation.
    ///
    /// Returns `true` only when every slot reports no remaining cleanup work.
    pub(super) fn retry_cleanup(&self) -> bool {
        let generation = self.registry_generation();
        let mut complete = true;
        for slot in generation.slots_in_claim_order() {
            match slot.retry_cleanup() {
                Ok(false) => {}
                Ok(true) => self.diagnostics.record_cleanup_retry(),
                Err(error) => {
                    self.diagnostics.record_cleanup_retry();
                    self.diagnostics.record_cleanup_failure();
                    tracing::warn!(
                        target: crate::telemetry::TARGET_MEMORY,
                        slot_id = slot.id(),
                        error = ?error,
                        "buffer-pool mapping cleanup retry failed"
                    );
                    complete = false;
                }
            }
        }
        complete
    }

    /// Records one block whose prepared capacity and backing were reclaimed.
    pub(super) fn record_block_reclaimed(&self) {
        self.diagnostics.record_block_reclaimed();
    }

    /// Returns one private arena diagnostic sample.
    pub(super) fn diagnostics(&self) -> ArenaDiagnosticSnapshot {
        self.diagnostics.snapshot()
    }

    /// Reconstructs lifecycle state across the current registry generation.
    ///
    /// Concurrent claims and returns may span individual slot samples.
    /// Operational diagnostics therefore use this as a best-effort view;
    /// externally quiescent tests may use it for exact reconciliation.
    pub(super) fn sample_lifecycle(&self) -> ArenaLifecycleSample {
        let generation = self.registry_generation();
        let mut sample = ArenaLifecycleSample::default();
        for slot in generation.slots_in_claim_order() {
            let block = slot.sample_lifecycle();
            sample.prepared_capacity = sample
                .prepared_capacity
                .checked_add(block.prepared_capacity)
                .unwrap_or_else(|| invariant_violation("arena prepared diagnostic overflow"));
            sample.live_carriers = sample
                .live_carriers
                .checked_add(block.live_carriers)
                .unwrap_or_else(|| invariant_violation("arena live diagnostic overflow"));
            if block.cleanup_pending {
                sample.cleanup_pending_blocks = sample
                    .cleanup_pending_blocks
                    .checked_add(1)
                    .unwrap_or_else(|| invariant_violation("arena cleanup diagnostic overflow"));
            }
        }
        sample
    }

    /// Prepares capacity through `target` under admission serialization.
    ///
    /// Whole-block preparation may raise prepared capacity above `target`.
    /// Stable slots remain in the arena after trim. This operation scans those
    /// slots, skips capacity that is already prepared, and revives inactive or
    /// recovery-pending slots before reserving another virtual range. A failure
    /// retains capacity prepared by earlier iterations but publishes no
    /// admission grant.
    pub(super) fn prepare_to(
        &self,
        admission: &mut AdmissionGuard<'_>,
        target: CarrierCount,
    ) -> Result<(), ArenaError> {
        if admission.prepared_capacity() >= target {
            return Ok(());
        }

        let mut state = self.state.lock();
        for slot in &state.slots {
            if admission.prepared_capacity() >= target {
                return Ok(());
            }
            match slot.prepare(admission.prepared_capacity_mut()) {
                Ok(()) => self.diagnostics.record_block_prepared(),
                Err(BlockError::AlreadyPrepared) => {}
                Err(error) => return Err(error.into()),
            }
        }

        while admission.prepared_capacity() < target {
            let slot = self.reserve_slot_locked(&mut state)?;
            slot.prepare(admission.prepared_capacity_mut())?;
            self.diagnostics.record_block_prepared();
        }
        Ok(())
    }

    /// Claims a complete or partial batch within the optimistic scan budget.
    ///
    /// The returned batch retains every gate-passed bit. A complete batch may
    /// be converted to carrier owners. A partial batch remains suitable for
    /// serialized fallback and returns its bits if dropped.
    pub(super) fn claim_optimistic(
        &self,
        required: CarrierCount,
    ) -> Result<ClaimBatch, ArenaError> {
        if required == CarrierCount::ZERO {
            return Err(ArenaError::InvalidClaimCount);
        }

        let generation = self.registry_generation();
        let slots = generation.slots_in_claim_order();
        let words_per_slot = self.geometry.bitmap_words();
        let total_positions =
            slots
                .len()
                .checked_mul(words_per_slot)
                .ok_or(ArenaError::ScanSpaceOverflow {
                    slots: slots.len(),
                    words_per_slot,
                })?;
        let mut batch = ClaimBatch::new(required, Arc::clone(&self.diagnostics));
        if total_positions == 0 {
            self.diagnostics.record_optimistic_scan(0, true);
            return Ok(batch);
        }

        let budget = self.optimistic_scan_words.min(total_positions);
        let origin = self.scan_origin.fetch_add(budget, Ordering::Relaxed) % total_positions;
        while batch.inspected_words < budget && !batch.is_complete() {
            let position = wrapped_position(origin, batch.inspected_words, total_positions);
            let slot_index = position / words_per_slot;
            let start_word = position % words_per_slot;
            let word_limit = (words_per_slot - start_word).min(budget - batch.inspected_words);
            let claim = BlockSlot::try_claim_words(
                &slots[slot_index],
                start_word,
                word_limit,
                batch.remaining(),
            )?;
            let inspected = claim.inspected_words();
            if inspected == 0 || inspected > word_limit {
                invariant_violation("bounded block claim reported invalid scan work");
            }
            batch.inspected_words += inspected;
            if let Some(provisional) = claim.into_provisional() {
                batch.push(provisional)?;
            }
        }
        self.diagnostics
            .record_optimistic_scan(batch.inspected_words, !batch.is_complete());
        Ok(batch)
    }

    /// Completes a partial batch through exhaustive reuse and private growth.
    ///
    /// `admission` proves that the caller owns the prepared-capacity floor.
    /// This method holds arena serialization through the full registry recheck
    /// and every preparation. Existing inactive slots are reused before
    /// another stable range is reserved.
    pub(super) fn complete_claim_serialized(
        &self,
        admission: &mut AdmissionGuard<'_>,
        batch: &mut ClaimBatch,
    ) -> Result<(), ArenaError> {
        if batch.is_complete() {
            return Ok(());
        }
        self.diagnostics.record_serialized_fallback();

        let mut state = self.state.lock();
        for slot in &state.slots {
            if batch.is_complete() {
                return Ok(());
            }
            if let Some(provisional) = BlockSlot::try_claim_exhaustive(slot, batch.remaining())? {
                batch.push(provisional)?;
            }
        }

        for slot in &state.slots {
            if batch.is_complete() {
                return Ok(());
            }
            let count = std::cmp::min(batch.remaining(), slot.carrier_count());
            if let Some(provisional) = BlockSlot::prepare_and_claim_if_inactive(
                slot,
                admission.prepared_capacity_mut(),
                count,
            )? {
                self.diagnostics.record_block_prepared();
                batch.push(provisional)?;
            }
        }

        while !batch.is_complete() {
            let slot = self.reserve_slot_locked(&mut state)?;
            let count = std::cmp::min(batch.remaining(), slot.carrier_count());
            let provisional = BlockSlot::prepare_and_claim_if_inactive(
                &slot,
                admission.prepared_capacity_mut(),
                count,
            )?
            .unwrap_or_else(|| {
                invariant_violation("new arena slot was not reusable for private growth")
            });
            self.diagnostics.record_block_prepared();
            batch.push(provisional)?;
        }
        Ok(())
    }

    /// Protects the current immutable registry generation.
    fn registry_generation(&self) -> RegistryGenerationGuard {
        self.registry.load()
    }
}

/// Failure to configure, claim, reserve, or publish arena storage.
#[derive(Debug)]
pub(super) enum ArenaError {
    /// A block operation failed.
    Block(BlockError),
    /// Registry metadata could not be allocated.
    Allocation(TryReserveError),
    /// The optimistic bitmap-word budget is zero.
    InvalidScanBudget,
    /// A batch claim requested no carriers.
    InvalidClaimCount,
    /// The flattened registry scan space cannot be represented.
    ScanSpaceOverflow {
        /// Slots in the registry generation.
        slots: usize,
        /// Bitmap words in each slot.
        words_per_slot: usize,
    },
    /// Conversion was requested before the complete batch was owned.
    IncompleteClaim {
        /// Carriers required by the batch.
        required: usize,
        /// Carriers provisionally owned by the batch.
        claimed: usize,
    },
    /// No stable block identifier remains.
    SlotIdExhausted,
    /// The registry cannot represent another slot.
    RegistryCapacityOverflow,
    /// A stable virtual range overflows address representation.
    AddressOverflow {
        /// Slot containing the invalid range.
        slot_id: u32,
        /// First byte of the range.
        start: usize,
        /// Reserved range length.
        len: usize,
    },
    /// Two stable virtual ranges overlap.
    AddressOverlap {
        /// First overlapping range start.
        first_start: usize,
        /// First overlapping range end.
        first_end: usize,
        /// Second overlapping range start.
        second_start: usize,
        /// Second overlapping range end.
        second_end: usize,
    },
}

impl ArenaError {
    /// Returns whether this failure left an inactive slot needing recovery.
    pub(super) fn cleanup_required(&self) -> bool {
        matches!(
            self,
            Self::Block(BlockError::VirtualMemory(error))
                if error.operation() == VirtualMemoryOperation::Prepare
        )
    }
}

impl fmt::Display for ArenaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Block(error) => error.fmt(f),
            Self::Allocation(error) => write!(f, "arena metadata allocation failed: {error}"),
            Self::InvalidScanBudget => f.write_str("arena optimistic scan budget must be nonzero"),
            Self::InvalidClaimCount => f.write_str("an arena claim must request carriers"),
            Self::ScanSpaceOverflow {
                slots,
                words_per_slot,
            } => write!(
                f,
                "arena scan space {slots} slots by {words_per_slot} words overflows"
            ),
            Self::IncompleteClaim { required, claimed } => write!(
                f,
                "cannot finish incomplete arena claim: required {required}, claimed {claimed}"
            ),
            Self::SlotIdExhausted => f.write_str("arena block identifier space is exhausted"),
            Self::RegistryCapacityOverflow => f.write_str("arena registry capacity would overflow"),
            Self::AddressOverflow {
                slot_id,
                start,
                len,
            } => write!(
                f,
                "block slot {slot_id} address range {start:#x}+{len:#x} overflows"
            ),
            Self::AddressOverlap {
                first_start,
                first_end,
                second_start,
                second_end,
            } => write!(
                f,
                "block ranges {first_start:#x}..{first_end:#x} and \
                 {second_start:#x}..{second_end:#x} overlap"
            ),
        }
    }
}

impl std::error::Error for ArenaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Block(error) => Some(error),
            Self::Allocation(error) => Some(error),
            Self::InvalidScanBudget
            | Self::InvalidClaimCount
            | Self::ScanSpaceOverflow { .. }
            | Self::IncompleteClaim { .. }
            | Self::SlotIdExhausted
            | Self::RegistryCapacityOverflow
            | Self::AddressOverflow { .. }
            | Self::AddressOverlap { .. } => None,
        }
    }
}

impl From<BlockError> for ArenaError {
    fn from(error: BlockError) -> Self {
        Self::Block(error)
    }
}

impl From<TryReserveError> for ArenaError {
    fn from(error: TryReserveError) -> Self {
        Self::Allocation(error)
    }
}

/// Gate-passed carrier bits retained across one arena acquisition.
#[must_use = "dropping a claim batch returns its provisional carrier bits"]
pub(super) struct ClaimBatch {
    /// Carriers required for a complete acquisition.
    required: CarrierCount,
    /// Carriers currently owned by `blocks`.
    claimed: CarrierCount,
    /// Bitmap-word positions charged to the optimistic scan.
    inspected_words: usize,
    /// Per-incarnation provisional ownership.
    blocks: Vec<ProvisionalBits>,
    /// Counters updated if provisional ownership rolls back.
    diagnostics: Arc<ArenaDiagnostics>,
}

impl ClaimBatch {
    /// Creates an empty batch for `required` carriers.
    fn new(required: CarrierCount, diagnostics: Arc<ArenaDiagnostics>) -> Self {
        Self {
            required,
            claimed: CarrierCount::ZERO,
            inspected_words: 0,
            blocks: Vec::new(),
            diagnostics,
        }
    }

    /// Returns the required carrier count.
    #[cfg(test)]
    pub(super) fn required(&self) -> CarrierCount {
        self.required
    }

    /// Returns the provisionally owned carrier count.
    pub(super) fn claimed(&self) -> CarrierCount {
        self.claimed
    }

    /// Returns the carrier count still needed.
    pub(super) fn remaining(&self) -> CarrierCount {
        self.required
            .checked_sub(self.claimed)
            .unwrap_or_else(|| invariant_violation("arena claim exceeds its required count"))
    }

    /// Returns bitmap-word positions charged to the optimistic scan.
    pub(super) fn inspected_words(&self) -> usize {
        self.inspected_words
    }

    /// Returns `true` when the complete requested batch is owned.
    pub(super) fn is_complete(&self) -> bool {
        self.claimed == self.required
    }

    /// Retains another block's provisional ownership.
    fn push(&mut self, provisional: ProvisionalBits) -> Result<(), ArenaError> {
        let next_claimed = self
            .claimed
            .checked_add(provisional.len())
            .unwrap_or_else(|| invariant_violation("arena claim count overflowed"));
        if next_claimed > self.required {
            invariant_violation("arena claim won more carriers than requested");
        }

        if let Err(error) = self.blocks.try_reserve(1) {
            self.diagnostics
                .record_rolled_back_carriers(provisional.len().get());
            return Err(error.into());
        }
        self.blocks.push(provisional);
        self.claimed = next_claimed;
        Ok(())
    }

    /// Converts one complete provisional batch into carrier owners.
    ///
    /// An incomplete batch returns an error and releases its provisional bits.
    /// Allocation failure drops converted owners and remaining provisional
    /// ownership, returning every bit exactly once.
    pub(super) fn finish(mut self) -> Result<Vec<CarrierAllocation>, ArenaError> {
        if !self.is_complete() {
            return Err(ArenaError::IncompleteClaim {
                required: self.required.get(),
                claimed: self.claimed.get(),
            });
        }

        let mut carriers = Vec::new();
        carriers.try_reserve_exact(self.required.get())?;
        for provisional in self.blocks.drain(..) {
            let mut block = provisional.into_carriers()?;
            carriers.append(&mut block);
        }
        if carriers.len() != self.required.get() {
            invariant_violation("complete arena claim converted the wrong carrier count");
        }
        self.claimed = CarrierCount::ZERO;
        Ok(carriers)
    }
}

impl Drop for ClaimBatch {
    fn drop(&mut self) {
        self.diagnostics
            .record_rolled_back_carriers(self.claimed.get());
    }
}

/// Internal arena counters that never participate in allocator decisions.
#[derive(Default)]
struct ArenaDiagnostics {
    /// Bitmap words charged to completed optimistic scans.
    optimistic_scan_words: AtomicU64,
    /// Optimistic scans that returned incomplete ownership.
    optimistic_misses: AtomicU64,
    /// Partial batches that entered serialized fallback.
    serialized_fallbacks: AtomicU64,
    /// Successful whole-block preparation operations in fallback.
    blocks_prepared: AtomicU64,
    /// Stable virtual ranges added to the registry.
    block_ranges_reserved: AtomicU64,
    /// Carriers returned because a claim batch ended without publication.
    rolled_back_carriers: AtomicU64,
    /// Slots inspected while selecting trim candidates.
    trim_slots_scanned: AtomicU64,
    /// Blocks removed from prepared capacity and successfully reclaimed.
    blocks_reclaimed: AtomicU64,
    /// Pending mapping-cleanup operations retried.
    cleanup_retries: AtomicU64,
    /// Mapping-cleanup attempts that remain pending.
    cleanup_failures: AtomicU64,
}

impl ArenaDiagnostics {
    /// Records one completed optimistic scan.
    fn record_optimistic_scan(&self, words: usize, missed: bool) {
        saturating_add(&self.optimistic_scan_words, diagnostic_count(words));
        if missed {
            saturating_add(&self.optimistic_misses, 1);
        }
    }

    /// Records one serialized fallback entry.
    fn record_serialized_fallback(&self) {
        saturating_add(&self.serialized_fallbacks, 1);
    }

    /// Records one successful block preparation.
    fn record_block_prepared(&self) {
        saturating_add(&self.blocks_prepared, 1);
    }

    /// Records one stable virtual-range reservation.
    fn record_block_range_reserved(&self) {
        saturating_add(&self.block_ranges_reserved, 1);
    }

    /// Records carriers returned when a claim batch ends without publication.
    fn record_rolled_back_carriers(&self, carriers: usize) {
        saturating_add(&self.rolled_back_carriers, diagnostic_count(carriers));
    }

    /// Records work spent selecting one trim candidate.
    fn record_trim_slots_scanned(&self, slots: usize) {
        saturating_add(&self.trim_slots_scanned, diagnostic_count(slots));
    }

    /// Records one successfully reclaimed block.
    fn record_block_reclaimed(&self) {
        saturating_add(&self.blocks_reclaimed, 1);
    }

    /// Records one pending cleanup operation attempted by recovery.
    fn record_cleanup_retry(&self) {
        saturating_add(&self.cleanup_retries, 1);
    }

    /// Records one cleanup attempt that remains pending.
    fn record_cleanup_failure(&self) {
        saturating_add(&self.cleanup_failures, 1);
    }

    /// Loads one relaxed diagnostic sample.
    fn snapshot(&self) -> ArenaDiagnosticSnapshot {
        ArenaDiagnosticSnapshot {
            optimistic_scan_words: self.optimistic_scan_words.load(DiagnosticOrdering::Relaxed),
            optimistic_misses: self.optimistic_misses.load(DiagnosticOrdering::Relaxed),
            serialized_fallbacks: self.serialized_fallbacks.load(DiagnosticOrdering::Relaxed),
            blocks_prepared: self.blocks_prepared.load(DiagnosticOrdering::Relaxed),
            block_ranges_reserved: self.block_ranges_reserved.load(DiagnosticOrdering::Relaxed),
            rolled_back_carriers: self.rolled_back_carriers.load(DiagnosticOrdering::Relaxed),
            trim_slots_scanned: self.trim_slots_scanned.load(DiagnosticOrdering::Relaxed),
            blocks_reclaimed: self.blocks_reclaimed.load(DiagnosticOrdering::Relaxed),
            cleanup_retries: self.cleanup_retries.load(DiagnosticOrdering::Relaxed),
            cleanup_failures: self.cleanup_failures.load(DiagnosticOrdering::Relaxed),
        }
    }
}

/// Private snapshot of arena work and fallback behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ArenaDiagnosticSnapshot {
    /// Bitmap words charged to completed optimistic scans.
    pub(super) optimistic_scan_words: u64,
    /// Optimistic scans that returned incomplete ownership.
    pub(super) optimistic_misses: u64,
    /// Partial batches that entered serialized fallback.
    pub(super) serialized_fallbacks: u64,
    /// Successful whole-block preparation operations in fallback.
    pub(super) blocks_prepared: u64,
    /// Stable virtual ranges added to the registry.
    pub(super) block_ranges_reserved: u64,
    /// Carriers returned because a claim batch ended without publication.
    pub(super) rolled_back_carriers: u64,
    /// Slots inspected while selecting trim candidates.
    pub(super) trim_slots_scanned: u64,
    /// Blocks removed from prepared capacity and successfully reclaimed.
    pub(super) blocks_reclaimed: u64,
    /// Pending mapping-cleanup operations retried.
    pub(super) cleanup_retries: u64,
    /// Mapping-cleanup attempts that remain pending.
    pub(super) cleanup_failures: u64,
}

/// Lifecycle state reconstructed across one arena registry generation.
///
/// Individual block samples need not represent one atomic instant while the
/// pool is active.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ArenaLifecycleSample {
    /// Capacity represented by prepared active incarnations.
    pub(super) prepared_capacity: CarrierCount,
    /// Valid bitmap bits owned by claims or carrier guards.
    pub(super) live_carriers: CarrierCount,
    /// Slots blocked by trim or mapping recovery.
    pub(super) cleanup_pending_blocks: usize,
}

/// Result of scanning the registry for one trim candidate.
pub(super) enum ArenaTrim {
    /// One block passed the claim-trim gate and owns cleanup authority.
    Started(TrimCleanup),
    /// No block could be removed without waiting for state to change.
    Blocked,
}

/// Adds a diagnostic value without wrapping.
fn saturating_add(counter: &AtomicU64, value: u64) {
    if value == 0 {
        return;
    }
    let _ = counter.fetch_update(
        DiagnosticOrdering::Relaxed,
        DiagnosticOrdering::Relaxed,
        |current| Some(current.saturating_add(value)),
    );
}

/// Converts a platform-sized work count into a saturating diagnostic count.
fn diagnostic_count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Adds a bounded offset to an origin and wraps at `len` without overflow.
fn wrapped_position(origin: usize, offset: usize, len: usize) -> usize {
    let tail = len - origin;
    if offset < tail {
        origin + offset
    } else {
        offset - tail
    }
}

/// Slot state protected by arena serialization.
struct ArenaState {
    /// Slots in stable claim order.
    slots: Vec<Arc<BlockSlot>>,
    /// Identifier assigned to the next slot, or `None` after exhaustion.
    next_slot: Option<u32>,
}

/// Atomic publication for one coherent registry generation.
struct BlockRegistry {
    current: registry_cell::RegistryCell,
}

impl BlockRegistry {
    /// Creates a registry containing no slots.
    fn new() -> Self {
        Self {
            current: registry_cell::RegistryCell::new(Arc::new(RegistryGeneration::empty())),
        }
    }

    /// Protects the current registry generation.
    fn load(&self) -> RegistryGenerationGuard {
        RegistryGenerationGuard {
            inner: self.current.load(),
        }
    }

    /// Publishes a complete registry generation.
    fn publish(&self, generation: RegistryGeneration) {
        self.current.store(Arc::new(generation));
    }
}

/// Protected access to one immutable registry generation.
struct RegistryGenerationGuard {
    inner: registry_cell::RegistryCellGuard,
}

impl RegistryGenerationGuard {
    /// Returns slots in stable physical-claim order.
    fn slots_in_claim_order(&self) -> &[Arc<BlockSlot>] {
        &self.inner.as_ref().slots_in_claim_order
    }

    /// Classifies a complete nonempty range within one slot.
    fn classify_range(&self, start: usize, len: usize) -> Option<ClassifiedRange> {
        self.inner.as_ref().classify_range(start, len)
    }
}

/// Immutable claim and address indexes published as one generation.
struct RegistryGeneration {
    /// Stable scan order for physical acquisition.
    slots_in_claim_order: Box<[Arc<BlockSlot>]>,
    /// Non-overlapping virtual ranges sorted by address.
    address_ranges: Box<[AddressRange]>,
}

impl RegistryGeneration {
    /// Creates an empty registry generation.
    fn empty() -> Self {
        Self {
            slots_in_claim_order: Box::default(),
            address_ranges: Box::default(),
        }
    }

    /// Builds a generation containing `slots` followed by `slot`.
    fn try_with_slot(slots: &[Arc<BlockSlot>], slot: &Arc<BlockSlot>) -> Result<Self, ArenaError> {
        let new_len = slots
            .len()
            .checked_add(1)
            .ok_or(ArenaError::RegistryCapacityOverflow)?;

        let mut slots_in_claim_order = Vec::new();
        slots_in_claim_order.try_reserve_exact(new_len)?;
        slots_in_claim_order.extend(slots.iter().cloned());
        slots_in_claim_order.push(Arc::clone(slot));

        let mut address_ranges = Vec::new();
        address_ranges.try_reserve_exact(new_len)?;
        for (slot_index, slot) in slots_in_claim_order.iter().enumerate() {
            address_ranges.push(AddressRange::for_slot(slot, slot_index)?);
        }
        address_ranges.sort_unstable_by_key(|range| range.start);
        validate_address_ranges(&address_ranges)?;

        Ok(Self {
            slots_in_claim_order: slots_in_claim_order.into_boxed_slice(),
            address_ranges: address_ranges.into_boxed_slice(),
        })
    }

    /// Classifies a complete nonempty range within one slot.
    fn classify_range(&self, start: usize, len: usize) -> Option<ClassifiedRange> {
        if len == 0 {
            return None;
        }
        let end = start.checked_add(len)?;
        let candidate = self
            .address_ranges
            .partition_point(|range| range.start <= start)
            .checked_sub(1)?;
        let range = self.address_ranges.get(candidate)?;
        if end > range.end {
            return None;
        }
        let slot = Arc::clone(
            self.slots_in_claim_order
                .get(range.slot_index)
                .unwrap_or_else(|| invariant_violation("registry range has an invalid slot index")),
        );
        Some(ClassifiedRange {
            slot,
            offset: start - range.start,
        })
    }
}

/// One stable virtual range in the sorted address index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AddressRange {
    /// First reserved byte.
    start: usize,
    /// Exclusive end of the reservation.
    end: usize,
    /// Index into [`RegistryGeneration::slots_in_claim_order`].
    slot_index: usize,
}

impl AddressRange {
    /// Constructs a checked address range for `slot`.
    fn for_slot(slot: &BlockSlot, slot_index: usize) -> Result<Self, ArenaError> {
        let start = slot.base_address();
        let len = slot.reserved_len();
        let end = start.checked_add(len).ok_or(ArenaError::AddressOverflow {
            slot_id: slot.id(),
            start,
            len,
        })?;
        Ok(Self {
            start,
            end,
            slot_index,
        })
    }

    /// Constructs a checked synthetic range.
    #[cfg(test)]
    fn new(start: usize, len: usize, slot_index: usize) -> Result<Self, ArenaError> {
        let end = start.checked_add(len).ok_or(ArenaError::AddressOverflow {
            slot_id: u32::MAX,
            start,
            len,
        })?;
        Ok(Self {
            start,
            end,
            slot_index,
        })
    }
}

/// Validates sorted, non-overlapping address ranges.
fn validate_address_ranges(ranges: &[AddressRange]) -> Result<(), ArenaError> {
    for pair in ranges.windows(2) {
        let first = pair[0];
        let second = pair[1];
        if first.end > second.start {
            return Err(ArenaError::AddressOverlap {
                first_start: first.start,
                first_end: first.end,
                second_start: second.start,
                second_end: second.end,
            });
        }
    }
    Ok(())
}

/// Classification of an integer range within one stable slot.
pub(super) struct ClassifiedRange {
    /// Slot containing the complete range.
    slot: Arc<BlockSlot>,
    /// Byte offset from the slot base.
    offset: usize,
}

impl ClassifiedRange {
    /// Returns the containing slot.
    pub(super) fn slot(&self) -> &Arc<BlockSlot> {
        &self.slot
    }

    /// Returns the byte offset from the slot base.
    pub(super) fn offset(&self) -> usize {
        self.offset
    }
}

#[cfg(not(all(test, s3_tm_loom)))]
mod registry_cell {
    //! Lock-free production publication for registry generations.

    use arc_swap::{ArcSwap, Guard};

    use super::RegistryGeneration;
    use crate::runtime::sync::sync::Arc;

    /// Atomic storage for the current registry generation.
    pub(super) struct RegistryCell {
        inner: ArcSwap<RegistryGeneration>,
    }

    impl RegistryCell {
        /// Creates a cell containing `initial`.
        pub(super) fn new(initial: Arc<RegistryGeneration>) -> Self {
            Self {
                inner: ArcSwap::from(initial),
            }
        }

        /// Protects the current generation from reclamation.
        pub(super) fn load(&self) -> RegistryCellGuard {
            RegistryCellGuard {
                inner: self.inner.load(),
            }
        }

        /// Replaces the current generation.
        pub(super) fn store(&self, generation: Arc<RegistryGeneration>) {
            self.inner.store(generation);
        }
    }

    /// Protection for one loaded registry generation.
    pub(super) struct RegistryCellGuard {
        inner: Guard<Arc<RegistryGeneration>>,
    }

    impl RegistryCellGuard {
        /// Returns the protected generation.
        pub(super) fn as_ref(&self) -> &RegistryGeneration {
            &self.inner
        }
    }
}

#[cfg(all(test, s3_tm_loom))]
mod registry_cell {
    //! Loom-instrumented publication with owned registry generations.

    use super::RegistryGeneration;
    use crate::runtime::sync::sync::{Arc, Mutex};

    /// Loom-instrumented storage for the current registry generation.
    pub(super) struct RegistryCell {
        inner: Mutex<Arc<RegistryGeneration>>,
    }

    impl RegistryCell {
        /// Creates a cell containing `initial`.
        pub(super) fn new(initial: Arc<RegistryGeneration>) -> Self {
            Self {
                inner: Mutex::new(initial),
            }
        }

        /// Loads an owned generation.
        pub(super) fn load(&self) -> RegistryCellGuard {
            RegistryCellGuard {
                inner: Arc::clone(&self.inner.lock()),
            }
        }

        /// Replaces the current generation.
        pub(super) fn store(&self, generation: Arc<RegistryGeneration>) {
            *self.inner.lock() = generation;
        }
    }

    /// Owned protection for one loaded registry generation.
    pub(super) struct RegistryCellGuard {
        inner: Arc<RegistryGeneration>,
    }

    impl RegistryCellGuard {
        /// Returns the protected generation.
        pub(super) fn as_ref(&self) -> &RegistryGeneration {
            &self.inner
        }
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::super::admission::{AdmissionGuard, AdmissionState, MAX_PACKED_CARRIERS};
    use super::super::block::TrimBlocked;
    use super::super::virtual_memory::{page_size, VirtualMemoryOperation};
    use super::super::CarrierCount;
    use super::*;

    fn geometry() -> PoolGeometry {
        geometry_with_carriers(4)
    }

    fn geometry_with_carriers(carriers: usize) -> PoolGeometry {
        let page_size = page_size().unwrap().get();
        PoolGeometry::new(
            page_size,
            page_size.checked_mul(carriers).unwrap(),
            page_size,
        )
        .unwrap()
    }

    fn admission_with_prepared(prepared: CarrierCount) -> Mutex<AdmissionState> {
        let state = Mutex::new(AdmissionState::new(MAX_PACKED_CARRIERS));
        {
            let mut admission = AdmissionGuard::new(state.lock());
            *admission.prepared_capacity_mut() = prepared;
        }
        state
    }

    fn prepare_slots(arena: &Arena, count: usize) -> Vec<Arc<BlockSlot>> {
        let mut prepared = CarrierCount::ZERO;
        (0..count)
            .map(|_| {
                let slot = arena.reserve_slot().unwrap();
                slot.prepare(&mut prepared).unwrap();
                slot
            })
            .collect()
    }

    fn assert_fully_free(slot: &Arc<BlockSlot>) {
        let all = BlockSlot::try_claim(slot, slot.carrier_count())
            .unwrap()
            .expect("every carrier should be free");
        assert_eq!(all.len(), slot.carrier_count());
        drop(all);
    }

    #[test]
    fn empty_registry_classifies_no_range() {
        let arena = Arena::new(geometry(), 4).unwrap();

        assert!(arena.classify_range(1, 1).is_none());
        assert!(arena
            .registry_generation()
            .slots_in_claim_order()
            .is_empty());
    }

    #[test]
    fn slot_reservation_publishes_one_coherent_generation() {
        let arena = Arena::new(geometry(), 4).unwrap();
        let first = arena.reserve_slot().unwrap();
        let second = arena.reserve_slot().unwrap();
        let generation = arena.registry_generation();

        assert_eq!(
            generation
                .slots_in_claim_order()
                .iter()
                .map(|slot| slot.id())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(generation.inner.as_ref().address_ranges.len(), 2);
        assert!(generation
            .inner
            .as_ref()
            .address_ranges
            .windows(2)
            .all(|pair| pair[0].end <= pair[1].start));
        assert!(generation
            .slots_in_claim_order()
            .iter()
            .any(|slot| Arc::ptr_eq(slot, &first)));
        assert!(generation
            .slots_in_claim_order()
            .iter()
            .any(|slot| Arc::ptr_eq(slot, &second)));
    }

    #[test]
    fn production_registry_guard_survives_concurrent_publication() {
        let arena = Arc::new(Arena::new(geometry(), 4).unwrap());
        let first = arena.reserve_slot().unwrap();
        let old_generation = arena.registry_generation();

        let writer_arena = Arc::clone(&arena);
        let writer = thread::spawn(move || writer_arena.reserve_slot().unwrap());
        let second = writer.join().unwrap();
        let new_generation = arena.registry_generation();

        assert_eq!(old_generation.slots_in_claim_order().len(), 1);
        assert!(Arc::ptr_eq(
            &old_generation.slots_in_claim_order()[0],
            &first
        ));
        assert_eq!(new_generation.slots_in_claim_order().len(), 2);
        assert!(new_generation
            .slots_in_claim_order()
            .iter()
            .any(|slot| Arc::ptr_eq(slot, &first)));
        assert!(new_generation
            .slots_in_claim_order()
            .iter()
            .any(|slot| Arc::ptr_eq(slot, &second)));
    }

    #[test]
    fn classification_requires_one_complete_nonempty_slot_range() {
        let arena = Arena::new(geometry(), 4).unwrap();
        let slot = arena.reserve_slot().unwrap();
        let range = slot.address_range().unwrap();

        let complete = arena
            .classify_range(range.start, range.len())
            .expect("complete slot range is classified");
        assert!(Arc::ptr_eq(complete.slot(), &slot));
        assert_eq!(complete.offset(), 0);

        let final_byte = arena
            .classify_range(range.end - 1, 1)
            .expect("final byte is inside the slot");
        assert!(Arc::ptr_eq(final_byte.slot(), &slot));
        assert_eq!(final_byte.offset(), range.len() - 1);

        assert!(arena.classify_range(range.start, 0).is_none());
        assert!(arena.classify_range(range.end - 1, 2).is_none());
        assert!(arena.classify_range(usize::MAX, 2).is_none());
    }

    #[test]
    fn classification_rejects_gaps_and_cross_slot_ranges() {
        let geometry = geometry();
        let slots_in_claim_order = (0..3)
            .map(|slot_id| Arc::new(BlockSlot::new(slot_id, geometry).unwrap()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let generation = RegistryGeneration {
            slots_in_claim_order,
            address_ranges: vec![
                AddressRange::new(0x1000, 0x1000, 0).unwrap(),
                AddressRange::new(0x2000, 0x1000, 1).unwrap(),
                AddressRange::new(0x4000, 0x1000, 2).unwrap(),
            ]
            .into_boxed_slice(),
        };

        let second = generation
            .classify_range(0x2000, 1)
            .expect("the second range begins at its lower boundary");
        assert!(Arc::ptr_eq(
            second.slot(),
            &generation.slots_in_claim_order[1]
        ));
        assert_eq!(second.offset(), 0);

        assert!(generation.classify_range(0x3000, 1).is_none());
        assert!(generation.classify_range(0x1fff, 2).is_none());
    }

    #[test]
    fn classification_survives_trimmed_block_backing() {
        let arena = Arena::new(geometry(), 4).unwrap();
        let slot = arena.reserve_slot().unwrap();
        let range = slot.address_range().unwrap();
        let mut prepared = CarrierCount::ZERO;
        slot.prepare(&mut prepared).unwrap();

        BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO)
            .unwrap()
            .finish()
            .unwrap();

        assert_eq!(prepared, CarrierCount::ZERO);
        let classified = arena
            .classify_range(range.start, range.len())
            .expect("stable slot remains classified after trim");
        assert!(Arc::ptr_eq(classified.slot(), &slot));
    }

    #[test]
    fn address_validation_accepts_adjacency_and_rejects_overlap() {
        let adjacent = [
            AddressRange::new(0x1000, 0x1000, 0).unwrap(),
            AddressRange::new(0x2000, 0x1000, 1).unwrap(),
        ];
        assert!(validate_address_ranges(&adjacent).is_ok());

        let overlap = [
            AddressRange::new(0x1000, 0x1001, 0).unwrap(),
            AddressRange::new(0x2000, 0x1000, 1).unwrap(),
        ];
        assert!(matches!(
            validate_address_ranges(&overlap),
            Err(ArenaError::AddressOverlap { .. })
        ));
    }

    #[test]
    fn synthetic_address_overflow_is_rejected() {
        assert!(matches!(
            AddressRange::new(usize::MAX, 2, 0),
            Err(ArenaError::AddressOverflow { .. })
        ));
    }

    #[test]
    fn exhausted_slot_ids_leave_the_registry_unchanged() {
        let arena = Arena::new(geometry(), 4).unwrap();
        arena.state.lock().next_slot = None;

        assert!(matches!(
            arena.reserve_slot(),
            Err(ArenaError::SlotIdExhausted)
        ));
        assert!(arena
            .registry_generation()
            .slots_in_claim_order()
            .is_empty());
    }

    #[test]
    fn rejects_zero_scan_budget_and_zero_claim_count() {
        assert!(matches!(
            Arena::new(geometry(), 0),
            Err(ArenaError::InvalidScanBudget)
        ));

        let arena = Arena::new(geometry(), 1).unwrap();
        assert!(matches!(
            arena.claim_optimistic(CarrierCount::ZERO),
            Err(ArenaError::InvalidClaimCount)
        ));
    }

    #[test]
    fn empty_registry_returns_an_empty_partial_batch() {
        let arena = Arena::new(geometry(), 2).unwrap();

        let batch = arena.claim_optimistic(CarrierCount::new(2)).unwrap();

        assert_eq!(batch.required(), CarrierCount::new(2));
        assert_eq!(batch.claimed(), CarrierCount::ZERO);
        assert_eq!(batch.remaining(), CarrierCount::new(2));
        assert_eq!(batch.inspected_words(), 0);
        assert!(!batch.is_complete());
    }

    #[test]
    fn incomplete_scan_charges_the_exact_budget() {
        let arena = Arena::new(geometry_with_carriers(130), 2).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let occupied = BlockSlot::try_claim(&slot, slot.carrier_count())
            .unwrap()
            .unwrap();

        let batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();

        assert_eq!(batch.claimed(), CarrierCount::ZERO);
        assert_eq!(batch.inspected_words(), 2);
        drop(batch);
        drop(occupied);
        assert_fully_free(&slot);
    }

    #[test]
    fn inactive_slots_consume_scan_positions() {
        let arena = Arena::new(geometry_with_carriers(1), 1).unwrap();
        let inactive = arena.reserve_slot().unwrap();
        let active = prepare_slots(&arena, 1).pop().unwrap();

        let batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();

        assert_eq!(batch.claimed(), CarrierCount::ZERO);
        assert_eq!(batch.inspected_words(), 1);
        drop(batch);
        assert_fully_free(&active);
        assert!(BlockSlot::try_claim(&inactive, CarrierCount::new(1))
            .unwrap()
            .is_none());
    }

    #[test]
    fn scan_origin_rotates_by_the_charged_budget() {
        let arena = Arena::new(geometry_with_carriers(130), 2).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let mut indices = Vec::new();

        for _ in 0..3 {
            let carriers = arena
                .claim_optimistic(CarrierCount::new(1))
                .unwrap()
                .finish()
                .unwrap();
            indices.push(carriers[0].carrier_index());
            drop(carriers);
        }

        assert_eq!(indices, vec![0, 128, 64]);
        assert_fully_free(&slot);
    }

    #[test]
    fn scan_wraps_across_slot_and_bitmap_boundaries() {
        let arena = Arena::new(geometry_with_carriers(65), 3).unwrap();
        let slots = prepare_slots(&arena, 2);
        arena.scan_origin.store(3, Ordering::Relaxed);

        let batch = arena.claim_optimistic(CarrierCount::new(66)).unwrap();

        assert!(batch.is_complete());
        assert_eq!(batch.inspected_words(), 3);
        let carriers = batch.finish().unwrap();
        assert_eq!(
            (carriers[0].slot_id(), carriers[0].carrier_index()),
            (slots[1].id(), 64)
        );
        assert!(carriers[1..]
            .iter()
            .all(|carrier| carrier.slot_id() == slots[0].id()));
        assert_eq!(carriers[1].carrier_index(), 0);
        assert_eq!(carriers.last().unwrap().carrier_index(), 64);
        drop(carriers);
        assert_fully_free(&slots[0]);
        assert_fully_free(&slots[1]);
    }

    #[test]
    fn one_batch_accumulates_carriers_across_blocks() {
        let arena = Arena::new(geometry_with_carriers(2), 2).unwrap();
        let slots = prepare_slots(&arena, 2);

        let batch = arena.claim_optimistic(CarrierCount::new(3)).unwrap();

        assert_eq!(batch.claimed(), CarrierCount::new(3));
        assert_eq!(batch.inspected_words(), 2);
        let carriers = batch.finish().unwrap();
        assert_eq!(
            carriers
                .iter()
                .filter(|carrier| carrier.slot_id() == slots[0].id())
                .count(),
            2
        );
        assert_eq!(
            carriers
                .iter()
                .filter(|carrier| carrier.slot_id() == slots[1].id())
                .count(),
            1
        );
        drop(carriers);
        assert_fully_free(&slots[0]);
        assert_fully_free(&slots[1]);
    }

    #[test]
    fn incomplete_finish_returns_all_provisional_bits() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let batch = arena.claim_optimistic(CarrierCount::new(3)).unwrap();
        assert_eq!(batch.claimed(), CarrierCount::new(2));

        let error = match batch.finish() {
            Ok(_) => panic!("an incomplete batch must not expose carriers"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ArenaError::IncompleteClaim {
                required: 3,
                claimed: 2
            }
        ));
        assert_fully_free(&slot);
    }

    #[test]
    fn dropping_partial_batch_returns_all_provisional_bits() {
        let arena = Arena::new(geometry_with_carriers(65), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let batch = arena.claim_optimistic(CarrierCount::new(66)).unwrap();
        assert_eq!(batch.claimed(), CarrierCount::new(64));

        drop(batch);

        assert_fully_free(&slot);
    }

    #[test]
    fn complete_batch_converts_exactly_the_requested_carriers() {
        let arena = Arena::new(geometry_with_carriers(70), 2).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();

        let carriers = arena
            .claim_optimistic(CarrierCount::new(67))
            .unwrap()
            .finish()
            .unwrap();

        assert_eq!(carriers.len(), 67);
        assert_eq!(carriers.first().unwrap().carrier_index(), 0);
        assert_eq!(carriers.last().unwrap().carrier_index(), 66);
        drop(carriers);
        assert_fully_free(&slot);
    }

    #[test]
    fn concurrent_optimistic_claimants_cannot_share_a_carrier() {
        let arena = Arc::new(Arena::new(geometry_with_carriers(2), 1).unwrap());
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let start = Arc::new(Barrier::new(2));
        let live = Arc::new(Barrier::new(2));

        let claim = |arena: Arc<Arena>, start: Arc<Barrier>, live: Arc<Barrier>| {
            thread::spawn(move || {
                start.wait();
                let carriers = loop {
                    let batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();
                    if batch.is_complete() {
                        break batch.finish().unwrap();
                    }
                    thread::yield_now();
                };
                let id = (carriers[0].slot_id(), carriers[0].carrier_index());
                live.wait();
                drop(carriers);
                id
            })
        };
        let first = claim(Arc::clone(&arena), Arc::clone(&start), Arc::clone(&live));
        let second = claim(Arc::clone(&arena), Arc::clone(&start), Arc::clone(&live));

        assert_ne!(first.join().unwrap(), second.join().unwrap());
        assert_fully_free(&slot);
    }

    #[test]
    fn test_prepare_to_rounds_to_blocks_and_is_idempotent() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .prepare_to(&mut admission, CarrierCount::new(3))
            .unwrap();

        assert_eq!(admission.prepared_capacity(), CarrierCount::new(4));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 2);
        assert_eq!(arena.diagnostics().blocks_prepared, 2);

        arena
            .prepare_to(&mut admission, CarrierCount::new(4))
            .unwrap();
        assert_eq!(arena.diagnostics().blocks_prepared, 2);
    }

    #[test]
    fn test_prepare_to_reuses_an_inactive_slot_before_reserving() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slot = arena.reserve_slot().unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .prepare_to(&mut admission, CarrierCount::new(1))
            .unwrap();

        assert_eq!(admission.prepared_capacity(), CarrierCount::new(2));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 1);
        assert_fully_free(&slot);
    }

    #[test]
    fn test_prepare_to_failure_retains_earlier_preparation() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let first = arena.reserve_slot().unwrap();
        let second = arena.reserve_slot().unwrap();
        second.inject_failure_once(VirtualMemoryOperation::Prepare);
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        let error = arena
            .prepare_to(&mut admission, CarrierCount::new(3))
            .unwrap_err();

        assert!(matches!(
            error,
            ArenaError::Block(BlockError::VirtualMemory(ref error))
                if error.operation() == VirtualMemoryOperation::Prepare
        ));
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(2));
        assert_fully_free(&first);
        assert!(BlockSlot::try_claim(&second, CarrierCount::new(1))
            .unwrap()
            .is_none());

        arena
            .prepare_to(&mut admission, CarrierCount::new(3))
            .unwrap();
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(4));
        assert_fully_free(&second);
    }

    #[test]
    fn serialized_fallback_rechecks_active_capacity_before_growth() {
        let arena = Arena::new(geometry_with_carriers(65), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let occupied = BlockSlot::try_claim_words(&slot, 0, 1, CarrierCount::new(64))
            .unwrap()
            .into_provisional()
            .unwrap();
        let mut batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();
        assert_eq!(batch.claimed(), CarrierCount::ZERO);
        let admission = admission_with_prepared(CarrierCount::new(65));
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap();

        assert!(batch.is_complete());
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(65));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 1);
        let carriers = batch.finish().unwrap();
        assert_eq!(carriers[0].carrier_index(), 64);
        drop(carriers);
        drop(occupied);
        assert_fully_free(&slot);
    }

    #[test]
    fn serialized_fallback_exhausts_fragmented_capacity_before_growth() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slots = prepare_slots(&arena, 3);
        let occupied = slots
            .iter()
            .map(|slot| {
                BlockSlot::try_claim(slot, CarrierCount::new(1))
                    .unwrap()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut batch = arena.claim_optimistic(CarrierCount::new(3)).unwrap();
        assert_eq!(batch.claimed(), CarrierCount::new(1));
        let admission = admission_with_prepared(CarrierCount::new(6));
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap();

        assert!(batch.is_complete());
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(6));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 3);
        let carriers = batch.finish().unwrap();
        assert!(slots.iter().all(|slot| {
            carriers
                .iter()
                .filter(|carrier| carrier.slot_id() == slot.id())
                .count()
                == 1
        }));
        drop(carriers);
        drop(occupied);
        for slot in &slots {
            assert_fully_free(slot);
        }
    }

    #[test]
    fn serialized_fallback_reuses_inactive_slot_before_reserving() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let inactive = arena.reserve_slot().unwrap();
        let mut batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap();

        assert_eq!(admission.prepared_capacity(), CarrierCount::new(2));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 1);
        let carriers = batch.finish().unwrap();
        assert_eq!(carriers[0].slot_id(), inactive.id());
        drop(carriers);
        assert_fully_free(&inactive);
    }

    #[test]
    fn serialized_fallback_grows_whole_blocks_until_complete() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let mut batch = arena.claim_optimistic(CarrierCount::new(5)).unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap();

        assert!(batch.is_complete());
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(6));
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 3);
        let carriers = batch.finish().unwrap();
        assert_eq!(carriers.len(), 5);
        assert_eq!(
            carriers
                .iter()
                .map(|carrier| carrier.slot_id())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn later_preparation_failure_preserves_earlier_capacity() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let first = arena.reserve_slot().unwrap();
        let second = arena.reserve_slot().unwrap();
        second.inject_failure_once(VirtualMemoryOperation::Prepare);
        let mut batch = arena.claim_optimistic(CarrierCount::new(3)).unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        let error = arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap_err();

        assert!(matches!(
            error,
            ArenaError::Block(BlockError::VirtualMemory(ref error))
                if error.operation() == VirtualMemoryOperation::Prepare
        ));
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(2));
        assert_eq!(batch.claimed(), CarrierCount::new(2));
        assert!(!batch.is_complete());
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 2);
        drop(batch);
        assert_fully_free(&first);
        assert!(BlockSlot::try_claim(&second, CarrierCount::new(1))
            .unwrap()
            .is_none());
    }

    #[test]
    fn serialized_growth_rejects_prepared_capacity_overflow() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let mut batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();
        let admission = admission_with_prepared(CarrierCount::new(usize::MAX));
        let mut admission = AdmissionGuard::new(admission.lock());

        let error = arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap_err();

        assert!(matches!(
            error,
            ArenaError::Block(BlockError::PreparedCapacityOverflow)
        ));
        assert_eq!(admission.prepared_capacity(), CarrierCount::new(usize::MAX));
        assert_eq!(batch.claimed(), CarrierCount::ZERO);
        let generation = arena.registry_generation();
        assert_eq!(generation.slots_in_claim_order().len(), 1);
        assert!(
            BlockSlot::try_claim(&generation.slots_in_claim_order()[0], CarrierCount::new(1))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn concurrent_serialized_fallbacks_retain_private_growth() {
        let arena = Arc::new(Arena::new(geometry_with_carriers(2), 1).unwrap());
        let admission = Arc::new(admission_with_prepared(CarrierCount::ZERO));
        let start = Arc::new(Barrier::new(2));

        let fallback =
            |arena: Arc<Arena>, admission: Arc<Mutex<AdmissionState>>, start: Arc<Barrier>| {
                thread::spawn(move || {
                    let mut batch = arena.claim_optimistic(CarrierCount::new(2)).unwrap();
                    start.wait();
                    let mut admission = AdmissionGuard::new(admission.lock());
                    arena
                        .complete_claim_serialized(&mut admission, &mut batch)
                        .unwrap();
                    drop(admission);
                    batch.finish().unwrap()
                })
            };
        let first = fallback(
            Arc::clone(&arena),
            Arc::clone(&admission),
            Arc::clone(&start),
        );
        let second = fallback(
            Arc::clone(&arena),
            Arc::clone(&admission),
            Arc::clone(&start),
        );

        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(
            AdmissionGuard::new(admission.lock()).prepared_capacity(),
            CarrierCount::new(4)
        );
        assert_eq!(arena.registry_generation().slots_in_claim_order().len(), 2);
        assert!(first.iter().all(|left| second.iter().all(|right| {
            (left.slot_id(), left.carrier_index()) != (right.slot_id(), right.carrier_index())
        })));
    }

    #[test]
    fn trim_selection_skips_inactive_and_occupied_slots() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let inactive = arena.reserve_slot().unwrap();
        let active = prepare_slots(&arena, 2);
        let occupied = BlockSlot::try_claim(&active[0], CarrierCount::new(1))
            .unwrap()
            .unwrap();

        let candidate = arena
            .select_trim_candidate()
            .expect("one active block is free");

        assert!(Arc::ptr_eq(&candidate, &active[1]));
        assert!(!Arc::ptr_eq(&candidate, &inactive));
        assert_eq!(arena.diagnostics().trim_slots_scanned, 3);
        drop(occupied);
    }

    #[test]
    fn trim_candidate_is_rechecked_after_selection() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let candidate = arena
            .select_trim_candidate()
            .expect("prepared block is initially free");
        let occupied = BlockSlot::try_claim(&slot, CarrierCount::new(1))
            .unwrap()
            .unwrap();
        let mut prepared = CarrierCount::new(2);

        assert!(matches!(
            BlockSlot::start_trim(&candidate, &mut prepared, CarrierCount::ZERO),
            Err(TrimBlocked::Busy)
        ));
        assert_eq!(prepared, CarrierCount::new(2));
        drop(occupied);
    }

    #[test]
    fn trim_selection_skips_cleanup_owned_slot() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();
        let mut prepared = CarrierCount::new(2);
        let cleanup = BlockSlot::start_trim(&slot, &mut prepared, CarrierCount::ZERO).unwrap();

        assert!(arena.select_trim_candidate().is_none());
        assert_eq!(arena.diagnostics().trim_slots_scanned, 1);

        cleanup.finish().unwrap();
    }

    #[test]
    fn arena_diagnostics_record_scan_growth_and_rollback() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        arena.reserve_slot().unwrap();
        let mut batch = arena.claim_optimistic(CarrierCount::new(3)).unwrap();
        let admission = admission_with_prepared(CarrierCount::ZERO);
        let mut admission = AdmissionGuard::new(admission.lock());

        arena
            .complete_claim_serialized(&mut admission, &mut batch)
            .unwrap();
        let before_rollback = arena.diagnostics();

        assert_eq!(before_rollback.optimistic_scan_words, 1);
        assert_eq!(before_rollback.optimistic_misses, 1);
        assert_eq!(before_rollback.serialized_fallbacks, 1);
        assert_eq!(before_rollback.blocks_prepared, 2);
        assert_eq!(before_rollback.block_ranges_reserved, 2);
        assert_eq!(before_rollback.rolled_back_carriers, 0);

        drop(batch);
        assert_eq!(arena.diagnostics().rolled_back_carriers, 3);
    }

    #[test]
    fn completed_batch_is_not_recorded_as_rollback() {
        let arena = Arena::new(geometry_with_carriers(2), 1).unwrap();
        let slot = prepare_slots(&arena, 1).pop().unwrap();

        let carriers = arena
            .claim_optimistic(CarrierCount::new(1))
            .unwrap()
            .finish()
            .unwrap();

        assert_eq!(arena.diagnostics().rolled_back_carriers, 0);
        drop(carriers);
        assert_eq!(arena.diagnostics().rolled_back_carriers, 0);
        assert_fully_free(&slot);
    }

    #[test]
    fn arena_diagnostics_saturate() {
        let diagnostics = ArenaDiagnostics::default();
        diagnostics
            .rolled_back_carriers
            .store(u64::MAX - 1, DiagnosticOrdering::Relaxed);

        diagnostics.record_rolled_back_carriers(usize::MAX);
        diagnostics.record_rolled_back_carriers(1);

        assert_eq!(diagnostics.snapshot().rolled_back_carriers, u64::MAX);
    }

    #[test]
    fn wrapped_scan_position_does_not_overflow() {
        assert_eq!(wrapped_position(3, 0, 4), 3);
        assert_eq!(wrapped_position(3, 1, 4), 0);
        assert_eq!(wrapped_position(3, 3, 4), 2);
        assert_eq!(wrapped_position(usize::MAX - 1, 1, usize::MAX), 0);
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::admission::{AdmissionGuard, AdmissionState, MAX_PACKED_CARRIERS};
    use super::super::virtual_memory::page_size;
    use super::*;
    use crate::runtime::sync::thread;

    fn admission_with_prepared(prepared: CarrierCount) -> Mutex<AdmissionState> {
        let state = Mutex::new(AdmissionState::new(MAX_PACKED_CARRIERS));
        {
            let mut admission = AdmissionGuard::new(state.lock());
            *admission.prepared_capacity_mut() = prepared;
        }
        state
    }

    fn assert_coherent(generation: &RegistryGenerationGuard) {
        let generation = generation.inner.as_ref();
        assert_eq!(
            generation.slots_in_claim_order.len(),
            generation.address_ranges.len()
        );
        for range in &generation.address_ranges {
            let slot = &generation.slots_in_claim_order[range.slot_index];
            assert_eq!(slot.address_range(), Some(range.start..range.end));
        }
    }

    #[test]
    fn registry_generation_is_published_coherently() {
        loom::model(|| {
            let page_size = page_size().unwrap().get();
            let geometry = PoolGeometry::new(page_size, page_size, page_size).unwrap();
            let arena = Arc::new(Arena::new(geometry, 1).unwrap());
            arena.reserve_slot().unwrap();

            let writer_arena = Arc::clone(&arena);
            let writer = thread::spawn(move || {
                writer_arena.reserve_slot().unwrap();
            });

            let reader_arena = Arc::clone(&arena);
            let reader = thread::spawn(move || {
                assert_coherent(&reader_arena.registry_generation());
            });

            writer.join().unwrap();
            reader.join().unwrap();
            let generation = arena.registry_generation();
            assert_coherent(&generation);
            assert_eq!(generation.slots_in_claim_order().len(), 2);
        });
    }

    #[test]
    fn completed_concurrent_batches_cannot_own_the_same_carrier() {
        loom::model(|| {
            let page_size = page_size().unwrap().get();
            let geometry = PoolGeometry::new(page_size, page_size * 2, page_size).unwrap();
            let arena = Arc::new(Arena::new(geometry, 1).unwrap());
            let slot = arena.reserve_slot().unwrap();
            let mut prepared = CarrierCount::ZERO;
            slot.prepare(&mut prepared).unwrap();

            let claim = |arena: Arc<Arena>| {
                thread::spawn(move || {
                    let batch = arena.claim_optimistic(CarrierCount::new(1)).unwrap();
                    if batch.is_complete() {
                        Some(batch.finish().unwrap())
                    } else {
                        None
                    }
                })
            };
            let first = claim(Arc::clone(&arena));
            let second = claim(Arc::clone(&arena));

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            assert!(first.is_some() || second.is_some());
            if let (Some(first), Some(second)) = (&first, &second) {
                assert_ne!(first[0].carrier_index(), second[0].carrier_index());
            }
        });
    }

    #[test]
    fn concurrent_claims_use_distinct_rotated_windows() {
        loom::model(|| {
            let page_size = page_size().unwrap().get();
            let geometry = PoolGeometry::new(page_size, page_size * 65, page_size).unwrap();
            let arena = Arc::new(Arena::new(geometry, 1).unwrap());
            let slot = arena.reserve_slot().unwrap();
            let mut prepared = CarrierCount::ZERO;
            slot.prepare(&mut prepared).unwrap();

            let claim = |arena: Arc<Arena>| {
                thread::spawn(move || {
                    arena
                        .claim_optimistic(CarrierCount::new(1))
                        .unwrap()
                        .finish()
                        .unwrap()
                })
            };
            let first = claim(Arc::clone(&arena));
            let second = claim(Arc::clone(&arena));

            let first = first.join().unwrap();
            let second = second.join().unwrap();
            let mut indices = [first[0].carrier_index(), second[0].carrier_index()];
            indices.sort_unstable();
            assert_eq!(indices, [0, 64]);

            drop(first);
            drop(second);
            let returned = BlockSlot::try_claim(&slot, slot.carrier_count())
                .unwrap()
                .expect("both rotated-window owners returned");
            assert_eq!(returned.len(), slot.carrier_count());
        });
    }

    #[test]
    fn private_growth_cannot_be_stolen_by_an_optimistic_claim() {
        loom::model(|| {
            let page_size = page_size().unwrap().get();
            let geometry = PoolGeometry::new(page_size, page_size * 2, page_size).unwrap();
            let arena = Arc::new(Arena::new(geometry, 1).unwrap());

            let fallback_arena = Arc::clone(&arena);
            let fallback = thread::spawn(move || {
                let mut batch = fallback_arena
                    .claim_optimistic(CarrierCount::new(1))
                    .unwrap();
                let admission = admission_with_prepared(CarrierCount::ZERO);
                let mut admission = AdmissionGuard::new(admission.lock());
                fallback_arena
                    .complete_claim_serialized(&mut admission, &mut batch)
                    .unwrap();
                assert_eq!(admission.prepared_capacity(), CarrierCount::new(2));
                batch.finish().unwrap()
            });

            let optimistic_arena = Arc::clone(&arena);
            let optimistic = thread::spawn(move || {
                let batch = optimistic_arena
                    .claim_optimistic(CarrierCount::new(1))
                    .unwrap();
                if batch.is_complete() {
                    Some(batch.finish().unwrap())
                } else {
                    None
                }
            });

            let fallback = fallback.join().unwrap();
            let optimistic = optimistic.join().unwrap();
            assert_eq!(fallback.len(), 1);
            if let Some(optimistic) = optimistic {
                assert_ne!(
                    (fallback[0].slot_id(), fallback[0].carrier_index()),
                    (optimistic[0].slot_id(), optimistic[0].carrier_index())
                );
            }
        });
    }
}
