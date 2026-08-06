/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Physical carrier acquisition, return, growth, and trim.
//!
//! The arena reserves stable block slots. Preparing a slot publishes a fresh
//! incarnation and bitmap; trimming decommits its heap-backed region without
//! reusing occupancy metadata. The heap region is a deterministic substitute
//! for mmap. Claim, drain, and revival use the production concurrency protocol.

use crate::runtime::sync::sync::{Arc, Mutex};

use super::AllocError;

#[cfg(not(all(test, s3_tm_loom)))]
use arc_swap::{ArcSwap, Guard};

mod block;
use block::{BlockSlot, ClaimedCarrier, ReviveError, TrimCleanup as BlockTrimCleanup};

/// Default bitmap geometry for reusable blocks.
const DEFAULT_BLOCK_CARRIERS: usize = 64;

/// Physical source selected for a carrier acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AcquisitionSource {
    Reused,
    Overflow,
}

/// Exclusive physical allocation transferred into a carrier guard.
///
/// Dropping this value returns its bitmap bit. Callers must move it into the
/// final carrier guard before publishing mutable or immutable views.
pub(super) struct CarrierAllocation {
    claimed: ClaimedCarrier,
    pub(super) source: AcquisitionSource,
}

/// Cleanup work detached from admission serialization after trim confirmation.
pub(super) struct TrimCleanup(BlockTrimCleanup);

/// Stable block registry and serialized growth state.
pub(super) struct Arena {
    carrier_size: usize,
    retained_limit: usize,
    blocks: BlockRegistry,
    state: Mutex<ArenaState>,
}

/// Registry state. Claim and return do not hold this lock while mutating bits.
struct ArenaState {
    blocks: Vec<ArenaBlock>,
    retained_geometry: usize,
    next_slot: u32,
    #[cfg(test)]
    fail_after_successes: Option<usize>,
}

#[derive(Clone)]
struct ArenaBlock {
    slot: Arc<BlockSlot>,
    kind: BlockKind,
}

/// Immutable block snapshot used by lock-free claim scans.
struct BlockRegistry {
    #[cfg(not(all(test, s3_tm_loom)))]
    inner: ArcSwap<Vec<ArenaBlock>>,
    #[cfg(all(test, s3_tm_loom))]
    inner: Mutex<Arc<Vec<ArenaBlock>>>,
}

struct BlockRegistryGuard {
    #[cfg(not(all(test, s3_tm_loom)))]
    inner: Guard<Arc<Vec<ArenaBlock>>>,
    #[cfg(all(test, s3_tm_loom))]
    inner: Arc<Vec<ArenaBlock>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BlockKind {
    Retained,
    Overflow,
}

/// Physical counters sampled from the stable registry and live bitmaps.
pub(super) struct ArenaSnapshot {
    pub(super) retained: usize,
    pub(super) free_retained: usize,
    pub(super) overflow: usize,
    pub(super) quarantined: usize,
    pub(super) physical_live: usize,
}

impl CarrierAllocation {
    fn new(claimed: ClaimedCarrier, source: AcquisitionSource) -> Self {
        Self { claimed, source }
    }

    pub(super) fn ptr(&self) -> std::ptr::NonNull<std::mem::MaybeUninit<u8>> {
        self.claimed.ptr()
    }

    pub(super) fn capacity(&self) -> usize {
        self.claimed.capacity()
    }
}

impl TrimCleanup {
    pub(super) fn carrier_count(&self) -> usize {
        self.0.carrier_count()
    }

    pub(super) fn finish(self) -> bool {
        self.0.finish()
    }
}

impl BlockRegistry {
    fn new() -> Self {
        Self {
            #[cfg(not(all(test, s3_tm_loom)))]
            inner: ArcSwap::from_pointee(Vec::new()),
            #[cfg(all(test, s3_tm_loom))]
            inner: Mutex::new(Arc::new(Vec::new())),
        }
    }

    fn load(&self) -> BlockRegistryGuard {
        BlockRegistryGuard {
            #[cfg(not(all(test, s3_tm_loom)))]
            inner: self.inner.load(),
            #[cfg(all(test, s3_tm_loom))]
            inner: Arc::clone(&self.inner.lock()),
        }
    }

    fn publish(&self, blocks: &[ArenaBlock]) {
        let blocks = Arc::new(blocks.to_vec());
        #[cfg(not(all(test, s3_tm_loom)))]
        self.inner.store(blocks);
        #[cfg(all(test, s3_tm_loom))]
        {
            *self.inner.lock() = blocks;
        }
    }
}

impl BlockRegistryGuard {
    fn as_slice(&self) -> &[ArenaBlock] {
        self.inner.as_slice()
    }
}

impl Arena {
    /// Create an arena that prefers at most `retained_limit` ordinary carriers.
    pub(super) fn new(carrier_size: usize, retained_limit: usize) -> Self {
        Self {
            carrier_size,
            retained_limit,
            blocks: BlockRegistry::new(),
            state: Mutex::new(ArenaState {
                blocks: Vec::new(),
                retained_geometry: 0,
                next_slot: 0,
                #[cfg(test)]
                fail_after_successes: None,
            }),
        }
    }

    /// Fixed capacity of every carrier.
    pub(super) fn carrier_size(&self) -> usize {
        self.carrier_size
    }

    /// Prepare at least `target` carriers under admission serialization.
    ///
    /// Block rounding may raise `prepared` beyond `target`.
    pub(super) fn prepare_to(&self, target: usize, prepared: &mut usize) -> Result<(), AllocError> {
        let mut state = self.state.lock();
        while *prepared < target {
            if let Some(block) = state
                .blocks
                .iter()
                .find(|block| !block.slot.is_mapped())
                .cloned()
            {
                block.slot.revive(prepared).map_err(map_revive_error)?;
                continue;
            }

            let block = self.create_block(&mut state)?;
            block.slot.revive(prepared).map_err(map_revive_error)?;
            state.blocks.push(block);
            self.blocks.publish(&state.blocks);
        }
        Ok(())
    }

    /// Try every currently Active block without preparing more capacity.
    pub(super) fn try_acquire_one(&self) -> Result<Option<CarrierAllocation>, AllocError> {
        self.check_injected_failure()?;
        let blocks = self.blocks.load();
        Ok(claim_one(blocks.as_slice()))
    }

    /// Exhaustively recheck and then prepare enough capacity for one claim.
    ///
    /// The caller holds admission serialization, which serializes growers and
    /// makes the prepared-capacity update atomic with the slow claim path.
    pub(super) fn acquire_or_grow_one(
        &self,
        prepared: &mut usize,
    ) -> Result<CarrierAllocation, AllocError> {
        let mut state = self.state.lock();
        loop {
            if let Some(allocation) = claim_one(&state.blocks) {
                return Ok(allocation);
            }

            let block = if let Some(block) = state
                .blocks
                .iter()
                .find(|block| !block.slot.is_mapped())
                .cloned()
            {
                block
            } else {
                self.create_block(&mut state)?
            };

            block.slot.revive(prepared).map_err(map_revive_error)?;
            if !state
                .blocks
                .iter()
                .any(|existing| Arc::ptr_eq(&existing.slot, &block.slot))
            {
                state.blocks.push(block);
                self.blocks.publish(&state.blocks);
            }
            // A lock-free claimant may consume freshly prepared capacity
            // before this grower. Recheck all blocks and continue growing for
            // the remaining deficit instead of reporting a false failure.
        }
    }

    /// Confirm one free overflow trim under admission serialization.
    pub(super) fn start_trim_excess(
        &self,
        prepared: &mut usize,
        floor: usize,
    ) -> Option<TrimCleanup> {
        let blocks = self.blocks.load();
        for block in blocks
            .as_slice()
            .iter()
            .filter(|block| block.kind == BlockKind::Overflow)
        {
            if *prepared <= floor {
                break;
            }
            if let Ok(cleanup) = BlockSlot::start_trim(&block.slot, prepared, floor) {
                return Some(TrimCleanup(cleanup));
            }
        }
        None
    }

    /// Inject a physical acquisition failure after `successes` allocations.
    pub(super) fn fail_after_successes(&self, successes: usize) {
        #[cfg(test)]
        {
            self.state.lock().fail_after_successes = Some(successes);
        }
        #[cfg(not(test))]
        {
            let _ = successes;
        }
    }

    /// Capture physical counters without serializing bitmap mutations.
    pub(super) fn snapshot(&self) -> ArenaSnapshot {
        let blocks = self.blocks.load();
        let mut snapshot = ArenaSnapshot {
            retained: 0,
            free_retained: 0,
            overflow: 0,
            quarantined: 0,
            physical_live: 0,
        };

        for block in blocks.as_slice() {
            let live = block.slot.live_carriers();
            snapshot.physical_live += live;
            if block.slot.is_quarantined() {
                snapshot.quarantined += block.slot.carrier_count();
            }
            if !block.slot.is_mapped() {
                continue;
            }
            match block.kind {
                BlockKind::Retained => {
                    snapshot.retained += block.slot.carrier_count();
                    if block.slot.is_prepared() {
                        snapshot.free_retained += block.slot.carrier_count() - live;
                    }
                }
                BlockKind::Overflow => {
                    snapshot.overflow += block.slot.carrier_count();
                }
            }
        }
        snapshot
    }

    fn create_block(&self, state: &mut ArenaState) -> Result<ArenaBlock, AllocError> {
        let (kind, carrier_count) = if state.retained_geometry < self.retained_limit {
            let remaining = self.retained_limit - state.retained_geometry;
            (BlockKind::Retained, remaining.min(DEFAULT_BLOCK_CARRIERS))
        } else {
            // Overflow is prepared only for the immediate deficit. This keeps
            // soft-cap overage granular even when retained blocks are larger.
            (BlockKind::Overflow, 1)
        };
        let slot_id = state.next_slot;
        state.next_slot = state
            .next_slot
            .checked_add(1)
            .ok_or(AllocError::CapacityOverflow)?;
        let slot = Arc::new(
            BlockSlot::new(slot_id, self.carrier_size, carrier_count).map_err(map_revive_error)?,
        );
        if kind == BlockKind::Retained {
            state.retained_geometry += carrier_count;
        }
        Ok(ArenaBlock { slot, kind })
    }

    fn check_injected_failure(&self) -> Result<(), AllocError> {
        #[cfg(test)]
        {
            let mut state = self.state.lock();
            if let Some(successes) = state.fail_after_successes {
                if successes == 0 {
                    state.fail_after_successes = None;
                    return Err(AllocError::PhysicalAllocationFailed);
                }
                state.fail_after_successes = Some(successes - 1);
            }
        }
        Ok(())
    }
}

fn claim_one(blocks: &[ArenaBlock]) -> Option<CarrierAllocation> {
    blocks.iter().find_map(claim_block)
}

fn claim_block(block: &ArenaBlock) -> Option<CarrierAllocation> {
    let claimed = BlockSlot::try_claim(&block.slot, 1)?
        .into_carriers()
        .pop()
        .expect("a successful one-carrier claim owns one bit");
    let source = match block.kind {
        BlockKind::Retained => AcquisitionSource::Reused,
        BlockKind::Overflow => AcquisitionSource::Overflow,
    };
    Some(CarrierAllocation::new(claimed, source))
}

fn map_revive_error(error: ReviveError) -> AllocError {
    match error {
        ReviveError::AlreadyPrepared => AllocError::PhysicalAllocationFailed,
        ReviveError::CapacityOverflow => AllocError::CapacityOverflow,
    }
}
