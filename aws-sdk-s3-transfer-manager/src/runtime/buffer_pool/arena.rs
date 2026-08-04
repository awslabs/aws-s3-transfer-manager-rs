/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Physical carrier acquisition and return.
//!
//! The arena owns address-stable carrier storage. Acquisition reuses a retained
//! carrier, grows retained storage up to its target, then creates temporary
//! overflow. Return makes retained storage reusable and destroys overflow.
//!
//! Block mapping, incarnation, topology, and trim belong behind this boundary.
//! The heap-backed implementation keeps the ownership and accounting paths
//! executable without exposing those mechanics to the pool facade.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::Mutex;

use super::AllocError;

/// Stable identity of one carrier in the physical backend.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct CarrierId(u64);

/// Physical source selected for a carrier acquisition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AcquisitionSource {
    Reused,
    RetainedGrowth,
    Overflow,
}

/// Exclusive physical allocation transferred into a writable carrier.
pub(super) struct CarrierAllocation {
    pub(super) id: CarrierId,
    pub(super) ptr: NonNull<MaybeUninit<u8>>,
    pub(super) capacity: usize,
    pub(super) source: AcquisitionSource,
}

/// Retained-carrier store and overflow allocator.
pub(super) struct Arena {
    carrier_size: usize,
    retained_limit: usize,
    state: Mutex<ArenaState>,
}

/// Mutable arena state protected by [`Arena::state`].
struct ArenaState {
    carriers: HashMap<CarrierId, CarrierSlot>,
    next_id: u64,
    fail_after_successes: Option<usize>,
}

/// Storage and checkout state for one carrier identity.
struct CarrierSlot {
    storage: Box<CarrierStorage>,
    kind: CarrierKind,
    checked_out: bool,
}

/// Retention policy applied when the final guard returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CarrierKind {
    Retained,
    Overflow,
}

/// Address-stable bytes owned by the arena.
struct CarrierStorage {
    data: Box<[UnsafeCell<MaybeUninit<u8>>]>,
}

// SAFETY: access is restricted to linear buffer::ExclusiveRange values and
// immutable buffer::PooledWindow values. Those ranges are disjoint by
// construction, and CarrierGuard prevents checkout reuse while either exists.
unsafe impl Send for CarrierStorage {}
unsafe impl Sync for CarrierStorage {}

/// Physical counters sampled under the arena lock.
pub(super) struct ArenaSnapshot {
    pub(super) retained: usize,
    pub(super) free_retained: usize,
    pub(super) overflow: usize,
    pub(super) physical_live: usize,
}

impl Arena {
    /// Create an arena that retains at most `retained_limit` carriers.
    pub(super) fn new(carrier_size: usize, retained_limit: usize) -> Self {
        Self {
            carrier_size,
            retained_limit,
            state: Mutex::new(ArenaState {
                carriers: HashMap::new(),
                next_id: 0,
                fail_after_successes: None,
            }),
        }
    }

    /// Fixed capacity of every carrier.
    pub(super) fn carrier_size(&self) -> usize {
        self.carrier_size
    }

    /// Reuse, grow retained storage, or allocate temporary overflow.
    pub(super) fn acquire_one(&self) -> Result<CarrierAllocation, AllocError> {
        let mut state = self.state.lock().unwrap();
        if let Some(successes) = state.fail_after_successes {
            if successes == 0 {
                state.fail_after_successes = None;
                return Err(AllocError::PhysicalAllocationFailed);
            }
            state.fail_after_successes = Some(successes - 1);
        }

        if let Some((id, slot)) = state
            .carriers
            .iter_mut()
            .find(|(_, slot)| slot.kind == CarrierKind::Retained && !slot.checked_out)
        {
            slot.checked_out = true;
            return Ok(CarrierAllocation {
                id: *id,
                ptr: slot.storage.ptr(),
                capacity: self.carrier_size,
                source: AcquisitionSource::Reused,
            });
        }

        let retained = state
            .carriers
            .values()
            .filter(|slot| slot.kind == CarrierKind::Retained)
            .count();
        let kind = if retained < self.retained_limit {
            CarrierKind::Retained
        } else {
            CarrierKind::Overflow
        };
        let source = match kind {
            CarrierKind::Retained => AcquisitionSource::RetainedGrowth,
            CarrierKind::Overflow => AcquisitionSource::Overflow,
        };
        let id = CarrierId(state.next_id);
        state.next_id += 1;
        let slot = CarrierSlot {
            storage: Box::new(CarrierStorage::new(self.carrier_size)),
            kind,
            checked_out: true,
        };
        let ptr = slot.storage.ptr();
        state.carriers.insert(id, slot);

        Ok(CarrierAllocation {
            id,
            ptr,
            capacity: self.carrier_size,
            source,
        })
    }

    /// Return one checkout and destroy overflow storage.
    pub(super) fn return_carrier(&self, id: CarrierId) {
        let mut state = self.state.lock().unwrap();
        let slot = state
            .carriers
            .get_mut(&id)
            .expect("returned carrier belongs to this arena");
        assert!(slot.checked_out, "carrier returned exactly once");
        slot.checked_out = false;
        if slot.kind == CarrierKind::Overflow {
            state.carriers.remove(&id);
        }
    }

    /// Inject a physical acquisition failure after `successes` allocations.
    pub(super) fn fail_after_successes(&self, successes: usize) {
        self.state.lock().unwrap().fail_after_successes = Some(successes);
    }

    /// Capture physical counters under the arena lock.
    pub(super) fn snapshot(&self) -> ArenaSnapshot {
        let state = self.state.lock().unwrap();
        ArenaSnapshot {
            retained: state
                .carriers
                .values()
                .filter(|slot| slot.kind == CarrierKind::Retained)
                .count(),
            free_retained: state
                .carriers
                .values()
                .filter(|slot| slot.kind == CarrierKind::Retained && !slot.checked_out)
                .count(),
            overflow: state
                .carriers
                .values()
                .filter(|slot| slot.kind == CarrierKind::Overflow)
                .count(),
            physical_live: state
                .carriers
                .values()
                .filter(|slot| slot.checked_out)
                .count(),
        }
    }
}

impl CarrierStorage {
    /// Allocate address-stable uninitialized storage.
    fn new(capacity: usize) -> Self {
        let data = std::iter::repeat_with(|| UnsafeCell::new(MaybeUninit::uninit()))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { data }
    }

    /// Return the stable base pointer for one exclusive checkout.
    fn ptr(&self) -> NonNull<MaybeUninit<u8>> {
        NonNull::new(self.data.as_ptr().cast::<MaybeUninit<u8>>().cast_mut())
            .expect("carrier storage has nonzero capacity")
    }
}
