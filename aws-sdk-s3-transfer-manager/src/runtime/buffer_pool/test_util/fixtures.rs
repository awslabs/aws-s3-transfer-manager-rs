/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Wakers and byte initialization helpers for tests.

use std::sync::Arc as StdArc;
use std::task::{Wake, Waker};

use bytes::BufMut;

use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::runtime::sync::sync::Arc;

use super::super::block::BlockSlot;
use super::super::{BufferPool, CarrierCount, PooledBufMut};

struct CountingWake {
    count: Arc<AtomicUsize>,
}

impl Wake for CountingWake {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }
}

pub(in crate::runtime::buffer_pool) struct ClaimingWakeState {
    wakes: AtomicUsize,
    claimed: AtomicBool,
}

impl ClaimingWakeState {
    pub(in crate::runtime::buffer_pool) fn wakes(&self) -> usize {
        self.wakes.load(Ordering::Acquire)
    }

    pub(in crate::runtime::buffer_pool) fn claimed(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }
}

enum ClaimTarget {
    Pool(BufferPool),
    Slot(Arc<BlockSlot>),
}

struct ClaimingWake {
    target: ClaimTarget,
    state: Arc<ClaimingWakeState>,
}

impl Wake for ClaimingWake {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        let claimed = match &self.target {
            ClaimTarget::Pool(pool) => pool
                .inner
                .arena
                .claim_optimistic(CarrierCount::new(1))
                .expect("wake-time claim")
                .is_complete(),
            ClaimTarget::Slot(slot) => BlockSlot::try_claim_exhaustive(slot, CarrierCount::new(1))
                .expect("wake-time slot claim")
                .is_some(),
        };
        self.state.claimed.store(claimed, Ordering::Release);
        self.state.wakes.fetch_add(1, Ordering::Release);
    }
}

/// Constructs a waker and its shared wake counter.
pub(in crate::runtime::buffer_pool) fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let state = CountingWake {
        count: Arc::clone(&count),
    };
    (Waker::from(StdArc::new(state)), count)
}

/// Constructs a waker that verifies one carrier is reusable on notification.
pub(in crate::runtime::buffer_pool) fn claiming_waker(
    pool: BufferPool,
) -> (Waker, Arc<ClaimingWakeState>) {
    let state = Arc::new(ClaimingWakeState {
        wakes: AtomicUsize::new(0),
        claimed: AtomicBool::new(false),
    });
    let waker = ClaimingWake {
        target: ClaimTarget::Pool(pool),
        state: Arc::clone(&state),
    };
    (Waker::from(StdArc::new(waker)), state)
}

/// Constructs a waker that verifies one exact slot is reusable on notification.
pub(in crate::runtime::buffer_pool) fn slot_claiming_waker(
    slot: Arc<BlockSlot>,
) -> (Waker, Arc<ClaimingWakeState>) {
    let state = Arc::new(ClaimingWakeState {
        wakes: AtomicUsize::new(0),
        claimed: AtomicBool::new(false),
    });
    let waker = ClaimingWake {
        target: ClaimTarget::Slot(slot),
        state: Arc::clone(&state),
    };
    (Waker::from(StdArc::new(waker)), state)
}

/// Loads a counting waker's observed wake count.
pub(in crate::runtime::buffer_pool) fn wake_count(count: &AtomicUsize) -> usize {
    count.load(Ordering::Acquire)
}

/// Initializes bytes through the pooled buffer's `BufMut` boundary.
pub(in crate::runtime::buffer_pool) fn write_pooled(buffer: &mut PooledBufMut, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let written = {
            let chunk = buffer.chunk_mut();
            let written = chunk.len().min(bytes.len());
            chunk[..written].copy_from_slice(&bytes[..written]);
            written
        };
        // SAFETY: the preceding copy initialized exactly `written` bytes.
        unsafe { buffer.advance_mut(written) };
        bytes = &bytes[written..];
    }
}
