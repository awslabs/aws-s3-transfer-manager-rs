/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared construction, polling, observation, and failure injection for tests.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc as StdArc;
use std::task::Wake;
use std::task::{Context, Poll, Waker};

use bytes::BufMut;

use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
use crate::runtime::sync::sync::Arc;

use super::admission::{Reservation, ReserveError, ReserveFuture};
use super::geometry::PoolGeometry;
use super::virtual_memory::page_size;
use super::{BufferPool, CarrierCount, PoolInner, PooledBufMut};

/// Waker state that records each wake through the active atomic backend.
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

/// Per-pool hooks that keep parallel and Loom tests isolated.
pub(super) struct TestHooks {
    /// Remaining metadata boundaries before one acquisition failure.
    acquisition_allocation_failure: AtomicUsize,
}

impl TestHooks {
    /// Creates a pool with every test hook disabled.
    pub(super) fn new() -> Self {
        Self {
            acquisition_allocation_failure: AtomicUsize::new(0),
        }
    }

    /// Fails the `boundary`th subsequent acquisition metadata reservation.
    fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        assert!(boundary != 0, "failure boundary must be nonzero");
        self.acquisition_allocation_failure
            .compare_exchange(0, boundary, Ordering::AcqRel, Ordering::Acquire)
            .expect("an acquisition allocation failure is already pending");
    }

    /// Returns whether this metadata boundary consumes the injected failure.
    pub(super) fn take_acquisition_allocation_failure(&self) -> bool {
        self.acquisition_allocation_failure
            .fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |remaining| match remaining {
                    0 => None,
                    1 => Some(0),
                    remaining => Some(remaining - 1),
                },
            )
            .is_ok_and(|previous| previous == 1)
    }
}

impl BufferPool {
    /// Injects one acquisition metadata allocation failure.
    pub(super) fn inject_acquisition_allocation_failure(&self, boundary: usize) {
        self.inner
            .test_hooks
            .inject_acquisition_allocation_failure(boundary);
    }

    /// Returns whether an acquisition metadata failure remains pending.
    pub(super) fn acquisition_allocation_failure_pending(&self) -> bool {
        self.inner
            .test_hooks
            .acquisition_allocation_failure
            .load(Ordering::Acquire)
            != 0
    }
}

impl PoolInner {
    /// Returns one coherent admission and aggregate sample.
    pub(super) fn test_accounting_state(
        &self,
    ) -> (
        CarrierCount,
        CarrierCount,
        CarrierCount,
        CarrierCount,
        usize,
    ) {
        let admission = self.admission.lock();
        let coverage = self.coverage.snapshot();
        (
            admission.ledger.prepared_capacity,
            admission.ledger.active_planned_demand,
            coverage.available,
            coverage.uncovered,
            admission.waiters.len(),
        )
    }
}

/// Constructs a waker and its shared wake counter.
pub(super) fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let state = CountingWake {
        count: Arc::clone(&count),
    };
    (Waker::from(StdArc::new(state)), count)
}

/// Loads a counting waker's observed wake count.
pub(super) fn wake_count(count: &AtomicUsize) -> usize {
    count.load(Ordering::Acquire)
}

/// Constructs a pool with a one-word optimistic scan budget.
pub(super) fn test_pool(block_carriers: usize, configured: usize) -> (BufferPool, usize) {
    test_pool_with_scan(block_carriers, configured, 1)
}

/// Constructs a pool with explicit block and optimistic-scan geometry.
pub(super) fn test_pool_with_scan(
    block_carriers: usize,
    configured: usize,
    optimistic_scan_words: usize,
) -> (BufferPool, usize) {
    let page_size = page_size().unwrap().get();
    let geometry = PoolGeometry::new(
        page_size,
        page_size.checked_mul(block_carriers).unwrap(),
        page_size,
    )
    .unwrap();
    let pool = BufferPool::from_parts(
        geometry,
        CarrierCount::new(configured),
        optimistic_scan_words,
    )
    .unwrap();
    (pool, page_size)
}

/// Constructs a pool whose block and carrier are one runtime page.
pub(super) fn test_single_carrier_pool(configured: usize) -> (BufferPool, usize) {
    test_pool(1, configured)
}

/// Initializes bytes through the pooled buffer's `BufMut` boundary.
pub(super) fn write_pooled(buffer: &mut PooledBufMut, mut bytes: &[u8]) {
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

/// Polls one reservation future with an explicit waker.
pub(super) fn poll_reserve(
    future: &mut ReserveFuture,
    waker: &Waker,
) -> Poll<Result<Reservation, ReserveError>> {
    let mut context = Context::from_waker(waker);
    Pin::new(future).poll(&mut context)
}
