/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Mutable and immutable views over checked-out carriers.
//!
//! Mutable access is represented by a linear [`ExclusiveRange`]. Publishing an
//! initialized prefix consumes mutable authority for that range and transfers
//! it to an owner-backed [`Bytes`]. The unpublished suffix remains writable.
//! Both ranges retain the same carrier guard, so the carrier returns only after
//! the final mutable or immutable owner drops.

use std::collections::VecDeque;
use std::io::IoSlice;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::Arc;

use bytes::{Buf, Bytes};

use super::arena::CarrierAllocation;
use super::CarrierGuard;

/// Exclusive mutable authority over one range in a carrier.
///
/// This type is linear: it is not cloneable, and splitting consumes the
/// original range. Every mutable or immutable view derived from it is therefore
/// disjoint.
struct ExclusiveRange {
    ptr: NonNull<MaybeUninit<u8>>,
    range: Range<usize>,
}

/// One checked-out carrier and its remaining mutable range.
pub(super) struct WritableCarrier {
    guard: Arc<CarrierGuard>,
    writable: Option<ExclusiveRange>,
    initialized: usize,
}

/// Carriers that are physically adjacent in one arena block.
///
/// The heap arena returns one carrier per run. A block arena may preserve
/// adjacent acquisitions in one run without changing the buffer API.
struct CarrierRun {
    carriers: VecDeque<WritableCarrier>,
}

/// Fixed-capacity mutable storage returned by one pool acquisition.
///
/// Writing and publication use separate cursors. A filler may initialize later
/// carriers before an earlier carrier's immutable views are published.
pub(super) struct PooledBufMut {
    runs: Vec<CarrierRun>,
    write_run: usize,
    publish_run: usize,
    initialized: usize,
    capacity: usize,
}

/// Range-limited immutable owner passed to [`Bytes::from_owner`].
///
/// The guard keeps the carrier checked out. `ptr..ptr + len` contains only
/// initialized bytes and never overlaps a live [`ExclusiveRange`].
struct PooledWindow {
    guard: Arc<CarrierGuard>,
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: PooledWindow is immutable. Its range was removed from ExclusiveRange
// before construction, and CarrierGuard prevents reuse of the carrier.
unsafe impl Send for PooledWindow {}
unsafe impl Sync for PooledWindow {}

/// One contiguous immutable region in a segmented output.
#[derive(Clone)]
struct Segment {
    bytes: Bytes,
}

/// Immutable, cloneable output over one or more carrier-backed segments.
///
/// Cloning duplicates only the cursor and `Bytes` handles. Carrier accounting
/// remains attached to the shared guards owned by those handles.
#[derive(Clone)]
pub(super) struct SegmentedBytes {
    segments: VecDeque<Segment>,
    remaining: usize,
}

impl ExclusiveRange {
    fn whole(ptr: NonNull<MaybeUninit<u8>>, capacity: usize) -> Self {
        Self {
            ptr,
            range: 0..capacity,
        }
    }

    fn len(&self) -> usize {
        self.range.len()
    }

    fn as_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: ExclusiveRange is linear. Splitting consumes the parent and
        // creates disjoint children, so this is the only mutable authority for
        // the represented range.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr.as_ptr().add(self.range.start),
                self.range.len(),
            )
        }
    }

    fn split_at(self, offset: usize) -> (Self, Self) {
        assert!(offset <= self.len(), "split exceeds exclusive range");
        let middle = self.range.start + offset;
        (
            Self {
                ptr: self.ptr,
                range: self.range.start..middle,
            },
            Self {
                ptr: self.ptr,
                range: middle..self.range.end,
            },
        )
    }

    fn into_window(self, guard: Arc<CarrierGuard>) -> PooledWindow {
        // SAFETY: the range is within its carrier allocation.
        let ptr = unsafe { self.ptr.as_ptr().add(self.range.start).cast::<u8>() };
        PooledWindow {
            guard,
            ptr: NonNull::new(ptr).expect("carrier window is non-null"),
            len: self.range.len(),
        }
    }
}

impl WritableCarrier {
    pub(super) fn new(allocation: CarrierAllocation, guard: Arc<CarrierGuard>) -> Self {
        let _ = allocation.source;
        Self {
            guard,
            writable: Some(ExclusiveRange::whole(allocation.ptr, allocation.capacity)),
            initialized: 0,
        }
    }

    fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let initialized = self.initialized;
        &mut self
            .writable
            .as_mut()
            .expect("writable carrier retains exclusive authority")
            .as_uninit_mut()[initialized..]
    }

    fn remaining_mut(&self) -> usize {
        self.writable
            .as_ref()
            .expect("writable carrier retains exclusive authority")
            .len()
            - self.initialized
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        assert!(
            count <= self.remaining_mut(),
            "initialized bytes exceed writable capacity"
        );
        self.initialized += count;
    }

    fn publish_prefix(&mut self, count: usize) -> Bytes {
        assert!(count > 0, "cannot publish an empty range");
        assert!(
            count <= self.initialized,
            "cannot publish uninitialized bytes"
        );
        let writable = self
            .writable
            .take()
            .expect("writable carrier retains exclusive authority");
        let (published, remaining) = writable.split_at(count);
        self.writable = Some(remaining);
        self.initialized -= count;
        Bytes::from_owner(published.into_window(Arc::clone(&self.guard)))
    }

    fn freeze(mut self) -> Option<Bytes> {
        if self.initialized == 0 {
            return None;
        }
        let writable = self
            .writable
            .take()
            .expect("writable carrier retains exclusive authority");
        let (initialized, _) = writable.split_at(self.initialized);
        Some(Bytes::from_owner(
            initialized.into_window(Arc::clone(&self.guard)),
        ))
    }
}

impl PooledBufMut {
    pub(super) fn new(carriers: Vec<WritableCarrier>) -> Self {
        let capacity = carriers
            .iter()
            .map(|carrier| {
                carrier
                    .writable
                    .as_ref()
                    .expect("new carrier is writable")
                    .len()
            })
            .sum();
        let runs = carriers
            .into_iter()
            .map(|carrier| CarrierRun {
                carriers: VecDeque::from([carrier]),
            })
            .collect();
        Self {
            runs,
            write_run: 0,
            publish_run: 0,
            initialized: 0,
            capacity,
        }
    }

    /// Total writable capacity acquired from the pool.
    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Initialized bytes that have not been published or consumed by freeze.
    pub(super) fn len(&self) -> usize {
        self.initialized
    }

    /// Uninitialized capacity remaining across all carriers.
    pub(super) fn remaining_mut(&self) -> usize {
        self.runs
            .iter()
            .flat_map(|run| run.carriers.iter())
            .map(WritableCarrier::remaining_mut)
            .sum()
    }

    /// Contiguous uninitialized suffix available to the next write.
    pub(super) fn chunk_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.runs[self.write_run]
            .carriers
            .front_mut()
            .expect("a run contains a carrier")
            .spare_capacity_mut()
    }

    /// Mark the next `count` bytes returned by [`Self::chunk_mut`] initialized.
    ///
    /// # Safety
    ///
    /// The caller must initialize those bytes before calling this method.
    pub(super) unsafe fn advance_mut(&mut self, count: usize) {
        let carrier = self.runs[self.write_run]
            .carriers
            .front_mut()
            .expect("a run contains a carrier");
        // SAFETY: forwarded from the caller.
        unsafe { carrier.advance_mut(count) };
        self.initialized += count;
        if carrier.remaining_mut() == 0 && self.write_run + 1 < self.runs.len() {
            self.write_run += 1;
        }
    }

    /// Publish an initialized prefix of the current publication carrier.
    ///
    /// The returned `Bytes` pins only that carrier. Any disjoint suffix remains
    /// writable through this buffer.
    pub(super) fn publish_prefix(&mut self, count: usize) -> Bytes {
        let carrier = self.runs[self.publish_run]
            .carriers
            .front_mut()
            .expect("a run contains a carrier");
        let bytes = carrier.publish_prefix(count);
        self.initialized -= count;
        if carrier.initialized == 0
            && carrier.remaining_mut() == 0
            && self.publish_run + 1 < self.runs.len()
        {
            self.publish_run += 1;
        }
        bytes
    }

    /// Freeze all initialized bytes and discard unused mutable capacity.
    pub(super) fn freeze(self) -> SegmentedBytes {
        let segments = self
            .runs
            .into_iter()
            .flat_map(|run| run.carriers)
            .filter_map(WritableCarrier::freeze)
            .map(|bytes| Segment { bytes })
            .collect::<VecDeque<_>>();
        SegmentedBytes::new(segments)
    }
}

impl AsRef<[u8]> for PooledWindow {
    fn as_ref(&self) -> &[u8] {
        let _ = &self.guard;
        // SAFETY: construction consumes exclusive authority for this
        // initialized range. Every remaining mutable range is disjoint.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl SegmentedBytes {
    fn new(segments: VecDeque<Segment>) -> Self {
        let remaining = segments.iter().map(|segment| segment.bytes.len()).sum();
        Self {
            segments,
            remaining,
        }
    }

    pub(super) fn first_bytes(&self) -> Option<Bytes> {
        self.segments.front().map(|segment| segment.bytes.clone())
    }
}

impl Buf for SegmentedBytes {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.segments
            .front()
            .map(|segment| segment.bytes.as_ref())
            .unwrap_or_default()
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.remaining, "advance exceeds remaining bytes");
        self.remaining -= count;

        while count > 0 {
            let front = self
                .segments
                .front_mut()
                .expect("remaining bytes imply a front segment");
            if count < front.bytes.len() {
                front.bytes.advance(count);
                return;
            }
            count -= front.bytes.len();
            self.segments.pop_front();
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let count = dst.len().min(self.segments.len());
        for (slot, segment) in dst.iter_mut().zip(&self.segments).take(count) {
            *slot = IoSlice::new(segment.bytes.as_ref());
        }
        count
    }
}
