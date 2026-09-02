/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Immutable byte streams with independent presentation and owner boundaries.
//!
//! A presentation segment is one range that [`Buf::chunk`] or
//! [`Buf::chunks_vectored`] may expose. An owner boundary records how long one
//! pooled carrier or opaque [`Bytes`] value must remain live. Several adjacent
//! owners may share one presentation segment, so vectored I/O sees fewer
//! ranges while [`Buf::advance`] can still release crossed owners promptly.
//!
//! Coalescing is a pointer-provenance operation, not an address-only
//! optimization. Pooled ranges merge only when they derive from the same
//! concrete block slot. Opaque views merge only after a pool-aware builder
//! classifies their complete range and derives a new pointer from that slot.

use std::collections::VecDeque;
use std::io::IoSlice;
use std::ptr::NonNull;

use bytes::{Buf, Bytes, BytesMut};

use super::acquisition::CarrierGuard;
use super::block::BlockSlot;
use super::invariant_violation;
use super::PoolInner;
use crate::runtime::sync::sync::Arc;

/// Immutable bytes presented as one or more contiguous segments.
///
/// Cloning creates an independent read cursor while sharing the underlying
/// owners. Advancing one clone releases an owner only after all other clones
/// and immutable views have released it.
///
/// [`Self::into_contiguous`] is zero-copy for zero or one remaining segment.
/// Multiple remaining segments are copied in logical order.
#[derive(Clone)]
pub struct SegmentedBytes {
    /// Presentation ranges in logical byte order.
    segments: VecDeque<Segment>,
    /// Consumed bytes within the front segment.
    front_offset: usize,
    /// Bytes remaining from this cursor.
    remaining: usize,
}

impl SegmentedBytes {
    /// Returns the number of bytes remaining from this cursor.
    pub fn len(&self) -> usize {
        self.remaining
    }

    /// Returns whether this cursor has no remaining bytes.
    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    /// Consumes this value and returns one contiguous immutable buffer.
    ///
    /// Empty and single-segment values do not copy. Multiple segments are
    /// copied in logical order while each source owner remains live until its
    /// bytes have been copied.
    pub fn into_contiguous(mut self) -> Bytes {
        match self.segments.len() {
            0 => Bytes::new(),
            1 => {
                let segment = self
                    .segments
                    .pop_front()
                    .unwrap_or_else(|| invariant_violation("single segment disappeared"))
                    .trim_prefix(self.front_offset);
                self.front_offset = 0;
                self.remaining = 0;
                Bytes::from_owner(ContiguousOwner::new(segment))
            }
            _ => {
                let mut contiguous = BytesMut::with_capacity(self.remaining);
                while self.has_remaining() {
                    let copied = self.chunk().len();
                    contiguous.extend_from_slice(self.chunk());
                    self.advance(copied);
                }
                contiguous.freeze()
            }
        }
    }

    /// Constructs one segmented value from its private builder.
    fn from_parts(segments: VecDeque<Segment>, remaining: usize) -> Self {
        let mut total = 0usize;
        for segment in &segments {
            if segment.len == 0 || segment.owners.is_empty() {
                invariant_violation("segment lacks bytes or owner coverage");
            }
            let mut previous = 0usize;
            for owner in &segment.owners {
                if owner.end <= previous || owner.end > segment.len {
                    invariant_violation("segment owner boundaries are not ordered coverage");
                }
                previous = owner.end;
            }
            if previous != segment.len {
                invariant_violation("segment owners do not cover its complete range");
            }
            total = total
                .checked_add(segment.len)
                .unwrap_or_else(|| invariant_violation("segmented byte length overflow"));
        }
        if total != remaining {
            invariant_violation("segment lengths do not match remaining bytes");
        }
        let value = Self {
            segments,
            front_offset: 0,
            remaining,
        };
        if value.remaining == 0 && !value.segments.is_empty() {
            invariant_violation("empty segmented value retained presentation ranges");
        }
        value
    }
}

impl std::fmt::Debug for SegmentedBytes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentedBytes")
            .field("segments", &self.segments.len())
            .field("remaining", &self.remaining)
            .finish()
    }
}

// SAFETY: methods form slices only over immutable initialized ranges retained
// by their owners. No such range overlaps mutable authority.
unsafe impl Send for SegmentedBytes {}

// SAFETY: all shared access is immutable, and each backing owner is safe to
// share through `Arc<CarrierGuard>` or `Bytes`.
unsafe impl Sync for SegmentedBytes {}

impl Buf for SegmentedBytes {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }
        let segment = self
            .segments
            .front()
            .unwrap_or_else(|| invariant_violation("remaining bytes have no front segment"));
        let len = segment
            .len
            .checked_sub(self.front_offset)
            .unwrap_or_else(|| invariant_violation("segment cursor exceeds its range"));
        if len == 0 {
            invariant_violation("remaining bytes have an empty front segment");
        }
        // SAFETY: owners cover this complete immutable initialized range, and
        // `front_offset` is within the segment.
        unsafe { std::slice::from_raw_parts(segment.ptr.as_ptr().add(self.front_offset), len) }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        if self.remaining == 0 || dst.is_empty() {
            return 0;
        }

        let mut written = 0;
        for (index, segment) in self.segments.iter().enumerate() {
            if written == dst.len() {
                break;
            }
            let offset = if index == 0 { self.front_offset } else { 0 };
            let len = segment
                .len
                .checked_sub(offset)
                .unwrap_or_else(|| invariant_violation("segment cursor exceeds its range"));
            if len == 0 {
                invariant_violation("segmented value contains an empty presentation range");
            }
            // SAFETY: the segment's owners retain this immutable initialized
            // range for the borrow of `self`.
            let bytes =
                unsafe { std::slice::from_raw_parts(segment.ptr.as_ptr().add(offset), len) };
            dst[written] = IoSlice::new(bytes);
            written += 1;
        }
        written
    }

    fn advance(&mut self, mut count: usize) {
        assert!(
            count <= self.remaining,
            "advanced beyond segmented byte length"
        );
        while count != 0 {
            let available = self
                .segments
                .front()
                .map(|segment| {
                    segment
                        .len
                        .checked_sub(self.front_offset)
                        .unwrap_or_else(|| invariant_violation("segment cursor exceeds its range"))
                })
                .unwrap_or_else(|| invariant_violation("remaining bytes have no front segment"));
            let advanced = count.min(available);
            self.front_offset += advanced;
            self.remaining -= advanced;
            count -= advanced;

            let segment = self
                .segments
                .front_mut()
                .unwrap_or_else(|| invariant_violation("front segment disappeared"));
            while segment
                .owners
                .front()
                .is_some_and(|owner| owner.end <= self.front_offset)
            {
                segment.owners.pop_front();
            }

            if self.front_offset == segment.len {
                if !segment.owners.is_empty() {
                    invariant_violation("exhausted segment retained owner ranges");
                }
                self.segments.pop_front();
                self.front_offset = 0;
            }
        }

        if self.remaining == 0 && !self.segments.is_empty() {
            invariant_violation("exhausted segmented value retained segments");
        }
    }
}

impl From<Bytes> for SegmentedBytes {
    /// Retains one opaque view without assuming that it belongs to a pool.
    ///
    /// This conversion preserves the input as one presentation segment.
    /// Values assembled directly from pooled storage may coalesce adjacent
    /// ranges while retaining their separate ownership boundaries.
    fn from(bytes: Bytes) -> Self {
        let mut builder = SegmentedBytesBuilder::new();
        builder.push_view(bytes);
        builder.finish()
    }
}

/// One contiguous initialized presentation range.
///
/// `owners` covers this complete range in order. Its boundaries may be finer
/// than the presentation range so consumption can release backing storage
/// without splitting the segment exposed through [`Buf`].
#[derive(Clone)]
struct Segment {
    /// Concrete slot proving common pointer provenance before coalescing.
    slot: Option<Arc<BlockSlot>>,
    /// First presented byte.
    ptr: NonNull<u8>,
    /// Presented byte length.
    len: usize,
    /// Complete ordered owner coverage.
    owners: VecDeque<OwnedRange>,
}

impl Segment {
    /// Removes a consumed prefix after its crossed owners were released.
    fn trim_prefix(mut self, count: usize) -> Self {
        assert!(count <= self.len, "trim exceeds segment");
        if count == 0 {
            return self;
        }
        if count == self.len {
            invariant_violation("complete segment must be removed instead of trimmed");
        }

        if self.owners.is_empty() {
            invariant_violation("remaining segment has no owner");
        }
        if self.owners.front().is_some_and(|owner| owner.end <= count) {
            invariant_violation("trimmed segment retained a consumed owner");
        }
        for owner in &mut self.owners {
            owner.end = owner
                .end
                .checked_sub(count)
                .unwrap_or_else(|| invariant_violation("trim exceeds an owner boundary"));
        }
        // SAFETY: `count` is within the segment retained by the remaining
        // owners.
        self.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(count)) };
        self.len -= count;
        self
    }

    /// Returns the first address after this segment.
    fn end_address(&self) -> Option<usize> {
        self.ptr.as_ptr().addr().checked_add(self.len)
    }

    /// Returns whether a range is adjacent and derives from this exact slot.
    fn can_append(&self, slot: Option<&Arc<BlockSlot>>, start_address: usize) -> bool {
        let (Some(current), Some(incoming)) = (self.slot.as_ref(), slot) else {
            return false;
        };
        Arc::ptr_eq(current, incoming) && self.end_address() == Some(start_address)
    }
}

/// One owner covering bytes through a segment-relative end offset.
#[derive(Clone)]
struct OwnedRange {
    end: usize,
    hold: Hold,
}

/// Storage owner for one immutable subrange.
#[derive(Clone)]
enum Hold {
    /// Direct ownership transferred from a mutable pooled carrier.
    Pooled(Arc<CarrierGuard>),
    /// Existing immutable producer ownership.
    View(Bytes),
}

/// Constructs segmented values while preserving complete owner coverage.
///
/// A pool-aware builder may recognize opaque views produced by that pool. It
/// recovers no return authority from an address: the incoming [`Bytes`] remains
/// the owner, while classification supplies only a slot-rooted pointer suitable
/// for safe coalescing.
pub(super) struct SegmentedBytesBuilder {
    /// Optional pool used to recover canonical pointers for opaque views.
    pool: Option<Arc<PoolInner>>,
    /// Presentation ranges assembled so far.
    segments: VecDeque<Segment>,
    /// Sum of assembled presentation lengths.
    remaining: usize,
}

impl SegmentedBytesBuilder {
    /// Creates an empty builder.
    pub(super) fn new() -> Self {
        Self {
            pool: None,
            segments: VecDeque::new(),
            remaining: 0,
        }
    }

    /// Creates a builder that recognizes opaque views backed by `pool`.
    pub(super) fn for_pool(pool: Arc<PoolInner>) -> Self {
        Self {
            pool: Some(pool),
            segments: VecDeque::new(),
            remaining: 0,
        }
    }

    /// Appends one initialized pooled range.
    pub(super) fn push_pooled(&mut self, ptr: NonNull<u8>, len: usize, guard: Arc<CarrierGuard>) {
        let slot = Arc::clone(guard.slot());
        self.push_range(Some(slot), ptr, len, Hold::Pooled(guard));
    }

    /// Appends the unconsumed ranges and existing owners from `buffer`.
    pub(super) fn push_segmented(&mut self, buffer: SegmentedBytes) {
        let SegmentedBytes {
            mut segments,
            front_offset,
            remaining,
        } = buffer;
        if remaining == 0 {
            return;
        }
        let previous = self.remaining;
        let front = segments
            .pop_front()
            .unwrap_or_else(|| invariant_violation("remaining bytes have no front segment"))
            .trim_prefix(front_offset);
        self.push_segment(front);
        for segment in segments {
            self.push_segment(segment);
        }
        if self.remaining.checked_sub(previous) != Some(remaining) {
            invariant_violation("transferred segments changed remaining byte length");
        }
    }

    /// Appends one immutable view without recovering pool return authority.
    ///
    /// A pool-aware builder may classify the range and re-derive its pointer
    /// from the concrete slot. The view remains the initialized owner.
    pub(super) fn push_view(&mut self, view: Bytes) {
        if view.is_empty() {
            return;
        }
        let classified = self
            .pool
            .as_ref()
            .and_then(|pool| pool.arena.classify_range(view.as_ptr().addr(), view.len()));
        let (slot, ptr) = match classified {
            Some(classified) => {
                let slot = Arc::clone(classified.slot());
                #[cfg(debug_assertions)]
                slot.debug_assert_immutable_range_live(classified.offset(), view.len());
                // SAFETY: `view` retains initialized immutable access to this
                // complete classified range. Pool-produced byte owners keep
                // the carrier bits live until their final clone drops.
                let ptr = unsafe {
                    slot.ptr_for_immutable_range(classified.offset(), view.len())
                        .unwrap_or_else(|| {
                            invariant_violation("classified view is outside its block slot")
                        })
                };
                (Some(slot), ptr)
            }
            None => {
                let ptr = NonNull::new(view.as_ptr().cast_mut())
                    .unwrap_or_else(|| invariant_violation("nonempty Bytes has a null pointer"));
                (None, ptr)
            }
        };
        self.push_range(slot, ptr, view.len(), Hold::View(view));
    }

    /// Finishes one segmented value.
    pub(super) fn finish(self) -> SegmentedBytes {
        SegmentedBytes::from_parts(self.segments, self.remaining)
    }

    /// Appends one range and coalesces only proven slot-local adjacency.
    fn push_range(
        &mut self,
        slot: Option<Arc<BlockSlot>>,
        ptr: NonNull<u8>,
        len: usize,
        hold: Hold,
    ) {
        if len == 0 {
            invariant_violation("segmented range must be nonempty");
        }
        self.remaining = self
            .remaining
            .checked_add(len)
            .unwrap_or_else(|| invariant_violation("segmented byte length overflow"));

        let can_merge = self
            .segments
            .back()
            .is_some_and(|segment| segment.can_append(slot.as_ref(), ptr.as_ptr().addr()));
        if can_merge {
            let segment = self
                .segments
                .back_mut()
                .unwrap_or_else(|| invariant_violation("merge target disappeared"));
            segment.len = segment
                .len
                .checked_add(len)
                .unwrap_or_else(|| invariant_violation("segment length overflow"));
            segment.owners.push_back(OwnedRange {
                end: segment.len,
                hold,
            });
            return;
        }

        let mut owners = VecDeque::new();
        owners.push_back(OwnedRange { end: len, hold });
        self.segments.push_back(Segment {
            slot,
            ptr,
            len,
            owners,
        });
    }

    /// Appends one existing segment and preserves its owner boundaries.
    fn push_segment(&mut self, segment: Segment) {
        if segment.len == 0 || segment.owners.is_empty() {
            invariant_violation("existing segment lacks bytes or owners");
        }
        self.remaining = self
            .remaining
            .checked_add(segment.len)
            .unwrap_or_else(|| invariant_violation("segmented byte length overflow"));

        let can_merge = self.segments.back().is_some_and(|previous| {
            previous.can_append(segment.slot.as_ref(), segment.ptr.as_ptr().addr())
        });
        if can_merge {
            let previous = self
                .segments
                .back_mut()
                .unwrap_or_else(|| invariant_violation("merge target disappeared"));
            let base = previous.len;
            previous.len = previous
                .len
                .checked_add(segment.len)
                .unwrap_or_else(|| invariant_violation("segment length overflow"));
            previous
                .owners
                .extend(segment.owners.into_iter().map(|mut owner| {
                    owner.end = owner
                        .end
                        .checked_add(base)
                        .unwrap_or_else(|| invariant_violation("owner boundary overflow"));
                    owner
                }));
        } else {
            self.segments.push_back(segment);
        }
    }
}

/// Owner used by the zero-copy one-segment conversion.
struct ContiguousOwner {
    ptr: NonNull<u8>,
    len: usize,
    owners: VecDeque<OwnedRange>,
}

impl ContiguousOwner {
    /// Takes complete ownership coverage from one remaining segment.
    fn new(segment: Segment) -> Self {
        Self {
            ptr: segment.ptr,
            len: segment.len,
            owners: segment.owners,
        }
    }
}

impl AsRef<[u8]> for ContiguousOwner {
    fn as_ref(&self) -> &[u8] {
        let _owners = &self.owners;
        // SAFETY: `owners` retain every byte in this immutable initialized
        // range for the lifetime of this owner.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

// SAFETY: the pointer names immutable initialized storage retained by owners.
unsafe impl Send for ContiguousOwner {}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use bytes::Buf;

    use super::super::test_util::{test_pool, write_pooled};
    use super::*;

    #[test]
    fn test_freeze_returns_wholly_unused_carriers() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut mutable = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut mutable, b"abc");

        let frozen = mutable.freeze();

        assert_eq!(frozen.len(), 3);
        assert_eq!(frozen.chunk(), b"abc");
        assert_eq!(frozen.segments.len(), 1);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_freeze_of_empty_mutable_buffer_returns_every_carrier() {
        let (pool, carrier_size) = test_pool(1, 2);
        let mutable = pool.acquire_unreserved(carrier_size * 2).unwrap();

        let frozen = mutable.freeze();

        assert!(frozen.is_empty());
        assert_eq!(frozen.segments.len(), 0);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_freeze_coalesces_adjacent_carriers_but_preserves_owner_ranges() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);

        let frozen = mutable.freeze();

        assert_eq!(frozen.segments.len(), 1);
        assert_eq!(frozen.segments[0].owners.len(), 2);
        assert_eq!(frozen.chunk(), input.as_slice());
    }

    #[test]
    fn test_freeze_and_published_views_coalesce_and_share_one_final_return() {
        let (pool, carrier_size) = test_pool(1, 1);
        let mut mutable = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut mutable, b"abcdef");
        let published = mutable.publish_prefix(3);
        let frozen = mutable.freeze();

        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&pool.inner));
        builder.push_view(published);
        builder.push_segmented(frozen);
        let mut combined = builder.finish();

        assert_eq!(combined.segments.len(), 1);
        assert_eq!(combined.segments[0].owners.len(), 2);
        assert_eq!(combined.chunk(), b"abcdef");
        combined.advance(3);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        combined.advance(3);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_foreign_adjacent_views_remain_separate_segments() {
        let (pool, _) = test_pool(1, 1);
        let source = Bytes::from_static(b"abcdef");
        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&pool.inner));
        builder.push_view(source.slice(0..3));
        builder.push_view(source.slice(3..6));
        let value = builder.finish();

        assert_eq!(value.segments.len(), 2);
        let mut slices = [IoSlice::new(&[]), IoSlice::new(&[])];
        assert_eq!(value.chunks_vectored(&mut slices), 2);
        assert_eq!(&*slices[0], b"abc");
        assert_eq!(&*slices[1], b"def");
    }

    #[test]
    fn test_pool_backed_views_coalesce_through_slot_rooted_pointer() {
        let (pool, carrier_size) = test_pool(1, 1);
        let mut mutable = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut mutable, b"abcdef");
        let first = mutable.publish_prefix(3);
        let second = mutable.publish_prefix(3);
        drop(mutable);

        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&pool.inner));
        builder.push_view(first);
        builder.push_view(second);
        let mut value = builder.finish();

        assert_eq!(value.segments.len(), 1);
        assert_eq!(value.segments[0].owners.len(), 2);
        assert_eq!(value.chunk(), b"abcdef");
        assert!(pool.inner.arena.select_trim_candidate().is_none());
        value.advance(3);
        assert_eq!(value.chunk(), b"def");
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        value.advance(3);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        assert!(pool.inner.arena.select_trim_candidate().is_some());
    }

    #[test]
    fn test_pool_backed_views_coalesce_across_carriers() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let first = mutable.publish_prefix(carrier_size);
        let second = mutable.publish_prefix(carrier_size);

        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&pool.inner));
        builder.push_view(first);
        builder.push_view(second);
        let mut value = builder.finish();

        assert_eq!(value.segments.len(), 1);
        assert_eq!(value.segments[0].owners.len(), 2);
        assert_eq!(value.chunk(), input);
        value.advance(carrier_size);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        value.advance(carrier_size);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_pool_aware_builder_does_not_classify_another_pools_view() {
        let (left_pool, _) = test_pool(1, 1);
        let (right_pool, carrier_size) = test_pool(1, 1);
        let mut mutable = right_pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut mutable, b"right");
        let view = mutable.publish_prefix(5);

        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&left_pool.inner));
        builder.push_view(view);
        let value = builder.finish();

        assert_eq!(value.segments.len(), 1);
        assert!(value.segments[0].slot.is_none());
        assert_eq!(value.chunk(), b"right");
    }

    #[test]
    fn test_pool_local_slot_indices_do_not_authorize_segment_merge() {
        let (left_pool, carrier_size) = test_pool(2, 1);
        let (right_pool, _) = test_pool(2, 1);
        let mut left = left_pool.acquire_unreserved(carrier_size).unwrap();
        let mut right = right_pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut left, &vec![0x11; carrier_size]);
        write_pooled(&mut right, &vec![0x22; carrier_size]);
        let left = left.freeze();
        let right = right.freeze();
        let left_segment = &left.segments[0];
        let right_segment = &right.segments[0];
        let left_slot = left_segment.slot.as_ref().unwrap();
        let right_slot = right_segment.slot.as_ref().unwrap();

        assert_eq!(left_slot.id(), right_slot.id());
        assert!(!Arc::ptr_eq(left_slot, right_slot));

        // Supply exact address adjacency without depending on mmap placement.
        let adjacent_start = left_segment.end_address().unwrap();
        assert!(left_segment.can_append(Some(left_slot), adjacent_start));
        assert!(!left_segment.can_append(Some(left_slot), adjacent_start + 1));
        assert!(!left_segment.can_append(Some(right_slot), adjacent_start));
    }

    #[test]
    fn test_same_slot_nonadjacent_ranges_remain_separate_segments() {
        let (pool, carrier_size) = test_pool(3, 3);
        let mut left = pool.acquire_unreserved(carrier_size).unwrap();
        let gap = pool.acquire_unreserved(carrier_size).unwrap();
        let mut right = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut left, &vec![0x11; carrier_size]);
        write_pooled(&mut right, &vec![0x22; carrier_size]);
        let left = left.freeze();
        let right = right.freeze();

        let left_segment = &left.segments[0];
        let right_segment = &right.segments[0];
        assert!(Arc::ptr_eq(
            left_segment.slot.as_ref().unwrap(),
            right_segment.slot.as_ref().unwrap()
        ));
        assert_ne!(
            left_segment.end_address(),
            Some(right_segment.ptr.as_ptr().addr())
        );

        let mut builder = SegmentedBytesBuilder::new();
        builder.push_segmented(left);
        builder.push_segmented(right);
        let combined = builder.finish();

        assert_eq!(combined.segments.len(), 2);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 3) as u64
        );
        drop(gap);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 2) as u64
        );
        drop(combined);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_push_segmented_rejects_forced_cross_pool_adjacency() {
        let (left_pool, carrier_size) = test_pool(1, 1);
        let (right_pool, _) = test_pool(1, 1);
        let mut left = left_pool.acquire_unreserved(carrier_size).unwrap();
        let mut right = right_pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut left, &vec![0x11; carrier_size]);
        write_pooled(&mut right, &vec![0x22; carrier_size]);
        let left = left.freeze();
        let mut right = right.freeze();

        let adjacent_start = left.segments[0].end_address().unwrap();
        let right_segment = &mut right.segments[0];
        let right_slot = Arc::clone(right_segment.slot.as_ref().unwrap());
        // Preserve the right slot's pointer provenance while forcing the
        // address comparison that previously merged equal pool-local indices.
        // The synthetic pointer is never dereferenced.
        let adjacent = right_segment.ptr.as_ptr().map_addr(|_| adjacent_start);
        right_segment.ptr = NonNull::new(adjacent)
            .unwrap_or_else(|| invariant_violation("adjacent address is null"));

        let mut builder = SegmentedBytesBuilder::new();
        builder.push_segmented(left);
        builder.push_segmented(right);
        let combined = builder.finish();

        assert_eq!(combined.segments.len(), 2);
        let left_slot = combined.segments[0].slot.as_ref().unwrap();
        assert!(!Arc::ptr_eq(left_slot, &right_slot));
        assert!(Arc::ptr_eq(
            combined.segments[1].slot.as_ref().unwrap(),
            &right_slot
        ));
    }

    #[test]
    fn test_advance_releases_crossed_carrier_owners() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input = vec![0x5a; carrier_size * 2];
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();

        frozen.advance(carrier_size);

        assert_eq!(frozen.len(), carrier_size);
        assert_eq!(frozen.chunk(), &input[carrier_size..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_one_advance_crosses_owner_and_segment_boundaries() {
        let (pool, carrier_size) = test_pool(2, 3);
        let input: Vec<u8> = (0..carrier_size * 3)
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        assert_eq!(frozen.segments.len(), 2);
        assert_eq!(frozen.segments[0].owners.len(), 2);

        let advanced = carrier_size * 2 + 7;
        frozen.advance(advanced);

        assert_eq!(frozen.len(), carrier_size - 7);
        assert_eq!(frozen.chunk(), &input[advanced..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_clones_advance_independently_and_retain_crossed_owners() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut mutable = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut mutable, &vec![0x33; carrier_size * 2]);
        let mut first = mutable.freeze();
        let second = first.clone();

        first.advance(carrier_size);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 2) as u64
        );
        drop(second);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(first);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_chunks_vectored_respects_destination_and_partial_front() {
        let (pool, carrier_size) = test_pool(1, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        frozen.advance(carrier_size - 2);

        let mut empty = [];
        assert_eq!(frozen.chunks_vectored(&mut empty), 0);

        let mut one = [IoSlice::new(&[])];
        assert_eq!(frozen.chunks_vectored(&mut one), 1);
        assert_eq!(&*one[0], &input[carrier_size - 2..carrier_size]);

        let mut two = [IoSlice::new(&[]), IoSlice::new(&[])];
        assert_eq!(frozen.chunks_vectored(&mut two), 2);
        assert_eq!(&*two[0], &input[carrier_size - 2..carrier_size]);
        assert_eq!(&*two[1], &input[carrier_size..]);
    }

    #[test]
    fn test_buf_copy_to_bytes_releases_crossed_pooled_owner() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index.wrapping_mul(41) % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        let copied_len = carrier_size + 3;

        let copied = frozen.copy_to_bytes(copied_len);

        assert_eq!(copied, input[..copied_len]);
        assert_eq!(frozen.chunk(), &input[copied_len..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_buf_get_u32_crosses_pooled_segment_boundary() {
        let (pool, carrier_size) = test_pool(1, 2);
        let mut input = vec![0; carrier_size + 2];
        input[carrier_size - 2..].copy_from_slice(&[0x01, 0x23, 0x45, 0x67]);
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        frozen.advance(carrier_size - 2);

        let value = frozen.get_u32();

        assert_eq!(value, 0x0123_4567);
        assert!(frozen.is_empty());
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_one_segment_contiguous_conversion_is_zero_copy() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input = vec![0x6b; carrier_size * 2];
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let frozen = mutable.freeze();
        let source_ptr = frozen.chunk().as_ptr();

        let contiguous = frozen.into_contiguous();

        assert_eq!(contiguous.as_ptr(), source_ptr);
        assert_eq!(contiguous, input);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 2) as u64
        );
        drop(contiguous);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_partially_consumed_one_segment_conversion_is_zero_copy() {
        let (pool, carrier_size) = test_pool(2, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index.wrapping_mul(13) % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        let advanced = carrier_size + 7;
        frozen.advance(advanced);
        let source_ptr = frozen.chunk().as_ptr();
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);

        let contiguous = frozen.into_contiguous();

        assert_eq!(contiguous.as_ptr(), source_ptr);
        assert_eq!(contiguous, input[advanced..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(contiguous);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_multiple_segment_contiguous_conversion_copies_and_releases_pool() {
        let (pool, carrier_size) = test_pool(1, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let frozen = mutable.freeze();

        let contiguous = frozen.into_contiguous();

        assert_eq!(contiguous, input);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_partially_consumed_multiple_segment_conversion_copies_remaining_bytes() {
        let (pool, carrier_size) = test_pool(1, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index.wrapping_mul(23) % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        let advanced = carrier_size - 3;
        frozen.advance(advanced);
        assert_eq!(frozen.segments.len(), 2);

        let contiguous = frozen.into_contiguous();

        assert_eq!(contiguous, input[advanced..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_from_bytes_and_single_segment_conversion_preserve_owner() {
        let source = Bytes::from_static(b"foreign");
        let source_ptr = source.as_ptr();
        let segmented = SegmentedBytes::from(source);

        assert_eq!(segmented.len(), 7);
        let contiguous = segmented.into_contiguous();
        assert_eq!(contiguous.as_ptr(), source_ptr);
        assert_eq!(contiguous, b"foreign"[..]);
    }

    #[test]
    fn test_builder_appends_partially_consumed_segmented_values() {
        let mut first = SegmentedBytes::from(Bytes::from_static(b"abcd"));
        first.advance(2);
        let second = SegmentedBytes::from(Bytes::from_static(b"efgh"));
        let mut builder = SegmentedBytesBuilder::new();
        builder.push_segmented(first);
        builder.push_segmented(second);
        let mut combined = builder.finish();

        assert_eq!(combined.len(), 6);
        assert_eq!(combined.copy_to_bytes(6), b"cdefgh"[..]);
    }

    #[test]
    fn test_segmented_read_and_conversion_match_bytes_for_varied_partitions() {
        let input: Vec<u8> = (0usize..521)
            .map(|index| (index.wrapping_mul(29) % 251) as u8)
            .collect();

        for width in [1, 2, 7, 64, 257, 521] {
            let mut builder = SegmentedBytesBuilder::new();
            for chunk in input.chunks(width) {
                builder.push_view(Bytes::copy_from_slice(chunk));
            }
            let value = builder.finish();

            for step in [1, 3, 31, 128, 1024] {
                let mut cursor = value.clone();
                let mut consumed = 0;
                while cursor.has_remaining() {
                    assert!(!cursor.chunk().is_empty());
                    let advanced = step.min(cursor.remaining());
                    cursor.advance(advanced);
                    consumed += advanced;
                    assert_eq!(cursor.remaining(), input.len() - consumed);
                }
                assert_eq!(consumed, input.len());
            }

            let contiguous = value.into_contiguous();
            assert_eq!(contiguous, input);
        }
    }

    #[test]
    fn test_empty_segmented_value_obeys_buf_contract() {
        let mut value = SegmentedBytes::from(Bytes::new());
        assert!(value.is_empty());
        assert_eq!(value.remaining(), 0);
        assert!(value.chunk().is_empty());
        value.advance(0);
        let mut slices = [IoSlice::new(&[])];
        assert_eq!(value.chunks_vectored(&mut slices), 0);
        assert!(value.into_contiguous().is_empty());
    }

    #[test]
    #[should_panic(expected = "segment owners do not cover its complete range")]
    fn test_segmented_bytes_rejects_incomplete_owner_coverage() {
        let source = Bytes::from_static(b"abcd");
        let ptr = NonNull::new(source.as_ptr().cast_mut()).expect("static bytes are nonempty");
        let segment = Segment {
            slot: None,
            ptr,
            len: source.len(),
            owners: VecDeque::from([OwnedRange {
                end: source.len() - 1,
                hold: Hold::View(source),
            }]),
        };

        let _ = SegmentedBytes::from_parts(VecDeque::from([segment]), 4);
    }

    #[test]
    #[should_panic(expected = "segment owner boundaries are not ordered coverage")]
    fn test_segmented_bytes_rejects_unordered_owner_boundaries() {
        let source = Bytes::from_static(b"abcd");
        let ptr = NonNull::new(source.as_ptr().cast_mut()).expect("static bytes are nonempty");
        let segment = Segment {
            slot: None,
            ptr,
            len: source.len(),
            owners: VecDeque::from([
                OwnedRange {
                    end: 3,
                    hold: Hold::View(source.clone()),
                },
                OwnedRange {
                    end: 2,
                    hold: Hold::View(source),
                },
            ]),
        };

        let _ = SegmentedBytes::from_parts(VecDeque::from([segment]), 4);
    }

    #[test]
    #[should_panic(expected = "segment lengths do not match remaining bytes")]
    fn test_segmented_bytes_rejects_remaining_length_mismatch() {
        let source = Bytes::from_static(b"abcd");
        let ptr = NonNull::new(source.as_ptr().cast_mut()).expect("static bytes are nonempty");
        let segment = Segment {
            slot: None,
            ptr,
            len: source.len(),
            owners: VecDeque::from([OwnedRange {
                end: source.len(),
                hold: Hold::View(source),
            }]),
        };

        let _ = SegmentedBytes::from_parts(VecDeque::from([segment]), 3);
    }

    #[test]
    #[should_panic(expected = "trimmed segment retained a consumed owner")]
    fn test_segment_trim_rejects_a_retained_consumed_owner() {
        let source = Bytes::from_static(b"abcd");
        let ptr = NonNull::new(source.as_ptr().cast_mut()).expect("static bytes are nonempty");
        let segment = Segment {
            slot: None,
            ptr,
            len: source.len(),
            owners: VecDeque::from([
                OwnedRange {
                    end: 2,
                    hold: Hold::View(source.clone()),
                },
                OwnedRange {
                    end: source.len(),
                    hold: Hold::View(source),
                },
            ]),
        };

        let _ = segment.trim_prefix(2);
    }

    #[test]
    #[should_panic(expected = "advanced beyond segmented byte length")]
    fn test_segmented_bytes_rejects_advance_beyond_remaining() {
        let mut value = SegmentedBytes::from(Bytes::from_static(b"abc"));
        value.advance(4);
    }

    #[test]
    fn test_segmented_bytes_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SegmentedBytes>();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::test_util::{test_single_carrier_pool as test_pool, write_pooled};
    use crate::runtime::sync::thread;

    #[test]
    fn test_concurrent_frozen_clone_drops_return_one_carrier_once() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let mut mutable = pool.acquire_unreserved(carrier_size).unwrap();
            write_pooled(&mut mutable, b"x");
            let first = mutable.freeze();
            let second = first.clone();

            let left = thread::spawn(move || drop(first));
            let right = thread::spawn(move || drop(second));
            left.join().unwrap();
            right.join().unwrap();

            assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        });
    }

    #[test]
    fn test_published_and_frozen_drop_race_returns_one_carrier_once() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let mut mutable = pool.acquire_unreserved(carrier_size).unwrap();
            write_pooled(&mut mutable, b"xy");
            let published = mutable.publish_prefix(1);
            let frozen = mutable.freeze();

            let publishing = thread::spawn(move || drop(published));
            let freezing = thread::spawn(move || drop(frozen));
            publishing.join().unwrap();
            freezing.join().unwrap();

            assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        });
    }
}
