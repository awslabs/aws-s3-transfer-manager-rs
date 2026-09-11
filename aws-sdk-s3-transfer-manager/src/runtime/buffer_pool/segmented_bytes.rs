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

use super::acquisition::{CarrierGuard, CarrierReturnBatch};
use super::block::BlockSlot;
use super::invariant_violation;
use super::BufferPool;
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

    /// Returns the number of presentation segments remaining from this cursor.
    pub(crate) fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Appends the remaining bytes from `other`.
    ///
    /// This preserves existing owner boundaries and coalesces presentation
    /// segments only when both ranges have the same pooled slot provenance.
    pub fn append(&mut self, other: Self) {
        let current = std::mem::replace(self, Self::empty());
        let mut builder = SegmentedBytesBuilder::new();
        builder.push_segmented(current);
        builder.push_segmented(other);
        *self = builder.finish();
    }

    /// Appends `view` after classifying its complete range against `pool`.
    ///
    /// Classification permits adjacent views from the same concrete block slot
    /// to share one presentation segment. The view retains all lifetime and
    /// accounting ownership; classification does not recover mutable or return
    /// authority. A foreign or otherwise unclassified view remains a separate
    /// segment.
    pub(crate) fn append_pool_view(&mut self, pool: &BufferPool, view: Bytes) {
        let current = std::mem::replace(self, Self::empty());
        let mut builder = SegmentedBytesBuilder::for_pool(Arc::clone(&pool.inner));
        builder.push_segmented(current);
        builder.push_view(view);
        *self = builder.finish();
    }

    /// Consumes this value and returns its remaining presentation segments.
    ///
    /// Each returned [`Bytes`] retains the owners for that complete segment.
    /// No payload bytes are copied.
    pub fn into_segments(mut self) -> Vec<Bytes> {
        let mut segments = Vec::with_capacity(self.segments.len());
        while let Some(segment) = self.take_front_segment() {
            segments.push(segment);
        }
        segments
    }

    /// Returns one contiguous immutable buffer when no gathering is required.
    ///
    /// Empty and single-segment values return `Ok` without copying. A
    /// multi-segment value is returned unchanged in `Err`.
    pub fn try_into_contiguous(self) -> Result<Bytes, Self> {
        if self.segments.len() > 1 {
            return Err(self);
        }

        let mut value = self;
        Ok(value.take_front_segment().unwrap_or_default())
    }

    /// Consumes this value and returns one contiguous immutable buffer.
    ///
    /// Empty and single-segment values do not copy. Multiple segments are
    /// copied in logical order while each source owner remains live until its
    /// bytes have been copied.
    pub fn into_contiguous(self) -> Bytes {
        match self.try_into_contiguous() {
            Ok(contiguous) => contiguous,
            Err(mut segmented) => {
                let mut contiguous = BytesMut::with_capacity(segmented.remaining);
                while let Some(segment) = segmented.take_front_segment() {
                    contiguous.extend_from_slice(&segment);
                }
                contiguous.freeze()
            }
        }
    }

    /// Removes and returns the front presentation segment without copying.
    pub(crate) fn take_front_segment(&mut self) -> Option<Bytes> {
        if self.remaining == 0 {
            if !self.segments.is_empty() {
                invariant_violation("empty segmented value retained presentation ranges");
            }
            return None;
        }

        let mut segment = self
            .segments
            .pop_front()
            .unwrap_or_else(|| invariant_violation("remaining bytes have no front segment"));
        if self.front_offset != 0 {
            segment = segment.trim_prefix(self.front_offset);
            self.front_offset = 0;
        }
        self.remaining = self
            .remaining
            .checked_sub(segment.len)
            .unwrap_or_else(|| invariant_violation("segment exceeds remaining byte length"));
        if self.remaining == 0 && !self.segments.is_empty() {
            invariant_violation("exhausted segmented value retained segments");
        }
        Some(Bytes::from_owner(ContiguousOwner::new(segment)))
    }

    /// Returns the contiguous bytes beginning at `offset` from this cursor.
    ///
    /// This borrowed traversal does not advance the cursor or clone owners.
    pub(crate) fn chunk_from(&self, offset: usize) -> &[u8] {
        assert!(
            offset <= self.remaining,
            "segmented byte offset exceeds remaining bytes"
        );
        if offset == self.remaining {
            return &[];
        }

        let mut skipped = offset;
        for (index, segment) in self.segments.iter().enumerate() {
            let segment_offset = if index == 0 { self.front_offset } else { 0 };
            let available = segment
                .len
                .checked_sub(segment_offset)
                .unwrap_or_else(|| invariant_violation("segment cursor exceeds its range"));
            if skipped >= available {
                skipped -= available;
                continue;
            }
            let start = segment_offset
                .checked_add(skipped)
                .unwrap_or_else(|| invariant_violation("segment offset overflow"));
            let len = segment
                .len
                .checked_sub(start)
                .unwrap_or_else(|| invariant_violation("segment offset exceeds its range"));
            // SAFETY: owners cover this complete immutable initialized range,
            // and `start` was checked within the segment.
            return unsafe { std::slice::from_raw_parts(segment.ptr.as_ptr().add(start), len) };
        }

        invariant_violation("segmented byte offset has no presentation range")
    }

    /// Writes presentation ranges beginning at `offset` into `dst`.
    ///
    /// This is the non-advancing counterpart to [`Buf::chunks_vectored`].
    pub(crate) fn chunks_vectored_from<'a>(
        &'a self,
        offset: usize,
        dst: &mut [IoSlice<'a>],
    ) -> usize {
        assert!(
            offset <= self.remaining,
            "segmented byte offset exceeds remaining bytes"
        );
        if offset == self.remaining || dst.is_empty() {
            return 0;
        }

        let mut skipped = offset;
        let mut written = 0;
        for (index, segment) in self.segments.iter().enumerate() {
            if written == dst.len() {
                break;
            }
            let mut segment_offset = if index == 0 { self.front_offset } else { 0 };
            let available = segment
                .len
                .checked_sub(segment_offset)
                .unwrap_or_else(|| invariant_violation("segment cursor exceeds its range"));
            if skipped >= available {
                skipped -= available;
                continue;
            }
            segment_offset = segment_offset
                .checked_add(skipped)
                .unwrap_or_else(|| invariant_violation("segment offset overflow"));
            skipped = 0;
            let len = segment
                .len
                .checked_sub(segment_offset)
                .unwrap_or_else(|| invariant_violation("segment offset exceeds its range"));
            // SAFETY: owners cover this complete immutable initialized range,
            // and `segment_offset` was checked within the segment.
            let bytes = unsafe {
                std::slice::from_raw_parts(segment.ptr.as_ptr().add(segment_offset), len)
            };
            dst[written] = IoSlice::new(bytes);
            written += 1;
        }
        written
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

    /// Constructs an empty segmented value.
    fn empty() -> Self {
        Self {
            segments: VecDeque::new(),
            front_offset: 0,
            remaining: 0,
        }
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

impl Drop for SegmentedBytes {
    fn drop(&mut self) {
        let Some(first) = first_pooled_owner(
            self.segments
                .iter()
                .flat_map(|segment| segment.owners.iter()),
        ) else {
            return;
        };
        let permit = first.begin_owner_return();
        if !permit.should_batch() {
            // Return owners before releasing the permit so overlapping drops
            // observe this whole-value return as active.
            self.segments.clear();
            return;
        }

        let mut batch = CarrierReturnBatch::for_guard(first);
        for segment in &mut self.segments {
            transfer_batchable_owners(&mut segment.owners, &mut batch);
        }
    }
}

impl PartialEq for SegmentedBytes {
    fn eq(&self, other: &Self) -> bool {
        buffers_equal(self.clone(), other.clone())
    }
}

impl Eq for SegmentedBytes {}

impl PartialEq<Bytes> for SegmentedBytes {
    fn eq(&self, other: &Bytes) -> bool {
        buffers_equal(self.clone(), other.clone())
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

/// Compares two byte cursors without gathering either value.
fn buffers_equal(mut left: impl Buf, mut right: impl Buf) -> bool {
    if left.remaining() != right.remaining() {
        return false;
    }

    while left.has_remaining() {
        let compared = left.chunk().len().min(right.chunk().len());
        if left.chunk()[..compared] != right.chunk()[..compared] {
            return false;
        }
        left.advance(compared);
        right.advance(compared);
    }
    true
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

/// Returns the first pooled owner when a value has several owner boundaries.
fn first_pooled_owner<'a>(
    owners: impl IntoIterator<Item = &'a OwnedRange>,
) -> Option<&'a CarrierGuard> {
    let mut first = None;
    let mut count = 0usize;
    for owner in owners {
        count += 1;
        if first.is_none() {
            if let Hold::Pooled(guard) = &owner.hold {
                first = Some(guard.as_ref());
            }
        }
        if count >= 2 && first.is_some() {
            return first;
        }
    }
    None
}

/// Transfers uniquely held matching owners into one physical-first batch.
fn transfer_batchable_owners(owners: &mut VecDeque<OwnedRange>, batch: &mut CarrierReturnBatch) {
    while let Some(owner) = owners.pop_front() {
        match owner.hold {
            Hold::Pooled(guard) => {
                if !batch.accepts(&guard) {
                    drop(guard);
                    continue;
                }
                match Arc::try_unwrap(guard) {
                    Ok(guard) => batch.push(guard.into_return()),
                    Err(guard) => drop(guard),
                }
            }
            Hold::View(view) => drop(view),
        }
    }
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
    pub(super) fn push_segmented(&mut self, mut buffer: SegmentedBytes) {
        let mut segments = std::mem::take(&mut buffer.segments);
        let front_offset = std::mem::take(&mut buffer.front_offset);
        let remaining = std::mem::take(&mut buffer.remaining);
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

impl Drop for ContiguousOwner {
    fn drop(&mut self) {
        let Some(first) = first_pooled_owner(self.owners.iter()) else {
            return;
        };
        let permit = first.begin_owner_return();
        if !permit.should_batch() {
            // Return owners before releasing the permit so overlapping drops
            // observe this whole-value return as active.
            self.owners.clear();
            return;
        }

        let mut batch = CarrierReturnBatch::for_guard(first);
        transfer_batchable_owners(&mut self.owners, &mut batch);
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use bytes::Buf;

    use super::super::admission::AdmissionGuard;
    use super::super::arena::ArenaTrim;
    use super::super::test_util::{poll_reserve, slot_claiming_waker, test_pool, write_pooled};
    use super::super::CarrierCount;
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
    fn test_whole_value_drop_batches_physical_and_accounting_return() {
        let (pool, carrier_size) = test_pool(8, 9);
        let reservation = pool
            .try_reserve(carrier_size * 8)
            .unwrap()
            .expect("part reservation");
        let mut mutable = pool.acquire(&reservation, carrier_size * 8).unwrap();
        write_pooled(&mut mutable, &vec![0x5a; carrier_size * 8]);
        let frozen = mutable.freeze();
        let slot = Arc::clone(
            frozen.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        drop(reservation);

        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation within configured capacity");
        let (waker, wake_state) = slot_claiming_waker(Arc::clone(&slot));
        let mut queued = pool.reserve(carrier_size * 7);
        assert!(poll_reserve(&mut queued, &waker).is_pending());
        let release_batches = slot.test_release_batches();

        drop(frozen);

        // One grouped return plus the wake-time probe returning its claim.
        assert_eq!(slot.test_release_batches() - release_batches, 2);
        assert_eq!(pool.return_admission_entries(), 1);
        assert_eq!(wake_state.wakes(), 1);
        assert!(
            wake_state.claimed(),
            "waiter reentry ran before the batched physical return was reusable"
        );
        let granted = match poll_reserve(&mut queued, &waker) {
            std::task::Poll::Ready(Ok(reservation)) => reservation,
            std::task::Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
            std::task::Poll::Pending => panic!("batched return stranded queued admission"),
        };
        drop(holder);
        drop(granted);
        let audit = pool.audit_quiescent();
        assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
        assert_eq!(audit.queued_reservations, 0);
    }

    #[test]
    fn test_overlapping_whole_value_return_batches_without_a_waiter() {
        let (pool, carrier_size) = test_pool(4, 4);
        let mut first = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut first, &vec![0x5a; carrier_size * 2]);
        let first = first.freeze();
        let Hold::Pooled(guard) = &first.segments[0].owners[0].hold else {
            panic!("frozen pool value should retain a carrier guard");
        };
        let active_return = guard.begin_owner_return();
        assert!(!active_return.should_batch());

        let mut second = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut second, &vec![0xa5; carrier_size * 2]);
        let second = second.freeze();
        let slot = Arc::clone(
            second.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        let release_batches = slot.test_release_batches();

        drop(second);

        assert_eq!(slot.test_release_batches() - release_batches, 1);
        drop(active_return);
        drop(first);
    }

    #[test]
    fn test_batched_drop_preserves_owners_shared_with_a_clone() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut mutable = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut mutable, &vec![0x5a; carrier_size * 2]);
        let frozen = mutable.freeze();
        let shared = frozen.clone();
        let slot = Arc::clone(
            frozen.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        let active_return = first_pooled_owner(
            frozen
                .segments
                .iter()
                .flat_map(|segment| segment.owners.iter()),
        )
        .expect("multi-owner pooled value")
        .begin_owner_return();
        let release_batches = slot.test_release_batches();

        drop(frozen);

        assert_eq!(slot.test_release_batches() - release_batches, 0);
        let audit = pool.audit_quiescent();
        assert_eq!(audit.charged_capacity, CarrierCount::new(2));
        assert_eq!(audit.live_carriers, CarrierCount::new(2));
        assert_eq!(shared.chunk(), vec![0x5a; carrier_size * 2]);

        drop(active_return);
        drop(shared);
        let audit = pool.audit_quiescent();
        assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
    }

    #[test]
    fn test_batched_drop_falls_back_for_different_reservation_owners() {
        let (pool, carrier_size) = test_pool(2, 2);
        let first_reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("first reservation");
        let mut first = pool.acquire(&first_reservation, carrier_size).unwrap();
        write_pooled(&mut first, &vec![0x5a; carrier_size]);
        let mut first = first.freeze();

        let second_reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("second reservation");
        let mut second = pool.acquire(&second_reservation, carrier_size).unwrap();
        write_pooled(&mut second, &vec![0xa5; carrier_size]);
        let second = second.freeze();

        let slot = Arc::clone(
            first.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        let Hold::Pooled(first_guard) = &first.segments[0].owners[0].hold else {
            panic!("pooled segment should retain its carrier guard");
        };
        let active_return = first_guard.begin_owner_return();
        first.append(second);
        drop(first_reservation);
        drop(second_reservation);
        let release_batches = slot.test_release_batches();

        drop(first);

        assert_eq!(slot.test_release_batches() - release_batches, 2);
        let audit = pool.audit_quiescent();
        assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
        drop(active_return);
    }

    #[test]
    fn test_contiguous_owner_batches_before_waking_waiter() {
        let (pool, carrier_size) = test_pool(2, 3);
        let reservation = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("part reservation");
        let mut mutable = pool.acquire(&reservation, carrier_size * 2).unwrap();
        write_pooled(&mut mutable, &vec![0x5a; carrier_size * 2]);
        let frozen = mutable.freeze();
        let slot = Arc::clone(
            frozen.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        let contiguous = frozen
            .try_into_contiguous()
            .expect("one-segment pooled value");
        drop(reservation);

        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation within configured capacity");
        let (waker, wake_state) = slot_claiming_waker(Arc::clone(&slot));
        let mut queued = pool.reserve(carrier_size);
        assert!(poll_reserve(&mut queued, &waker).is_pending());
        let release_batches = slot.test_release_batches();

        drop(contiguous);

        // One grouped return plus the wake-time probe returning its claim.
        assert_eq!(slot.test_release_batches() - release_batches, 2);
        assert_eq!(wake_state.wakes(), 1);
        assert!(
            wake_state.claimed(),
            "waiter reentry ran before the contiguous physical return was reusable"
        );
        let granted = match poll_reserve(&mut queued, &waker) {
            std::task::Poll::Ready(Ok(reservation)) => reservation,
            std::task::Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
            std::task::Poll::Pending => panic!("contiguous return stranded queued admission"),
        };
        drop(holder);
        drop(granted);
        let audit = pool.audit_quiescent();
        assert_eq!(audit.charged_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
        assert_eq!(audit.queued_reservations, 0);
    }

    #[test]
    fn test_batched_return_makes_the_block_trimmable() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut mutable = pool.acquire_unreserved(carrier_size * 2).unwrap();
        write_pooled(&mut mutable, &vec![0x5a; carrier_size * 2]);
        let frozen = mutable.freeze();
        let slot = Arc::clone(
            frozen.segments[0]
                .slot
                .as_ref()
                .expect("pooled segment slot"),
        );
        let active_return = first_pooled_owner(
            frozen
                .segments
                .iter()
                .flat_map(|segment| segment.owners.iter()),
        )
        .expect("multi-owner pooled value")
        .begin_owner_return();
        assert!(pool.inner.arena.select_trim_candidate().is_none());
        let release_batches = slot.test_release_batches();

        drop(frozen);

        assert_eq!(slot.test_release_batches() - release_batches, 1);
        let candidate = pool
            .inner
            .arena
            .select_trim_candidate()
            .expect("batched return should expose one free block");
        assert!(Arc::ptr_eq(&candidate, &slot));
        let cleanup = {
            let mut admission = AdmissionGuard::new(pool.inner.admission.lock());
            match pool
                .inner
                .arena
                .start_trim(&mut admission, CarrierCount::ZERO)
            {
                ArenaTrim::Started(cleanup) => cleanup,
                ArenaTrim::Blocked => panic!("free block was not trimmable"),
            }
        };
        cleanup.finish().expect("trim cleanup");
        drop(active_return);

        let audit = pool.audit_quiescent();
        assert_eq!(audit.prepared_capacity, CarrierCount::ZERO);
        assert_eq!(audit.live_carriers, CarrierCount::ZERO);
        assert_eq!(audit.cleanup_pending_blocks, 0);
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
    fn test_borrowed_offset_traversal_crosses_presentation_segments() {
        let mut builder = SegmentedBytesBuilder::new();
        builder.push_view(Bytes::from_static(b"ab"));
        builder.push_view(Bytes::from_static(b"cdef"));
        builder.push_view(Bytes::from_static(b"gh"));
        let mut value = builder.finish();
        value.advance(1);

        assert_eq!(value.chunk_from(0), b"b");
        assert_eq!(value.chunk_from(1), b"cdef");
        assert_eq!(value.chunk_from(5), b"gh");
        assert_eq!(value.chunk_from(7), b"");

        let mut one = [IoSlice::new(&[])];
        assert_eq!(value.chunks_vectored_from(1, &mut one), 1);
        assert_eq!(&*one[0], b"cdef");

        let mut all = [IoSlice::new(&[]); 3];
        assert_eq!(value.chunks_vectored_from(0, &mut all), 3);
        assert_eq!(&*all[0], b"b");
        assert_eq!(&*all[1], b"cdef");
        assert_eq!(&*all[2], b"gh");

        let mut empty = [IoSlice::new(&[])];
        assert_eq!(value.chunks_vectored_from(value.len(), &mut empty), 0);
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
    fn test_try_into_contiguous_returns_multisegment_value_unchanged() {
        let left = Bytes::from_static(b"left");
        let right = Bytes::from_static(b"right");
        let mut segmented = SegmentedBytes::from(left);
        segmented.append(SegmentedBytes::from(right));

        let segmented = segmented
            .try_into_contiguous()
            .expect_err("foreign segments must not be gathered");

        assert_eq!(segmented.len(), 9);
        assert_eq!(segmented.into_contiguous(), b"leftright"[..]);
    }

    #[test]
    fn test_into_segments_preserves_partial_cursor_and_releases_each_owner() {
        let (pool, carrier_size) = test_pool(1, 2);
        let input: Vec<u8> = (0..carrier_size * 2)
            .map(|index| (index.wrapping_mul(17) % 251) as u8)
            .collect();
        let mut mutable = pool.acquire_unreserved(input.len()).unwrap();
        write_pooled(&mut mutable, &input);
        let mut frozen = mutable.freeze();
        frozen.advance(carrier_size - 3);
        assert_eq!(frozen.segments.len(), 2);

        let mut segments = frozen.into_segments().into_iter();
        let first = segments.next().expect("partial front segment");
        let second = segments.next().expect("second segment");

        assert_eq!(first, input[carrier_size - 3..carrier_size]);
        assert_eq!(second, input[carrier_size..]);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 2) as u64
        );
        drop(first);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(second);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_append_preserves_unconsumed_bytes_and_segmentation_independent_equality() {
        let mut left = SegmentedBytes::from(Bytes::from_static(b"abcd"));
        left.advance(2);
        left.append(SegmentedBytes::from(Bytes::from_static(b"efgh")));

        let right = SegmentedBytes::from(Bytes::from_static(b"cdefgh"));

        assert_eq!(left, right);
        assert_eq!(left, Bytes::from_static(b"cdefgh"));
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
    use std::task::Poll;

    use super::super::block::BlockSlot;
    use super::super::test_util::{
        poll_reserve, slot_claiming_waker, test_pool, test_single_carrier_pool, write_pooled,
    };
    use super::super::CarrierCount;
    use crate::runtime::sync::sync::Arc;
    use crate::runtime::sync::thread;

    #[test]
    fn test_batched_return_races_claim_and_publishes_before_wake() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(2, 3);
            let reservation = pool
                .try_reserve(carrier_size * 2)
                .unwrap()
                .expect("part reservation");
            let mut mutable = pool.acquire(&reservation, carrier_size * 2).unwrap();
            write_pooled(&mut mutable, &vec![0x5a; carrier_size * 2]);
            let frozen = mutable.freeze();
            let slot = Arc::clone(
                frozen.segments[0]
                    .slot
                    .as_ref()
                    .expect("pooled segment slot"),
            );
            // The racer and wake probe need distinct carriers published by one
            // bitmap-word update; otherwise contention can consume the only
            // returned carrier before the probe runs.
            assert_eq!(slot.carrier_count(), CarrierCount::new(2));
            assert_eq!(slot.bitmap_words(), 1);
            drop(reservation);

            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("reservation within configured capacity");
            let (waker, wake_state) = slot_claiming_waker(Arc::clone(&slot));
            let mut queued = pool.reserve(carrier_size);
            assert!(poll_reserve(&mut queued, &waker).is_pending());

            let returning = thread::spawn(move || drop(frozen));
            let raced_claim =
                BlockSlot::try_claim(&slot, CarrierCount::new(1)).expect("racing slot claim");
            let first_poll = poll_reserve(&mut queued, &waker);
            returning.join().unwrap();

            assert_eq!(wake_state.wakes(), 1);
            assert!(
                wake_state.claimed(),
                "batched return woke admission before physical ownership was reusable"
            );
            let granted = match first_poll {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                Poll::Pending => match poll_reserve(&mut queued, &waker) {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                    Poll::Pending => panic!("batched return stranded queued admission"),
                },
            };
            drop(raced_claim);
            drop(holder);
            drop(granted);
            assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        });
    }

    #[test]
    fn test_concurrent_frozen_clone_drops_return_one_carrier_once() {
        loom::model(|| {
            let (pool, carrier_size) = test_single_carrier_pool(1);
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
            let (pool, carrier_size) = test_single_carrier_pool(1);
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
