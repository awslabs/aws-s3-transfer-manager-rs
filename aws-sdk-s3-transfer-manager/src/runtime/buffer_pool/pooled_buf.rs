/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Growable mutable buffers backed by exclusively owned pool carriers.
//!
//! Acquisition installs accounting and returns one guard per carrier. This
//! module arranges those guards in logical write order, tracks initialization,
//! and transfers initialized prefixes into immutable owners without moving
//! bytes.

use std::mem::MaybeUninit;
use std::ptr::NonNull;

use bytes::buf::UninitSlice;
use bytes::{BufMut, Bytes};
use smallvec::SmallVec;

use super::acquisition::{acquire_count, allocation_failure_injected, AcquireError, CarrierGuard};
use super::admission::ReservationState;
use super::geometry::GeometryError;
use super::segmented_bytes::SegmentedBytesBuilder;
use super::{invariant_violation, CarrierCount, PoolInner, SegmentedBytes};
use crate::runtime::sync::sync::Arc;

/// Carrier runs stored inline before metadata spills to the heap.
const INLINE_CARRIER_RUNS: usize = 4;

/// Contiguous physical runs in logical byte order.
type CarrierRuns = SmallVec<[CarrierRun; INLINE_CARRIER_RUNS]>;

/// One growable mutable allocation stream.
pub(crate) struct PooledBufMut {
    /// Selects reserved or unreserved acquisition for every later growth.
    growth: GrowthAuthority,
    /// Preserves logical byte order across physical runs.
    runs: CarrierRuns,
    /// First carrier with uninitialized writable capacity.
    write_cursor: BufferCursor,
    /// First carrier with initialized unpublished bytes.
    publish_cursor: BufferCursor,
    /// Initialized bytes awaiting publication.
    initialized: usize,
    /// Initialized bytes plus uninitialized writable capacity.
    retained_capacity: usize,
}

impl PooledBufMut {
    /// Builds a mutable stream from one complete carrier acquisition.
    pub(super) fn try_new(
        growth: GrowthAuthority,
        guards: Vec<Arc<CarrierGuard>>,
    ) -> Result<Self, AcquireError> {
        let (runs, retained_capacity) = build_runs(growth.pool(), guards)?;
        let mut buffer = Self {
            growth,
            runs,
            write_cursor: BufferCursor::START,
            publish_cursor: BufferCursor::START,
            initialized: 0,
            retained_capacity,
        };
        buffer.reset_cursors();
        Ok(buffer)
    }

    /// Returns initialized and writable bytes retained by this buffer.
    pub(crate) fn capacity(&self) -> usize {
        self.retained_capacity
    }

    /// Returns initialized bytes that have not been published.
    pub(crate) fn len(&self) -> usize {
        self.initialized
    }

    /// Returns whether no initialized unpublished bytes remain.
    pub(crate) fn is_empty(&self) -> bool {
        self.initialized == 0
    }

    /// Returns the number of grouped runs for acquisition tests.
    #[cfg(test)]
    pub(super) fn test_run_count(&self) -> usize {
        self.runs.len()
    }

    /// Returns one grouped run's complete carrier capacity.
    #[cfg(test)]
    pub(super) fn test_run_capacity(&self, index: usize) -> usize {
        self.runs[index]
            .carriers
            .iter()
            .map(|carrier| carrier.writable.len())
            .sum()
    }

    /// Returns the next contiguous initialized prefix.
    ///
    /// The result is empty exactly when [`Self::len`] is zero. A nonempty
    /// result never crosses a carrier boundary.
    pub(crate) fn initialized_chunk(&self) -> &[u8] {
        if self.initialized == 0 {
            return &[];
        }
        let carrier = self
            .carrier(self.publish_cursor)
            .filter(|carrier| carrier.initialized != 0)
            .or_else(|| self.first_carrier_matching(|carrier| carrier.initialized != 0))
            .unwrap_or_else(|| {
                invariant_violation("initialized buffer has no publication carrier")
            });

        // SAFETY: `initialized` is the prefix written through this buffer's
        // exclusive range. The carrier guard keeps the mapping prepared.
        unsafe {
            std::slice::from_raw_parts(
                carrier.writable.ptr().as_ptr().cast::<u8>(),
                carrier.initialized,
            )
        }
    }

    /// Ensures that at least `min_writable` uninitialized bytes remain.
    ///
    /// Existing tail capacity is consumed before the shortfall is rounded to
    /// complete carriers. Failure leaves this buffer unchanged.
    pub(crate) fn reserve(&mut self, min_writable: usize) -> Result<(), AcquireError> {
        let remaining = self.remaining_mut();
        if min_writable <= remaining {
            return Ok(());
        }

        let shortfall = min_writable
            .checked_sub(remaining)
            .ok_or(AcquireError::CapacityOverflow)?;
        let count = self
            .growth
            .pool()
            .geometry
            .carriers_for_bytes(shortfall)
            .map_err(map_geometry_error)?;
        let guards = self.growth.acquire(count)?;
        let (staged, added_capacity) = build_runs(self.growth.pool(), guards)?;
        self.append_staged(staged, added_capacity)?;
        Ok(())
    }

    /// Publishes one contiguous initialized prefix without copying.
    ///
    /// # Panics
    ///
    /// Panics unless `count` is nonzero and no larger than
    /// `initialized_chunk().len()`.
    pub(crate) fn publish_prefix(&mut self, count: usize) -> Bytes {
        self.normalize_publish_cursor();
        let available = self.initialized_chunk().len();
        assert!(
            count != 0 && count <= available,
            "published prefix must be nonzero and within initialized_chunk"
        );

        let cursor = self.publish_cursor;
        let (range, guard) =
            {
                let carrier = self.carrier_mut(cursor).unwrap_or_else(|| {
                    invariant_violation("publication cursor is outside the buffer")
                });
                let range = carrier.writable.take_prefix(count);
                carrier.initialized = carrier.initialized.checked_sub(count).unwrap_or_else(|| {
                    invariant_violation("publication exceeds initialized bytes")
                });
                let guard =
                    if carrier.writable.is_empty() {
                        carrier.guard.take().unwrap_or_else(|| {
                            invariant_violation("exhausted writable carrier lost its guard")
                        })
                    } else {
                        Arc::clone(carrier.guard.as_ref().unwrap_or_else(|| {
                            invariant_violation("writable carrier lost its guard")
                        }))
                    };
                (range, guard)
            };

        self.initialized = self
            .initialized
            .checked_sub(count)
            .unwrap_or_else(|| invariant_violation("buffer initialization underflow"));
        self.retained_capacity = self
            .retained_capacity
            .checked_sub(count)
            .unwrap_or_else(|| invariant_violation("buffer capacity underflow"));
        self.normalize_publish_cursor();

        Bytes::from_owner(PooledWindow::new(guard, range))
    }

    /// Ends growth and transfers initialized unpublished ranges.
    ///
    /// Wholly unused carriers return immediately. An initialized prefix in a
    /// partially used carrier retains that carrier and discards its writable
    /// suffix.
    pub(crate) fn freeze(self) -> SegmentedBytes {
        let pool = Arc::clone(self.growth.pool());
        let Self { runs, .. } = self;
        let mut builder = SegmentedBytesBuilder::for_pool(pool);

        for run in runs {
            for mut carrier in run.carriers {
                if carrier.initialized == 0 {
                    continue;
                }
                let initialized = carrier.initialized;
                let range = carrier.writable.take_prefix(initialized);
                carrier.initialized = 0;
                let guard = carrier
                    .guard
                    .take()
                    .unwrap_or_else(|| invariant_violation("initialized carrier lost its guard"));
                builder.push_pooled(range.ptr.cast::<u8>(), range.len, guard);
            }
        }

        builder.finish()
    }

    /// Appends one fully staged growth transaction.
    fn append_staged(
        &mut self,
        mut staged: CarrierRuns,
        added_capacity: usize,
    ) -> Result<(), AcquireError> {
        let retained_capacity = self
            .retained_capacity
            .checked_add(added_capacity)
            .ok_or(AcquireError::CapacityOverflow)?;
        if staged.is_empty() {
            invariant_violation("successful growth produced no carrier runs");
        }

        let merge_first = self
            .runs
            .last()
            .zip(staged.first())
            .is_some_and(|(current, incoming)| current.can_append_run(incoming));
        let appended_runs = staged.len() - usize::from(merge_first);

        if allocation_failure_injected(self.growth.pool()) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        self.runs
            .try_reserve(appended_runs)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;

        if merge_first {
            let incoming = staged.remove(0);
            let current = self
                .runs
                .last_mut()
                .unwrap_or_else(|| invariant_violation("merge target disappeared"));
            if allocation_failure_injected(self.growth.pool()) {
                return Err(AcquireError::PhysicalAllocationFailed);
            }
            current
                .carriers
                .try_reserve(incoming.carriers.len())
                .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
            current.carriers.extend(incoming.carriers);
        }
        self.runs.extend(staged);
        self.retained_capacity = retained_capacity;
        self.reset_write_cursor();
        Ok(())
    }

    /// Finds both cursors after initial construction.
    fn reset_cursors(&mut self) {
        self.reset_write_cursor();
        self.publish_cursor = self
            .first_cursor_matching(|carrier| carrier.initialized != 0)
            .unwrap_or_else(|| BufferCursor::end(self.runs.len()));
    }

    /// Finds the first writable carrier after run topology changes.
    fn reset_write_cursor(&mut self) {
        self.write_cursor = self
            .first_cursor_matching(|carrier| carrier.remaining_mut() != 0)
            .unwrap_or_else(|| BufferCursor::end(self.runs.len()));
    }

    /// Advances the write cursor past exhausted carrier entries.
    fn normalize_write_cursor(&mut self) {
        self.write_cursor = self
            .cursor_matching_from(self.write_cursor, |carrier| carrier.remaining_mut() != 0)
            .unwrap_or_else(|| BufferCursor::end(self.runs.len()));
    }

    /// Advances the publication cursor past entries with no initialized bytes.
    fn normalize_publish_cursor(&mut self) {
        self.publish_cursor = self
            .cursor_matching_from(self.publish_cursor, |carrier| carrier.initialized != 0)
            .unwrap_or_else(|| BufferCursor::end(self.runs.len()));
    }

    /// Returns the first carrier satisfying `predicate`.
    fn first_carrier_matching(
        &self,
        predicate: impl Fn(&WritableCarrier) -> bool,
    ) -> Option<&WritableCarrier> {
        self.runs
            .iter()
            .flat_map(|run| run.carriers.iter())
            .find(|carrier| predicate(carrier))
    }

    /// Returns the first carrier cursor satisfying `predicate`.
    fn first_cursor_matching(
        &self,
        predicate: impl Fn(&WritableCarrier) -> bool,
    ) -> Option<BufferCursor> {
        for (run, entry) in self.runs.iter().enumerate() {
            for (carrier, value) in entry.carriers.iter().enumerate() {
                if predicate(value) {
                    return Some(BufferCursor { run, carrier });
                }
            }
        }
        None
    }

    /// Returns the next carrier at or after `cursor` satisfying `predicate`.
    fn cursor_matching_from(
        &self,
        mut cursor: BufferCursor,
        predicate: impl Fn(&WritableCarrier) -> bool,
    ) -> Option<BufferCursor> {
        loop {
            let carrier = self.carrier(cursor)?;
            if predicate(carrier) {
                return Some(cursor);
            }
            cursor = self.next_cursor(cursor)?;
        }
    }

    /// Returns the carrier after `cursor`.
    fn next_cursor(&self, cursor: BufferCursor) -> Option<BufferCursor> {
        let run = self.runs.get(cursor.run)?;
        let carrier = cursor.carrier.checked_add(1)?;
        if carrier < run.carriers.len() {
            return Some(BufferCursor {
                run: cursor.run,
                carrier,
            });
        }
        let run = cursor.run.checked_add(1)?;
        (run < self.runs.len()).then_some(BufferCursor { run, carrier: 0 })
    }

    /// Returns the carrier at `cursor`.
    fn carrier(&self, cursor: BufferCursor) -> Option<&WritableCarrier> {
        self.runs
            .get(cursor.run)
            .and_then(|run| run.carriers.get(cursor.carrier))
    }

    /// Returns the mutable carrier at `cursor`.
    fn carrier_mut(&mut self, cursor: BufferCursor) -> Option<&mut WritableCarrier> {
        self.runs
            .get_mut(cursor.run)
            .and_then(|run| run.carriers.get_mut(cursor.carrier))
    }

    /// Marks `count` sequential writable bytes initialized.
    fn advance_initialized(&mut self, mut count: usize) {
        assert!(
            count <= self.remaining_mut(),
            "advanced beyond pooled writable capacity"
        );
        if count == 0 {
            return;
        }

        self.normalize_write_cursor();
        while count != 0 {
            let cursor = self.write_cursor;
            let publish_was_empty = self.initialized == 0;
            let (advanced, exhausted) = {
                let carrier = self.carrier_mut(cursor).unwrap_or_else(|| {
                    invariant_violation("writable cursor is outside the buffer")
                });
                let advanced = count.min(carrier.remaining_mut());
                if advanced == 0 {
                    invariant_violation("writable cursor names an exhausted carrier");
                }
                carrier.initialized = carrier
                    .initialized
                    .checked_add(advanced)
                    .unwrap_or_else(|| invariant_violation("carrier initialization overflow"));
                (advanced, carrier.remaining_mut() == 0)
            };
            if publish_was_empty {
                self.publish_cursor = cursor;
            }
            self.initialized = self
                .initialized
                .checked_add(advanced)
                .unwrap_or_else(|| invariant_violation("buffer initialization overflow"));
            count -= advanced;
            if exhausted {
                self.write_cursor = self
                    .next_cursor(cursor)
                    .unwrap_or_else(|| BufferCursor::end(self.runs.len()));
                self.normalize_write_cursor();
            }
        }
    }
}

// SAFETY: moving the buffer transfers every unique `ExclusiveRange`.
// Carrier and growth state is retained through synchronized shared ownership.
unsafe impl Send for PooledBufMut {}

// SAFETY: each `ExclusiveRange` is unique to this buffer, every pointer is
// retained by its carrier guard, and `advance_mut` exposes only initialized
// state transitions made by the caller.
unsafe impl BufMut for PooledBufMut {
    fn remaining_mut(&self) -> usize {
        self.retained_capacity
            .checked_sub(self.initialized)
            .unwrap_or_else(|| invariant_violation("initialized bytes exceed retained capacity"))
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.normalize_write_cursor();
        if self.remaining_mut() == 0 {
            // SAFETY: a dangling pointer is valid for a zero-length slice and
            // the returned borrow remains tied to `self`.
            return unsafe {
                UninitSlice::from_raw_parts_mut(NonNull::<u8>::dangling().as_ptr(), 0)
            };
        }
        let cursor = self.write_cursor;
        let carrier = self
            .carrier_mut(cursor)
            .unwrap_or_else(|| invariant_violation("buffer has no writable carrier"));
        let remaining = carrier.remaining_mut();
        if remaining == 0 {
            invariant_violation("writable cursor names an exhausted carrier");
        }
        // SAFETY: the returned range is the uninitialized suffix of one
        // exclusive carrier range and remains borrowed through `self`.
        unsafe {
            UninitSlice::from_raw_parts_mut(
                carrier
                    .writable
                    .ptr()
                    .as_ptr()
                    .add(carrier.initialized)
                    .cast::<u8>(),
                remaining,
            )
        }
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        self.advance_initialized(count);
    }
}

/// Authority retained for every later growth of one buffer.
pub(super) enum GrowthAuthority {
    /// New carriers must debit this reservation.
    Reserved(Arc<ReservationState>),
    /// New carriers use aggregate pool authority only.
    Unreserved(Arc<PoolInner>),
}

impl GrowthAuthority {
    /// Creates reserved growth authority.
    pub(super) fn reserved(state: Arc<ReservationState>) -> Self {
        Self::Reserved(state)
    }

    /// Creates unreserved growth authority.
    pub(super) fn unreserved(pool: Arc<PoolInner>) -> Self {
        Self::Unreserved(pool)
    }

    /// Returns the shared pool.
    fn pool(&self) -> &Arc<PoolInner> {
        match self {
            Self::Reserved(state) => state.pool(),
            Self::Unreserved(pool) => pool,
        }
    }

    /// Acquires one complete carrier-rounded growth batch.
    fn acquire(&self, count: CarrierCount) -> Result<Vec<Arc<CarrierGuard>>, AcquireError> {
        match self {
            Self::Reserved(state) => acquire_count(state.pool(), Some(Arc::clone(state)), count),
            Self::Unreserved(pool) => acquire_count(pool, None, count),
        }
    }
}

/// Adjacent carriers within one stable block slot.
struct CarrierRun {
    /// Stable block slot containing the run.
    slot_id: u32,
    /// First carrier index within the slot.
    first_carrier: u32,
    /// Per-carrier mutable ownership in ascending address order.
    carriers: Vec<WritableCarrier>,
}

impl CarrierRun {
    /// Creates a run containing one carrier.
    fn try_new(pool: &PoolInner, guard: Arc<CarrierGuard>) -> Result<Self, AcquireError> {
        let (slot_id, first_carrier) = guard.identity();
        let mut carriers = Vec::new();
        if allocation_failure_injected(pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        carriers
            .try_reserve_exact(1)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        carriers.push(WritableCarrier::new(guard));
        Ok(Self {
            slot_id,
            first_carrier,
            carriers,
        })
    }

    /// Returns whether `guard` immediately follows this run.
    fn can_append(&self, guard: &CarrierGuard) -> bool {
        let run_len = u32::try_from(self.carriers.len())
            .unwrap_or_else(|_| invariant_violation("carrier run length exceeds block geometry"));
        let next = self
            .first_carrier
            .checked_add(run_len)
            .unwrap_or_else(|| invariant_violation("carrier run end exceeds block geometry"));
        guard.identity() == (self.slot_id, next)
    }

    /// Returns whether `incoming` immediately follows this run.
    fn can_append_run(&self, incoming: &Self) -> bool {
        let run_len = u32::try_from(self.carriers.len())
            .unwrap_or_else(|_| invariant_violation("carrier run length exceeds block geometry"));
        self.slot_id == incoming.slot_id
            && self.first_carrier.checked_add(run_len) == Some(incoming.first_carrier)
    }

    /// Appends one adjacent carrier.
    fn try_append(
        &mut self,
        pool: &PoolInner,
        guard: Arc<CarrierGuard>,
    ) -> Result<(), AcquireError> {
        if allocation_failure_injected(pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        self.carriers
            .try_reserve(1)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        self.carriers.push(WritableCarrier::new(guard));
        Ok(())
    }
}

/// Unique mutable authority over one carrier's unpublished suffix.
struct WritableCarrier {
    /// Shared physical and accounting ownership while a range remains.
    guard: Option<Arc<CarrierGuard>>,
    /// Initialized prefix followed by uninitialized writable capacity.
    writable: ExclusiveRange,
    /// Initialized prefix length within `writable`.
    initialized: usize,
}

impl WritableCarrier {
    /// Creates a wholly uninitialized writable carrier.
    fn new(guard: Arc<CarrierGuard>) -> Self {
        let writable = ExclusiveRange::new(guard.ptr(), guard.capacity());
        Self {
            guard: Some(guard),
            writable,
            initialized: 0,
        }
    }

    /// Returns uninitialized writable bytes.
    fn remaining_mut(&self) -> usize {
        self.writable
            .len()
            .checked_sub(self.initialized)
            .unwrap_or_else(|| invariant_violation("carrier initialization exceeds its range"))
    }
}

/// Non-cloneable authority over one mutable byte range.
///
/// Construction requires a nonempty range. Prefix transfer may later consume
/// the complete range while its metadata entry remains positionally stable.
struct ExclusiveRange {
    ptr: NonNull<MaybeUninit<u8>>,
    len: usize,
}

impl ExclusiveRange {
    /// Creates authority over one complete carrier.
    fn new(ptr: NonNull<MaybeUninit<u8>>, len: usize) -> Self {
        if len == 0 {
            invariant_violation("exclusive range must be nonempty");
        }
        Self { ptr, len }
    }

    /// Returns the first byte.
    fn ptr(&self) -> NonNull<MaybeUninit<u8>> {
        self.ptr
    }

    /// Returns the range length.
    fn len(&self) -> usize {
        self.len
    }

    /// Returns whether prefix transfer consumed the complete range.
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Removes and returns one prefix, leaving a disjoint suffix.
    fn take_prefix(&mut self, count: usize) -> Self {
        assert!(count <= self.len, "split exceeds exclusive range");
        let prefix = Self {
            ptr: self.ptr,
            len: count,
        };
        // SAFETY: `count` lies within this range. The resulting pointer is
        // either within the same allocation or one byte past its end.
        self.ptr = unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(count)) };
        self.len -= count;
        prefix
    }
}

/// Run and carrier position inside one mutable buffer.
#[derive(Clone, Copy)]
struct BufferCursor {
    run: usize,
    carrier: usize,
}

impl BufferCursor {
    const START: Self = Self { run: 0, carrier: 0 };

    /// Creates a sentinel past the final run.
    fn end(run: usize) -> Self {
        Self { run, carrier: 0 }
    }
}

/// Immutable initialized view retained by one carrier guard.
struct PooledWindow {
    guard: Arc<CarrierGuard>,
    ptr: NonNull<u8>,
    len: usize,
}

impl PooledWindow {
    /// Consumes one exclusive initialized range.
    fn new(guard: Arc<CarrierGuard>, range: ExclusiveRange) -> Self {
        Self {
            guard,
            ptr: range.ptr.cast::<u8>(),
            len: range.len,
        }
    }
}

impl AsRef<[u8]> for PooledWindow {
    fn as_ref(&self) -> &[u8] {
        let _guard = &self.guard;
        // SAFETY: publication consumed mutable authority over this initialized
        // range, and `guard` retains the prepared carrier.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

// SAFETY: the pointer names immutable initialized storage retained by `guard`.
unsafe impl Send for PooledWindow {}

/// Groups one acquisition into adjacent physical runs.
fn build_runs(
    pool: &PoolInner,
    mut guards: Vec<Arc<CarrierGuard>>,
) -> Result<(CarrierRuns, usize), AcquireError> {
    if guards.is_empty() {
        invariant_violation("complete acquisition contains no carriers");
    }
    guards.sort_unstable_by_key(|guard| guard.identity());

    let mut runs = CarrierRuns::new();
    let mut capacity = 0usize;
    for guard in guards {
        capacity = capacity
            .checked_add(guard.capacity())
            .ok_or(AcquireError::CapacityOverflow)?;
        if let Some(run) = runs.last_mut() {
            if run.can_append(&guard) {
                run.try_append(pool, guard)?;
                continue;
            }
        }
        if allocation_failure_injected(pool) {
            return Err(AcquireError::PhysicalAllocationFailed);
        }
        runs.try_reserve(1)
            .map_err(|_| AcquireError::PhysicalAllocationFailed)?;
        runs.push(CarrierRun::try_new(pool, guard)?);
    }
    Ok((runs, capacity))
}

/// Maps byte geometry failures into the acquisition API.
fn map_geometry_error(error: GeometryError) -> AcquireError {
    match error {
        GeometryError::ZeroByteRequest => AcquireError::InvalidSize,
        _ => AcquireError::CapacityOverflow,
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use bytes::{Buf, BufMut, BytesMut};

    use super::super::test_util::{test_pool, write_pooled};
    use super::*;

    #[test]
    fn test_mutable_initialization_crosses_carrier_boundaries() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut buffer = pool.acquire_unreserved(carrier_size + 1).unwrap();
        let input: Vec<u8> = (0..carrier_size + 1)
            .map(|index| (index % 251) as u8)
            .collect();

        write_pooled(&mut buffer, &input);

        assert_eq!(buffer.capacity(), carrier_size * 2);
        assert_eq!(buffer.len(), input.len());
        assert_eq!(buffer.remaining_mut(), carrier_size - 1);
        assert_eq!(buffer.initialized_chunk(), &input[..carrier_size]);
        assert_eq!(buffer.chunk_mut().len(), carrier_size - 1);
    }

    #[test]
    fn test_incremental_growth_reuses_one_buffers_writable_tail() {
        let (pool, carrier_size) = test_pool(4, 3);
        let total = carrier_size * 2 + 1;
        let reservation = pool
            .try_reserve(total)
            .unwrap()
            .expect("three-carrier reservation");
        let mut buffer = pool.acquire(&reservation, 1).unwrap();
        let increments = [
            carrier_size / 2,
            carrier_size / 2,
            carrier_size / 2,
            carrier_size / 2 + 1,
        ];

        let mut next = 0u8;
        for increment in increments {
            buffer.reserve(increment).unwrap();
            let bytes: Vec<u8> = (0..increment)
                .map(|_| {
                    let value = next;
                    next = next.wrapping_add(1);
                    value
                })
                .collect();
            write_pooled(&mut buffer, &bytes);
        }

        assert_eq!(buffer.len(), total);
        assert_eq!(buffer.capacity(), carrier_size * 3);
        assert_eq!(buffer.remaining_mut(), carrier_size - 1);
        assert_eq!(buffer.test_run_count(), 1);
    }

    #[test]
    fn test_reserve_zero_and_satisfied_tail_do_not_acquire() {
        let (pool, carrier_size) = test_pool(2, 2);
        let reservation = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("reservation");
        let mut buffer = pool.acquire(&reservation, 1).unwrap();
        write_pooled(&mut buffer, b"tail");
        let capacity = buffer.capacity();
        let metrics = pool.metrics();

        buffer.reserve(0).unwrap();
        buffer.reserve(buffer.remaining_mut()).unwrap();

        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            metrics.charged_capacity_bytes()
        );
    }

    #[test]
    fn test_closed_reservation_allows_tail_use_but_rejects_growth() {
        let (pool, carrier_size) = test_pool(2, 2);
        let reservation = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("reservation");
        let mut buffer = pool.acquire(&reservation, 1).unwrap();
        reservation.close_acquisition();

        buffer.reserve(carrier_size).unwrap();
        let before = (buffer.capacity(), buffer.len(), buffer.remaining_mut());
        assert_eq!(
            buffer.reserve(carrier_size + 1),
            Err(AcquireError::ReservationClosed)
        );
        assert_eq!(
            (buffer.capacity(), buffer.len(), buffer.remaining_mut()),
            before
        );
    }

    #[test]
    fn test_reservation_exhaustion_preserves_existing_buffer() {
        let (pool, carrier_size) = test_pool(1, 1);
        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("reservation");
        let mut buffer = pool.acquire(&reservation, 1).unwrap();
        let input = vec![0x5a; carrier_size];
        write_pooled(&mut buffer, &input);
        let before = (buffer.capacity(), buffer.len(), buffer.remaining_mut());

        assert_eq!(
            buffer.reserve(1),
            Err(AcquireError::ReservationCapacityExceeded)
        );
        assert_eq!(
            (buffer.capacity(), buffer.len(), buffer.remaining_mut()),
            before
        );
        assert_eq!(buffer.initialized_chunk(), input.as_slice());
    }

    #[test]
    fn test_growth_metadata_failure_preserves_bytes_cursors_and_authority() {
        for boundary in 1.. {
            let (pool, carrier_size) = test_pool(2, 2);
            let reservation = pool
                .try_reserve(carrier_size * 2)
                .unwrap()
                .expect("reservation");
            let mut buffer = pool.acquire(&reservation, 1).unwrap();
            write_pooled(&mut buffer, b"abc");
            pool.inject_acquisition_allocation_failure(boundary);

            let result = buffer.reserve(carrier_size);
            if pool.acquisition_allocation_failure_pending() {
                assert!(result.is_ok(), "boundary {boundary} was not reached");
                break;
            }
            assert_eq!(result, Err(AcquireError::PhysicalAllocationFailed));
            assert_eq!(buffer.capacity(), carrier_size);
            assert_eq!(buffer.len(), 3);
            assert_eq!(buffer.remaining_mut(), carrier_size - 3);
            assert_eq!(buffer.initialized_chunk(), b"abc");
            assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);

            buffer
                .reserve(carrier_size)
                .expect("rollback restored direct authority");
            assert_eq!(buffer.capacity(), carrier_size * 2);
        }
    }

    #[test]
    fn test_exhausted_buffer_returns_empty_mutable_chunk() {
        let (pool, carrier_size) = test_pool(1, 1);
        let mut buffer = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut buffer, &vec![0; carrier_size]);

        assert_eq!(buffer.remaining_mut(), 0);
        assert_eq!(buffer.chunk_mut().len(), 0);
    }

    #[test]
    fn test_rounding_is_per_buffer_instead_of_per_growth_call() {
        let (stream_pool, carrier_size) = test_pool(2, 2);
        let increment = carrier_size / 2 + 1;
        let total = increment * 3;
        assert!(total <= carrier_size * 2);
        let stream_reservation = stream_pool
            .try_reserve(total)
            .unwrap()
            .expect("two-carrier stream reservation");
        let mut stream = stream_pool.acquire(&stream_reservation, increment).unwrap();
        write_pooled(&mut stream, &vec![0; increment]);
        for _ in 0..2 {
            stream.reserve(increment).unwrap();
            write_pooled(&mut stream, &vec![0; increment]);
        }
        assert_eq!(stream.capacity(), carrier_size * 2);

        let (split_pool, _) = test_pool(2, 2);
        let split_reservation = split_pool
            .try_reserve(total)
            .unwrap()
            .expect("two-carrier split reservation");
        let first = split_pool.acquire(&split_reservation, increment).unwrap();
        let second = split_pool.acquire(&split_reservation, increment).unwrap();
        assert!(matches!(
            split_pool.acquire(&split_reservation, increment),
            Err(AcquireError::ReservationCapacityExceeded)
        ));
        drop((first, second));
    }

    #[test]
    fn test_publish_prefix_leaves_a_disjoint_mutable_suffix() {
        let (pool, carrier_size) = test_pool(1, 1);
        let mut buffer = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut buffer, b"abcdef");

        let first = buffer.publish_prefix(3);
        assert_eq!(first, b"abc"[..]);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.capacity(), carrier_size - 3);
        assert_eq!(buffer.initialized_chunk(), b"def");

        write_pooled(&mut buffer, b"XYZ");
        assert_eq!(first, b"abc"[..]);
        assert_eq!(buffer.initialized_chunk(), b"defXYZ");
        let second = buffer.publish_prefix(6);
        assert_eq!(second, b"defXYZ"[..]);
    }

    #[test]
    fn test_publication_stops_at_each_carrier_boundary() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut buffer = pool.acquire_unreserved(carrier_size + 3).unwrap();
        let input: Vec<u8> = (0..carrier_size + 3)
            .map(|index| (index % 251) as u8)
            .collect();
        write_pooled(&mut buffer, &input);

        assert_eq!(buffer.initialized_chunk().len(), carrier_size);
        let first = buffer.publish_prefix(carrier_size);
        assert_eq!(first, input[..carrier_size]);
        assert_eq!(buffer.initialized_chunk(), &input[carrier_size..]);
        assert_eq!(buffer.capacity(), carrier_size);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            (carrier_size * 2) as u64
        );

        drop(first);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        let second = buffer.publish_prefix(3);
        drop(buffer);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(second);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_published_clones_and_slices_share_one_carrier_return() {
        let (pool, carrier_size) = test_pool(1, 1);
        let mut buffer = pool.acquire_unreserved(carrier_size).unwrap();
        write_pooled(&mut buffer, b"immutable");
        let published = buffer.publish_prefix(9);
        let clone = published.clone();
        let slice = published.slice(2..7);
        drop(buffer);

        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(published);
        drop(clone);
        assert_eq!(slice, b"mutab"[..]);
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(slice);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_published_owner_holds_direct_authority_until_final_drop() {
        let (pool, carrier_size) = test_pool(2, 2);
        let reservation = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("reservation");
        let mut buffer = pool.acquire(&reservation, carrier_size).unwrap();
        write_pooled(&mut buffer, &vec![0x11; carrier_size]);
        let published = buffer.publish_prefix(carrier_size);

        buffer.reserve(1).unwrap();
        write_pooled(&mut buffer, &vec![0x22; carrier_size]);
        assert_eq!(
            buffer.reserve(1),
            Err(AcquireError::ReservationCapacityExceeded)
        );

        drop(published);
        buffer
            .reserve(1)
            .expect("final immutable drop restored direct authority");
    }

    #[test]
    fn test_publication_and_freeze_preserve_bytes_across_representative_lengths() {
        for carriers in 1..=3 {
            let (pool, carrier_size) = test_pool(3, 3);
            let lengths = [
                1,
                carrier_size - 1,
                carrier_size,
                carrier_size + 1,
                carrier_size * carriers - 1,
            ];
            for length in lengths {
                if length == 0 || length > carrier_size * 3 {
                    continue;
                }
                let input: Vec<u8> = (0..length)
                    .map(|index| (index.wrapping_mul(17) % 251) as u8)
                    .collect();
                let mut buffer = pool.acquire_unreserved(length).unwrap();
                write_pooled(&mut buffer, &input);
                let publish_target = length / 2;
                let mut published = 0;
                let mut views = Vec::new();
                while published < publish_target {
                    let count = (publish_target - published)
                        .min(buffer.initialized_chunk().len())
                        .min(97);
                    views.push(buffer.publish_prefix(count));
                    published += count;
                }
                let mut frozen = buffer.freeze();
                let mut output = BytesMut::with_capacity(length);
                for view in views {
                    output.extend_from_slice(&view);
                }
                while frozen.has_remaining() {
                    let count = frozen.chunk().len();
                    output.extend_from_slice(frozen.chunk());
                    frozen.advance(count);
                }
                assert_eq!(output.as_ref(), input.as_slice(), "length {length}");
                assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
            }
        }
    }

    #[test]
    #[should_panic(expected = "published prefix must be nonzero")]
    fn test_publish_prefix_rejects_zero() {
        let (pool, _) = test_pool(1, 1);
        let mut buffer = pool.acquire_unreserved(1).unwrap();
        write_pooled(&mut buffer, b"x");
        let _ = buffer.publish_prefix(0);
    }

    #[test]
    #[should_panic(expected = "published prefix must be nonzero")]
    fn test_publish_prefix_rejects_cross_carrier_count() {
        let (pool, carrier_size) = test_pool(2, 2);
        let mut buffer = pool.acquire_unreserved(carrier_size + 1).unwrap();
        write_pooled(&mut buffer, &vec![0; carrier_size + 1]);
        let _ = buffer.publish_prefix(carrier_size + 1);
    }
}
