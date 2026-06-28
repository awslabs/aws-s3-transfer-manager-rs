/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! In-order delivery over out-of-order arrival.
//!
//! `PagedRecvBuffer<T>` assigns each payload a sequence number at claim time, accepts
//! completed payloads into their slots in any order from many producers, and
//! delivers them to a single consumer in sequence order. It grows to absorb
//! head-of-line backlog and shrinks as delivery advances; its resident footprint
//! tracks the gap between the oldest undelivered and newest claimed sequence,
//! not the total sequence count.
//!
//! # Structure
//!
//! Two levels. The primary level is a `VecDeque<Arc<Segment<T>>>` guarded by one
//! `Mutex`; each segment holds a fixed run of `seg_size` slots covering a contiguous
//! span of sequence numbers. The deque grows at the tail and is reclaimed from the
//! front, so only the segments spanning the live window are resident.
//!
//! ```text
//!         reclaim ◀── front                    tail ──▶ grow
//!   ┌───────────── Mutex<{ segments, issued }> ─────────────┐
//!   │  ┌─────────┐     ┌─────────┐     ┌─────────┐          │
//!   │  │  seg 0  │─nx─▶│  seg 1  │─nx─▶│  seg 2  │          │
//!   │  │ F F . . │     │ F F F F │     │ F . _ _ │          │
//!   │  │ 0 1 2 3 │     │ 4 5 6 7 │     │ 8 9     │          │
//!   │  │ base=0  │     │ base=4  │     │ base=8  │          │
//!   │  │ [DRAIND]│     │ [FULL]  │     │ [OPEN]  │ issued=9 │
//!   │  └─────────┘     └────▲────┘     └─▲───────┘          │
//!   └───────────────────────┼───────────┼──────────────────┘
//!     consumed=6            cursor=5    fill (producers, any order, lock-free)
//!     (RecvBufferConsumer.current ─┘
//!      hops seg→seg via `nx`, lock-free)
//!
//!   F = FILLED slot   _ = EMPTY slot   . = delivered/consumed   nx = next pointer
//! ```
//!
//! In this picture the consumer has delivered seqs 0–4 (cursor at 5, the head of
//! `seg 1`), producers have filled 5–8 out of order, seq 9 is still in flight, and
//! `seg 0` is fully consumed (eligible for front-reclaim on the next claim). A
//! `VecDeque` reallocation moves only the `Arc` pointers; the segments are
//! heap-stable behind `Arc`, so no producer or consumer reference is invalidated by
//! growth. The only intrusive link is one `next` pointer per segment, by which the
//! consumer reaches the successor segment without taking the lock.
//!
//! # Locking
//!
//! One `Mutex` guards the segment deque and the `issued` counter. Only the issuer
//! paths take it — `claim` (assign a sequence, maybe grow, opportunistic reclaim),
//! `take_completed_segment` (find a full segment), and `complete`/drop reclaim. The
//! two hot paths are lock-free: `fill` writes a claimed slot and publishes it with a
//! `Release` store; `poll_next` reads the cursor slot under `Acquire` and hops
//! segments via the `next` pointer. The delivery cursor lives in `RecvBufferConsumer` and
//! is mirrored to an atomic `consumed` so the issuer's reclaim can read it without
//! synchronizing with the consumer.
//!
//! # Roles
//!
//! - **issuer** — assigns sequence numbers and grows and reclaims segments. The
//!   only writer to the deque; the one internal lock serializes issuers. Off the
//!   hot path (producers fill lock-free), and claims are infrequent relative to
//!   fills, so lock contention is incidental.
//! - **producers** — each writes the single slot it claimed. Concurrent and
//!   lock-free, touching disjoint memory.
//! - **consumer** — reads slots in sequence order. Single and lock-free.
//!
//! # Surfaces
//!
//! [`PagedRecvBuffer::new`] returns a `(PagedRecvBuffer<T>, RecvBufferConsumer<T>)` pair, channel-style.
//! The `PagedRecvBuffer` is cloneable and carries the producer and block surfaces; the
//! `RecvBufferConsumer` is unique (not cloneable) and carries the stream surface.
//!
//! - **producer** (`PagedRecvBuffer`) — [`claim`](PagedRecvBuffer::claim) then
//!   [`fill`](PagedRecvBuffer::fill).
//! - **stream** (`RecvBufferConsumer`) — [`poll_next`](RecvBufferConsumer::poll_next) delivers
//!   one payload at a time in strict sequence order, advancing a single cursor.
//!   Taking `&mut self`, on a non-cloneable handle, makes "single consumer" a
//!   type-level fact and lets the cursor and current-segment cache live in the
//!   handle (so the hop needs no lock).
//! - **block** (`PagedRecvBuffer`) — [`take_completed_segment`](PagedRecvBuffer::take_completed_segment)
//!   hands out a whole `FULL` segment as an owned [`SegmentWrite`], for bulk
//!   consumption that does not need in-order single-item delivery. Segments may be
//!   taken and completed out of order and concurrently.
//!
//! An instance is driven through one consumption surface or the other, never both:
//! the stream cursor empties slots as it passes; the block surface consumes whole
//! segments. A block-only caller drops the `RecvBufferConsumer`. **This exclusion is a
//! caller obligation, not type-enforced** — `PagedRecvBuffer` is `Clone` and exposes the
//! block surface, so the type system does not prevent driving both at once. Doing so
//! races the stream `take` against the block in-place read and is undefined; the
//! `unsafe` in both paths relies on the caller honoring this.
//!
//! # Slot states
//!
//! ```text
//!   EMPTY ──claim, then fill──▶ FILLED ──stream take──▶ EMPTY
//! ```
//!
//! `FILLED` is published with a `Release` store after the payload write; a reader
//! observes it with an `Acquire` load before reading the payload. The block
//! surface leaves slots `FILLED` and reclaims at segment granularity.
//!
//! # Segment states
//!
//! ```text
//!   OPEN ──last slot filled──▶ FULL ──taken──▶ DRAINING ──complete──▶ DRAINED
//! ```
//!
//! The fill that fills a segment's last slot *seals* it: `OPEN → FULL`. Sealing is
//! by a count (`filled_count`), not a high-water index, so out-of-order fills reach
//! `FULL` correctly. The block surface claims `FULL → DRAINING` once (so a segment
//! is handed out at most once) and `DRAINING → DRAINED` on completion, after which
//! the segment is reclaimable. The stream surface ignores these states and reclaims
//! by cursor position.
//!
//! # Memory ordering and safety
//!
//! 1. **Exclusive producer write.** Each sequence number is claimed once (the
//!    issuer advances `issued` monotonically), so the producer holding a
//!    [`SlotHandle`] is the sole writer of that slot.
//! 2. **State-gated read.** A reader touches a slot's payload only after observing
//!    `FILLED` via an `Acquire` load (`SlotState::is_filled`); the producer's
//!    `Release` store of `FILLED` (`SlotState::publish_filled`) publishes the
//!    preceding payload write. The two form the happens-before edge of the handoff:
//!    payload write → `Release` `FILLED` → `Acquire` `FILLED` → payload read.
//! 3. **No slot aliasing.** Sequence numbers are assigned monotonically and each
//!    maps to exactly one slot in one segment for that segment's whole lifetime;
//!    segments are allocated fresh on growth and freed on reclaim, never recycled.
//!    So a slot is never the live target of two sequences — there is no reuse window
//!    to race. (The window the caller must bound is *resident memory*, not aliasing:
//!    see Caller obligations.)
//! 4. **Segment-reclaim safety (the hop).** A segment is removed from the deque
//!    (`pop_front`, front-only) only once `base + len <= consumed`, and only the
//!    consumer advances `consumed`. So when the consumer hops, the successor it is
//!    about to enter (base `== consumed`) cannot be popped, and stays strong-
//!    referenced by the deque — which is what lets the consumer reconstruct an `Arc`
//!    from the `next` pointer without the lock. This concerns *segment* removal
//!    only; emptying a *slot's* payload (delivery, or future demand reclaim) leaves
//!    the segment in place and does not bear on the hop.
//! 5. **Outstanding block pin.** A live [`SegmentWrite`] holds an
//!    `Arc<Segment<T>>`, so a segment being consumed by the block surface cannot
//!    be reclaimed until that token completes.
//!
//! # Caller obligations
//!
//! The ring does not bound its own growth: it allocates a segment whenever a claim
//! reaches a sequence past the current tail. The caller must bound outstanding
//! (claimed-but-undelivered) sequences, or resident memory grows without limit.

use crossbeam_utils::CachePadded;

use crate::runtime::sync::cell::UnsafeCell;
use crate::runtime::sync::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};

/// Default slots per segment for [`PagedRecvBuffer::new`].
///
/// Slots per segment is the unit of allocation and front-reclaim. A larger value
/// amortizes allocation and deque churn over more sequences but holds more memory
/// resident per shelf and coarsens reclaim (a segment is freed only once fully
/// delivered or drained). It is fixed for an instance at construction; tests pick
/// other sizes via [`PagedRecvBuffer::new_with_segment_size`].
pub(crate) const DEFAULT_SEG_SIZE: usize = 16;

/// Slot empty: initial state, and the state after the stream surface takes a payload.
const SLOT_EMPTY: u8 = 0;
/// Slot filled: a producer has published a payload. Readers observe this with an
/// `Acquire` load (the reader's side of the handoff), pairing with the producer's
/// `Release` store.
const SLOT_FILLED: u8 = 1;

/// Occupancy of one slot, over an `AtomicU8` so transitions read as named state
/// changes with explicit orderings rather than raw atomic ops (cf.
/// `StateMachineStatus` in `transfer.rs`).
struct SlotState(AtomicU8);

impl SlotState {
    fn new() -> Self {
        Self(AtomicU8::new(SLOT_EMPTY))
    }

    /// Publish a completed fill. `Release`: the payload write happens-before this,
    /// so a reader that observes the slot filled via [`is_filled`](Self::is_filled)
    /// sees the payload.
    fn publish_filled(&self) {
        self.0.store(SLOT_FILLED, Ordering::Release);
    }

    /// Whether the slot is filled, `Acquire` to pair with
    /// [`publish_filled`](Self::publish_filled) before reading the payload.
    fn is_filled(&self) -> bool {
        self.0.load(Ordering::Acquire) == SLOT_FILLED
    }

    /// Mark the slot empty after its payload has been taken.
    fn set_empty(&self) {
        self.0.store(SLOT_EMPTY, Ordering::Release);
    }
}

/// Segment open: accepting fills; not all slots filled yet.
const SEG_OPEN: u8 = 0;
/// Segment full: every slot filled, eligible to be taken by the block surface.
const SEG_FULL: u8 = 1;
/// Segment draining: claimed by a block consumer, pinned by a live [`SegmentWrite`],
/// not yet reclaimable.
const SEG_DRAINING: u8 = 2;
/// Segment drained: block consumption complete, payloads freed, reclaimable.
const SEG_DRAINED: u8 = 3;

/// Segment lifecycle over an `AtomicU8`. The `FULL → DRAINING` transition is a CAS
/// so a full segment is handed to the block surface at most once.
struct SegState(AtomicU8);

impl SegState {
    fn new() -> Self {
        Self(AtomicU8::new(SEG_OPEN))
    }

    /// Mark the segment full when its last slot fills: `OPEN → FULL` (`Release`).
    fn set_full(&self) {
        self.0.store(SEG_FULL, Ordering::Release);
    }

    /// Claim a full segment for block consumption exactly once: `FULL → DRAINING`.
    /// Returns `true` for the single caller that wins the CAS.
    fn try_claim_for_drain(&self) -> bool {
        self.0
            .compare_exchange(SEG_FULL, SEG_DRAINING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Mark consumption complete: `DRAINING → DRAINED` (`Release`).
    fn set_drained(&self) {
        self.0.store(SEG_DRAINED, Ordering::Release);
    }

    /// Whether the segment has been fully drained (`Acquire`).
    fn is_drained(&self) -> bool {
        self.0.load(Ordering::Acquire) == SEG_DRAINED
    }
}

/// One slot: a single payload behind state-gated interior mutability.
///
/// `data` is written by the producer that claimed the slot and read by the
/// consumer (or the block surface) after `state` is observed filled.
pub(crate) struct Slot<T> {
    state: SlotState,
    data: UnsafeCell<Option<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            state: SlotState::new(),
            data: UnsafeCell::new(None),
        }
    }

    /// Borrow the payload of a filled slot. For the block surface, which reads
    /// payloads in place from a [`SegmentWrite`] (every slot of a full segment is
    /// filled). Returns `None` for an empty slot.
    ///
    /// # Safety contract
    /// The two consumption surfaces are mutually exclusive per instance (module
    /// docs), so no stream consumer races this in-place read.
    pub(crate) fn get(&self) -> Option<&T> {
        if !self.state.is_filled() {
            return None;
        }
        // SAFETY: the slot is filled (Acquire above pairs with the producer's
        // Release), and the surfaces are mutually exclusive per instance, so no
        // stream consumer mutates this slot concurrently.
        self.data.with(|p| unsafe { (*p).as_ref() })
    }
}

/// A fixed run of `slots.len()` slots covering sequences `[base, base + slots.len())`.
///
/// Heap-stable behind `Arc`. `next` chains to the successor for the consumer's
/// lock-free hop. The slot count is fixed at construction and is read from
/// `slots.len()` wherever the segment's sequence span is needed.
struct Segment<T> {
    /// Sequence number of slot 0. Immutable after construction.
    base: u64,
    /// The slots, boxed so segment size is a runtime value rather than a const.
    slots: Box<[Slot<T>]>,
    /// Successor segment, or null. Published `Release` by the issuer when it grows
    /// past this segment; loaded `Acquire` by the consumer at a segment boundary.
    /// The pointee stays alive via the deque's `Arc` (ordering rule 4), so the
    /// consumer may reconstruct an `Arc` from this pointer.
    next: AtomicPtr<Segment<T>>,
    /// Number of filled slots. Contended across this segment's producers, hence
    /// padded off the header's read-mostly fields. The fill that brings it to
    /// `slots.len()` seals the segment (`OPEN → FULL`).
    filled_count: CachePadded<AtomicUsize>,
    /// Block-surface lifecycle: `OPEN → FULL → DRAINING → DRAINED`.
    state: SegState,
}

impl<T> Segment<T> {
    fn new(base: u64, seg_size: usize) -> Self {
        let slots = (0..seg_size)
            .map(|_| Slot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            base,
            slots,
            next: AtomicPtr::new(std::ptr::null_mut()),
            filled_count: CachePadded::new(AtomicUsize::new(0)),
            state: SegState::new(),
        }
    }

    /// Number of slots, i.e. the span of sequence numbers this segment covers.
    fn len(&self) -> u64 {
        self.slots.len() as u64
    }
}

/// State behind the single internal lock (issuer only).
struct Locked<T> {
    /// Primary level: segments spanning the live window, front = oldest.
    segments: std::collections::VecDeque<Arc<Segment<T>>>,
    /// Next sequence number to hand out.
    issued: u64,
}

/// Shared interior, held by `Arc` from every `PagedRecvBuffer`/`RecvBufferConsumer` handle and
/// from every outstanding [`SegmentWrite`] token.
struct Inner<T> {
    /// The one lock — guards the segment deque and `issued`. Off the hot paths.
    locked: Mutex<Locked<T>>,
    /// In-order delivery cursor. Written only by the consumer, read by the issuer
    /// for reclaim, so it is atomic and padded off the lock's cache line.
    consumed: CachePadded<AtomicU64>,
    /// Slots per segment for this instance. Fixed at construction; used to size
    /// the initial segment and every grown successor.
    seg_size: usize,
}

/// Producer + block handle to an in-order delivery buffer. Cloneable; share across
/// the issuer, producers, and block-surface consumer. The stream surface is the
/// separate [`RecvBufferConsumer`]. See module docs.
#[derive(Clone)]
pub(crate) struct PagedRecvBuffer<T> {
    inner: Arc<Inner<T>>,
}

/// Unique stream-surface handle: in-order single-payload delivery via
/// [`poll_next`](Self::poll_next). Not cloneable — its existence is the
/// single-consumer guarantee. Caches the current segment and cursor so delivery
/// and the segment hop are lock-free.
pub(crate) struct RecvBufferConsumer<T> {
    inner: Arc<Inner<T>>,
    /// The segment holding `cursor`. Kept as a strong ref so the consumer can read
    /// it lock-free; the deque also holds it (the unpoppable invariant).
    current: Arc<Segment<T>>,
    /// Local copy of the delivery cursor, mirrored to `inner.consumed` on advance.
    cursor: u64,
}

/// A producer's exclusive claim on one slot — the only means to fill a slot, and
/// consumed by [`fill`](PagedRecvBuffer::fill) so a slot cannot be written twice.
///
/// Resolves its slot at claim time and holds an `Arc<Segment<T>>`, so `fill` is
/// lock-free and the segment cannot be reclaimed while an unfilled claim remains.
///
/// A claimed-but-never-filled sequence blocks in-order delivery at that point (the
/// consumer waits on it) and holds its segment resident. The producer is obliged to
/// fill every claim; the `#[must_use]` guards against discarding a claim outright,
/// though it cannot catch a handle bound and then dropped.
#[must_use = "a claimed slot must be filled, or in-order delivery blocks at its sequence"]
pub(crate) struct SlotHandle<T> {
    seq: u64,
    seg: Arc<Segment<T>>,
    /// Index within `seg.slots` (`seq - seg.base`).
    idx: usize,
}

impl<T> SlotHandle<T> {
    /// The sequence number assigned to this slot.
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }
}

/// The result of a [`fill`](PagedRecvBuffer::fill).
///
/// `SealedSegment` is an edge signal: this fill was the one that filled a segment's
/// last slot, so a block-surface consumer should drain via
/// [`take_completed_segment`](PagedRecvBuffer::take_completed_segment). It carries no
/// sequence — the consumer locates the full segment itself — so the only
/// information is *that* a segment became drainable, not which. A stream-surface
/// consumer ignores the outcome entirely.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FillOutcome {
    /// Payload stored; this fill did not fill a segment's last slot.
    Stored,
    /// This fill filled a segment's last slot; a block consumer should drain.
    SealedSegment,
}

/// An owned token granting consumption of one `FULL` segment's payloads.
///
/// Holds an `Arc<Segment<T>>`, pinning the segment until [`complete`](Self::complete),
/// and a strong `Arc<Inner<T>>` so completion can free memory and reclaim the
/// shelf independent of issuance. It is owned rather than borrowed so it can outlive
/// the call that produced it: a synchronous consumer completes it immediately, an
/// asynchronous one parks it and completes it on a later event.
///
/// The strong ring reference keeps the buffer accounting alive for exactly the
/// outstanding-write window — the correct lifetime, since a write may be reading a
/// segment's bytes when every other handle has dropped. It is bounded (released at
/// `complete` or drop) and forms no cycle (segments do not point back at `Inner`).
///
/// Dropping the token without calling [`complete`](Self::complete) is safe: `Drop`
/// runs the same drain, so an aborted or short-circuited consumer cannot wedge
/// reclaim by leaving the segment stuck `DRAINING`.
#[must_use = "a taken segment must be completed (or dropped) to free its payloads and unblock reclaim"]
pub(crate) struct SegmentWrite<T> {
    seg: Arc<Segment<T>>,
    inner: Arc<Inner<T>>,
}

impl<T> SegmentWrite<T> {
    /// Sequence number of the segment's first slot.
    pub(crate) fn base_seq(&self) -> u64 {
        self.seg.base
    }

    /// The segment's payloads in sequence order, all `FILLED`. Borrowed for the
    /// duration of consumption; payloads are read in place, never moved out.
    pub(crate) fn payloads(&self) -> &[Slot<T>] {
        &self.seg.slots
    }

    /// Free the segment's payloads, mark it `DRAINED`, and reclaim drained front
    /// segments. Idempotent and shared by [`complete`](Self::complete) and `Drop`,
    /// guarded on the `DRAINED` state so it runs exactly once per token.
    ///
    /// Dropping the payloads here — rather than at segment pop — releases the budget
    /// the bytes hold **eagerly and per-segment**, decoupled from the in-order
    /// front-pop. A segment written out of order (e.g. seg 2 done while seg 0 still
    /// writes) frees its memory immediately even though its empty shelf waits its
    /// turn at the front.
    fn drain(&self) {
        // Idempotency guard: complete() then drop, or a double drain, is a no-op the
        // second time. Acquire pairs with the set_drained Release below.
        if self.seg.state.is_drained() {
            return;
        }
        for slot in self.seg.slots.iter() {
            // SAFETY: winning the FULL → DRAINING CAS made this token the segment's
            // sole accessor, and the two consumption surfaces are mutually exclusive
            // per instance (ordering rule 5, plus the surface contract), so no stream
            // consumer touches these slots. Freeing the payload releases whatever it
            // carries (e.g. a memory reservation).
            slot.data.with_mut(|p| unsafe {
                *p = None;
            });
        }
        self.seg.state.set_drained();
        reclaim_front(&self.inner);
    }

    /// Finish consuming the segment: free its payloads now, mark it drained, and
    /// reclaim drained front segments. Equivalent to dropping the token, but
    /// explicit at the call site.
    pub(crate) fn complete(self) {
        self.drain();
        // `self` drops here; Drop sees DRAINED and the drain is a no-op.
    }
}

impl<T> Drop for SegmentWrite<T> {
    /// Safety net: a token dropped without [`complete`](SegmentWrite::complete) —
    /// an aborted or early-returning consumer — still drains, so the segment cannot
    /// linger in `DRAINING` and stall front-reclaim behind it.
    fn drop(&mut self) {
        self.drain();
    }
}

/// Pop reclaimable segments from the front of the deque. A segment is
/// reclaimable if fully consumed (`base + len <= consumed`) or drained
/// by the block surface. Never pops the last segment so the deque is never empty.
fn reclaim_locked<T>(guard: &mut Locked<T>, consumed: u64) {
    while guard.segments.len() > 1 {
        let front = guard.segments.front().unwrap();
        let fully_consumed = front.base + front.len() <= consumed;
        let drained = front.state.is_drained();
        if fully_consumed || drained {
            guard.segments.pop_front();
        } else {
            break;
        }
    }
}

/// Take the lock and run front-reclaim.
fn reclaim_front<T>(inner: &Inner<T>) {
    let mut guard = inner.locked.lock();
    let consumed = inner.consumed.load(Ordering::Acquire);
    reclaim_locked(&mut guard, consumed);
}

// Safety: producers write to distinct slots (claimed via monotonic seq assignment
// under the lock). The consumer reads only after observing FILLED. T: Send is
// required because items cross thread boundaries.
unsafe impl<T: Send> Send for PagedRecvBuffer<T> {}
unsafe impl<T: Send> Sync for PagedRecvBuffer<T> {}
unsafe impl<T: Send> Send for RecvBufferConsumer<T> {}
unsafe impl<T: Send> Send for SegmentWrite<T> {}
unsafe impl<T: Send> Sync for SegmentWrite<T> {}

impl<T> PagedRecvBuffer<T> {
    /// Create an empty ring with the default segment size ([`DEFAULT_SEG_SIZE`]),
    /// returning the producer/block handle and the unique stream [`RecvBufferConsumer`].
    pub(crate) fn new() -> (Self, RecvBufferConsumer<T>) {
        Self::new_with_segment_size(DEFAULT_SEG_SIZE)
    }

    /// Create an empty ring whose segments hold `seg_size` slots each. One segment
    /// based at sequence 0 exists initially, shared by the producer/block handle and
    /// the consumer's `current`. Panics if `seg_size == 0`.
    pub(crate) fn new_with_segment_size(seg_size: usize) -> (Self, RecvBufferConsumer<T>) {
        assert!(seg_size > 0, "segment size must be non-zero");
        let seg0 = Arc::new(Segment::new(0, seg_size));
        let mut segments = std::collections::VecDeque::new();
        segments.push_back(seg0.clone());
        let inner = Arc::new(Inner {
            locked: Mutex::new(Locked {
                segments,
                issued: 0,
            }),
            consumed: CachePadded::new(AtomicU64::new(0)),
            seg_size,
        });
        let consumer = RecvBufferConsumer {
            inner: inner.clone(),
            current: seg0,
            cursor: 0,
        };
        (PagedRecvBuffer { inner }, consumer)
    }

    // ── Producer surface ───────────────────────────────────────────────────────

    /// Assign the next sequence number and resolve its slot, growing a tail segment
    /// if the sequence reached a new one. Takes the lock.
    pub(crate) fn claim(&self) -> SlotHandle<T> {
        let mut guard = self.inner.locked.lock();

        // Opportunistic front-reclaim: we already hold the lock, so drop any front
        // segments the consumer has passed before assigning the next sequence.
        let consumed = self.inner.consumed.load(Ordering::Acquire);
        reclaim_locked(&mut guard, consumed);

        let seq = guard.issued;
        guard.issued += 1;

        let tail = guard.segments.back().unwrap().clone();
        // The sequence space is u64; exhausting it (2^64 claims) is unreachable in
        // any real run, so this guards the invariant in debug rather than paying for
        // checked arithmetic on the hot path.
        debug_assert!(
            tail.base.checked_add(tail.len()).is_some(),
            "sequence space exhausted"
        );
        let seg = if seq >= tail.base + tail.len() {
            // Grow: allocate a successor segment of the same size.
            let new = Arc::new(Segment::new(tail.base + tail.len(), self.inner.seg_size));
            tail.next
                .store(Arc::as_ptr(&new) as *mut _, Ordering::Release);
            guard.segments.push_back(new.clone());
            debug_assert!(seq < new.base + new.len());
            new
        } else {
            tail
        };

        let idx = (seq - seg.base) as usize;
        SlotHandle { seq, seg, idx }
    }

    /// Publish a payload into its claimed slot, consuming the handle. Lock-free:
    /// writes the slot's `data`, publishes filled via [`SlotState::publish_filled`],
    /// increments the segment's `filled_count`. Returns [`FillOutcome::SealedSegment`]
    /// when this fill brings the count to the segment's slot count.
    pub(crate) fn fill(&self, handle: SlotHandle<T>, value: T) -> FillOutcome {
        let SlotHandle { seg, idx, .. } = handle;
        // Sole writer for this sequence number.
        seg.slots[idx].data.with_mut(|p| unsafe {
            *p = Some(value);
        });
        seg.slots[idx].state.publish_filled();
        let prev = seg.filled_count.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == seg.slots.len() {
            seg.state.set_full();
            FillOutcome::SealedSegment
        } else {
            FillOutcome::Stored
        }
    }

    // ── Block surface (out-of-order, segment-granular) ───────────────────────────

    /// Hand out the frontmost full, not-yet-taken segment exactly once as an owned
    /// [`SegmentWrite`] (claimed `FULL → DRAINING` via
    /// [`SegState::try_claim_for_drain`]), or `None` if no segment is full. Segments
    /// may be outstanding concurrently.
    pub(crate) fn take_completed_segment(&self) -> Option<SegmentWrite<T>> {
        let guard = self.inner.locked.lock();
        for seg in guard.segments.iter() {
            if seg.state.try_claim_for_drain() {
                return Some(SegmentWrite {
                    seg: seg.clone(),
                    inner: self.inner.clone(),
                });
            }
        }
        None
    }

    // ── Introspection (advisory) ─────────────────────────────────────────────────

    /// Advisory point-in-time skeleton in sequence space. Takes the lock (so the
    /// issuer cannot grow or reclaim mid-walk and segments cannot vanish); slot
    /// states still race under the lock-free producers and consumer, so the result
    /// is advisory and never a correctness input. Cold path.
    ///
    /// Callers that need byte offsets translate sequence numbers themselves; the
    /// ring has no notion of payload size.
    pub(crate) fn snapshot(&self) -> RecvBufferSnapshot {
        let guard = self.inner.locked.lock();
        let cursor = self.inner.consumed.load(Ordering::Acquire);
        let frontier = guard.issued;

        let mut arrived = Vec::new();
        let mut run_start: Option<u64> = None;

        for seq in cursor..frontier {
            // Locate the segment containing `seq`.
            let seg = guard
                .segments
                .iter()
                .find(|s| seq >= s.base && seq < s.base + s.len());
            let filled = match seg {
                Some(s) => {
                    let idx = (seq - s.base) as usize;
                    s.slots[idx].state.is_filled()
                }
                None => false,
            };

            if filled {
                if run_start.is_none() {
                    run_start = Some(seq);
                }
            } else if let Some(start) = run_start.take() {
                arrived.push(start..seq);
            }
        }
        if let Some(start) = run_start {
            arrived.push(start..frontier);
        }

        RecvBufferSnapshot {
            cursor,
            frontier,
            arrived,
        }
    }
}

impl<T> RecvBufferConsumer<T> {
    /// Take the next in-order payload if it has arrived, else `None`. Lock-free.
    ///
    /// Checks the slot at the cursor with [`SlotState::is_filled`]; on a hit takes
    /// the owned payload, empties the slot, advances the cursor (mirrored to
    /// `inner.consumed` with `Release`), and at a segment boundary hops `current`
    /// to the successor via the segment's `next` pointer.
    ///
    /// The cursor advances by one and no segment is reclaimed ahead of it, so a
    /// single call crosses **at most one** segment boundary — hence a single `if`,
    /// not a loop: a multi-segment skip cannot occur.
    pub(crate) fn poll_next(&mut self) -> Option<T> {
        // Hop: if the cursor reached the end of the current segment, advance to the
        // successor before reading. Crosses one boundary at most (see the contract).
        if self.cursor == self.current.base + self.current.len() {
            let raw = self.current.next.load(Ordering::Acquire);
            if raw.is_null() {
                return None;
            }
            // SAFETY: `raw` points to the successor segment, whose base is
            // `cursor` (we are at `current`'s end, so `consumed == cursor ==
            // successor.base`). A segment is popped only when `base + len <=
            // consumed`; for the successor that is `cursor + len <= cursor`, which
            // is false, so it cannot be popped while we hold here (ordering rule 4).
            // Its allocation is therefore live and still strong-referenced by the
            // deque, so increment_strong_count is sound; the matching Drop of this
            // Arc balances the increment. (`current` itself may now be poppable, but
            // we hold it via `self.current` until the reassignment below.)
            let next = unsafe {
                Arc::increment_strong_count(raw);
                Arc::from_raw(raw)
            };
            self.current = next;
        }

        let idx = (self.cursor - self.current.base) as usize;
        if !self.current.slots[idx].state.is_filled() {
            return None;
        }

        let value = self.current.slots[idx]
            .data
            .with_mut(|p| unsafe { (*p).take() })
            .unwrap();
        self.current.slots[idx].state.set_empty();
        self.cursor += 1;
        self.inner.consumed.store(self.cursor, Ordering::Release);

        Some(value)
    }
}

/// Advisory sequence-space snapshot from [`PagedRecvBuffer::snapshot`].
///
/// Three sequence numbers and the arrived runs between them describe the whole
/// state: `[0, cursor)` is delivered and gone, `cursor` is where in-order delivery
/// stands, `[cursor, frontier)` is claimed-but-undelivered, and the arrived runs
/// mark which sequences in that span have landed (the complement is claimed but not
/// yet filled). All accessors are derived from these; the raw runs are private so
/// callers express intent through the methods rather than re-deriving them.
pub(crate) struct RecvBufferSnapshot {
    /// In-order delivery cursor: the next sequence to be delivered. Everything
    /// below it has been delivered.
    pub(crate) cursor: u64,
    /// Issuance frontier: the next sequence to be claimed. Nothing at or above it
    /// exists yet.
    pub(crate) frontier: u64,
    /// Contiguous arrived (filled) sequence runs within `[cursor, frontier)`, in
    /// ascending order and non-adjacent. The gaps between them are claimed-but-
    /// unfilled sequences.
    arrived: Vec<std::ops::Range<u64>>,
}

impl RecvBufferSnapshot {
    /// Number of claimed-but-undelivered sequences: `frontier - cursor`. The size of
    /// the live window the ring is holding open.
    pub(crate) fn outstanding(&self) -> u64 {
        self.frontier - self.cursor
    }

    /// The first claimed-but-unfilled sequence at or after the cursor — the
    /// head-of-line blocker that in-order delivery is waiting on — or `None` if every
    /// outstanding sequence has arrived (delivery is limited only by the consumer).
    ///
    /// It is the start of the first gap: the cursor itself if the cursor has not
    /// arrived, otherwise the end of the arrived run that begins at the cursor.
    pub(crate) fn head_of_line(&self) -> Option<u64> {
        match self.arrived.first() {
            // A run starts exactly at the cursor: the blocker is just past it.
            Some(first) if first.start == self.cursor => {
                if first.end < self.frontier {
                    Some(first.end)
                } else {
                    None
                }
            }
            // The cursor itself has not arrived (a run exists later, or none does)
            // but there is something outstanding.
            _ if self.cursor < self.frontier => Some(self.cursor),
            // Nothing outstanding.
            _ => None,
        }
    }

    /// The gaps between arrived runs within `[cursor, frontier)` — sequences claimed
    /// but not yet filled, in ascending order. Their union with the arrived runs is
    /// exactly `[cursor, frontier)`.
    pub(crate) fn pending(&self) -> Vec<std::ops::Range<u64>> {
        let mut gaps = Vec::new();
        let mut at = self.cursor;
        for run in &self.arrived {
            if at < run.start {
                gaps.push(at..run.start);
            }
            at = run.end;
        }
        if at < self.frontier {
            gaps.push(at..self.frontier);
        }
        gaps
    }

    /// The contiguous arrived (filled) runs within `[cursor, frontier)`.
    pub(crate) fn arrived(&self) -> &[std::ops::Range<u64>] {
        &self.arrived
    }
}

#[cfg(test)]
impl<T> PagedRecvBuffer<T> {
    /// Number of segments currently in the deque (test introspection only).
    fn segment_count(&self) -> usize {
        self.inner.locked.lock().segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// All permutations of `0..n`, via Heap's algorithm. Used to exhaustively drive
    /// fill orders in the sequencing tests.
    fn permutations(n: usize) -> Vec<Vec<usize>> {
        let mut items: Vec<usize> = (0..n).collect();
        let mut out = Vec::new();
        let mut c = vec![0usize; n];
        out.push(items.clone());
        let mut i = 0;
        while i < n {
            if c[i] < i {
                if i % 2 == 0 {
                    items.swap(0, i);
                } else {
                    items.swap(c[i], i);
                }
                out.push(items.clone());
                c[i] += 1;
                i = 0;
            } else {
                c[i] = 0;
                i += 1;
            }
        }
        out
    }

    #[test]
    fn new_then_poll_returns_none() {
        let (_ring, mut consumer) = PagedRecvBuffer::<u64>::new();
        assert_eq!(consumer.poll_next(), None);
    }

    #[test]
    fn single_item_round_trip() {
        let (ring, mut consumer) = PagedRecvBuffer::new();
        let handle = ring.claim();
        assert_eq!(handle.seq(), 0);
        ring.fill(handle, 42u64);
        assert_eq!(consumer.poll_next(), Some(42));
        assert_eq!(consumer.poll_next(), None);
    }

    #[test]
    fn out_of_order_fill_within_segment() {
        let (ring, mut consumer) = PagedRecvBuffer::new();
        let h0 = ring.claim();
        let h1 = ring.claim();
        let h2 = ring.claim();

        // Fill out of order: 2, 0, 1
        ring.fill(h2, 200u64);
        // Cannot deliver seq 0 yet.
        assert_eq!(consumer.poll_next(), None);

        ring.fill(h0, 0u64);
        // Now seq 0 is available but seq 1 blocks further delivery.
        assert_eq!(consumer.poll_next(), Some(0));
        assert_eq!(consumer.poll_next(), None);

        ring.fill(h1, 100u64);
        assert_eq!(consumer.poll_next(), Some(100));
        assert_eq!(consumer.poll_next(), Some(200));
        assert_eq!(consumer.poll_next(), None);
    }

    #[test]
    fn cross_segment_delivery() {
        let seg_size = 4;
        let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        // Claim enough to force at least 2 segment growths (3 segments total).
        let n = seg_size * 3 + 2;
        let handles: Vec<_> = (0..n).map(|_| ring.claim()).collect();
        // Fill all in order.
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        // Drain all in order.
        for i in 0..n {
            assert_eq!(consumer.poll_next(), Some(i as u64), "mismatch at seq {i}");
        }
        assert_eq!(consumer.poll_next(), None);
    }

    #[test]
    fn fill_outcome_sealed_on_last_fill() {
        let seg_size = 8;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            let outcome = ring.fill(h, i as u64);
            if i < seg_size - 1 {
                assert_eq!(outcome, FillOutcome::Stored, "expected Stored at fill {i}");
            } else {
                assert_eq!(
                    outcome,
                    FillOutcome::SealedSegment,
                    "expected SealedSegment on last fill"
                );
            }
        }
    }

    #[test]
    fn block_path_take_and_complete() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        // Fill a full segment.
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }

        // Take the completed segment.
        let sw = ring
            .take_completed_segment()
            .expect("should have a full segment");
        assert_eq!(sw.base_seq(), 0);

        // Second immediate take returns None (already claimed).
        assert!(ring.take_completed_segment().is_none());

        // Complete it.
        sw.complete();

        // After completing, a new full segment can be taken.
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (seg_size + i) as u64);
        }
        let sw2 = ring
            .take_completed_segment()
            .expect("second full segment should be takeable");
        assert_eq!(sw2.base_seq(), seg_size as u64);
        sw2.complete();
    }

    #[test]
    fn take_returns_none_when_partial() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        // Fill only some slots.
        let handles: Vec<_> = (0..seg_size - 1).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        assert!(ring.take_completed_segment().is_none());
    }

    /// Block surface reads payloads in place via `Slot::get`, then completes.
    #[test]
    fn block_path_reads_payloads_in_place() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (i * 10) as u64);
        }
        let sw = ring.take_completed_segment().expect("full segment");
        let seen: Vec<u64> = sw.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen, vec![0, 10, 20, 30]);
        sw.complete();
    }

    /// `Slot::get` returns the payload for a filled slot. (The empty-slot `None`
    /// branch is not reachable through the public block surface, which hands out only
    /// FULL segments — every slot filled.)
    #[test]
    fn slot_get_returns_filled_payload() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (i * 7) as u64);
        }
        let sw = ring.take_completed_segment().expect("full segment");
        for (i, slot) in sw.payloads().iter().enumerate() {
            assert_eq!(*slot.get().expect("filled"), (i * 7) as u64);
        }
        sw.complete();
    }

    /// A `SegmentWrite` dropped without `complete()` still drains: the segment is
    /// reclaimed (not left stuck DRAINING), so reclaim does not wedge behind it.
    #[test]
    fn drop_without_complete_reclaims() {
        let seg_size = 2;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        // Fill seg0 full and take it.
        for i in 0..seg_size {
            let h = ring.claim();
            ring.fill(h, i as u64);
        }
        let sw = ring.take_completed_segment().expect("seg0 full");
        assert_eq!(sw.base_seq(), 0);
        // Drop WITHOUT complete(): the Drop safety net marks seg0 DRAINED.
        drop(sw);
        // `claim` runs reclaim before it grows, so the drained front pops on the
        // claim *after* seg1 exists. Two claims: the first grows seg1, the second
        // observes seg0 drained and reclaims it. A segment stuck DRAINING would
        // never pop and the count would climb without bound.
        for i in 0..2 {
            let h = ring.claim();
            ring.fill(h, 99 + i);
        }
        assert_eq!(
            ring.segment_count(),
            1,
            "dropped-without-complete segment must drain and reclaim, not wedge"
        );
    }

    /// `complete()` and `Drop` are exactly-once: payloads are freed once, never twice
    /// (the DRAINED state guard makes the second drain a no-op).
    #[test]
    fn complete_then_drop_frees_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};
        use std::sync::Arc as StdArc;

        // Payload that bumps a shared counter on drop.
        struct DropCounter(StdArc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, O::SeqCst);
            }
        }

        let drops = StdArc::new(AtomicUsize::new(0));
        let seg_size = 2;
        let (ring, _consumer) = PagedRecvBuffer::<DropCounter>::new_with_segment_size(seg_size);
        for _ in 0..seg_size {
            let h = ring.claim();
            ring.fill(h, DropCounter(drops.clone()));
        }
        let sw = ring.take_completed_segment().expect("full");
        // complete() frees both payloads.
        sw.complete();
        assert_eq!(
            drops.load(O::SeqCst),
            seg_size,
            "complete frees every payload once"
        );
        // The implicit Drop after complete() must NOT double-free (state guard).
        assert_eq!(
            drops.load(O::SeqCst),
            seg_size,
            "Drop after complete is a no-op; no double free"
        );
    }

    /// Eager per-segment reclaim: a later segment completed out of order frees its
    /// payloads immediately, even while an earlier segment is still open.
    #[test]
    fn out_of_order_complete_frees_eagerly() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};
        use std::sync::Arc as StdArc;

        struct DropCounter(StdArc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, O::SeqCst);
            }
        }

        let drops = StdArc::new(AtomicUsize::new(0));
        let seg_size = 2;
        let (ring, _consumer) = PagedRecvBuffer::<DropCounter>::new_with_segment_size(seg_size);
        // Claim two full segments' worth (seg0: 0,1 ; seg1: 2,3).
        let handles: Vec<_> = (0..seg_size * 2).map(|_| ring.claim()).collect();
        // Fill seg1 fully but leave seg0 entirely unfilled (out-of-order arrival).
        // handles[2], handles[3] are seg1.
        let mut it = handles.into_iter();
        let h0 = it.next().unwrap(); // seg0 slot0 — held, not filled yet
        let h1 = it.next().unwrap(); // seg0 slot1 — held, not filled yet
        let h2 = it.next().unwrap();
        let h3 = it.next().unwrap();
        ring.fill(h2, DropCounter(drops.clone()));
        ring.fill(h3, DropCounter(drops.clone()));
        // seg1 is now FULL while seg0 is still OPEN. Take and complete seg1.
        let sw = ring.take_completed_segment().expect("seg1 is full");
        assert_eq!(
            sw.base_seq(),
            seg_size as u64,
            "frontmost full segment is seg1"
        );
        sw.complete();
        // seg1's two payloads freed immediately, even though seg0 sits ahead unfilled.
        assert_eq!(
            drops.load(O::SeqCst),
            seg_size,
            "out-of-order completed segment frees its budget eagerly"
        );
        // Fill seg0 so the held handles do not leak into later assertions.
        ring.fill(h0, DropCounter(drops.clone()));
        ring.fill(h1, DropCounter(drops.clone()));
    }

    /// Snapshot cursor tracks delivery after the stream consumer advances.
    #[test]
    fn snapshot_cursor_after_drain() {
        let seg_size = 2;
        let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let handles: Vec<_> = (0..5).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        // Deliver three in order.
        for _ in 0..3 {
            consumer.poll_next().unwrap();
        }
        let snap = ring.snapshot();
        assert_eq!(
            snap.cursor, 3,
            "snapshot cursor reflects the consumer's position"
        );
        assert_eq!(snap.frontier, 5);
        assert_eq!(snap.outstanding(), 2);
        assert_eq!(snap.arrived(), &[3..5]);
        assert_eq!(
            snap.head_of_line(),
            None,
            "remaining outstanding have all arrived"
        );
    }

    /// Exhaustive within-segment sequencing (invariant 3): for every fill order of a
    /// single segment, delivery is the in-order prefix and yields all payloads
    /// exactly once. Deterministic and single-threaded — the property is ordering
    /// logic, not memory ordering, so it does not belong in loom.
    #[test]
    fn exhaustive_fill_order_within_segment() {
        let seg_size = 4;
        for perm in permutations(seg_size) {
            let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
            let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();

            // Fill in the permuted order; after each fill, everything delivered so
            // far must equal the in-order prefix that is now contiguous from 0.
            let mut filled = vec![false; seg_size];
            let mut delivered = Vec::new();
            for &slot in &perm {
                ring.fill_at(&handles, slot, (slot * 10) as u64);
                filled[slot] = true;
                // The deliverable prefix is the longest run of filled-from-0.
                let mut deliverable = 0;
                while deliverable < seg_size && filled[deliverable] {
                    deliverable += 1;
                }
                while let Some(v) = consumer.poll_next() {
                    delivered.push(v);
                }
                let expect: Vec<u64> = (0..deliverable as u64).map(|i| i * 10).collect();
                assert_eq!(
                    delivered, expect,
                    "perm {perm:?}: wrong prefix after filling {slot}"
                );
            }
            // Everything delivered, nothing left.
            assert_eq!(consumer.poll_next(), None, "perm {perm:?}: residue");
        }
    }

    /// Exhaustive cross-segment sequencing: every fill order across three segments
    /// delivers the in-order prefix and yields all payloads exactly once, covering
    /// the hop's sequencing logic deterministically (concurrency is loom's job).
    #[test]
    fn exhaustive_fill_order_across_segments() {
        let seg_size = 2;
        let n = seg_size * 3; // 6 sequences across 3 segments
        for perm in permutations(n) {
            let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
            let handles: Vec<_> = (0..n).map(|_| ring.claim()).collect();

            let mut delivered = Vec::new();
            for &slot in &perm {
                ring.fill_at(&handles, slot, (slot * 10) as u64);
                while let Some(v) = consumer.poll_next() {
                    delivered.push(v);
                }
            }
            let expect: Vec<u64> = (0..n as u64).map(|i| i * 10).collect();
            assert_eq!(delivered, expect, "perm {perm:?}: out-of-order or lost");
            assert_eq!(consumer.poll_next(), None);
        }
    }

    #[test]
    fn snapshot_non_contiguous() {
        let (ring, _consumer) = PagedRecvBuffer::new();
        let mut handles = Vec::new();
        for _ in 0..6 {
            handles.push(ring.claim());
        }

        // Fill seq 0, 1, 3, 5 — leave gaps at 2 and 4.
        ring.fill(handles.remove(0), 100u64); // seq 0
        ring.fill(handles.remove(0), 101u64); // seq 1
        let h2 = handles.remove(0); // seq 2 — not filled
        ring.fill(handles.remove(0), 103u64); // seq 3
        let h4 = handles.remove(0); // seq 4 — not filled
        ring.fill(handles.remove(0), 105u64); // seq 5

        let snap = ring.snapshot();
        assert_eq!(snap.cursor, 0);
        assert_eq!(snap.frontier, 6);
        assert_eq!(snap.arrived(), &[0..2, 3..4, 5..6]);
        assert_eq!(snap.outstanding(), 6);
        // 0,1 arrived; the run starting at the cursor ends at 2, so 2 is the blocker.
        assert_eq!(snap.head_of_line(), Some(2));
        assert_eq!(snap.pending(), vec![2..3, 4..5]);

        // Fill the gaps to avoid leaking.
        ring.fill(h2, 102u64);
        ring.fill(h4, 104u64);
    }

    /// Snapshot accessors when the cursor sequence itself has not arrived.
    #[test]
    fn snapshot_head_of_line_at_cursor() {
        let (ring, _consumer) = PagedRecvBuffer::new();
        let h0 = ring.claim();
        let h1 = ring.claim();
        // Fill only seq 1; seq 0 (the cursor) is the blocker.
        ring.fill(h1, 11u64);

        let snap = ring.snapshot();
        assert_eq!(snap.cursor, 0);
        assert_eq!(snap.frontier, 2);
        assert_eq!(snap.head_of_line(), Some(0));
        assert_eq!(snap.pending(), vec![0..1]);
        assert_eq!(snap.arrived(), &[1..2]);

        ring.fill(h0, 10u64);
    }

    /// Snapshot when the whole outstanding window has arrived: no head-of-line
    /// blocker (delivery is consumer-limited only).
    #[test]
    fn snapshot_fully_arrived_no_blocker() {
        let (ring, _consumer) = PagedRecvBuffer::new();
        let h0 = ring.claim();
        let h1 = ring.claim();
        ring.fill(h0, 10u64);
        ring.fill(h1, 11u64);

        let snap = ring.snapshot();
        assert_eq!(snap.outstanding(), 2);
        assert_eq!(snap.head_of_line(), None);
        assert!(snap.pending().is_empty());
        assert_eq!(snap.arrived(), &[0..2]);
    }

    #[test]
    fn reclaim_via_stream_and_claim() {
        let seg_size = 4;
        let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        // Fill two full segments.
        let n = seg_size * 2;
        let handles: Vec<_> = (0..n).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        // At this point we have 2 segments in the deque.
        assert_eq!(ring.segment_count(), 2);

        // Consume all of the first segment and one item into the second.
        for _ in 0..seg_size + 1 {
            consumer.poll_next().unwrap();
        }

        // Trigger reclaim by claiming more (which runs reclaim_locked).
        let h = ring.claim();
        ring.fill(h, 999u64);

        // The first segment should have been reclaimed.
        assert_eq!(
            ring.segment_count(),
            2,
            "first segment should be reclaimed, leaving the second + newly grown"
        );
    }

    #[test]
    fn concurrent_producer_consumer() {
        let seg_size = 4;
        let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let n = seg_size * 4;
        let ring_clone = ring.clone();

        std::thread::scope(|s| {
            // Producer thread: claim and fill in order.
            s.spawn(move || {
                for i in 0..n {
                    let h = ring_clone.claim();
                    ring_clone.fill(h, i as u64);
                }
            });

            // Consumer in main thread.
            let mut received = Vec::with_capacity(n);
            while received.len() < n {
                if let Some(v) = consumer.poll_next() {
                    received.push(v);
                }
            }
            let expected: Vec<u64> = (0..n as u64).collect();
            assert_eq!(received, expected);
        });
    }
}

#[cfg(test)]
impl<T> PagedRecvBuffer<T> {
    /// Test helper: fill the handle for sequence `slot`, taken out of a slice of
    /// claimed handles by index. Lets a test drive fills in an arbitrary order
    /// without consuming the whole slice up front. Each handle is filled at most
    /// once; the cell is taken on use.
    fn fill_at(&self, handles: &[SlotHandle<T>], slot: usize, value: T) {
        // Fill by index without consuming the handle, so a test can drive an
        // arbitrary fill order over a pre-claimed slice.
        let h = &handles[slot];
        let seg = h.seg.clone();
        let idx = h.idx;
        seg.slots[idx].data.with_mut(|p| unsafe {
            *p = Some(value);
        });
        seg.slots[idx].state.publish_filled();
        let prev = seg.filled_count.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == seg.slots.len() {
            seg.state.set_full();
        }
    }
}

// Loom model-checks the real structure directly (no model of it): the compat layer
// swaps in loom's atomics/Arc/Mutex under `cfg(s3_tm_loom)`. Segment size is set to
// 2 per ring (via new_with_segment_size) so boundary-crossing interleavings stay
// tractable; the algorithm is identical at any size. Six models cover the handoff,
// the segment hop, reclaim vs an outstanding write (complete and drop paths),
// multi-producer fill, and the block-take CAS. Run with:
//   RUSTFLAGS="--cfg s3_tm_loom" cargo test --lib --release loom_tests -- --test-threads=1
#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::sync::thread;

    /// Model 1 — the fill→poll_next handoff.
    ///
    /// One producer fills seq 0 on another thread while the consumer polls. The
    /// `publish_filled` (Release) / `is_filled` (Acquire) pair must ensure the
    /// consumer either sees nothing yet (`None`) or the *correct* payload — never a
    /// slot observed filled before its data write is visible. loom's `UnsafeCell`
    /// flags any read not ordered after the write.
    #[test]
    fn fill_poll_handoff() {
        loom::model(|| {
            let (ring, mut consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            let handle = ring.claim(); // seq 0, on this thread (claim takes the lock)

            let producer = thread::spawn(move || {
                ring.fill(handle, 42);
            });

            // Poll concurrently with the fill. Either outcome is legal; a torn or
            // premature read is not.
            if let Some(v) = consumer.poll_next() {
                assert_eq!(v, 42, "observed filled slot must carry the published value");
            }

            producer.join().unwrap();

            // After the producer has finished, the value is deliverable exactly once.
            let tail = consumer.poll_next();
            assert!(tail.is_none() || tail == Some(42));
        });
    }

    /// Model 2 — the segment hop.
    ///
    /// With segment size 2, seqs 0,1 live in seg0 and seq 2 forces seg1 (the issuer
    /// publishes `seg0.next` with Release). A producer fills all three while the
    /// consumer drains across the boundary, where `poll_next` loads `next` (Acquire)
    /// and reconstructs an `Arc` from the raw pointer. loom's Arc registry detects any
    /// use-after-free if the hop could observe a freed segment; the `Acquire`/`Release`
    /// on `next` must make the successor visible before use.
    #[test]
    fn segment_hop() {
        loom::model(|| {
            let (ring, mut consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);

            let producer = thread::spawn(move || {
                for i in 0..3u64 {
                    let h = ring.claim();
                    ring.fill(h, i);
                }
            });

            // Drain up to all three, tolerating not-yet-arrived gaps. Bounded so the
            // model terminates.
            let mut got = Vec::new();
            for _ in 0..6 {
                match consumer.poll_next() {
                    Some(v) => got.push(v),
                    None => {
                        if got.len() == 3 {
                            break;
                        }
                    }
                }
            }
            producer.join().unwrap();

            // Whatever prefix was delivered must be the in-order prefix 0,1,2,...
            for (i, v) in got.iter().enumerate() {
                assert_eq!(*v, i as u64, "delivery must be in order across the hop");
            }
            while let Some(v) = consumer.poll_next() {
                got.push(v);
            }
            assert_eq!(got, vec![0, 1, 2]);
        });
    }

    /// Model 3 — segment reclaim vs an outstanding write pin.
    ///
    /// Fill seg0 full (seqs 0,1) so it is takeable on the block surface. One thread
    /// takes it and `complete`s it (frees payloads, marks DRAINED, reclaims drained
    /// front segments); concurrently the other thread `claim`s seq 2 (which grows
    /// seg1 and runs front-reclaim). The `SegmentWrite` pins seg0 via its `Arc`
    /// until `complete`, so the segment must not be freed while the token is alive,
    /// and the two reclaim paths (claim's and complete's) must not double-free.
    /// loom's Arc registry asserts exactly-once teardown.
    #[test]
    fn reclaim_vs_outstanding_write() {
        loom::model(|| {
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            // Fill seg0 (seqs 0,1) to FULL.
            for i in 0..2 {
                let h = ring.claim();
                ring.fill(h, i as u64);
            }

            let ring2 = ring.clone();
            let taker = thread::spawn(move || {
                let sw = ring2
                    .take_completed_segment()
                    .expect("seg0 is full and takeable");
                assert_eq!(sw.base_seq(), 0);
                sw.complete(); // frees payloads, DRAINED, reclaim_front
            });

            // Concurrently grow + reclaim from the issuer side.
            let h = ring.claim(); // seq 2 → grows seg1, runs reclaim_locked
            ring.fill(h, 99);

            taker.join().unwrap();
            assert!(ring.segment_count() >= 1);
        });
    }

    /// Model 4 — two producers, out-of-order fill, one consumer.
    ///
    /// Both sequences of seg0 are claimed on this thread (claim is serialized under
    /// the lock), then producer A fills seq 1 and producer B fills seq 0 concurrently
    /// while the consumer drains. This is the multi-producer handoff that Model 1
    /// does not exercise: two `filled_count` `fetch_add`s race, exactly one observes
    /// the count reach the segment size and seals it, and the consumer must still see
    /// each payload exactly once and strictly in order regardless of which fill lands
    /// first. loom's `UnsafeCell` and Arc registry catch a torn read or a lost/dup
    /// payload.
    #[test]
    fn two_producers_out_of_order() {
        loom::model(|| {
            let (ring, mut consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            let h0 = ring.claim(); // seq 0
            let h1 = ring.claim(); // seq 1

            let ring_a = ring.clone();
            let a = thread::spawn(move || {
                ring_a.fill(h1, 1); // out of order: seq 1 first
            });
            let b = thread::spawn(move || {
                ring.fill(h0, 0);
            });

            // Drain concurrently; bounded.
            let mut got = Vec::new();
            for _ in 0..4 {
                if let Some(v) = consumer.poll_next() {
                    got.push(v);
                }
                if got.len() == 2 {
                    break;
                }
            }
            a.join().unwrap();
            b.join().unwrap();
            while let Some(v) = consumer.poll_next() {
                got.push(v);
            }
            // In-order, exactly once, both present.
            assert_eq!(got, vec![0, 1]);
        });
    }

    /// Model 5 — two block takers race one full segment.
    ///
    /// seg0 is filled FULL, then two threads call `take_completed_segment`
    /// concurrently. The `FULL → DRAINING` CAS must hand the segment to exactly one:
    /// one thread gets `Some(SegmentWrite)`, the other `None`. This proves the
    /// at-most-once handout invariant under contention, which the single-threaded
    /// "second take returns None" test cannot.
    #[test]
    fn two_takers_race_one_segment() {
        loom::model(|| {
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            for i in 0..2 {
                let h = ring.claim();
                ring.fill(h, i as u64);
            }

            let ring2 = ring.clone();
            let t1 = thread::spawn(move || ring2.take_completed_segment().map(|sw| sw.base_seq()));
            let t2 = thread::spawn(move || ring.take_completed_segment().map(|sw| sw.base_seq()));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one winner, and it got seg0.
            let winners: Vec<u64> = [r1, r2].into_iter().flatten().collect();
            assert_eq!(
                winners,
                vec![0],
                "exactly one taker wins the FULL→DRAINING CAS"
            );
        });
    }

    /// Model 6 — segment drop (no complete) vs concurrent reclaim.
    ///
    /// Same shape as Model 3, but the taker **drops** the `SegmentWrite` instead of
    /// calling `complete()`. The `Drop` safety net runs the same drain (free
    /// payloads, set DRAINED, reclaim_front) concurrently with the issuer's `claim`
    /// reclaim. The drain's idempotency guard and the `Arc` pin must still give
    /// exactly-once teardown across the drop path and claim's reclaim — loom's Arc
    /// registry catches a double-free or use-after-free.
    #[test]
    fn drop_without_complete_vs_reclaim() {
        loom::model(|| {
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            for i in 0..2 {
                let h = ring.claim();
                ring.fill(h, i as u64);
            }

            let ring2 = ring.clone();
            let taker = thread::spawn(move || {
                let sw = ring2
                    .take_completed_segment()
                    .expect("seg0 is full and takeable");
                assert_eq!(sw.base_seq(), 0);
                // Drop without complete(): Drop must drain and reclaim safely.
                drop(sw);
            });

            let h = ring.claim(); // seq 2 → grows seg1, runs reclaim_locked
            ring.fill(h, 99);

            taker.join().unwrap();
            assert!(ring.segment_count() >= 1);
        });
    }
}
