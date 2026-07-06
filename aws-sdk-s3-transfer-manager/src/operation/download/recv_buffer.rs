/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! In-order delivery over out-of-order arrival.
//!
//! `PagedRecvBuffer<T>` assigns each payload a sequence number at claim time, accepts
//! completed payloads into their slots in any order from many producers, and delivers
//! them to a single consumer. It grows to absorb head-of-line backlog and shrinks as
//! delivery advances; its resident footprint tracks the gap between the oldest
//! undelivered and newest claimed sequence, not the total sequence count.
//!
//! A payload is consumed through exactly one of two surfaces (see Surface exclusion):
//! the **stream** surface delivers one payload at a time in strict sequence order; the
//! **block** surface hands out contiguous filled runs for bulk consumption (disk
//! writes) with no in-order requirement. The Structure and Locking below are shared;
//! each surface is then described on its own.
//!
//! # Structure
//!
//! A `VecDeque<Arc<Segment<T>>>` guarded by one `Mutex`; each segment holds a fixed run
//! of `seg_size` slots covering a contiguous span of sequence numbers. The deque grows
//! at the tail and is reclaimed from the front, so only the segments spanning the live
//! window are resident.
//!
//! ```text
//!   reclaim ◀── front                        tail ──▶ grow
//!   ┌─────────┐    ┌─────────┐    ┌─────────┐
//!   │  seg 0  │─nx▶│  seg 1  │─nx▶│  seg 2  │
//!   │ x x x x │    │ x x x . │    │ F . _ _ │
//!   │ 0 1 2 3 │    │ 4 5 6 7 │    │ 8 9     │
//!   │ base=0  │    │ base=4  │    │ base=8  │
//!   └─────────┘    └───────▲─┘    └─────────┘
//!                          │
//!                     cursor=7   (issued=10; cursor mirrored to inner.consumed)
//!
//!   x = delivered (below the cursor; slot emptied)   F = filled, awaiting delivery
//!   . = claimed but not yet filled (in flight)       _ = slot not yet claimed
//!   nx = next pointer   (stream-surface view)
//! ```
//!
//! In this picture (a stream consumer) seqs 0–6 are delivered (cursor at 7). Seq 7 is
//! in flight and is the head-of-line gap: the cursor cannot advance until it fills,
//! even though seq 8 has already arrived (`F` in `seg 2`) and waits behind it. Seq 9 is
//! in flight; seqs 10–11 are not yet claimed (`issued == 10`). `seg 0` is fully
//! delivered, eligible for front-reclaim on the next claim.
//!
//! A `VecDeque` reallocation moves only the `Arc` pointers; the segments are
//! heap-stable behind `Arc`, so no producer or consumer reference is invalidated by
//! growth. The only intrusive link is one `next` pointer per segment, by which the
//! stream consumer reaches the successor segment without taking the lock.
//!
//! A slot moves `EMPTY → FILLED` and, on the stream path, back to `EMPTY`:
//!
//! ```text
//!   EMPTY ──claim, then fill──▶ FILLED ──stream take──▶ EMPTY
//! ```
//!
//! `FILLED` is published with a `Release` store after the payload write; a reader
//! observes it with an `Acquire` load before reading the payload (the handoff, ordering
//! rule 2). The block surface leaves slots `FILLED` and frees their payloads at run
//! granularity rather than emptying them.
//!
//! # Locking
//!
//! One `Mutex` guards the segment deque and the `issued` counter. Only three paths take
//! it — `claim` (assign a sequence, maybe grow, opportunistic reclaim),
//! `take_drain_run` (scan a filled run and claim it by advancing `claim_cursor`), and
//! `complete`/drop reclaim. Each is a short critical section — a cursor scan or a
//! `pop_front` loop — never held across the disk write itself. The two hot paths are
//! lock-free: `fill` writes a claimed slot and publishes it with a `Release` store;
//! `poll_next` reads the cursor slot under `Acquire`. One lock acquisition per claim,
//! none per fill.
//!
//! # Stream surface
//!
//! [`RecvBufferConsumer`] is the unique (non-cloneable) stream handle;
//! [`poll_next`](RecvBufferConsumer::poll_next) delivers one payload at a time in
//! strict sequence order. `poll_next` takes `&mut self`, so "single consumer" is a
//! type-level fact: the delivery cursor and the current-segment `Arc` are fields of the
//! handle rather than shared state, and `poll_next` reads and advances them — including
//! the hop to a successor segment via its `next` pointer — without taking the lock.
//!
//! The cursor is mirrored to an atomic `consumed` (the two are always equal) so the
//! issuer can read the delivery position for reclaim without synchronizing with the
//! consumer. A segment is stream-reclaimable once `base + len <= consumed`: every slot
//! it covers has been delivered.
//!
//! # Block surface
//!
//! [`PagedRecvBuffer`] is the cloneable producer/block handle;
//! [`take_drain_run`](PagedRecvBuffer::take_drain_run) hands out a contiguous filled
//! *run* as an owned [`SegmentWrite`] for bulk consumption. Runs may be taken and
//! completed out of order and concurrently, and one segment may be partitioned into
//! several runs. The block surface never uses the delivery cursor; it tracks two
//! per-segment counters instead:
//!
//! ```text
//!   block surface: one segment, seg_size = 8, drain batch = 3
//!
//!     slot      0 1 2   3 4 5   6 7
//!     payload   D D D   F F F   F .
//!               └run A┘ └run B┘
//!
//!   claim_cursor = 6   slots 0-5 claimed for draining (runs A and B)
//!   drained_count = 3  run A written to the sink and freed; run B claimed, still
//!                      draining (its slots stay FILLED, only the payload is freed)
//!   slot 6 is filled but the run beyond claim_cursor (just slot 6, slot 7 in flight)
//!   is below the drain batch, so it cannot be claimed yet
//!
//!   D = drained (payload freed)   F = filled   . = claimed, in flight
//! ```
//!
//! - `claim_cursor` — leading slots claimed for draining, advanced only under the lock
//!   in `take_drain_run`, so a run is handed out at most once and concurrent drainers
//!   take disjoint runs.
//! - `drained_count` — slots written to the sink and freed. Advanced (lock-free) when a
//!   run completes; when it reaches the segment length every claimed slot has been
//!   written and the segment is block-reclaimable.
//!
//! A *drainable run* is the contiguous `FILLED` slice between `claim_cursor` and the
//! first unfilled slot. A non-terminal claim takes it once it spans the drain batch (or
//! once it reaches the end of a full segment, so a sub-batch tail residue still drains);
//! a terminal claim takes whatever contiguous filled prefix exists. Completing a run
//! frees its payloads and advances `drained_count`; dropping the token without
//! completing runs the same drain.
//!
//! A third count, `filled_count` (slots a producer has filled, lock-free), feeds only
//! the advisory fill probe that raises the drain edge. `claim_cursor` and `filled_count`
//! are *not* mutually ordered: a drainer claims by scanning per-slot `FILLED` state, not
//! by reading `filled_count`, and a producer publishes a slot `FILLED` before bumping
//! `filled_count`. So a drainer can advance `claim_cursor` past a slot whose
//! `filled_count` increment a concurrent producer has not yet performed — from that
//! producer's view `claim_cursor > filled_count` transiently. Both are bounded by `len`,
//! and the per-slot `FILLED` state, not their relative order, gates a safe read; the
//! fill probe's `filled - claim_cursor` is therefore a saturating subtraction, never a
//! bare one.
//!
//! # Surface exclusion
//!
//! An instance is driven through one consumption surface or the other, never both: the
//! stream cursor empties slots as it passes; the block surface reads runs in place and
//! frees them. A block-only caller drops the `RecvBufferConsumer`. This exclusion is a
//! caller obligation, not type-enforced — `PagedRecvBuffer` is `Clone` and exposes the
//! block surface, so the type system does not prevent driving both at once. Doing so
//! races the stream `take` against the block in-place read and is undefined behavior;
//! the `unsafe` in both paths relies on the caller honoring this.
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
//! 4. **Segment-reclaim safety (the hop).** A front segment is removed (`pop_front`,
//!    front-only) once `base + len <= consumed` (stream-drained) or `drained_count ==
//!    len` (block-drained). On the stream path only the first applies, and only the
//!    consumer advances `consumed`, so when the consumer hops, the successor it is
//!    about to enter (base `== consumed`) has `base + len <= consumed` false and is not
//!    block-drained (the surfaces are exclusive, so `drained_count` stays 0). It cannot
//!    be popped and stays strong-referenced by the deque — which is what lets the
//!    consumer reconstruct an `Arc` from the `next` pointer without the lock. This
//!    concerns *segment* removal only; emptying a *slot's* payload leaves the segment
//!    in place and does not bear on the hop.
//! 5. **Outstanding block pin.** A live [`SegmentWrite`] holds an
//!    `Arc<Segment<T>>`, so a segment with a run still being consumed by the block
//!    surface stays alive until that token drains, even if front-reclaim pops it from
//!    the deque first.
//! 6. **Drain claim and reclaim handoff.** `claim_cursor` is advanced only under the
//!    deque lock, so a run is claimed at most once and concurrent drainers take
//!    disjoint runs; the fill probe's lock-free `Acquire` read of it is advisory only.
//!    `drained_count` is advanced with `AcqRel` after a run's payloads are freed and
//!    read with `Acquire` by front-reclaim, so a reclaimer observing `drained_count ==
//!    len` has seen those frees. The per-slot `is_filled` `Acquire` (rule 2) is what
//!    makes a claimed run's payloads safe to read; the lock orders drainers against
//!    each other and against grow/reclaim, not against the lock-free fills.
//!
//! # Caller obligations
//!
//! The ring does not bound its own growth: it allocates a segment whenever a claim
//! reaches a sequence past the current tail. The caller must bound outstanding
//! (claimed-but-undelivered) sequences, or resident memory grows without limit.

use crossbeam_utils::CachePadded;

use crate::runtime::sync::cell::UnsafeCell;
use crate::runtime::sync::sync::atomic::{
    AtomicBool, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};
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
    /// padded off the header's read-mostly fields. The fill probe compares it against
    /// `claim_cursor` and the drain batch to raise the [`FillOutcome::DrainReady`] edge.
    filled_count: CachePadded<AtomicUsize>,
    /// Number of leading slots claimed for draining by the block surface, contiguous
    /// from slot 0. Advanced only under the deque lock (in `take_drain_run`), so the
    /// claim of a run is exclusive; read lock-free by the fill probe as an advisory
    /// lower bound. Not contended (single writer under the lock), so unpadded.
    claim_cursor: AtomicUsize,
    /// Number of slots whose payload the block surface has written and freed. Reaches
    /// `slots.len()` once every claimed run has completed, at which point the segment
    /// is reclaimable. Written `AcqRel` by a draining [`SegmentWrite`]; read `Acquire`
    /// by front-reclaim, so a reclaimer observing `== len` has seen the freed payloads.
    drained_count: AtomicUsize,
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
            claim_cursor: AtomicUsize::new(0),
            drained_count: AtomicUsize::new(0),
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
    /// Drain batch, in slots: the minimum contiguous filled run the block surface
    /// coalesces into one drain, and the threshold the fill probe raises
    /// [`FillOutcome::DrainReady`] at. Defaults to `seg_size` (drain whole segments);
    /// lowered toward 1 (drain on arrival) when the caller's resident-occupancy bound
    /// is smaller than a segment, so the block surface never waits for a run it cannot
    /// accumulate. Read lock-free by the fill probe (`Relaxed`) and under the lock by
    /// `take_drain_run`.
    drain_batch: AtomicUsize,
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
/// `DrainReady` is an advisory edge signal: after this fill, either the filled count
/// reached the drain batch ahead of the slots already claimed for draining, or the
/// segment became completely filled (so a sub-batch tail residue can still drain). A
/// block-surface consumer should then attempt a drain via
/// [`take_drain_run`](PagedRecvBuffer::take_drain_run). It carries no sequence and is
/// not authoritative — the filled slots may not form a contiguous run yet (an earlier
/// slot in the batch is still in flight), in which case `take_drain_run` claims
/// nothing and the fill that later closes the gap re-raises the edge. The
/// authoritative check is `take_drain_run` under the lock. A stream-surface consumer
/// ignores the outcome entirely.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FillOutcome {
    /// Payload stored; the segment is neither full nor has its filled count reached the
    /// drain batch beyond what is already claimed for draining.
    Stored,
    /// Payload stored and a drain edge was raised (drain batch reached, or segment
    /// filled); a block consumer should attempt [`take_drain_run`](PagedRecvBuffer::take_drain_run).
    DrainReady,
}

/// An owned token granting consumption of one claimed run of a segment's payloads.
///
/// A run is the contiguous slice `[start, start + len)` of a segment's slots, claimed
/// by [`take_drain_run`](PagedRecvBuffer::take_drain_run). The token holds an
/// `Arc<Segment<T>>`, pinning the segment until [`complete`](Self::complete), and a
/// strong `Arc<Inner<T>>` so completion can free memory and reclaim the shelf
/// independent of issuance. It is owned rather than borrowed so it can outlive the
/// call that produced it: a synchronous consumer completes it immediately, an
/// asynchronous one parks it and completes it on a later event.
///
/// The strong ring reference keeps the buffer accounting alive for exactly the
/// outstanding-write window — the correct lifetime, since a write may be reading a
/// run's bytes when every other handle has dropped. It is bounded (released at
/// `complete` or drop) and forms no cycle (segments do not point back at `Inner`).
///
/// Dropping the token without calling [`complete`](Self::complete) is safe: `Drop`
/// runs the same drain, so an aborted or short-circuited consumer still frees the
/// run's payloads and advances `drained_count`, and cannot stall front-reclaim by
/// leaving the run's slots accounted-for-but-unfreed.
#[must_use = "a claimed run must be completed (or dropped) to free its payloads and unblock reclaim"]
pub(crate) struct SegmentWrite<T> {
    seg: Arc<Segment<T>>,
    inner: Arc<Inner<T>>,
    /// First slot of the claimed run within `seg.slots`.
    start: usize,
    /// Number of slots in the claimed run.
    len: usize,
    /// Exactly-once guard for [`drain`](Self::drain), shared by `complete` and `Drop`.
    /// Per-token (not per-segment): a segment may have several outstanding runs, each
    /// of which must free its own slots exactly once.
    drained: AtomicBool,
}

impl<T> SegmentWrite<T> {
    /// Sequence number of the run's first slot.
    #[cfg(test)]
    pub(crate) fn base_seq(&self) -> u64 {
        self.seg.base + self.start as u64
    }

    /// The run's payloads in sequence order, all `FILLED`. Borrowed for the duration
    /// of consumption; payloads are read in place, never moved out.
    pub(crate) fn payloads(&self) -> &[Slot<T>] {
        &self.seg.slots[self.start..self.start + self.len]
    }

    /// Free the run's payloads, advance the segment's `drained_count`, and reclaim
    /// drained front segments. Returns the number of payloads freed **by this call**
    /// (zero on a repeat, since the drain runs at most once per token). Idempotent and
    /// shared by [`complete`](Self::complete) and `Drop`, guarded on the token's
    /// `drained` flag.
    ///
    /// The freed count is the caller's read-ahead occupancy release: a claimed run is
    /// entirely filled (`take_drain_run` scans only filled slots), so it equals the run
    /// length, but counting the payloads actually taken keeps occupancy exact
    /// regardless. The buffer does not track a running `released` total — it reports the
    /// per-run count and lets the download layer account for it under its state lock.
    ///
    /// Dropping the payloads here — rather than at segment pop — releases the budget
    /// the bytes hold **eagerly and per-run**, decoupled from the in-order front-pop.
    /// A run written out of order (e.g. a later segment completed while an earlier one
    /// still writes) frees its memory immediately even though its shelf waits its turn
    /// at the front.
    fn drain(&self) -> u64 {
        // Idempotency guard: complete() then drop, or a double drain, frees nothing the
        // second time. AcqRel so the freeing below happens once.
        if self.drained.swap(true, Ordering::AcqRel) {
            return 0;
        }
        let mut freed = 0u64;
        for slot in self.payloads() {
            // SAFETY: advancing `claim_cursor` under the lock in `take_drain_run` made
            // this token the sole accessor of `[start, start + len)`, and the two
            // consumption surfaces are mutually exclusive per instance (ordering rule
            // 5, plus the surface contract), so no other drainer or stream consumer
            // touches these slots. Freeing the payload releases whatever it carries
            // (e.g. a memory reservation).
            slot.data.with_mut(|p| unsafe {
                if (*p).take().is_some() {
                    freed += 1;
                }
            });
        }
        // Publish this run as drained. `AcqRel` so a reclaimer that reads `== len`
        // (`Acquire`) has seen the payload frees above. Adds `len`, not the freed
        // count, because reclaim gates on every claimed slot being accounted for.
        self.seg.drained_count.fetch_add(self.len, Ordering::AcqRel);
        reclaim_front(&self.inner);
        freed
    }

    /// Finish consuming the run: free its payloads now, advance `drained_count`, and
    /// reclaim drained front segments. Returns the parts freed (the run length) so the
    /// caller can release that much read-ahead occupancy. Equivalent to dropping the
    /// token, but explicit at the call site and it hands back the freed count.
    pub(crate) fn complete(self) -> u64 {
        // `drain` runs the payload free + reclaim; `self` then drops here and its Drop
        // sees the `drained` guard set, so the drop-time drain is a no-op.
        self.drain()
    }
}

impl<T> Drop for SegmentWrite<T> {
    /// Safety net: a token dropped without [`complete`](SegmentWrite::complete) —
    /// an aborted or early-returning consumer — still drains, so the run's slots are
    /// freed and counted toward `drained_count` and cannot stall front-reclaim. The
    /// freed count is discarded: a drop-without-complete is the IO-error/abort path,
    /// where the transfer is heading terminal and read-ahead occupancy no longer gates.
    fn drop(&mut self) {
        let _ = self.drain();
    }
}

/// Pop reclaimable segments from the front of the deque. A segment is reclaimable if
/// fully consumed by the stream surface (`base + len <= consumed`) or fully drained by
/// the block surface (every claimed slot written and freed, `drained_count == len`).
/// Never pops the last segment so the deque is never empty.
fn reclaim_locked<T>(guard: &mut Locked<T>, consumed: u64) {
    while guard.segments.len() > 1 {
        let front = guard.segments.front().unwrap();
        let fully_consumed = front.base + front.len() <= consumed;
        // `Acquire` pairs with the draining token's `AcqRel` add: observing the full
        // count here means the freed payloads are visible.
        let drained = front.drained_count.load(Ordering::Acquire) == front.slots.len();
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
// Safety: all mutation goes through `poll_next(&mut self)`, so a shared `&consumer`
// exposes only Arc/Copy state with no interior mutation — Sync is sound. Needed so a
// holder of the consumer (e.g. the download `Body`) can itself be Sync.
unsafe impl<T: Send> Sync for RecvBufferConsumer<T> {}
// Safety: a SlotHandle is claimed on one thread and may be filled on another (the
// download claims in poll_work, fills in the async execute task). It carries an
// Arc<Segment<T>> and indices; moving it and filling moves a `T`, so T: Send suffices.
unsafe impl<T: Send> Send for SlotHandle<T> {}
unsafe impl<T: Send> Send for SegmentWrite<T> {}
unsafe impl<T: Send> Sync for SegmentWrite<T> {}

impl<T> PagedRecvBuffer<T> {
    /// Create an empty ring with the default segment size ([`DEFAULT_SEG_SIZE`]),
    /// returning the producer/block handle and the unique stream [`RecvBufferConsumer`].
    #[cfg(test)]
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
            drain_batch: AtomicUsize::new(seg_size),
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
    /// increments the segment's `filled_count`. Returns [`FillOutcome::DrainReady`]
    /// when the filled count reaches the drain batch beyond the slots already claimed
    /// for draining, or when this fill completes the segment (so a sub-batch tail
    /// residue can still drain).
    pub(crate) fn fill(&self, handle: SlotHandle<T>, value: T) -> FillOutcome {
        let SlotHandle { seg, idx, .. } = handle;
        // Sole writer for this sequence number.
        seg.slots[idx].data.with_mut(|p| unsafe {
            *p = Some(value);
        });
        seg.slots[idx].state.publish_filled();
        let filled = seg.filled_count.fetch_add(1, Ordering::AcqRel) + 1;

        // Advisory drain probe. Both inputs race a concurrent `take_drain_run` (which
        // advances `claim_cursor` under the lock) and the other producers' fills, so the
        // result is only a hint to attempt a drain. `claim_cursor` is read lock-free and
        // may either lag the cursor (under-read) or run ahead of *this* fill's local
        // `filled` snapshot — another producer's later fill can let a drainer claim a run
        // past this thread's count, so `claimed > filled` is reachable; the run length is
        // therefore `saturating_sub`, never a bare subtraction. The probe can over-signal
        // — `filled` counts slots anywhere in the segment, but `take_drain_run` claims
        // only a contiguous run from the cursor, so a gap (an earlier slot still in
        // flight) yields nothing until the fill that closes it raises the edge again. It
        // cannot under-signal a drainable run: the fill that makes a run of `batch`
        // contiguous brings `filled` to at least `claimed + batch`. Authority rests
        // entirely with `take_drain_run`.
        //
        // The second term (`filled == len`) handles the segment's final residue. When
        // the drain batch does not divide the segment size, the slots past the last
        // batch-aligned boundary form a run shorter than the batch; no further fill can
        // extend it (the segment is full), so the batch edge would never fire for it
        // and it would wait for terminal drain — pinning occupancy and wedging a window
        // smaller than a segment. The fill that completes the segment raises the edge so
        // that residue drains. It is the only fill that observes `filled == len`.
        let claimed = seg.claim_cursor.load(Ordering::Acquire);
        let batch = self.inner.drain_batch.load(Ordering::Relaxed);
        if filled == seg.slots.len() || filled.saturating_sub(claimed) >= batch {
            FillOutcome::DrainReady
        } else {
            FillOutcome::Stored
        }
    }

    /// Set the drain batch, in slots, clamped to at least 1. The block surface
    /// coalesces a contiguous filled run of at least this many slots into one drain,
    /// and the fill probe raises [`FillOutcome::DrainReady`] at this threshold. A value
    /// of 1 drains each slot as it arrives; the default is the segment size (drain
    /// whole segments). Lock-free; the next fill probe and `take_drain_run` observe it.
    pub(crate) fn set_drain_batch(&self, batch: usize) {
        self.inner
            .drain_batch
            .store(batch.max(1), Ordering::Relaxed);
    }

    // ── Block surface (out-of-order, segment-granular) ───────────────────────────

    /// Claim the frontmost drainable run as an owned [`SegmentWrite`], or `None`.
    ///
    /// Walks segments front to back under the lock. For each segment, scans the
    /// contiguous `FILLED` run beyond its `claim_cursor`. A non-terminal call claims
    /// that run only if it spans at least the drain batch, and then claims the whole
    /// run (coalescing greedily, bounded by the segment). A terminal call claims
    /// whatever contiguous filled run exists, however short — this is what drains the
    /// partial final segment at end-of-stream, which a non-terminal call would leave
    /// below the batch forever. The run is `[claim_cursor, claim_cursor + len)`.
    ///
    /// Advancing `claim_cursor` under the lock makes a run claimed at most once: a
    /// second caller, racing or sequential, observes the advanced cursor and scans
    /// beyond it, so concurrent drainers partition a segment into disjoint runs. The
    /// per-slot `is_filled` check is an `Acquire` load pairing with the producer's
    /// `publish_filled` `Release` — the same handoff [`poll_next`](RecvBufferConsumer::poll_next)
    /// relies on — so the claimed run's payloads are safe to read.
    ///
    /// # Terminal caller obligation
    /// Pass `terminal` only once issuance is complete and all outstanding fills have
    /// landed: a slot claimed here must not receive a later fill (it would race the
    /// block consumer reading the run). Unfilled trailing slots are simply not claimed.
    pub(crate) fn take_drain_run(&self, terminal: bool) -> Option<SegmentWrite<T>> {
        let guard = self.inner.locked.lock();
        let batch = self.inner.drain_batch.load(Ordering::Relaxed);
        for seg in guard.segments.iter() {
            let start = seg.claim_cursor.load(Ordering::Relaxed);
            // Scan the contiguous filled run beyond what is already claimed.
            let mut end = start;
            while end < seg.slots.len() && seg.slots[end].state.is_filled() {
                end += 1;
            }
            let avail = end - start;
            // A non-terminal claim takes the run when it reaches the batch, or when it
            // reaches the end of a full segment (no further fill can extend it, so a
            // sub-batch residue at the segment tail must still drain — otherwise a
            // window smaller than a segment would pin that residue forever). A terminal
            // claim takes whatever contiguous filled prefix exists.
            let at_segment_end = end == seg.slots.len();
            let take = if terminal || avail >= batch || (avail > 0 && at_segment_end) {
                avail
            } else {
                0
            };
            if take > 0 {
                // Claim the run under the lock: the next scanner sees the advanced
                // cursor and cannot re-claim these slots.
                seg.claim_cursor.store(start + take, Ordering::Relaxed);
                return Some(SegmentWrite {
                    seg: seg.clone(),
                    inner: self.inner.clone(),
                    start,
                    len: take,
                    drained: AtomicBool::new(false),
                });
            }
        }
        None
    }

    /// The in-order delivery cursor: the next sequence the consumer will deliver, i.e.
    /// the count of sequences already delivered. Lock-free read of the shared cursor.
    ///
    /// This is the in-order reclaim threshold, not the read-ahead gate's denominator:
    /// the block surface delivers to disk without advancing this cursor. Occupancy
    /// accounting (`released`) is the caller's concern — the buffer reports parts freed
    /// via the return value of [`poll_next`](RecvBufferConsumer::poll_next) (one) and
    /// [`SegmentWrite::complete`](SegmentWrite::complete) (the run length) and does not
    /// track a running total itself.
    #[cfg(test)]
    pub(crate) fn consumed(&self) -> u64 {
        self.inner.consumed.load(Ordering::Acquire)
    }

    // ── Introspection (advisory) ─────────────────────────────────────────────────

    /// Advisory point-in-time skeleton in sequence space. Takes the lock (so the
    /// issuer cannot grow or reclaim mid-walk and segments cannot vanish); slot
    /// states still race under the lock-free producers and consumer, so the result
    /// is advisory and never a correctness input. Cold path.
    ///
    /// Callers that need byte offsets translate sequence numbers themselves; the
    /// ring has no notion of payload size.
    #[cfg(test)]
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
            // successor.base`). A segment is popped when `base + len <= consumed` or
            // when it is block-drained (`drained_count == len`); for the successor the
            // first is `cursor + len <= cursor`, false, and the second cannot hold
            // because driving the block surface alongside the stream is forbidden (the
            // surface contract), so `drained_count` stays 0. It therefore cannot be
            // popped while we hold here (ordering rule 4). Its allocation is live and
            // still strong-referenced by the deque, so increment_strong_count is sound;
            // the matching Drop of this Arc balances the increment. (`current` itself
            // may now be poppable, but we hold it via `self.current` until the
            // reassignment below.)
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
        // This delivery freed exactly one part's payload. The caller releases that
        // one part of read-ahead occupancy (under its state lock, ordered against the
        // issuance gate) — the buffer does not track occupancy itself. A `Some` return
        // is therefore also the signal "one part freed".
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
#[cfg(test)]
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

#[cfg(test)]
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

    /// Base sequence of the front (oldest) segment (test introspection only). Rises as
    /// front segments are reclaimed, so a test can assert *which* segment is gone, not
    /// merely how many remain.
    fn front_base(&self) -> u64 {
        self.inner.locked.lock().segments.front().unwrap().base
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
        seg.filled_count.fetch_add(1, Ordering::AcqRel);
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

    /// With the default batch (the segment size) the probe raises `DrainReady` exactly
    /// on the fill that completes the segment — the same edge the prior full-segment
    /// seal produced.
    #[test]
    fn fill_outcome_drain_ready_on_last_fill() {
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
                    FillOutcome::DrainReady,
                    "expected DrainReady on the segment-completing fill"
                );
            }
        }
    }

    /// At the default batch (the segment size), `take_drain_run(false)` yields a whole
    /// segment as one run, exactly once, and the next segment after completion.
    #[test]
    fn block_path_take_and_complete() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        // Fill a full segment.
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }

        // Claim the full segment as one run.
        let sw = ring
            .take_drain_run(false)
            .expect("should have a full segment");
        assert_eq!(sw.base_seq(), 0);
        assert_eq!(sw.payloads().len(), seg_size);

        // Second immediate take returns None (whole segment already claimed).
        assert!(ring.take_drain_run(false).is_none());

        // Complete it.
        sw.complete();

        // After completing, a new full segment can be taken.
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (seg_size + i) as u64);
        }
        let sw2 = ring
            .take_drain_run(false)
            .expect("second full segment should be takeable");
        assert_eq!(sw2.base_seq(), seg_size as u64);
        sw2.complete();
    }

    /// A non-terminal claim waits for a full batch: a segment filled below its size
    /// (the default batch) yields nothing.
    #[test]
    fn take_returns_none_when_below_batch() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        // Fill only some slots — below the default batch (== seg_size).
        let handles: Vec<_> = (0..seg_size - 1).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        assert!(ring.take_drain_run(false).is_none());
    }

    /// A relief-regime batch below the segment size drains a segment in disjoint runs
    /// as fills arrive incrementally: a batch's worth lands and drains, then the next.
    /// (A claim is greedy — it takes the whole contiguous filled run, bounded by what
    /// has arrived — so partition follows arrival, not a fixed slice size.)
    #[test]
    fn drain_runs_partition_segment_at_small_batch() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        ring.set_drain_batch(2);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();

        // First batch arrives: slots 0,1.
        ring.fill_at(&handles, 0, 0);
        ring.fill_at(&handles, 1, 10);
        let r0 = ring.take_drain_run(false).expect("first run");
        assert_eq!(r0.base_seq(), 0);
        let seen0: Vec<u64> = r0.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen0, vec![0, 10]);
        // Nothing more until the next batch arrives.
        assert!(ring.take_drain_run(false).is_none());

        // Second batch arrives: slots 2,3.
        ring.fill_at(&handles, 2, 20);
        ring.fill_at(&handles, 3, 30);
        let r1 = ring.take_drain_run(false).expect("second run");
        assert_eq!(r1.base_seq(), 2);
        let seen1: Vec<u64> = r1.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen1, vec![20, 30]);

        assert!(ring.take_drain_run(false).is_none(), "segment exhausted");
        r0.complete();
        r1.complete();
    }

    /// A gap (an unfilled slot inside the segment) stops a run at the gap. A claim
    /// takes the contiguous prefix before it, yields nothing more until the gap fills,
    /// then takes the rest.
    #[test]
    fn drain_run_stops_at_gap() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        ring.set_drain_batch(2);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        // Fill slots 0, 1, 3 — gap at slot 2.
        ring.fill_at(&handles, 0, 0);
        ring.fill_at(&handles, 1, 10);
        ring.fill_at(&handles, 3, 30);

        // The contiguous run from 0 is [0,2); slot 2 blocks the rest.
        let r0 = ring.take_drain_run(false).expect("prefix before the gap");
        assert_eq!(r0.base_seq(), 0);
        assert_eq!(r0.payloads().len(), 2);
        // Nothing more: the run beyond the cursor starts at the unfilled slot 2.
        assert!(ring.take_drain_run(false).is_none());

        // Fill the gap; now [2,4) is a contiguous run of the batch size.
        ring.fill_at(&handles, 2, 20);
        let r1 = ring.take_drain_run(false).expect("run after the gap fills");
        assert_eq!(r1.base_seq(), 2);
        let seen: Vec<u64> = r1.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen, vec![20, 30]);
        r0.complete();
        r1.complete();
    }

    /// A terminal claim takes a partial prefix below the batch that a non-terminal
    /// claim leaves untouched.
    #[test]
    fn terminal_drains_partial_below_batch() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        // Default batch == seg_size. Fill only 2 of 4.
        let handles: Vec<_> = (0..2).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (i * 10) as u64);
        }
        // Non-terminal: below batch, nothing.
        assert!(ring.take_drain_run(false).is_none());
        // Terminal: take the filled prefix.
        let sw = ring.take_drain_run(true).expect("partial tail run");
        assert_eq!(sw.base_seq(), 0);
        let seen: Vec<u64> = sw.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen, vec![0, 10]);
        sw.complete();
        // Once claimed, a second terminal call finds nothing more here.
        assert!(ring.take_drain_run(true).is_none());
    }

    /// Reclaim gates on every claimed slot being drained: a segment split into two runs
    /// reclaims only after both runs complete.
    #[test]
    fn reclaim_waits_for_all_runs_of_a_segment() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        ring.set_drain_batch(2);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        // Two batches arrive separately, so each is claimed as its own run.
        ring.fill_at(&handles, 0, 0);
        ring.fill_at(&handles, 1, 1);
        let r0 = ring.take_drain_run(false).expect("run [0,2)");
        ring.fill_at(&handles, 2, 2);
        ring.fill_at(&handles, 3, 3);
        let r1 = ring.take_drain_run(false).expect("run [2,4)");

        // Complete only the first run, then grow + reclaim. seg0 is not fully drained
        // (drained_count == 2 < 4), so it must not be reclaimed.
        r0.complete();
        let h = ring.claim(); // seq 4 → grows seg1, runs reclaim_locked
        ring.fill(h, 4);
        assert_eq!(
            ring.segment_count(),
            2,
            "seg0 has an outstanding run; must not reclaim yet"
        );

        // Complete the second run; now drained_count == len, and `complete`'s own
        // front-reclaim pops seg0 (a second segment exists, so the deque-non-empty
        // floor does not block it).
        r1.complete();
        assert_eq!(
            ring.segment_count(),
            1,
            "seg0 reclaims once every claimed run completed"
        );
    }

    /// A drain batch that does not divide the segment size leaves a sub-batch tail
    /// residue. The batch-sized prefix drains on the batch edge; the tail cannot form a
    /// batch-sized run (the segment fills first), so it drains only because the fill
    /// that completes the segment raises the edge and `take_drain_run` takes the
    /// segment-end residue. Without that path the residue would pin occupancy forever.
    #[test]
    fn non_dividing_batch_drains_segment_tail_residue() {
        let seg_size = 4;
        let batch = 3; // does not divide 4: prefix of 3, residue of 1
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        ring.set_drain_batch(batch);

        // Fill the first `batch` slots in order via the production claim/fill path, so
        // the asserted FillOutcome is the real probe's, not a test mirror. The fill that
        // reaches the batch raises the edge.
        let mut outcomes = Vec::new();
        for i in 0..batch {
            let h = ring.claim();
            outcomes.push(ring.fill(h, (i * 10) as u64));
        }
        assert_eq!(
            outcomes,
            vec![
                FillOutcome::Stored,
                FillOutcome::Stored,
                FillOutcome::DrainReady
            ],
            "the fill that reaches the batch raises DrainReady"
        );
        let r0 = ring.take_drain_run(false).expect("batch-sized prefix run");
        assert_eq!(r0.base_seq(), 0);
        assert_eq!(
            r0.payloads().len(),
            batch,
            "prefix run is exactly the batch"
        );
        r0.complete();

        // One slot of residue remains (slot 3). A non-terminal claim cannot take it —
        // it is below the batch and the segment is not yet full.
        assert!(
            ring.take_drain_run(false).is_none(),
            "sub-batch residue is not drainable while the segment is incomplete"
        );

        // Fill the final slot: this completes the segment (`filled == len`), which
        // raises the edge for exactly this residue.
        let h = ring.claim();
        assert_eq!(
            ring.fill(h, 30),
            FillOutcome::DrainReady,
            "the fill that completes the segment raises the edge for the tail residue"
        );
        let r1 = ring.take_drain_run(false).expect("segment-end residue run");
        assert_eq!(r1.base_seq(), batch as u64);
        assert_eq!(
            r1.payloads().len(),
            seg_size - batch,
            "residue is the sub-batch tail"
        );
        let freed = r1.complete();
        assert_eq!(
            freed,
            (seg_size - batch) as u64,
            "residue run frees its tail"
        );
    }

    /// A block drain reports its run's filled count as `freed` (the read-ahead
    /// occupancy the caller releases), while `consumed` (the in-order stream cursor)
    /// never moves — the two are distinct on the block path. Two batches arrive
    /// separately (a drain between them), so the segment is claimed as two runs of 2;
    /// `take_drain_run` takes the whole contiguous filled run available, so batches must
    /// be separated to observe per-run accounting.
    #[test]
    fn block_drain_reports_freed_not_consumed() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
        ring.set_drain_batch(2);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        assert_eq!(ring.consumed(), 0);

        // First batch of 2 arrives and drains as run [0,2).
        ring.fill_at(&handles, 0, 0);
        ring.fill_at(&handles, 1, 10);
        let freed = ring.take_drain_run(false).expect("run [0,2)").complete();
        assert_eq!(freed, 2, "a block drain frees the run length");
        assert_eq!(
            ring.consumed(),
            0,
            "the block surface never advances the stream cursor"
        );

        // Second batch of 2 arrives and drains as run [2,4).
        ring.fill_at(&handles, 2, 20);
        ring.fill_at(&handles, 3, 30);
        let freed = ring.take_drain_run(false).expect("run [2,4)").complete();
        assert_eq!(freed, 2, "the second run frees its own length");
        assert_eq!(ring.consumed(), 0, "consumed stays put on the block path");
    }

    /// `consumed()` reports the in-order delivery cursor, distinct from occupancy
    /// accounting (which the caller derives from per-call freed counts).
    #[test]
    fn consumed_tracks_delivery_cursor() {
        let seg_size = 2;
        let (ring, mut consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        assert_eq!(ring.consumed(), 0);
        let handles: Vec<_> = (0..3).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, i as u64);
        }
        assert_eq!(ring.consumed(), 0, "filling does not advance the cursor");
        consumer.poll_next().unwrap();
        consumer.poll_next().unwrap();
        assert_eq!(ring.consumed(), 2, "two deliveries advance the cursor to 2");
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
        let sw = ring.take_drain_run(false).expect("full segment");
        let seen: Vec<u64> = sw.payloads().iter().map(|s| *s.get().unwrap()).collect();
        assert_eq!(seen, vec![0, 10, 20, 30]);
        sw.complete();
    }

    /// `Slot::get` returns the payload for a filled slot. (The empty-slot `None`
    /// branch is not reachable through a non-terminal claim, whose run is filled by
    /// construction — every slot of the run is filled.)
    #[test]
    fn slot_get_returns_filled_payload() {
        let seg_size = 4;
        let (ring, _consumer) = PagedRecvBuffer::new_with_segment_size(seg_size);
        let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
        for (i, h) in handles.into_iter().enumerate() {
            ring.fill(h, (i * 7) as u64);
        }
        let sw = ring.take_drain_run(false).expect("full segment");
        for (i, slot) in sw.payloads().iter().enumerate() {
            assert_eq!(*slot.get().expect("filled"), (i * 7) as u64);
        }
        sw.complete();
    }

    /// A `SegmentWrite` dropped without `complete()` still drains: its run's payloads
    /// are freed and the run is counted toward `drained_count` so the segment reclaims
    /// and reclaim does not stall behind it. (Occupancy accounting is the caller's
    /// concern via `complete()`'s return value; `Drop` discards the freed count, since a
    /// drop-without-complete is the abort/error path where the gate no longer matters.)
    #[test]
    fn drop_without_complete_reclaims() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};
        use std::sync::Arc as StdArc;

        // Payload that bumps a shared counter on drop, so we can prove the drop path
        // actually freed the run's payloads — not merely advanced the reclaim counters.
        struct DropCounter(#[allow(dead_code)] StdArc<AtomicUsize>);
        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.fetch_add(1, O::SeqCst);
            }
        }

        let drops = StdArc::new(AtomicUsize::new(0));
        let seg_size = 2;
        let (ring, _consumer) = PagedRecvBuffer::<DropCounter>::new_with_segment_size(seg_size);
        // Fill seg0 full and take it as one run.
        for _ in 0..seg_size {
            let h = ring.claim();
            ring.fill(h, DropCounter(drops.clone()));
        }
        let sw = ring.take_drain_run(false).expect("seg0 full");
        assert_eq!(sw.base_seq(), 0);
        assert_eq!(
            drops.load(O::SeqCst),
            0,
            "nothing freed until the run drains"
        );

        // Drop WITHOUT complete(): the Drop safety net must free the run's payloads and
        // advance `drained_count` so the segment reclaims.
        drop(sw);
        assert_eq!(
            drops.load(O::SeqCst),
            seg_size,
            "drop-without-complete must free every payload in the run"
        );

        // `claim` runs reclaim before it grows, so the drained front pops on the
        // claim *after* seg1 exists. Two claims: the first grows seg1, the second
        // observes seg0 fully drained and reclaims it. A run left unaccounted would
        // never reach drained_count == len and the count would climb without bound.
        for _ in 0..2 {
            let h = ring.claim();
            ring.fill(h, DropCounter(drops.clone()));
        }
        assert_eq!(
            ring.segment_count(),
            1,
            "dropped-without-complete run must drain and reclaim, not wedge"
        );
        assert_eq!(
            ring.front_base(),
            seg_size as u64,
            "seg0 (base 0) is the segment reclaimed after its run drained"
        );
    }

    /// `complete()` and `Drop` are exactly-once: payloads are freed once, never twice
    /// (the token's `drained` guard makes the second drain a no-op).
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
        let sw = ring.take_drain_run(false).expect("full");
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
        // seg1 is now full while seg0 has no contiguous filled run. take_drain_run
        // skips seg0 (its run from the cursor is empty) and claims seg1.
        let sw = ring.take_drain_run(false).expect("seg1 is full");
        assert_eq!(
            sw.base_seq(),
            seg_size as u64,
            "frontmost drainable run is in seg1"
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
        assert_eq!(snap.arrived(), std::slice::from_ref(&(3..5)));
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
        // Native: 3 segments (6! = 720 orders). Under miri, 2 segments (4! = 24) — the
        // hop across a segment boundary is still exercised, without the factorial blowup
        // at miri's ~1000x slowdown.
        let segments = if cfg!(miri) { 2 } else { 3 };
        let n = seg_size * segments;
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
        assert_eq!(snap.arrived(), std::slice::from_ref(&(1..2)));

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
        assert_eq!(snap.arrived(), std::slice::from_ref(&(0..2)));
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
        // At this point we have 2 segments in the deque, front based at seq 0.
        assert_eq!(ring.segment_count(), 2);
        assert_eq!(ring.front_base(), 0);

        // Consume all of the first segment and one item into the second.
        for _ in 0..seg_size + 1 {
            consumer.poll_next().unwrap();
        }

        // Trigger reclaim by claiming more (which runs reclaim_locked before it grows).
        let h = ring.claim();
        ring.fill(h, 999u64);

        // seg0 (base 0) is fully consumed (`base + len <= consumed`) and must be the
        // segment reclaimed — the front now bases at seg_size, and one segment grew for
        // the new claim, so the count returns to 2. Asserting `front_base` (not just the
        // count) proves the *right* segment went: a reclaim that popped the wrong
        // segment, or none, would leave `front_base == 0`.
        assert_eq!(ring.segment_count(), 2, "seg0 reclaimed, seg2 grew");
        assert_eq!(
            ring.front_base(),
            seg_size as u64,
            "the fully-consumed front segment (base 0) is the one reclaimed"
        );
    }

    /// The fill probe's run-length (`filled - claim_cursor`) must not underflow when a
    /// concurrent drainer advances `claim_cursor` past *this* fill's local `filled`
    /// snapshot. `filled` is captured at this thread's `fetch_add`; `claim_cursor` is
    /// read lock-free afterward, and another producer's later fill can let a drainer
    /// claim a run beyond this thread's count, so `claim_cursor > filled` is reachable.
    /// A bare subtraction wraps in release but panics under overflow-checks (the default
    /// for debug and `cargo test`), crashing the fill path. Reproduces only under real
    /// concurrency — single-threaded, `claim_cursor <= filled` always holds — so this is
    /// a multi-thread stress test at batch 1 (the disk relief regime) over a wide
    /// segment, looped enough to make the `claim_cursor > filled` race likely. It is a
    /// tripwire for reverting the probe's `saturating_sub` to a bare subtraction; it can
    /// only fail on a regression, never false-positives. The drain race's *correctness*
    /// is proven exhaustively by the loom models — this only guards the arithmetic.
    ///
    /// `#[cfg_attr(miri, ignore)]`: the guarded property is saturating arithmetic (defined
    /// behavior, invisible to miri) plus a scheduling race (loom's job); miri adds nothing
    /// here and the loop is prohibitively slow under it.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn fill_probe_does_not_underflow_under_concurrent_drain() {
        // Past the diminishing-returns knee for surfacing the race under native
        // scheduling, without the wall-clock cost of the original 2000.
        use std::sync::atomic::{AtomicU64, Ordering as O};
        for _ in 0..200 {
            let seg_size = 16;
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(seg_size);
            ring.set_drain_batch(1);
            let handles: Vec<_> = (0..seg_size).map(|_| ring.claim()).collect();
            let ring = Arc::new(ring);
            // Sum every run's freed count across all drainers. The buffer no longer
            // tracks a running total, so the test accumulates the per-run `freed` the
            // way the download layer does under its state lock.
            let freed_total = Arc::new(AtomicU64::new(0));
            std::thread::scope(|s| {
                for h in handles {
                    let ring = ring.clone();
                    let freed_total = freed_total.clone();
                    s.spawn(move || {
                        if ring.fill(h, 0) == FillOutcome::DrainReady {
                            while let Some(sw) = ring.take_drain_run(false) {
                                freed_total.fetch_add(sw.complete(), O::Relaxed);
                            }
                        }
                    });
                }
            });
            // Terminal sweep mops up any run a filler produced after its own drain
            // scan (the fill/drain interleaving can leave a slot claimed-but-untaken).
            while let Some(sw) = ring.take_drain_run(true) {
                freed_total.fetch_add(sw.complete(), O::Relaxed);
            }
            // Beyond the underflow (a bare subtraction panics here under overflow-checks),
            // assert the outcome is correct: every slot drained exactly once, so the freed
            // counts sum to the segment size. Catches a double-take or a lost run that a
            // panic-only check would miss.
            assert_eq!(
                freed_total.load(O::Relaxed),
                seg_size as u64,
                "every slot must drain exactly once under concurrent fill+drain"
            );
        }
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

// Loom model-checks the real structure directly (no model of it): the compat layer
// swaps in loom's atomics/Arc/Mutex under `cfg(s3_tm_loom)`. Segment size is set to
// 2 per ring (via new_with_segment_size) so boundary-crossing interleavings stay
// tractable; the algorithm is identical at any size. Ten models cover the handoff
// (1), the segment hop (2), reclaim vs an outstanding run — complete (3) and drop
// (6) paths, multi-producer fill (4), one full segment claimed by racing drainers
// (5), out-of-order fill across a hop (7), reclaim racing the consumer hop (8), two
// drainers over distinct segments (9), and two drainers partitioning one segment into
// disjoint runs at batch 1 (10). Run with:
//   LOOM_MAX_PREEMPTIONS=3 RUSTFLAGS="--cfg s3_tm_loom" \
//     cargo test --lib --release loom_tests -- --test-threads=1
// The preemption bound is required: unbounded exploration of the multi-producer and
// hop-vs-reclaim models is exponential and does not terminate in practical time. A
// bound of 3 explores all interleavings with at most three preemptions, which
// exercises every published ordering edge here while keeping the suite to seconds.
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

    /// Model 3 — segment reclaim vs an outstanding run pin.
    ///
    /// Fill seg0 full (seqs 0,1) so a run is drainable. One thread claims the run and
    /// `complete`s it (frees payloads, advances `drained_count`, reclaims drained
    /// front segments); concurrently the other thread `claim`s seq 2 (which grows
    /// seg1 and runs front-reclaim). The `SegmentWrite` pins seg0 via its `Arc`
    /// until `complete`, so the segment must not be freed while the token is alive,
    /// and the two reclaim paths (claim's and complete's) must not double-free.
    /// loom's Arc registry asserts exactly-once teardown.
    #[test]
    fn reclaim_vs_outstanding_write() {
        loom::model(|| {
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            // Fill seg0 (seqs 0,1) full; the default batch (== seg_size) makes it one
            // drainable run.
            for i in 0..2 {
                let h = ring.claim();
                ring.fill(h, i as u64);
            }

            let ring2 = ring.clone();
            let taker = thread::spawn(move || {
                let sw = ring2
                    .take_drain_run(false)
                    .expect("seg0 is full and drainable");
                assert_eq!(sw.base_seq(), 0);
                sw.complete(); // frees payloads, drained_count += len, reclaim_front
            });

            // Concurrently grow + reclaim from the issuer side.
            let h = ring.claim(); // seq 2 → grows seg1, runs reclaim_locked
            ring.fill(h, 99);

            taker.join().unwrap();
            // After both threads finish, seg0's run has drained (`complete`) so
            // `drained_count == len`. A final claim runs reclaim deterministically
            // (no new interleaving — both threads have joined), which must pop the
            // now-drained seg0 exactly once. Asserting `front_base` past seg0, not just
            // a count, proves the reclaim happened and freed the right segment without
            // the two reclaim paths double-popping.
            let h = ring.claim();
            ring.fill(h, 100);
            assert_eq!(
                ring.front_base(),
                2,
                "drained seg0 (base 0) reclaimed exactly once"
            );
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

    /// Model 5 — two drainers race one full segment at the default batch.
    ///
    /// seg0 is filled full, then two threads call `take_drain_run(false)` concurrently
    /// with the batch equal to the segment size, so the only drainable run is the whole
    /// segment. Advancing `claim_cursor` under the lock must hand that run to exactly
    /// one: one thread gets `Some(SegmentWrite)` based at 0, the other `None`. This
    /// proves the at-most-once handout under contention, which the single-threaded
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
            let t1 = thread::spawn(move || ring2.take_drain_run(false).map(|sw| sw.base_seq()));
            let t2 = thread::spawn(move || ring.take_drain_run(false).map(|sw| sw.base_seq()));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one winner, and it got the run based at seq 0.
            let winners: Vec<u64> = [r1, r2].into_iter().flatten().collect();
            assert_eq!(
                winners,
                vec![0],
                "exactly one drainer claims the full-segment run"
            );
        });
    }

    /// Model 6 — run drop (no complete) vs concurrent reclaim.
    ///
    /// Same shape as Model 3, but the drainer **drops** the `SegmentWrite` instead of
    /// calling `complete()`. The `Drop` safety net runs the same drain (free payloads,
    /// advance `drained_count`, reclaim_front) concurrently with the issuer's `claim`
    /// reclaim. The token's idempotency guard and the `Arc` pin must still give
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
                    .take_drain_run(false)
                    .expect("seg0 is full and drainable");
                assert_eq!(sw.base_seq(), 0);
                // Drop without complete(): Drop must drain and reclaim safely.
                drop(sw);
            });

            let h = ring.claim(); // seq 2 → grows seg1, runs reclaim_locked
            ring.fill(h, 99);

            taker.join().unwrap();
            // seg0's run drained via the Drop safety net. A final claim runs reclaim
            // deterministically (both threads joined) and must pop the drained seg0
            // exactly once — `front_base` past seg0 proves the drop path advanced
            // `drained_count` and reclaim freed the right segment without double-pop.
            let h = ring.claim();
            ring.fill(h, 100);
            assert_eq!(
                ring.front_base(),
                2,
                "drop-drained seg0 (base 0) reclaimed exactly once"
            );
        });
    }

    /// Model 7 — out-of-order fill across a segment boundary, with the consumer
    /// hopping.
    ///
    /// Four seqs span two segments (seg0={0,1}, seg1={2,3}). One producer fills seq 3
    /// (sealing seg1) before seq 0, while another fills 0,1,2; the consumer drains
    /// across the seg0→seg1 hop concurrently. This combines what Model 2 (hop) and
    /// Model 4 (out-of-order fill) cover separately: a later segment may seal before
    /// an earlier one, yet in-order delivery and the hop's `Arc` reconstruction must
    /// still yield 0,1,2,3 exactly once with no use-after-free. loom's `UnsafeCell`
    /// and Arc registry catch a torn read, a lost/dup payload, or a freed-segment hop.
    #[test]
    fn out_of_order_fill_across_segment_hop() {
        loom::model(|| {
            let (ring, mut consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            // Claim all four up front (claim is serialized under the lock).
            let handles: Vec<_> = (0..4).map(|_| ring.claim()).collect();

            let ring_a = ring.clone();
            let a = thread::spawn(move || {
                // Seal seg1 before seg0 is touched: fill 3 then 2.
                ring_a.fill_at(&handles, 3, 3);
                ring_a.fill_at(&handles, 2, 2);
                ring_a.fill_at(&handles, 1, 1);
                ring_a.fill_at(&handles, 0, 0);
            });

            let mut got = Vec::new();
            for _ in 0..8 {
                if let Some(v) = consumer.poll_next() {
                    got.push(v);
                }
                if got.len() == 4 {
                    break;
                }
            }
            a.join().unwrap();
            while let Some(v) = consumer.poll_next() {
                got.push(v);
            }
            assert_eq!(
                got,
                vec![0, 1, 2, 3],
                "in-order, exactly once, across the hop"
            );
        });
    }

    /// Model 8 — front-reclaim racing the consumer's hop.
    ///
    /// seg0 is filled and fully delivered (its slots emptied, `consumed` past its
    /// end); the consumer is poised at the boundary about to hop to seg1. One thread
    /// claims seq 2 (growing seg1) and then claims seq 3, whose `reclaim_locked` is
    /// now eligible to pop the fully-consumed seg0 from the deque — concurrently with
    /// the consumer loading `seg0.next` and reconstructing seg1's `Arc`. The hop holds
    /// `self.current` (seg0) by strong ref until the reassignment, and seg1 cannot be
    /// reclaimed while `consumed <= seg1.base`, so the reconstruction stays live.
    /// loom's Arc registry catches a use-after-free if reclaim could free a segment
    /// the hop still touches.
    #[test]
    fn reclaim_races_consumer_hop() {
        loom::model(|| {
            let (ring, mut consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            // Fill and drain seg0 fully so it is reclaim-eligible and the consumer is
            // at the seg0→seg1 boundary.
            let h0 = ring.claim();
            let h1 = ring.claim();
            ring.fill(h0, 0);
            ring.fill(h1, 1);
            assert_eq!(consumer.poll_next(), Some(0));
            assert_eq!(consumer.poll_next(), Some(1));

            let ring2 = ring.clone();
            let issuer = thread::spawn(move || {
                let h2 = ring2.claim(); // grows seg1
                ring2.fill(h2, 2);
                let h3 = ring2.claim(); // reclaim_locked may pop the drained seg0
                ring2.fill(h3, 3);
            });

            // Hop into seg1 concurrently with the reclaim above.
            let mut got = Vec::new();
            for _ in 0..6 {
                if let Some(v) = consumer.poll_next() {
                    got.push(v);
                }
                if got.len() == 2 {
                    break;
                }
            }
            issuer.join().unwrap();
            while let Some(v) = consumer.poll_next() {
                got.push(v);
            }
            assert_eq!(got, vec![2, 3], "post-hop delivery in order, exactly once");
        });
    }

    /// Model 9 — two drainers race two distinct full segments.
    ///
    /// seg0 and seg1 are both full, then two threads call `take_drain_run(false)`
    /// concurrently at the default batch (so each segment is one run). The forward
    /// scan plus the `claim_cursor` advance must hand the two segments' runs to the two
    /// drainers — one each, no duplicate, no segment skipped. Model 5 proves
    /// at-most-once on a *single* contended segment; this proves the forward scan hands
    /// out *every* drainable run exactly once when drainers race across segments.
    #[test]
    fn two_takers_distinct_segments() {
        loom::model(|| {
            let (ring, _consumer) = PagedRecvBuffer::<u64>::new_with_segment_size(2);
            // Fill seg0 (0,1) and seg1 (2,3) both full.
            for i in 0..4u64 {
                let h = ring.claim();
                ring.fill(h, i);
            }

            let ring2 = ring.clone();
            let t1 = thread::spawn(move || ring2.take_drain_run(false).map(|sw| sw.base_seq()));
            let t2 = thread::spawn(move || ring.take_drain_run(false).map(|sw| sw.base_seq()));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Both segments handed out, one per drainer, no duplication. Order between
            // the two threads is nondeterministic, so compare as a sorted set.
            let mut bases: Vec<u64> = [r1, r2].into_iter().flatten().collect();
            bases.sort_unstable();
            assert_eq!(
                bases,
                vec![0, 2],
                "both full segments handed out exactly once across racing drainers"
            );
        });
    }

    /// Model 10 — two drainers partition one segment into disjoint runs at batch 1.
    ///
    /// One segment of size 2, batch lowered to 1 so each slot is individually
    /// drainable. Each thread fills its own slot then drains, the realistic disk path
    /// where a completing part probes and drains. The fills and drains interleave: a
    /// drainer may find only its own slot filled and claim a single-slot run, or find
    /// both and claim the pair — but advancing `claim_cursor` under the lock means the
    /// two drainers' runs are always disjoint, so no slot is read by both. A terminal
    /// sweep after the join mops up any slot whose filler had not reached its own drain
    /// scan. A `DropCounter` payload proves every slot is freed exactly once: a run
    /// overlap would either double-free (count > 2) or trip loom's `UnsafeCell` on the
    /// concurrent in-place reads. This is the sub-segment partition the cursor
    /// introduces, which a whole-segment handout could not race.
    #[test]
    fn two_drainers_partition_one_segment() {
        loom::model(|| {
            // Count drops through a loom `Arc<AtomicUsize>` (the compat re-export under
            // loom cfg) so the model tracks every payload free. `Clone` is required by
            // `PagedRecvBuffer<T>: Clone`'s `T: Clone` bound; cloning shares the same
            // counter, and each instance still drops exactly once.
            #[derive(Clone)]
            struct DropCounter(Arc<AtomicUsize>);
            impl Drop for DropCounter {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }

            let drops = Arc::new(AtomicUsize::new(0));
            let (ring, _consumer) = PagedRecvBuffer::<DropCounter>::new_with_segment_size(2);
            ring.set_drain_batch(1);
            // Claim both slots up front (claim is serialized under the lock).
            let h0 = ring.claim();
            let h1 = ring.claim();

            let ring_a = ring.clone();
            let da = drops.clone();
            let a = thread::spawn(move || {
                ring_a.fill(h0, DropCounter(da));
                if let Some(sw) = ring_a.take_drain_run(false) {
                    sw.complete();
                }
            });
            let ring_b = ring.clone();
            let db = drops.clone();
            let b = thread::spawn(move || {
                ring_b.fill(h1, DropCounter(db));
                if let Some(sw) = ring_b.take_drain_run(false) {
                    sw.complete();
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            // Mop up any slot a drainer's scan missed (its sibling filled after the
            // scan). Single-threaded here, so no race; terminal takes a lone slot.
            while let Some(sw) = ring.take_drain_run(true) {
                sw.complete();
            }

            // Every payload freed exactly once. Overlapping runs would double-free
            // (count > 2) or be caught earlier by loom's UnsafeCell.
            assert_eq!(
                drops.load(Ordering::SeqCst),
                2,
                "each slot's payload freed exactly once across partitioned runs"
            );
        });
    }
}
