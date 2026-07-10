/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Fungible global memory admission-control budget.
//!
//! `MemoryBudget` tracks how many fixed-size chunks of memory are logically
//! reserved across all transfers. It grants reservations (RAII tickets whose drop
//! returns chunks), parks callers when the budget is exhausted, and wakes them in
//! strict arrival order as chunks are released.
//!
//! # Admission order
//!
//! Reservations are granted first-come-first-served: a request is served only once
//! every earlier-queued request has been. A fresh request never jumps one already
//! waiting, even when free chunks would satisfy it, and the release-time drain
//! stops at the first queued request that does not fit rather than serving smaller
//! ones behind it. This bounds the wait for every `need <= capacity` request: once
//! queued it sees `in_use` only fall until its need is met, so a stream of smaller
//! requests can never indefinitely defer a larger one ahead of them. The cost is
//! head-of-line blocking — a large request at the front idles the budget while
//! free chunks accumulate to its need — which arises only under mixed part sizes;
//! a uniform part size never triggers it.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::metrics::unit::ByteUnit;
use crate::runtime::sync::Mutex;

/// Closure the budget invokes to wake a parked reserver (= `scheduler.wake(tid)`).
pub(crate) type NotifyFn = Arc<dyn Fn() + Send + Sync>;

/// Fungible global memory budget. Accounts in fixed-size chunks; grants RAII
/// `Reservation` tickets whose drop returns chunks and wakes parked waiters.
pub(crate) struct MemoryBudget {
    inner: Mutex<Inner>,
    /// Fixed nominal accounting unit, in bytes (e.g. 8 MiB). NOT any transfer's
    /// part size: a part of `n` bytes costs `ceil(n / chunk)` chunks, so a 16 MiB
    /// part against an 8 MiB chunk costs 2. Bounds accounting error to within one
    /// chunk per reservation while keeping the budget part-size-agnostic.
    chunk: usize,
}

impl std::fmt::Debug for MemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBudget")
            .field("chunk", &self.chunk)
            .finish_non_exhaustive()
    }
}

// Lock-ordering invariant (developer note).
//
// Two lock flavors exist: the budget lock (`MemoryBudget.inner`) and per-waiter
// slot locks (`WaitSlot.state`). The orderings `budget → slot` (drain stores a
// grant) and `slot → budget` (WaitTicket::drop acts on a waiter) must NEVER nest,
// or they deadlock: drain holds budget and takes slot while a concurrent
// WaitTicket::drop holds slot and takes budget. Enforced by:
// - `drain`: takes a slot lock only briefly to store a grant; never drops a
//   Reservation while holding a slot lock.
// - `WaitTicket::take`: takes the slot lock only; never touches the budget lock.
// - `WaitTicket::drop`: takes the slot lock, extracts the state, RELEASES the slot
//   lock, THEN acts (dropping a granted Reservation re-locks budget; canceling a
//   Waiting entry locks budget). Neither path holds slot while taking budget.

/// Budget state behind the single budget lock.
struct Inner {
    /// Chunks currently reserved (granted and not yet released).
    in_use: u64,
    /// Chunk ceiling; resizable via `set_limit`.
    capacity: u64,
    /// Parked requests in strict arrival order; the front is served first.
    waiters: VecDeque<Waiter>,
    /// Cumulative count of requests that ever parked (returned `Pending`).
    /// Monotonic; distinguishes a budget that has bound from one that never has.
    total_parked: u64,
}

/// A parked reservation request: how many chunks it needs, the slot through which
/// drain delivers its grant, and the closure to wake it once granted.
struct Waiter {
    need: u64,
    slot: Arc<WaitSlot>,
    notify: NotifyFn,
}

/// Per-waiter slot through which drain delivers a granted Reservation. Carries its
/// own lock so a grant can be stored without holding the budget lock for long.
struct WaitSlot {
    state: Mutex<WaitState>,
}

/// Lifecycle of a parked request's slot.
enum WaitState {
    /// Enqueued, not yet granted.
    Waiting,
    /// drain has granted a reservation; awaiting `WaitTicket::take`.
    Granted(Reservation),
    /// The grant was taken (or the ticket dropped); terminal.
    Taken,
}

/// Result of `MemoryBudget::reserve`: either an immediate grant or a ticket to
/// poll later after the budget wakes the caller.
pub(crate) enum Reserve {
    /// The request fit immediately; chunks are reserved.
    Ready(Reservation),
    /// The request was enqueued; poll the ticket after the notify fires.
    Pending(WaitTicket),
}

/// RAII ticket representing reserved chunks. Drop returns the chunks to the budget
/// and drains parked waiters that now fit.
pub(crate) struct Reservation {
    budget: Arc<MemoryBudget>,
    chunks: u64,
}

/// Opaque handle to a parked reservation request. The caller holds this until
/// `take()` yields the granted `Reservation` (after the budget's notify fires).
pub(crate) struct WaitTicket {
    slot: Arc<WaitSlot>,
    budget: Arc<MemoryBudget>,
}

impl std::fmt::Debug for WaitTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitTicket").finish_non_exhaustive()
    }
}

/// Nominal 8 MiB accounting unit for budget reservations. A part costs
/// `ceil(part_size / chunk)` chunks, so non-uniform part sizes account correctly.
pub(crate) const BUDGET_CHUNK_BYTES: usize = 8 * ByteUnit::Mebibyte.as_bytes_usize();

// A request fits when the free chunks cover its need. The `in_use == 0` clause is
// the forced grant: a request larger than the entire capacity can only ever proceed
// when nothing else holds the budget, and strict FIFO guarantees that point is
// eventually reached for the front waiter. This is the sole path by which a
// `need > capacity` request makes progress (the upload / un-sliced-object backstop;
// downloads slice to part_size so never hit it). Without it such a request would
// park forever.
fn can_grant(need: u64, in_use: u64, capacity: u64) -> bool {
    need <= capacity.saturating_sub(in_use) || in_use == 0
}

/// Drain the wait queue in arrival order. Grants the front waiter if it fits, stops
/// at the first that does not (no skip-ahead, so a smaller request behind a too-large
/// front is not served early). Returns collected NotifyFns that callers MUST invoke
/// AFTER releasing the budget lock.
fn drain(budget: &Arc<MemoryBudget>, inner: &mut Inner) -> Vec<NotifyFn> {
    let mut notifies = Vec::new();
    while let Some(front) = inner.waiters.front() {
        if !can_grant(front.need, inner.in_use, inner.capacity) {
            break;
        }
        let waiter = inner.waiters.pop_front().unwrap();
        inner.in_use += waiter.need;
        tracing::trace!(
            target: crate::telemetry::TARGET_SCHEDULING,
            need = waiter.need,
            in_use = inner.in_use,
            capacity = inner.capacity,
            waiters = inner.waiters.len(),
            "budget grant to parked waiter",
        );
        let reservation = Reservation {
            budget: budget.clone(),
            chunks: waiter.need,
        };
        {
            let mut slot_state = waiter.slot.state.lock();
            *slot_state = WaitState::Granted(reservation);
        }
        notifies.push(waiter.notify);
    }
    notifies
}

impl MemoryBudget {
    /// Create a new budget. `chunk_bytes` is the fixed accounting unit (must be > 0).
    /// Capacity is `floor(capacity_bytes / chunk_bytes)` chunks, with a floor of one
    /// chunk: a capacity that rounds to zero would force every request through the
    /// idle-only grant path, serializing all transfers, so a budget configured below
    /// one chunk is raised to one rather than silently throttling to a single part.
    pub(crate) fn new(capacity_bytes: usize, chunk_bytes: usize) -> Arc<Self> {
        assert!(chunk_bytes > 0, "chunk_bytes must be > 0");
        let capacity = (capacity_bytes / chunk_bytes).max(1) as u64;
        if capacity_bytes < chunk_bytes {
            tracing::debug!(
                requested_bytes = capacity_bytes,
                chunk_bytes,
                "memory budget below one chunk; raised to a single chunk"
            );
        }
        tracing::debug!(
            capacity_chunks = capacity,
            chunk_bytes,
            "memory budget resolved"
        );
        Arc::new(Self {
            inner: Mutex::new(Inner {
                in_use: 0,
                capacity,
                waiters: VecDeque::new(),
                total_parked: 0,
            }),
            chunk: chunk_bytes,
        })
    }

    /// Number of chunks required to hold `bytes` (ceiling division).
    pub(crate) fn chunks_for(&self, bytes: usize) -> u64 {
        if bytes == 0 {
            return 0;
        }
        bytes.div_ceil(self.chunk) as u64
    }

    /// Attempt an immediate grant without parking. Returns `None` if a request is already
    /// queued (arrival order is preserved) or this one does not fit; registers no waker and
    /// never enqueues a waiter. Grants under exactly the condition [`reserve`](Self::reserve)
    /// returns `Ready`, so it is the allocation-free fast path when a caller can re-drive
    /// itself on failure.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let need = self.chunks_for(bytes);
        let mut inner = self.inner.lock();
        // Arrival-order admission (see module doc): a queued request is never bypassed,
        // even by a fresh one that would fit.
        if !inner.waiters.is_empty() {
            return None;
        }
        if can_grant(need, inner.in_use, inner.capacity) {
            inner.in_use += need;
            Some(Reservation {
                budget: Arc::clone(self),
                chunks: need,
            })
        } else {
            None
        }
    }

    /// Reserve chunks for `bytes`. Returns `Ready` with an immediate grant when the
    /// queue is empty and the request fits, otherwise enqueues a waiter and returns
    /// `Pending` with a `WaitTicket`.
    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize, notify: NotifyFn) -> Reserve {
        let need = self.chunks_for(bytes);
        let mut inner = self.inner.lock();
        // Arrival-order admission (see module doc): a queued request is never bypassed,
        // even by a fresh one that would fit.
        if inner.waiters.is_empty() && can_grant(need, inner.in_use, inner.capacity) {
            inner.in_use += need;
            tracing::trace!(
                target: crate::telemetry::TARGET_SCHEDULING,
                need,
                in_use = inner.in_use,
                capacity = inner.capacity,
                "budget reserve granted",
            );
            return Reserve::Ready(Reservation {
                budget: Arc::clone(self),
                chunks: need,
            });
        }
        let slot = Arc::new(WaitSlot {
            state: Mutex::new(WaitState::Waiting),
        });
        inner.waiters.push_back(Waiter {
            need,
            slot: Arc::clone(&slot),
            notify,
        });
        inner.total_parked += 1;
        // Saturation edge: a reserve had to park. The budget cannot admit `need`
        // until a live reservation releases, so a caller that never sees a release
        // waits here indefinitely. Logged per park (a parked reserve is already a
        // backpressure event, not steady-state noise).
        tracing::debug!(
            target: crate::telemetry::TARGET_SCHEDULING,
            need,
            in_use = inner.in_use,
            capacity = inner.capacity,
            waiters = inner.waiters.len(),
            "budget reserve parked: request queued until a reservation releases",
        );
        Reserve::Pending(WaitTicket {
            slot,
            budget: Arc::clone(self),
        })
    }

    /// Resize the capacity (in bytes). Growing may drain parked waiters. Shrinking is soft: it
    /// never revokes already-granted chunks, only tightens future grants.
    ///
    /// Resizable by design, but not yet wired to a caller, so it carries `allow(dead_code)`.
    #[allow(dead_code)]
    pub(crate) fn set_limit(self: &Arc<Self>, capacity_bytes: usize) {
        let new_capacity = (capacity_bytes / self.chunk) as u64;
        let notifies = {
            let mut inner = self.inner.lock();
            inner.capacity = new_capacity;
            drain(self, &mut inner)
        };
        for f in notifies {
            f();
        }
    }

    /// Chunks currently reserved (granted and not yet released). Test introspection;
    /// production reads admission state through [`stats`](Self::stats).
    #[cfg(test)]
    pub(crate) fn in_use_chunks(&self) -> u64 {
        self.inner.lock().in_use
    }

    /// Current capacity in chunks. Test introspection; production reads admission
    /// state through [`stats`](Self::stats).
    #[cfg(test)]
    pub(crate) fn capacity_chunks(&self) -> u64 {
        self.inner.lock().capacity
    }

    /// Snapshot the budget's admission state under a single lock. Chunk counts are
    /// converted to bytes; `reserved_bytes` is admitted memory (a ceiling), not
    /// resident memory.
    pub(crate) fn stats(&self) -> crate::types::MemoryBudgetSnapshot {
        let inner = self.inner.lock();
        crate::types::MemoryBudgetSnapshot {
            capacity_bytes: inner.capacity * self.chunk as u64,
            reserved_bytes: inner.in_use * self.chunk as u64,
            waiters: inner.waiters.len(),
            total_parked: inner.total_parked,
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let notifies = {
            let mut inner = self.budget.inner.lock();
            inner.in_use = inner.in_use.saturating_sub(self.chunks);
            tracing::trace!(
                target: crate::telemetry::TARGET_SCHEDULING,
                released = self.chunks,
                in_use = inner.in_use,
                capacity = inner.capacity,
                waiters = inner.waiters.len(),
                "budget reservation released",
            );
            drain(&self.budget, &mut inner)
        };
        for f in notifies {
            f();
        }
    }
}

impl WaitTicket {
    /// If the budget has granted this ticket's reservation, take it out and return
    /// it. Returns None if still waiting or already taken.
    pub(crate) fn take(&mut self) -> Option<Reservation> {
        let mut slot_state = self.slot.state.lock();
        let state = std::mem::replace(&mut *slot_state, WaitState::Taken);
        match state {
            WaitState::Granted(res) => Some(res),
            WaitState::Waiting => {
                *slot_state = WaitState::Waiting;
                None
            }
            WaitState::Taken => {
                *slot_state = WaitState::Taken;
                None
            }
        }
    }
}

impl Drop for WaitTicket {
    fn drop(&mut self) {
        // Take the state out under the slot lock, then release it before acting.
        let state = {
            let mut slot_state = self.slot.state.lock();
            std::mem::replace(&mut *slot_state, WaitState::Taken)
        };
        // Slot lock is released here. Now act on the extracted state.
        match state {
            WaitState::Granted(res) => {
                // Granted but never taken: drop the reservation to return chunks.
                // Safe: slot lock is released, so Reservation::drop can lock the budget.
                drop(res);
            }
            WaitState::Waiting => {
                // Cancel: remove this waiter from the queue, then drain. Removing a
                // queued waiter can unblock those behind it — cancelling the front
                // exposes the next waiter to free capacity that arrival order had held
                // it back from — so the queue must be re-evaluated here, or that waiter
                // stays parked until an unrelated reservation happens to release.
                // Notifies fire after the budget lock is dropped (the same discipline
                // `Reservation::drop` follows).
                let notifies = {
                    let mut inner = self.budget.inner.lock();
                    inner.waiters.retain(|w| !Arc::ptr_eq(&w.slot, &self.slot));
                    drain(&self.budget, &mut inner)
                };
                for f in notifies {
                    f();
                }
            }
            WaitState::Taken => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    fn notify_flag() -> (NotifyFn, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        let f = Arc::clone(&flag);
        (Arc::new(move || f.store(true, Ordering::Release)), flag)
    }

    fn notify_counter() -> (NotifyFn, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let c = Arc::clone(&counter);
        (
            Arc::new(move || {
                c.fetch_add(1, Ordering::Release);
            }),
            counter,
        )
    }

    #[test]
    fn test_chunks_for_rounds_up() {
        let budget = MemoryBudget::new(1024, 100);
        assert_eq!(budget.chunks_for(0), 0);
        assert_eq!(budget.chunks_for(1), 1);
        assert_eq!(budget.chunks_for(100), 1);
        assert_eq!(budget.chunks_for(101), 2);
    }

    #[test]
    fn test_capacity_floored_at_one_chunk() {
        // A budget configured below one chunk would otherwise round to zero
        // capacity, forcing every request through the idle-only grant path and
        // serializing transfers. It is raised to one chunk instead.
        let chunk = 8 * 1024 * 1024;
        let budget = MemoryBudget::new(chunk / 2, chunk);
        assert_eq!(budget.capacity_chunks(), 1);
        // The first request fits; a second parks rather than barging.
        let first = budget.try_reserve(chunk);
        assert!(first.is_some());
        assert!(budget.try_reserve(chunk).is_none());
    }

    #[test]
    fn test_stats_reports_bytes_and_park_counter() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 2, chunk); // capacity = 2 chunks

        let s = budget.stats();
        assert_eq!(s.capacity_bytes, (chunk * 2) as u64);
        assert_eq!(s.reserved_bytes, 0);
        assert_eq!(s.waiters, 0);
        assert_eq!(s.total_parked, 0);

        // Reserve both chunks: reserved tracks in bytes, still no parking.
        let holder = match budget.reserve(chunk * 2, notify_flag().0) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };
        let s = budget.stats();
        assert_eq!(s.reserved_bytes, (chunk * 2) as u64);
        assert_eq!(s.waiters, 0);
        assert_eq!(s.total_parked, 0);

        // Next request parks: waiters rises and the cumulative counter ticks.
        let ticket = match budget.reserve(chunk, notify_flag().0) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };
        let s = budget.stats();
        assert_eq!(s.waiters, 1);
        assert_eq!(s.total_parked, 1);

        // Drop the waiter: waiters falls back, but total_parked is monotonic.
        drop(ticket);
        let s = budget.stats();
        assert_eq!(s.waiters, 0);
        assert_eq!(s.total_parked, 1, "park counter is cumulative");

        drop(holder);
    }

    #[test]
    fn test_reserve_within_capacity_is_ready_and_tracks_in_use() {
        let chunk = 8 * 1024 * 1024; // 8 MiB
        let budget = MemoryBudget::new(chunk * 4, chunk);
        let (notify, _flag) = notify_flag();

        match budget.reserve(chunk * 2, notify) {
            Reserve::Ready(_res) => {
                assert_eq!(budget.in_use_chunks(), 2);
            }
            Reserve::Pending(_) => panic!("expected Ready"),
        }
    }

    #[test]
    fn test_drop_returns_chunks() {
        let chunk = 8 * 1024 * 1024;
        let budget = MemoryBudget::new(chunk * 4, chunk);
        let (notify, _flag) = notify_flag();

        let res = match budget.reserve(chunk * 2, notify) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };
        assert_eq!(budget.in_use_chunks(), 2);
        drop(res);
        assert_eq!(budget.in_use_chunks(), 0);
    }

    #[test]
    fn test_exhaustion_returns_pending_then_release_grants_waiter() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 2, chunk); // 2 chunks capacity
        let (n1, _) = notify_flag();
        let (n2, flag2) = notify_flag();
        let (n2_counter, counter) = notify_counter();
        // Use counter-based notify to check exactly-once
        let _ = n2; // discard flag-based
        let _ = flag2;

        // Fill capacity
        let res1 = match budget.reserve(chunk * 2, n1) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };
        assert_eq!(budget.in_use_chunks(), 2);

        // Next reserve exhausts budget -> Pending
        let mut ticket = match budget.reserve(chunk, n2_counter) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // ticket.take() is None (still waiting)
        assert!(ticket.take().is_none());

        // Drop the first reservation -> should grant the waiter
        drop(res1);

        // Notify fired exactly once
        assert_eq!(counter.load(Ordering::Acquire), 1);

        // ticket.take() now returns Some
        let res2 = ticket.take().expect("should be granted");
        assert_eq!(budget.in_use_chunks(), 1);
        drop(res2);
        assert_eq!(budget.in_use_chunks(), 0);
    }

    #[test]
    fn test_strict_fifo_two_waiters_granted_in_order() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk, chunk); // capacity = 1 chunk
        let (n_hold, _) = notify_flag();
        let (n_a, flag_a) = notify_flag();
        let (n_b, flag_b) = notify_flag();

        // Hold the single chunk
        let holder = match budget.reserve(chunk, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Waiter A needs 1 chunk
        let mut ticket_a = match budget.reserve(chunk, n_a) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Waiter B needs 1 chunk
        let mut ticket_b = match budget.reserve(chunk, n_b) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Release holder -> A granted, B still waiting
        drop(holder);
        assert!(flag_a.load(Ordering::Acquire));
        assert!(!flag_b.load(Ordering::Acquire));

        let res_a = ticket_a.take().expect("A should be granted");
        assert!(ticket_b.take().is_none());

        // Release A -> B granted
        drop(res_a);
        assert!(flag_b.load(Ordering::Acquire));
        let _res_b = ticket_b.take().expect("B should be granted");
    }

    #[test]
    fn test_arrival_order_small_request_does_not_jump_queued_large() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 4, chunk); // 4 chunks capacity
        let (n_hold, _) = notify_flag();
        let (n_large, flag_large) = notify_flag();

        // Hold 3 of 4 chunks, leaving 1 free.
        let holder = match budget.reserve(chunk * 3, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Queue a 2-chunk waiter. It does not fit now (1 free < 2), so it parks.
        let mut ticket_large = match budget.reserve(chunk * 2, n_large) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // A 1-chunk request WOULD fit on capacity alone (1 chunk is free), so this
        // isolates arrival order from exhaustion: it is rejected only because a
        // waiter is queued ahead of it.
        assert!(
            budget.try_reserve(chunk).is_none(),
            "arrival order: must not jump the queued waiter even though it fits"
        );

        // Drain the queue: releasing the holder frees 3 chunks; the parked 2-chunk
        // waiter is granted, leaving in_use == 2.
        drop(holder);
        assert!(flag_large.load(Ordering::Acquire));
        let _res_large = ticket_large.take().expect("waiter should be granted");
        assert_eq!(budget.in_use_chunks(), 2);

        // Same 1-chunk request now succeeds: the queue is empty and 2 chunks are
        // free, confirming the earlier rejection was arrival order, not exhaustion.
        assert!(
            budget.try_reserve(chunk).is_some(),
            "with an empty queue and free capacity the request now fits"
        );
    }

    #[test]
    fn test_forced_grant_oversized_when_idle() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 2, chunk); // 2 chunks capacity

        // try_reserve of 5 chunks while in_use == 0 -> Ready (forced grant)
        let res = budget
            .try_reserve(chunk * 5)
            .expect("forced grant when idle");
        assert_eq!(budget.in_use_chunks(), 5);
        drop(res);

        // Test forced grant via FIFO: hold 1 chunk, queue a 5-chunk waiter,
        // then release -> waiter granted once in_use hits 0.
        let (n_hold, _) = notify_flag();
        let (n_big, flag_big) = notify_flag();

        let holder = match budget.reserve(chunk, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        let mut ticket = match budget.reserve(chunk * 5, n_big) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Release holder -> in_use becomes 0 -> forced grant fires
        drop(holder);
        assert!(flag_big.load(Ordering::Acquire));
        let res = ticket.take().expect("oversized waiter should be granted");
        assert_eq!(budget.in_use_chunks(), 5);
        drop(res);
    }

    #[test]
    fn test_set_limit_grow_drains_waiters() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk, chunk); // 1 chunk capacity
        let (n_hold, _) = notify_flag();
        let (n_wait, flag_wait) = notify_flag();

        // Fill capacity
        let _holder = match budget.reserve(chunk, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Park a waiter needing 1 chunk
        let mut ticket = match budget.reserve(chunk, n_wait) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Grow capacity to 2 -> waiter now fits
        budget.set_limit(chunk * 2);
        assert!(flag_wait.load(Ordering::Acquire));
        let _res = ticket.take().expect("should be granted after grow");
        assert_eq!(budget.in_use_chunks(), 2);
    }

    #[test]
    fn test_set_limit_shrink_is_soft() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 4, chunk); // 4 chunks
        let (n, _) = notify_flag();

        // Reserve 3 chunks
        let res = match budget.reserve(chunk * 3, n) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };
        assert_eq!(budget.in_use_chunks(), 3);

        // Shrink capacity below in_use: no panic, reservation still valid
        budget.set_limit(chunk); // capacity now 1 chunk
        assert_eq!(budget.capacity_chunks(), 1);
        assert_eq!(budget.in_use_chunks(), 3); // unchanged

        // New reserve is gated by lower capacity
        let (n2, _) = notify_flag();
        match budget.reserve(chunk, n2) {
            Reserve::Pending(_) => {} // expected: 3 in_use > 1 capacity
            Reserve::Ready(_) => panic!("expected Pending under shrunk capacity"),
        }

        drop(res);
    }

    #[test]
    fn test_wait_ticket_drop_while_waiting_removes_from_queue() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk, chunk); // 1 chunk
        let (n_hold, _) = notify_flag();
        let (n_wait, flag_wait) = notify_flag();

        // Fill capacity
        let holder = match budget.reserve(chunk, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Park a waiter
        let ticket = match budget.reserve(chunk, n_wait) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Drop the ticket (cancel the waiter)
        drop(ticket);

        // Release the holder — the dropped waiter must not be granted
        drop(holder);
        assert!(!flag_wait.load(Ordering::Acquire));
        assert_eq!(budget.in_use_chunks(), 0);

        // A subsequent waiter works correctly
        let (n_new, flag_new) = notify_flag();
        match budget.reserve(chunk, n_new) {
            Reserve::Ready(_) => {} // queue is empty, fits
            Reserve::Pending(_) => panic!("expected Ready after cancel"),
        }
        let _ = flag_new;
    }

    #[test]
    fn test_cancel_middle_waiter_preserves_order_of_others() {
        // Three waiters queued behind a holder; cancel the middle one. The
        // remaining two must still be granted in arrival order — guarding against
        // a cancel that removes the wrong entry (or all entries).
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk, chunk); // capacity = 1 chunk
        let (n_hold, _) = notify_flag();
        let (n_a, flag_a) = notify_flag();
        let (n_b, flag_b) = notify_flag();
        let (n_c, flag_c) = notify_flag();

        let holder = match budget.reserve(chunk, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };
        let mut ticket_a = match budget.reserve(chunk, n_a) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };
        let ticket_b = match budget.reserve(chunk, n_b) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };
        let mut ticket_c = match budget.reserve(chunk, n_c) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Cancel the middle waiter (B).
        drop(ticket_b);

        // Release the holder: A (front) is granted; B and C are not yet.
        drop(holder);
        assert!(flag_a.load(Ordering::Acquire), "A should be granted first");
        assert!(!flag_b.load(Ordering::Acquire), "B was cancelled");
        assert!(!flag_c.load(Ordering::Acquire), "C waits behind A");
        let res_a = ticket_a.take().expect("A granted");

        // Release A: C is now front (B was removed, not C) and is granted.
        drop(res_a);
        assert!(
            flag_c.load(Ordering::Acquire),
            "C should be granted after A"
        );
        let _res_c = ticket_c.take().expect("C granted");
        assert_eq!(budget.in_use_chunks(), 1);
    }

    #[test]
    fn test_cancel_front_waiter_grants_next_that_now_fits() {
        // Arrival order can park a waiter that would fit on free capacity behind a
        // larger front waiter. If the front is then cancelled, the freed head-of-line
        // must let the next waiter through immediately — the cancel is what unblocks
        // it, so the cancel path must drain. Otherwise it stays parked until some
        // unrelated reservation happens to release (a liveness gap).
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 4, chunk); // capacity = 4 chunks
        let (n_hold, _) = notify_flag();
        let (n_big, flag_big) = notify_flag();
        let (n_small, flag_small) = notify_flag();

        // Hold 3 of 4 chunks: 1 free.
        let _holder = match budget.reserve(chunk * 3, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Front waiter needs 2 (does not fit: 1 free < 2) → parks.
        let ticket_big = match budget.reserve(chunk * 2, n_big) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };
        // Next waiter needs 1 (would fit on the 1 free chunk) but parks behind the
        // larger front — arrival order forbids jumping it.
        let mut ticket_small = match budget.reserve(chunk, n_small) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };
        assert_eq!(budget.stats().waiters, 2);

        // Cancel the FRONT waiter. No reservation releases here — `_holder` still holds
        // its 3 chunks. The small waiter is now the front and fits on the 1 free chunk.
        drop(ticket_big);

        // The cancel must have drained the queue and granted the small waiter.
        assert!(
            flag_small.load(Ordering::Acquire),
            "cancelling the front waiter must grant the next one that now fits"
        );
        assert!(
            !flag_big.load(Ordering::Acquire),
            "cancelled waiter is not granted"
        );
        let _res_small = ticket_small
            .take()
            .expect("small waiter granted by the cancel");
        assert_eq!(budget.in_use_chunks(), 4, "3 held + 1 just granted");
        assert_eq!(budget.stats().waiters, 0);
    }

    #[test]
    fn test_wait_ticket_drop_after_granted_returns_chunks() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 2, chunk); // 2 chunks
        let (n_hold, _) = notify_flag();
        let (n_wait, flag_wait) = notify_flag();

        // Reserve 2 chunks (fill capacity)
        let holder = match budget.reserve(chunk * 2, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Park a 1-chunk waiter
        let ticket = match budget.reserve(chunk, n_wait) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // Release holder: drain grants the waiter (notify fires)
        drop(holder);
        assert!(flag_wait.load(Ordering::Acquire));
        assert_eq!(budget.in_use_chunks(), 1); // 1 chunk granted to waiter

        // Drop the ticket WITHOUT calling take() — granted-but-untaken chunks released
        drop(ticket);
        assert_eq!(budget.in_use_chunks(), 0);
    }
}

// Concurrency models for the lock-ordering invariant: `drain` takes the budget
// lock then a slot lock; `WaitTicket::drop` takes a slot lock then the budget
// lock. The invariant is that these never nest (drain holds the slot lock only to
// store a grant; WaitTicket::drop releases the slot lock before touching the
// budget). Loom exhaustively explores the interleavings; a regression that nested
// the locks would deadlock (loom reports it) and a regression in the accounting
// would trip an assertion.
#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use crate::runtime::sync::sync::atomic::{AtomicBool, Ordering};
    use crate::runtime::sync::thread;

    fn noop_notify() -> NotifyFn {
        Arc::new(|| {})
    }

    #[test]
    fn release_races_cancel() {
        // One holder fills the budget, one waiter is parked. Releasing the holder
        // (drain: budget -> slot) races dropping the waiter's ticket
        // (cancel/return: slot -> budget). Whatever the interleaving, the holder's
        // chunk is returned and the waiter ends with no chunk outstanding.
        loom::model(|| {
            let chunk = 1024;
            let budget = MemoryBudget::new(chunk, chunk); // capacity = 1 chunk

            let holder = match budget.reserve(chunk, noop_notify()) {
                Reserve::Ready(r) => r,
                Reserve::Pending(_) => unreachable!("first reserve fits"),
            };
            let ticket = match budget.reserve(chunk, noop_notify()) {
                Reserve::Pending(t) => t,
                Reserve::Ready(_) => unreachable!("budget is full"),
            };

            let h = thread::spawn(move || drop(holder));
            drop(ticket);
            h.join().unwrap();

            // Reaching here means no deadlock. The waiter was either cancelled
            // before its grant or granted then released by the ticket drop; either
            // way nothing remains reserved.
            assert_eq!(budget.in_use_chunks(), 0);
        });
    }

    #[test]
    fn release_races_take() {
        // Releasing the holder (drain stores the grant) races the waiter taking it.
        // After both complete the grant is delivered exactly once and accounting is
        // consistent across release.
        //
        // The waiter carries a real `NotifyFn` (production passes `scheduler.wake`, which
        // re-polls the parked transfer) rather than a no-op, so the release exercises the
        // actual notify path: `Reservation::drop` collects notifies under the budget lock
        // AFTER the drain has stored every grant, then fires them once the lock is dropped.
        // A regression that fired the notify before publishing the grant — the lost-wake
        // class this wireup is exposed to — would let the woken side take() nothing, which
        // the `taken.or_else(take)` recovery plus the exactly-once accounting below catch.
        loom::model(|| {
            let chunk = 1024;
            let budget = MemoryBudget::new(chunk, chunk); // capacity = 1 chunk

            let woken = Arc::new(AtomicBool::new(false));
            let notify: NotifyFn = {
                let woken = Arc::clone(&woken);
                Arc::new(move || woken.store(true, Ordering::SeqCst))
            };

            let holder = match budget.reserve(chunk, noop_notify()) {
                Reserve::Ready(r) => r,
                Reserve::Pending(_) => unreachable!("first reserve fits"),
            };
            let mut ticket = match budget.reserve(chunk, notify) {
                Reserve::Pending(t) => t,
                Reserve::Ready(_) => unreachable!("budget is full"),
            };

            let h = thread::spawn(move || drop(holder));
            let taken = ticket.take();
            h.join().unwrap();

            // The holder is gone, so the grant is available: take() either caught it
            // or a second take() retrieves it now.
            let granted = taken.or_else(|| ticket.take());
            assert!(granted.is_some(), "waiter must be granted after release");
            assert!(
                woken.load(Ordering::SeqCst),
                "release must fire the parked waiter's notify",
            );
            assert_eq!(budget.in_use_chunks(), 1);

            drop(granted);
            drop(ticket);
            assert_eq!(budget.in_use_chunks(), 0);
        });
    }

    #[test]
    fn cancel_front_grants_next_races_take() {
        // Cancelling the front waiter drains the queue and can grant the next waiter,
        // so the cancel path takes `budget -> slot` (to store the grant) just like a
        // release. This races that cancel against the granted waiter's `take`. The
        // cancelling thread already released its own slot lock before taking the budget
        // lock (WaitTicket::drop extracts state first), so no `slot -> budget` inversion
        // exists; loom exhausts the interleavings and would report a deadlock or a
        // double/lost grant.
        loom::model(|| {
            let chunk = 1024;
            let budget = MemoryBudget::new(chunk, chunk); // capacity = 1 chunk

            // Holder pins the single chunk; front and next both park behind it.
            let holder = match budget.reserve(chunk, noop_notify()) {
                Reserve::Ready(r) => r,
                Reserve::Pending(_) => unreachable!("first reserve fits"),
            };
            let front = match budget.reserve(chunk, noop_notify()) {
                Reserve::Pending(t) => t,
                Reserve::Ready(_) => unreachable!("budget is full"),
            };
            let mut next = match budget.reserve(chunk, noop_notify()) {
                Reserve::Pending(t) => t,
                Reserve::Ready(_) => unreachable!("budget is full"),
            };

            // Free the chunk, then race cancelling the front (which drains and grants
            // `next`) against `next.take()`.
            drop(holder);
            let h = thread::spawn(move || drop(front));
            let taken = next.take();
            h.join().unwrap();

            // The freed chunk reaches `next` exactly once, via whichever side wins.
            let granted = taken.or_else(|| next.take());
            assert!(
                granted.is_some(),
                "cancelling the front must grant the next waiter that now fits"
            );
            assert_eq!(budget.in_use_chunks(), 1);

            drop(granted);
            drop(next);
            assert_eq!(budget.in_use_chunks(), 0);
        });
    }
}
