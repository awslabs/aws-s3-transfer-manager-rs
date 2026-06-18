/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Fungible global memory admission-control budget.
//!
//! `MemoryBudget` tracks how many fixed-size chunks of memory are logically
//! reserved across all transfers. It grants reservations (RAII tickets whose drop
//! returns chunks), parks callers when the budget is exhausted, and wakes them
//! strictly FIFO as chunks are released.

use std::collections::VecDeque;

use crate::metrics::unit::ByteUnit;
use crate::runtime::sync::sync::Arc;
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
/// Non-binding budget for test handles, which size objects far below it.
#[cfg(test)]
pub(crate) const DEFAULT_MEMORY_BUDGET_BYTES: usize = 8 * ByteUnit::Gibibyte.as_bytes_usize();

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

/// Drain the wait queue strictly FIFO. Grants the front waiter if it fits, stops
/// at the first that does not (no skip-ahead). Returns collected NotifyFns that
/// callers MUST invoke AFTER releasing the budget lock.
fn drain(budget: &Arc<MemoryBudget>, inner: &mut Inner) -> Vec<NotifyFn> {
    let mut notifies = Vec::new();
    while let Some(front) = inner.waiters.front() {
        if !can_grant(front.need, inner.in_use, inner.capacity) {
            break;
        }
        let waiter = inner.waiters.pop_front().unwrap();
        inner.in_use += waiter.need;
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
    /// Capacity is `floor(capacity_bytes / chunk_bytes)` chunks.
    pub(crate) fn new(capacity_bytes: usize, chunk_bytes: usize) -> Arc<Self> {
        assert!(chunk_bytes > 0, "chunk_bytes must be > 0");
        let capacity = (capacity_bytes / chunk_bytes) as u64;
        Arc::new(Self {
            inner: Mutex::new(Inner {
                in_use: 0,
                capacity,
                waiters: VecDeque::new(),
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

    /// Attempt an immediate grant. Returns None if the queue is non-empty (no-barge)
    /// or the request does not fit.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let need = self.chunks_for(bytes);
        let mut inner = self.inner.lock();
        // No-barge: a fresh request never jumps a queued waiter, even if it would fit.
        // Combined with strict-FIFO drain (which stops at the first front that does not
        // fit), this makes a front waiter see in_use only fall until its need is met, so it
        // is never starved for need <= capacity. Bargeing would let a stream of small
        // requests indefinitely starve a larger queued one. Cost: strict FIFO
        // head-of-line-blocks smaller requests behind a large front waiter (the budget
        // idles while it accumulates) — bounded, only under mixed part sizes; uniform part
        // size never triggers it.
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
        // No-barge: a fresh request never jumps a queued waiter, even if it would fit.
        // Combined with strict-FIFO drain (which stops at the first front that does not
        // fit), this makes a front waiter see in_use only fall until its need is met, so it
        // is never starved for need <= capacity. Bargeing would let a stream of small
        // requests indefinitely starve a larger queued one. Cost: strict FIFO
        // head-of-line-blocks smaller requests behind a large front waiter (the budget
        // idles while it accumulates) — bounded, only under mixed part sizes; uniform part
        // size never triggers it.
        if inner.waiters.is_empty() && can_grant(need, inner.in_use, inner.capacity) {
            inner.in_use += need;
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
        Reserve::Pending(WaitTicket {
            slot,
            budget: Arc::clone(self),
        })
    }

    /// Resize the capacity (in bytes). Growing may drain parked waiters. Shrinking
    /// is soft: never revokes already-granted chunks, just tightens future grants.
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

    /// Chunks currently reserved (granted and not yet released).
    pub(crate) fn in_use_chunks(&self) -> u64 {
        self.inner.lock().in_use
    }

    /// Current capacity in chunks.
    pub(crate) fn capacity_chunks(&self) -> u64 {
        self.inner.lock().capacity
    }

    /// The fixed chunk size in bytes.
    pub(crate) fn chunk_bytes(&self) -> usize {
        self.chunk
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let notifies = {
            let mut inner = self.budget.inner.lock();
            inner.in_use = inner.in_use.saturating_sub(self.chunks);
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
                // Cancel: remove this waiter from the queue.
                let mut inner = self.budget.inner.lock();
                inner.waiters.retain(|w| !Arc::ptr_eq(&w.slot, &self.slot));
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
    fn test_no_barge_small_request_does_not_jump_queued_large() {
        let chunk = 1024;
        let budget = MemoryBudget::new(chunk * 4, chunk); // 4 chunks capacity
        let (n_hold, _) = notify_flag();
        let (n_large, flag_large) = notify_flag();

        // Fill all 4 chunks
        let holder = match budget.reserve(chunk * 4, n_hold) {
            Reserve::Ready(r) => r,
            Reserve::Pending(_) => panic!("expected Ready"),
        };

        // Queue a 4-chunk waiter
        let mut ticket_large = match budget.reserve(chunk * 4, n_large) {
            Reserve::Pending(t) => t,
            Reserve::Ready(_) => panic!("expected Pending"),
        };

        // try_reserve of 1 chunk must return None (no-barge) even though 1 would
        // fit if we freed 1 chunk.
        assert!(budget.try_reserve(chunk).is_none());

        // Release the holder -> the queued large waiter is served
        drop(holder);
        assert!(flag_large.load(Ordering::Acquire));
        let _res_large = ticket_large.take().expect("large waiter should be granted");

        // Now try_reserve works (queue is empty)
        // But capacity is fully used by the large waiter, so it won't fit
        assert!(budget.try_reserve(chunk).is_none());
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
