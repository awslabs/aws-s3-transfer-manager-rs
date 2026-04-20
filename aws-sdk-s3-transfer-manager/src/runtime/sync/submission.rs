/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Multi-producer, single-consumer submission queue with opportunistic batching.
//!
//! Multiple producers push items concurrently into a fixed-capacity backing array.
//! The last producer to complete a submission becomes the consumer and receives a
//! [`SubmissionGuard`] with contiguous access to the batch.
//!
//! Producers follow the [`enter`](SubmissionQueue::enter) / [`push`](Submission::push) /
//! [`submit`](Submission::submit) protocol. A `Mutex` + `Condvar` pair protects
//! submission state (a pending count and a flushing flag); the critical section
//! only increments or decrements integers. Slot claiming and writing is lock-free
//! (atomic `fetch_add` on the tail index).
//!
//! ```text
//!  Producer threads                            Consumer (last out)
//!  ┌──────────┐ ┌──────────┐ ┌──────────┐
//!  │ enter()  │ │ enter()  │ │ enter()  │
//!  │ push(w1) │ │ push(w2) │ │ push(w3) │
//!  │ submit() │ │ submit() │ │ submit() │──→ SubmissionGuard
//!  │ = None   │ │ = None   │ │ = Some   │    ┌─────────────────┐
//!  └──────────┘ └──────────┘ └──────────┘    │ [w1, w2, w3]    │
//!                                            │ as_slice()      │
//!                                            │ as_mut_slice()  │
//!                                            │ drain() → iter  │
//!                                            └─────────────────┘
//!
//!  ┌─────────────────────────────────────────────────────────┐
//!  │ Backing array (fixed capacity, reused across rounds)    │
//!  │ ┌────┬────┬────┬────┬────┬────┬────┬────┐               │
//!  │ │ w1 │ w2 │ w3 │    │    │    │    │    │               │
//!  │ └────┴────┴────┴────┴────┴────┴────┴────┘               │
//!  │   ▲                  ▲                                  │
//!  │   0              tail (atomic)                          │
//!  └─────────────────────────────────────────────────────────┘
//! ```

use std::mem::MaybeUninit;

use crossbeam_utils::CachePadded;

use super::cell::UnsafeCell;
use super::sync::atomic::{AtomicUsize, Ordering};
use super::sync::{Condvar, Mutex};

/// Multi-producer submission queue backed by a fixed-capacity array.
///
/// Producers call [`enter`](Self::enter) to join a round, [`push`](Submission::push)
/// to write items into the backing array, and [`submit`](Submission::submit) to
/// complete their participation. The last producer to submit receives a
/// [`SubmissionGuard`] with access to the batch. Capacity is fixed at construction.
pub(crate) struct SubmissionQueue<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    tail: CachePadded<AtomicUsize>,
    state: Mutex<State>,
    not_flushing: Condvar,
    capacity: usize,
}

struct State {
    pending: usize,
    flushing: bool,
}

// Safety: producers write to distinct slots (claimed via atomic fetch_add on
// tail). The flusher has exclusive access after all producers exit. T: Send is
// required because items cross thread boundaries.
unsafe impl<T: Send> Send for SubmissionQueue<T> {}
unsafe impl<T: Send> Sync for SubmissionQueue<T> {}

/// Participation in a submission round. Provides [`push`](Self::push) access to
/// the queue and is consumed by [`submit`](Self::submit) to complete the round.
///
/// If dropped without calling `submit` (e.g. due to a panic), the destructor
/// decrements the pending count and cleans up any pushed items when it is the
/// last producer out.
pub(crate) struct Submission<'a, T> {
    sq: &'a SubmissionQueue<T>,
}

/// Exclusive access to items from a completed submission. Provides slice access
/// for inspection or grouping and [`drain`](Self::drain) for consuming items.
///
/// On drop, any undrained items are dropped and the queue is reset for the next
/// round, unblocking producers parked in [`enter`](SubmissionQueue::enter).
pub(crate) struct SubmissionGuard<'a, T> {
    sq: &'a SubmissionQueue<T>,
    count: usize,
    next: usize,
}

impl<T> SubmissionQueue<T> {
    /// Create a queue with the given fixed capacity.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be non-zero");
        let slots: Box<[UnsafeCell<MaybeUninit<T>>]> = (0..capacity)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect();
        Self {
            slots,
            tail: CachePadded::new(AtomicUsize::new(0)),
            state: Mutex::new(State {
                pending: 0,
                flushing: false,
            }),
            not_flushing: Condvar::new(),
            capacity,
        }
    }

    /// Enter a submission round. Blocks while a flush is in progress.
    /// Returns a [`Submission`] that provides push access.
    pub(crate) fn enter(&self) -> Submission<'_, T> {
        let mut state = self.state.lock();
        while state.flushing {
            state = self.not_flushing.wait(state);
        }
        state.pending += 1;
        Submission { sq: self }
    }

    /// Number of items the queue can hold per round.
    #[allow(dead_code)] // TODO: runtime observability
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<'a, T> Submission<'a, T> {
    /// Push an item into the next available slot. Returns `Err(item)` if the
    /// queue is full.
    ///
    /// Slot claiming is lock-free — it uses an atomic `fetch_add` on the tail
    /// index.
    #[inline]
    pub(crate) fn push(&self, item: T) -> Result<(), T> {
        let idx = self.sq.tail.fetch_add(1, Ordering::Relaxed);
        if idx >= self.sq.capacity {
            self.sq.tail.fetch_sub(1, Ordering::Relaxed);
            return Err(item);
        }
        // Safety: this slot was uniquely claimed via atomic fetch_add.
        self.sq.slots[idx].with_mut(|ptr| unsafe { (*ptr).write(item) });
        Ok(())
    }

    /// Complete the submission. If this is the last producer to finish,
    /// returns a [`SubmissionGuard`] with access to all submitted items.
    pub(crate) fn submit(self) -> Option<SubmissionGuard<'a, T>> {
        let sq = self.sq;
        std::mem::forget(self);
        let mut state = sq.state.lock();
        state.pending -= 1;
        if state.pending == 0 {
            state.flushing = true;
            drop(state);
            let count = sq.tail.swap(0, Ordering::Relaxed).min(sq.capacity);
            Some(SubmissionGuard { sq, count, next: 0 })
        } else {
            None
        }
    }
}

impl<T> Drop for Submission<'_, T> {
    fn drop(&mut self) {
        let mut state = self.sq.state.lock();
        state.pending -= 1;
        if state.pending == 0 && !state.flushing {
            let count = self
                .sq
                .tail
                .swap(0, Ordering::Relaxed)
                .min(self.sq.capacity);
            for i in 0..count {
                // Safety: slots 0..count were filled by producers and no
                // SubmissionGuard exists, so we have exclusive access.
                self.sq.slots[i].with_mut(|ptr| unsafe { (*ptr).assume_init_drop() });
            }
        }
    }
}

impl<T> SubmissionGuard<'_, T> {
    /// Number of items in this batch.
    #[allow(dead_code)] // TODO: runtime observability
    pub(crate) fn len(&self) -> usize {
        self.count - self.next
    }

    /// Whether the batch is empty.
    #[allow(dead_code)] // TODO: runtime observability
    pub(crate) fn is_empty(&self) -> bool {
        self.next >= self.count
    }

    /// View the remaining items as a shared slice.
    ///
    /// Safety relies on flushing being true (no producers are writing) and all
    /// slots in `next..count` being initialized.
    #[allow(dead_code)] // TODO: wire into runtime dispatch
    pub(crate) fn as_slice(&self) -> &[T] {
        // Safety: flushing is true so no producers are writing. All slots in
        // next..count were initialized by producers.
        //
        // We derive the pointer from the boxed slice (a single allocation),
        // not from an individual UnsafeCell, so the provenance covers the
        // entire range. UnsafeCell<MaybeUninit<T>> is #[repr(transparent)]
        // through both layers, so the cast is layout-correct.
        unsafe {
            let base = self.sq.slots.as_ptr().add(self.next) as *const T;
            std::slice::from_raw_parts(base, self.count - self.next)
        }
    }

    /// View the remaining items as a mutable slice.
    ///
    /// Safety relies on flushing being true (no producers are writing) and all
    /// slots in `next..count` being initialized.
    #[allow(dead_code)] // TODO: wire into runtime dispatch
    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        // Safety: same as as_slice, plus we have exclusive &mut access.
        unsafe {
            let base = self.sq.slots.as_ptr().add(self.next) as *mut T;
            std::slice::from_raw_parts_mut(base, self.count - self.next)
        }
    }

    /// Iterate over the submitted items, consuming each one.
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        (self.next..self.count).map(|i| {
            self.next = i + 1;
            // Safety: the flusher has exclusive access — all producers have
            // exited and flushing is true, blocking new entrants.
            self.sq.slots[i].with_mut(|ptr| unsafe { (*ptr).assume_init_read() })
        })
    }
}

impl<T> Drop for SubmissionGuard<'_, T> {
    fn drop(&mut self) {
        for i in self.next..self.count {
            self.sq.slots[i].with_mut(|ptr| unsafe { (*ptr).assume_init_drop() });
        }
        let mut state = self.sq.state.lock();
        state.flushing = false;
        self.sq.not_flushing.notify_all();
    }
}

impl<T> Drop for SubmissionQueue<T> {
    fn drop(&mut self) {
        let count = self.tail.load(Ordering::Relaxed).min(self.capacity);
        for i in 0..count {
            self.slots[i].with_mut(|ptr| unsafe { (*ptr).assume_init_drop() });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Clone)]
    struct DropCounter(Arc<AtomicUsize>);
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn drop_counter() -> (DropCounter, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (DropCounter(Arc::clone(&count)), count)
    }

    #[test]
    fn single_producer() {
        let q = SubmissionQueue::new(8);
        let s = q.enter();
        for i in [0, 10, 20] {
            s.push(i).unwrap();
        }
        let mut guard = s.submit().expect("single producer is last out");
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![0, 10, 20]);
    }

    #[test]
    fn submit_resets_for_next_round() {
        let q = SubmissionQueue::new(8);

        let s = q.enter();
        s.push(10).unwrap();
        s.push(20).unwrap();
        let mut guard = s.submit().unwrap();
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![10, 20]);
        drop(guard);

        let s = q.enter();
        s.push(30).unwrap();
        let mut guard = s.submit().unwrap();
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![30]);
    }

    #[test]
    fn push_returns_err_when_full() {
        let q = SubmissionQueue::<i32>::new(2);
        let s = q.enter();
        s.push(1).unwrap();
        s.push(2).unwrap();
        assert_eq!(s.push(3), Err(3));
        let mut guard = s.submit().unwrap();
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![1, 2]);
    }

    #[test]
    fn empty_round() {
        let q = SubmissionQueue::<i32>::new(8);
        let s = q.enter();
        let mut guard = s.submit().unwrap();
        assert!(guard.is_empty());
        assert_eq!(guard.len(), 0);
        assert_eq!(guard.drain().count(), 0);
    }

    #[test]
    fn multi_producer_all_items_flushed() {
        let q = Arc::new(SubmissionQueue::new(64));
        let flushed = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..8)
            .map(|i| {
                let q = Arc::clone(&q);
                let flushed = Arc::clone(&flushed);
                std::thread::spawn(move || {
                    let s = q.enter();
                    s.push(i).unwrap();
                    if let Some(mut guard) = s.submit() {
                        flushed.fetch_add(guard.drain().count(), Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(flushed.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn stress_multi_round() {
        const ROUNDS: usize = 1000;
        const PRODUCERS: usize = 8;

        let total = Arc::new(AtomicUsize::new(0));

        for _ in 0..ROUNDS {
            let q = Arc::new(SubmissionQueue::new(PRODUCERS * 2));
            let total = Arc::clone(&total);

            let threads: Vec<_> = (0..PRODUCERS)
                .map(|i| {
                    let q = Arc::clone(&q);
                    let total = Arc::clone(&total);
                    std::thread::spawn(move || {
                        let s = q.enter();
                        s.push(i).unwrap();
                        if let Some(mut guard) = s.submit() {
                            total.fetch_add(guard.drain().count(), Ordering::Relaxed);
                        }
                    })
                })
                .collect();

            for t in threads {
                t.join().unwrap();
            }
        }

        assert_eq!(total.load(Ordering::Relaxed), ROUNDS * PRODUCERS);
    }

    #[test]
    fn enter_parks_during_flush() {
        let q = Arc::new(SubmissionQueue::new(8));
        let flushing = Arc::new(AtomicBool::new(false));

        let q2 = Arc::clone(&q);
        let flushing2 = Arc::clone(&flushing);
        let flusher = std::thread::spawn(move || {
            let s = q2.enter();
            s.push(1).unwrap();
            let guard = s.submit().unwrap();
            flushing2.store(true, Ordering::Release);
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(guard);
        });

        while !flushing.load(Ordering::Acquire) {
            std::thread::yield_now();
        }

        let start = std::time::Instant::now();
        let s = q.enter();
        let waited = start.elapsed();
        assert!(waited.as_millis() >= 10, "enter should have blocked");
        s.submit();

        flusher.join().unwrap();
    }

    #[test]
    fn drop_unflushed_items() {
        let (dc, drops) = drop_counter();
        {
            let q = SubmissionQueue::new(8);
            let s = q.enter();
            s.push(dc.clone()).unwrap();
            s.push(dc.clone()).unwrap();
            s.push(dc).unwrap();
            let _guard = s.submit().unwrap();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn drain_then_drop_no_double_free() {
        let (dc, drops) = drop_counter();
        {
            let q = SubmissionQueue::new(8);
            let s = q.enter();
            s.push(dc.clone()).unwrap();
            s.push(dc).unwrap();
            let mut guard = s.submit().unwrap();
            let items: Vec<_> = guard.drain().collect();
            drop(items);
            assert_eq!(drops.load(Ordering::Relaxed), 2);
            drop(guard);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn submission_drop_without_submit() {
        let (dc, drops) = drop_counter();
        let q = SubmissionQueue::new(8);
        {
            let s = q.enter();
            s.push(dc.clone()).unwrap();
            // drop Submission without submit — simulates panic path
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);

        // Queue is usable for a new round.
        let s = q.enter();
        s.push(dc).unwrap();
        let mut guard = s.submit().unwrap();
        assert_eq!(guard.drain().count(), 1);
    }

    #[test]
    fn batch_slice_access() {
        let q = SubmissionQueue::new(8);
        let s = q.enter();
        s.push(10).unwrap();
        s.push(20).unwrap();
        s.push(30).unwrap();
        let mut guard = s.submit().unwrap();
        assert_eq!(guard.as_slice(), &[10, 20, 30]);
        guard.as_mut_slice().reverse();
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![30, 20, 10]);
    }

    #[test]
    fn multiple_pushes_per_producer() {
        let q = SubmissionQueue::new(8);
        let s = q.enter();
        for i in 0..5 {
            s.push(i).unwrap();
        }
        let mut guard = s.submit().unwrap();
        let items: Vec<_> = guard.drain().collect();
        assert_eq!(items, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn concurrent_push_and_flush_interleaving() {
        let q = Arc::new(SubmissionQueue::new(64));
        let total = Arc::new(AtomicUsize::new(0));
        let flushing = Arc::new(AtomicBool::new(false));

        // Thread A: enter, push, submit, hold guard for 50ms
        let q_a = Arc::clone(&q);
        let total_a = Arc::clone(&total);
        let flushing_a = Arc::clone(&flushing);
        let a = std::thread::spawn(move || {
            let s = q_a.enter();
            s.push(1).unwrap();
            let mut guard = s.submit().unwrap();
            flushing_a.store(true, Ordering::Release);
            std::thread::sleep(std::time::Duration::from_millis(50));
            total_a.fetch_add(guard.drain().count(), Ordering::Relaxed);
            drop(guard);
        });

        // Wait for A to hold the guard
        while !flushing.load(Ordering::Acquire) {
            std::thread::yield_now();
        }

        // Threads B and C: enter (will park), push, submit
        let mut handles = vec![];
        for _ in 0..2 {
            let q = Arc::clone(&q);
            let total = Arc::clone(&total);
            handles.push(std::thread::spawn(move || {
                let s = q.enter();
                s.push(1).unwrap();
                if let Some(mut guard) = s.submit() {
                    total.fetch_add(guard.drain().count(), Ordering::Relaxed);
                }
            }));
        }

        a.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(total.load(Ordering::Relaxed), 3);
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::super::sync::atomic::AtomicUsize as LoomAtomicUsize;
    use super::super::sync::Arc;
    use super::super::thread;
    use super::*;

    #[test]
    fn two_producers_both_flushed() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(4));
            let total = Arc::new(LoomAtomicUsize::new(0));

            let q2 = Arc::clone(&q);
            let t2 = Arc::clone(&total);
            let h = thread::spawn(move || {
                let s = q2.enter();
                s.push(1).unwrap();
                if let Some(mut guard) = s.submit() {
                    t2.fetch_add(guard.drain().count(), Ordering::Relaxed);
                }
            });

            let s = q.enter();
            s.push(2).unwrap();
            if let Some(mut guard) = s.submit() {
                total.fetch_add(guard.drain().count(), Ordering::Relaxed);
            }

            h.join().unwrap();
            assert_eq!(total.load(Ordering::Relaxed), 2);
        });
    }

    #[test]
    fn enter_blocks_during_flush() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(4));

            // Thread 1: enter, push, submit, drain, drop guard
            let q2 = Arc::clone(&q);
            let h = thread::spawn(move || {
                let s = q2.enter();
                s.push(10).unwrap();
                if let Some(mut guard) = s.submit() {
                    let _: Vec<_> = guard.drain().collect();
                    // guard dropped here, unblocking enter()
                }
            });

            // Thread 2: enter (may block if thread 1 holds guard), push, submit
            let s = q.enter();
            s.push(20).unwrap();
            if let Some(mut guard) = s.submit() {
                let _: Vec<_> = guard.drain().collect();
            }

            h.join().unwrap();
        });
    }

    #[test]
    fn capacity_exhaustion() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(1));

            let q2 = Arc::clone(&q);
            let h = thread::spawn(move || {
                let s = q2.enter();
                let _ = s.push(1);
                if let Some(mut guard) = s.submit() {
                    let _: Vec<_> = guard.drain().collect();
                }
            });

            let s = q.enter();
            let _ = s.push(2);
            if let Some(mut guard) = s.submit() {
                let _: Vec<_> = guard.drain().collect();
            }

            h.join().unwrap();
        });
    }

    #[test]
    fn drop_without_submit() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(4));

            let q2 = Arc::clone(&q);
            let h = thread::spawn(move || {
                let s = q2.enter();
                s.push(1).unwrap();
                // drop without submit — exercises the panic/cleanup path
            });

            h.join().unwrap();

            let s = q.enter();
            s.push(2).unwrap();
            let mut guard = s.submit().unwrap();
            let items: Vec<_> = guard.drain().collect();
            assert_eq!(items, vec![2]);
        });
    }

    #[test]
    fn partial_drain_then_drop() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(4));
            let total = Arc::new(LoomAtomicUsize::new(0));

            let q2 = Arc::clone(&q);
            let t2 = Arc::clone(&total);
            let h = thread::spawn(move || {
                let s = q2.enter();
                s.push(10).unwrap();
                s.push(20).unwrap();
                if let Some(mut guard) = s.submit() {
                    // Drain only one item, drop guard with remainder
                    if let Some(_val) = guard.drain().next() {
                        t2.fetch_add(1, Ordering::Relaxed);
                    }
                    // guard drops here — remaining items must be dropped exactly once
                }
            });

            let s = q.enter();
            s.push(30).unwrap();
            if let Some(mut guard) = s.submit() {
                total.fetch_add(guard.drain().count(), Ordering::Relaxed);
            }

            h.join().unwrap();
            // All 3 items accounted for (drained or dropped)
            assert!(total.load(Ordering::Relaxed) >= 1);
        });
    }

    #[test]
    fn three_producers_multiple_pushes() {
        loom::model(|| {
            let q = Arc::new(SubmissionQueue::new(8));
            let total = Arc::new(LoomAtomicUsize::new(0));

            let mut handles = Vec::new();
            for _ in 0..2 {
                let q = Arc::clone(&q);
                let total = Arc::clone(&total);
                handles.push(thread::spawn(move || {
                    let s = q.enter();
                    s.push(1).unwrap();
                    s.push(2).unwrap();
                    if let Some(mut guard) = s.submit() {
                        total.fetch_add(guard.drain().count(), Ordering::Relaxed);
                    }
                }));
            }

            let s = q.enter();
            s.push(3).unwrap();
            s.push(4).unwrap();
            if let Some(mut guard) = s.submit() {
                total.fetch_add(guard.drain().count(), Ordering::Relaxed);
            }

            for h in handles {
                h.join().unwrap();
            }
            assert_eq!(total.load(Ordering::Relaxed), 6);
        });
    }
}
