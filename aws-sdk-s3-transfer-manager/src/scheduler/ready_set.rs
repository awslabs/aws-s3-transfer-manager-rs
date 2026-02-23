/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Priority-aware ready set for transfer scheduling using CFS-style fairness.
//!
//! Transfers are scheduled by virtual runtime (vruntime) - lower vruntime runs first.
//! Priority acts as a weight affecting how fast vruntime accumulates:
//! - Higher priority = slower accumulation = more work before "catching up"
//! - Lower priority = faster accumulation = less work before yielding
//!
//! This ensures all transfers make progress (no starvation) while respecting priority.

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam_skiplist::SkipMap;

use super::descriptor::TransferDescriptor;
use super::TransferId;

/// Key for ordering transfers in the ready set.
///
/// Ordered by (vruntime ASC, id ASC) so lowest vruntime runs first,
/// with ties broken by transfer id (earlier transfers win).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyKey {
    vruntime: u64,
    id: u64,
}

impl ReadyKey {
    fn new(vruntime: u64, id: TransferId) -> Self {
        Self {
            vruntime,
            id: id.id,
        }
    }
}

impl Ord for ReadyKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.vruntime
            .cmp(&other.vruntime)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for ReadyKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Priority-aware set of ready transfers using CFS-style scheduling.
///
/// Transfers with lower vruntime are scheduled first. Priority affects
/// how fast vruntime accumulates, not strict ordering.
#[derive(Debug)]
pub(super) struct ReadySet {
    inner: SkipMap<ReadyKey, TransferDescriptor>,
    /// Tracks minimum vruntime for initializing new transfers
    min_vruntime: AtomicU64,
}

impl Default for ReadySet {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadySet {
    pub(super) fn new() -> Self {
        Self {
            inner: SkipMap::new(),
            min_vruntime: AtomicU64::new(0),
        }
    }

    /// Get current minimum vruntime for initializing new transfers.
    pub(super) fn min_vruntime(&self) -> u64 {
        self.min_vruntime.load(Ordering::Acquire)
    }

    /// Add a transfer to the ready set.
    ///
    /// INVARIANT: Callers must ensure a transfer is not inserted twice. This happens
    /// naturally if: (1) `poll_work()` returning `Ready` is immediately followed by
    /// re-insert, and (2) `wake()` is only called when the transfer returned `Pending`.
    /// Violating this invariant causes duplicate polling.
    pub(super) fn insert(&self, descriptor: TransferDescriptor) {
        let vruntime = descriptor.vruntime();
        let key = ReadyKey::new(vruntime, descriptor.id());
        self.inner.insert(key, descriptor);
    }

    /// Remove a transfer from the ready set.
    pub(super) fn remove(&self, id: TransferId, vruntime: u64) {
        let key = ReadyKey::new(vruntime, id);
        self.inner.remove(&key);
    }

    /// Pop the transfer with lowest vruntime (highest scheduling priority).
    ///
    /// Updates min_vruntime to the popped transfer's vruntime.
    pub(super) fn pop(&self) -> Option<TransferDescriptor> {
        let entry = self.inner.pop_front()?;
        let descriptor = entry.value().clone();

        // Update min_vruntime (monotonically increasing)
        self.min_vruntime
            .fetch_max(entry.key().vruntime, Ordering::AcqRel);

        Some(descriptor)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.inner.len()
    }

    #[cfg(test)]
    pub(super) fn contains(&self, id: TransferId, vruntime: u64) -> bool {
        let key = ReadyKey::new(vruntime, id);
        self.inner.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::transfer::mock::FixedWorkCount;
    use crate::scheduler::MockTransfer;
    use std::sync::Arc;

    fn make_descriptor(id: u64, priority: u8, vruntime: u64) -> TransferDescriptor {
        let tid = TransferId { id, parent: None };
        let sm = Arc::new(FixedWorkCount::new(1));
        let transfer = Box::new(MockTransfer::new(tid, sm));
        let desc = TransferDescriptor::new(transfer);
        desc.set_priority(priority);
        desc.set_vruntime(vruntime);
        desc
    }

    #[test]
    fn test_empty_set() {
        let set = ReadySet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(set.pop().is_none());
        assert_eq!(set.min_vruntime(), 0);
    }

    #[test]
    fn test_insert_and_pop() {
        let set = ReadySet::new();
        let desc = make_descriptor(1, 128, 100);

        set.insert(desc);
        assert!(!set.is_empty());
        assert_eq!(set.len(), 1);

        let popped = set.pop().unwrap();
        assert_eq!(popped.id().id, 1);
        assert!(set.is_empty());
        assert_eq!(set.min_vruntime(), 100);
    }

    #[test]
    fn test_vruntime_ordering_lowest_first() {
        let set = ReadySet::new();

        let high_vruntime = make_descriptor(1, 128, 300);
        let low_vruntime = make_descriptor(2, 128, 100);
        let mid_vruntime = make_descriptor(3, 128, 200);

        set.insert(high_vruntime);
        set.insert(low_vruntime);
        set.insert(mid_vruntime);

        // Should pop in vruntime order: lowest first
        assert_eq!(set.pop().unwrap().id().id, 2); // vruntime 100
        assert_eq!(set.pop().unwrap().id().id, 3); // vruntime 200
        assert_eq!(set.pop().unwrap().id().id, 1); // vruntime 300
        assert!(set.pop().is_none());
    }

    #[test]
    fn test_same_vruntime_ordered_by_id() {
        let set = ReadySet::new();

        let d1 = make_descriptor(10, 128, 100);
        let d2 = make_descriptor(5, 128, 100);
        let d3 = make_descriptor(15, 128, 100);

        set.insert(d1);
        set.insert(d2);
        set.insert(d3);

        // Same vruntime - ordered by id ascending (earlier transfers win)
        assert_eq!(set.pop().unwrap().id().id, 5);
        assert_eq!(set.pop().unwrap().id().id, 10);
        assert_eq!(set.pop().unwrap().id().id, 15);
    }

    #[test]
    fn test_min_vruntime_tracks_minimum() {
        let set = ReadySet::new();

        set.insert(make_descriptor(1, 128, 100));
        set.insert(make_descriptor(2, 128, 50));
        set.insert(make_descriptor(3, 128, 200));

        assert_eq!(set.min_vruntime(), 0); // Not updated until pop

        set.pop(); // pops vruntime 50
        assert_eq!(set.min_vruntime(), 50);

        set.pop(); // pops vruntime 100
        assert_eq!(set.min_vruntime(), 100);

        set.pop(); // pops vruntime 200
        assert_eq!(set.min_vruntime(), 200);
    }

    #[test]
    fn test_min_vruntime_monotonic() {
        let set = ReadySet::new();

        set.insert(make_descriptor(1, 128, 200));
        set.pop();
        assert_eq!(set.min_vruntime(), 200);

        // Insert something with lower vruntime (late arrival)
        set.insert(make_descriptor(2, 128, 100));
        set.pop();
        // min_vruntime should NOT decrease
        assert_eq!(set.min_vruntime(), 200);
    }

    #[test]
    fn test_remove() {
        let set = ReadySet::new();
        let desc = make_descriptor(1, 128, 100);

        set.insert(desc.clone());
        assert!(set.contains(desc.id(), 100));

        set.remove(desc.id(), 100);
        assert!(!set.contains(desc.id(), 100));
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_nonexistent_is_noop() {
        let set = ReadySet::new();
        let id = TransferId {
            id: 999,
            parent: None,
        };
        set.remove(id, 100);
        assert!(set.is_empty());
    }

    #[test]
    fn test_concurrent_pop() {
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let set = Arc::new(ReadySet::new());
        let pop_count = Arc::new(AtomicUsize::new(0));

        // Insert 100 descriptors - must keep references alive
        let descriptors: Vec<_> = (0..100u64)
            .map(|i| make_descriptor(i, 128, i * 10))
            .collect();

        for desc in &descriptors {
            set.insert(desc.clone());
        }

        assert_eq!(set.len(), 100);

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let set = Arc::clone(&set);
                let count = Arc::clone(&pop_count);
                thread::spawn(move || {
                    while set.pop().is_some() {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pop_count.load(Ordering::Relaxed), 100);
        assert!(set.is_empty());
    }

    #[test]
    fn test_fairness_simulation() {
        // Simulate 3 transfers with different priorities competing
        let set = ReadySet::new();

        // All start at min_vruntime (0)
        let high = make_descriptor(1, 200, 0); // High priority
        let normal = make_descriptor(2, 128, 0); // Normal priority
        let low = make_descriptor(3, 64, 0); // Low priority

        set.insert(high.clone());
        set.insert(normal.clone());
        set.insert(low.clone());

        // First round: all at vruntime 0, ordered by id
        assert_eq!(set.pop().unwrap().id().id, 1);
        assert_eq!(set.pop().unwrap().id().id, 2);
        assert_eq!(set.pop().unwrap().id().id, 3);

        // Simulate poll_work returning Ready - update vruntime BEFORE re-insert
        // delta = 128 / priority
        // high (200): delta = 128 / 200 = 0 (integer division)
        // normal (128): delta = 128 / 128 = 1
        // low (64): delta = 128 / 64 = 2

        high.work_generated();
        normal.work_generated();
        low.work_generated();

        set.insert(high);
        set.insert(normal);
        set.insert(low);

        // Now ordered by vruntime: high(0) < normal(1) < low(2)
        assert_eq!(set.pop().unwrap().id().id, 1); // high, vruntime 0
        assert_eq!(set.pop().unwrap().id().id, 2); // normal, vruntime 1
        assert_eq!(set.pop().unwrap().id().id, 3); // low, vruntime 2
    }
}
