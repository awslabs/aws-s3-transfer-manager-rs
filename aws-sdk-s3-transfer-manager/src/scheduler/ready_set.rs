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
//!
//! Scheduling is hierarchical: each top-level transfer forms a "group" containing
//! itself and its descendants. Pop selects the group with lowest group_vruntime,
//! then the member with lowest individual vruntime within that group. This ensures
//! fairness across top-level transfers regardless of how many children each spawns.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crossbeam_skiplist::SkipMap;

use super::descriptor::TransferDescriptor;
use crate::runtime::sync::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::transfer::TransferId;

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

/// Key for ordering groups in the root tree.
///
/// Ordered by (group_vruntime ASC, group_id ASC) so lowest group_vruntime
/// is scheduled first, with ties broken by group id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GroupKey {
    group_vruntime: u64,
    group_id: u64,
}

impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.group_vruntime
            .cmp(&other.group_vruntime)
            .then_with(|| self.group_id.cmp(&other.group_id))
    }
}

impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-tree CFS queue. Holds the top-level transfer plus all its descendants
/// in a single SkipMap ordered by individual vruntime. Tracks the group's
/// own vruntime (used to position the group in the root tree) and a
/// member counter that gates root-tree presence: a group with zero members
/// is not present in the root tree.
struct GroupQueue {
    /// Members of this group, ordered by individual vruntime.
    inner: SkipMap<ReadyKey, TransferDescriptor>,
    /// Floor used when a new descendant joins the group (prevents stale
    /// vruntime from giving late arrivals a head start over current members).
    min_vruntime: AtomicU64,
    /// Group's accumulated vruntime. Sorts the group against peers in the
    /// root tree.
    group_vruntime: AtomicU64,
    /// Member count. Drives root-tree presence: 0 means the group is not in
    /// the root tree; >0 means it is.
    nr_queued: AtomicUsize,
}

impl GroupQueue {
    fn new(initial_group_vruntime: u64) -> Self {
        Self {
            inner: SkipMap::new(),
            min_vruntime: AtomicU64::new(0),
            group_vruntime: AtomicU64::new(initial_group_vruntime),
            nr_queued: AtomicUsize::new(0),
        }
    }

    /// Insert a descriptor. Returns the previous nr_queued count (0 means
    /// this insert transitioned the group from empty to non-empty).
    fn insert(&self, descriptor: TransferDescriptor) -> usize {
        let vruntime = descriptor.vruntime();
        let key = ReadyKey::new(vruntime, descriptor.id());
        self.inner.insert(key, descriptor);
        self.nr_queued.fetch_add(1, Ordering::SeqCst)
    }

    fn pop(&self) -> Option<TransferDescriptor> {
        let entry = self.inner.pop_front()?;
        self.nr_queued.fetch_sub(1, Ordering::SeqCst);
        // Update group's min_vruntime (monotonically increasing)
        self.min_vruntime
            .fetch_max(entry.key().vruntime, Ordering::AcqRel);
        Some(entry.value().clone())
    }

    fn nr_queued(&self) -> usize {
        self.nr_queued.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn min_vruntime(&self) -> u64 {
        self.min_vruntime.load(Ordering::Acquire)
    }

    fn group_vruntime(&self) -> u64 {
        self.group_vruntime.load(Ordering::SeqCst)
    }
}

/// Hierarchical CFS ready set for transfer scheduling.
///
/// Groups transfers by their top-level ancestor. Pop selects the group with
/// lowest group_vruntime, then the member with lowest individual vruntime
/// within that group. This ensures fairness across top-level transfers
/// regardless of how many children each spawns.
pub(super) struct ReadySet {
    /// Root tree: groups sorted by their group_vruntime. Pop selects the
    /// lowest-key group. A group is present here only when it has at least
    /// one member (`nr_queued > 0`).
    groups: SkipMap<GroupKey, Arc<GroupQueue>>,
    /// Lookup by top-level transfer id. Always contains a group while its
    /// owning top-level transfer is alive in the scheduler, even when the
    /// group is currently empty (and therefore absent from `groups`).
    /// Removed only by [`ReadySet::remove_group`] on terminal cleanup.
    by_group: RwLock<HashMap<u64, Arc<GroupQueue>>>,
    /// Floor used when placing a new top-level group in the root tree.
    /// Monotonically non-decreasing.
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
            groups: SkipMap::new(),
            by_group: RwLock::new(HashMap::new()),
            min_vruntime: AtomicU64::new(0),
        }
    }

    /// Get current minimum vruntime for initializing new transfers.
    pub(super) fn min_vruntime(&self) -> u64 {
        self.min_vruntime.load(Ordering::Acquire)
    }

    /// Add a transfer to the ready set.
    ///
    /// Dispatches on `descriptor.id().parent`:
    /// - `parent = None` — creates a new group and inserts the descriptor as its first member.
    /// - `parent = Some(pid)` — routes the descriptor into the existing group for `pid`.
    ///
    /// Returns `Err(OrphanedChild)` if `parent` is `Some(pid)` but no group with that id exists
    /// (the parent was cancelled between spawn and enqueue).
    ///
    /// Idempotent against double-insert: gated by a compare-and-swap on
    /// the descriptor's claim flag. A descriptor that is already claimed
    /// (queued or being polled) silently returns `Ok(())` — the existing
    /// presence in the ready set or the in-flight poll is the canonical
    /// one.
    pub(super) fn insert(&self, descriptor: TransferDescriptor) -> Result<(), OrphanedChild> {
        // try_claim succeeds only when the descriptor is not already
        // claimed. On failure, the existing claim owner is responsible
        // for re-insertion (wake_requested handles wakes that arrive
        // mid-poll).
        if !descriptor.try_claim() {
            return Ok(());
        }

        let tid = descriptor.id();
        match tid.parent {
            None => {
                self.insert_top_level(tid.id, descriptor);
                Ok(())
            }
            Some(parent_id) => self.insert_child(parent_id, descriptor),
        }
    }

    /// Re-insert a descriptor that already holds its claim (i.e., the
    /// current thread popped it, polled it, and got `Ready`). Bypasses
    /// the CAS gate in [`Self::insert`] because we know `claimed` is
    /// already `true` and we are the single owner of the claim.
    pub(super) fn reinsert_under_claim(&self, descriptor: TransferDescriptor) {
        let tid = descriptor.id();
        let group_id = tid.parent.unwrap_or(tid.id);
        let by_group = self.by_group.read().unwrap();
        if let Some(group) = by_group.get(&group_id) {
            let group = Arc::clone(group);
            drop(by_group);
            let prev_count = group.insert(descriptor);
            // If we transitioned from 0→1, group was not in root tree — re-add it
            if prev_count == 0 {
                let gv = group.group_vruntime();
                let key = GroupKey {
                    group_vruntime: gv,
                    group_id,
                };
                self.groups.insert(key, group);
            }
        }
    }

    /// Pop the transfer with lowest vruntime (highest scheduling priority).
    ///
    /// Selects the group with lowest group_vruntime, then the member with
    /// lowest individual vruntime within that group.
    ///
    /// Updates min_vruntime to the popped group's group_vruntime.
    ///
    /// Does **not** release the descriptor's claim — the claim is held
    /// through `poll_work` and released by `generate_work` after the
    /// poll outcome is handled. This is what guarantees at most one
    /// worker is inside `poll_work` for any given transfer at a time.
    pub(super) fn pop(&self) -> Option<TransferDescriptor> {
        let entry = self.groups.pop_front()?;
        let group_key = *entry.key();
        let group = entry.value().clone();

        let descriptor = group.pop()?;

        // Update root min_vruntime (monotonically increasing)
        self.min_vruntime
            .fetch_max(group_key.group_vruntime, Ordering::AcqRel);

        // Re-insert group if it still has members
        if group.nr_queued() > 0 {
            let new_key = GroupKey {
                group_vruntime: group.group_vruntime(),
                group_id: group_key.group_id,
            };
            self.groups.insert(new_key, group);
        }

        Some(descriptor)
    }

    /// Advance a group's group_vruntime by `units`.
    ///
    /// Used to charge spawn cost when a child is enqueued under a parent group.
    /// No-op if `group_id` is not found (parent already cancelled).
    pub(super) fn advance_group_vruntime(&self, group_id: u64, units: u64) {
        let by_group = self.by_group.read().unwrap();
        if let Some(group) = by_group.get(&group_id) {
            group.group_vruntime.fetch_add(units, Ordering::SeqCst);
        }
    }

    /// Remove a group from the ready set. Called when the top-level transfer
    /// owning the group is terminated. After this, future child enqueues with
    /// `parent = Some(group_id)` will return `Err(OrphanedChild)`.
    pub(super) fn remove_group(&self, group_id: u64) {
        let removed = self.by_group.write().unwrap().remove(&group_id);
        if removed.is_some() {
            // Linear scan of root tree to find and remove the entry for this group.
            // Acceptable because remove_group is called once per top-level transfer
            // lifetime (terminal path only), not on the hot scheduling path.
            let key = self
                .groups
                .iter()
                .find(|entry| entry.key().group_id == group_id)
                .map(|entry| *entry.key());
            if let Some(k) = key {
                self.groups.remove(&k);
            }
        }
    }

    /// Pre-register an empty group for `group_id` so subsequent child
    /// inserts can find it. Use when a parent transfer is constructed
    /// outside the normal `Scheduler::enqueue_transfer` path (test setups
    /// that drive `poll_work` directly) but its children still need a
    /// group to land in. The group is empty (not in the root tree) until
    /// the first child arrives.
    #[cfg(test)]
    pub(crate) fn register_empty_group_for_test(&self, group_id: u64) {
        let root_min = self.min_vruntime();
        let group = Arc::new(GroupQueue::new(root_min));
        self.by_group.write().unwrap().insert(group_id, group);
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    #[cfg(test)]
    pub(super) fn group_count(&self) -> usize {
        self.by_group.read().unwrap().len()
    }

    #[cfg(test)]
    pub(super) fn member_count(&self, group_id: u64) -> Option<usize> {
        let by_group = self.by_group.read().unwrap();
        by_group.get(&group_id).map(|g| g.nr_queued())
    }

    #[cfg(test)]
    pub(super) fn group_vruntime(&self, group_id: u64) -> Option<u64> {
        let by_group = self.by_group.read().unwrap();
        by_group.get(&group_id).map(|g| g.group_vruntime())
    }

    #[cfg(test)]
    pub(super) fn group_min_vruntime(&self, group_id: u64) -> Option<u64> {
        let by_group = self.by_group.read().unwrap();
        by_group.get(&group_id).map(|g| g.min_vruntime())
    }

    #[cfg(test)]
    pub(super) fn root_contains_group(&self, group_id: u64) -> bool {
        self.groups
            .iter()
            .any(|entry| entry.key().group_id == group_id)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn insert_top_level(&self, group_id: u64, descriptor: TransferDescriptor) {
        // If a group already exists for this id (re-insert path: top-level
        // transfer returned Pending, was popped, then woken and re-inserted),
        // add the descriptor to the existing group rather than overwriting it.
        // Overwriting would orphan any descendants currently in the group.
        let by_group = self.by_group.read().unwrap();
        if let Some(group) = by_group.get(&group_id) {
            let group = Arc::clone(group);
            drop(by_group);
            let prev_count = group.insert(descriptor);
            if prev_count == 0 {
                let root_min = self.min_vruntime();
                group.group_vruntime.store(root_min, Ordering::SeqCst);
                let key = GroupKey {
                    group_vruntime: root_min,
                    group_id,
                };
                self.groups.insert(key, group);
            }
            return;
        }
        drop(by_group);

        let root_min = self.min_vruntime();
        let group = Arc::new(GroupQueue::new(root_min));
        group.insert(descriptor);

        let key = GroupKey {
            group_vruntime: root_min,
            group_id,
        };
        self.groups.insert(key, Arc::clone(&group));
        self.by_group.write().unwrap().insert(group_id, group);
    }

    fn insert_child(
        &self,
        parent_id: u64,
        descriptor: TransferDescriptor,
    ) -> Result<(), OrphanedChild> {
        let by_group = self.by_group.read().unwrap();
        let group = match by_group.get(&parent_id) {
            Some(g) => Arc::clone(g),
            None => {
                // Release the claim we took since we're rejecting this insert
                descriptor.release_claim();
                return Err(OrphanedChild);
            }
        };
        drop(by_group);

        let prev_count = group.insert(descriptor);

        // If we transitioned from 0→1, group was not in root tree — re-add it
        if prev_count == 0 {
            let root_min = self.min_vruntime();
            group.group_vruntime.store(root_min, Ordering::SeqCst);
            let key = GroupKey {
                group_vruntime: root_min,
                group_id: parent_id,
            };
            self.groups.insert(key, group);
        }

        Ok(())
    }
}

/// Marker error returned from [`ReadySet::insert`] when a child's parent group
/// does not exist (parent was cancelled between spawn and enqueue).
/// The caller is responsible for cancelling the orphaned child.
#[derive(Debug)]
pub(super) struct OrphanedChild;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::transfer::mock::FixedWorkCount;
    use crate::scheduler::MockTransfer;
    use std::sync::Arc as StdArc;

    fn make_descriptor(
        id: u64,
        parent: Option<u64>,
        priority: u8,
        vruntime: u64,
    ) -> TransferDescriptor {
        let tid = TransferId { id, parent };
        let sm = StdArc::new(FixedWorkCount::new(1));
        let transfer = Box::new(MockTransfer::new(tid, sm));
        let desc = TransferDescriptor::new(transfer);
        desc.set_priority(priority);
        desc.set_vruntime(vruntime);
        desc
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn group_queue_insert_pop_single() {
        let gq = GroupQueue::new(0);
        let desc = make_descriptor(1, None, 128, 0);
        gq.insert(desc.clone());
        assert_eq!(gq.nr_queued(), 1);

        let popped = gq.pop().unwrap();
        assert_eq!(popped.id().id, 1);
        assert_eq!(gq.nr_queued(), 0);
        assert!(gq.pop().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn group_queue_pop_lowest_vruntime() {
        let gq = GroupQueue::new(0);
        gq.insert(make_descriptor(10, None, 128, 5));
        gq.insert(make_descriptor(11, None, 128, 1));
        gq.insert(make_descriptor(12, None, 128, 3));

        assert_eq!(gq.pop().unwrap().id().id, 11); // vruntime 1
        assert_eq!(gq.pop().unwrap().id().id, 12); // vruntime 3
        assert_eq!(gq.pop().unwrap().id().id, 10); // vruntime 5
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn ready_set_insert_no_parent_creates_group() {
        let set = ReadySet::new();
        let desc = make_descriptor(42, None, 128, 0);
        set.insert(desc).unwrap();

        assert_eq!(set.group_count(), 1);
        assert_eq!(set.member_count(42), Some(1));
        assert!(set.root_contains_group(42));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn ready_set_insert_with_parent_finds_existing_group() {
        let set = ReadySet::new();
        set.insert(make_descriptor(10, None, 128, 0)).unwrap();
        set.insert(make_descriptor(20, Some(10), 128, 0)).unwrap();

        assert_eq!(set.group_count(), 1);
        assert_eq!(set.member_count(10), Some(2));
        assert!(set.root_contains_group(10));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn ready_set_insert_child_with_unknown_parent() {
        let set = ReadySet::new();
        let desc = make_descriptor(20, Some(99), 128, 0);
        let result = set.insert(desc);
        assert!(result.is_err());
        assert_eq!(set.group_count(), 0);
        assert!(!set.root_contains_group(99));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn ready_set_pop_returns_lowest_group_lowest_member() {
        let set = ReadySet::new();

        // Create group B (id=2) first at root_min=0 (gv=0 in root tree)
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(21, Some(2), 128, 3)).unwrap();

        // Create group A (id=1) also at root_min=0 (gv=0 in root tree)
        // With gv tie, group_id breaks it: 1 < 2, so group A pops first.
        // To make group B pop first, we need group A to have higher gv.
        // Drain group A, advance root_min, then have A rejoin at higher floor.

        // Instead: insert group A with members that have higher vruntimes.
        // The group_vruntime in the root tree is set at insert time to root_min.
        // Both groups get gv=0. Tie-break: lower group_id wins.
        // To test "lowest group first", we need different gv values.

        // Strategy: insert group A, pop all its members (advancing root_min),
        // then insert group B which gets gv = new root_min.
        let set = ReadySet::new();

        // Group A (id=1): insert with vruntime=10
        set.insert(make_descriptor(1, None, 128, 10)).unwrap();
        // Pop group A to advance root min_vruntime to 10 (gv of group A was 0)
        // Actually root min_vruntime advances to group_vruntime of popped group = 0
        set.pop().unwrap();
        // root min_vruntime is now 0 (the gv of group A when it was in root tree)

        // Better approach: use the fact that groups are keyed by (gv, group_id).
        // Insert group A at root_min=0, pop it to advance root_min to 0.
        // Insert group B at root_min=0. Both start at gv=0.
        // To get different gv: drain group A, insert group B with high vruntime,
        // pop B to advance root_min, then have A rejoin at new root_min.

        let set = ReadySet::new();

        // Step 1: Create group B (id=2) with gv=0
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(21, Some(2), 128, 0)).unwrap();

        // Step 2: Create group A (id=1), drain it, then rejoin at higher root_min
        // First advance root_min by popping group B temporarily... this is circular.

        // Simplest correct test: two groups with different gv achieved by
        // having one group drain and rejoin after root_min advances.
        let set = ReadySet::new();

        // Insert group A (id=1) with high vruntime member to advance root_min on pop
        set.insert(make_descriptor(1, None, 128, 100)).unwrap();
        // Pop group A: root_min advances to gv=0 (group A's gv was 0 at insert)
        let _a = set.pop().unwrap();
        // root_min is still 0

        // Insert group B (id=2) at root_min=0, gv=0
        set.insert(make_descriptor(2, None, 128, 50)).unwrap();
        // Pop group B: root_min advances to 0 (group B's gv was 0)
        let _b = set.pop().unwrap();

        // Now root_min = 0. We need to get it higher.
        // The issue: root_min advances to the popped group's group_vruntime,
        // which is always the value at insert time (root_min at that point).

        // OK, different approach entirely. The root_min only advances when we pop.
        // It advances to the popped group's gv (which was root_min at insert time).
        // So root_min can only advance if a group was inserted when root_min was
        // already > 0. Chicken-and-egg.

        // The way root_min advances in practice: group is inserted at root_min=X,
        // then popped, advancing root_min to X. Next group inserted at root_min=X,
        // popped, root_min stays X. To advance further, we need advance_group_vruntime
        // to change the gv BEFORE the group is popped... but the SkipMap key is fixed.

        // Actually wait — let me re-read my pop implementation. The pop uses
        // `group.group_vruntime()` for re-insert, but `group_key.group_vruntime`
        // for updating root min_vruntime. So root_min advances to the KEY's gv,
        // not the atomic's current value.

        // The only way to get different gv values in the root tree is to have
        // groups inserted at different root_min values. And root_min only advances
        // when we pop. So:
        // 1. Insert group X at root_min=0 with a high-vruntime member
        // 2. Pop group X → root_min advances to 0 (X's gv was 0)
        // Hmm, this doesn't help.

        // Actually I realize the issue: root_min advances to the GROUP's gv,
        // not the member's vruntime. All groups start with gv = root_min at
        // insert time. So root_min can never advance beyond 0 unless we
        // advance a group's gv and then somehow get that reflected in the key.

        // The re-insert after pop uses `group.group_vruntime()` (the atomic).
        // So if we advance_group_vruntime BEFORE a pop that re-inserts the group,
        // the re-inserted key will have the new gv!

        // Strategy:
        // 1. Insert group A (id=1) with 2 members. gv=0 in root tree.
        // 2. advance_group_vruntime(1, 100) — atomic is now 100, but key is still 0.
        // 3. Pop from group A — pops one member. Group still has 1 member.
        //    Re-inserts group A with key gv=100 (reads atomic). root_min advances to 0.
        // 4. Insert group B (id=2). gv = root_min = 0.
        // 5. Now root tree has: group B (gv=0, id=2) and group A (gv=100, id=1).
        // 6. Pop → should pick group B (lower gv).

        let set = ReadySet::new();

        // Group A with 2 members
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(11, Some(1), 128, 5)).unwrap();

        // Advance group A's gv so re-insert after pop uses higher key
        set.advance_group_vruntime(1, 100);

        // Pop one member from group A. Group re-inserted with gv=100.
        let first = set.pop().unwrap();
        assert_eq!(first.id().id, 1); // lowest vruntime member (id=1, vrt=0)

        // Now insert group B at root_min=0
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(22, Some(2), 128, 3)).unwrap();

        // Root tree: group B (gv=0, id=2), group A (gv=100, id=1)
        // Pop should pick group B, then lowest member in B (id=2, vrt=0)
        let popped = set.pop().unwrap();
        assert_eq!(popped.id().id, 2);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn pop_competes_composite_with_its_children() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 0)).unwrap(); // composite, iv=0
        set.insert(make_descriptor(11, Some(1), 128, 5)).unwrap(); // child, iv=5 (set by floor but we override)
        set.insert(make_descriptor(12, Some(1), 128, 10)).unwrap(); // child, iv=10

        // Override child vruntimes to be higher than composite
        // (insert_child sets them to group.min_vruntime which is 0, so we need
        // to use descriptors with pre-set vruntimes that won't be overridden)
        // Actually insert_child calls set_vruntime to floor. Let's just verify
        // the composite (id=1, vruntime=0) pops first since all children also got vruntime=0
        // but id=1 < id=11 < id=12 in tie-breaking.
        let popped = set.pop().unwrap();
        assert_eq!(popped.id().id, 1); // composite wins (lowest id at same vruntime)
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn group_leaves_root_when_empty() {
        let set = ReadySet::new();
        assert!(set.is_empty());
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(2, Some(1), 128, 0)).unwrap();

        // Pop first member
        set.pop().unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.member_count(1), Some(1));

        // Pop second member — group should leave root
        set.pop().unwrap();
        assert!(!set.root_contains_group(1));
        assert_eq!(set.member_count(1), Some(0));
        // Group still in by_group
        assert_eq!(set.group_count(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn group_rejoins_root_on_next_insert() {
        let set = ReadySet::new();
        // Create group 1 and drain it
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.pop().unwrap();
        assert!(!set.root_contains_group(1));

        // Advance root min_vruntime: insert group 2 with 2 members,
        // advance its gv, pop one (re-inserts with new gv), then pop again.
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(22, Some(2), 128, 0)).unwrap();
        set.advance_group_vruntime(2, 50);
        // Pop first member: group 2 re-inserted with gv=50. root_min advances to 0.
        set.pop().unwrap();
        // Pop second member: group 2 removed. root_min advances to 50.
        set.pop().unwrap();
        assert_eq!(set.min_vruntime(), 50);

        // Insert child into group 1 — should rejoin at root floor (50)
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.group_vruntime(1), Some(50));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn enqueue_with_parent_advances_parent_vruntime() {
        let set = ReadySet::new();
        let parent = make_descriptor(1, None, 128, 0);
        set.insert(parent.clone()).unwrap();

        assert_eq!(set.group_vruntime(1), Some(0));

        // Simulate spawn cost: advance group vruntime per child
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();
        set.advance_group_vruntime(1, 128);
        assert_eq!(set.group_vruntime(1), Some(128));

        set.insert(make_descriptor(12, Some(1), 128, 0)).unwrap();
        set.advance_group_vruntime(1, 128);
        assert_eq!(set.group_vruntime(1), Some(256));

        set.insert(make_descriptor(13, Some(1), 128, 0)).unwrap();
        set.advance_group_vruntime(1, 128);
        assert_eq!(set.group_vruntime(1), Some(384));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn enqueue_no_parent_does_not_advance_anyone() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();

        assert_eq!(set.group_vruntime(1), Some(0));
        assert_eq!(set.group_vruntime(2), Some(0));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn child_initial_vruntime_uses_group_min() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 100)).unwrap();

        // Pop and re-insert to advance group min_vruntime
        let popped = set.pop().unwrap();
        // group min_vruntime is now 100
        assert_eq!(set.group_min_vruntime(1), Some(100));

        // Re-insert parent under claim
        set.reinsert_under_claim(popped);

        // Insert child at group.min_vruntime (caller's responsibility to set this)
        let floor = set.group_min_vruntime(1).unwrap();
        let child = make_descriptor(11, Some(1), 128, floor);
        set.insert(child.clone()).unwrap();
        assert_eq!(child.vruntime(), 100);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn reinsert_under_claim_in_group_bypasses_cas() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();

        // Pop (claim held)
        let popped = set.pop().unwrap();

        // Simulate work_generated to advance vruntime
        popped.work_generated();
        let new_vrt = popped.vruntime();
        assert!(new_vrt > 0);

        // Re-insert under claim
        set.reinsert_under_claim(popped);
        assert!(set.root_contains_group(1));
        // Group should have 2 members again (the other one + re-inserted)
        assert_eq!(set.member_count(1), Some(2));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn child_terminating_keeps_group_alive_until_last_child() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();
        set.insert(make_descriptor(12, Some(1), 128, 0)).unwrap();

        // Pop one (simulates termination)
        set.pop().unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.member_count(1), Some(2));

        // Pop second
        set.pop().unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.member_count(1), Some(1));

        // Pop last
        set.pop().unwrap();
        assert!(!set.root_contains_group(1));
        assert_eq!(set.member_count(1), Some(0));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn priority_change_on_top_level_reweights_group() {
        let set = ReadySet::new();
        let t1 = make_descriptor(1, None, 128, 0);
        let t2 = make_descriptor(2, None, 128, 0);
        set.insert(t1.clone()).unwrap();
        set.insert(t2.clone()).unwrap();

        // Simulate work on both — same priority means same vruntime delta
        t1.work_generated();
        t2.work_generated();

        // Now change t1's priority to higher (slower accumulation)
        t1.set_priority(255);

        // Pop both, re-insert, do more work
        let p1 = set.pop().unwrap();
        let p2 = set.pop().unwrap();
        set.reinsert_under_claim(p1.clone());
        set.reinsert_under_claim(p2.clone());

        // After more work, higher priority (t1) should have lower vruntime
        p1.work_generated();
        p2.work_generated();

        // t1 (priority 255) accumulates less vruntime than t2 (priority 128)
        assert!(
            p1.vruntime() < p2.vruntime(),
            "higher priority should accumulate less vruntime: t1={}, t2={}",
            p1.vruntime(),
            p2.vruntime()
        );
    }

    // remove_group: removes group, orphans future children, allows recreation
    #[cfg_attr(miri, ignore)]
    #[test]
    fn remove_group_orphans_children_and_allows_recreation() {
        let set = ReadySet::new();
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();

        set.remove_group(1);
        assert!(!set.root_contains_group(1));
        assert_eq!(set.group_count(), 0);

        // Subsequent child insert returns OrphanedChild
        let result = set.insert(make_descriptor(12, Some(1), 128, 0));
        assert!(result.is_err());

        // Recreating the group with a new top-level insert works
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.group_count(), 1);
        assert_eq!(set.member_count(1), Some(1));
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Simulates the nr_queued protocol: increment on insert, decrement on pop.
    /// The group-in-root invariant depends on nr_queued being accurate.
    struct GroupState {
        nr_queued: AtomicUsize,
        in_root: AtomicBool,
        group_vruntime: AtomicU64,
    }

    impl GroupState {
        fn new(initial_count: usize) -> Self {
            Self {
                nr_queued: AtomicUsize::new(initial_count),
                in_root: AtomicBool::new(initial_count > 0),
                group_vruntime: AtomicU64::new(0),
            }
        }

        /// Simulate insert_child: increment nr_queued atomically, add to root
        /// if we transitioned from 0 to non-zero.
        fn insert(&self) {
            let prev = self.nr_queued.fetch_add(1, Ordering::SeqCst);
            if prev == 0 {
                // We transitioned 0→1, responsible for adding to root
                self.in_root.store(true, Ordering::SeqCst);
            }
        }

        /// Simulate pop: remove from root, decrement, conditionally re-add.
        fn pop(&self) {
            // Pop removes group from root tree first
            self.in_root.store(false, Ordering::SeqCst);
            // Decrement nr_queued
            self.nr_queued.fetch_sub(1, Ordering::SeqCst);
            // Re-insert if still has members
            if self.nr_queued.load(Ordering::SeqCst) > 0 {
                self.in_root.store(true, Ordering::SeqCst);
            }
        }

        fn is_in_root(&self) -> bool {
            self.in_root.load(Ordering::SeqCst)
        }

        fn count(&self) -> usize {
            self.nr_queued.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn concurrent_child_enqueue_under_same_parent() {
        loom::model(|| {
            let group = Arc::new(GroupState::new(1)); // parent already in group

            let g2 = Arc::clone(&group);
            let a = thread::spawn(move || {
                g2.insert(); // child 1
            });

            let g3 = Arc::clone(&group);
            let b = thread::spawn(move || {
                g3.insert(); // child 2
            });

            a.join().unwrap();
            b.join().unwrap();

            assert_eq!(group.count(), 3); // parent + 2 children
            assert!(group.is_in_root());
        });
    }

    #[test]
    fn pop_last_member_concurrent_with_insert() {
        loom::model(|| {
            let group = Arc::new(GroupState::new(1));

            let g2 = Arc::clone(&group);
            let a = thread::spawn(move || {
                g2.pop();
            });

            let g3 = Arc::clone(&group);
            let b = thread::spawn(move || {
                g3.insert();
            });

            a.join().unwrap();
            b.join().unwrap();

            // Pop decrements once, insert increments once: count is 1 in
            // every interleaving (started at 1).
            assert_eq!(group.count(), 1);
            // With a member present, the group must be in the root tree.
            // The empty-flag race between in_root.store(false) in pop and
            // the in_root.store(true) in insert (when prev was 0) must
            // converge to true regardless of ordering.
            assert!(group.is_in_root());
        });
    }

    #[test]
    fn concurrent_pop_from_different_groups() {
        loom::model(|| {
            let g1 = Arc::new(GroupState::new(1));
            let g2 = Arc::new(GroupState::new(1));

            let g1c = Arc::clone(&g1);
            let a = thread::spawn(move || {
                g1c.pop();
            });

            let g2c = Arc::clone(&g2);
            let b = thread::spawn(move || {
                g2c.pop();
            });

            a.join().unwrap();
            b.join().unwrap();

            // Both pops succeed independently
            assert_eq!(g1.count(), 0);
            assert_eq!(g2.count(), 0);
        });
    }

    #[test]
    fn enqueue_concurrent_with_pop_same_group() {
        loom::model(|| {
            // Start with 2 members so neither thread triggers the
            // empty-transition path. Distinguishes from
            // pop_last_member_concurrent_with_insert (which starts at 1).
            let group = Arc::new(GroupState::new(2));

            let g2 = Arc::clone(&group);
            let a = thread::spawn(move || {
                g2.pop();
            });

            let g3 = Arc::clone(&group);
            let b = thread::spawn(move || {
                g3.insert();
            });

            a.join().unwrap();
            b.join().unwrap();

            // Pop decrements once, insert increments once: count is 2 in
            // every interleaving (started at 2).
            assert_eq!(group.count(), 2);
            assert!(group.is_in_root());
        });
    }

    #[test]
    fn group_vruntime_advance_concurrent_with_pop() {
        loom::model(|| {
            let group = Arc::new(GroupState::new(2)); // two members

            let g2 = Arc::clone(&group);
            let a = thread::spawn(move || {
                g2.group_vruntime.fetch_add(100, Ordering::SeqCst);
            });

            let g3 = Arc::clone(&group);
            let b = thread::spawn(move || {
                g3.pop();
            });

            a.join().unwrap();
            b.join().unwrap();

            // gv is either 100 (advance happened) regardless of pop ordering
            let gv = group.group_vruntime.load(Ordering::SeqCst);
            assert_eq!(gv, 100);
            // One member remains
            assert_eq!(group.count(), 1);
            assert!(group.is_in_root());
        });
    }

    #[test]
    fn concurrent_top_level_enqueue_creates_one_group() {
        loom::model(|| {
            // Simulates the try_claim CAS gate: only one thread wins.
            let claimed = Arc::new(AtomicBool::new(false));
            let insert_count = Arc::new(AtomicUsize::new(0));

            let c2 = Arc::clone(&claimed);
            let ic2 = Arc::clone(&insert_count);
            let a = thread::spawn(move || {
                // try_claim: CAS false -> true
                if c2
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    ic2.fetch_add(1, Ordering::SeqCst);
                }
            });

            let c3 = Arc::clone(&claimed);
            let ic3 = Arc::clone(&insert_count);
            let b = thread::spawn(move || {
                if c3
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    ic3.fetch_add(1, Ordering::SeqCst);
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            // Exactly one insert succeeds
            assert_eq!(insert_count.load(Ordering::SeqCst), 1);
        });
    }

    //
    // Thread A: scheduler released claim on D, about to call take_wake_requested.
    // Thread B: caller invokes wake(D): mark_wake_requested then ready_set.insert(D).
    // Assert: D is reachable (in its group) at end of model.
    #[test]
    fn claim_protocol_in_hierarchical_insert() {
        loom::model(|| {
            // Model the claim + wake_requested + group membership protocol.
            // claimed=true initially (descriptor was just popped and polled).
            let claimed = Arc::new(AtomicBool::new(true));
            let wake_requested = Arc::new(AtomicBool::new(false));
            let group = Arc::new(GroupState::new(0)); // group exists but D not currently in it

            let c_a = Arc::clone(&claimed);
            let w_a = Arc::clone(&wake_requested);
            let g_a = Arc::clone(&group);
            let a = thread::spawn(move || {
                // Thread A: release claim, then check wake_requested.
                // If wake_requested, try_claim and insert into group.
                c_a.store(false, Ordering::SeqCst);
                if w_a.swap(false, Ordering::SeqCst) {
                    // Re-claim and insert
                    if c_a
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        g_a.insert();
                    }
                }
            });

            let c_b = Arc::clone(&claimed);
            let w_b = Arc::clone(&wake_requested);
            let g_b = Arc::clone(&group);
            let b = thread::spawn(move || {
                // Thread B: mark wake_requested, then try_claim and insert.
                w_b.store(true, Ordering::SeqCst);
                if c_b
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    g_b.insert();
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            // D must be reachable: at least one thread inserted it into the group.
            assert!(
                group.count() > 0,
                "descriptor not reachable in group after claim/wake protocol"
            );
            assert!(group.is_in_root());
        });
    }
}
