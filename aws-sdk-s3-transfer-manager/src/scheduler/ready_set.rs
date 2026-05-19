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
use std::sync::RwLock;

use crossbeam_skiplist::SkipMap;

use super::descriptor::TransferDescriptor;
use crate::runtime::sync::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use crate::runtime::sync::sync::Arc;
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

/// Atomic coordination state for a group's position in the scheduling
/// hierarchy. Tracks member count, root-tree presence, and vruntime.
///
/// Separated from `GroupQueue` so that loom tests can exercise the
/// coordination protocol directly without requiring the `SkipMap` member
/// storage (which loom cannot instrument).
pub(super) struct GroupState {
    /// Floor vruntime for new members joining the group. Prevents a late
    /// arrival from having stale vruntime that gives it a head start over
    /// current members. Monotonically increasing (advanced on each pop).
    min_vruntime: AtomicU64,
    /// Group's accumulated vruntime. Determines the group's position in
    /// the root tree relative to peer groups. Shared with all descriptors
    /// in this group so that `work_generated` can advance it without a
    /// lookup back to the GroupQueue.
    group_vruntime: Arc<AtomicU64>,
    /// Number of members currently queued. Drives root-tree presence:
    /// a group with zero members is removed from the root tree.
    nr_queued: AtomicUsize,
    /// CAS flag coordinating root-tree insertion. Only the thread that
    /// wins `compare_exchange(false, true)` may add the group to the root
    /// SkipMap. Prevents duplicate entries when pop's re-insert races with
    /// insert_child's 0->1 transition.
    in_root: AtomicBool,
}

impl GroupState {
    pub(super) fn new(group_vruntime: Arc<AtomicU64>) -> Self {
        Self {
            min_vruntime: AtomicU64::new(0),
            group_vruntime,
            nr_queued: AtomicUsize::new(0),
            in_root: AtomicBool::new(false),
        }
    }

    /// A member entered the group's queue. Returns previous count.
    /// A transition from 0 to 1 means the caller should attempt
    /// `try_enter_root`.
    pub(super) fn enqueue(&self) -> usize {
        self.nr_queued.fetch_add(1, Ordering::SeqCst)
    }

    /// A member left the group's queue. Returns previous count.
    pub(super) fn dequeue(&self) -> usize {
        self.nr_queued.fetch_sub(1, Ordering::SeqCst)
    }

    /// Attempt to claim root-tree presence. Returns true if this thread
    /// won the CAS and is responsible for inserting the group into the
    /// root tree.
    pub(super) fn try_enter_root(&self) -> bool {
        self.in_root
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Clear root-tree presence (group was removed from root tree).
    pub(super) fn exit_root(&self) {
        self.in_root.store(false, Ordering::SeqCst);
    }

    pub(super) fn nr_queued(&self) -> usize {
        self.nr_queued.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn min_vruntime(&self) -> u64 {
        self.min_vruntime.load(Ordering::Acquire)
    }

    pub(super) fn advance_min_vruntime(&self, vruntime: u64) {
        self.min_vruntime.fetch_max(vruntime, Ordering::AcqRel);
    }

    pub(super) fn group_vruntime(&self) -> u64 {
        self.group_vruntime.load(Ordering::SeqCst)
    }

    pub(super) fn set_group_vruntime(&self, val: u64) {
        self.group_vruntime.store(val, Ordering::SeqCst);
    }

    pub(super) fn group_vruntime_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.group_vruntime)
    }
}

/// Per-tree CFS queue. Holds the top-level transfer plus all its descendants
/// in a single SkipMap ordered by individual vruntime.
///
/// A group with zero members is not present in the root tree. When a
/// member is inserted into an empty group, the inserting thread claims
/// root-tree entry via [`GroupState::try_enter_root`]. When the last
/// member is popped, the group exits the root tree.
struct GroupQueue {
    /// Atomic coordination state (member count, root presence, vruntime).
    state: GroupState,
    /// Members of this group, ordered by individual vruntime.
    inner: SkipMap<ReadyKey, TransferDescriptor>,
}

impl GroupQueue {
    fn new(group_vruntime: Arc<AtomicU64>) -> Self {
        Self {
            state: GroupState::new(group_vruntime),
            inner: SkipMap::new(),
        }
    }

    #[cfg(test)]
    fn new_with_initial_vruntime(initial: u64) -> Self {
        Self::new(Arc::new(AtomicU64::new(initial)))
    }

    /// Insert a descriptor. Returns the previous nr_queued count (0 means
    /// this insert transitioned the group from empty to non-empty).
    fn insert(&self, descriptor: TransferDescriptor) -> usize {
        let vruntime = descriptor.vruntime();
        let key = ReadyKey::new(vruntime, descriptor.id());
        self.inner.insert(key, descriptor);
        self.state.enqueue()
    }

    /// Pop the member with the lowest vruntime. Returns `None` if the
    /// group is empty. Advances the group's min_vruntime floor.
    fn pop(&self) -> Option<TransferDescriptor> {
        let entry = self.inner.pop_front()?;
        self.state.dequeue();
        self.state.advance_min_vruntime(entry.key().vruntime);
        Some(entry.value().clone())
    }

    /// Number of members currently queued in this group.
    fn count(&self) -> usize {
        self.state.nr_queued()
    }

    #[cfg(test)]
    /// Vruntime floor for new members joining this group.
    fn min_vruntime(&self) -> u64 {
        self.state.min_vruntime()
    }

    /// The group's accumulated vruntime (its position in the root tree).
    fn group_vruntime(&self) -> u64 {
        self.state.group_vruntime()
    }

    /// Shared handle to the group's vruntime atomic. Cloned into each
    /// descriptor so `work_generated` can advance it without a lookup.
    fn group_vruntime_arc(&self) -> Arc<AtomicU64> {
        self.state.group_vruntime_arc()
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

    /// Resolve the group_vruntime Arc for a transfer about to be enqueued.
    ///
    /// - For children: returns the parent group's existing Arc.
    /// - For top-level: creates a new Arc (will be installed into the
    ///   GroupQueue during insert_top_level).
    ///
    /// Returns None if the parent group has been removed (orphaned child).
    pub(super) fn resolve_group_vruntime(&self, id: &TransferId) -> Option<Arc<AtomicU64>> {
        match id.parent {
            Some(parent_id) => {
                let by_group = self.by_group.read().unwrap();
                by_group.get(&parent_id).map(|g| g.group_vruntime_arc())
            }
            None => {
                // Check if group already exists (re-enqueue after Pending/wake)
                let by_group = self.by_group.read().unwrap();
                if let Some(group) = by_group.get(&id.id) {
                    return Some(group.group_vruntime_arc());
                }
                drop(by_group);
                // New top-level: fresh Arc at current floor
                Some(Arc::new(AtomicU64::new(self.min_vruntime())))
            }
        }
    }

    /// Add a transfer to the ready set.
    ///
    /// Dispatches on `descriptor.id().parent`:
    /// - `parent = None` - creates a new group and inserts the descriptor as its first member.
    /// - `parent = Some(pid)` - routes the descriptor into the existing group for `pid`.
    ///
    /// Returns `Err(OrphanedChild)` if `parent` is `Some(pid)` but no group with that id exists
    /// (the parent was cancelled between spawn and enqueue).
    ///
    /// Idempotent against double-insert: gated by a compare-and-swap on
    /// the descriptor's claim flag. A descriptor that is already claimed
    /// (queued or being polled) silently returns `Ok(())` - the existing
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
            if prev_count == 0 && group.state.try_enter_root() {
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
    /// Does **not** release the descriptor's claim - the claim is held
    /// through `poll_work` and released by `generate_work` after the
    /// poll outcome is handled. This is what guarantees at most one
    /// worker is inside `poll_work` for any given transfer at a time.
    pub(super) fn pop(&self) -> Option<TransferDescriptor> {
        let entry = self.groups.pop_front()?;
        let group_key = *entry.key();
        let group = entry.value().clone();

        let descriptor = group.pop()?;

        // Mark group as not in root tree (we just popped it out).
        group.state.exit_root();

        // Update root min_vruntime (monotonically increasing)
        self.min_vruntime
            .fetch_max(group_key.group_vruntime, Ordering::AcqRel);

        // Re-insert group if it still has members, using CAS to
        // coordinate with concurrent insert_child's 0->1 path.
        // Only the thread that wins the CAS adds the group to the
        // root tree, preventing duplicate entries.
        if group.count() > 0 && group.state.try_enter_root() {
            let new_key = GroupKey {
                group_vruntime: group.group_vruntime(),
                group_id: group_key.group_id,
            };
            self.groups.insert(new_key, group);
        }

        Some(descriptor)
    }

    /// Test-only helper: advance a group's `group_vruntime` by `units`.
    ///
    /// Useful when a test wants to set up a specific gv state without
    /// running through pop. Production code does not need to call this:
    /// `pop` advances gv as part of running-cost accounting.
    /// No-op if `group_id` is not found.
    #[cfg(test)]
    pub(super) fn advance_group_vruntime(&self, group_id: u64, units: u64) {
        let by_group = self.by_group.read().unwrap();
        if let Some(group) = by_group.get(&group_id) {
            group
                .state
                .group_vruntime
                .fetch_add(units, Ordering::SeqCst);
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
        let group = Arc::new(GroupQueue::new_with_initial_vruntime(root_min));
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
        by_group.get(&group_id).map(|g| g.count())
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
            if prev_count == 0 && group.state.try_enter_root() {
                let root_min = self.min_vruntime();
                group.state.set_group_vruntime(root_min);
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
        let gv_arc = descriptor.group_vruntime_arc();
        let group = Arc::new(GroupQueue::new(gv_arc));
        group.insert(descriptor);
        // Fresh group with a member goes directly into root tree.
        // No CAS needed: we just created it, no one else has a reference.
        group.state.in_root.store(true, Ordering::SeqCst);

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

        // If we transitioned from 0->1, try to claim root-tree insertion.
        if prev_count == 0 && group.state.try_enter_root() {
            let root_min = self.min_vruntime();
            group.state.set_group_vruntime(root_min);
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
        let gq = GroupQueue::new_with_initial_vruntime(0);
        let desc = make_descriptor(1, None, 128, 0);
        gq.insert(desc.clone());
        assert_eq!(gq.count(), 1);

        let popped = gq.pop().unwrap();
        assert_eq!(popped.id().id, 1);
        assert_eq!(gq.count(), 0);
        assert!(gq.pop().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn group_queue_pop_lowest_vruntime() {
        let gq = GroupQueue::new_with_initial_vruntime(0);
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
        // Set up two groups with different group_vruntimes so pop order
        // is deterministic: group B (gv=0) should pop before group A (gv=100).
        let set = ReadySet::new();

        // Group A (id=1) with 2 members, then advance its gv to 100.
        // When pop re-inserts the group, it uses the atomic gv (100).
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.insert(make_descriptor(11, Some(1), 128, 5)).unwrap();
        set.advance_group_vruntime(1, 100);

        // Pop one member from group A. Re-inserts group A with gv=100.
        let first = set.pop().unwrap();
        assert_eq!(first.id().id, 1);

        // Group B (id=2) inserted at root_min=0, so gv=0.
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(22, Some(2), 128, 3)).unwrap();

        // Root tree: group B (gv=0), group A (gv=100).
        // Pop picks group B (lower gv), then its lowest-vruntime member.
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

        // Pop second member - group should leave root
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
        // Create group 1 and drain it.
        set.insert(make_descriptor(1, None, 128, 0)).unwrap();
        set.pop().unwrap();
        assert!(!set.root_contains_group(1));

        // Advance the root floor by exercising group 2: insert two members,
        // bump its group_vruntime via spawn cost, then pop both. Each pop
        // advances root_min via fetch_max with the popped key's gv, so by
        // the end root_min is strictly greater than zero.
        set.insert(make_descriptor(2, None, 128, 0)).unwrap();
        set.insert(make_descriptor(22, Some(2), 128, 0)).unwrap();
        set.advance_group_vruntime(2, 50);
        set.pop().unwrap();
        set.pop().unwrap();
        let floor = set.min_vruntime();
        assert!(floor > 0, "root floor should have advanced past 0");

        // Inserting a new child into the now-empty group 1 should rejoin
        // it at the current root floor (not at 0, which would let it
        // unfairly outpace other groups).
        set.insert(make_descriptor(11, Some(1), 128, 0)).unwrap();
        assert!(set.root_contains_group(1));
        assert_eq!(set.group_vruntime(1), Some(floor));
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

        // Simulate work on both - same priority means same vruntime delta
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
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    // Loom tests for the ready-set's atomic protocols.
    //
    // Tests use the real `ClaimState` and `GroupState` types (both
    // loom-compatible via the compat layer). `LoomGroup` wraps the real
    // `GroupState` with a `root_entries` counter to detect duplicate
    // root-tree insertions (a correctness violation the real code prevents
    // but that loom should verify).
    //
    // The scheduler integration tests in `tests/upload_objects_test.rs`
    // exercise the full `ReadySet` + `GroupQueue` under concurrent load
    // without loom's exhaustive exploration.

    /// Wraps the real `GroupState` with a root-tree entry counter for
    /// loom verification. In production, the root SkipMap naturally
    /// prevents duplicate keys; here we model that as a counter and
    /// assert it never exceeds 1.
    struct LoomGroup {
        state: super::GroupState,
        /// Tracks how many times this group is "in" the root tree.
        /// Must never exceed 1.
        root_entries: AtomicUsize,
    }

    impl LoomGroup {
        fn new(initial_count: usize) -> Self {
            let state = super::GroupState::new(Arc::new(
                crate::runtime::sync::sync::atomic::AtomicU64::new(0),
            ));
            // Pre-enqueue `initial_count` members.
            for _ in 0..initial_count {
                state.enqueue();
            }
            // If starting with members, mark as in root.
            if initial_count > 0 {
                state.in_root.store(true, Ordering::SeqCst);
            }
            Self {
                state,
                root_entries: AtomicUsize::new(if initial_count > 0 { 1 } else { 0 }),
            }
        }

        /// Mirrors `ReadySet::insert_child` + the 0->1 root-entry path.
        fn insert(&self) {
            let prev = self.state.enqueue();
            if prev == 0 && self.state.try_enter_root() {
                self.state.set_group_vruntime(0);
                self.root_entries.fetch_add(1, Ordering::SeqCst);
            }
        }

        /// Mirrors `ReadySet::pop` for this group.
        fn pop(&self) {
            self.root_entries.fetch_sub(1, Ordering::SeqCst);
            self.state.exit_root();
            self.state.dequeue();
            if self.state.nr_queued() > 0 && self.state.try_enter_root() {
                self.root_entries.fetch_add(1, Ordering::SeqCst);
            }
        }

        fn is_in_root(&self) -> bool {
            self.root_entries.load(Ordering::SeqCst) > 0
        }

        fn root_entries_valid(&self) -> bool {
            self.root_entries.load(Ordering::SeqCst) <= 1
        }

        fn count(&self) -> usize {
            self.state.nr_queued()
        }
    }

    #[test]
    fn concurrent_child_enqueue_under_same_parent() {
        loom::model(|| {
            let group = Arc::new(LoomGroup::new(1)); // parent already in group

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
            let group = Arc::new(LoomGroup::new(1));

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
            assert!(group.is_in_root());
            // No duplicate root-tree entries allowed.
            assert!(
                group.root_entries_valid(),
                "duplicate root-tree entry detected"
            );
        });
    }

    #[test]
    fn concurrent_pop_from_different_groups() {
        loom::model(|| {
            let g1 = Arc::new(LoomGroup::new(1));
            let g2 = Arc::new(LoomGroup::new(1));

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
            let group = Arc::new(LoomGroup::new(2));

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
            assert!(
                group.root_entries_valid(),
                "duplicate root-tree entry detected"
            );
        });
    }

    #[test]
    fn group_vruntime_advance_concurrent_with_pop() {
        loom::model(|| {
            let group = Arc::new(LoomGroup::new(2)); // two members

            let g2 = Arc::clone(&group);
            let a = thread::spawn(move || {
                g2.state.group_vruntime.fetch_add(100, Ordering::SeqCst);
            });

            let g3 = Arc::clone(&group);
            let b = thread::spawn(move || {
                g3.pop();
            });

            a.join().unwrap();
            b.join().unwrap();

            // Pop no longer advances gv; only the external advance fires.
            let gv = group.state.group_vruntime();
            assert_eq!(gv, 100);
            // One member remains
            assert_eq!(group.count(), 1);
            assert!(group.is_in_root());
        });
    }

    /// Two threads racing to enqueue the same top-level transfer. The
    /// claim gate (`ClaimState::try_claim`) ensures exactly one succeeds.
    ///
    /// Production path: `ReadySet::insert` (ready_set.rs) calls
    /// `descriptor.try_claim()` before inserting into the group tree.
    #[test]
    fn concurrent_top_level_enqueue_creates_one_group() {
        loom::model(|| {
            let claim = Arc::new(super::super::descriptor::claim::ClaimState::new());
            let insert_count = Arc::new(AtomicUsize::new(0));

            let c2 = Arc::clone(&claim);
            let ic2 = Arc::clone(&insert_count);
            let a = thread::spawn(move || {
                if c2.try_claim() {
                    ic2.fetch_add(1, Ordering::SeqCst);
                }
            });

            let c3 = Arc::clone(&claim);
            let ic3 = Arc::clone(&insert_count);
            let b = thread::spawn(move || {
                if c3.try_claim() {
                    ic3.fetch_add(1, Ordering::SeqCst);
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            // Exactly one insert succeeds
            assert_eq!(insert_count.load(Ordering::SeqCst), 1);
        });
    }

    /// Models the release-and-recheck protocol between `generate_work`
    /// (Pending path, scheduler.rs:533-537) and `Scheduler::wake`
    /// (scheduler.rs:258-262).
    ///
    /// Thread A = scheduler worker after poll_work returned Pending
    /// Thread B = external caller invoking wake(descriptor_id)
    ///
    /// Invariant: descriptor is reachable in its group after both complete.
    #[test]
    fn claim_protocol_in_hierarchical_insert() {
        loom::model(|| {
            let claim = Arc::new(super::super::descriptor::claim::ClaimState::new());
            // Descriptor starts claimed (just popped and polled).
            assert!(claim.try_claim());
            let group = Arc::new(LoomGroup::new(0)); // group exists but D not in it

            let cl_a = Arc::clone(&claim);
            let g_a = Arc::clone(&group);
            let a = thread::spawn(move || {
                // Thread A: release claim, then check wake_requested.
                // Production: ClaimGuard::release() then take_wake_requested()
                cl_a.release_claim();
                if cl_a.take_wake_requested() {
                    if cl_a.try_claim() {
                        g_a.insert();
                    }
                }
            });

            let cl_b = Arc::clone(&claim);
            let g_b = Arc::clone(&group);
            let b = thread::spawn(move || {
                // Thread B: mark wake_requested, then try_claim and insert.
                // Production: Scheduler::wake() calls mark_wake_requested
                // then ReadySet::insert() calls try_claim.
                cl_b.mark_wake_requested();
                if cl_b.try_claim() {
                    g_b.insert();
                }
            });

            a.join().unwrap();
            b.join().unwrap();

            // D must be reachable: at least one thread inserted it.
            assert!(
                group.count() > 0,
                "descriptor not reachable in group after claim/wake protocol"
            );
            assert!(group.is_in_root());
        });
    }
}
