/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A composite transfer is a parent that supervises child transfers. It starts one child per file
//! or object and collects each child's result. `upload_objects`, `download_objects`, and sync are
//! all composite transfers.
//!
//! This module holds the bookkeeping every composite needs. The parent reserves a slot and starts a
//! child in it. When the child finishes, the parent hands it to a reap. The reap joins the child
//! and frees its slot. `Children::is_idle` returns true when the parent holds no reserved slot,
//! runs no child, and has no reap out. A parent that waits for it before finishing leaves no child
//! behind.
//!
//! Each step outside the parent's state lock holds a token. If a work item ends before its step
//! completes, its token gives the slot back. If a reap ends before it reports its results, its
//! token counts the reap's children as lost. A parent can read `Children::lost` and report each
//! lost child's outcome as unknown.
//!
//! Each parent decides where its children come from, what a failure means, and what it records
//! about each child.
//!
//! TODO(vnext): `upload_objects` and `download_objects` keep their own copy of this
//! bookkeeping. They log a warning with the parent's id when a token drops unconsumed, so moving
//! them here needs the parent's id in `Children`.

use std::collections::HashMap;
use std::future::Future;

use crate::error::Error;
use crate::runtime::sync::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::runtime::sync::sync::Arc;
use crate::transfer::TransferId;

/// One poll hands at most this many finished children to a reap.
pub(crate) const MAX_REAP_PER_POLL: usize = 64;

/// A handle to a child transfer implements this trait. The parent asks the handle whether the child
/// finished and then joins the child to read its result.
pub(crate) trait JoinChild: Send + 'static {
    type Output: Send;

    fn id(&self) -> TransferId;

    /// Return true once the child reached a terminal status. Joining it then returns at once.
    fn is_finished(&self) -> bool;

    fn join(self) -> impl Future<Output = Result<Self::Output, Error>> + Send;
}

#[derive(Debug)]
struct Counts {
    reserved: AtomicUsize,
    reaping: AtomicUsize,
    lost: AtomicU64,
}

impl Counts {
    fn new() -> Self {
        Self {
            reserved: AtomicUsize::new(0),
            reaping: AtomicUsize::new(0),
            lost: AtomicU64::new(0),
        }
    }
}

/// `Children` holds the live children and the record the parent keeps for each one.
pub(crate) struct Children<H, M> {
    live: HashMap<TransferId, (H, M)>,
    max: usize,
    counts: Arc<Counts>,
}

impl<H: JoinChild, M: Send + 'static> Children<H, M> {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            live: HashMap::new(),
            max: max.max(1),
            counts: Arc::new(Counts::new()),
        }
    }

    /// Reserve a slot for one child, or return `None` at capacity. A finished child that is waiting
    /// for its reap does not count, because it holds no network or disk concurrency.
    pub(crate) fn try_reserve(&self) -> Option<Reservation> {
        let running = self.live.values().filter(|(h, _)| !h.is_finished()).count();
        let reserved = self.counts.reserved.load(Ordering::Acquire);
        if running + reserved >= self.max {
            return None;
        }
        self.counts.reserved.fetch_add(1, Ordering::AcqRel);
        Some(Reservation {
            counts: self.counts.clone(),
            released: false,
        })
    }

    /// Turn a reserved slot into a live child.
    pub(crate) fn insert(&mut self, mut slot: Reservation, handle: H, record: M) {
        slot.release();
        self.live.insert(handle.id(), (handle, record));
    }

    /// Move up to `MAX_REAP_PER_POLL` finished children out for a reap.
    pub(crate) fn drain_terminal(&mut self) -> Option<Reaping<H, M>> {
        let ids: Vec<TransferId> = self
            .live
            .iter()
            .filter(|(_, (h, _))| h.is_finished())
            .map(|(id, _)| *id)
            .take(MAX_REAP_PER_POLL)
            .collect();
        if ids.is_empty() {
            return None;
        }
        let children: Vec<(H, M)> = ids
            .into_iter()
            .filter_map(|id| self.live.remove(&id))
            .collect();
        let count = children.len();
        self.counts.reaping.fetch_add(count, Ordering::AcqRel);
        Some(Reaping {
            children: Some(children),
            counts: self.counts.clone(),
            count,
            released: false,
        })
    }

    /// Return true when no child is live, reserved, or out for a reap.
    pub(crate) fn is_idle(&self) -> bool {
        self.live.is_empty()
            && self.counts.reserved.load(Ordering::Acquire) == 0
            && self.counts.reaping.load(Ordering::Acquire) == 0
    }

    pub(crate) fn live(&self) -> impl Iterator<Item = &H> {
        self.live.values().map(|(h, _)| h)
    }

    pub(crate) fn live_len(&self) -> usize {
        self.live.len()
    }

    pub(crate) fn reaping_len(&self) -> usize {
        self.counts.reaping.load(Ordering::Acquire)
    }

    /// Return how many children a dropped reap took with it. Their outcomes are unknown.
    pub(crate) fn lost(&self) -> u64 {
        self.counts.lost.load(Ordering::Acquire)
    }

    /// Hand back every live child for `on_terminal`. Dropping a handle cancels its child.
    pub(crate) fn abandon(&mut self) -> Vec<(H, M)> {
        self.live.drain().map(|(_, child)| child).collect()
    }
}

/// `Reservation` holds one reserved slot. `Children::insert` releases it. Dropping it unreleased
/// gives the slot back.
pub(crate) struct Reservation {
    counts: Arc<Counts>,
    released: bool,
}

impl Reservation {
    fn release(&mut self) {
        if !self.released {
            self.counts.reserved.fetch_sub(1, Ordering::AcqRel);
            self.released = true;
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.release();
    }
}

/// `Reaping` holds finished children while a work item joins them. It keeps their count until
/// `release`, so the parent stays busy while it applies the results. Dropping it unreleased gives
/// the count back and counts its children as lost.
pub(crate) struct Reaping<H, M> {
    children: Option<Vec<(H, M)>>,
    counts: Arc<Counts>,
    count: usize,
    released: bool,
}

impl<H, M> std::fmt::Debug for Reaping<H, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reaping")
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

impl<H: JoinChild, M> Reaping<H, M> {
    /// Join every child concurrently. The count stays held until `release`.
    pub(crate) async fn join(&mut self) -> Vec<(M, Result<H::Output, Error>)> {
        let children = self.children.take().expect("a reap joins once");
        let joins = children
            .into_iter()
            .map(|(handle, record)| async move { (record, handle.join().await) });
        futures_util::future::join_all(joins).await
    }

    pub(crate) fn len(&self) -> usize {
        self.count
    }

    /// Give the count back. Call this under the parent's state lock after applying the results, so
    /// `is_idle` never reports idle while results are half applied.
    pub(crate) fn release(mut self) {
        self.counts.reaping.fetch_sub(self.count, Ordering::AcqRel);
        self.released = true;
    }
}

impl<H, M> Drop for Reaping<H, M> {
    // Count the loss before giving the count back. A parent that sees the reap gone then also sees
    // the loss.
    fn drop(&mut self) {
        if !self.released {
            self.counts
                .lost
                .fetch_add(self.count as u64, Ordering::AcqRel);
            self.counts.reaping.fetch_sub(self.count, Ordering::AcqRel);
        }
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct Fake {
        id: u64,
        done: std::sync::Arc<AtomicBool>,
    }

    impl JoinChild for Fake {
        type Output = u64;

        fn id(&self) -> TransferId {
            TransferId {
                id: self.id,
                parent: None,
            }
        }

        fn is_finished(&self) -> bool {
            self.done.load(Ordering::SeqCst)
        }

        async fn join(self) -> Result<u64, Error> {
            Ok(self.id)
        }
    }

    fn fake(id: u64, done: bool) -> Fake {
        Fake {
            id,
            done: std::sync::Arc::new(AtomicBool::new(done)),
        }
    }

    fn with_children(max: usize, done: &[bool]) -> Children<Fake, u64> {
        let mut children = Children::new(max);
        for (id, done) in done.iter().enumerate() {
            let slot = children.try_reserve().expect("a slot");
            children.insert(slot, fake(id as u64, *done), id as u64);
        }
        children
    }

    #[test]
    fn a_finished_child_frees_its_slot_before_the_reap() {
        let mut children: Children<Fake, ()> = Children::new(1);
        let child = fake(1, false);
        let done = child.done.clone();
        let slot = children.try_reserve().expect("a slot");
        children.insert(slot, child, ());

        assert!(
            children.try_reserve().is_none(),
            "a running child left room"
        );
        done.store(true, Ordering::SeqCst);
        assert!(
            children.try_reserve().is_some(),
            "a finished child kept its slot"
        );
    }

    #[test]
    fn a_reserved_slot_counts_against_the_cap() {
        let children: Children<Fake, ()> = Children::new(1);
        let _slot = children.try_reserve().expect("a slot");
        assert!(children.try_reserve().is_none());
        assert!(!children.is_idle());
    }

    #[test]
    fn a_dropped_reservation_gives_its_slot_back() {
        let children: Children<Fake, ()> = Children::new(1);
        drop(children.try_reserve().expect("a slot"));
        assert!(children.is_idle());
        assert!(children.try_reserve().is_some());
    }

    #[test]
    fn a_reap_takes_at_most_the_cap() {
        let mut children = with_children(200, &[true; 100]);
        let reap = children.drain_terminal().expect("a reap");
        assert_eq!(reap.len(), MAX_REAP_PER_POLL);
        assert_eq!(children.live_len(), 100 - MAX_REAP_PER_POLL);
    }

    #[test]
    fn a_reap_holds_the_parent_busy_until_released() {
        let mut children = with_children(4, &[true, true]);
        let reap = children.drain_terminal().expect("a reap");
        assert!(
            !children.is_idle(),
            "a reap out for joining left the parent idle"
        );
        reap.release();
        assert!(children.is_idle());
        assert_eq!(children.lost(), 0);
    }

    #[tokio::test]
    async fn a_reap_returns_each_record_with_its_result() {
        let mut children = with_children(4, &[true, false, true]);
        let mut reap = children.drain_terminal().expect("a reap");
        let mut results: Vec<(u64, u64)> = reap
            .join()
            .await
            .into_iter()
            .map(|(record, result)| (record, result.expect("a fake joins")))
            .collect();
        results.sort();
        assert_eq!(results, [(0, 0), (2, 2)]);
        reap.release();
        assert_eq!(children.live_len(), 1);
    }

    #[test]
    fn a_dropped_reap_counts_its_children_as_lost() {
        let mut children = with_children(4, &[true, true, true]);
        let reap = children.drain_terminal().expect("a reap");
        drop(reap);
        assert!(children.is_idle(), "a dropped reap kept its count");
        assert_eq!(children.lost(), 3);
    }

    #[test]
    fn abandoning_hands_back_every_live_child() {
        let mut children = with_children(4, &[false, true]);
        let abandoned = children.abandon();
        assert_eq!(abandoned.len(), 2);
        assert!(children.is_idle());
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use super::*;
    use loom::thread;

    struct Done(u64);

    impl JoinChild for Done {
        type Output = ();

        fn id(&self) -> TransferId {
            TransferId {
                id: self.0,
                parent: None,
            }
        }

        fn is_finished(&self) -> bool {
            true
        }

        async fn join(self) -> Result<(), Error> {
            Ok(())
        }
    }

    fn two_finished() -> Children<Done, ()> {
        let mut children = Children::new(4);
        for id in 0..2 {
            let slot = children.try_reserve().expect("a slot");
            children.insert(slot, Done(id), ());
        }
        children
    }

    /// A reap dropped on another thread gives its count back once and counts each child as lost
    /// once. A parent that sees idle during the drop also sees the loss.
    #[test]
    fn a_dropped_reap_and_an_idle_check_agree() {
        loom::model(|| {
            let mut children = two_finished();
            let reap = children.drain_terminal().expect("a reap");
            let dropper = thread::spawn(move || drop(reap));
            let idle_during = children.is_idle();
            let lost_during = children.lost();
            dropper.join().expect("the drop finishes");
            if idle_during {
                assert_eq!(lost_during, 2, "the parent saw idle before it saw the loss");
            }
            assert!(children.is_idle());
            assert_eq!(children.lost(), 2);
        });
    }

    #[test]
    fn a_dropped_reservation_and_a_reserve_agree() {
        loom::model(|| {
            let children: Children<Done, ()> = Children::new(1);
            let slot = children.try_reserve().expect("a slot");
            let dropper = thread::spawn(move || drop(slot));
            let _ = children.try_reserve();
            dropper.join().expect("the drop finishes");
            assert!(children.try_reserve().is_some());
            assert!(children.is_idle());
        });
    }
}
