/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! This module holds the state of one sync run.
//!
//! `SyncTransfer` locks `State` to choose work and to record finished work. Each part of `State`
//! changes its own fields through its methods. `transfer.rs` decides what to do next and calls them.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::io::key::stream::{KeyStream, StreamError};
use crate::operation::sync::compare::Decision;
use crate::operation::sync::walk::{Pairing, Progress, Walk};

use super::child::SyncChild;
use super::delete::Namespace;
use crate::transfer::composite::{Children, Reaping, Reservation};

// How many failures a run keeps. The walk reports failures per entry, so keeping every failure
// would grow memory with the tree. The run keeps the first failures and counts the rest.
//
// The sample comes from the first failing subtree. It does not represent the whole run.
pub(super) const FAILURES_KEPT: usize = 64;

// A transfer decision with its pairing. The key names the destination; the source entry supplies
// bytes.
pub(super) type Qualified<S, D> = (Pairing<S, D>, Decision);

// What the comparison decided by kind. Every pairing produces one decision.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Decided {
    pub(super) transfers: u64,
    pub(super) deletes: u64,
    pub(super) skipped: Skipped,
    // Keys marked for removal. The count stays independent of delete mode, so callers can compare
    // intended removals with completed and refused removals.
    pub(super) deletable: u64,
    // Which obstruction produced a skip. An obstruction is an entry, not a walk failure.
    pub(super) obstructed: Obstructed,
}

// Every skip by reason. A single count cannot distinguish an unchanged key, a protected
// destination, and an incomplete plan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Skipped {
    pub(super) unchanged: u64,
    pub(super) destination_exists: u64,
    pub(super) deferred: u64,
    pub(super) unread: u64,
    pub(super) obstructed: u64,
    // A delete decision that the mode turned into a skip.
    pub(super) delete_not_allowed: u64,
}

impl Skipped {
    // The match is exhaustive, so a reason added to the comparison has to find a home here. A
    // wildcard arm could put that reason inside whichever count it happened to name.
    pub(super) fn record(&mut self, reason: crate::operation::sync::compare::SkipReason) {
        use crate::operation::sync::compare::SkipReason;
        match reason {
            SkipReason::Unchanged => self.unchanged += 1,
            SkipReason::DestinationExists => self.destination_exists += 1,
            SkipReason::Deferred => self.deferred += 1,
            SkipReason::Unknown => self.unread += 1,
            SkipReason::Obstructed => self.obstructed += 1,
        }
    }

    // A removal the run was not allowed to make. Named apart from `record` because no comparison
    // reports it: the key arrives decided for removal and the mode turns it into a skip.
    pub(super) fn record_delete_not_allowed(&mut self) {
        self.delete_not_allowed += 1;
    }

    pub(super) fn total(&self) -> u64 {
        let Self {
            unchanged,
            destination_exists,
            deferred,
            unread,
            obstructed,
            delete_not_allowed,
        } = self;
        unchanged + destination_exists + deferred + unread + obstructed + delete_not_allowed
    }
}

impl std::ops::AddAssign for Skipped {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            unchanged,
            destination_exists,
            deferred,
            unread,
            obstructed,
            delete_not_allowed,
        } = batch;
        self.unchanged += unchanged;
        self.destination_exists += destination_exists;
        self.deferred += deferred;
        self.unread += unread;
        self.obstructed += obstructed;
        self.delete_not_allowed += delete_not_allowed;
    }
}

// Why a name held no bytes to transfer. An archive can become readable after a restore. Each reason
// needs its own count.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct Obstructed {
    // A socket, device, named pipe, or excluded symlink.
    pub(super) nothing_to_read: u64,
    // Bytes in an archive, with no restored copy to read.
    pub(super) archived: u64,
    // A restore under way, so a later run finds the bytes there.
    pub(super) restoring: u64,
}

impl Obstructed {
    // The match is exhaustive, so a kind added later needs a deliberate destination. A wildcard arm
    // could put that kind inside whichever count it happened to name.
    pub(super) fn record(&mut self, why: crate::io::key::stream::Obstruction) {
        use crate::io::key::stream::Obstruction;
        match why {
            // An unfollowed link holds nothing to read here, like a special file.
            Obstruction::NothingToRead | Obstruction::UnfollowedLink => self.nothing_to_read += 1,
            Obstruction::Archived => self.archived += 1,
            Obstruction::BeingRestored => self.restoring += 1,
        }
    }

    fn any(&self) -> bool {
        self.nothing_to_read > 0 || self.archived > 0 || self.restoring > 0
    }
}

impl std::ops::AddAssign for Obstructed {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            nothing_to_read,
            archived,
            restoring,
        } = batch;
        self.nothing_to_read += nothing_to_read;
        self.archived += archived;
        self.restoring += restoring;
    }
}

impl std::ops::AddAssign for Decided {
    fn add_assign(&mut self, batch: Self) {
        let Self {
            transfers,
            deletes,
            skipped,
            deletable,
            obstructed,
        } = batch;
        self.transfers += transfers;
        self.deletes += deletes;
        self.skipped += skipped;
        self.deletable += deletable;
        self.obstructed += obstructed;
    }
}

// What went wrong: an exact count and a bounded sample. The sample caps memory; the count records
// every failure.
#[derive(Debug)]
pub(super) struct Sampled<T> {
    kept: Vec<T>,
    total: u64,
}

impl<T> Default for Sampled<T> {
    fn default() -> Self {
        Self {
            kept: Vec::new(),
            total: 0,
        }
    }
}

impl<T> Sampled<T> {
    fn record(&mut self, item: T) {
        self.total += 1;
        if self.kept.len() < FAILURES_KEPT {
            self.kept.push(item);
        }
    }

    fn total(&self) -> u64 {
        self.total
    }

    fn any(&self) -> bool {
        self.total > 0
    }

    fn sample(&self) -> &[T] {
        &self.kept
    }
}

// `Merge` keeps the walk, its work-item status, and the pairing count.
// A merge work item takes the walk out of `State`, so `in_flight` records that interval.
pub(super) struct Merge<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, so the merge leaves
    // the state lock while it runs.
    walk: Option<Walk<S, D>>,
    in_flight: bool,
    paired: u64,
}

impl<S: KeyStream, D: KeyStream> Merge<S, D> {
    // Hand the walk to a merge work item. Return `None` while another work item holds the walk or
    // after the walk stops pairing.
    pub(super) fn take_walk_to_advance(&mut self) -> Option<Walk<S, D>> {
        if self.in_flight {
            return None;
        }
        let walk = match self.walk.take() {
            Some(walk) if walk.progress() == Progress::Pairing => walk,
            // Exhausted. Put it back so the completion test can see it.
            Some(walk) => {
                self.walk = Some(walk);
                return None;
            }
            None => return None,
        };
        self.in_flight = true;
        Some(walk)
    }

    // Put the walk back after a merge work item and count its pairings. A work item that stopped
    // early returns the walk with zero pairings.
    pub(super) fn return_walk(&mut self, walk: Walk<S, D>, paired: u64) {
        self.walk = Some(walk);
        self.in_flight = false;
        self.paired += paired;
    }

    // Report the walk's progress. Return `None` while a work item holds the walk.
    pub(super) fn progress(&self) -> Option<Progress> {
        self.walk.as_ref().map(Walk::progress)
    }

    pub(super) fn in_flight(&self) -> bool {
        self.in_flight
    }

    #[cfg(test)]
    pub(super) fn hold_in_flight(&mut self, walk: Walk<S, D>) {
        self.walk = Some(walk);
        self.in_flight = true;
    }

    #[cfg(test)]
    pub(super) fn release(&mut self) {
        self.in_flight = false;
    }
}

// `Transfers` keeps every child transfer from decision through reap.
// A child moves from `waiting` to `running`, then out to a reap. The counters and failures record
// the result.
// This alias names the held transfers, grouped by the name the run is removing.
type Held<S, D> = HashMap<String, Vec<Qualified<S, D>>>;

pub(super) struct Transfers<S: KeyStream, D: KeyStream> {
    waiting: VecDeque<Qualified<S::Source, D::Source>>,
    // This map holds the transfers that wait for a removal on their path. Each entry's key is the
    // name the run is removing.
    held: Held<S::Source, D::Source>,
    running: Children<SyncChild, ()>,
    arrived: u64,
    bytes: u64,
    failures: Sampled<String>,
    outcomes_unknown: u64,
}

impl<S: KeyStream, D: KeyStream> Transfers<S, D> {
    // Queue the transfer decisions from one merge batch.
    pub(super) fn queue(&mut self, batch: &mut VecDeque<Qualified<S::Source, D::Source>>) {
        self.waiting.append(batch);
    }

    // Reserve a slot for one child, or return `None` at capacity. A finished child that waits for
    // its reap holds no network or disk concurrency, so it does not count.
    pub(super) fn try_reserve(&self) -> Option<Reservation> {
        self.running.try_reserve()
    }

    // Take the oldest waiting decision.
    pub(super) fn next_waiting(&mut self) -> Option<Qualified<S::Source, D::Source>> {
        self.waiting.pop_front()
    }

    // Hold a transfer until the deleter answers for `blocker`.
    pub(super) fn hold(&mut self, blocker: String, decision: Qualified<S::Source, D::Source>) {
        self.held.entry(blocker).or_default().push(decision);
    }

    // The deleter removed `blocker`. The transfers held behind it go back to the front of the
    // queue, in the order the merge decided them.
    pub(super) fn release(&mut self, blocker: &str) {
        if let Some(decisions) = self.held.remove(blocker) {
            for decision in decisions.into_iter().rev() {
                self.waiting.push_front(decision);
            }
        }
    }

    // The deleter refused `blocker`. It still stands on each held transfer's path, so each would
    // write through it or fail on it. The run records each one as a failure.
    pub(super) fn fail_held(&mut self, blocker: &str) {
        for (pairing, _) in self.held.remove(blocker).unwrap_or_default() {
            self.failures.record(format!(
                "{}: `{blocker}` stands on its path, and the run could not remove it",
                pairing.key()
            ));
        }
    }

    pub(super) fn is_holding(&self) -> bool {
        !self.held.is_empty()
    }

    // Record a child that started in a reserved slot. A later reap joins it.
    pub(super) fn start(&mut self, slot: Reservation, child: SyncChild) {
        self.running.insert(slot, child, ());
    }

    // Record a key that never became a child, with the reason.
    pub(super) fn record_failure(&mut self, reason: String) {
        self.failures.record(reason);
    }

    // Move up to `MAX_REAP_PER_POLL` finished children out for a reap.
    pub(super) fn take_finished(&mut self) -> Option<Reaping<SyncChild, ()>> {
        self.running.drain_terminal()
    }

    // Record what a reap learned: how many children arrived, the bytes they moved, and why the
    // others failed. This method releases the reap after it records the results, so the run stays
    // busy until then.
    pub(super) fn record_reap(
        &mut self,
        batch: Reaping<SyncChild, ()>,
        arrived: u64,
        bytes: u64,
        reasons: Vec<String>,
    ) {
        self.bytes += bytes;
        self.arrived += arrived;
        // Record every child failure. The first failure controls `Abort`; callers still need every
        // reason.
        for reason in reasons {
            self.failures.record(reason);
        }
        batch.release();
    }

    // Hand back every running child so the run can drop them. Their outcomes stay unknown. This
    // method reads their bytes first, because dropping a handle cancels its child.
    pub(super) fn abandon_running(&mut self) -> Vec<(SyncChild, ())> {
        let live = self.running.live_len();
        if live > 0 {
            self.outcomes_unknown += live as u64;
            let moved: u64 = self.running.live().map(SyncChild::bytes_so_far).sum();
            self.bytes += moved;
        }
        self.running.abandon()
    }

    // Count the children whose outcome the run never learned: the ones `abandon_running` dropped
    // and the ones a dropped reap took with it.
    pub(super) fn outcomes_unknown(&self) -> u64 {
        self.outcomes_unknown + self.running.lost()
    }
}

// `Deletes` keeps keys from a delete decision through the service response.
// A batch moves from `waiting` to `in_flight`; `removed` and `refusals` record the response.
pub(super) struct Deletes {
    waiting: Vec<String>,
    in_flight: usize,
    removed: u64,
    refusals: Sampled<String>,
    namespace: Namespace,
    // On a tree destination, this set holds every key the run queued or sent for removal that has
    // no answer yet. A transfer under such a key waits until that key's answer arrives.
    clearing: HashSet<String>,
}

impl Deletes {
    // Queue the destination-only keys from one merge batch.
    pub(super) fn queue(&mut self, keys: &mut Vec<String>) {
        if self.namespace == Namespace::Tree {
            self.clearing.extend(keys.iter().cloned());
        }
        self.waiting.append(keys);
    }

    // Return the name on this key's path that the run is still removing. On a tree destination,
    // the key can land only after that name is gone.
    pub(super) fn blocker_of(&self, key: &str) -> Option<String> {
        key.bytes()
            .enumerate()
            .filter(|(_, byte)| *byte == b'/')
            .map(|(at, _)| &key[..at])
            .find(|name| self.clearing.contains(*name))
            .map(str::to_string)
    }

    // Mark these keys answered. A transfer under one of them no longer waits.
    pub(super) fn settle(&mut self, keys: &[String]) {
        for key in keys {
            self.clearing.remove(key);
        }
    }

    // Take up to `size` keys for one delete request. A full batch goes out at once. A partial batch
    // waits until `nothing_can_grow` is true, because sending early costs a request per handful of
    // keys. `in_flight` counts the keys until the response arrives.
    pub(super) fn take_batch(
        &mut self,
        size: usize,
        nothing_can_grow: bool,
    ) -> Option<Vec<String>> {
        let full = self.waiting.len() >= size;
        let last_call = nothing_can_grow && !self.waiting.is_empty();
        if !full && !last_call {
            return None;
        }
        let take = self.waiting.len().min(size);
        let keys: Vec<String> = self.waiting.drain(..take).collect();
        self.in_flight += keys.len();
        Some(keys)
    }

    // Record one response: how many keys the request named, how many the destination removed, and
    // why it refused the others.
    pub(super) fn record_response(&mut self, sent: usize, removed: u64, refusals: Vec<String>) {
        self.in_flight -= sent;
        self.removed += removed;
        for refusal in refusals {
            self.refusals.record(refusal);
        }
    }
}

// `State` combines merge, transfer, delete, and run-wide records.
// The scheduler locks `State` while it chooses work or records completed work.
pub(super) struct State<S: KeyStream, D: KeyStream> {
    pub(super) merge: Merge<S, D>,
    decided: Decided,
    pub(super) transfers: Transfers<S, D>,
    pub(super) deletes: Deletes,
    failures: Sampled<StreamError>,
    warnings: Sampled<StreamError>,
    plan_incomplete: bool,
    walk_plan_incomplete: bool,
    stopped_by: Option<crate::error::Error>,
}

impl<S: KeyStream, D: KeyStream> State<S, D> {
    pub(super) fn new(walk: Walk<S, D>, max_children: usize, namespace: Namespace) -> Self {
        State {
            merge: Merge {
                walk: Some(walk),
                in_flight: false,
                paired: 0,
            },
            decided: Decided::default(),
            transfers: Transfers {
                waiting: VecDeque::new(),
                held: HashMap::new(),
                running: Children::new(max_children),
                arrived: 0,
                bytes: 0,
                failures: Sampled::default(),
                outcomes_unknown: 0,
            },
            deletes: Deletes {
                waiting: Vec::new(),
                in_flight: 0,
                removed: 0,
                refusals: Sampled::default(),
                namespace,
                clearing: HashSet::new(),
            },
            failures: Sampled::default(),
            warnings: Sampled::default(),
            plan_incomplete: false,
            walk_plan_incomplete: false,
            stopped_by: None,
        }
    }

    // Stop the run for `why`. The first stop wins, so the run reports the failure that stopped it.
    pub(super) fn stop(&mut self, why: crate::error::Error) {
        if self.stopped_by.is_none() {
            self.stopped_by = Some(why);
        }
    }

    // Return true after a failure stopped the run.
    pub(super) fn is_stopped(&self) -> bool {
        self.stopped_by.is_some()
    }

    // Take the reason that stopped the run, for the terminal status.
    pub(super) fn take_stop(&mut self) -> Option<crate::error::Error> {
        self.stopped_by.take()
    }

    // Record a walk failure. The run keeps a bounded sample and counts every failure.
    pub(super) fn record_walk_failure(&mut self, failure: StreamError) {
        self.failures.record(failure);
    }

    // Record a walk warning. A warning does not stop the run.
    pub(super) fn record_walk_warning(&mut self, warning: StreamError) {
        self.warnings.record(warning);
    }

    // Add the decision counts from one merge batch.
    pub(super) fn add_decided(&mut self, batch: Decided) {
        self.decided += batch;
    }

    pub(super) fn mark_plan_incomplete(&mut self) {
        self.plan_incomplete = true;
    }

    pub(super) fn mark_walk_plan_incomplete(&mut self) {
        self.walk_plan_incomplete = true;
    }

    pub(super) fn discard_waiting_deletes(&mut self) {
        if !self.deletes.waiting.is_empty() {
            self.mark_plan_incomplete();
            let dropped: Vec<String> = self.deletes.waiting.drain(..).collect();
            self.deletes.settle(&dropped);
        }
    }

    pub(super) fn discard_waiting_transfers(&mut self) {
        if !self.transfers.waiting.is_empty() || self.transfers.is_holding() {
            self.mark_plan_incomplete();
            self.transfers.waiting.clear();
            self.transfers.held.clear();
        }
    }

    pub(super) fn abandon_delete_batch(&mut self, key_count: usize) {
        if key_count > 0 {
            self.mark_plan_incomplete();
        }
        self.deletes.in_flight = self.deletes.in_flight.saturating_sub(key_count);
    }

    pub(super) fn work_outstanding(&self) -> bool {
        self.merge.in_flight || self.deletes.in_flight > 0 || !self.transfers.running.is_idle()
    }

    pub(super) fn is_execution_complete(&self) -> bool {
        !self.work_outstanding()
            && self.transfers.waiting.is_empty()
            && !self.transfers.is_holding()
            && self.deletes.waiting.is_empty()
            && self
                .merge
                .walk
                .as_ref()
                .is_some_and(|walk| walk.progress() != Progress::Pairing)
    }

    pub(super) fn is_plan_complete(&self, transfer_active: bool) -> bool {
        !self.plan_incomplete
            && !self.walk_plan_incomplete
            && (transfer_active || !self.work_outstanding())
    }

    pub(super) fn has_failures(&self) -> bool {
        self.failures.any() || self.transfers.failures.any() || self.deletes.refusals.any()
    }

    pub(super) fn has_warnings(&self, transfer_active: bool) -> bool {
        self.warnings.any()
            || self.decided.obstructed.any()
            || self.plan_incomplete
            || self.walk_plan_incomplete
            || self.transfers.outcomes_unknown() > 0
            || (!transfer_active && self.work_outstanding())
    }
}

// A test reads the run's counters and flags from this copy. The copy does not change after
// `snapshot` returns.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(super) struct RunSnapshot {
    pub(super) decided: Decided,
    pub(super) paired: u64,
    pub(super) merge_in_flight: bool,
    pub(super) merge_present: bool,
    pub(super) transfers_waiting: usize,
    pub(super) transfers_held: usize,
    pub(super) children_running: usize,
    pub(super) children_reaping: usize,
    pub(super) arrived: u64,
    pub(super) bytes: u64,
    pub(super) transfer_failures: u64,
    pub(super) outcomes_unknown: u64,
    pub(super) deletes_waiting: usize,
    pub(super) deletes_in_flight: usize,
    pub(super) removed: u64,
    pub(super) refusals: u64,
    pub(super) walk_failures: u64,
    pub(super) walk_failures_kept: usize,
    pub(super) warnings_kept: usize,
    pub(super) plan_incomplete: bool,
    pub(super) walk_plan_incomplete: bool,
    pub(super) stopped: bool,
}

#[cfg(test)]
impl<S: KeyStream, D: KeyStream> State<S, D> {
    // Copy the run's counters and flags for a test.
    pub(super) fn snapshot(&self) -> RunSnapshot {
        RunSnapshot {
            decided: self.decided,
            paired: self.merge.paired,
            merge_in_flight: self.merge.in_flight,
            merge_present: self.merge.walk.is_some(),
            transfers_waiting: self.transfers.waiting.len(),
            transfers_held: self.transfers.held.values().map(Vec::len).sum(),
            children_running: self.transfers.running.live_len(),
            children_reaping: self.transfers.running.reaping_len(),
            arrived: self.transfers.arrived,
            bytes: self.transfers.bytes,
            transfer_failures: self.transfers.failures.total(),
            outcomes_unknown: self.transfers.outcomes_unknown(),
            deletes_waiting: self.deletes.waiting.len(),
            deletes_in_flight: self.deletes.in_flight,
            removed: self.deletes.removed,
            refusals: self.deletes.refusals.total(),
            walk_failures: self.failures.total(),
            walk_failures_kept: self.failures.sample().len(),
            warnings_kept: self.warnings.sample().len(),
            plan_incomplete: self.plan_incomplete,
            walk_plan_incomplete: self.walk_plan_incomplete,
            stopped: self.stopped_by.is_some(),
        }
    }

    pub(super) fn walk_failure_sample(&self) -> &[StreamError] {
        self.failures.sample()
    }

    pub(super) fn transfer_failure_sample(&self) -> &[String] {
        self.transfers.failures.sample()
    }

    pub(super) fn delete_refusals(&self) -> &[String] {
        self.deletes.refusals.sample()
    }

    pub(super) fn waiting_transfers(
        &self,
    ) -> impl Iterator<Item = &Qualified<S::Source, D::Source>> {
        self.transfers.waiting.iter()
    }

    pub(super) fn set_plan_flags(&mut self, plan_incomplete: bool, walk_plan_incomplete: bool) {
        self.plan_incomplete = plan_incomplete;
        self.walk_plan_incomplete = walk_plan_incomplete;
    }
}
