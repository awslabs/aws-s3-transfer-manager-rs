/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::io::key::stream::{KeyStream, StreamError};
use crate::operation::sync::compare::{Compare, Decision, Verdict};
use crate::operation::sync::walk::{Pairing, Progress, Walk};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::FailedTransferPolicy;

use child::{ChildHandle, SpawnChild};
use delete::Deleter;

// Pairings per merge work item. A merge draws from either side, so a listing page cannot size the
// batch. The bound limits how long one work item holds an executor slot.
const MERGE_BATCH: usize = 64;

// How many failures a run keeps. The walk reports failures per entry, so keeping every failure
// would grow memory with the tree. The run keeps the first failures and counts the rest.
//
// The sample comes from the first failing subtree. It does not represent the whole run.
const FAILURES_KEPT: usize = 64;

// Where a key lands, or the key a destination refuses.
//
// A key ending in the delimiter names a directory. Path derivation removes the trailing delimiter,
// so `photos/2019/` becomes a file named `2019`. Both local sync paths refuse that key.
//
// Directory download keeps its existing behavior. Sync owns the rejection for sync runs.
fn local_path_for_key(
    root: &std::path::Path,
    key: &str,
) -> Result<std::path::PathBuf, crate::error::Error> {
    if key.ends_with('/') {
        return Err(crate::error::Error::new(
            crate::error::ErrorKind::InputInvalid,
            format!("the key '{key}' names a place rather than a file"),
        ));
    }
    crate::io::key::local_key_path(root, key, None, None)
}

// Whether a run may remove destination keys. The named type shows a caller what `true` would
// enable.
//
// Delete mode is off by default. A delete can remove data the caller never sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DeleteMode {
    On,
    #[default]
    Off,
}

// Settings chosen by the caller. The comparison, child factory, and deleter choose the direction.
#[derive(Debug, Clone)]
pub(crate) struct RunSettings {
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    pub(crate) max_children: usize,
    pub(crate) delete_mode: DeleteMode,
    // Sync continues after a failure unless the caller selects `Abort`.
    pub(crate) failure_policy: FailedTransferPolicy,
}

impl Default for RunSettings {
    fn default() -> Self {
        Self {
            max_children: crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN,
            delete_mode: DeleteMode::default(),
            failure_policy: FailedTransferPolicy::Continue,
        }
    }
}

// How a run turned out. A run can finish without failures and still leave keys unaccounted for.
// `Warned` reports that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunOutcome {
    Failed,
    Warned,
    Clean,
}

// The merge leaves `State` while a work item advances it. `Walk::next` needs `&mut`, and the state
// lock cannot stay held across that wait.
pub(crate) enum SyncWork<S: KeyStream, D: KeyStream> {
    AdvanceMerge { walk: Option<Box<Walk<S, D>>> },
    // Terminal children waiting for a reap. `reap_in_flight` counts them after they leave
    // `children`.
    ReapChildren { children: Vec<ChildHandle> },
    // Keys waiting for deletion. `deletes_in_flight` counts them after a delete work item takes
    // them.
    DeleteKeys { keys: Vec<String> },
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncWork::AdvanceMerge { .. } => f.write_str("AdvanceMerge"),
            SyncWork::ReapChildren { children } => {
                write!(f, "ReapChildren({})", children.len())
            }
            SyncWork::DeleteKeys { keys } => write!(f, "DeleteKeys({})", keys.len()),
        }
    }
}

// A transfer decision with its pairing. The key names the destination; the source entry supplies
// bytes.
type Qualified<S, D> = (Pairing<S, D>, Decision);

// What the comparison decided by kind. Every pairing produces one decision.
#[derive(Debug, Default)]
struct Decided {
    transfers: u64,
    deletes: u64,
    skipped: Skipped,
    // Keys marked for removal. The count stays independent of delete mode, so callers can compare
    // intended removals with completed and refused removals.
    deletable: u64,
    // Which obstruction produced a skip. An obstruction is an entry, not a walk failure.
    obstructed: Obstructed,
}

// Every skip by reason. A single count cannot distinguish an unchanged key, a protected
// destination, and an incomplete plan.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Skipped {
    unchanged: u64,
    destination_exists: u64,
    deferred: u64,
    unread: u64,
    obstructed: u64,
    // A delete decision that the mode turned into a skip.
    delete_not_allowed: u64,
}

impl Skipped {
    // The match is exhaustive, so a reason added to the comparison has to find a home here. A
    // wildcard arm could put that reason inside whichever count it happened to name.
    fn record(&mut self, reason: crate::operation::sync::compare::SkipReason) {
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
    fn record_delete_not_allowed(&mut self) {
        self.delete_not_allowed += 1;
    }

    fn total(&self) -> u64 {
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
struct Obstructed {
    // A socket, device, named pipe, or excluded symlink.
    nothing_to_read: u64,
    // Bytes in an archive, with no restored copy to read.
    archived: u64,
    // A restore under way, so a later run finds the bytes there.
    restoring: u64,
}

impl Obstructed {
    // The match is exhaustive, so a kind added later needs a deliberate destination. A wildcard arm
    // could put that kind inside whichever count it happened to name.
    fn record(&mut self, why: crate::io::key::stream::Obstruction) {
        use crate::io::key::stream::Obstruction;
        match why {
            Obstruction::NothingToRead => self.nothing_to_read += 1,
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
struct Reported<T> {
    kept: Vec<T>,
    total: u64,
}

impl<T> Default for Reported<T> {
    fn default() -> Self {
        Self {
            kept: Vec::new(),
            total: 0,
        }
    }
}

impl<T> Reported<T> {
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
struct Merge<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, so the merge leaves
    // the state lock while it runs.
    walk: Option<Walk<S, D>>,
    in_flight: bool,
    paired: u64,
}

// `Transfers` keeps every child transfer from decision through reap.
// A child moves from `waiting` to `running` to `reaping`; the counters and failures record the result.
struct Transfers<S: KeyStream, D: KeyStream> {
    waiting: VecDeque<Qualified<S::Source, D::Source>>,
    running: std::collections::HashMap<crate::transfer::TransferId, ChildHandle>,
    reaping: usize,
    arrived: u64,
    bytes: u64,
    failures: Reported<String>,
    outcomes_unknown: u64,
}

// `Deletes` keeps keys from a delete decision through the service response.
// A batch moves from `waiting` to `in_flight`; `removed` and `refusals` record the response.
struct Deletes {
    waiting: Vec<String>,
    in_flight: usize,
    removed: u64,
    refusals: Reported<String>,
}

// `State` combines merge, transfer, delete, and run-wide records.
// The scheduler locks `State` while it chooses work or records completed work.
struct State<S: KeyStream, D: KeyStream> {
    merge: Merge<S, D>,
    decided: Decided,
    transfers: Transfers<S, D>,
    deletes: Deletes,
    failures: Reported<StreamError>,
    warnings: Reported<StreamError>,
    plan_incomplete: bool,
    walk_plan_incomplete: bool,
    stopped_by: Option<crate::error::Error>,
}

impl<S: KeyStream, D: KeyStream> State<S, D> {
    fn mark_plan_incomplete(&mut self) {
        self.plan_incomplete = true;
    }

    fn mark_walk_plan_incomplete(&mut self) {
        self.walk_plan_incomplete = true;
    }

    fn discard_waiting_deletes(&mut self) {
        if !self.deletes.waiting.is_empty() {
            self.mark_plan_incomplete();
            self.deletes.waiting.clear();
        }
    }

    fn discard_waiting_transfers(&mut self) {
        if !self.transfers.waiting.is_empty() {
            self.mark_plan_incomplete();
            self.transfers.waiting.clear();
        }
    }

    fn abandon_delete_batch(&mut self, key_count: usize) {
        if key_count > 0 {
            self.mark_plan_incomplete();
        }
        self.deletes.in_flight = self.deletes.in_flight.saturating_sub(key_count);
    }

    fn work_outstanding(&self) -> bool {
        self.merge.in_flight
            || self.transfers.reaping > 0
            || self.deletes.in_flight > 0
            || !self.transfers.running.is_empty()
    }

    fn is_execution_complete(&self) -> bool {
        !self.work_outstanding()
            && self.transfers.waiting.is_empty()
            && self.deletes.waiting.is_empty()
            && self
                .merge
                .walk
                .as_ref()
                .is_some_and(|walk| walk.progress() != Progress::Pairing)
    }

    fn is_plan_complete(&self, transfer_active: bool) -> bool {
        !self.plan_incomplete
            && !self.walk_plan_incomplete
            && (transfer_active || !self.work_outstanding())
    }

    fn has_failures(&self) -> bool {
        self.failures.any() || self.transfers.failures.any() || self.deletes.refusals.any()
    }

    fn has_warnings(&self, transfer_active: bool) -> bool {
        self.warnings.any()
            || self.decided.obstructed.any()
            || self.plan_incomplete
            || self.walk_plan_incomplete
            || self.transfers.outcomes_unknown > 0
            || (!transfer_active && self.work_outstanding())
    }
}

pub(crate) struct SyncTransfer<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    inner: Arc<Inner<S, D>>,
}

impl<S: KeyStream, D: KeyStream> Clone for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTransfer").finish_non_exhaustive()
    }
}

struct Inner<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    ctx: TransferContext,
    state: Mutex<State<S, D>>,
    // Compare each pair according to the sync direction. Upload and download choose opposite newer sides.
    comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
    // The other one. Unlike the comparison this holds state, because building a child needs the
    // client and the bucket.
    spawner: Arc<dyn SpawnChild<S::Source>>,
    // How keys leave the destination. A third direction-specific thing, and the one that differs
    // most between a bucket and a local tree.
    deleter: Deleter,
    // What to do when something fails. Read at every site a failure can arrive, so one answer
    // covers the run.
    failure_policy: FailedTransferPolicy,
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    max_children: usize,
    // Allow sync to remove destination-only keys. Delete mode requires caller opt-in.
    delete_mode: DeleteMode,
}

impl<S, D> SyncTransfer<S, D>
where
    S: KeyStream + Send + 'static,
    D: KeyStream + Send + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    pub(crate) fn new(
        ctx: TransferContext,
        walk: Walk<S, D>,
        comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
        spawner: Arc<dyn SpawnChild<S::Source>>,
        deleter: Deleter,
        settings: RunSettings,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                ctx,
                comparison,
                spawner,
                deleter,
                max_children: settings.max_children.max(1),
                delete_mode: settings.delete_mode,
                failure_policy: settings.failure_policy,
                state: Mutex::new(State {
                    merge: Merge {
                        walk: Some(walk),
                        in_flight: false,
                        paired: 0,
                    },
                    decided: Decided::default(),
                    transfers: Transfers {
                        waiting: VecDeque::new(),
                        running: std::collections::HashMap::new(),
                        reaping: 0,
                        arrived: 0,
                        bytes: 0,
                        failures: Reported::default(),
                        outcomes_unknown: 0,
                    },
                    deletes: Deletes {
                        waiting: Vec::new(),
                        in_flight: 0,
                        removed: 0,
                        refusals: Reported::default(),
                    },
                    failures: Reported::default(),
                    warnings: Reported::default(),
                    plan_incomplete: false,
                    walk_plan_incomplete: false,
                    stopped_by: None,
                }),
            }),
        }
    }

    // Return true when the comparison reached every key and the finished run has no work
    // outstanding.
    //
    // The run stores comparison holes and walk holes in state. A merge work item can hold the walk,
    // so plan completeness cannot read the walk directly.
    pub(crate) fn is_plan_complete(&self) -> bool {
        let state = self.inner.state.lock();
        state.is_plan_complete(self.inner.ctx.is_active())
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let mut state = self.inner.state.lock();

        // Check for stop between phases. A failed spawn can stop the run before the delete phase
        // starts.
        if !self.has_stopped(&state) {
            if let Some(work) = self.dispatch_merge(&mut state) {
                return work;
            }
            if self.spawn_one(&mut state) {
                return PollWork::Spawned;
            }
        }

        if !self.has_stopped(&state) {
            if let Some(work) = self.dispatch_deletes(&mut state) {
                return work;
            }
        }

        // Reap terminal children after cancellation. The run still needs each child outcome.
        if let Some(work) = self.dispatch_reap(&mut state) {
            return work;
        }

        if let Some(done) = self.check_terminal(&mut state) {
            return done;
        }

        // Mark the transfer pending before returning. A work item signal wakes only a pending
        // transfer.
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    // Return true after cancellation or an aborting failure. The transfer stays active until
    // outstanding work returns.
    fn has_stopped(&self, state: &State<S, D>) -> bool {
        !self.inner.ctx.is_active() || state.stopped_by.is_some()
    }

    // Record the first aborting failure. The terminal path sets the transfer result after
    // outstanding work returns.
    fn stop_if_aborting(&self, state: &mut State<S, D>, why: impl Into<crate::error::Error>) {
        if self.inner.failure_policy == FailedTransferPolicy::Abort && state.stopped_by.is_none() {
            state.stopped_by = Some(why.into());
        }
    }

    // Return the run outcome. Failures outrank warnings; warnings outrank a clean result.
    pub(crate) fn outcome(&self) -> RunOutcome {
        let state = self.inner.state.lock();
        if state.has_failures() {
            return RunOutcome::Failed;
        }
        if state.has_warnings(self.inner.ctx.is_active()) {
            return RunOutcome::Warned;
        }
        RunOutcome::Clean
    }

    // Report `Done` only after merge work returns. A work item can still hold keys after the merge
    // leaves state.
    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if self.has_stopped(state) {
            // Everything dispatched is still owed an answer, however the run ended.
            if state.work_outstanding() {
                return None;
            }
            // Set the terminal status after outstanding work returns. Every earlier phase reads
            // `stopped_by` and starts no new work.
            if let Some(why) = state.stopped_by.take() {
                self.inner.ctx.set_failed(why);
            }
            // Drop pending deletes and transfers after a stop. Their absence decisions came from an
            // incomplete source stream.
            //
            // The next run compares those keys from a complete view. The run marks the plan
            // incomplete.
            state.discard_waiting_deletes();
            state.discard_waiting_transfers();
            // The run recorded the terminal outcome. Signal the caller.
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        if state.is_execution_complete() {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
    }

    // Start one waiting transfer when a child slot is free. The spawned child uses its own
    // scheduler slot.
    fn spawn_one(&self, state: &mut State<S, D>) -> bool {
        // Count only live children. A child in a reap holds no network or disk concurrency.
        if state.transfers.running.len() >= self.inner.max_children {
            return false;
        }

        // Skip a decision that cannot create a child. Keep reading waiting decisions until one
        // starts or the queue empties.
        loop {
            // A stopped run starts no new child. Check before removing the key from the waiting
            // queue.
            if self.has_stopped(state) {
                return false;
            }
            let Some((pairing, _)) = state.transfers.waiting.pop_front() else {
                break;
            };
            let Some(entry) = pairing.source().entry() else {
                // A transfer needs a source entry. Record a comparison error when that entry is
                // absent.
                let why = crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "a transfer was decided for a key with no source entry",
                );
                state
                    .transfers
                    .failures
                    .record(format!("{}: {why}", pairing.key()));
                self.stop_if_aborting(state, why);
                continue;
            };
            match self
                .inner
                .spawner
                .spawn(pairing.key(), &entry.source, self.inner.ctx.id.id)
            {
                Ok(child) => {
                    state.transfers.running.insert(child.id(), child);
                    return true;
                }
                Err(err) => {
                    state
                        .transfers
                        .failures
                        .record(format!("{}: {err}", pairing.key()));
                    self.stop_if_aborting(state, err);
                    continue;
                }
            }
        }
        false
    }

    // Send a delete batch when it fills or the merge cannot add another key.
    fn dispatch_deletes(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let size = self.inner.deleter.batch_size();
        let merge_done = match state.merge.walk.as_ref().map(Walk::progress) {
            // Both sides paired every key either of them holds, so nothing can grow a part batch.
            Some(Progress::Accounted) => true,
            // More pairings will come, and any of them could add to the batch. A merge away with a
            // work item says nothing either way.
            Some(Progress::Pairing) | None => false,
            // A failed merge leaves deletes from an incomplete source stream. Drop the batch and mark the
            // plan incomplete.
            Some(Progress::Stopped) => {
                state.discard_waiting_deletes();
                return None;
            }
        };
        let full = state.deletes.waiting.len() >= size;
        // A part batch waits until nothing can grow it. Sending early would cost a request per
        // handful of keys for no gain.
        let last_call = merge_done && !state.merge.in_flight && !state.deletes.waiting.is_empty();
        if !full && !last_call {
            return None;
        }
        let take = state.deletes.waiting.len().min(size);
        let keys: Vec<String> = state.deletes.waiting.drain(..take).collect();
        state.deletes.in_flight += keys.len();
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::DeleteKeys { keys })),
        }))
    }

    // Collect terminal children for a reap. `reaping` counts them after they leave `running`.
    fn dispatch_reap(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let finished: Vec<crate::transfer::TransferId> = state
            .transfers
            .running
            .iter()
            .filter(|(_, child)| child.is_finished())
            .map(|(id, _)| *id)
            .collect();
        if finished.is_empty() {
            return None;
        }
        let children: Vec<ChildHandle> = finished
            .into_iter()
            .map(|id| {
                state
                    .transfers
                    .running
                    .remove(&id)
                    .expect("id came from this map")
            })
            .collect();
        state.transfers.reaping += children.len();
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::ReapChildren { children })),
        }))
    }

    // Hand the merge to a work item. When no merge work remains, the poll chooses `Done` or
    // `Pending`.
    fn dispatch_merge(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if state.merge.in_flight {
            return None;
        }

        let walk = match state.merge.walk.take() {
            Some(walk) if walk.progress() == Progress::Pairing => walk,
            // Exhausted. Put it back so the completion test can see it.
            Some(walk) => {
                state.merge.walk = Some(walk);
                return None;
            }
            None => return None,
        };

        state.merge.in_flight = true;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::AdvanceMerge {
                walk: Some(Box::new(walk)),
            })),
        }))
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let work: &mut SyncWork<S, D> = work.data_mut();
        match work {
            SyncWork::AdvanceMerge { walk } => {
                let walk = walk.take().expect("the merge was already taken");
                self.execute_advance_merge(*walk).await
            }
            SyncWork::ReapChildren { children } => {
                let children = std::mem::take(children);
                self.execute_reap(children).await
            }
            SyncWork::DeleteKeys { keys } => {
                let keys = std::mem::take(keys);
                self.execute_deletes(keys).await
            }
        }
    }

    async fn execute_deletes(&self, keys: Vec<String>) -> WorkOutcome {
        // Check for stop again before sending a delete batch. A stopped run made absence decisions
        // from an incomplete source stream.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                // This work item owns the batch. Mark the plan incomplete before dropping it.
                state.abandon_delete_batch(keys.len());
                if self.check_terminal(&mut state).is_some() {
                    return WorkOutcome::Cancelled;
                }
                drop(state);
                self.inner.ctx.try_wake();
                return WorkOutcome::Cancelled;
            }
        }

        let sent = keys.len();
        // Pass the stop check to the destination retry loop. The loop checks before each retry.
        let stopped = || {
            let state = self.inner.state.lock();
            self.has_stopped(&state)
        };
        let outcomes = self.inner.deleter.delete(keys, &stopped).await;

        let mut gone = 0u64;
        let mut refused = Vec::new();
        for outcome in &outcomes {
            match outcome {
                Ok(_) => gone += 1,
                Err(why) => refused.push(why.clone()),
            }
        }

        // S3 reports a per-key refusal inside a successful response. The run records the key and
        // reason.
        let mut state = self.inner.state.lock();
        if let Some(first) = refused.first() {
            let why = crate::error::Error::new(first.kind.clone(), first.why.clone());
            self.stop_if_aborting(&mut state, why);
        }
        state.deletes.in_flight -= sent;
        state.deletes.removed += gone;

        for refusal in refused {
            state.deletes.refusals.record(refusal.why);
        }
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_reap(&self, children: Vec<ChildHandle>) -> WorkOutcome {
        let count = children.len();
        let mut moved = 0u64;
        let mut arrived = 0u64;
        let mut why = None;
        let mut reasons = Vec::new();
        for child in children {
            match child.join().await {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(err) => {
                    reasons.push(err.to_string());
                    if why.is_none() {
                        why = Some(err);
                    }
                }
            }
        }
        let mut state = self.inner.state.lock();
        if let Some(why) = why {
            self.stop_if_aborting(&mut state, why);
        }
        state.transfers.reaping -= count;
        state.transfers.bytes += moved;
        state.transfers.arrived += arrived;
        // Record every child failure. The first failure controls `Abort`; callers still need every
        // reason.
        for reason in reasons {
            state.transfers.failures.record(reason);
        }
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_advance_merge(&self, mut walk: Walk<S, D>) -> WorkOutcome {
        // Release the state lock before the merge waits.
        {
            let mut state = self.inner.state.lock();
            if self.has_stopped(&state) {
                state.merge.walk = Some(walk);
                state.merge.in_flight = false;
                // Return the merge before checking for completion. The returned merge can finish the run.
                if self.check_terminal(&mut state).is_some() {
                    return WorkOutcome::Cancelled;
                }
                drop(state);
                self.inner.ctx.try_wake();
                return WorkOutcome::Cancelled;
            }
        }

        let mut paired = 0u64;
        let mut decided = Decided::default();
        let mut deferred = false;
        let mut failures = Vec::new();
        let mut batch = VecDeque::new();
        let mut pending_deletes = Vec::new();

        for _ in 0..MERGE_BATCH {
            match walk.next().await {
                Some(Ok(pairing)) => {
                    paired += 1;
                    // TODO(sync): Map this comparison `Decision` into the per-entry
                    // transfer event record before dispatching its work.
                    match self.inner.comparison.compare(&pairing) {
                        Verdict::Decided(decision @ Decision::Transfer(_)) => {
                            decided.transfers += 1;
                            batch.push_back((pairing, decision));
                        }
                        // A delete decision means the destination has a key the source lacks. Delete mode
                        // turns an unrequested delete into a skip.
                        Verdict::Decided(Decision::Delete(_)) => {
                            decided.deletable += 1;
                            match self.inner.delete_mode {
                                DeleteMode::On => {
                                    decided.deletes += 1;
                                    pending_deletes.push(pairing.key().to_string());
                                }
                                DeleteMode::Off => decided.skipped.record_delete_not_allowed(),
                            }
                        }
                        // A skip needs no scheduler work. Record the skip reason.
                        Verdict::Decided(Decision::Skip(skip)) => {
                            decided.skipped.record(skip.reason());
                            // Record the obstruction separately from the skip reason.
                            if let Some(why) = skip.obstruction() {
                                decided.obstructed.record(why);
                            }
                        }
                        // No shipped comparison returns `Deferred`. Record an unexpected deferred decision
                        // as a skip and a plan hole.
                        Verdict::Deferred(_) => {
                            decided
                                .skipped
                                .record(crate::operation::sync::compare::SkipReason::Deferred);
                            deferred = true;
                        }
                    }
                }
                Some(Err(err)) => failures.push(err),
                None => break,
            }
        }

        let mut state = self.inner.state.lock();
        if !walk.is_plan_complete() {
            state.mark_walk_plan_incomplete();
        }
        state.merge.walk = Some(walk);
        state.merge.in_flight = false;
        state.merge.paired += paired;
        state.decided += decided;
        if deferred {
            state.mark_plan_incomplete();
        }
        state.transfers.waiting.append(&mut batch);
        state.deletes.waiting.append(&mut pending_deletes);
        // Warnings continue the run. Failures enter the run record and can stop the run.
        //
        // Record every failure. A fatal failure stops the run under both policies.
        let mut failed = None;
        let mut nothing_left = None;
        for entry in failures {
            if entry.is_warning() {
                state.warnings.record(entry);
                continue;
            }
            // A fatal entry leaves no source stream to continue.
            if entry.is_fatal() && nothing_left.is_none() {
                nothing_left = Some((entry.category(), entry.to_string()));
            }
            if failed.is_none() {
                // Read the category and message before moving the failure into the run record.
                failed = Some((entry.category(), entry.to_string()));
            }
            state.failures.record(entry);
        }
        // The run record keeps every failure. The terminal result carries the first failure.
        // A fatal failure stops the run under both policies.
        if let Some((kind, why)) = nothing_left {
            if state.stopped_by.is_none() {
                state.stopped_by = Some(crate::error::Error::new(kind, why));
            }
        } else if let Some((kind, why)) = failed {
            self.stop_if_aborting(&mut state, crate::error::Error::new(kind, why));
        }

        // The last merge work item can end the run. Signal the caller after the terminal check.
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);

        // Wake the pending run after merge work returns.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }
}

impl<S, D> Transfer for SyncTransfer<S, D>
where
    S: KeyStream + Send + Sync + fmt::Debug + 'static,
    D: KeyStream + Send + Sync + fmt::Debug + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    // Handle terminal cleanup. The scheduler does not poll a terminal transfer again.
    //
    // Read bytes moved before releasing child handles. Released children have unknown outcomes.
    fn on_terminal(&self) {
        // Take child handles under the state lock and drop them after releasing it. Dropping a
        // child can wake the scheduler.
        let children = {
            let mut state = self.inner.state.lock();
            if !state.transfers.running.is_empty() {
                state.transfers.outcomes_unknown += state.transfers.running.len() as u64;
                // Read bytes moved before dropping child handles. Dropping a handle cancels its
                // child.
                let moved: u64 = state
                    .transfers
                    .running
                    .values()
                    .map(ChildHandle::bytes_so_far)
                    .sum();
                state.transfers.bytes += moved;
            }
            std::mem::take(&mut state.transfers.running)
        };
        drop(children);
    }

    fn poll_work(&self) -> PollWork {
        SyncTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(SyncTransfer::execute(self, work))
    }
}

// Upload and download each implement `Transfer`.
// A trait bound that excludes either direction fails to compile here.
const _: fn() = || {
    fn implements_transfer<T: Transfer>() {}
    implements_transfer::<SyncTransfer<crate::io::walk::FsWalk, crate::io::walk::S3Walk>>();
    implements_transfer::<
        SyncTransfer<crate::io::walk::S3Walk, crate::operation::sync::walk::LocalDestination>,
    >();
};

mod child;
mod delete;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tests;
