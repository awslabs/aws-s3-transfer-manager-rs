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
use crate::operation::sync::input::{DeleteMode, RunSettings};
use crate::operation::sync::walk::{Progress, Walk};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};
use crate::types::FailedTransferPolicy;

use crate::operation::sync::walk::Pairing;
use crate::transfer::composite::{Reaping, Reservation};
use child::{SpawnChild, SyncChild};
use delete::DeleteKeys;
use state::{Decided, FailedSyncKey, State};

// Pairings per merge work item. A merge draws from either side, so a listing page cannot size the
// batch. The bound limits how long one work item holds an executor slot.
const MERGE_BATCH: usize = 64;

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
    AdvanceMerge {
        walk: Option<Box<Walk<S, D>>>,
    },
    // Terminal children waiting for a reap. `reap_in_flight` counts them after they leave
    // `children`.
    ReapChildren {
        batch: Option<Reaping<SyncChild, String>>,
    },
    // This variant carries every live child of a stopped run. The work item cancels each child and
    // waits until it settles.
    CancelChildren {
        batch: Option<Reaping<SyncChild, String>>,
    },
    // Keys waiting for deletion. `deletes_in_flight` counts them after a delete work item takes
    // them.
    DeleteKeys {
        keys: Vec<String>,
    },
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncWork::AdvanceMerge { .. } => f.write_str("AdvanceMerge"),
            SyncWork::ReapChildren { batch } => {
                write!(f, "ReapChildren({:?})", batch.as_ref().map(Reaping::len))
            }
            SyncWork::CancelChildren { batch } => {
                write!(f, "CancelChildren({:?})", batch.as_ref().map(Reaping::len))
            }
            SyncWork::DeleteKeys { keys } => write!(f, "DeleteKeys({})", keys.len()),
        }
    }
}

// A claim holds the slot its child will run in and the pairing its decision came from.
type Claim<S, D> = (Reservation, Pairing<S, D>);

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
    deleter: Arc<dyn DeleteKeys>,
    // What to do when something fails. Read at every site a failure can arrive, so one answer
    // covers the run.
    failure_policy: FailedTransferPolicy,
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
        deleter: Arc<dyn DeleteKeys>,
        settings: RunSettings,
    ) -> Self {
        let namespace = deleter.namespace();
        Self {
            inner: Arc::new(Inner {
                ctx,
                comparison,
                spawner,
                deleter,
                delete_mode: settings.delete_mode,
                failure_policy: settings.failure_policy,
                state: Mutex::new(State::new(walk, settings.max_children, namespace)),
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
            if let Some((slot, pairing)) = self.claim_one(&mut state) {
                // Spawn without the state lock. The reservation holds the slot until the child
                // starts.
                drop(state);
                let spawned = match pairing.source().entry() {
                    Some(entry) => {
                        self.inner
                            .spawner
                            .spawn(pairing.key(), &entry.source, self.inner.ctx.id.id)
                    }
                    // A transfer needs a source entry. Record a comparison error when that entry is
                    // absent.
                    None => Err(crate::error::Error::new(
                        crate::error::ErrorKind::RuntimeError,
                        "a transfer was decided for a key with no source entry",
                    )),
                };
                let mut state = self.inner.state.lock();
                match spawned {
                    Ok(child) => state
                        .transfers
                        .start(slot, child, pairing.key().to_string()),
                    Err(err) => {
                        let failure = FailedSyncKey::new(pairing.key(), err);
                        state.fail_transfer(failure, &self.inner.failure_policy);
                    }
                }
                // The claimed key already left the queue. Returning `Spawned` makes the scheduler
                // poll again, so a failed spawn does not strand the keys behind it.
                return PollWork::Spawned;
            }
        }

        if !self.has_stopped(&state) {
            if let Some(work) = self.dispatch_deletes(&mut state) {
                return work;
            }
        }

        // A stopped run cancels its live children before it fails. Each child settles first, so the
        // run reports every child's outcome.
        if let Some(work) = self.dispatch_cancel(&mut state) {
            return work;
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
        !self.inner.ctx.is_active() || state.is_stopped()
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
            // Set the terminal status after outstanding work returns. Every earlier phase checks
            // `has_stopped` and starts no new work.
            if let Some(why) = state.take_stop() {
                self.inner.ctx.set_failed(why);
            }
            // Drop pending deletes and transfers after a stop. Their absence decisions came from an
            // incomplete source stream.
            //
            // The next run compares those keys from a complete view. The run marks the plan
            // incomplete.
            state.discard_waiting_deletes();
            state.discard_waiting_transfers();
            // A merge that stopped before it reached every key left the unreached keys undecided,
            // so the plan is partial even when both queues are empty.
            if state.merge.progress() != Some(Progress::Accounted) {
                state.mark_plan_incomplete();
            }
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

    // Claim one waiting transfer and a slot for it. A stopped run claims nothing.
    fn claim_one(&self, state: &mut State<S, D>) -> Option<Claim<S::Source, D::Source>> {
        if self.has_stopped(state) {
            return None;
        }
        let slot = state.transfers.try_reserve()?;
        // When the run is still removing a name on a transfer's path, it holds the transfer and
        // gives the slot to the next one. The merge decides a name before every key under it, so
        // the removal is already queued when the transfers under that name arrive here.
        loop {
            let (pairing, decision) = state.transfers.next_waiting()?;
            match state.deletes.blocker_of(pairing.key()) {
                Some(blocker) => state.transfers.hold(blocker, (pairing, decision)),
                None => return Some((slot, pairing)),
            }
        }
    }

    // Send a delete batch when it fills or the merge cannot add another key.
    fn dispatch_deletes(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let size = self.inner.deleter.batch_size();
        let merge_done = match state.merge.progress() {
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
        let keys = state
            .deletes
            .take_batch(size, merge_done && !state.merge.in_flight())?;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::DeleteKeys { keys })),
        }))
    }

    // Collect terminal children for a reap. `reaping` counts them after they leave `running`.
    fn dispatch_reap(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let batch = state.transfers.take_finished()?;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::ReapChildren {
                batch: Some(batch),
            })),
        }))
    }

    // Hand every live child of a stopped run to a cancel. A run that its caller cancelled skips
    // this step, because the scheduler already cancelled the run's children.
    fn dispatch_cancel(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if !state.is_stopped() || !self.inner.ctx.is_active() {
            return None;
        }
        let batch = state.transfers.take_all_to_cancel()?;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::CancelChildren {
                batch: Some(batch),
            })),
        }))
    }

    // Hand the merge to a work item. When no merge work remains, the poll chooses `Done` or
    // `Pending`.
    fn dispatch_merge(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let walk = state.merge.take_walk_to_advance()?;
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
            SyncWork::ReapChildren { batch } => {
                let batch = batch.take().expect("the reap was already taken");
                self.execute_reap(batch).await
            }
            SyncWork::CancelChildren { batch } => {
                let batch = batch.take().expect("the cancel was already taken");
                self.execute_cancel(batch).await
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
                state.deletes.settle(&keys);
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
        let asked = keys.clone();
        // Pass the stop check to the destination retry loop. The loop checks before each retry.
        let stopped = || {
            let state = self.inner.state.lock();
            self.has_stopped(&state)
        };
        let outcomes = self.inner.deleter.delete(keys, &stopped).await;

        // S3 reports a per-key refusal inside a successful response. The run records the key and
        // the error.
        let mut state = self.inner.state.lock();
        let gone = outcomes.iter().filter(|outcome| outcome.is_ok()).count() as u64;
        state.deletes.record_response(sent, gone);
        // A destination answers in request order. Each answer releases or fails the transfers held
        // behind its key.
        for (key, outcome) in asked.iter().zip(outcomes) {
            match outcome {
                Ok(_) => state.transfers.release(key),
                Err(failure) => {
                    state.fail_delete(failure, &self.inner.failure_policy);
                    state.transfers.fail_held(key);
                }
            }
        }
        state.deletes.settle(&asked);
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_reap(&self, mut batch: Reaping<SyncChild, String>) -> WorkOutcome {
        let mut moved = 0u64;
        let mut arrived = 0u64;
        let mut failures = Vec::new();
        for (key, result) in batch.join().await {
            match result {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(err) => failures.push(FailedSyncKey::new(key, err)),
            }
        }
        let mut state = self.inner.state.lock();
        for failure in failures {
            state.fail_transfer(failure, &self.inner.failure_policy);
        }
        state.transfers.record_reap(batch, arrived, moved);
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    // A finished child keeps its own result. A child the cancel stopped counts as cancelled. A
    // child that failed on its own counts as a failure.
    async fn execute_cancel(&self, mut batch: Reaping<SyncChild, String>) -> WorkOutcome {
        let mut moved = 0u64;
        let mut arrived = 0u64;
        let mut cancelled = 0u64;
        let mut failures = Vec::new();
        for (key, result) in batch.cancel().await {
            match result {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(err) if *err.kind() == crate::error::ErrorKind::OperationCancelled => {
                    cancelled += 1;
                }
                Err(err) => failures.push(FailedSyncKey::new(key, err)),
            }
        }
        let mut state = self.inner.state.lock();
        for failure in failures {
            state.fail_transfer(failure, &self.inner.failure_policy);
        }
        state.transfers.record_cancelled(cancelled);
        state.transfers.record_reap(batch, arrived, moved);
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
                state.merge.return_walk(walk, 0);
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
        state.merge.return_walk(walk, paired);
        state.add_decided(decided);
        if deferred {
            state.mark_plan_incomplete();
        }
        state.transfers.queue(&mut batch);
        state.deletes.queue(&mut pending_deletes);
        // Warnings continue the run. Failures enter the run record and can stop the run.
        let mut failed = Vec::new();
        for entry in failures {
            if entry.is_warning() {
                state.record_walk_warning(entry);
            } else {
                failed.push(entry);
            }
        }
        // A fatal failure ends the source stream, so the fatal failure stops the run under both
        // policies. Otherwise, under `Abort`, the first failure stops the run. The failure that
        // stops the run becomes the run's terminal error and keeps its service code.
        let stops_at = failed.iter().position(StreamError::is_fatal).or_else(|| {
            (self.inner.failure_policy == FailedTransferPolicy::Abort && !failed.is_empty())
                .then_some(0)
        });
        for (at, entry) in failed.into_iter().enumerate() {
            if Some(at) == stops_at {
                state.stop_on_walk_failure(entry);
            } else {
                state.record_walk_failure(entry);
            }
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
            state.transfers.abandon_running()
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
mod state;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tests;
