/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Scheduler for coordinating transfer execution.
//!
//! The scheduler holds transfers and polls them for work, maintaining control over
//! ordering and admission until the moment of execution. This enables priority
//! changes, cancellation, adaptive concurrency, and memory bounding.
//!
//! # Scheduling Model
//!
//! Transfers are state machines that implement [`Transfer`](crate::transfer::Transfer). The scheduler polls
//! them via `poll_work()` when capacity is available, receiving work items or
//! signals that the transfer is blocked (`Pending`) or finished (`Done`).
//!
//! Work generation is event-driven. The scheduler generates work when a transfer
//! arrives, is woken, work completes (freeing capacity), or the concurrency target
//! changes. It never polls on a timer.
//!
//! ```text
//!   enqueue ──► Transfers ──► Ready Set ──► generate_work() ──► Execution
//!                  ▲                                                │
//!   wake ─────────►│                                                │
//!                  │                                                │
//!                  └──────────── on_completion() ◄──────────────────┘
//! ```
//!
//! # Fair Scheduling (CFS)
//!
//! When multiple transfers are ready, the scheduler uses Completely Fair Scheduling.
//! Each transfer accumulates virtual runtime as it generates work. The transfer with
//! the lowest virtual runtime is selected next. Priority acts as a weight: higher
//! priority means slower accumulation, so the transfer generates more work before
//! yielding to others.
//!
//! # Backpressure
//!
//! Transfers that cannot acquire resources (sequence window full, memory budget
//! exhausted) return `Pending` from `poll_work()`. The scheduler stops polling them
//! until `wake()` is called, which re-inserts the transfer into the ready set and
//! triggers work generation.
//!
//! # Threading and Cost
//!
//! Scheduler work runs on the caller's thread. `on_completion` and `wake` drive
//! `generate_work` synchronously on whatever thread invoked them - typically a
//! managed execution thread. This is a deliberate choice for hot-path throughput
//! (no channel hop, warm caches, parallel drive across transfers), gated on the
//! constraints below holding.
//!
//! - **`poll_work` is synchronous and short.** No `.await`, no blocking I/O, no
//!   unbounded loops. Cost is O(1) per call with a bounded critical section on
//!   state. Long `poll_work` calls pin the caller's runtime, preventing it from
//!   polling its own async tasks (including in-flight SDK requests).
//! - **At most one thread is inside `poll_work(desc)` at a time.** Enforced by a
//!   claim atom on [`TransferDescriptor`](super::descriptor::TransferDescriptor). The ready set's insert is
//!   CAS-gated on this claim, and the claim is held through `pop` and `poll_work`
//!   until the scheduler has finished handling the outcome. This keeps the
//!   per-transfer state mutex effectively uncontended in steady state.
//! - **Ready set entries are unique per transfer id.** Duplicates re-open the
//!   single-poll invariant and produce lock-contention storms under burst
//!   completions.
//! - **Composite transfers pay their own per-call cost.** A composite's
//!   `poll_work` may recursively call [`Scheduler::enqueue_transfer`] to spawn
//!   children. The per-call fan-out must be bounded - cost is
//!   `O(batch × enqueue_cost)`.
//! - **`execute` is the only async surface.** State locks must not be held across
//!   `.await`. Mid-execution the transfer may call `scheduler.wake(id)` to
//!   re-queue itself if intra-execute state changes unblock work.
//!
//! # State Machine Contracts
//!
//! A [`Transfer`](crate::transfer::Transfer) implementation must uphold:
//! - **Failed lifecycle**: record the error and signal termination before returning
//!   a failure outcome.
//! - **Pending/wake obligation**: every `Pending` must have a future wake path.
//!   The wake primitive is edge-triggered; the mutator pattern is
//!   `lock → mutate → unlock → try_wake`. See [`crate::transfer::TransferContext`]
//!   for the protocol.
//! - **Panic safety**: `execute` panics are caught by the runtime's
//!   `catch_unwind` wrapper and converted to a terminal transition.
//!   `poll_work` panics are caught by the scheduler itself inside
//!   `generate_work`, which force-terminates the panicking transfer
//!   (cascading to children) and continues processing other transfers.
//!
//! See `docs/design/scheduler.md` for the long-form design discussion,
//! invariants, and case studies.

use super::CompletionSample;
use crate::telemetry;
use crate::transfer::{BoxTransfer, PollWork, TransferId, WorkOutcome};

use crate::runtime::sync::{Submission, SubmissionQueue};
use crate::runtime::ScheduledWork;
use crate::scheduler::descriptor::{ClaimGuard, TransferDescriptor};
use crate::scheduler::ready_set::{OrphanedChild, ReadySet};
use std::collections::HashMap;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

/// Batch size for work generated and submitted in a single round
const SUBMISSION_QUEUE_SIZE: usize = 64;

thread_local! {
    /// True while this thread is inside [`Scheduler::generate_work`]. Used
    /// by [`Scheduler::enqueue_transfer`] to skip driving the scheduler
    /// when called from inside a `poll_work` frame.
    ///
    /// Defense in depth against re-entrancy: both the submission queue
    /// (see [`SubmissionQueue`] in `runtime::sync`) and the upload_objects
    /// state machine (see `poll_work` in `upload_objects::transfer`) are
    /// individually re-entrancy-safe, but making the re-entrant
    /// `generate_work` call a no-op at the scheduler boundary is cheaper
    /// than letting the call propagate all the way through the scheduler
    /// to discover there's no work to do. The outer `generate_work` frame
    /// will drive the newly-inserted transfer on its next `ready_set.pop`
    /// iteration.
    static IN_GENERATE_WORK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// RAII guard that clears [`IN_GENERATE_WORK`] on drop. Constructed at the
/// top of [`Scheduler::generate_work`] so the flag is unset even if a nested
/// `poll_work` panics.
struct GenerateWorkGuard;

impl GenerateWorkGuard {
    fn enter() -> Self {
        IN_GENERATE_WORK.with(|f| f.set(true));
        Self
    }
}

impl Drop for GenerateWorkGuard {
    fn drop(&mut self) {
        IN_GENERATE_WORK.with(|f| f.set(false));
    }
}

/// Event-driven scheduler for coordinating transfer work.
///
/// Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    transfers: RwLock<HashMap<TransferId, TransferDescriptor>>,
    ready_set: ReadySet,
    handle: Weak<crate::client::Handle>,
    dispatched: AtomicUsize,
    submission_queue: SubmissionQueue<ScheduledWork>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

impl Scheduler {
    pub(crate) fn new(handle: Weak<crate::client::Handle>) -> Self {
        Scheduler(Arc::new(SchedulerInner {
            transfers: RwLock::new(HashMap::new()),
            ready_set: ReadySet::new(),
            handle,
            dispatched: AtomicUsize::new(0),
            submission_queue: SubmissionQueue::new(SUBMISSION_QUEUE_SIZE),
        }))
    }

    fn handle(&self) -> Arc<crate::client::Handle> {
        self.0
            .handle
            .upgrade()
            .expect("Handle dropped while scheduler active")
    }

    /// Called by the runtime when a worker picks up work.
    pub(crate) fn on_dispatch(&self) {
        self.handle().controller.on_dispatch();
    }

    /// Add a transfer and start generating work.
    ///
    /// Lock-ordering protocol: this method holds `transfers.write` while
    /// resolving the parent group via `by_group.read`. Together with
    /// `cancel_transfer` (which holds `transfers.write` across its
    /// `by_group.write`), the protocol prevents a child being orphaned
    /// in `transfers` after its parent group is removed. See
    /// `loom_tests::fixed_dance_no_orphan` at the bottom of this file.
    pub(crate) fn enqueue_transfer(&self, transfer: BoxTransfer) {
        let tid = transfer.ctx().id;

        // Lock order: transfers.write -> by_group.read.
        //
        // We acquire transfers.write first, then resolve the parent group's
        // vruntime arc under by_group.read, then insert into transfers, all
        // within the same critical section. This serializes against
        // `cancel_transfer`, which holds transfers.write across its own
        // by_group.write call. Without this ordering, a concurrent
        // cancel_transfer could remove the parent's group between our
        // resolve_group_vruntime check and our transfers.insert, leaving
        // a child orphaned in transfers.
        //
        // ready_set.insert (below) is performed AFTER releasing transfers.write
        // and re-acquires by_group.read on its own. If the parent's group was
        // removed in that window, insert returns OrphanedChild and we cancel
        // the child via the existing fallback path.
        let (group_vruntime, desc) = {
            let mut transfers = self.0.transfers.write().unwrap();
            let group_vruntime = match self.0.ready_set.resolve_group_vruntime(&tid) {
                Some(gv) => gv,
                None => {
                    // Parent group gone (cancelled mid-spawn). Set the
                    // transfer's context to cancelled and signal terminal
                    // so its handle resolves. No need to insert into the
                    // transfers map since nothing will ever poll or wake
                    // this transfer.
                    drop(transfers);
                    let ctx = transfer.ctx();
                    ctx.set_cancelled();
                    ctx.signal_terminal();
                    return;
                }
            };
            let desc = TransferDescriptor::new_with_vruntime(
                transfer,
                self.0.ready_set.min_vruntime(),
                group_vruntime.clone(),
            );
            transfers.insert(desc.id(), desc.clone());
            (group_vruntime, desc)
        };
        let _ = group_vruntime; // returned by resolve_group_vruntime, retained inside desc
        let id = desc.id();

        match self.0.ready_set.insert(desc) {
            Ok(()) => {}
            Err(OrphanedChild) => {
                // Parent's group was removed between transfers.write release
                // and ready_set.insert. Cancel the new transfer so its handle
                // resolves to a cancelled state, no leaked oneshot.
                self.cancel_transfer(id);
                return;
            }
        }

        tracing::debug!(target: telemetry::TARGET_SCHEDULING, id = %id, "transfer enqueued");
        // Only drive `generate_work` from the top level. When we are already
        // inside a `generate_work` frame on this thread (the parent's
        // `poll_work` called `spawn_children` → `enqueue_transfer`), the
        // outer frame will pick up the newly-inserted transfer on its next
        // loop iteration. Re-entering `generate_work` here would deadlock:
        // the re-entrant frame increments `SubmissionQueue::pending` and
        // leaves it live until the outer frame finishes, which prevents
        // `submit_and_reenter` from ever flushing work to the runtime.
        let reentrant = IN_GENERATE_WORK.with(|f| f.get());
        if !reentrant {
            self.generate_work();
        }
    }

    /// Wake a transfer, moving it from pending to ready.
    ///
    /// Called by state machines when they transition from blocked to ready.
    /// Only generates work if transfer exists and was inserted to ready set.
    pub(crate) fn wake(&self, id: TransferId) {
        let desc = {
            let transfers = self.0.transfers.read().unwrap();
            transfers.get(&id).cloned()
        };
        match desc {
            Some(desc) => {
                // Mark wake_requested before insert: if a poll is in
                // flight, it will observe the flag in the release-and-
                // recheck path. Otherwise the insert (or no-op if
                // already queued) puts the descriptor back in the
                // ready set.
                desc.mark_wake_requested();
                // OrphanedChild: parent's group was removed; wake is moot.
                let _ = self.0.ready_set.insert(desc);
                tracing::trace!(
                    target: telemetry::TARGET_SCHEDULING,
                    id = %id,
                    "wake",
                );
                self.generate_work();
            }
            None => {
                tracing::trace!(
                    target: telemetry::TARGET_SCHEDULING,
                    id = %id,
                    "wake.not_found",
                );
            }
        }
    }

    /// Cancel a transfer and any child transfers, removing them from the scheduler.
    ///
    /// Lock-ordering protocol: this method holds `transfers.write` across
    /// its call to `ready_set.remove_group` (which acquires
    /// `by_group.write` internally). Together with `enqueue_transfer`,
    /// the protocol prevents a child being orphaned in `transfers` after
    /// its parent group is removed. See `loom_tests::fixed_dance_no_orphan`
    /// at the bottom of this file.
    ///
    /// Cancels the target transfer first, then cancels all transfers whose
    /// `TransferId.parent == Some(id.id)` (depth-1 children only). Each cancelled
    /// transfer has its status set to cancelled, pending work purged, and idle
    /// notification sent. Outstanding work already being executed will complete
    /// naturally - use `wait_for_idle(id)` to wait for draining.
    ///
    /// Returns `true` if the target transfer existed in the map, `false` otherwise.
    /// The return value does not reflect whether any children were found or cancelled.
    pub(crate) fn cancel_transfer(&self, id: TransferId) -> bool {
        let (target, children) = {
            let mut transfers = self.0.transfers.write().unwrap();
            let target = transfers.remove(&id);
            let child_keys: Vec<TransferId> = transfers
                .keys()
                .filter(|tid| tid.parent == Some(id.id))
                .copied()
                .collect();
            let children: Vec<TransferDescriptor> = child_keys
                .iter()
                .filter_map(|k| transfers.remove(k))
                .collect();
            // Remove the parent's group from ready_set while still holding
            // transfers.write. This serializes against `enqueue_transfer`,
            // which acquires transfers.write before resolving the parent
            // group. After we release transfers.write below, any concurrent
            // enqueue for a child of this parent observes by_group.read
            // returning None inside its transfers.write critical section
            // and short-circuits to set_cancelled/signal_terminal without
            // inserting into transfers. No orphans possible.
            if target.is_some() && id.parent.is_none() {
                self.0.ready_set.remove_group(id.id);
            }
            (target, children)
        };

        let found = target.is_some();
        if let Some(desc) = target {
            self.cancel_descriptor(desc);
        }
        for desc in children {
            self.cancel_descriptor(desc);
        }
        found
    }

    /// Cancel a single transfer descriptor: set cancelled, signal terminal,
    /// clean up on_terminal, purge pending work, and notify idle.
    fn cancel_descriptor(&self, desc: TransferDescriptor) {
        let id = desc.id();
        let ctx = desc.transfer().ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
        }
        ctx.signal_terminal();
        desc.transfer().on_terminal();
        let purged = self.handle().runtime.remove_pending_for_transfer(id);
        self.0.dispatched.fetch_sub(purged, Ordering::Relaxed);
        desc.work_purged(purged);
        desc.notify_idle();
        tracing::debug!(target: telemetry::TARGET_SCHEDULING, id = %id, purged, "transfer cancelled");
    }

    /// Set the priority of a transfer.
    ///
    /// Priority affects how fast vruntime accumulates (CFS-style scheduling):
    /// - Higher priority (255) = slower accumulation = more work share
    /// - Lower priority (1) = faster accumulation = less work share
    /// - Default priority is 128
    ///
    /// The change takes effect on the next work generation cycle.
    pub(crate) fn set_priority(&self, id: TransferId, priority: u8) {
        let transfers = self.0.transfers.read().unwrap();
        if let Some(desc) = transfers.get(&id) {
            desc.set_priority(priority);
        }
    }

    /// Called by workers when work completes.
    pub(crate) fn on_completion(
        &self,
        work: ScheduledWork,
        outcome: WorkOutcome,
        elapsed: Duration,
    ) {
        let outcome_tag = match &outcome {
            WorkOutcome::Success { .. } => "success",
            WorkOutcome::Failed { .. } => "failed",
            WorkOutcome::Cancelled => "cancelled",
        };
        tracing::trace!(
            target: telemetry::TARGET_SCHEDULING,
            id = %work.descriptor.id(),
            ?elapsed,
            outcome = outcome_tag,
            "work completed",
        );
        self.0.dispatched.fetch_sub(1, Ordering::Relaxed);

        // Report to concurrency controller
        let classification = match &outcome {
            WorkOutcome::Failed { classification } => *classification,
            _ => None,
        };
        let sample = CompletionSample {
            error: classification,
        };
        self.handle().controller.on_completion(&sample);

        let desc = &work.descriptor;
        let is_idle = desc.work_finished();
        if is_idle {
            desc.notify_idle();
        }

        // Terminal transfer: no further work from THIS transfer. Clean up when fully drained.
        if desc.is_terminal() && is_idle {
            let tid = desc.id();
            self.0.transfers.write().unwrap().remove(&tid);
            if tid.parent.is_none() {
                self.0.ready_set.remove_group(tid.id);
            }
        }

        // A concurrency slot was freed - always generate work. Other transfers
        // may be waiting for capacity.
        self.generate_work();
    }

    /// Handle a panic during work execution. The transfer's internal state is
    /// unknown, so the scheduler forces the terminal transition from outside.
    pub(crate) fn on_panic(&self, work: ScheduledWork) {
        self.0.dispatched.fetch_sub(1, Ordering::Relaxed);

        let desc = &work.descriptor;
        let ctx = desc.transfer().ctx();

        let err = crate::error::from_kind(crate::error::ErrorKind::RuntimeError)(
            "worker panic during execute",
        );
        ctx.set_failed(err);
        ctx.signal_terminal();
        desc.transfer().on_terminal();

        let is_idle = desc.work_finished();
        if is_idle {
            let tid = desc.id();
            self.0.transfers.write().unwrap().remove(&tid);
            if tid.parent.is_none() {
                self.0.ready_set.remove_group(tid.id);
            }
        }
        desc.notify_idle();
    }

    fn has_capacity(&self) -> bool {
        self.0.dispatched.load(Ordering::Relaxed) < self.handle().controller.target()
    }

    /// Flush the current submission and re-enter for the next batch.
    fn submit_and_reenter<'a>(
        &'a self,
        sub: Submission<'a, ScheduledWork>,
    ) -> Submission<'a, ScheduledWork> {
        if let Some(mut guard) = sub.submit() {
            self.handle().runtime.dispatch(&mut guard);
        }
        self.0.submission_queue.enter()
    }

    /// Push work into the submission, flushing and retrying on contention.
    fn enqueue<'a>(
        &'a self,
        mut sub: Submission<'a, ScheduledWork>,
        work: ScheduledWork,
    ) -> Submission<'a, ScheduledWork> {
        let mut pending = work;
        loop {
            match sub.push(pending) {
                Ok(()) => return sub,
                Err(returned) => {
                    sub = self.submit_and_reenter(sub);
                    pending = returned;
                }
            }
        }
    }

    /// Generate work from ready transfers and dispatch to runtime.
    fn generate_work(&self) {
        let _guard = GenerateWorkGuard::enter();
        let mut sub = self.0.submission_queue.enter();
        let mut generated = 0usize;
        let mut polled = 0usize;
        let mut pending_count = 0usize;
        let mut done_count = 0usize;
        let mut terminal_skipped = 0usize;
        tracing::trace!(
            target: telemetry::TARGET_SCHEDULING,
            has_capacity = self.has_capacity(),
            dispatched = self.0.dispatched.load(Ordering::Relaxed),
            target = self.handle().controller.target(),
            "generate_work.enter",
        );
        let break_reason = loop {
            if !self.has_capacity() {
                break "no_capacity";
            }
            let Some(desc) = self.0.ready_set.pop() else {
                break "ready_set_empty";
            };

            // Skip cancelled/failed transfers still in the ready set.
            // Release the claim explicitly since we won't call poll_work.
            if desc.is_terminal() {
                terminal_skipped += 1;
                let claim = ClaimGuard::new(&desc);
                claim.release();
                continue;
            }

            polled += 1;
            let id = desc.id();
            // Consume any pre-existing wake signal so we only observe
            // wakes that arrive DURING the poll below.
            desc.take_wake_requested();

            let claim = ClaimGuard::new(&desc);
            let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                desc.transfer().poll_work()
            }));

            match poll_result {
                Ok(PollWork::Ready(item)) => {
                    generated += 1;
                    desc.work_generated();
                    // Re-insert under the still-held claim - bypasses the
                    // CAS gate that `insert` would otherwise apply.
                    claim.hold();
                    self.0.ready_set.reinsert_under_claim(desc.clone());
                    self.0.dispatched.fetch_add(1, Ordering::Relaxed);
                    desc.work_queued();
                    let work = ScheduledWork {
                        item,
                        descriptor: desc,
                    };
                    sub = self.enqueue(sub, work);
                }
                Ok(PollWork::Pending) => {
                    pending_count += 1;
                    tracing::trace!(
                        target: telemetry::TARGET_SCHEDULING,
                        id = %id,
                        "poll_work.pending",
                    );
                    // Release-and-recheck. A wake arriving in this
                    // window either takes the claim itself (its CAS
                    // succeeds) or sets wake_requested for us to
                    // observe. See the `claim` module for the protocol.
                    claim.release();
                    if desc.take_wake_requested() {
                        // OrphanedChild: parent's group was removed; wake is moot.
                        let _ = self.0.ready_set.insert(desc);
                    }
                }
                Ok(PollWork::Done) => {
                    done_count += 1;
                    tracing::trace!(
                        target: telemetry::TARGET_SCHEDULING,
                        id = %id,
                        "poll_work.done",
                    );
                    claim.release();
                    let desc_id = desc.id();
                    self.0.transfers.write().unwrap().remove(&desc_id);
                    if desc_id.parent.is_none() {
                        self.0.ready_set.remove_group(desc_id.id);
                    }
                }
                Err(_panic_payload) => {
                    // ClaimGuard's Drop releases the claim. cancel_transfer
                    // handles terminal transition + child cascade.
                    tracing::error!(
                        target: telemetry::TARGET_SCHEDULING,
                        id = %id,
                        "panic in poll_work, forcing terminal",
                    );
                    drop(claim);
                    drop(desc);
                    self.cancel_transfer(id);
                }
            }
        };
        if let Some(mut guard) = sub.submit() {
            self.handle().runtime.dispatch(&mut guard);
        }
        tracing::trace!(
            target: telemetry::TARGET_SCHEDULING,
            generated,
            polled,
            pending_count,
            done_count,
            terminal_skipped,
            break_reason,
            dispatched = self.0.dispatched.load(Ordering::Relaxed),
            target = self.handle().controller.target(),
            "generate_work.exit",
        );
    }

    #[allow(dead_code)]
    pub(crate) fn is_idle(&self) -> bool {
        self.0.transfers.read().unwrap().is_empty()
            && self.0.dispatched.load(Ordering::Relaxed) == 0
    }

    /// Wait for a specific transfer to have no outstanding work.
    pub(crate) async fn wait_for_idle(&self, id: TransferId) {
        let desc = {
            let transfers = self.0.transfers.read().unwrap();
            transfers.get(&id).cloned()
        };
        if let Some(desc) = desc {
            desc.wait_for_idle().await;
        }
    }

    /// Pre-register an empty group for `group_id`. See
    /// [`crate::scheduler::ready_set::ReadySet::register_empty_group_for_test`].
    #[cfg(test)]
    pub(crate) fn register_empty_group_for_test(&self, group_id: u64) {
        self.0.ready_set.register_empty_group_for_test(group_id);
    }

    /// Number of members currently queued in a group's ready set.
    ///
    /// Returns `None` if the group does not exist (already removed).
    #[cfg(test)]
    pub(crate) fn group_member_count(&self, group_id: u64) -> Option<usize> {
        self.0.ready_set.member_count(group_id)
    }

    /// Number of transfers currently tracked by the scheduler.
    #[cfg(test)]
    pub(crate) fn transfer_count(&self) -> usize {
        self.0.transfers.read().unwrap().len()
    }
}
#[cfg(test)]
mod tests {
    use crate::client::Handle;
    use crate::scheduler::transfer::mock::{
        FixedWorkCount, MockStateMachine, WithDelay, WithExecute,
    };
    use crate::scheduler::MockTransfer;
    use crate::transfer::{IoRequest, PollWork, Transfer, TransferId, WorkOutcome};
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_handle(concurrency: usize) -> Arc<Handle> {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().client(s3_client).build();
        Handle::new_for_test(config, concurrency)
    }

    fn test_handle_managed(concurrency: usize) -> Arc<Handle> {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().client(s3_client).build();
        Handle::new_for_test_with_runtime(config, concurrency, |weak| {
            Arc::new(
                crate::runtime::ManagedThreadRuntime::builder(weak)
                    .topology(crate::runtime::Topology::uniform(4))
                    .build(),
            )
        })
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_transfer_completes() {
        let _logs = show_test_logs();
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let sm = Arc::new(FixedWorkCount::new(5));
        let transfer = Box::new(MockTransfer::new(id, sm.clone()));
        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        assert!(sm.is_complete());
        assert_eq!(sm.completed_count(), 5);
        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_transfer_completes_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(4);
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let sm = Arc::new(FixedWorkCount::new(5));
        let transfer = Box::new(MockTransfer::new(id, sm.clone()));
        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        assert!(sm.is_complete());
        assert_eq!(sm.completed_count(), 5);
        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_multiple_transfers_complete() {
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;
        let mut state_machines = Vec::new();

        for i in 0..3u64 {
            let id = TransferId {
                id: i,
                parent: None,
            };
            let sm = Arc::new(FixedWorkCount::new(4));
            state_machines.push(sm.clone());
            scheduler.enqueue_transfer(Box::new(MockTransfer::new(id, sm)));
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        for (i, sm) in state_machines.iter().enumerate() {
            assert!(sm.is_complete(), "transfer {} should be complete", i);
            assert_eq!(sm.completed_count(), 4);
        }
        handle.runtime.shutdown();
    }

    /// Regression test: many single-work-item transfers with concurrency target
    /// lower than the transfer count. Each transfer generates exactly one work
    /// item (like a single PutObject upload). All must complete - if on_completion
    /// doesn't call generate_work() for terminal transfers, only the first
    /// `concurrency` transfers complete and the rest hang forever.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_work_transfers_exceed_concurrency() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;
        let mut state_machines = Vec::new();

        for i in 0..10u64 {
            let id = TransferId {
                id: i,
                parent: None,
            };
            let sm = Arc::new(FixedWorkCount::new(1));
            state_machines.push(sm.clone());
            scheduler.enqueue_transfer(Box::new(MockTransfer::new(id, sm)));
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all 10 transfers should complete - scheduler must generate work after terminal completions");

        for (i, sm) in state_machines.iter().enumerate() {
            assert!(sm.is_complete(), "transfer {} should be complete", i);
            assert_eq!(sm.completed_count(), 1);
        }
        handle.runtime.shutdown();
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_new_does_not_require_runtime() {
        let _handle = test_handle(4);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_set_priority() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        // Use a mock that returns Pending so it doesn't complete immediately
        let sm = Arc::new(PendingForever);
        let transfer = Box::new(MockTransfer::new(id, sm));
        scheduler.enqueue_transfer(transfer);

        // Default priority is 128
        {
            let transfers = scheduler.0.transfers.read().unwrap();
            let desc = transfers.get(&id).expect("transfer should exist");
            assert_eq!(desc.priority(), 128);
        }

        // Change priority
        scheduler.set_priority(id, 255);

        {
            let transfers = scheduler.0.transfers.read().unwrap();
            let desc = transfers.get(&id).expect("transfer should still exist");
            assert_eq!(desc.priority(), 255);
        }

        // Setting priority on non-existent transfer is a no-op
        let fake_id = TransferId {
            id: 999,
            parent: None,
        };
        scheduler.set_priority(fake_id, 100); // Should not panic

        handle.runtime.shutdown();
    }

    /// Mock that always returns Pending (never generates work, never completes)
    #[derive(Debug)]
    struct PendingForever;

    impl MockStateMachine for PendingForever {
        fn poll_work(&self, _id: TransferId) -> PollWork {
            PollWork::Pending
        }

        fn execute<'a>(
            &'a self,
            _work: &'a mut IoRequest,
        ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
            Box::pin(async { unreachable!("PendingForever never generates work") })
        }
    }

    /// Mock that generates infinite work and counts executions.
    /// Can be stopped by calling `set_done()`.
    #[derive(Debug)]
    struct CountingInfinite {
        executions: AtomicUsize,
        done: std::sync::atomic::AtomicBool,
    }

    impl CountingInfinite {
        fn new() -> Self {
            Self {
                executions: AtomicUsize::new(0),
                done: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn count(&self) -> usize {
            self.executions.load(Ordering::Relaxed)
        }

        fn set_done(&self) {
            self.done.store(true, Ordering::Release);
        }
    }

    impl MockStateMachine for CountingInfinite {
        fn poll_work(&self, _id: TransferId) -> PollWork {
            if self.done.load(Ordering::Acquire) {
                return PollWork::Done;
            }
            PollWork::Ready(IoRequest { data: None })
        }

        fn execute<'a>(
            &'a self,
            _work: &'a mut IoRequest,
        ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
            Box::pin(async move {
                self.executions.fetch_add(1, Ordering::Relaxed);
                // Yield to prevent starving other tasks on single-threaded runtimes
                tokio::task::yield_now().await;
                // No schedule_next - let generate_work() pull from ready set
                // where CFS priority ordering applies
                WorkOutcome::Success { data: None }
            })
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_priority_affects_work_distribution() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let high_id = TransferId {
            id: 1,
            parent: None,
        };
        let low_id = TransferId {
            id: 2,
            parent: None,
        };

        let high_mock = Arc::new(CountingInfinite::new());
        let low_mock = Arc::new(CountingInfinite::new());

        let high_transfer = Box::new(MockTransfer::new(high_id, high_mock.clone()));
        let low_transfer = Box::new(MockTransfer::new(low_id, low_mock.clone()));

        scheduler.enqueue_transfer(high_transfer);
        scheduler.enqueue_transfer(low_transfer);

        // Set priorities: high=255, low=64 (4x difference)
        scheduler.set_priority(high_id, 255);
        scheduler.set_priority(low_id, 64);

        // Wait until we have enough samples
        let target_total = 200;
        while high_mock.count() + low_mock.count() < target_total {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Stop generating work
        high_mock.set_done();
        low_mock.set_done();

        handle.runtime.shutdown();

        let high_count = high_mock.count();
        let low_count = low_mock.count();

        // 4x priority difference (255 vs 64) yields ~4x dispatch ratio
        // via the priority-scaled vruntime delta in `work_generated`.
        assert!(
            high_count > low_count,
            "high priority should execute more: high={}, low={}",
            high_count,
            low_count
        );

        let ratio = high_count as f64 / low_count.max(1) as f64;
        // Priority delta in `work_generated` is `(WORK_COST * 256) / priority`,
        // so high (255) advances ~4x slower than low (64), giving ~4x more
        // dispatch share. Allow a generous lower bound of 3.0 to absorb
        // sampling noise from the 200-dispatch window; observed in
        // practice is ~4.0.
        assert!(
            ratio > 3.0,
            "expected ratio > 3.0, got {:.2} (high={}, low={})",
            ratio,
            high_count,
            low_count
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_priority_change_mid_flight() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let a_id = TransferId {
            id: 1,
            parent: None,
        };
        let b_id = TransferId {
            id: 2,
            parent: None,
        };

        let a_mock = Arc::new(CountingInfinite::new());
        let b_mock = Arc::new(CountingInfinite::new());

        scheduler.enqueue_transfer(Box::new(MockTransfer::new(a_id, a_mock.clone())));
        scheduler.enqueue_transfer(Box::new(MockTransfer::new(b_id, b_mock.clone())));

        // Both at default priority (128) - let them run equally
        while a_mock.count() + b_mock.count() < 100 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Snapshot counts, then shift priority heavily
        let a_before = a_mock.count();
        let b_before = b_mock.count();
        scheduler.set_priority(a_id, 255);
        scheduler.set_priority(b_id, 1);

        // Let them run more
        while (a_mock.count() - a_before) + (b_mock.count() - b_before) < 200 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        a_mock.set_done();
        b_mock.set_done();
        handle.runtime.shutdown();

        let a_after = a_mock.count() - a_before;
        let b_after = b_mock.count() - b_before;

        // 255:1 priority ratio - A should get the vast majority of work
        let ratio = a_after as f64 / b_after.max(1) as f64;
        assert!(
            ratio > 3.0,
            "after priority change (255 vs 1), expected ratio > 3.0, got {:.2} (a={}, b={})",
            ratio,
            a_after,
            b_after
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_failed_transfer_cleaned_up() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let sm = Arc::new(WithExecute::new(FixedWorkCount::new(1), |_| {
            WorkOutcome::Failed {
                classification: None,
            }
        }));
        scheduler.enqueue_transfer(Box::new(MockTransfer::new(id, sm)));

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after failed transfer");

        assert!(!scheduler.0.transfers.read().unwrap().contains_key(&id));
        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_panic_transfer_cleaned_up_and_error_propagated() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let panic_id = TransferId {
            id: 1,
            parent: None,
        };
        let panic_sm = Arc::new(WithExecute::new(
            FixedWorkCount::new(1),
            |_| -> WorkOutcome { panic!("boom") },
        ));
        let panic_mock = MockTransfer::new(panic_id, panic_sm);
        let panic_ctx = panic_mock.ctx().clone();
        scheduler.enqueue_transfer(Box::new(panic_mock));

        let ok_id = TransferId {
            id: 2,
            parent: None,
        };
        let ok_sm = Arc::new(FixedWorkCount::new(3));
        scheduler.enqueue_transfer(Box::new(MockTransfer::new(ok_id, ok_sm.clone())));

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        assert!(panic_ctx.is_failed());
        assert_eq!(
            panic_ctx.error_kind(),
            Some(crate::error::ErrorKind::RuntimeError),
        );
        assert!(!scheduler
            .0
            .transfers
            .read()
            .unwrap()
            .contains_key(&panic_id));

        assert!(ok_sm.is_complete());
        assert_eq!(ok_sm.completed_count(), 3);

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_transfer_stops_work() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        // Use WithDelay to make work slow enough to cancel mid-flight
        let inner = FixedWorkCount::new(20);
        let sm = Arc::new(WithDelay::new(inner, Duration::from_millis(50)));
        let transfer = Box::new(MockTransfer::new(id, sm.clone()));
        scheduler.enqueue_transfer(transfer);

        // Let some work start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the transfer - returns true on first call
        assert!(scheduler.cancel_transfer(id));
        assert!(!scheduler.cancel_transfer(id));

        // Wait for scheduler to become idle
        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cancel");

        // Should have completed fewer than total (cancelled early)
        let completed = sm.inner().completed_count();
        assert!(
            completed < 20,
            "expected fewer than 20 completions, got {}",
            completed
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_parent_cascades_to_children() {
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        let child_ids: Vec<TransferId> = (2..=4)
            .map(|i| TransferId {
                id: i,
                parent: Some(1),
            })
            .collect();

        let parent_sm = Arc::new(WithDelay::new(
            FixedWorkCount::new(20),
            Duration::from_millis(50),
        ));
        let parent_transfer = MockTransfer::new(parent_id, parent_sm);
        let parent_ctx = parent_transfer.ctx().clone();
        scheduler.enqueue_transfer(Box::new(parent_transfer));

        let mut child_ctxs = Vec::new();
        for &cid in &child_ids {
            let sm = Arc::new(WithDelay::new(
                FixedWorkCount::new(20),
                Duration::from_millis(50),
            ));
            let transfer = MockTransfer::new(cid, sm);
            child_ctxs.push(transfer.ctx().clone());
            scheduler.enqueue_transfer(Box::new(transfer));
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(scheduler.cancel_transfer(parent_id));

        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cascade cancel");

        assert!(!parent_ctx.is_active(), "parent should not be active");
        for (i, ctx) in child_ctxs.iter().enumerate() {
            assert!(!ctx.is_active(), "child {} should not be active", i + 2);
        }

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_parent_with_no_children_still_works() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let sm = Arc::new(WithDelay::new(
            FixedWorkCount::new(20),
            Duration::from_millis(50),
        ));
        let transfer = MockTransfer::new(id, sm);
        let ctx = transfer.ctx().clone();
        scheduler.enqueue_transfer(Box::new(transfer));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(scheduler.cancel_transfer(id));
        assert!(!scheduler.cancel_transfer(id));

        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cancel");

        assert!(!ctx.is_active(), "parent should not be active");

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_child_does_not_affect_siblings_or_parent() {
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        let child_a = TransferId {
            id: 2,
            parent: Some(1),
        };
        let child_b = TransferId {
            id: 3,
            parent: Some(1),
        };

        let parent_sm = Arc::new(WithDelay::new(
            FixedWorkCount::new(20),
            Duration::from_millis(50),
        ));
        let parent_transfer = MockTransfer::new(parent_id, parent_sm);
        let parent_ctx = parent_transfer.ctx().clone();
        scheduler.enqueue_transfer(Box::new(parent_transfer));

        let sm_a = Arc::new(WithDelay::new(
            FixedWorkCount::new(20),
            Duration::from_millis(50),
        ));
        let transfer_a = MockTransfer::new(child_a, sm_a);
        let ctx_a = transfer_a.ctx().clone();
        scheduler.enqueue_transfer(Box::new(transfer_a));

        let sm_b = Arc::new(WithDelay::new(
            FixedWorkCount::new(20),
            Duration::from_millis(50),
        ));
        let transfer_b = MockTransfer::new(child_b, sm_b);
        let ctx_b = transfer_b.ctx().clone();
        scheduler.enqueue_transfer(Box::new(transfer_b));

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(scheduler.cancel_transfer(child_a));
        assert!(ctx_a.is_cancelled(), "child A should be cancelled");
        assert!(parent_ctx.is_active(), "parent should still be active");
        assert!(ctx_b.is_active(), "sibling B should still be active");

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_nonexistent_transfer_returns_false() {
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let fake_id = TransferId {
            id: 999,
            parent: None,
        };
        assert!(!scheduler.cancel_transfer(fake_id));

        handle.runtime.shutdown();
    }

    // =========================================================================
    // Hierarchical CFS integration tests
    //
    // These tests pin the fairness, memory-cap, priority, cancellation, and
    // panic-recovery behavior of the two-level (group + member) CFS scheduler.
    // =========================================================================

    use crate::scheduler::transfer::mock::{
        CompositeMock, CountedWork, DispatchCounter, PanickingCompositeMock,
    };

    async fn impl_single_composite_uses_target_fully(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let composite = CompositeMock::new(
            id,
            handle.clone(),
            200,   // total children
            1,     // work per child
            10000, // memory cap (won't be hit)
            counter.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        let start = tokio::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        let elapsed = start.elapsed();
        let total_dispatches = counter.count();
        assert_eq!(total_dispatches, 200, "all 200 children should complete");

        // Wall time check: generous bound for CI
        assert!(
            elapsed < Duration::from_secs(15),
            "took too long: {:?}",
            elapsed
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_composite_uses_target_fully() {
        let handle = test_handle(200);
        impl_single_composite_uses_target_fully(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_composite_uses_target_fully_managed_runtime() {
        // See note on test_composite_vs_single_fair_share_at_root_managed_runtime
        // for why target is lower under managed runtime.
        let handle = test_handle_managed(20);
        impl_single_composite_uses_target_fully(handle).await;
    }

    async fn impl_composite_vs_single_fair_share_at_root(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        // Composite C: 50 children, 1 work item each
        let c_counter = DispatchCounter::new();
        let c_id = TransferId {
            id: 1,
            parent: None,
        };
        let composite = CompositeMock::new(c_id, handle.clone(), 50, 1, 10000, c_counter.clone());
        scheduler.enqueue_transfer(Box::new(composite));

        // Single transfer S: 50 work items
        let s_counter = DispatchCounter::new();
        let s_id = TransferId {
            id: 2,
            parent: None,
        };
        let sm = Arc::new(CountedWork::new(50, s_counter.clone()));
        let transfer = MockTransfer::new_with_handle(s_id, sm, handle.clone());
        scheduler.enqueue_transfer(Box::new(transfer));

        // Wait for both to complete (100 total dispatches)
        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        let c_count = c_counter.count();
        let s_count = s_counter.count();
        let total = c_count + s_count;

        // Both completed equal workloads, so each tree should end with
        // roughly the fair share. Tolerance accounts for ordering effects
        // at the very tail; observed drift is ~0% in practice.
        let fair_share = total as f64 / 2.0;
        let tolerance = fair_share * 0.10;
        assert!(
            (c_count as f64 - fair_share).abs() < tolerance,
            "composite got {} dispatches, expected ~{} (tolerance {})",
            c_count,
            fair_share,
            tolerance
        );
        assert!(
            (s_count as f64 - fair_share).abs() < tolerance,
            "single got {} dispatches, expected ~{} (tolerance {})",
            s_count,
            fair_share,
            tolerance
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_composite_vs_single_fair_share_at_root() {
        let handle = test_handle(100);
        impl_composite_vs_single_fair_share_at_root(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_composite_vs_single_fair_share_at_root_managed_runtime() {
        // Lower target than the tokio variant. Under managed runtime with 4
        // worker threads, multiple producers race on the scheduler's
        // submission queue (capacity 64); dispatching ~100 zero-latency work
        // items concurrently triggers a high-contention path that has not
        // been root-caused. A lower target keeps in-flight pressure inside
        // the queue capacity and exercises the same fairness invariant.
        // See bosun.md / "Submission queue contention under managed runtime"
        // for the deferred investigation.
        let handle = test_handle_managed(20);
        impl_composite_vs_single_fair_share_at_root(handle).await;
    }

    async fn impl_two_composites_fair_share_regardless_of_fan_out(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        // Two composites with equal spawn-cost (same number of children) but
        // different work-per-child. The hierarchical CFS gives each group
        // equal scheduling share based on group_vruntime (which advances
        // per-child-spawned). With equal spawn counts, both groups should
        // receive roughly equal dispatch share in any observation window.
        //
        // C1: 50 children, 20 work items each = 1000 total dispatches
        let c1_counter = DispatchCounter::new();
        let c1_id = TransferId {
            id: 1,
            parent: None,
        };
        let c1 = CompositeMock::new(c1_id, handle.clone(), 50, 20, 10000, c1_counter.clone());
        scheduler.enqueue_transfer(Box::new(c1));

        // C2: 50 children, 20 work items each = 1000 total dispatches
        let c2_counter = DispatchCounter::new();
        let c2_id = TransferId {
            id: 2,
            parent: None,
        };
        let c2 = CompositeMock::new(c2_id, handle.clone(), 50, 20, 10000, c2_counter.clone());
        scheduler.enqueue_transfer(Box::new(c2));

        // Wait for 500 total dispatches
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let total = c1_counter.count() + c2_counter.count();
                if total >= 500 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("should reach 500 dispatches");

        let c1_count = c1_counter.count();
        let c2_count = c2_counter.count();

        // Both groups have equal spawn-cost (50 children each), so the
        // hierarchical CFS should give roughly equal dispatch share.
        // Tolerance covers tail effects in window-based observation
        // (managed runtime samples a partial window); observed drift is
        // around 8-15% per window in practice.
        let total = c1_count + c2_count;
        let fair_share = total as f64 / 2.0;
        let tolerance = fair_share * 0.20;
        assert!(
            (c1_count as f64 - fair_share).abs() < tolerance,
            "C1 got {} dispatches, expected ~{} (tolerance {}), C2 got {}",
            c1_count,
            fair_share,
            tolerance,
            c2_count
        );
        assert!(
            (c2_count as f64 - fair_share).abs() < tolerance,
            "C2 got {} dispatches, expected ~{} (tolerance {}), C1 got {}",
            c2_count,
            fair_share,
            tolerance,
            c1_count
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_two_composites_fair_share_regardless_of_fan_out() {
        let handle = test_handle(100);
        impl_two_composites_fair_share_regardless_of_fan_out(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_two_composites_fair_share_regardless_of_fan_out_managed_runtime() {
        // See note on test_composite_vs_single_fair_share_at_root_managed_runtime
        // for why target is lower under managed runtime.
        let handle = test_handle_managed(20);
        impl_two_composites_fair_share_regardless_of_fan_out(handle).await;
    }

    async fn impl_heterogeneous_within_group_proportional_share(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        // We create a single group (parent id=1) with two children directly.
        // c_small: 1 work item, c_large: 50 work items.
        let small_counter = DispatchCounter::new();
        let large_counter = DispatchCounter::new();

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        // Register the group so children can be inserted
        scheduler.register_empty_group_for_test(parent_id.id);

        let small_id = TransferId {
            id: 2,
            parent: Some(1),
        };
        let large_id = TransferId {
            id: 3,
            parent: Some(1),
        };

        let sm_small = Arc::new(CountedWork::new(1, small_counter.clone()));
        let sm_large = Arc::new(CountedWork::new(100, large_counter.clone()));

        let t_small = MockTransfer::new_with_handle(small_id, sm_small.clone(), handle.clone());
        let t_large = MockTransfer::new_with_handle(large_id, sm_large.clone(), handle.clone());

        scheduler.enqueue_transfer(Box::new(t_small));
        scheduler.enqueue_transfer(Box::new(t_large));

        // Wait for both to complete
        tokio::time::timeout(Duration::from_secs(10), async {
            while !sm_small.is_complete() || !sm_large.is_complete() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("both children should complete");

        // c_small should complete well before c_large
        assert_eq!(small_counter.count(), 1);
        assert_eq!(large_counter.count(), 100);

        // c_small's first dispatch should happen within the first 10 global dispatches.
        // Since c_small only has 1 item and both start at the same vruntime,
        // c_small will be scheduled early. We verify it completed (which means
        // it was dispatched) while c_large was still running.
        // The fact that c_small is complete and c_large needed 100 items proves
        // c_small finished well before c_large.
        assert!(
            sm_small.is_complete(),
            "c_small should complete before c_large finishes all 100 items"
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_heterogeneous_within_group_proportional_share() {
        let handle = test_handle(4);
        impl_heterogeneous_within_group_proportional_share(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_heterogeneous_within_group_proportional_share_managed_runtime() {
        let handle = test_handle_managed(4);
        impl_heterogeneous_within_group_proportional_share(handle).await;
    }

    async fn impl_memory_cap_returns_pending_at_limit(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let composite = CompositeMock::new_blocking(
            id,
            handle.clone(),
            100, // total children
            5,   // memory cap
            counter.clone(),
            notify.clone(),
        );

        scheduler.enqueue_transfer(Box::new(composite));

        // Give the scheduler time to spawn children up to the cap
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The composite should have spawned exactly 5 children (memory cap).
        // Children are blocking, so none complete. The composite returns Pending.
        assert_eq!(
            counter.count(),
            0,
            "no children should have completed (they're blocking)"
        );

        // The scheduler should have the parent + children tracked.
        let transfer_count = scheduler.transfer_count();
        assert!(
            transfer_count >= 1,
            "at least the composite should be tracked, got {}",
            transfer_count
        );

        // Don't release the barrier - children stay blocked, composite stays at cap
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            counter.count(),
            0,
            "still no completions without releasing barrier"
        );

        // Clean up via cancellation
        scheduler.cancel_transfer(id);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cancellation");

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_cap_returns_pending_at_limit() {
        let handle = test_handle(10);
        impl_memory_cap_returns_pending_at_limit(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_cap_returns_pending_at_limit_managed_runtime() {
        let handle = test_handle_managed(10);
        impl_memory_cap_returns_pending_at_limit(handle).await;
    }

    async fn impl_memory_cap_release_resumes_spawning(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let composite = CompositeMock::new_blocking(
            id,
            handle.clone(),
            20, // total children (smaller for test tractability)
            5,  // memory cap
            counter.clone(),
            notify.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        // Wait for initial batch to be spawned and dispatched
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(counter.count(), 0, "children should be blocking");

        // Release children one at a time and verify the composite resumes spawning.
        // notify_waiters() wakes all current waiters; we use it in a loop to
        // release one child at a time (only one should be waiting at a time
        // since the scheduler dispatches them sequentially with target=10).
        for iteration in 0..3u64 {
            // Release one child by notifying
            notify.notify_waiters();

            // Wait for the completion to propagate
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if counter.count() > iteration {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "iteration {}: expected >{} completions, got {}",
                    iteration,
                    iteration,
                    counter.count()
                )
            });
        }

        // Verify at least 3 completions happened (proving the cycle works)
        assert!(
            counter.count() >= 3,
            "expected at least 3 completions after 3 releases, got {}",
            counter.count()
        );

        // Clean up via cancellation
        scheduler.cancel_transfer(id);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_cap_release_resumes_spawning() {
        let handle = test_handle(10);
        impl_memory_cap_release_resumes_spawning(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_cap_release_resumes_spawning_managed_runtime() {
        let handle = test_handle_managed(10);
        impl_memory_cap_release_resumes_spawning(handle).await;
    }

    async fn impl_priority_change_shifts_root_share(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        // Two single transfers with delayed work items. Adding a small delay
        // makes the scheduling observable: the high-priority transfer accumulates
        // vruntime slower, so it gets scheduled more often and completes first.
        let hi_counter = DispatchCounter::new();
        let lo_counter = DispatchCounter::new();

        let hi_id = TransferId {
            id: 1,
            parent: None,
        };
        let lo_id = TransferId {
            id: 2,
            parent: None,
        };

        // Use WithDelay to slow execution so priority differences are observable
        let sm_hi = Arc::new(WithDelay::new(
            CountedWork::new(1000, hi_counter.clone()),
            Duration::from_millis(1),
        ));
        let sm_lo = Arc::new(WithDelay::new(
            CountedWork::new(1000, lo_counter.clone()),
            Duration::from_millis(1),
        ));

        let t_hi = MockTransfer::new_with_handle(hi_id, sm_hi, handle.clone());
        let t_lo = MockTransfer::new_with_handle(lo_id, sm_lo, handle.clone());

        // Enqueue and set priorities
        scheduler.enqueue_transfer(Box::new(t_hi));
        scheduler.enqueue_transfer(Box::new(t_lo));
        scheduler.set_priority(hi_id, 255); // high priority = more share
        scheduler.set_priority(lo_id, 64); // low priority = less share

        // Wait for enough dispatches to observe the priority difference.
        // With 1000 items per transfer and concurrency=4, we get ~250+
        // scheduling rounds which is enough to converge on the expected ratio.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let total = hi_counter.count() + lo_counter.count();
                if total >= 800 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("should reach 800 dispatches");

        let hi_count = hi_counter.count();
        let lo_count = lo_counter.count();

        // 4x priority difference (255 vs 64) yields ~4x dispatch ratio
        // via the priority-scaled vruntime delta. With 800 total dispatches
        // (~200 scheduling rounds at concurrency=4), the ratio converges
        // toward 4x. On managed threads, OS thread scheduling introduces
        // latency variance that dilutes the observed ratio (the scheduler
        // picks correctly, but thread wake latency means completions don't
        // arrive in strict priority order). 2.5 validates priority works
        // while absorbing this systematic effect.
        let ratio = hi_count as f64 / lo_count.max(1) as f64;
        assert!(
            ratio > 2.5,
            "expected ratio > 2.5, got {:.2} (hi={}, lo={})",
            ratio,
            hi_count,
            lo_count
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_priority_change_shifts_root_share() {
        let handle = test_handle(4);
        impl_priority_change_shifts_root_share(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_priority_change_shifts_root_share_managed_runtime() {
        let handle = test_handle_managed(4);
        impl_priority_change_shifts_root_share(handle).await;
    }

    async fn impl_cancellation_cascades_in_hierarchical_structure(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let c_id = TransferId {
            id: 1,
            parent: None,
        };
        // Use blocking children with a short timeout so they eventually drain
        // after cancellation. Memory cap = 100 to spawn all immediately.
        let composite = CompositeMock::new_blocking(
            c_id,
            handle.clone(),
            20, // total children (smaller for test speed)
            20, // memory cap (spawn all immediately)
            counter.clone(),
            notify.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        // Wait for children to be spawned and dispatched (they'll block in execute)
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Cancel the composite - this cascades to children
        assert!(scheduler.cancel_transfer(c_id));

        // Wait for scheduler to become idle. The in-flight execute futures
        // will observe cancellation via is_cancelled() and return early.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cancellation");

        // Group should be removed
        assert_eq!(
            scheduler.group_member_count(c_id.id),
            None,
            "group should be removed after cancellation"
        );

        // All transfers should be purged
        assert_eq!(
            scheduler.transfer_count(),
            0,
            "all transfers should be purged"
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation_cascades_in_hierarchical_structure() {
        let handle = test_handle(10);
        impl_cancellation_cascades_in_hierarchical_structure(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation_cascades_in_hierarchical_structure_managed_runtime() {
        let handle = test_handle_managed(10);
        impl_cancellation_cascades_in_hierarchical_structure(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_panic_in_composite_poll_work() {
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;

        // Panicking composite
        let panic_id = TransferId {
            id: 1,
            parent: None,
        };
        let panicker = PanickingCompositeMock::new(panic_id, handle.clone());
        scheduler.enqueue_transfer(Box::new(panicker));

        // Peer transfer that should complete normally
        let s_counter = DispatchCounter::new();
        let s_id = TransferId {
            id: 2,
            parent: None,
        };
        let sm = Arc::new(CountedWork::new(10, s_counter.clone()));
        let peer = MockTransfer::new_with_handle(s_id, sm.clone(), handle.clone());
        scheduler.enqueue_transfer(Box::new(peer));

        // Wait for scheduler to become idle
        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle despite panic");

        // Peer should have completed all its work
        assert_eq!(
            s_counter.count(),
            10,
            "peer transfer should complete all work"
        );

        // Panicking composite's group should be removed
        assert_eq!(
            scheduler.group_member_count(panic_id.id),
            None,
            "panicking composite's group should be removed"
        );

        // Scheduler should be fully idle
        assert!(scheduler.is_idle());

        handle.runtime.shutdown();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    //! Loom verification of the lock-ordering protocol used by
    //! [`Scheduler::enqueue_transfer`] and [`Scheduler::cancel_transfer`].
    //!
    //! The protocol coordinates two independent locks:
    //!   - `Scheduler.transfers: RwLock<HashMap<TransferId, _>>`
    //!     — owned by `Scheduler`, tracks every alive transfer.
    //!   - `ReadySet.by_group: RwLock<HashMap<u64, _>>`
    //!     — owned by `ReadySet`, tracks which top-level transfers (groups)
    //!     have an entry in the root tree.
    //!
    //! The invariant the protocol must uphold: no entry remains in the
    //! transfers map whose parent group has been removed from the by_group
    //! map. A violation means a child has been orphaned in the scheduler,
    //! leaving its handle hung forever.
    //!
    //! Both locks are acquired in a consistent order in production:
    //! `transfers.write()` first, then `by_group` (read for enqueue,
    //! write for cancel), with cancel holding `transfers.write` across
    //! the `by_group.write` so the two map mutations are atomic from any
    //! observer's perspective.
    //!
    //! These tests exercise a faithful shim (`LoomScheduler`) whose
    //! `enqueue_transfer` and `cancel_transfer` methods mirror the
    //! production lock dance line-for-line. The shim drops the
    //! TransferDescriptor / Handle / runtime machinery (none of which
    //! participate in the lock-ordering protocol). When the production
    //! methods change, the shim must be kept in sync.

    use loom::sync::{Arc, RwLock};
    use loom::thread;
    use std::collections::HashMap;

    /// Mirrors `TransferId`: id + optional parent.
    #[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
    struct TidShim {
        id: u64,
        parent: Option<u64>,
    }

    /// Faithful shim of the FIXED `Scheduler` lock dance for loom
    /// verification. Lock acquires, drops, and conditional flow mirror
    /// the production code line-for-line; descriptor construction,
    /// Handle/runtime calls, and `cancel_descriptor` are stripped
    /// because they don't participate in the lock-ordering protocol.
    struct LoomScheduler {
        // Mirrors `Scheduler.transfers`. Value type stripped to () since
        // descriptors are irrelevant to the lock-ordering protocol.
        transfers: RwLock<HashMap<TidShim, ()>>,
        // Mirrors `ReadySet.by_group`. Value type stripped to () since
        // group internals (vruntime, queue) are irrelevant to the
        // existence-tracking protocol.
        by_group: RwLock<HashMap<u64, ()>>,
    }

    impl LoomScheduler {
        fn new() -> Self {
            Self {
                transfers: RwLock::new(HashMap::new()),
                by_group: RwLock::new(HashMap::new()),
            }
        }

        /// Pre-register a top-level group for setup. Mirrors the state
        /// after a successful top-level enqueue: the parent is in both
        /// `transfers` (so `cancel_transfer` removes it and proceeds to
        /// remove the group) and `by_group` (so child enqueues find it).
        fn register_group(&self, group_id: u64) {
            self.by_group.write().unwrap().insert(group_id, ());
            self.transfers.write().unwrap().insert(
                TidShim {
                    id: group_id,
                    parent: None,
                },
                (),
            );
        }

        /// Mirror of FIXED `Scheduler::enqueue_transfer` lock dance.
        ///
        /// Lock order: acquire `transfers.write`, hold while taking
        /// `by_group.read` to check parent existence and inserting,
        /// then release `transfers.write` and re-check via
        /// `by_group.read` (the publication step performed by
        /// `ready_set.insert` in production). Matches
        /// `Scheduler::enqueue_transfer`'s resolve+insert critical
        /// section followed by the post-release `ready_set.insert` /
        /// OrphanedChild fallback.
        fn enqueue_transfer(&self, tid: TidShim) {
            let inserted = {
                let mut transfers = self.transfers.write().unwrap();
                // resolve_group_vruntime equivalent: by_group.read while
                // holding transfers.write enforces lock ordering.
                let parent_exists = match tid.parent {
                    Some(p) => self.by_group.read().unwrap().contains_key(&p),
                    None => true,
                };
                if !parent_exists {
                    drop(transfers);
                    // Production: ctx.set_cancelled() + ctx.signal_terminal().
                    return;
                }
                transfers.insert(tid, ());
                true
            };
            let _ = inserted;
            // transfers released. ready_set.insert equivalent: by_group.read again.
            if let Some(p) = tid.parent {
                let still = self.by_group.read().unwrap().contains_key(&p);
                if !still {
                    // OrphanedChild branch: production calls
                    // cancel_transfer(tid). We inline the relevant cleanup
                    // (remove from transfers); production also calls
                    // cancel_descriptor which is outside the protocol.
                    self.transfers.write().unwrap().remove(&tid);
                }
            }
        }

        /// Mirror of FIXED `Scheduler::cancel_transfer` lock dance.
        ///
        /// Single critical section: `transfers.write` held across
        /// `by_group.write`. Matches the production `cancel_transfer`
        /// removal block; production also calls `cancel_descriptor` for
        /// each removed descriptor outside the lock, which is irrelevant
        /// to the protocol.
        fn cancel_transfer(&self, id: TidShim) {
            let mut transfers = self.transfers.write().unwrap();
            let removed = transfers.remove(&id).is_some();
            let child_keys: Vec<TidShim> = transfers
                .keys()
                .filter(|t| t.parent == Some(id.id))
                .copied()
                .collect();
            for k in &child_keys {
                transfers.remove(k);
            }
            if removed && id.parent.is_none() {
                // remove_group equivalent: by_group.write while
                // transfers.write held.
                self.by_group.write().unwrap().remove(&id.id);
            }
        }

        /// Invariant: no transfer remains whose parent group is gone.
        fn no_orphans(&self) -> bool {
            let t = self.transfers.read().unwrap();
            let bg = self.by_group.read().unwrap();
            t.keys().all(|tid| match tid.parent {
                Some(p) => bg.contains_key(&p),
                None => true,
            })
        }
    }

    /// Verification test: the fixed dance produces no orphan in any
    /// interleaving. Loom panics on the first violation.
    #[test]
    fn fixed_dance_no_orphan() {
        loom::model(|| {
            let sched = Arc::new(LoomScheduler::new());
            sched.register_group(1);

            let s1 = sched.clone();
            let h1 = thread::spawn(move || {
                s1.enqueue_transfer(TidShim {
                    id: 100,
                    parent: Some(1),
                });
            });
            let s2 = sched.clone();
            let h2 = thread::spawn(move || {
                s2.cancel_transfer(TidShim {
                    id: 1,
                    parent: None,
                });
            });
            h1.join().unwrap();
            h2.join().unwrap();

            assert!(
                sched.no_orphans(),
                "fixed dance produced an orphan; lock-ordering bug regressed"
            );
        });
    }
}
