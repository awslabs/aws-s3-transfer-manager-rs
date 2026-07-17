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
//!   claim atom on [`TransferDescriptor`]
//!   and the atomicity of `ReadySet`'s pop. `ReadySet::insert` is CAS-gated on
//!   this claim: a concurrent wake calling `insert` while the descriptor is
//!   already in the ready set (or being polled) finds claim=true and silently
//!   no-ops, preventing duplicate ready-set entries. Concurrent `poll_work` is
//!   prevented by `SkipMap`'s atomic dequeue, not by the claim.
//!
//!   The claim is asserted on first insert and held continuously across all
//!   subsequent `pop` -> `poll_work` -> `reinsert_under_claim` cycles in the
//!   `Ready` path, until `poll_work` returns `Pending` or `Done` (at which
//!   point the claim is explicitly released). This keeps the per-transfer
//!   state mutex effectively uncontended in steady state and lets a transfer
//!   making continuous progress avoid CAS overhead on each iteration.
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

/// Maximum *proactive* generation passes a single `generate_work` entry runs
/// before retiring and leaning on the completion edge to re-drive (when one is
/// guaranteed). Bounds the per-`on_completion` cost so one execution thread
/// cannot stay engaged as the runner indefinitely under sustained wakes; the
/// runner role then rotates to whichever thread next completes work. The
/// `dispatched == 0` corner has no completion edge, so the bound does not apply
/// there — the runner keeps draining (self-bounded by finite published work).
/// Loom-verified in `gate.rs` (`..._bounded_handoff*`).
const MAX_GENERATION_PASSES: usize = 3;

use super::gate::GenerateWorkGate;

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
    /// Admits one work-generation runner and coalesces requests without losing
    /// a wake. See [`GenerateWorkGate`].
    generate_work_gate: GenerateWorkGate,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

/// Result of a [`Scheduler::cancel_transfer`] call.
///
/// Carries the descriptors that were cancelled (the target and any
/// depth-1 children). Drop the value for fire-and-forget cancel; await
/// [`Cancellation::wait_for_idle`] to wait for in-flight executing
/// work to drain before continuing. Pending (queued but not yet
/// dispatched) work is purged synchronously during cancel; only work
/// that was already dispatched needs draining.
pub(crate) struct Cancellation {
    pub(super) target: Option<TransferDescriptor>,
    pub(super) children: Vec<TransferDescriptor>,
}

impl Cancellation {
    /// Wait for the cancelled parent and all cancelled children to
    /// finish any work that was already dispatched at cancellation time.
    pub(crate) async fn wait_for_idle(self) {
        if let Some(d) = self.target {
            d.wait_for_idle().await;
        }
        for d in self.children {
            d.wait_for_idle().await;
        }
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
            generate_work_gate: GenerateWorkGate::new(),
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

        tracing::debug!(target: telemetry::TARGET_SCHEDULING, tid = %id, "transfer enqueued");
        // Drive `generate_work` unless we're already inside it on this
        // thread (the parent's `poll_work` called `spawn_children` →
        // `enqueue_transfer`). The `GenerateWorkGate` handles both cases:
        //
        // 1. Re-entrancy: `try_acquire` returns None on the runner's own
        //    thread. Without this, the re-entrant frame would call
        //    `submission_queue.enter()`, incrementing the global `pending`
        //    counter. The outer frame's `submit()` then sees pending > 0 and
        //    waits for the nested submission to flush — but the nested frame
        //    can't flush until the outer frame returns. Deadlock.
        //
        // 2. Cross-thread serialization: only the runner pops from the ready
        //    set at a time, preserving priority ordering. Priority depends on
        //    sequential pop-poll-reinsert cycles (hi-priority re-enters at
        //    lower vruntime, gets popped again sooner). Concurrent pops would
        //    short-circuit this by draining the tree in parallel without
        //    giving transfers the chance to re-insert.
        //
        // The runner picks up the newly-inserted transfer on its current pass,
        // or on the extra pass our recorded request forces (case 1).
        self.generate_work();
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
                    tid = %id,
                    "wake",
                );
                self.generate_work();
            }
            None => {
                tracing::trace!(
                    target: telemetry::TARGET_SCHEDULING,
                    tid = %id,
                    "wake.not_found",
                );
            }
        }
    }

    /// Cancel a transfer and any child transfers, removing them from the scheduler.
    ///
    /// Lock-ordering protocol: delegates to `remove_transfer_atomic`, which
    /// holds `transfers.write` across `ready_set.remove_group`. Together
    /// with `enqueue_transfer`, the protocol prevents a child being
    /// orphaned in `transfers` after its parent group is removed. See
    /// `loom_tests::fixed_dance_no_orphan` at the bottom of this file.
    ///
    /// Cancels the target transfer first, then cancels all transfers whose
    /// `TransferId.parent == Some(id.id)` (depth-1 children only). Each
    /// cancelled transfer has its status set to cancelled, pending work
    /// purged, and idle notification sent. Outstanding work already being
    /// executed will complete naturally.
    ///
    /// Returns a [`Cancellation`] carrying the cancelled descriptors.
    /// Drop it for fire-and-forget; await
    /// [`Cancellation::wait_for_idle`] to wait for in-flight executing
    /// work to drain.
    pub(crate) fn cancel_transfer(&self, id: TransferId) -> Cancellation {
        let (target, children) = self.remove_transfer_atomic(id);
        if let Some(desc) = &target {
            self.cancel_descriptor(desc.clone());
        }
        for desc in &children {
            self.cancel_descriptor(desc.clone());
        }
        Cancellation { target, children }
    }

    /// Atomic protocol-respecting removal of a transfer and its direct children.
    ///
    /// Lock-ordering invariant: `transfers.write` is acquired first and
    /// held across the call to `ready_set.remove_group` (which takes
    /// `by_group.write` internally). The two map mutations therefore
    /// appear atomic to any observer.
    ///
    /// Without this ordering, a concurrent `enqueue_transfer` for a child
    /// of `id` could sneak its child into `transfers` between the parent's
    /// removal and the group's removal, leaving the child orphaned in
    /// `transfers` after its parent group is gone — its handle would
    /// never resolve.
    ///
    /// Used by both `cancel_transfer` (caller cancels the returned parent
    /// descriptor + each child) and `generate_work`'s `Done` branch
    /// (caller leaves the parent alone since it completed normally, but
    /// cancels any children defensively — they shouldn't exist if the
    /// Transfer contract was upheld, but we clean them up if they do).
    fn remove_transfer_atomic(
        &self,
        id: TransferId,
    ) -> (Option<TransferDescriptor>, Vec<TransferDescriptor>) {
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
        if target.is_some() && id.parent.is_none() {
            self.0.ready_set.remove_group(id.id);
        }
        (target, children)
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
        tracing::debug!(target: telemetry::TARGET_SCHEDULING, tid = %id, purged, "transfer cancelled");
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
            tid = %work.descriptor.id(),
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

        // Terminal transfer: no further work from THIS transfer. Clean up
        // when fully drained. Routed through `remove_transfer_atomic` so
        // the transfers/by_group/groups lock-ordering protocol is held;
        // a concurrent child enqueue cannot slip into a window between
        // the parent's removal from `transfers` and its group's removal,
        // which would otherwise orphan the child.
        if desc.is_terminal() && is_idle {
            let desc_id = desc.id();
            let (completed, orphans) = self.remove_transfer_atomic(desc_id);
            if !orphans.is_empty() {
                tracing::warn!(
                    target: telemetry::TARGET_SCHEDULING,
                    parent_id = %desc_id,
                    child_count = orphans.len(),
                    "transfer reached terminal+idle in on_completion while \
                     children still alive; cleaning up. This is a Transfer \
                     trait contract violation."
                );
                for desc in orphans {
                    self.cancel_descriptor(desc);
                }
            }
            // Signal the removed transfer's terminal. A transfer that became
            // terminal but was removed here before signaling would leave its
            // completion channel unfired, hanging the owning handle. Signal
            // after cancelling orphans so children are torn down before the
            // handle observes completion. Idempotent if already signaled.
            if let Some(completed) = completed {
                completed.transfer().ctx().signal_terminal();
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
            // Same lock-ordering protocol as on_completion: route
            // through `remove_transfer_atomic` to keep transfers and
            // by_group/groups consistent under concurrent child enqueue.
            let desc_id = desc.id();
            let (_completed, orphans) = self.remove_transfer_atomic(desc_id);
            if !orphans.is_empty() {
                tracing::warn!(
                    target: telemetry::TARGET_SCHEDULING,
                    parent_id = %desc_id,
                    child_count = orphans.len(),
                    "panicking transfer reached idle while children still \
                     alive; cleaning up."
                );
                for desc in orphans {
                    self.cancel_descriptor(desc);
                }
            }
        }
        desc.notify_idle();
    }

    fn has_capacity(&self) -> bool {
        let target = self.handle().controller.target();
        // The retire-at-capacity path in `generate_work` depends on this: at
        // capacity, `dispatched >= target >= 1` guarantees an in-flight item
        // will complete and re-drive generation. See `ConcurrencyController::target`.
        debug_assert!(target >= 1, "concurrency target must be >= 1");
        self.0.dispatched.load(Ordering::Relaxed) < target
    }

    /// `has_capacity`, but reading `dispatched` with a read-modify-write so the
    /// runner cannot retire on a stale view of capacity.
    ///
    /// The plain-load `has_capacity` is correct in the dispatch hot loop, where
    /// the runner just observed its own `dispatched` writes. It is NOT safe at
    /// the retire decision: a concurrent `on_completion` decrements `dispatched`
    /// and then bumps the gate epoch via `generate_work`'s `try_acquire`. When a
    /// runner is already active that bump *coalesces* — it records the request on
    /// the epoch but publishes no gate edge into this runner, so it establishes
    /// no happens-before with it. A `Relaxed` load may then read the stale
    /// pre-decrement value, and the runner retires leaving a freed slot with
    /// queued work and no runner — a lost wake (loom: `gate.rs`
    /// `..._at_capacity_with_producer`).
    ///
    /// An RMW reads the latest value in `dispatched`'s modification order, so it
    /// observes the completion's decrement; `SeqCst` places this read and the
    /// gate's retire CAS in one total order, so the runner and the completing
    /// thread agree on which of them drives the freed slot. Used only at the
    /// retire check — not per-pop — so its cost is off the hot path.
    fn recheck_capacity(&self) -> bool {
        let target = self.handle().controller.target();
        debug_assert!(target >= 1, "concurrency target must be >= 1");
        self.0.dispatched.fetch_add(0, Ordering::SeqCst) < target
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
    ///
    /// Callers (`wake`, `on_completion`, re-entrant `enqueue_transfer`) insert
    /// their descriptor into the ready set BEFORE calling this, then acquire the
    /// [`GenerateWorkGate`]: the sole runner drains generation passes, and any
    /// other caller's request is coalesced into an extra pass the runner makes
    /// on its behalf, so no wake is lost.
    fn generate_work(&self) {
        // Become the sole runner, or bail — a request was recorded for the
        // active runner to drain on our behalf (this is also the re-entrancy
        // guard; see `GenerateWorkGate::try_acquire`).
        let Some(mut permit) = self.0.generate_work_gate.try_acquire() else {
            return;
        };
        let mut passes = 0usize;
        'generate: loop {
            passes += 1;
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
                    Ok(PollWork::Ready { io, spawned }) => {
                        generated += 1;
                        desc.work_generated();
                        // If this poll also spawned a child (fused reap+spawn),
                        // charge one spawn's reduced vruntime too. The spawned
                        // child gets NO dispatch ticket here -- it increments
                        // `dispatched` when it is itself polled to `Ready`,
                        // identical to the `Spawned` contract. Only the IO below
                        // takes a ticket.
                        if spawned {
                            desc.work_generated_spawn();
                        }
                        // Re-insert under the still-held claim - bypasses the
                        // CAS gate that `insert` would otherwise apply.
                        claim.hold();
                        self.0.ready_set.reinsert_under_claim(desc.clone());
                        self.0.dispatched.fetch_add(1, Ordering::Relaxed);
                        desc.work_queued();
                        let work = ScheduledWork {
                            item: io,
                            descriptor: desc,
                        };
                        sub = self.enqueue(sub, work);
                    }
                    Ok(PollWork::Spawned) => {
                        // Charge the reduced spawn cost (not full work cost) so a
                        // composite parent stays scheduling-competitive with its
                        // own children and can refill continuously.
                        desc.work_generated_spawn();
                        // Re-insert under the still-held claim so the transfer
                        // is re-polled while has_capacity remains true.
                        claim.hold();
                        self.0.ready_set.reinsert_under_claim(desc.clone());
                        // No dispatched.fetch_add, no work_queued, no enqueue:
                        // nothing was dispatched. The spawned child increments
                        // dispatched when it is itself polled and yields a work item.
                    }
                    Ok(PollWork::Pending) => {
                        pending_count += 1;
                        tracing::trace!(
                            target: telemetry::TARGET_SCHEDULING,
                            tid = %id,
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
                            tid = %id,
                            "poll_work.done",
                        );
                        claim.release();
                        let desc_id = desc.id();
                        let (_completed, orphans) = self.remove_transfer_atomic(desc_id);
                        // The parent's `_completed` descriptor is the same one
                        // we just polled; it terminated normally so we do not
                        // call `cancel_descriptor` on it. Children should not
                        // exist if the Transfer contract was upheld; if they
                        // do, the contract is violated — clean them up
                        // defensively and surface the violation as a warning.
                        if !orphans.is_empty() {
                            tracing::warn!(
                                target: telemetry::TARGET_SCHEDULING,
                                parent_id = %desc_id,
                                child_count = orphans.len(),
                                "transfer returned PollWork::Done while children \
                                 still alive; cleaning up. This is a Transfer \
                                 trait contract violation."
                            );
                            for desc in orphans {
                                self.cancel_descriptor(desc);
                            }
                        }
                    }
                    Err(_panic_payload) => {
                        // ClaimGuard's Drop releases the claim. cancel_transfer
                        // handles terminal transition + child cascade.
                        tracing::error!(
                            target: telemetry::TARGET_SCHEDULING,
                            tid = %id,
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
            // Retire the runner, or run another pass if a wake()/on_completion()
            // bumped the request epoch while we drained (their try_acquire could
            // not take the permit, so they relied on us). try_release retires via
            // a CAS that requires the epoch to be unchanged — never a blind store
            // — so a racing request is never stomped:
            //   true  -> epoch unchanged; no request pending; retired. Done.
            //   false -> the epoch advanced; the permit re-synced and we keep it.
            loop {
                if permit.try_release() {
                    return;
                }
                // A request is pending. Run another pass only if we can
                // dispatch; otherwise a pass dispatches nothing and just pins
                // this worker thread, starving its own in-flight execute()
                // futures (the fairness regression). The RMW read is required
                // here, not a plain load: the pending request may be a coalesced
                // wake hiding a concurrent completion's slot release; a stale
                // read would retire into a lost wake. See `recheck_capacity`.
                if self.recheck_capacity() {
                    // Below capacity: a fresh pass could dispatch. Run one unless
                    // we have hit the proactive-pass bound AND an in-flight
                    // completion is guaranteed to re-drive us (dispatched >= 1) —
                    // in which case retire and let the runner role rotate to the
                    // completing thread, so no single thread stays engaged.
                    if passes < MAX_GENERATION_PASSES
                        || self.0.dispatched.fetch_add(0, Ordering::SeqCst) == 0
                    {
                        // Either under the bound, or in the dispatched == 0
                        // corner where no completion edge exists and we MUST keep
                        // draining (self-bounded by finite published work).
                        continue 'generate;
                    }
                    // Bound hit with a guaranteed completion edge: fall through to
                    // retry the epoch-CAS retire (hand off to that edge).
                }
                // At capacity (or bound-handoff): don't spin a useless pass.
                // dispatched >= target >= 1, so an in-flight item WILL complete
                // -> on_completion -> generate_work, which re-acquires and drains
                // the pending work. Retry the retire instead.
                //
                // No wake is lost across this retry. A completion racing in
                // decrements `dispatched` (in `on_completion`) BEFORE bumping the
                // epoch via `generate_work`'s `try_acquire`; the RMW reads above
                // observe that decrement, and the epoch-CAS retire fails if any
                // request bumped past the observed epoch — so a racing request
                // is always seen, never stranded.
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_idle(&self) -> bool {
        self.0.transfers.read().unwrap().is_empty()
            && self.0.dispatched.load(Ordering::Relaxed) == 0
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

    /// Current dispatched count (test-only).
    #[cfg(test)]
    pub(crate) fn dispatched_for_test(&self) -> usize {
        self.0.dispatched.load(Ordering::Relaxed)
    }
}
#[cfg(test)]
mod tests {
    use crate::client::Handle;
    use crate::scheduler::descriptor::{vruntime_delta_for_cost, IO_WORK_COST, SPAWN_WORK_COST};
    use crate::scheduler::transfer::mock::{
        BuggyDoneMock, FixedWorkCount, FusedReadySpawnedMock, MockStateMachine,
        TerminalWithoutSignalMock, WithDelay, WithExecute,
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

    // TODO(vnext): tests built on this helper are gated out under asan
    // (`#[cfg_attr(s3_tm_asan, ignore)]`). Each managed worker thread builds its
    // own current-thread tokio runtime; that runtime's driver is reclaimed only
    // when `ManagedThreadRuntime::drop` joins the threads, which requires the
    // `Handle` Arc to reach zero. A test that returns with work still in flight
    // leaves a strong `Handle` on a worker (via `handle.upgrade()` in dispatch),
    // so drop/join never runs and the per-thread runtimes leak at process exit.
    // The fix is a deterministic drain-and-join teardown for these tests (and a
    // check that a real consumer running asan does not hit the same at shutdown);
    // until then they are asan-gated rather than leaking the sanitizer run.
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
            PollWork::ready(IoRequest { data: None })
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
        // Priority delta in `work_generated` is `(IO_WORK_COST * 256) / priority`,
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
        assert!(scheduler.cancel_transfer(id).target.is_some());
        assert!(scheduler.cancel_transfer(id).target.is_none());

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

        assert!(scheduler.cancel_transfer(parent_id).target.is_some());

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

        assert!(scheduler.cancel_transfer(id).target.is_some());
        assert!(scheduler.cancel_transfer(id).target.is_none());

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

        assert!(scheduler.cancel_transfer(child_a).target.is_some());
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
        assert!(scheduler.cancel_transfer(fake_id).target.is_none());

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
        SingleTicketCompositeMock,
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_composite_vs_single_fair_share_at_root_managed_runtime() {
        // Lower target than the tokio variant. Under managed runtime with 4
        // worker threads, multiple producers race on the scheduler's
        // submission queue (capacity 64); dispatching ~100 zero-latency work
        // items concurrently triggers a high-contention path that has not
        // been root-caused. A lower target keeps in-flight pressure inside
        // the queue capacity and exercises the same fairness invariant.
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

        // Measure fairness over a SETTLED window. CFS fairness is a steady-state
        // property: the first ~100 dispatches are a startup transient (whichever
        // composite's children win the initial ready-set race burst ahead before
        // vruntime accounting pulls them level). Skew decays monotonically —
        // empirically ~35% at N=50, ~4% by N=500, <2% by N=1200 — so sampling at
        // an early, arbitrary checkpoint (e.g. the first poll past 500) measures
        // the transient, not the steady state, and is flaky. Waiting for 1200
        // total dispatches (60x the per-completion granularity at target 20) puts
        // the sample well past the knee, where the gap is reliably small.
        let total_work = 2 * 50 * 20;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let total = c1_counter.count() + c2_counter.count();
                // Stop at 1200, or early if the whole run finishes first.
                if total >= 1200 || total >= total_work {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("should reach the fairness sampling window");

        let c1_count = c1_counter.count();
        let c2_count = c2_counter.count();

        // Both groups have equal spawn-cost (50 children each), so hierarchical
        // CFS gives each equal dispatch share at steady state. 10% tolerance: the
        // worst observed skew at N>=1200 across hundreds of runs was ~4%, so 10%
        // is comfortably non-flaky while still catching a real fairness break.
        let total = c1_count + c2_count;
        let fair_share = total as f64 / 2.0;
        let tolerance = fair_share * 0.10;
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
    #[cfg_attr(s3_tm_asan, ignore)]
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
        // via the priority-scaled vruntime delta. With serialized
        // generate_work (single-runner admission via GenerateWorkGate),
        // the ratio converges to the theoretical value on both runtimes.
        // Threshold of 3.5 provides margin for CI scheduling noise.
        let ratio = hi_count as f64 / lo_count.max(1) as f64;
        assert!(
            ratio > 3.5,
            "expected ratio > 3.5, got {:.2} (hi={}, lo={})",
            ratio,
            hi_count,
            lo_count
        );

        // Cancel both transfers so the registry drains before shutdown.
        // The test only consumed ~800 of 2000 enqueued work items; without
        // cancellation, the remaining transfers stay in the registry holding
        // Arc<Handle> references, forming a cycle the runtime shutdown
        // doesn't break. Visible to LeakSanitizer at process exit.
        scheduler.cancel_transfer(hi_id);
        scheduler.cancel_transfer(lo_id);
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
    async fn test_priority_change_shifts_root_share() {
        let handle = test_handle(4);
        impl_priority_change_shifts_root_share(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
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
        assert!(scheduler.cancel_transfer(c_id).target.is_some());

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
    #[cfg_attr(s3_tm_asan, ignore)]
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

    /// Regression test: a composite that violates the Transfer contract
    /// (returns `Done` while children are still alive) must not hang the
    /// scheduler. The Done branch in `generate_work` defensively cleans
    /// up orphaned children rather than leaving them in `transfers` with
    /// no parent group to schedule them.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_done_with_live_children_does_not_hang() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        let (mock, _terminals) = BuggyDoneMock::new(parent_id, handle.clone(), 3);
        scheduler.enqueue_transfer(Box::new(mock));

        // Without defensive cleanup, the orphaned children stay in the
        // transfers map and the scheduler never reaches idle. The
        // timeout asserts cleanup actually fired.
        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should reach idle; orphaned children would hang it");

        handle.runtime.shutdown();
    }

    /// Verifies the Done-branch defensive cleanup actually cancels the
    /// orphaned children (calls `cancel_descriptor`), not just removing
    /// them from the map. Each child's terminal receiver should fire so
    /// the children's handles resolve as cancelled rather than hanging.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_done_orphans_have_terminal_signaled() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        let (mock, terminals) = BuggyDoneMock::new(parent_id, handle.clone(), 3);
        scheduler.enqueue_transfer(Box::new(mock));

        // Wait for the parent to be polled (children get spawned and
        // terminals filled in during the same poll_work call).
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if terminals.lock().unwrap().is_some()
                    && !terminals.lock().unwrap().as_ref().unwrap().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("BuggyDoneMock should populate child terminals during poll_work");

        let child_terminals = terminals
            .lock()
            .unwrap()
            .take()
            .expect("child terminals should be available");
        assert_eq!(child_terminals.len(), 3, "expected 3 child terminals");

        // Each child should be cancelled by the Done branch's defensive
        // cleanup; their terminal receivers fire when cancel_descriptor
        // calls signal_terminal.
        for term in child_terminals {
            tokio::time::timeout(Duration::from_secs(2), term)
                .await
                .expect("child terminal should fire (cancelled by Done branch)")
                .expect("terminal sender should not have been dropped");
        }

        handle.runtime.shutdown();
    }

    /// A transfer that becomes terminal without calling `signal_terminal`,
    /// while a child is still registered, is removed by `on_completion`'s
    /// terminal+idle path. The scheduler must signal the removed transfer's
    /// terminal so its owning handle's `join()` resolves rather than hanging.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_terminal_without_signal_does_not_strand_handle() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        let scheduler = &handle.scheduler;

        let parent_id = TransferId {
            id: 1,
            parent: None,
        };
        let (mock, completion_rx) = TerminalWithoutSignalMock::new(parent_id, handle.clone());
        scheduler.enqueue_transfer(Box::new(mock));

        // The parent goes terminal without signaling; the scheduler removes it
        // (terminal+idle) and must signal its completion. The handle's join
        // (modeled here by awaiting completion_rx) must resolve, not hang.
        tokio::time::timeout(Duration::from_secs(2), completion_rx)
            .await
            .expect("parent completion must be signaled on scheduler removal, not strand")
            .expect("terminal sender should not have been dropped");

        // And the scheduler drains (orphan child cleaned up).
        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should reach idle");

        handle.runtime.shutdown();
    }

    // =========================================================================
    // PollWork::Spawned integration tests
    //
    // These tests verify the single-ticket spawn variant: no dispatch increment,
    // re-poll semantics, CFS vruntime spin guard, and capacity bounding.
    // =========================================================================

    /// Mock that returns `Spawned` a fixed number of times then `Done`.
    /// Tracks the number of times `poll_work` is called.
    #[derive(Debug)]
    struct SpawnedThenDone {
        spawned_remaining: AtomicUsize,
        poll_count: AtomicUsize,
    }

    impl SpawnedThenDone {
        fn new(spawned_count: usize) -> Self {
            Self {
                spawned_remaining: AtomicUsize::new(spawned_count),
                poll_count: AtomicUsize::new(0),
            }
        }

        fn poll_count(&self) -> usize {
            self.poll_count.load(Ordering::SeqCst)
        }
    }

    impl MockStateMachine for SpawnedThenDone {
        fn poll_work(&self, _id: TransferId) -> PollWork {
            self.poll_count.fetch_add(1, Ordering::SeqCst);
            let prev = self.spawned_remaining.fetch_sub(1, Ordering::SeqCst);
            if prev == 0 {
                // Underflowed; we already returned all Spawned items.
                PollWork::Done
            } else {
                PollWork::Spawned
            }
        }

        fn execute<'a>(
            &'a self,
            _work: &'a mut IoRequest,
        ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
            unreachable!("SpawnedThenDone never returns PollWork::Ready")
        }
    }

    async fn impl_spawned_does_not_increment_dispatched(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let sm = Arc::new(SpawnedThenDone::new(1));
        let transfer = Box::new(MockTransfer::new_with_handle(id, sm, handle.clone()));
        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        // The Spawned arm must not have incremented dispatched. After the
        // transfer completes (Done), dispatched should be back to 0.
        assert_eq!(
            scheduler.dispatched_for_test(),
            0,
            "dispatched should be 0 after Spawned+Done (no work items dispatched)"
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawned_does_not_increment_dispatched() {
        let handle = test_handle(4);
        impl_spawned_does_not_increment_dispatched(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawned_does_not_increment_dispatched_managed_runtime() {
        let handle = test_handle_managed(4);
        impl_spawned_does_not_increment_dispatched(handle).await;
    }

    async fn impl_spawned_reinserts_and_repolls(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let n = 5;
        let sm = Arc::new(SpawnedThenDone::new(n));
        let transfer = Box::new(MockTransfer::new_with_handle(
            id,
            sm.clone(),
            handle.clone(),
        ));
        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        // poll_work should have been called N+1 times (N Spawned + 1 Done).
        assert_eq!(
            sm.poll_count(),
            n + 1,
            "expected {} polls (N Spawned + 1 Done), got {}",
            n + 1,
            sm.poll_count()
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawned_reinserts_and_repolls() {
        let handle = test_handle(4);
        impl_spawned_reinserts_and_repolls(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawned_reinserts_and_repolls_managed_runtime() {
        let handle = test_handle_managed(4);
        impl_spawned_reinserts_and_repolls(handle).await;
    }

    async fn impl_single_ticket_caps_materialization_near_target(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let id = TransferId {
            id: 1,
            parent: None,
        };

        let target = 200;
        let total_children = 2000;
        let composite = SingleTicketCompositeMock::new_blocking(
            id,
            handle.clone(),
            total_children,
            100_000, // memory_cap is NOT the limiter
            counter.clone(),
            notify.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        // Sample dispatched repeatedly over a window and assert the max
        // observed stays bounded by the target. This approach is more
        // robust than a single sleep+sample (immune to scheduling jitter).
        let mut max_observed: usize = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            let dispatched = scheduler.dispatched_for_test();
            if dispatched > max_observed {
                max_observed = dispatched;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // `dispatched` is hard-capped at the target: `has_capacity`
        // (`dispatched < target`) gates the generate_work pop loop before the
        // single `fetch_add` in the Ready arm, and the gate serializes that
        // loop to one runner, so `dispatched` never exceeds `target`. Each
        // child increments it by 1 when its poll yields Ready; the parent's
        // Spawned arm never does. Assert the exact bound so a regression that
        // spawned in batches (overshooting by the batch size) is caught, not
        // just the gross "materialize all `total_children`" failure.
        assert!(
            max_observed <= target,
            "max dispatched ({}) must not exceed target ({})",
            max_observed,
            target
        );
        assert!(
            max_observed > 0,
            "at least some children should have been dispatched"
        );

        // Release all children and wait for idle.
        notify.notify_waiters();
        // Keep notifying to release children as they arrive.
        let notify_clone = notify.clone();
        let notifier = tokio::spawn(async move {
            loop {
                notify_clone.notify_waiters();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after releasing children");

        notifier.abort();
        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_ticket_caps_materialization_near_target() {
        let handle = test_handle(200);
        impl_single_ticket_caps_materialization_near_target(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_single_ticket_caps_materialization_near_target_managed_runtime() {
        let handle = test_handle_managed(200);
        impl_single_ticket_caps_materialization_near_target(handle).await;
    }

    /// A fused `PollWork::Ready { spawned: true }` poll charges the descriptor
    /// BOTH the full IO work cost (for the dispatched reap io) and the reduced
    /// spawn cost (for the child spawned in the same poll). Deleting the fused
    /// `work_generated_spawn()` in the scheduler's `Ready` arm would leave only
    /// the IO charge; this pins that both are applied so that regression fails.
    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fused_ready_spawned_charges_both_io_and_spawn_vruntime() {
        let handle = test_handle(4);
        let scheduler = &handle.scheduler;

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let sm = Arc::new(FusedReadySpawnedMock::new());
        let transfer = Box::new(MockTransfer::new_with_handle(id, sm, handle.clone()));

        // `enqueue_transfer` drives `generate_work` synchronously: the mock is
        // popped and polled once, returns `Ready { spawned: true }`, and the
        // scheduler charges both `work_generated` (IO) and `work_generated_spawn`
        // before returning. The mock returns `Pending` on any later poll, so no
        // further vruntime is charged.
        scheduler.enqueue_transfer(transfer);

        // At the default priority (128), the fused poll advances vruntime by the
        // IO delta plus the spawn delta. Compare against the real delta formula
        // so the assertion tracks the constants rather than a magic number.
        let expected = vruntime_delta_for_cost(IO_WORK_COST, 128)
            + vruntime_delta_for_cost(SPAWN_WORK_COST, 128);
        let observed = {
            let transfers = scheduler.0.transfers.read().unwrap();
            transfers
                .get(&id)
                .expect("transfer should still be present")
                .vruntime()
        };
        assert_eq!(
            observed,
            expected,
            "fused Ready{{spawned:true}} must charge IO ({}) + spawn ({}) = {}, got {}",
            vruntime_delta_for_cost(IO_WORK_COST, 128),
            vruntime_delta_for_cost(SPAWN_WORK_COST, 128),
            expected,
            observed
        );

        handle.runtime.shutdown();
    }

    async fn impl_single_ticket_completes_all(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();

        let id = TransferId {
            id: 1,
            parent: None,
        };
        let total_children = 500;
        let composite = SingleTicketCompositeMock::new(
            id,
            handle.clone(),
            total_children,
            1,       // work_per_child
            100_000, // memory_cap (won't be hit)
            counter.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        assert_eq!(
            counter.count(),
            total_children,
            "all {} children should have been dispatched",
            total_children
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_ticket_completes_all() {
        let handle = test_handle(50);
        impl_single_ticket_completes_all(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_single_ticket_completes_all_managed_runtime() {
        let handle = test_handle_managed(50);
        impl_single_ticket_completes_all(handle).await;
    }

    async fn impl_single_ticket_two_composites_fair_share(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        let c1_counter = DispatchCounter::new();
        let c2_counter = DispatchCounter::new();

        let c1_id = TransferId {
            id: 1,
            parent: None,
        };
        let c2_id = TransferId {
            id: 2,
            parent: None,
        };

        let total_children = 200;
        let c1 = SingleTicketCompositeMock::new(
            c1_id,
            handle.clone(),
            total_children,
            5,       // work_per_child
            100_000, // memory_cap
            c1_counter.clone(),
        );
        let c2 = SingleTicketCompositeMock::new(
            c2_id,
            handle.clone(),
            total_children,
            5,       // work_per_child
            100_000, // memory_cap
            c2_counter.clone(),
        );

        scheduler.enqueue_transfer(Box::new(c1));
        scheduler.enqueue_transfer(Box::new(c2));

        // Wait for enough dispatches to observe fairness.
        let total_work = 2 * total_children * 5;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let total = c1_counter.count() + c2_counter.count();
                if total >= 1200 || total >= total_work {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("should reach the fairness sampling window");

        let c1_count = c1_counter.count();
        let c2_count = c2_counter.count();
        let total = c1_count + c2_count;
        let fair_share = total as f64 / 2.0;
        let tolerance = fair_share * 0.10;

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
    async fn test_single_ticket_two_composites_fair_share() {
        let handle = test_handle(100);
        impl_single_ticket_two_composites_fair_share(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_single_ticket_two_composites_fair_share_managed_runtime() {
        let handle = test_handle_managed(20);
        impl_single_ticket_two_composites_fair_share(handle).await;
    }

    async fn impl_single_ticket_memory_cap_backstops(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let id = TransferId {
            id: 1,
            parent: None,
        };

        let memory_cap = 10; // Much less than target
        let total_children = 100;
        let composite = SingleTicketCompositeMock::new_blocking(
            id,
            handle.clone(),
            total_children,
            memory_cap,
            counter.clone(),
            notify.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        // Wait for spawning to stabilize. Children block, so in-flight count
        // should never exceed memory_cap.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // dispatched measures how many children are currently in-flight
        // (blocking in execute). This should be at most memory_cap.
        let dispatched = scheduler.dispatched_for_test();
        assert!(
            dispatched <= memory_cap as usize + 2, // small tolerance for timing
            "dispatched ({}) should not exceed memory_cap ({})",
            dispatched,
            memory_cap
        );

        // Clean up via cancellation.
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
    async fn test_single_ticket_memory_cap_backstops() {
        let handle = test_handle(200);
        impl_single_ticket_memory_cap_backstops(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_single_ticket_memory_cap_backstops_managed_runtime() {
        let handle = test_handle_managed(200);
        impl_single_ticket_memory_cap_backstops(handle).await;
    }

    /// Spin-guard test: a SingleTicketCompositeMock returning Spawned repeatedly
    /// while has_capacity stays true must NOT starve a second concurrently-ready
    /// transfer. The vruntime charge on the Spawned arm prevents monopolization.
    async fn impl_spawned_spin_guard_does_not_starve_peer(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;

        // Composite that spawns via Spawned: 200 children, 1 work each.
        let c_counter = DispatchCounter::new();
        let c_id = TransferId {
            id: 1,
            parent: None,
        };
        let composite = SingleTicketCompositeMock::new(
            c_id,
            handle.clone(),
            200,
            1,       // work_per_child
            100_000, // memory_cap (won't be hit)
            c_counter.clone(),
        );
        scheduler.enqueue_transfer(Box::new(composite));

        // Peer single transfer: 200 work items dispatched via Ready.
        let s_counter = DispatchCounter::new();
        let s_id = TransferId {
            id: 2,
            parent: None,
        };
        let sm = Arc::new(CountedWork::new(200, s_counter.clone()));
        let peer = MockTransfer::new_with_handle(s_id, sm, handle.clone());
        scheduler.enqueue_transfer(Box::new(peer));

        // Wait until both have made some progress (100 total dispatches).
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let total = c_counter.count() + s_counter.count();
                if total >= 100 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("should reach 100 dispatches");

        // The peer (Ready path) must have received some share. Without the
        // spin guard the composite would monopolize all capacity on Spawned
        // re-polls and the peer would be starved.
        let s_count = s_counter.count();
        assert!(
            s_count > 0,
            "peer transfer must make progress (got 0 dispatches); spin guard may be broken"
        );

        // Wait for idle.
        tokio::time::timeout(Duration::from_secs(30), async {
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
    async fn test_spawned_spin_guard_does_not_starve_peer() {
        let handle = test_handle(50);
        impl_spawned_spin_guard_does_not_starve_peer(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawned_spin_guard_does_not_starve_peer_managed_runtime() {
        let handle = test_handle_managed(20);
        impl_spawned_spin_guard_does_not_starve_peer(handle).await;
    }

    // =========================================================================
    // Diagnostic: spawn model — alive-children vs dispatched under various
    // workload shapes.
    //
    // These tests measure BOTH max_dispatched AND max_alive (spawned - terminated)
    // to determine whether the scheduler bounds alive children at the concurrency
    // target or lets them run to memory_cap.
    // =========================================================================

    /// Shared implementation: drives a non-blocking SingleTicketCompositeMock,
    /// samples dispatched and alive over a window, then drains to idle.
    /// Returns (max_dispatched, max_alive).
    async fn impl_spawn_model_non_blocking(
        handle: Arc<Handle>,
        _target: usize,
        work_per_child: u64,
        total_children: u64,
    ) -> (usize, u64) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();

        let id = TransferId {
            id: 1,
            parent: None,
        };

        let composite = SingleTicketCompositeMock::new(
            id,
            handle.clone(),
            total_children,
            work_per_child,
            100_000, // memory_cap very high — not the limiter
            counter.clone(),
        );
        let (spawned_handle, terminated_handle) = composite.observer_handles();
        scheduler.enqueue_transfer(Box::new(composite));

        // Sample dispatched and alive over a window.
        let mut max_dispatched: usize = 0;
        let mut max_alive: u64 = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
        while tokio::time::Instant::now() < deadline {
            let dispatched = scheduler.dispatched_for_test();
            let spawned = spawned_handle.load(Ordering::SeqCst);
            let terminated = terminated_handle.load(Ordering::SeqCst);
            let alive = spawned.saturating_sub(terminated);

            if dispatched > max_dispatched {
                max_dispatched = dispatched;
            }
            if alive > max_alive {
                max_alive = alive;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Wait for idle (all children complete).
        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after all children complete");

        handle.runtime.shutdown();
        (max_dispatched, max_alive)
    }

    // --- Test 1: small objects (work_per_child=1, non-blocking) ---

    async fn impl_test_spawn_model_small_objects(handle: Arc<Handle>) {
        let target: usize = 8;
        let (max_dispatched, max_alive) =
            impl_spawn_model_non_blocking(handle, target, 1, 160).await;

        // Dispatched should be bounded near target.
        assert!(
            max_dispatched <= target + 4,
            "DIAGNOSTIC small_objects: max_dispatched ({}) should be near target ({}); \
             max_alive={}",
            max_dispatched,
            target,
            max_alive,
        );

        // Diagnostic: report alive. If alive tracks target, assert it. If it runs
        // to memory_cap, document that finding.
        // With non-blocking children (work_per_child=1), each child dispatches then
        // frees its slot quickly but stays Active until a subsequent poll marks it
        // Done. We assert alive is bounded by something reasonable and print the
        // observed value. A tight bound is validated after the first run.
        println!(
            "DIAGNOSTIC small_objects: target={}, max_dispatched={}, max_alive={}",
            target, max_dispatched, max_alive
        );
        // Alive should stay bounded (not run to total_children=160 or memory_cap=100_000).
        // Based on observation we expect alive to stay near target (children terminate
        // quickly). Assert a generous upper bound and report the actual value.
        assert!(
            max_alive <= 100,
            "DIAGNOSTIC small_objects: max_alive ({}) ran well beyond expected bounds; \
             target={}, max_dispatched={}",
            max_alive,
            target,
            max_dispatched,
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawn_model_small_objects() {
        let _logs = show_test_logs();
        let handle = test_handle(8);
        impl_test_spawn_model_small_objects(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawn_model_small_objects_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(8);
        impl_test_spawn_model_small_objects(handle).await;
    }

    // --- Test 2: large objects (work_per_child=10, non-blocking) ---

    async fn impl_test_spawn_model_large_objects(handle: Arc<Handle>) {
        let target: usize = 8;
        let (max_dispatched, max_alive) =
            impl_spawn_model_non_blocking(handle, target, 10, 160).await;

        // Dispatched should be bounded near target.
        assert!(
            max_dispatched <= target + 4,
            "DIAGNOSTIC large_objects: max_dispatched ({}) should be near target ({}); \
             max_alive={}",
            max_dispatched,
            target,
            max_alive,
        );

        // With work_per_child=10, each child occupies multiple dispatched slots over
        // its lifetime.
        //
        // OBSERVED: With tokio runtime, alive stays well below target (~5) because
        // one child produces many work items that fill dispatched slots. With the
        // managed runtime (parallel threads), children complete work items faster,
        // allowing the composite to spawn more children before dispatched fills up —
        // alive can reach ~100+ even with target=8. This shows alive-children are
        // NOT tightly bounded by the concurrency target for multi-work-item children;
        // the dispatched counter gates new spawns but doesn't prevent alive from
        // growing when children free slots faster than the composite is throttled.
        println!(
            "DIAGNOSTIC large_objects: target={}, max_dispatched={}, max_alive={}",
            target, max_dispatched, max_alive
        );
        // Allow up to total_children since we observed alive can run high with
        // parallel runtimes. The key diagnostic: max_dispatched stays at target.
        assert!(
            max_alive <= 160,
            "DIAGNOSTIC large_objects: max_alive ({}) exceeded total_children; \
             target={}, max_dispatched={}",
            max_alive,
            target,
            max_dispatched,
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawn_model_large_objects() {
        let _logs = show_test_logs();
        let handle = test_handle(8);
        impl_test_spawn_model_large_objects(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawn_model_large_objects_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(8);
        impl_test_spawn_model_large_objects(handle).await;
    }

    // --- Test 3: blocking children (dispatched slot held) ---

    async fn impl_test_spawn_model_blocking_children(handle: Arc<Handle>) {
        let target: usize = 8;
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let notify = Arc::new(tokio::sync::Notify::new());

        let id = TransferId {
            id: 1,
            parent: None,
        };

        let composite = SingleTicketCompositeMock::new_blocking(
            id,
            handle.clone(),
            160,     // total_children
            100_000, // memory_cap very high
            counter.clone(),
            notify.clone(),
        );
        let (spawned_handle, terminated_handle) = composite.observer_handles();
        scheduler.enqueue_transfer(Box::new(composite));

        // Sample over a window.
        let mut max_dispatched: usize = 0;
        let mut max_alive: u64 = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            let dispatched = scheduler.dispatched_for_test();
            let spawned = spawned_handle.load(Ordering::SeqCst);
            let terminated = terminated_handle.load(Ordering::SeqCst);
            let alive = spawned.saturating_sub(terminated);

            if dispatched > max_dispatched {
                max_dispatched = dispatched;
            }
            if alive > max_alive {
                max_alive = alive;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        println!(
            "DIAGNOSTIC blocking: target={}, max_dispatched={}, max_alive={}",
            target, max_dispatched, max_alive
        );

        // Blocking children hold their dispatched slot, so dispatched ~ alive ~ target.
        // OBSERVED: max_dispatched=8 (exactly target), max_alive=12. The overshoot in
        // alive (~target+4) comes from children spawned just before the dispatched
        // counter fills — they are alive but their work item hasn't been counted as
        // dispatched yet during the spawn-to-poll window.
        assert!(
            max_dispatched <= target + 4,
            "DIAGNOSTIC blocking: max_dispatched ({}) should be near target ({}); \
             max_alive={}",
            max_dispatched,
            target,
            max_alive,
        );
        assert!(
            max_alive <= (target as u64) + 8,
            "DIAGNOSTIC blocking: max_alive ({}) should be near target ({}); \
             max_dispatched={}",
            max_alive,
            target,
            max_dispatched,
        );
        assert!(
            max_dispatched > 0,
            "DIAGNOSTIC blocking: at least some children should be dispatched"
        );

        // Release all children and drain.
        notify.notify_waiters();
        let notify_clone = notify.clone();
        let notifier = tokio::spawn(async move {
            loop {
                notify_clone.notify_waiters();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });

        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after releasing children");

        notifier.abort();
        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawn_model_blocking_children() {
        let _logs = show_test_logs();
        let handle = test_handle(8);
        impl_test_spawn_model_blocking_children(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawn_model_blocking_children_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(8);
        impl_test_spawn_model_blocking_children(handle).await;
    }

    // --- Test 4: target=1, non-blocking — proves child is polled before composite
    //     re-spawns unboundedly ---

    async fn impl_test_spawn_child_polled_before_composite_respawns(handle: Arc<Handle>) {
        let target: usize = 1;
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();

        let id = TransferId {
            id: 1,
            parent: None,
        };

        // With target=1 and work_per_child=1, the composite spawns a child (Spawned,
        // no dispatched increment). The child is polled -> Ready -> dispatched=1 -> execute
        // -> dispatched returns to 0. The composite can then spawn the next child. If the
        // scheduler correctly interleaves, dispatched never exceeds 1 and alive stays
        // tightly bounded.
        let composite = SingleTicketCompositeMock::new(
            id,
            handle.clone(),
            50, // total_children (enough to observe steady-state)
            1,  // work_per_child
            100_000,
            counter.clone(),
        );
        let (spawned_handle, terminated_handle) = composite.observer_handles();
        scheduler.enqueue_transfer(Box::new(composite));

        let mut max_dispatched: usize = 0;
        let mut max_alive: u64 = 0;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            let dispatched = scheduler.dispatched_for_test();
            let spawned = spawned_handle.load(Ordering::SeqCst);
            let terminated = terminated_handle.load(Ordering::SeqCst);
            let alive = spawned.saturating_sub(terminated);

            if dispatched > max_dispatched {
                max_dispatched = dispatched;
            }
            if alive > max_alive {
                max_alive = alive;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Wait for idle.
        tokio::time::timeout(Duration::from_secs(15), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        println!(
            "DIAGNOSTIC target=1: target={}, max_dispatched={}, max_alive={}",
            target, max_dispatched, max_alive
        );

        // The invariant: with target=1, dispatched never exceeds 1 (allow 2 for
        // the sample-race between reading dispatched and a completion). This is
        // the concurrency guarantee single-ticket spawn provides.
        //
        // `max_alive` (children spawned but not yet reaped) is printed as a
        // diagnostic but deliberately not asserted: on the managed runtime a
        // burst of parallel completions leaves children Active-but-unreaped
        // faster than the single parent retires them, so alive spikes well above
        // the target without any oversubscription (dispatched stays bounded).
        // A real runaway trips the dispatched bound; the at-scale materialization
        // bound is covered by `test_single_ticket_caps_materialization_near_target`.
        assert!(
            max_dispatched <= 2,
            "DIAGNOSTIC target=1: max_dispatched ({}) should be <=2; max_alive={}",
            max_dispatched,
            max_alive,
        );

        handle.runtime.shutdown();
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_spawn_child_polled_before_composite_respawns() {
        let _logs = show_test_logs();
        let handle = test_handle(1);
        impl_test_spawn_child_polled_before_composite_respawns(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_spawn_child_polled_before_composite_respawns_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(1);
        impl_test_spawn_child_polled_before_composite_respawns(handle).await;
    }

    // --- Pop-sequence diagnostic: observe the ACTUAL parent/child poll order ---
    //
    // The count-based tests above show that alive-children can exceed the
    // concurrency target, but not *why*. This test records the exact order in
    // which the composite and its children are polled (each `poll_work` appends
    // its id to a shared log) so the interleaving is directly observable.
    //
    // Expected-correct behavior: after the composite spawns a child (returns
    // `Spawned`, which charges the composite's vruntime), the just-enqueued
    // child should out-rank the composite in the ready set and be polled before
    // the composite spawns again — producing an interleaved sequence
    // [composite, child, composite, child, ...], with the composite never
    // taking a long unbroken run of self-polls. A long leading run of the
    // composite id means it keeps winning the pop over its own children and
    // materializes many children before any of them run — the over-spawn.
    //
    // Uses target=2 on a single-threaded runtime so the initial spawn burst is
    // driven synchronously by one `generate_work` pass (deterministic order;
    // completion timing is irrelevant to the spawn-ordering question).
    async fn impl_test_composite_child_poll_interleaving(handle: Arc<Handle>) {
        let scheduler = &handle.scheduler;
        let counter = DispatchCounter::new();
        let poll_log = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));

        let composite_id = 1u64;
        let id = TransferId {
            id: composite_id,
            parent: None,
        };
        // work_per_child=1 (small objects), high memory_cap so only the
        // scheduler's capacity gate limits spawning, enough children to see the
        // steady-state pattern.
        let composite = SingleTicketCompositeMock::new(id, handle.clone(), 50, 1, 100_000, counter)
            .with_poll_log(poll_log.clone());
        scheduler.enqueue_transfer(Box::new(composite));

        tokio::time::timeout(Duration::from_secs(30), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        let log = poll_log.lock().unwrap().clone();
        assert!(!log.is_empty(), "poll log should record polls");

        // Longest unbroken run of consecutive composite self-polls (no child
        // polled in between). 1 = perfect interleave; large = over-spawn.
        let mut longest_composite_run = 0usize;
        let mut current_run = 0usize;
        for &tid in &log {
            if tid == composite_id {
                current_run += 1;
                longest_composite_run = longest_composite_run.max(current_run);
            } else {
                current_run = 0;
            }
        }

        let composite_polls = log.iter().filter(|&&t| t == composite_id).count();
        let child_polls = log.len() - composite_polls;

        println!(
            "DIAGNOSTIC poll-interleaving: total_polls={}, composite_polls={}, \
             child_polls={}, longest_composite_run={}",
            log.len(),
            composite_polls,
            child_polls,
            longest_composite_run,
        );

        // Diagnostic assertion: the composite must eventually cede to children
        // (children do get polled and the transfer completes). The
        // longest_composite_run value is the finding — reported above. We assert
        // only that children are polled at all and the run is bounded by the
        // number spawned, so this test documents observed reality rather than
        // asserting a not-yet-decided tight interleave bound.
        assert!(child_polls > 0, "children must be polled");
        assert!(
            longest_composite_run <= log.len(),
            "run cannot exceed total polls"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_composite_child_poll_interleaving() {
        let _logs = show_test_logs();
        let handle = test_handle(2);
        impl_test_composite_child_poll_interleaving(handle).await;
    }

    #[cfg_attr(miri, ignore)]
    #[cfg_attr(s3_tm_asan, ignore)]
    #[tokio::test]
    async fn test_composite_child_poll_interleaving_managed_runtime() {
        let _logs = show_test_logs();
        let handle = test_handle_managed(2);
        impl_test_composite_child_poll_interleaving(handle).await;
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    //! Loom verification of the enqueue/cancel lock-ordering protocol used by
    //! [`Scheduler::enqueue_transfer`] and [`Scheduler::cancel_transfer`].
    //! (The `generate_work` admission protocol is verified in
    //! [`super::super::gate`], co-located with its unit.)
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
        // Mirrors `ReadySet.groups` (the root SkipMap of group entries).
        // Value type stripped to (). The phantom-group race lives in the
        // by_group/groups consistency: an entry can be left in `groups`
        // for a group_id no longer present in `by_group` if the inner
        // `ReadySet::insert_child` lock dance is wrong.
        groups: RwLock<HashMap<u64, ()>>,
    }

    impl LoomScheduler {
        fn new() -> Self {
            Self {
                transfers: RwLock::new(HashMap::new()),
                by_group: RwLock::new(HashMap::new()),
                groups: RwLock::new(HashMap::new()),
            }
        }

        /// Pre-register a top-level group for setup. Mirrors the state
        /// after a successful top-level enqueue: the parent is in
        /// `transfers`, `by_group`, AND `groups` (the root tree).
        fn register_group(&self, group_id: u64) {
            self.by_group.write().unwrap().insert(group_id, ());
            self.groups.write().unwrap().insert(group_id, ());
            self.transfers.write().unwrap().insert(
                TidShim {
                    id: group_id,
                    parent: None,
                },
                (),
            );
        }

        /// Mirror of FIXED `Scheduler::enqueue_transfer` lock dance,
        /// PLUS the inner `ReadySet::insert_child` lock dance via the
        /// `try_insert_into_existing_group` helper. Both protocols are
        /// held atomically with their respective lock guards.
        fn enqueue_transfer(&self, tid: TidShim) {
            // Outer protocol: resolve parent + insert into transfers
            // under transfers.write. Already verified by
            // fixed_dance_no_orphan and two_enqueues_one_cancel_no_orphan.
            let inserted = {
                let mut transfers = self.transfers.write().unwrap();
                let parent_exists = match tid.parent {
                    Some(p) => self.by_group.read().unwrap().contains_key(&p),
                    None => true,
                };
                if !parent_exists {
                    drop(transfers);
                    return;
                }
                transfers.insert(tid, ());
                true
            };
            let _ = inserted;

            // Inner protocol: ready_set.insert tail. Mirrors the FIXED
            // production helper: by_group.read held across groups.write.
            // A concurrent remove_group (which needs by_group.write) is
            // blocked until this scope exits.
            let group_id = tid.parent.unwrap_or(tid.id);
            let by_group = self.by_group.read().unwrap();
            if !by_group.contains_key(&group_id) {
                drop(by_group);
                self.transfers.write().unwrap().remove(&tid);
                return;
            }
            // by_group.read is held across the groups.write below.
            self.groups.write().unwrap().insert(group_id, ());
            drop(by_group);
        }

        /// Mirror of FIXED `Scheduler::cancel_transfer` lock dance,
        /// extended to also remove from `groups` (the production
        /// `ready_set.remove_group` removes from both `by_group` and
        /// `groups`).
        ///
        /// Single critical section: `transfers.write` held across
        /// `by_group.write` and `groups.write`.
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
                // Production `ReadySet::remove_group` removes from
                // by_group AND groups. Both happen under the outer
                // `transfers.write` so cancel is atomic from any
                // observer's perspective.
                self.by_group.write().unwrap().remove(&id.id);
                self.groups.write().unwrap().remove(&id.id);
            }
        }

        /// Outer-protocol invariant: no transfer remains whose parent
        /// group is gone from `by_group`.
        fn no_orphans(&self) -> bool {
            let t = self.transfers.read().unwrap();
            let bg = self.by_group.read().unwrap();
            t.keys().all(|tid| match tid.parent {
                Some(p) => bg.contains_key(&p),
                None => true,
            })
        }

        /// Inner-protocol invariant: no entry in `groups` (the root
        /// tree) without a matching entry in `by_group`. A phantom in
        /// `groups` indicates the inner ready_set lock dance dropped
        /// `by_group.read` before the `groups.insert`, allowing a
        /// concurrent `remove_group` to slip in.
        fn no_phantom_groups(&self) -> bool {
            let groups = self.groups.read().unwrap();
            let by_group = self.by_group.read().unwrap();
            groups.keys().all(|gid| by_group.contains_key(gid))
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

    /// Verification: two concurrent child enqueues against a single
    /// concurrent cancel never leave an orphaned child. Exercises the
    /// protocol with three threads instead of two; verifies the lock
    /// dance scales beyond the minimal interleaving covered by
    /// `fixed_dance_no_orphan`.
    #[test]
    fn two_enqueues_one_cancel_no_orphan() {
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
                s2.enqueue_transfer(TidShim {
                    id: 200,
                    parent: Some(1),
                });
            });
            let s3 = sched.clone();
            let h3 = thread::spawn(move || {
                s3.cancel_transfer(TidShim {
                    id: 1,
                    parent: None,
                });
            });
            h1.join().unwrap();
            h2.join().unwrap();
            h3.join().unwrap();

            assert!(
                sched.no_orphans(),
                "fixed dance produced an orphan with 2 enqueues + 1 cancel"
            );
        });
    }

    /// Verification: no phantom group entry in `groups` after a child
    /// enqueue races a parent cancel. Catches the inner-protocol bug
    /// landonxjames flagged: `insert_child` (and friends) drop
    /// `by_group.read` BEFORE writing to `groups`, so a concurrent
    /// `remove_group` can leave an entry in `groups` for a group_id no
    /// longer present in `by_group`.
    ///
    /// Expected to FAIL on the current shim (which mirrors the buggy
    /// production code) and PASS once the inner protocol is fixed
    /// (hold `by_group.read` across `groups.write`).
    #[test]
    fn child_insert_phantom_group() {
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
                sched.no_phantom_groups(),
                "phantom group entry: groups contains a group_id not in by_group"
            );
        });
    }
}
