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
//! Transfers are state machines that implement [`Transfer`]. The scheduler polls
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
//! # State Machine Contracts
//!
//! A [`Transfer`] implementation must uphold:
//! - **Failed lifecycle**: record the error and signal termination before returning
//!   a failure outcome.
//! - **Pending/wake obligation**: every `Pending` must have a future wake path.
//! - **Panic safety**: handled by the scheduler via `catch_unwind`.

use super::{
    BoxTransfer, CompletionSample, ConcurrencyController, IoRequest, PollWork, TransferId,
    WorkOutcome, WorkerPool,
};

use crate::metrics::{IOCounters, IoSample};

use crate::scheduler::descriptor::TransferDescriptor;
use crate::scheduler::ready_set::ReadySet;
use crate::scheduler::work::ScheduledWork;
use futures_util::FutureExt;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Event-driven scheduler for coordinating transfer work.
///
/// Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    transfers: RwLock<HashMap<TransferId, TransferDescriptor>>,
    ready_set: ReadySet,
    pool: Arc<WorkerPool>,
    controller: Arc<dyn ConcurrencyController>,
    io_counters: Arc<IOCounters>,
    worker_count: AtomicUsize,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

impl Scheduler {
    #[cfg(test)]
    pub(crate) fn new(concurrency: usize) -> Self {
        Self::with_controller(
            Arc::new(super::FixedConcurrency::new(concurrency)),
            Arc::new(IOCounters::new(Duration::from_millis(500))),
        )
    }

    pub(crate) fn with_controller(
        controller: Arc<dyn ConcurrencyController>,
        io_counters: Arc<IOCounters>,
    ) -> Self {
        Self(Arc::new(SchedulerInner {
            transfers: RwLock::new(HashMap::new()),
            ready_set: ReadySet::new(),
            pool: Arc::new(WorkerPool::new()),
            controller,
            io_counters,
            worker_count: AtomicUsize::new(0),
        }))
    }

    /// Ensure workers are spawned for a pool. Called lazily on first enqueue.
    fn ensure_workers_started(&self) {
        let pool = &self.0.pool;
        if pool.mark_started() {
            let target = self.0.controller.target();
            for _ in 0..target {
                let pool = Arc::clone(pool);
                let scheduler = self.clone();
                tokio::spawn(async move {
                    worker_loop(pool, scheduler).await;
                });
            }
            self.0.worker_count.store(target, Ordering::Relaxed);
        }
    }

    /// Add a transfer and start generating work.
    pub(crate) fn enqueue_transfer(&self, transfer: BoxTransfer) {
        self.ensure_workers_started();

        // start with lowest current vruntime to avoid new transfer
        // playing aggressive catchup over already in-flight transfers
        let desc = TransferDescriptor::new_with_vruntime(transfer, self.0.ready_set.min_vruntime());

        {
            let mut transfers = self.0.transfers.write().unwrap();
            transfers.insert(desc.id(), desc.clone());
            self.0.ready_set.insert(desc);
        }

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
        if let Some(desc) = desc {
            self.0.ready_set.insert(desc);
            self.generate_work();
        }
    }

    /// Cancel a transfer, removing it from the scheduler.
    ///
    /// - Removes from transfers map
    /// - Purges queued work for this transfer from pools
    /// - Outstanding work being executed will complete naturally
    /// - Use `wait_for_idle(id)` to wait for all outstanding work to finish
    ///
    /// TODO: When upload_objects/download_objects use the new scheduler,
    /// this needs to cancel child transfers too (where tid.parent == Some(id.id)).
    pub(crate) fn cancel_transfer(&self, id: TransferId) -> bool {
        let desc = self.0.transfers.write().unwrap().remove(&id);
        if let Some(desc) = desc {
            let ctx = desc.transfer().ctx();
            // Ensure transfer is terminal so ready_set and workers skip it.
            // May already be set by the handle (set_cancelled/set_failed) — that's fine.
            if ctx.is_active() {
                ctx.set_cancelled();
            }
            ctx.signal_terminal();
            let purged = self.0.pool.remove_for_transfer(id);
            desc.work_purged(purged);
            desc.notify_idle();
            true
        } else {
            false
        }
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
    pub(super) fn on_completion(
        &self,
        work: ScheduledWork,
        outcome: WorkOutcome,
        elapsed: Duration,
    ) {
        // Report to concurrency controller
        let (mut io_sample, classification) = match &outcome {
            WorkOutcome::Success { metrics, .. } => (metrics.unwrap_or_default(), None),
            WorkOutcome::Failed { classification } => (IoSample::default(), *classification),
            WorkOutcome::Cancelled => (IoSample::default(), None),
        };
        io_sample.duration = elapsed;
        self.0.io_counters.record(&io_sample);
        let sample = CompletionSample {
            io: io_sample,
            error: classification,
            kind: work.item.kind,
        };
        self.0.controller.on_completion(&sample);

        let desc = &work.descriptor;
        let is_idle = desc.work_finished();
        if is_idle {
            desc.notify_idle();
        }

        // Terminal transfer: no further work to generate. Clean up when fully drained.
        if desc.is_terminal() {
            if is_idle {
                self.0.transfers.write().unwrap().remove(&desc.id());
            }
            return;
        }

        // handle any follow-on work
        if let WorkOutcome::Success {
            schedule_next: Some(kind),
            data,
            ..
        } = outcome
        {
            let next = ScheduledWork {
                item: IoRequest { kind, data },
                descriptor: desc.clone(),
            };
            self.enqueue_to_pool(next);
        }

        // capacity has freed try to queue up more work
        self.generate_work();
    }

    /// Handle a panic during work execution. The transfer's internal state is
    /// unknown, so the scheduler forces the terminal transition from outside.
    pub(super) fn on_panic(&self, work: ScheduledWork) {
        let desc = &work.descriptor;
        let ctx = desc.transfer().ctx();

        let err = crate::error::from_kind(crate::error::ErrorKind::RuntimeError)(
            "worker panic during execute",
        );
        ctx.set_failed(err);
        ctx.signal_terminal();

        let is_idle = desc.work_finished();
        if is_idle {
            self.0.transfers.write().unwrap().remove(&desc.id());
        }
        desc.notify_idle();
    }

    fn has_capacity(&self) -> bool {
        let pending = self.0.pool.pending_count();
        let in_flight = self.0.pool.in_flight_count();
        pending + in_flight < self.0.controller.target()
    }

    /// Spawn additional workers if the concurrency target has grown beyond
    /// the current worker count. Workers that become excess when the target
    /// shrinks simply idle-park on `next_work`.
    fn ensure_worker_capacity(&self) {
        let target = self.0.controller.target();
        let current = self.0.worker_count.load(Ordering::Relaxed);
        if target > current {
            // CAS to avoid double-spawning from concurrent generate_work calls
            if self
                .0
                .worker_count
                .compare_exchange(current, target, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                for _ in current..target {
                    let pool = Arc::clone(&self.0.pool);
                    let scheduler = self.clone();
                    tokio::spawn(async move {
                        worker_loop(pool, scheduler).await;
                    });
                }
                tracing::debug!(target: crate::telemetry::TARGET_SCHEDULING, old = current, new = target, "spawning additional workers");
            }
        }
    }

    /// Generate work from ready transfers and push to pools.
    fn generate_work(&self) {
        self.ensure_worker_capacity();
        while self.has_capacity() {
            let Some(desc) = self.0.ready_set.pop() else {
                break;
            };

            // Skip cancelled/failed transfers still in the ready set
            if desc.is_terminal() {
                continue;
            }

            match desc.transfer().poll_work() {
                PollWork::Ready(item) => {
                    desc.work_generated();
                    self.0.ready_set.insert(desc.clone());
                    self.enqueue_to_pool(ScheduledWork {
                        item,
                        descriptor: desc,
                    });
                }
                PollWork::Pending => {
                    // re-added on wake as state machine progresses
                }
                PollWork::Done => {
                    // done generating work, remove from transfers
                    self.0.transfers.write().unwrap().remove(&desc.id());
                }
            }
        }
    }

    fn enqueue_to_pool(&self, work: ScheduledWork) {
        work.descriptor.work_queued();
        self.0.pool.push(work);
    }

    #[allow(dead_code)] // TODO: wire into Handle for graceful shutdown
    pub(crate) fn is_idle(&self) -> bool {
        self.0.transfers.read().unwrap().is_empty()
            && self.0.pool.pending_count() == 0
            && self.0.pool.in_flight_count() == 0
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

    /// Shutdown the scheduler. Workers will exit after completing current work.
    #[allow(dead_code)] // TODO(phase3): wire into Handle::drop
    pub(crate) fn shutdown(&self) {
        self.0.pool.shutdown();
    }
}

/// Worker loop - pulls work from pool and executes it.
async fn worker_loop(pool: Arc<WorkerPool>, scheduler: Scheduler) {
    static WORKER_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let wid = WORKER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    loop {
        let Some(mut work) = pool.next_work().await else {
            tracing::debug!(target: crate::telemetry::TARGET_EXECUTION, wid, "shutdown");
            break;
        };
        scheduler.0.controller.on_dispatch();

        let tid = work.descriptor.id();
        work.descriptor.work_started();

        // Skip execution if transfer already terminal (failed/cancelled by another work item)
        if work.descriptor.is_terminal() {
            tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, work = ?work.item.data, "skipped (terminal)");
            pool.complete();
            scheduler.on_completion(work, WorkOutcome::Cancelled, Duration::ZERO);
            continue;
        }

        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, work = ?work.item.data, "executing");
        let transfer = work.descriptor.transfer();
        let started = Instant::now();

        let token = transfer.ctx().cancellation_token().clone();
        let outcome = AssertUnwindSafe(async {
            tokio::select! {
                biased;
                _ = token.cancelled() => WorkOutcome::Cancelled,
                outcome = transfer.execute(&mut work.item) => outcome,
            }
        })
        .catch_unwind()
        .await;

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_panic) => {
                tracing::error!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, "panic in transfer execute");
                pool.complete();
                scheduler.on_panic(work);
                continue;
            }
        };

        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, work = ?work.item.data, ?outcome, "completed");

        let elapsed = started.elapsed();
        pool.complete();
        scheduler.on_completion(work, outcome, elapsed);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::transfer::mock::{
        FixedWorkCount, MockStateMachine, WithDelay, WithExecute,
    };
    use crate::scheduler::transfer::Transfer;
    use crate::scheduler::work::{IoKind, WorkOutcome};
    use crate::scheduler::MockTransfer;
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_single_transfer_completes() {
        let _logs = show_test_logs();
        let scheduler = Scheduler::new(4);
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
        scheduler.shutdown();
    }

    #[tokio::test]
    async fn test_multiple_transfers_complete() {
        let scheduler = Scheduler::new(4);
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
        scheduler.shutdown();
    }

    #[test]
    fn test_new_does_not_require_runtime() {
        let _scheduler = Scheduler::new(4);
    }

    #[tokio::test]
    async fn test_set_priority() {
        let scheduler = Scheduler::new(2);
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

        scheduler.shutdown();
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
            PollWork::Ready(IoRequest {
                kind: IoKind::Network,
                data: None,
            })
        }

        fn execute<'a>(
            &'a self,
            _work: &'a mut IoRequest,
        ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
            Box::pin(async move {
                self.executions.fetch_add(1, Ordering::Relaxed);
                // Yield to prevent starving other tasks on single-threaded runtimes
                tokio::task::yield_now().await;
                // No schedule_next — let generate_work() pull from ready set
                // where CFS priority ordering applies
                WorkOutcome::Success {
                    schedule_next: None,
                    data: None,
                    metrics: None,
                }
            })
        }
    }

    #[tokio::test]
    async fn test_priority_affects_work_distribution() {
        let scheduler = Scheduler::new(2);

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

        scheduler.shutdown();

        let high_count = high_mock.count();
        let low_count = low_mock.count();

        // High priority should get significantly more work
        // With 4x priority difference, expect at least 1.5x more executions
        // (conservative to avoid flakiness)
        assert!(
            high_count > low_count,
            "high priority should execute more: high={}, low={}",
            high_count,
            low_count
        );

        let ratio = high_count as f64 / low_count.max(1) as f64;
        assert!(
            ratio > 1.5,
            "expected ratio > 1.5, got {:.2} (high={}, low={})",
            ratio,
            high_count,
            low_count
        );
    }

    #[tokio::test]
    async fn test_priority_change_mid_flight() {
        let scheduler = Scheduler::new(2);

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

        // Both at default priority (128) — let them run equally
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
        scheduler.shutdown();

        let a_after = a_mock.count() - a_before;
        let b_after = b_mock.count() - b_before;

        // 255:1 priority ratio — A should get the vast majority of work
        let ratio = a_after as f64 / b_after.max(1) as f64;
        assert!(
            ratio > 3.0,
            "after priority change (255 vs 1), expected ratio > 3.0, got {:.2} (a={}, b={})",
            ratio,
            a_after,
            b_after
        );
    }

    #[tokio::test]
    async fn test_failed_transfer_cleaned_up() {
        let _logs = show_test_logs();
        let scheduler = Scheduler::new(2);

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
        scheduler.shutdown();
    }

    #[tokio::test]
    async fn test_panic_transfer_cleaned_up_and_error_propagated() {
        let _logs = show_test_logs();
        let scheduler = Scheduler::new(2);

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

        scheduler.shutdown();
    }

    #[tokio::test]
    async fn test_cancel_transfer_stops_work() {
        let scheduler = Scheduler::new(2);
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

        scheduler.shutdown();
    }
}
