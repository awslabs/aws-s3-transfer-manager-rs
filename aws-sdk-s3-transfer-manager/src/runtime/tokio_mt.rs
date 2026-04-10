/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tokio-based multi-threaded execution runtime.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::FutureExt;

mod worker_pool;

use super::{ExecutionRuntime, RuntimeComponents, ScheduledWork};
use crate::runtime::sync::SubmissionGuard;
use crate::scheduler::Scheduler;
use crate::transfer::{TransferId, WorkOutcome};
use worker_pool::WorkerPool;

/// Runtime that spawns tokio tasks to execute work from a shared [`WorkerPool`].
///
/// Assumes it is running within an existing tokio multi-threaded runtime context.
/// Workers are spawned via `tokio::spawn` and pull work from a shared queue.
#[allow(dead_code)] // TODO: expose runtime selection on public config
#[derive(Debug)]
pub(crate) struct TokioMultiThreadRuntime {
    pool: Arc<WorkerPool>,
    scheduler: Scheduler,
    worker_count: AtomicUsize,
    components: RuntimeComponents,
}

#[allow(dead_code)] // TODO: expose runtime selection on public config
impl TokioMultiThreadRuntime {
    pub(crate) fn new(scheduler: Scheduler) -> Self {
        Self {
            pool: Arc::new(WorkerPool::new()),
            scheduler,
            worker_count: AtomicUsize::new(0),
            components: RuntimeComponents::default(),
        }
    }

    /// Ensure workers are spawned. Called lazily on first dispatch.
    fn ensure_workers_started(&self) {
        if self.pool.mark_started() {
            let target = self.scheduler.controller_target();
            self.spawn_workers(target);
        }
    }

    /// Spawn additional workers if the concurrency target has grown beyond
    /// the current worker count.
    fn ensure_worker_capacity(&self) {
        let target = self.scheduler.controller_target();
        let current = self.worker_count.load(Ordering::Relaxed);
        if target > current
            && self
                .worker_count
                .compare_exchange(current, target, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.spawn_workers(target - current);
            tracing::debug!(target: crate::telemetry::TARGET_SCHEDULING, old = current, new = target, "spawning additional workers");
        }
    }

    fn spawn_workers(&self, count: usize) {
        for _ in 0..count {
            let pool = Arc::clone(&self.pool);
            let scheduler = self.scheduler.clone();
            tokio::spawn(async move {
                worker_loop(pool, scheduler).await;
            });
        }
        self.worker_count.fetch_add(count, Ordering::Relaxed);
    }
}

impl ExecutionRuntime for TokioMultiThreadRuntime {
    fn dispatch(&self, batch: &mut SubmissionGuard<'_, ScheduledWork>) {
        self.ensure_workers_started();
        self.ensure_worker_capacity();
        for work in batch.drain() {
            self.pool.push(work);
        }
    }

    fn shutdown(&self) {
        self.pool.shutdown();
    }

    fn remove_pending_for_transfer(&self, id: TransferId) -> usize {
        self.pool.remove_for_transfer(id)
    }

    fn components(&self) -> &RuntimeComponents {
        &self.components
    }
}

/// Worker loop — pulls work from pool and executes it.
async fn worker_loop(pool: Arc<WorkerPool>, scheduler: Scheduler) {
    static WORKER_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let wid = WORKER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    loop {
        let Some(mut work) = pool.next_work().await else {
            tracing::debug!(target: crate::telemetry::TARGET_EXECUTION, wid, "shutdown");
            break;
        };
        scheduler.on_dispatch();

        let tid = work.descriptor.id();
        work.descriptor.work_started();

        // Skip execution if transfer already terminal (failed/cancelled by another work item)
        if work.descriptor.is_terminal() {
            tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, "skipped (terminal)");
            pool.complete();
            scheduler.on_completion(work, WorkOutcome::Cancelled, Duration::ZERO);
            continue;
        }

        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, "executing");
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

        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, ?outcome, "completed");

        let elapsed = started.elapsed();
        pool.complete();
        scheduler.on_completion(work, outcome, elapsed);
    }
}
