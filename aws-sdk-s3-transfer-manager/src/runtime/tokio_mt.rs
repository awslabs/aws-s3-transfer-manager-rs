/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tokio-based multi-threaded execution runtime.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Weak;
use std::time::{Duration, Instant};

use futures_util::FutureExt;

mod worker_pool;

use super::{ExecutionRuntime, RuntimeComponents, ScheduledWork};
use crate::runtime::sync::SubmissionGuard;
use crate::transfer::{TransferId, WorkOutcome};
use worker_pool::WorkerPool;

/// Runtime that spawns tokio tasks to execute work from a shared [`WorkerPool`].
///
/// Assumes it is running within an existing tokio multi-threaded runtime context.
/// Workers are spawned via `tokio::spawn` and pull work from a shared queue.
#[derive(Debug)]
pub(crate) struct TokioMultiThreadRuntime {
    pool: Arc<WorkerPool>,
    handle: Weak<crate::client::Handle>,
    worker_count: AtomicUsize,
    components: RuntimeComponents,
}

impl TokioMultiThreadRuntime {
    pub(crate) fn new(handle: Weak<crate::client::Handle>) -> Self {
        Self {
            pool: Arc::new(WorkerPool::new()),
            handle,
            worker_count: AtomicUsize::new(0),
            components: RuntimeComponents::default(),
        }
    }

    /// Ensure workers are spawned on first dispatch.
    ///
    /// The `mark_started` CAS (AcqRel) guarantees exactly one caller wins the
    /// genesis spawn. The subsequent `Release` store to `worker_count` publishes
    /// `target` so that a concurrent `ensure_worker_capacity` load (Acquire)
    /// observes the post-genesis count and does not re-spawn.
    fn ensure_workers_started(&self) {
        if self.pool.mark_started() {
            let target = self
                .handle
                .upgrade()
                .expect("Handle dropped")
                .controller
                .target();

            // Emit the current-thread guardrail warning once, at the site where
            // workers actually execute (not at Client::new, which may observe a
            // different runtime).
            if matches!(
                tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
                Ok(tokio::runtime::RuntimeFlavor::CurrentThread)
            ) {
                tracing::warn!(
                    target: crate::telemetry::TARGET_SCHEDULING,
                    "RuntimeMode::MultiThreadTokio selected on a current-thread runtime; \
                     transfer workers will serialize onto one thread. Use a \
                     multi-threaded runtime or RuntimeMode::Managed."
                );
            }

            self.spawn_workers_raw(target);
            // Release: publish the genesis count so growth-path Acquire loads see it.
            self.worker_count.store(target, Ordering::Release);
        }
    }

    /// Spawn additional workers if the concurrency target has grown beyond
    /// the current worker count.
    ///
    /// The CAS claims the growth atomically: it publishes `target` into
    /// `worker_count` so concurrent callers observe the new count and skip.
    /// Only the CAS winner spawns the delta. Acquire on load pairs with the
    /// Release in genesis and in the CAS success ordering, preventing a stale
    /// read of 0 after genesis has completed.
    fn ensure_worker_capacity(&self) {
        // Only attempt growth after genesis; avoids a spurious load/CAS before
        // `mark_started` has published the initial count.
        if !self.pool.is_started() {
            return;
        }

        let target = self
            .handle
            .upgrade()
            .expect("Handle dropped")
            .controller
            .target();
        let current = self.worker_count.load(Ordering::Acquire);
        if target > current
            && self
                .worker_count
                .compare_exchange(current, target, Ordering::Release, Ordering::Relaxed)
                .is_ok()
        {
            self.spawn_workers_raw(target - current);
            tracing::debug!(target: crate::telemetry::TARGET_SCHEDULING, old = current, new = target, "spawning additional workers");
        }
    }

    /// Spawn `count` tokio tasks that pull from the shared worker pool.
    ///
    /// Does NOT update `worker_count` — callers own the counter update
    /// so each path (genesis / growth) can publish the authoritative value
    /// without double-counting.
    fn spawn_workers_raw(&self, count: usize) {
        for _ in 0..count {
            let pool = Arc::clone(&self.pool);
            let handle = self.handle.clone();
            tokio::spawn(async move {
                worker_loop(pool, handle).await;
            });
        }
    }
}

impl TokioMultiThreadRuntime {
    /// Current value of the worker counter. Test-only.
    #[cfg(test)]
    pub(crate) fn worker_count(&self) -> usize {
        self.worker_count.load(Ordering::Acquire)
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

/// Worker loop -- pulls work from pool and executes it.
async fn worker_loop(pool: Arc<WorkerPool>, handle: Weak<crate::client::Handle>) {
    static WORKER_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let wid = WORKER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    loop {
        let Some(mut work) = pool.next_work().await else {
            tracing::debug!(target: crate::telemetry::TARGET_EXECUTION, wid, "shutdown");
            break;
        };
        let Some(h) = handle.upgrade() else {
            break;
        };
        h.scheduler.on_dispatch();

        let tid = work.descriptor.id();
        work.descriptor.work_started();

        // Skip execution if transfer already terminal (failed/cancelled by another work item)
        if work.descriptor.is_terminal() {
            tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, "skipped (terminal)");
            pool.complete();
            h.scheduler
                .on_completion(work, WorkOutcome::Cancelled, Duration::ZERO);
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
                h.scheduler.on_panic(work);
                continue;
            }
        };

        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, wid, %tid, ?outcome, "completed");

        let elapsed = started.elapsed();
        pool.complete();
        h.scheduler.on_completion(work, outcome, elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Handle;
    use crate::scheduler::ConcurrencyController;
    use std::sync::atomic::AtomicUsize as StdAtomicUsize;
    use std::sync::OnceLock;

    /// Concurrency controller with a mutable target for testing growth.
    #[derive(Debug)]
    struct AdjustableConcurrency(StdAtomicUsize);

    impl AdjustableConcurrency {
        fn new(target: usize) -> Self {
            Self(StdAtomicUsize::new(target))
        }

        fn set_target(&self, target: usize) {
            self.0.store(target, std::sync::atomic::Ordering::Release);
        }
    }

    impl ConcurrencyController for AdjustableConcurrency {
        fn target(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    fn test_config() -> crate::Config {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        crate::Config::builder().client(s3_client).build()
    }

    /// Helper: builds a Handle with a TokioMultiThreadRuntime and an adjustable
    /// concurrency controller, returning the runtime reference for counter checks.
    fn build_handle_with_adjustable_concurrency(
        initial_target: usize,
    ) -> (
        Arc<Handle>,
        Arc<TokioMultiThreadRuntime>,
        Arc<AdjustableConcurrency>,
    ) {
        let controller = Arc::new(AdjustableConcurrency::new(initial_target));
        let controller_clone = controller.clone();
        let slot: Arc<OnceLock<Arc<TokioMultiThreadRuntime>>> = Arc::new(OnceLock::new());
        let slot_clone = slot.clone();

        let mut config = test_config();
        let handle = Arc::new_cyclic(|weak| {
            let scheduler = crate::scheduler::Scheduler::new(weak.clone());
            let rt = Arc::new(TokioMultiThreadRuntime::new(weak.clone()));
            slot_clone.set(rt.clone()).ok();
            let runtime: Arc<dyn crate::runtime::ExecutionRuntime> = rt;
            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => client,
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    aws_sdk_s3::Client::from_conf(s3_config.builder.build())
                }
            };
            Handle {
                config,
                s3_client,
                scheduler,
                runtime,
                controller: controller_clone.clone(),
                telemetry: Arc::new(crate::telemetry::Telemetry::new(Duration::from_millis(500))),
                memory_budget: crate::runtime::memory::MemoryBudget::new(
                    1024 * 1024 * 1024,
                    8 * 1024 * 1024,
                ),
            }
        });
        let runtime = slot.get().unwrap().clone();
        (handle, runtime, controller)
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn genesis_spawns_exact_target_workers() {
        let (_handle, runtime, _controller) = build_handle_with_adjustable_concurrency(4);
        assert_eq!(
            runtime.worker_count(),
            0,
            "no workers before first dispatch"
        );

        // Trigger genesis via dispatch with an empty batch.
        let sq = crate::runtime::sync::SubmissionQueue::new(8);
        let sub = sq.enter();
        if let Some(mut guard) = sub.submit() {
            runtime.dispatch(&mut guard);
        }

        assert_eq!(
            runtime.worker_count(),
            4,
            "genesis must spawn exactly target workers"
        );
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn growth_does_not_double_count() {
        let (_handle, runtime, controller) = build_handle_with_adjustable_concurrency(4);

        // Genesis dispatch.
        let sq = crate::runtime::sync::SubmissionQueue::new(8);
        let sub = sq.enter();
        if let Some(mut guard) = sub.submit() {
            runtime.dispatch(&mut guard);
        }
        assert_eq!(runtime.worker_count(), 4);

        // Grow target from 4 to 8.
        controller.set_target(8);

        // Second dispatch triggers growth.
        let sub = sq.enter();
        if let Some(mut guard) = sub.submit() {
            runtime.dispatch(&mut guard);
        }
        assert_eq!(
            runtime.worker_count(),
            8,
            "after growth 4 -> 8, counter must be 8 (not 12)"
        );
    }
}
