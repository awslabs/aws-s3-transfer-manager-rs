/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-core managed thread runtime.
//!
//! Spawns one OS thread per core in the topology, each running a tokio
//! current-thread runtime. Work arrives via `Handle::spawn` from the dispatch
//! path. Shutdown cancels a shared token; each thread's `block_on` returns and
//! the thread exits.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;

use super::topology::ThreadId;
use super::Topology;
use super::{ExecutionRuntime, ScheduledWork};
use crate::scheduler::Scheduler;
use crate::transfer::{TransferId, WorkOutcome};

/// One managed OS thread and its tokio current-thread handle.
struct ThreadHandle {
    id: ThreadId,
    runtime_handle: tokio::runtime::Handle,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    in_flight: Arc<AtomicUsize>,
}

impl std::fmt::Debug for ThreadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Selects which managed thread should execute a work item.
struct DispatchRouter;

impl DispatchRouter {
    /// Select a thread to execute the given work.
    fn select(&self, threads: &[ThreadHandle]) -> ThreadId {
        threads
            .iter()
            .min_by_key(|th| (th.in_flight.load(Ordering::Relaxed), th.id.0))
            .expect("no threads available")
            .id
    }
}

/// Result of executing a single work item.
enum ExecuteResult {
    Completed(WorkOutcome, Duration),
    Panicked,
}

/// Execute a single work item, mirroring the `worker_loop` semantics from
/// `tokio_mt` but for one-shot per-task spawning.
async fn execute_work(work: &mut ScheduledWork, scheduler: &Scheduler) -> ExecuteResult {
    scheduler.on_dispatch();
    let tid = work.descriptor.id();
    work.descriptor.work_started();

    if work.descriptor.is_terminal() {
        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = ?work.item.data, "skipped (terminal)");
        return ExecuteResult::Completed(WorkOutcome::Cancelled, Duration::ZERO);
    }

    tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = ?work.item.data, "executing");
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

    match outcome {
        Ok(outcome) => {
            tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, ?outcome, "completed");
            ExecuteResult::Completed(outcome, started.elapsed())
        }
        Err(_panic) => {
            tracing::error!(target: crate::telemetry::TARGET_EXECUTION, %tid, "panic in transfer execute");
            ExecuteResult::Panicked
        }
    }
}

/// Execution runtime backed by per-core OS threads, each running a tokio
/// current-thread runtime.
pub(crate) struct ManagedThreadRuntime {
    scheduler: Scheduler,
    #[allow(dead_code)] // TODO(phase3): used for topology-aware routing
    topology: Topology,
    #[allow(dead_code)] // TODO(phase3): used for thread pinning
    pin_threads: bool,
    threads: Vec<ThreadHandle>,
    shutdown_token: CancellationToken,
    router: DispatchRouter,
}

impl std::fmt::Debug for ManagedThreadRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedThreadRuntime")
            .field("threads", &self.threads.len())
            .field("topology", &self.topology)
            .finish()
    }
}

impl ManagedThreadRuntime {
    /// Create a [`ManagedThreadRuntimeBuilder`].
    pub(crate) fn builder(scheduler: Scheduler) -> ManagedThreadRuntimeBuilder {
        ManagedThreadRuntimeBuilder::new(scheduler)
    }

    /// Create a new managed thread runtime.
    ///
    /// Spawns one OS thread per core in the topology. Each thread creates its
    /// own tokio current-thread runtime (the I/O driver binds to the creating
    /// thread).
    fn new(scheduler: Scheduler, topology: Topology, pin_threads: bool) -> Self {
        let shutdown_token = CancellationToken::new();

        // spawn and initialize concurrently
        let pending: Vec<_> = topology
            .thread_ids()
            .map(|id| {
                let shutdown = shutdown_token.clone();
                let (tx, rx) = std::sync::mpsc::channel();

                let join_handle = std::thread::Builder::new()
                    .name(format!("s3-tm-{}", id))
                    .spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("failed to create tokio current-thread runtime");
                        let _ = tx.send(rt.handle().clone());
                        rt.block_on(shutdown.cancelled());
                    })
                    .expect("failed to spawn managed thread");

                (id, rx, join_handle)
            })
            .collect();

        let threads = pending
            .into_iter()
            .map(|(id, rx, join_handle)| {
                let runtime_handle = rx.recv().expect("managed thread failed to start");
                ThreadHandle {
                    id,
                    runtime_handle,
                    join_handle: Mutex::new(Some(join_handle)),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                }
            })
            .collect();

        Self {
            scheduler,
            topology,
            pin_threads,
            threads,
            shutdown_token,
            router: DispatchRouter,
        }
    }
}

/// Builder for [`ManagedThreadRuntime`].
pub(crate) struct ManagedThreadRuntimeBuilder {
    scheduler: Scheduler,
    topology: Option<Topology>,
    pin_threads: bool,
}

impl ManagedThreadRuntimeBuilder {
    fn new(scheduler: Scheduler) -> Self {
        Self {
            scheduler,
            topology: None,
            pin_threads: false,
        }
    }

    /// Set the hardware topology. Defaults to `Topology::uniform(num_cpus)`
    /// where `num_cpus` is detected at build time.
    pub(crate) fn topology(mut self, topology: Topology) -> Self {
        self.topology = Some(topology);
        self
    }

    /// Enable thread pinning to cores. Default: false.
    #[allow(dead_code)] // TODO(phase3): not yet wired
    pub(crate) fn pin_threads(mut self, pin: bool) -> Self {
        self.pin_threads = pin;
        self
    }

    /// Build the runtime, spawning managed threads.
    pub(crate) fn build(self) -> ManagedThreadRuntime {
        let topology = self.topology.unwrap_or_else(|| {
            // TODO - use many_cpu's / Topologoy::detect()
            Topology::uniform(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1),
            )
        });
        ManagedThreadRuntime::new(self.scheduler, topology, self.pin_threads)
    }
}

impl ExecutionRuntime for ManagedThreadRuntime {
    fn dispatch(&self, mut work: ScheduledWork) {
        let thread_id = self.router.select(&self.threads);
        let th = &self.threads[thread_id.0];
        th.in_flight.fetch_add(1, Ordering::Relaxed);

        let scheduler = self.scheduler.clone();
        let in_flight = Arc::clone(&th.in_flight);

        th.runtime_handle.spawn(async move {
            let result = execute_work(&mut work, &scheduler).await;
            in_flight.fetch_sub(1, Ordering::Relaxed);
            match result {
                ExecuteResult::Completed(outcome, elapsed) => {
                    scheduler.on_completion(work, outcome, elapsed);
                }
                ExecuteResult::Panicked => {
                    scheduler.on_panic(work);
                }
            }
        });
    }

    fn shutdown(&self) {
        // Signal all threads to exit. Join happens in Drop.
        self.shutdown_token.cancel();
    }

    fn remove_pending_for_transfer(&self, _id: TransferId) -> usize {
        // Work is spawned as tasks on thread runtimes and cannot be removed.
        // The terminal check in the execute path handles cancelled transfers.
        0
    }
}

impl Drop for ManagedThreadRuntime {
    fn drop(&mut self) {
        self.shutdown_token.cancel();
        for th in &self.threads {
            if let Some(jh) = th.join_handle.lock().unwrap().take() {
                let _ = jh.join();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::Topology;

    /// Create a [`ThreadHandle`] with a preset in-flight count for testing
    /// router selection. No real work is spawned — only `id` and `in_flight`
    /// are meaningful.
    fn test_thread_handle(id: ThreadId, in_flight_count: usize) -> ThreadHandle {
        let rt = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        ));
        ThreadHandle {
            id,
            runtime_handle: rt.handle().clone(),
            join_handle: Mutex::new(None),
            in_flight: Arc::new(AtomicUsize::new(in_flight_count)),
        }
    }

    fn test_runtime(num_cores: usize) -> ManagedThreadRuntime {
        let scheduler = Scheduler::new(num_cores);
        ManagedThreadRuntime::builder(scheduler)
            .topology(Topology::uniform(num_cores))
            .build()
    }

    #[test]
    fn threads_start_and_shutdown() {
        let rt = test_runtime(4);
        assert_eq!(rt.threads.len(), 4);
        rt.shutdown();
        drop(rt);
    }

    #[test]
    fn shutdown_is_idempotent() {
        let rt = test_runtime(2);
        rt.shutdown();
        rt.shutdown();
        drop(rt);
    }

    #[test]
    fn drop_without_shutdown() {
        let rt = test_runtime(3);
        drop(rt);
    }

    #[test]
    fn router_selects_least_loaded() {
        let router = DispatchRouter;
        let handles = vec![
            test_thread_handle(ThreadId(0), 5),
            test_thread_handle(ThreadId(1), 2),
            test_thread_handle(ThreadId(2), 8),
            test_thread_handle(ThreadId(3), 3),
        ];
        assert_eq!(router.select(&handles), ThreadId(1));
    }

    #[test]
    fn router_breaks_ties_by_id() {
        let router = DispatchRouter;
        let handles = vec![
            test_thread_handle(ThreadId(0), 5),
            test_thread_handle(ThreadId(1), 5),
            test_thread_handle(ThreadId(2), 5),
        ];
        assert_eq!(router.select(&handles), ThreadId(0));
    }

    #[test]
    fn router_all_zero() {
        let router = DispatchRouter;
        let handles = vec![
            test_thread_handle(ThreadId(0), 0),
            test_thread_handle(ThreadId(1), 0),
        ];
        assert_eq!(router.select(&handles), ThreadId(0));
    }
}
