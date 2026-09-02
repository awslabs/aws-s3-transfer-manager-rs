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
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns};
use aws_smithy_runtime_api::client::http::{http_client_fn, HttpClient, SharedHttpClient};
use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::topology::Cpu;
use super::Topology;
use super::{ExecutionRuntime, RuntimeComponents, ScheduledWork};
use crate::runtime::sync::SubmissionGuard;
use crate::scheduler::Scheduler;
use crate::transfer::{TransferId, WorkOutcome};

/// DNS resolver that shuffles returned IPs to distribute connections across
/// S3 fleet addresses. Wraps any [`ResolveDns`] implementation.
///
/// hyper tries resolved IPs sequentially, so without shuffling all connections
/// land on the first IP. Shuffling gives each connection a random starting IP,
/// spreading load across all resolved addresses.
#[derive(Debug, Clone)]
struct ShufflingDnsResolver<R> {
    inner: R,
}

impl<R> ShufflingDnsResolver<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: ResolveDns + 'static> ResolveDns for ShufflingDnsResolver<R> {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move {
            let mut ips = self.inner.resolve_dns(name).await?;
            fastrand::shuffle(&mut ips);
            Ok(ips)
        })
    }
}

std::thread_local! {
    /// Identifies which managed thread the current OS thread corresponds to.
    /// Set once during thread startup, read by the per-thread HTTP client dispatch.
    static MANAGED_THREAD_CPU: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// One managed OS thread and its tokio current-thread handle.
struct ThreadHandle {
    id: Cpu,
    runtime_handle: tokio::runtime::Handle,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    in_flight: Arc<AtomicUsize>,
    http_client: SharedHttpClient,
}

impl std::fmt::Debug for ThreadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadHandle")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Selects which managed thread should execute a work item.
///
/// Uses power-of-two-random-choices: pick two threads at random, return the
/// least loaded. O(1) regardless of thread count, low contention on the
/// atomics (only two loads instead of N). Same algorithm CRT uses for
/// event loop selection (`get_next_loop`).
struct DispatchRouter;

impl DispatchRouter {
    /// Select a thread to execute the given work.
    fn select(&self, threads: &[ThreadHandle]) -> Cpu {
        let len = threads.len();
        debug_assert!(len > 0, "no threads available");
        if len == 1 {
            return threads[0].id;
        }
        let a = fastrand::usize(..len);
        let mut b = fastrand::usize(..len - 1);
        if b >= a {
            b += 1;
        }
        let load_a = threads[a].in_flight.load(Ordering::Relaxed);
        let load_b = threads[b].in_flight.load(Ordering::Relaxed);
        if load_a <= load_b {
            threads[a].id
        } else {
            threads[b].id
        }
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
    let work_kind = work.item.kind();
    work.descriptor.work_started();

    if work.descriptor.is_terminal() {
        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = work_kind, "skipped (terminal)");
        return ExecuteResult::Completed(WorkOutcome::Cancelled, Duration::ZERO);
    }

    tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = work_kind, "executing");
    let transfer = work.descriptor.transfer();
    let started = Instant::now();

    let token = transfer.ctx().cancellation_token().clone();
    // Not a child of the span that initiated the transfer: dispatch crosses a
    // thread, so `tid` is what ties this span back to its transfer.
    let outcome = AssertUnwindSafe(async {
        tokio::select! {
            biased;
            _ = token.cancelled() => WorkOutcome::Cancelled,
            outcome = transfer.execute(&mut work.item) => outcome,
        }
    })
    .catch_unwind()
    .instrument(tracing::debug_span!(
        target: crate::telemetry::TARGET_EXECUTION,
        "execute",
        tid = %tid,
        work = work_kind
    ))
    .await;

    match outcome {
        Ok(outcome) => {
            tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = work_kind, ?outcome, "completed");
            ExecuteResult::Completed(outcome, started.elapsed())
        }
        Err(_panic) => {
            tracing::error!(target: crate::telemetry::TARGET_EXECUTION, %tid, work = work_kind, "panic in transfer execute");
            ExecuteResult::Panicked
        }
    }
}

/// Execution runtime backed by per-core OS threads, each running a tokio
/// current-thread runtime.
pub(crate) struct ManagedThreadRuntime {
    handle: Weak<crate::client::Handle>,
    #[allow(dead_code)] // used for topology-aware routing when wired
    topology: Topology,
    #[allow(dead_code)] // used for core pinning when wired
    pin_threads: bool,
    threads: Vec<ThreadHandle>,
    shutdown_token: CancellationToken,
    router: DispatchRouter,
    components: RuntimeComponents,
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
    pub(crate) fn builder(handle: Weak<crate::client::Handle>) -> ManagedThreadRuntimeBuilder {
        ManagedThreadRuntimeBuilder::new(handle)
    }

    /// Create a new managed thread runtime.
    ///
    /// Spawns one OS thread per core in the topology. Each thread creates its
    /// own tokio current-thread runtime (the I/O driver binds to the creating
    /// thread).
    fn new(
        handle: Weak<crate::client::Handle>,
        topology: Topology,
        pin_threads: bool,
        #[cfg(feature = "dial9")] telemetry_guard: Option<
            std::sync::Arc<dial9_tokio_telemetry::telemetry::TelemetryGuard>,
        >,
    ) -> Self {
        let shutdown_token = CancellationToken::new();

        let dns_resolver = ShufflingDnsResolver::new(aws_smithy_dns::HickoryDnsResolver::default());
        // spawn and initialize concurrently
        let pending: Vec<_> = topology
            .thread_ids()
            .map(|id| {
                let shutdown = shutdown_token.clone();
                let (tx, rx) = std::sync::mpsc::channel();
                let cpu_index = id.0;

                let resolver = dns_resolver.clone();
                #[cfg(feature = "dial9")]
                let telemetry_guard = telemetry_guard.clone();

                let join_handle = std::thread::Builder::new()
                    .name(format!("s3-tm-{}", id))
                    .spawn(move || {
                        let mut builder = tokio::runtime::Builder::new_current_thread();
                        builder.enable_all().max_blocking_threads(1);

                        #[cfg(feature = "dial9")]
                        let rt = if let Some(ref guard) = telemetry_guard {
                            let (rt, _handle) = guard
                                .trace_runtime(format!("s3-tm-{}", cpu_index))
                                .build(builder)
                                .expect("failed to create traced tokio runtime");
                            rt
                        } else {
                            builder
                                .build()
                                .expect("failed to create tokio current-thread runtime")
                        };

                        #[cfg(not(feature = "dial9"))]
                        let rt = builder
                            .build()
                            .expect("failed to create tokio current-thread runtime");

                        // Create per-thread HTTP client on this thread's runtime.
                        // The TLS connector and connection pool bind to this thread's reactor.
                        let http_client = aws_smithy_http_client::Builder::new()
                            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                                aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
                            ))
                            // .tls_provider(aws_smithy_http_client::tls::Provider::S2nTls)
                            .build_with_resolver(resolver);

                        let _ = tx.send((rt.handle().clone(), http_client));
                        MANAGED_THREAD_CPU.set(Some(cpu_index));
                        rt.block_on(shutdown.cancelled());
                    })
                    .expect("failed to spawn managed thread");

                (id, rx, join_handle)
            })
            .collect();

        let threads: Vec<_> = pending
            .into_iter()
            .map(|(id, rx, join_handle)| {
                let (runtime_handle, http_client) =
                    rx.recv().expect("managed thread failed to start");
                ThreadHandle {
                    id,
                    runtime_handle,
                    join_handle: Mutex::new(Some(join_handle)),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                    http_client,
                }
            })
            .collect();

        // Build an http_client_fn that dispatches to the per-thread HTTP client
        // based on which managed thread is calling.
        let per_thread_clients: Arc<Vec<SharedHttpClient>> =
            Arc::new(threads.iter().map(|th| th.http_client.clone()).collect());

        let shared_http_client = http_client_fn(move |settings, components| {
            let cpu_index = MANAGED_THREAD_CPU
                .with(|c| c.get())
                .expect("http_client_fn called from non-managed thread");
            per_thread_clients[cpu_index].http_connector(settings, components)
        });

        let mut components = RuntimeComponents::default();
        components.set_http_client(shared_http_client);
        components.set_direct_io(true);

        Self {
            handle,
            topology,
            pin_threads,
            threads,
            shutdown_token,
            router: DispatchRouter,
            components,
        }
    }
}

/// Builder for [`ManagedThreadRuntime`].
pub(crate) struct ManagedThreadRuntimeBuilder {
    handle: Weak<crate::client::Handle>,
    topology: Option<Topology>,
    pin_threads: bool,
    #[cfg(feature = "dial9")]
    telemetry_guard: Option<std::sync::Arc<dial9_tokio_telemetry::telemetry::TelemetryGuard>>,
}

impl ManagedThreadRuntimeBuilder {
    fn new(handle: Weak<crate::client::Handle>) -> Self {
        Self {
            handle,
            topology: None,
            pin_threads: false,
            #[cfg(feature = "dial9")]
            telemetry_guard: None,
        }
    }

    /// Set a dial9 telemetry guard for tracing per-thread runtimes.
    #[cfg(feature = "dial9")]
    pub(crate) fn telemetry_guard(
        mut self,
        guard: std::sync::Arc<dial9_tokio_telemetry::telemetry::TelemetryGuard>,
    ) -> Self {
        self.telemetry_guard = Some(guard);
        self
    }

    /// Set the hardware topology. Defaults to `Topology::uniform(num_cpus)`
    /// where `num_cpus` is detected at build time.
    #[allow(dead_code)] // TODO: expose on public config
    pub(crate) fn topology(mut self, topology: Topology) -> Self {
        self.topology = Some(topology);
        self
    }

    /// Enable thread pinning to cores. Default: false.
    #[allow(dead_code)]
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
        ManagedThreadRuntime::new(
            self.handle,
            topology,
            self.pin_threads,
            #[cfg(feature = "dial9")]
            self.telemetry_guard,
        )
    }
}

impl ExecutionRuntime for ManagedThreadRuntime {
    fn dispatch(&self, batch: &mut SubmissionGuard<'_, ScheduledWork>) {
        for mut work in batch.drain() {
            let thread_id = self.router.select(&self.threads);
            let th = &self.threads[thread_id.0];
            th.in_flight.fetch_add(1, Ordering::Relaxed);

            let handle = self.handle.clone();
            let in_flight = Arc::clone(&th.in_flight);
            let tid = work.descriptor.id();

            tracing::trace!(
                target: crate::telemetry::TARGET_EXECUTION,
                %tid,
                thread = thread_id.0,
                "dispatching to managed thread",
            );

            th.runtime_handle.spawn(async move {
                let Some(h) = handle.upgrade() else {
                    return;
                };
                tracing::trace!(
                    target: crate::telemetry::TARGET_EXECUTION,
                    %tid,
                    "execute starting",
                );
                let result = execute_work(&mut work, &h.scheduler).await;
                tracing::trace!(
                    target: crate::telemetry::TARGET_EXECUTION,
                    %tid,
                    "execute finished, entering on_completion",
                );
                in_flight.fetch_sub(1, Ordering::Relaxed);
                match result {
                    ExecuteResult::Completed(outcome, elapsed) => {
                        h.scheduler.on_completion(work, outcome, elapsed);
                    }
                    ExecuteResult::Panicked => {
                        h.scheduler.on_panic(work);
                    }
                }
                tracing::trace!(
                    target: crate::telemetry::TARGET_EXECUTION,
                    %tid,
                    "on_completion returned",
                );
            });
        }
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

    fn components(&self) -> &RuntimeComponents {
        &self.components
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

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use crate::runtime::Topology;

    /// Create a [`ThreadHandle`] with a preset in-flight count for testing
    /// router selection. No real work is spawned — only `id` and `in_flight`
    /// are meaningful.
    fn test_thread_handle(
        id: Cpu,
        in_flight_count: usize,
    ) -> (ThreadHandle, tokio::runtime::Runtime) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let th = ThreadHandle {
            id,
            runtime_handle: rt.handle().clone(),
            join_handle: Mutex::new(None),
            in_flight: Arc::new(AtomicUsize::new(in_flight_count)),
            http_client: http_client_fn(|_, _| unreachable!("test http client")),
        };
        (th, rt)
    }

    fn test_handle(num_cores: usize) -> (Arc<crate::client::Handle>, Arc<ManagedThreadRuntime>) {
        let rt_holder: Arc<std::sync::OnceLock<Arc<ManagedThreadRuntime>>> =
            Arc::new(std::sync::OnceLock::new());
        let rt_holder2 = rt_holder.clone();
        let handle = crate::client::Handle::new_for_test_with_runtime(
            crate::Config::builder()
                .client(aws_smithy_mocks::mock_client!(aws_sdk_s3, []))
                .build(),
            Arc::new(crate::scheduler::FixedConcurrency::new(num_cores)),
            move |weak| {
                let rt = Arc::new(
                    ManagedThreadRuntime::builder(weak)
                        .topology(Topology::uniform(num_cores))
                        .build(),
                );
                rt_holder2.set(rt.clone()).ok();
                rt
            },
        );
        let rt = rt_holder.get().unwrap().clone();
        (handle, rt)
    }

    #[test]
    fn threads_start_and_shutdown() {
        let (handle, rt) = test_handle(4);
        assert_eq!(rt.threads.len(), 4);
        rt.shutdown();
        drop(rt);
        drop(handle);
    }

    #[test]
    fn shutdown_is_idempotent() {
        let (handle, rt) = test_handle(2);
        rt.shutdown();
        rt.shutdown();
        drop(rt);
        drop(handle);
    }

    #[test]
    fn drop_without_shutdown() {
        let (_handle, rt) = test_handle(3);
        drop(rt);
    }

    #[test]
    fn router_selects_least_loaded() {
        let router = DispatchRouter;
        let (h0, _r0) = test_thread_handle(Cpu(0), 5);
        let (h1, _r1) = test_thread_handle(Cpu(1), 2);
        let (h2, _r2) = test_thread_handle(Cpu(2), 8);
        let (h3, _r3) = test_thread_handle(Cpu(3), 3);
        let handles = vec![h0, h1, h2, h3];
        // Power-of-two: picks 2 random threads, returns least loaded.
        // Over many iterations, should never pick the most loaded (Cpu(2)=8)
        // when a lighter option exists.
        let mut selected = std::collections::HashMap::new();
        for _ in 0..1000 {
            let cpu = router.select(&handles);
            *selected.entry(cpu).or_insert(0u32) += 1;
        }
        // Cpu(1) with load=2 should be selected most often
        assert!(
            selected.get(&Cpu(1)).copied().unwrap_or(0)
                > selected.get(&Cpu(2)).copied().unwrap_or(0),
            "least loaded thread should be selected more than most loaded: {selected:?}"
        );
    }

    #[test]
    fn router_single_thread() {
        let router = DispatchRouter;
        let (h0, _r0) = test_thread_handle(Cpu(0), 5);
        let handles = vec![h0];
        assert_eq!(router.select(&handles), Cpu(0));
    }

    #[test]
    fn router_two_threads_prefers_lighter() {
        let router = DispatchRouter;
        let (h0, _r0) = test_thread_handle(Cpu(0), 10);
        let (h1, _r1) = test_thread_handle(Cpu(1), 0);
        let handles = vec![h0, h1];
        // With only 2 threads, power-of-two always picks both, returns lighter
        for _ in 0..100 {
            assert_eq!(router.select(&handles), Cpu(1));
        }
    }
}
