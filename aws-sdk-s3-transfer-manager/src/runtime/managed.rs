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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use aws_smithy_http_client::pool::{
    Authority, ConnectionClosedEvent, ConnectionCreatedEvent, ConnectionEventListener,
    ConnectionFailedEvent, ConnectionReusedEvent, PartitionId, SharedPool,
};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns};
use aws_smithy_runtime_api::client::http::{http_client_fn, HttpClient};
use futures_util::FutureExt;
use tokio_util::sync::CancellationToken;

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

/// Idle connections are evicted after this duration. Set below S3's server-side
/// idle close so a socket the server already dropped is never reused (which
/// would cost a reset + retry).
const POOL_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

std::thread_local! {
    /// Identifies which managed thread the current OS thread corresponds to.
    /// Set once during thread startup, read by the per-thread HTTP client dispatch.
    static MANAGED_THREAD_CPU: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Enables the periodic connection-pool snapshot sampler. Set to a positive
/// interval in milliseconds (e.g. `200`) to spawn the aggregate snapshot task at
/// that cadence. Off (absent / non-positive) by default. The per-connection
/// event listener is installed regardless (its events are TRACE-level on the
/// runtime target); this env only controls the periodic aggregate sampler.
const POOL_SNAPSHOT_ENV: &str = "AWS_S3_TM_POOL_SNAPSHOT_MS";

/// Connection-event listener for the diagnostic snapshot. Records the set of
/// authorities the pool connects to (so the snapshot task knows which to query)
/// and logs per-connection lifecycle at TRACE. The pool's events carry the
/// authority but not the partition, so the per-NIC view is reconstructed by the
/// snapshot task from the partition→NIC map.
struct PoolObserver {
    authorities: Mutex<HashSet<String>>,
}

impl PoolObserver {
    fn new() -> Self {
        Self {
            authorities: Mutex::new(HashSet::new()),
        }
    }

    fn authorities(&self) -> Vec<String> {
        self.authorities
            .lock()
            .expect("pool observer poisoned")
            .iter()
            .cloned()
            .collect()
    }

    fn note(&self, authority: &Authority) {
        let mut set = self.authorities.lock().expect("pool observer poisoned");
        if !set.contains(authority.as_str()) {
            set.insert(authority.as_str().to_string());
        }
    }
}

impl ConnectionEventListener for PoolObserver {
    fn on_created(&self, event: &ConnectionCreatedEvent) {
        self.note(event.authority());
        tracing::trace!(
            target: crate::telemetry::TARGET_RUNTIME,
            authority = event.authority().as_str(),
            remote = ?event.remote_addr(),
            protocol = ?event.protocol(),
            connect_ms = event.timing().connect_duration().as_millis() as u64,
            conn = ?event.conn_id(),
            "pool connection created",
        );
    }

    fn on_reused(&self, event: &ConnectionReusedEvent) {
        tracing::trace!(
            target: crate::telemetry::TARGET_RUNTIME,
            authority = event.authority().as_str(),
            conn = ?event.conn_id(),
            "pool connection reused",
        );
    }

    fn on_closed(&self, event: &ConnectionClosedEvent) {
        tracing::trace!(
            target: crate::telemetry::TARGET_RUNTIME,
            authority = event.authority().as_str(),
            remote = ?event.remote_addr(),
            reason = ?event.reason(),
            error = ?event.error().map(|e| e.to_string()),
            "pool connection closed",
        );
    }

    fn on_connection_failed(&self, event: &ConnectionFailedEvent) {
        // Failures are rare and diagnostic (a NIC whose routing is broken would
        // surface here), so DEBUG rather than TRACE.
        tracing::debug!(
            target: crate::telemetry::TARGET_RUNTIME,
            authority = event.authority().as_str(),
            remote = ?event.remote_addr(),
            error = %event.error(),
            "pool connection failed",
        );
    }
}

/// Inputs for the periodic pool snapshot, bundled so the task owns one value
/// (and the spawn signature stays small).
struct PoolSnapshotState {
    handle: Weak<crate::client::Handle>,
    pool: SharedPool,
    observer: Arc<PoolObserver>,
    partition_nics: HashMap<PartitionId, String>,
    in_flight: Vec<Arc<AtomicUsize>>,
}

/// Spawn the periodic per-NIC connection-pool snapshot on `runtime`. DEBUG: per
/// NIC established/establishing/active + total in-flight work + window-blocked
/// downloads. TRACE: per partition. Stops when `shutdown` is cancelled.
fn spawn_pool_snapshot(
    runtime: &tokio::runtime::Handle,
    state: PoolSnapshotState,
    interval: Duration,
    shutdown: CancellationToken,
) {
    runtime.spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => state.emit(),
            }
        }
    });
}

impl PoolSnapshotState {
    fn emit(&self) {
        let total_in_flight: usize = self
            .in_flight
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        // Downloads currently blocked on their prefetch window (head-of-line).
        // High here with low in-flight = prefetch-window-bound pipeline.
        let window_blocked = self.handle.upgrade().map_or(0, |h| {
            h.telemetry.window_blocked_downloads.load(Ordering::Relaxed)
        });
        let authorities = self.observer.authorities();
        if authorities.is_empty() {
            tracing::debug!(
                target: crate::telemetry::TARGET_RUNTIME,
                in_flight = total_in_flight,
                window_blocked,
                "pool snapshot (no authorities yet)",
            );
            return;
        }
        for auth in &authorities {
            let stats = self.pool.stats(&Authority::from_host(auth.clone()));
            // Aggregate per NIC for DEBUG; emit per-partition at TRACE.
            let mut by_nic: BTreeMap<&str, [usize; 3]> = BTreeMap::new();
            for (pid, s) in stats.iter() {
                let nic = self
                    .partition_nics
                    .get(&pid)
                    .map(String::as_str)
                    .unwrap_or("<unknown>");
                let agg = by_nic.entry(nic).or_default();
                agg[0] += s.established;
                agg[1] += s.establishing;
                agg[2] += s.active;
                tracing::trace!(
                    target: crate::telemetry::TARGET_RUNTIME,
                    authority = auth.as_str(),
                    partition = ?pid,
                    nic,
                    established = s.established,
                    establishing = s.establishing,
                    active = s.active,
                    "pool snapshot (partition)",
                );
            }
            for (nic, [established, establishing, active]) in by_nic {
                tracing::debug!(
                    target: crate::telemetry::TARGET_RUNTIME,
                    authority = auth.as_str(),
                    nic,
                    established,
                    establishing,
                    active,
                    in_flight = total_in_flight,
                    window_blocked,
                    "pool snapshot (nic)",
                );
            }
        }
    }
}

/// One managed OS thread and its tokio current-thread handle.
struct ThreadHandle {
    id: Cpu,
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
    work.descriptor.work_started();

    if work.descriptor.is_terminal() {
        tracing::trace!(target: crate::telemetry::TARGET_RUNTIME, %tid, "skipped (terminal)");
        return ExecuteResult::Completed(WorkOutcome::Cancelled, Duration::ZERO);
    }

    tracing::trace!(target: crate::telemetry::TARGET_RUNTIME, %tid, "executing");
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
            tracing::trace!(target: crate::telemetry::TARGET_RUNTIME, %tid, ?outcome, "completed");
            ExecuteResult::Completed(outcome, started.elapsed())
        }
        Err(_panic) => {
            tracing::error!(target: crate::telemetry::TARGET_RUNTIME, %tid, "panic in transfer execute");
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

        // The connection-event listener is always installed: per-connection
        // lifecycle is TRACE-level observability available via the runtime
        // target without any opt-in. Only the periodic aggregate snapshot is
        // env-gated (its interval), since it's a sampling task with a cadence.
        let snapshot_interval = std::env::var(POOL_SNAPSHOT_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        let pool_observer = Arc::new(PoolObserver::new());

        let dns_resolver = ShufflingDnsResolver::new(aws_smithy_dns::HickoryDnsResolver::default());
        // spawn and initialize concurrently
        let pending: Vec<_> = topology
            .thread_ids()
            .map(|id| {
                let shutdown = shutdown_token.clone();
                let (tx, rx) = std::sync::mpsc::channel();
                let cpu_index = id.0;
                // Pin only when the topology carries real hardware core ids
                // (detected/explicit); synthetic uniform cores are not pinned.
                let pin_to =
                    (pin_threads && topology.pinnable()).then(|| topology.core_for_thread(id));

                #[cfg(feature = "dial9")]
                let telemetry_guard = telemetry_guard.clone();

                let join_handle = std::thread::Builder::new()
                    .name(format!("s3-tm-{}", id))
                    .spawn(move || {
                        if let Some(core) = pin_to {
                            super::topology::pin_current_thread(core);
                        }
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

                        // Send the runtime handle back. The shared pool is built
                        // once all handles are collected, with one partition bound
                        // to this runtime (TokioDriverSpawner::from_handle) so its
                        // connection drivers run on this thread.
                        let _ = tx.send(rt.handle().clone());
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
                let runtime_handle = rx.recv().expect("managed thread failed to start");
                ThreadHandle {
                    id,
                    runtime_handle,
                    join_handle: Mutex::new(Some(join_handle)),
                    in_flight: Arc::new(AtomicUsize::new(0)),
                }
            })
            .collect();

        // Topology + NIC/pinning binding decisions, logged once at construction.
        // DEBUG: a summary plus one line per NUMA node. TRACE: per-thread
        // placement (one line per thread, noisy with many cores). The NIC
        // distribution is the direct signal for whether a configured NIC is
        // assigned to any partition at all.
        {
            let pinned = if pin_threads && topology.pinnable() {
                threads.len()
            } else {
                0
            };
            let mut nic_distribution: BTreeMap<&str, usize> = BTreeMap::new();
            for th in &threads {
                *nic_distribution
                    .entry(topology.nic_for_thread(th.id).unwrap_or("<none>"))
                    .or_default() += 1;
            }
            tracing::debug!(
                target: crate::telemetry::TARGET_RUNTIME,
                threads = threads.len(),
                nodes = topology.num_nodes(),
                pinnable = topology.pinnable(),
                pin_threads,
                pinned,
                nic_distribution = ?nic_distribution,
                "managed runtime topology and binding",
            );
            for node in topology.nodes() {
                tracing::debug!(
                    target: crate::telemetry::TARGET_RUNTIME,
                    node = node.id,
                    cores = node.cores.len(),
                    nics = ?node.nics,
                    "topology node",
                );
            }
            for th in &threads {
                tracing::trace!(
                    target: crate::telemetry::TARGET_RUNTIME,
                    thread = th.id.0,
                    core = topology.core_for_thread(th.id),
                    node = topology.node_for_thread(th.id),
                    pinned = pin_threads && topology.pinnable(),
                    nic = topology.nic_for_thread(th.id).unwrap_or("<none>"),
                    "thread placement",
                );
            }
        }

        // One shared pool, one partition per managed thread (PartitionId == cpu
        // index). `from_handle` spawns each partition's connection driver on that
        // thread's runtime, so a connection stays local to the thread that issues
        // requests on it. A partition binds a NIC only when its node has one
        // (`nic_for_thread`); with no NIC the partition egresses on the default
        // interface.
        //
        // pool_idle_timeout must be set explicitly: the builder default is None
        // (idle connections are never evicted). Connections are uncapped and the
        // default cross-partition policy applies.
        let mut pool_builder = aws_smithy_http_client::pool::SharedPool::builder()
            .dns_resolver(dns_resolver)
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .partitions(threads.iter().map(|th| {
                let partition = aws_smithy_http_client::pool::Partition::new(
                    aws_smithy_http_client::pool::PartitionId::from_index(th.id.0),
                    aws_smithy_http_client::pool::TokioDriverSpawner::from_handle(
                        th.runtime_handle.clone(),
                    ),
                );
                match topology.nic_for_thread(th.id) {
                    Some(nic) => partition.interface(nic),
                    None => partition,
                }
            }));
        pool_builder = pool_builder.connection_event_listener(pool_observer.clone());
        let pool = pool_builder
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .build_https();

        // One per-partition client handle per managed thread; each implements
        // `HttpClient`. The dispatch fn routes a request to the calling thread's
        // partition handle.
        let per_thread_clients: Arc<Vec<aws_smithy_http_client::pool::Client>> = Arc::new(
            threads
                .iter()
                .map(|th| {
                    aws_smithy_http_client::pool::Client::from_partition(
                        &pool,
                        aws_smithy_http_client::pool::PartitionId::from_index(th.id.0),
                    )
                })
                .collect(),
        );

        // Opt-in diagnostic: periodic per-NIC pool snapshot. The partition→NIC
        // map lets the task render the pool's partition-keyed stats per NIC; it
        // runs on the first managed thread and stops on shutdown.
        if let Some(interval) = snapshot_interval {
            let partition_nics = threads
                .iter()
                .map(|th| {
                    (
                        PartitionId::from_index(th.id.0),
                        topology
                            .nic_for_thread(th.id)
                            .unwrap_or("<none>")
                            .to_string(),
                    )
                })
                .collect::<HashMap<_, _>>();
            let in_flight = threads.iter().map(|th| Arc::clone(&th.in_flight)).collect();
            spawn_pool_snapshot(
                &threads[0].runtime_handle,
                PoolSnapshotState {
                    handle: handle.clone(),
                    pool: pool.clone(),
                    observer: pool_observer.clone(),
                    partition_nics,
                    in_flight,
                },
                interval,
                shutdown_token.clone(),
            );
        }

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
    pub(crate) fn topology(mut self, topology: Topology) -> Self {
        self.topology = Some(topology);
        self
    }

    /// Enable thread pinning to cores. Default: false.
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
                target: crate::telemetry::TARGET_RUNTIME,
                %tid,
                thread = thread_id.0,
                "dispatching to managed thread",
            );

            th.runtime_handle.spawn(async move {
                let Some(h) = handle.upgrade() else {
                    return;
                };
                tracing::trace!(
                    target: crate::telemetry::TARGET_RUNTIME,
                    %tid,
                    "execute starting",
                );
                let result = execute_work(&mut work, &h.scheduler).await;
                tracing::trace!(
                    target: crate::telemetry::TARGET_RUNTIME,
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
                    target: crate::telemetry::TARGET_RUNTIME,
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
            num_cores,
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
