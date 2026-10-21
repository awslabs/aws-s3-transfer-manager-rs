/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer execution on owned managed partitions.

mod http;

use aws_smithy_execution::{ManagedRuntime, ManagedRuntimeHandle, RuntimePartition};
use aws_smithy_http_client::pool::{self, ConnectionPool, DriverSpawner, Partition, PartitionId};
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        dns::{DnsFuture, ResolveDns},
        http::SharedHttpClient,
    },
    shared::IntoShared,
};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use self::http::ManagedHttpClient;
use super::{ExecutionRuntime, RuntimeComponents, RuntimeHttpOptions, ScheduledWork};
use crate::runtime::sync::SubmissionGuard;
use crate::scheduler::Scheduler;
use crate::transfer::{TransferId, WorkOutcome};

/// DNS resolver that shuffles returned IPs to distribute connections across
/// S3 fleet addresses. Wraps any [`ResolveDns`] implementation.
///
/// hyper tries resolved IPs sequentially, so without shuffling all connections
/// land on the first IP. Shuffling gives each connection a random starting IP,
/// spreading load across all resolved addresses. The connection pool's
/// transport has the same sequential behavior, so shuffling still applies.
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

/// How long a pooled connection may sit idle before the pool closes it.
///
/// S3 closes idle connections server-side. Retiring them first avoids reusing a
/// connection the server is closing, which fails before the request is accepted
/// and costs a retry on a fresh connection.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(15);

/// Build one connection pool with a partition per managed thread and return an
/// HTTP client that dispatches each request through the calling thread's
/// partition.
///
/// Each partition spawns connection establishment, protocol drivers, and idle
/// maintenance on its thread's runtime, so a connection's I/O stays on the
/// thread that opened it. Pool partition indices match execution partition indices.
///
/// When network interfaces are configured, each partition binds its
/// connections to the interface [`partition_interface`] assigns its index.
///
/// Proxy selection follows the standard environment variables (`HTTP_PROXY`,
/// `HTTPS_PROXY`, `ALL_PROXY`, `NO_PROXY`), matching the SDK's default HTTPS
/// client.
///
/// Returns native pool construction and partition-client errors.
fn build_http_client(
    execution: ManagedRuntimeHandle,
    options: &RuntimeHttpOptions,
) -> Result<SharedHttpClient, BoxError> {
    #[cfg(target_os = "android")]
    let dns_resolver = ShufflingDnsResolver::new(
        // Hickory reads /etc/resolv.conf on Unix. Android routes libc
        // resolution through netd and does not provide that file.
        aws_smithy_runtime::client::dns::TokioDnsResolver::new(),
    );
    #[cfg(not(target_os = "android"))]
    let dns_resolver = ShufflingDnsResolver::new(aws_smithy_dns::HickoryDnsResolver::default());

    let partitions = execution.partitions().iter().map(|target| {
        let index = target.id().index();
        let target = target.clone();
        let spawner = DriverSpawner::from_fn(move |task| {
            drop(target.spawn(task));
        });
        let partition = Partition::new(PartitionId::from_index(index), spawner);
        bind_interface(partition, index, &options.network_interfaces)
    });
    let pool = ConnectionPool::builder()
        .dns_resolver(dns_resolver)
        .idle_timeout(POOL_IDLE_TIMEOUT)
        .max_connections_per_host(options.max_connections_per_host)
        .proxy_config(ProxyConfig::from_env())
        .partitions(partitions)
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .build_https()?;
    let clients = execution
        .partitions()
        .iter()
        .map(|target| {
            pool::Client::from_partition(&pool, PartitionId::from_index(target.id().index()))
        })
        .collect::<Result<Arc<[_]>, _>>()?;
    Ok(ManagedHttpClient::new(execution, clients).into_shared())
}

/// Bind `partition` to the interface assigned to its execution index.
#[cfg(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn bind_interface(partition: Partition, index: usize, interfaces: &[String]) -> Partition {
    match partition_interface(index, interfaces) {
        Some(interface) => partition.interface(interface),
        None => partition,
    }
}

/// Interface binding is unavailable on this platform, and the public setters
/// that populate `interfaces` are not compiled.
#[cfg(not(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
)))]
fn bind_interface(partition: Partition, _index: usize, _interfaces: &[String]) -> Partition {
    partition
}

/// The network interface a managed thread's partition binds to: interfaces are
/// assigned to threads round-robin in configuration order. `None` when no
/// interfaces are configured, leaving selection to OS routing.
///
/// Threads are not pinned to NUMA nodes, so assignment cannot follow NIC
/// locality; round-robin spreads threads evenly across interfaces.
#[cfg(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn partition_interface(index: usize, interfaces: &[String]) -> Option<&str> {
    if interfaces.is_empty() {
        return None;
    }
    Some(&interfaces[index % interfaces.len()])
}

/// Selects the less loaded of two randomly sampled partitions.
struct DispatchRouter;

impl DispatchRouter {
    fn select(&self, loads: &[Arc<AtomicUsize>]) -> usize {
        let len = loads.len();
        assert!(len > 0, "no execution partitions");
        if len == 1 {
            return 0;
        }
        let a = fastrand::usize(..len);
        let mut b = fastrand::usize(..len - 1);
        if b >= a {
            b += 1;
        }
        if loads[a].load(Ordering::Relaxed) <= loads[b].load(Ordering::Relaxed) {
            a
        } else {
            b
        }
    }
}

/// Accounts for a submission until execution finishes or its future is dropped.
struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    fn acquire(load: Arc<AtomicUsize>) -> Self {
        load.fetch_add(1, Ordering::Relaxed);
        Self(load)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
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
        tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, "skipped (terminal)");
        return ExecuteResult::Completed(WorkOutcome::Cancelled, Duration::ZERO);
    }

    tracing::trace!(target: crate::telemetry::TARGET_EXECUTION, %tid, "executing");
    let transfer = work.descriptor.transfer();
    let started = Instant::now();

    let token = work.descriptor.cancellation_token().clone();
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

/// Owns managed execution while retaining TM-local dispatch and accounting.
pub(crate) struct ManagedExecution {
    handle: Weak<crate::client::Handle>,
    owner: Mutex<Option<ManagedRuntime>>,
    partitions: Box<[RuntimePartition]>,
    in_flight: Box<[Arc<AtomicUsize>]>,
    router: DispatchRouter,
    components: RuntimeComponents,
}

impl std::fmt::Debug for ManagedExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedExecution")
            .field("partitions", &self.partitions)
            .finish_non_exhaustive()
    }
}

/// Builds only the transport that will be installed, before client initialization.
pub(crate) fn prepare_components(
    owner: &ManagedRuntime,
    http: Option<RuntimeHttpOptions>,
) -> Result<RuntimeComponents, BoxError> {
    let http_client = match http {
        Some(options) => Some(build_http_client(owner.handle(), &options)?),
        None => None,
    };
    Ok(RuntimeComponents::managed(http_client))
}

impl ManagedExecution {
    pub(crate) fn new(
        handle: Weak<crate::client::Handle>,
        owner: ManagedRuntime,
        components: RuntimeComponents,
    ) -> Self {
        let partitions: Box<[_]> = owner.handle().partitions().to_vec().into_boxed_slice();
        let in_flight = partitions
            .iter()
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        Self {
            handle,
            owner: Mutex::new(Some(owner)),
            partitions,
            in_flight,
            router: DispatchRouter,
            components,
        }
    }

    #[cfg(test)]
    pub(crate) fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    #[cfg(test)]
    pub(crate) fn for_test(handle: Weak<crate::client::Handle>, max_partitions: usize) -> Self {
        let topology = aws_smithy_execution::topology::Topology::detect()
            .unwrap()
            .with_max_placements(max_partitions);
        let owner = ManagedRuntime::builder()
            .topology(topology)
            .pin_threads(false)
            .build()
            .unwrap();
        let components = prepare_components(&owner, None).unwrap();
        Self::new(handle, owner, components)
    }
}

impl ExecutionRuntime for ManagedExecution {
    fn dispatch(&self, batch: &mut SubmissionGuard<'_, ScheduledWork>) {
        for mut work in batch.drain() {
            let index = self.router.select(&self.in_flight);
            let flight = InFlight::acquire(self.in_flight[index].clone());
            let handle = self.handle.clone();
            let tid = work.descriptor.id();
            tracing::trace!(
                target: crate::telemetry::TARGET_EXECUTION,
                %tid,
                partition = index,
                "dispatching to managed partition",
            );
            let task = self.partitions[index].spawn(async move {
                let flight = flight;
                let Some(h) = handle.upgrade() else {
                    return;
                };
                let result = execute_work(&mut work, &h.scheduler).await;
                drop(flight);
                match result {
                    ExecuteResult::Completed(outcome, elapsed) => {
                        h.scheduler.on_completion(work, outcome, elapsed);
                    }
                    ExecuteResult::Panicked => {
                        h.scheduler.on_panic(work);
                    }
                }
            });
            drop(task);
        }
    }

    fn shutdown(&self) {
        let owner = self
            .owner
            .lock()
            .expect("managed owner lock poisoned")
            .take();
        drop(owner);
    }

    fn remove_pending_for_transfer(&self, _id: TransferId) -> usize {
        // Submitted tasks check terminal state and transfer cancellation.
        0
    }

    fn components(&self) -> &RuntimeComponents {
        &self.components
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;

    #[cfg(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    ))]
    #[test]
    fn partition_interface_assigns_round_robin() {
        let interfaces = ["ens5".to_string(), "ens6".to_string()];
        let assigned: Vec<_> = (0..5)
            .map(|i| partition_interface(i, &interfaces))
            .collect();
        assert_eq!(
            assigned,
            [
                Some("ens5"),
                Some("ens6"),
                Some("ens5"),
                Some("ens6"),
                Some("ens5")
            ]
        );
    }

    #[cfg(any(
        target_os = "android",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "solaris",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
    ))]
    #[test]
    fn partition_interface_unbound_without_interfaces() {
        assert_eq!(partition_interface(0, &[]), None);
        assert_eq!(partition_interface(3, &[]), None);
    }

    fn loads(values: &[usize]) -> Vec<Arc<AtomicUsize>> {
        values
            .iter()
            .map(|value| Arc::new(AtomicUsize::new(*value)))
            .collect()
    }

    fn test_handle(max_partitions: usize) -> (Arc<crate::client::Handle>, Arc<ManagedExecution>) {
        let holder = Arc::new(std::sync::OnceLock::new());
        let captured = holder.clone();
        let handle = crate::client::Handle::new_for_test_with_runtime(
            crate::Config::builder()
                .client(aws_smithy_mocks::mock_client!(aws_sdk_s3, []))
                .build(),
            Arc::new(crate::scheduler::FixedConcurrency::new(max_partitions)),
            move |weak| {
                let execution = Arc::new(ManagedExecution::for_test(weak, max_partitions));
                captured.set(execution.clone()).ok();
                execution
            },
        );
        let execution = holder.get().unwrap().clone();
        (handle, execution)
    }

    #[test]
    fn partitions_start_and_shutdown() {
        let (handle, execution) = test_handle(4);
        assert!(!execution.partitions.is_empty());
        assert!(execution.partitions.len() <= 4);
        let expected = execution
            .owner
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .handle()
            .topology()
            .placements()
            .len();
        assert_eq!(execution.partitions.len(), expected);
        execution.shutdown();
        drop(execution);
        drop(handle);
    }

    #[test]
    fn shutdown_is_idempotent() {
        let (handle, execution) = test_handle(2);
        execution.shutdown();
        execution.shutdown();
        assert!(execution.owner.lock().unwrap().is_none());
        drop(execution);
        drop(handle);
    }

    #[test]
    fn drop_without_shutdown() {
        let owner = ManagedRuntime::builder()
            .pin_threads(false)
            .build()
            .unwrap();
        let partition = owner.handle().partitions()[0].clone();
        let execution = ManagedExecution::new(Weak::new(), owner, RuntimeComponents::default());
        let (tx, rx) = std::sync::mpsc::channel();
        let finished = Completion(tx);
        let task = partition.spawn(async move {
            let _finished = finished;
            std::future::pending::<()>().await;
        });
        drop(execution);
        drop(task);
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    struct Completion(std::sync::mpsc::Sender<()>);
    impl Drop for Completion {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn router_selects_least_loaded() {
        let router = DispatchRouter;
        let loads = loads(&[5, 2, 8, 3]);
        let mut selected = [0usize; 4];
        for _ in 0..1000 {
            selected[router.select(&loads)] += 1;
        }
        assert_eq!(selected[2], 0);
        assert!(selected[1] > selected[2]);
    }

    #[test]
    fn router_single_partition() {
        assert_eq!(DispatchRouter.select(&loads(&[5])), 0);
    }

    #[test]
    fn router_two_partitions_prefers_lighter() {
        let loads = loads(&[10, 0]);
        for _ in 0..100 {
            assert_eq!(DispatchRouter.select(&loads), 1);
        }
    }

    #[test]
    fn cancelled_submission_releases_load() {
        let owner = ManagedRuntime::builder()
            .pin_threads(false)
            .build()
            .unwrap();
        let load = Arc::new(AtomicUsize::new(0));
        let flight = InFlight::acquire(load.clone());
        let task = owner.handle().partitions()[0].spawn(async move {
            let _flight = flight;
            std::future::pending::<()>().await;
        });
        assert_eq!(load.load(Ordering::Relaxed), 1);
        task.abort();
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                assert!(task.await.unwrap_err().is_cancelled());
            });
        assert_eq!(load.load(Ordering::Relaxed), 0);
    }
}
