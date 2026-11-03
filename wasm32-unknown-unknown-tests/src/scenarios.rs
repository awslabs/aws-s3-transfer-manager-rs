/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer scenarios against the mock S3 server, through the host.
//!
//! [`start`] begins one scenario and the host drives it with [`step`]. Each
//! scenario uses a fresh [`Store`] and client, then drops the client and runs
//! the executor until idle. Every worker the client spawned must have exited
//! by then.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::IdentityCache;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::memory::{BufferPool, BufferPoolBuildError};
use aws_sdk_s3_transfer_manager::types::{
    ConcurrencyMode, MemoryBudgetConfig, MemoryConfig, PartSize, TransferInstant,
};
use aws_sdk_s3_transfer_manager::wasm::{BoxFuture, SharedWasmRuntime, WasmRuntime};
use aws_sdk_s3_transfer_manager::{Client, Config, RuntimeConfig};
use aws_smithy_runtime_api::client::http::SharedHttpConnector;
use aws_types::sdk_config::{StalledStreamProtectionConfig, TimeoutConfig};
use bytes::Bytes;

use crate::host::{self, Driver, HostRuntime, Progress};
use crate::host_http::{self, HostHttp};
use crate::store::{sdk_config, HoldRule, Store};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// The automatic memory limit on this target.
const AUTO_MEMORY_BYTES: u64 = 64 * MIB as u64;

/// Epoch flushes that advance a single-participant collector far enough to
/// destroy garbage deferred before the first flush.
const EPOCH_FLUSHES: usize = 4;

/// The endpoint of clients that never send a request.
const UNUSED_ENDPOINT: &str = "http://unused.invalid";

/// Returns `Err(message)` from the enclosing scenario when `condition` fails.
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

type ScenarioFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;

/// Starts a scenario against the server at the endpoint it receives.
type Scenario = fn(String) -> ScenarioFuture;

/// Scenarios by index.
const SCENARIOS: &[(&str, Scenario)] = &[
    ("per-client runtimes", |e| Box::pin(per_client_runtimes(e))),
    ("explicit buffer pool", |e| {
        Box::pin(explicit_buffer_pool(e))
    }),
    ("client construction", |e| Box::pin(client_construction(e))),
    ("single-part round trip", |e| {
        Box::pin(single_part_round_trip(e))
    }),
    ("multipart round trip", |e| {
        Box::pin(multipart_round_trip(e))
    }),
    ("outer retry", |e| Box::pin(outer_retry(e))),
    ("hedge deadline", |e| Box::pin(hedge_deadline(e))),
    ("cancellation", |e| Box::pin(cancellation(e))),
    ("host cancellation", |e| Box::pin(host_cancellation(e))),
    ("high fan-out", |e| Box::pin(high_fan_out(e))),
    ("filesystem unavailable", |e| {
        Box::pin(filesystem_unavailable(e))
    }),
];

/// Number of scenarios.
pub(crate) fn scenario_count() -> usize {
    SCENARIOS.len()
}

/// The scenario that [`step`] drives.
struct Running {
    name: &'static str,
    driver: Driver<Result<String, String>>,
}

static RUNNING: Mutex<Option<Running>> = Mutex::new(None);

/// Starts scenario `index` against the server at `endpoint`.
///
/// Returns an error if the index is out of range or a scenario is running.
pub(crate) fn start(index: usize, endpoint: String) -> Result<(), String> {
    let mut running = RUNNING.lock().unwrap();
    if let Some(running) = &*running {
        return Err(format!("scenario {:?} is running", running.name));
    }
    let (name, scenario) = SCENARIOS
        .get(index)
        .ok_or_else(|| format!("no scenario {index}; there are {}", SCENARIOS.len()))?;
    *running = Some(Running {
        name,
        driver: Driver::new(scenario(endpoint)),
    });
    Ok(())
}

/// Outcome of one [`step`].
pub(crate) enum Step {
    /// The scenario passed and reported `PASS <scenario>: <summary>`.
    Passed,
    /// The scenario waits for a host response or, if present, this deadline
    /// on the host's monotonic clock.
    Waiting(Option<Duration>),
}

/// Drives the running scenario until nothing can progress.
///
/// Returns `FAIL <scenario>: <reason>` if the scenario fails or stalls, which
/// it does when it awaits neither a timer nor a host response.
pub(crate) fn step() -> Result<Step, String> {
    let mut slot = RUNNING.lock().unwrap();
    let running = slot.as_mut().ok_or("no scenario is running")?;
    let name = running.name;
    let fail = |message: String| format!("FAIL {name}: {message}");
    match running.driver.drive().map_err(fail)? {
        Progress::Complete(result) => {
            *slot = None;
            let summary = result.map_err(fail)?;
            if host_http::awaiting_host() {
                return Err(fail("requests await responses after the scenario".into()));
            }
            crate::report(&format!("PASS {name}: {summary}"));
            Ok(Step::Passed)
        }
        Progress::Waiting(None) if !host_http::awaiting_host() => Err(fail(format!(
            "stalled: no task is ready, no timer is live, and no request awaits a response ({:?}, {:?})",
            host::stats(),
            host_http::stats()
        ))),
        Progress::Waiting(deadline) => Ok(Step::Waiting(deadline)),
    }
}

/// Executor and request counters at one moment.
#[derive(Clone, Copy, Debug)]
struct Snapshot {
    host: host::Stats,
    http: host_http::Stats,
}

fn snapshot() -> Snapshot {
    Snapshot {
        host: host::stats(),
        http: host_http::stats(),
    }
}

/// Bytes whose value depends on their offset, so a misplaced range is detected.
fn pattern(len: usize) -> Bytes {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add((i >> 16) as u8))
        .collect()
}

async fn upload(client: &Client, key: &str, data: Bytes) -> Result<(), String> {
    client
        .upload()
        .bucket("bucket")
        .key(key)
        .body(InputStream::from(data))
        .initiate()
        .map_err(|e| format!("initiate upload: {e}"))?
        .join()
        .await
        .map(drop)
        .map_err(|e| format!("upload: {e}"))
}

async fn download(client: &Client, key: &str) -> Result<Vec<u8>, String> {
    let mut handle = client
        .download()
        .bucket("bucket")
        .key(key)
        .initiate()
        .map_err(|e| format!("initiate download: {e}"))?;
    let mut data = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        let chunk = chunk.map_err(|e| format!("download chunk: {e}"))?;
        data.extend_from_slice(&chunk.data.into_contiguous());
    }
    handle.join().await.map_err(|e| format!("download: {e}"))?;
    Ok(data)
}

fn ensure_same(expected: &[u8], actual: &[u8]) -> Result<(), String> {
    ensure!(
        expected.len() == actual.len(),
        "received {} bytes, expected {}",
        actual.len(),
        expected.len()
    );
    if let Some(offset) = expected.iter().zip(actual).position(|(a, b)| a != b) {
        return Err(format!("bytes differ at offset {offset}"));
    }
    Ok(())
}

/// Drops `client`, runs the executor until idle, and verifies that every
/// worker the scenario spawned has exited and that every request sent to the
/// host returned `Pending` at least once. Returns a summary of the requests
/// `store` recorded and the workers that exited.
async fn shut_down(client: Client, before: Snapshot, store: &Store) -> Result<String, String> {
    let workers = release(client, before).await?;
    let after = host_http::stats();
    let sent = after.sent - before.http.sent;
    let pending = after.pending_polls - before.http.pending_polls;
    ensure!(
        pending >= sent,
        "{sent} requests were sent to the host but only {pending} polls returned Pending"
    );
    Ok(format!(
        "{store}; {workers} tasks exited after the client was dropped"
    ))
}

/// Drops `client`, runs the executor until idle, and verifies that every
/// worker spawned since `before` has exited. Returns how many of those workers
/// were live when the client was dropped.
///
/// Workers exit when the client's shared state drops. The scheduler's
/// lock-free sets hand removed transfer descriptors, which reference that
/// state, to crossbeam-epoch, which destroys them only after the epoch
/// advances. Flushing the epoch here makes the drop deterministic; worker exit
/// itself still depends on the executor waking each worker's queue
/// notification.
async fn release(client: Client, before: Snapshot) -> Result<usize, String> {
    let workers = host::stats()
        .live_tasks
        .saturating_sub(before.host.live_tasks);
    drop(client);
    for _ in 0..EPOCH_FLUSHES {
        crossbeam_epoch::pin().flush();
    }
    host::idle().await;
    let after = host::stats();
    ensure!(
        after.live_tasks == before.host.live_tasks,
        "{} spawned futures remain live after the client was dropped",
        after.live_tasks - before.host.live_tasks
    );
    Ok(workers)
}

/// A runtime on the host's executor whose clock is offset from the host's
/// monotonic clock, and which counts the futures it accepts.
#[derive(Debug)]
struct OffsetRuntime {
    offset: Duration,
    spawned: Arc<AtomicUsize>,
}

impl WasmRuntime for OffsetRuntime {
    fn monotonic_now(&self) -> Duration {
        HostRuntime.monotonic_now() + self.offset
    }

    fn spawn(&self, future: BoxFuture) {
        self.spawned.fetch_add(1, Ordering::SeqCst);
        HostRuntime.spawn(future);
    }
}

/// Two clients with their own runtimes run their workers on, and timestamp
/// their transfers with, their own runtime.
async fn per_client_runtimes(endpoint: String) -> Result<String, String> {
    const OFFSETS: [Duration; 2] = [Duration::ZERO, Duration::from_secs(3600)];
    let before = snapshot();
    let spawned = OFFSETS.map(|_| Arc::new(AtomicUsize::new(0)));
    let runtimes: Vec<_> = OFFSETS
        .iter()
        .zip(&spawned)
        .map(|(&offset, spawned)| {
            SharedWasmRuntime::new(OffsetRuntime {
                offset,
                spawned: Arc::clone(spawned),
            })
        })
        .collect();
    let stores = OFFSETS.map(|_| Store::new(&endpoint));
    let mut clients = Vec::new();
    for (index, runtime) in runtimes.iter().enumerate() {
        let client = stores[index]
            .client(|config| config.runtime_config(RuntimeConfig::wasm(runtime.clone())));
        // The transfer's start time must come from this client's runtime,
        // whose clock is an hour away from the other's.
        let earliest = TransferInstant::now(runtime);
        let handle = client
            .upload()
            .bucket("bucket")
            .key(format!("runtime-{index}"))
            .body(InputStream::from(pattern(64 * KIB)))
            .initiate()
            .map_err(|e| format!("initiate upload {index}: {e}"))?;
        let started_at = handle.metrics().started_at;
        ensure!(
            earliest <= started_at && started_at <= TransferInstant::now(runtime),
            "client {index} did not timestamp its transfer on its own runtime"
        );
        handle
            .join()
            .await
            .map_err(|e| format!("upload {index}: {e}"))?;
        ensure!(
            spawned[index].load(Ordering::SeqCst) > 0,
            "client {index} spawned no worker on its own runtime"
        );
        clients.push(client);
    }
    let second = clients.pop().expect("two clients");
    drop(clients);
    let summary = shut_down(second, before, &stores[1]).await?;
    Ok(format!(
        "{} and {} futures on separate runtimes; {summary}",
        spawned[0].load(Ordering::SeqCst),
        spawned[1].load(Ordering::SeqCst),
    ))
}

/// A caller-built pool requires a runtime, and a client whose memory is that
/// pool admits transfers against its capacity.
async fn explicit_buffer_pool(endpoint: String) -> Result<String, String> {
    const CAPACITY: usize = 16 * MIB;
    let builder = BufferPool::builder().memory_budget(MemoryBudgetConfig::Limit(CAPACITY));
    ensure!(
        matches!(
            builder.clone().build(),
            Err(BufferPoolBuildError::MissingWasmRuntime)
        ),
        "a pool built without a runtime"
    );
    let pool = builder
        .wasm_runtime(HostRuntime)
        .build()
        .map_err(|e| format!("build pool: {e}"))?;

    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config.memory(MemoryConfig::Explicit(pool.clone())));
    let data = pattern(64 * KIB);
    upload(&client, "pooled", data.clone()).await?;
    ensure_same(&data, &download(&client, "pooled").await?)?;
    let capacity = pool.metrics().configured_capacity_bytes();
    ensure!(
        capacity == CAPACITY as u64,
        "pool capacity is {capacity} bytes, expected {CAPACITY}"
    );
    shut_down(client, before, &store).await
}

/// A client builds from explicit capabilities and default transfer-manager
/// configuration, and its automatic memory limit is the target's fallback.
async fn client_construction(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    let capacity = client.metrics().memory().configured_capacity_bytes();
    ensure!(
        capacity == AUTO_MEMORY_BYTES,
        "automatic memory capacity is {capacity} bytes, expected {AUTO_MEMORY_BYTES}"
    );
    shut_down(client, before, &store).await
}

/// A 64 KiB object uploads with one `PutObject` and downloads byte for byte.
async fn single_part_round_trip(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    let data = pattern(64 * KIB);
    upload(&client, "single", data.clone()).await?;
    ensure_same(&data, &download(&client, "single").await?)?;
    let requests = store.requests();
    ensure!(
        requests.put_object == 1 && requests.upload_part == 0 && requests.get_object == 1,
        "unexpected requests: {requests:?}"
    );
    ensure!(
        host::stats().spawned_tasks > before.host.spawned_tasks,
        "no worker ran on the client's runtime"
    );
    shut_down(client, before, &store).await
}

/// A 20 MiB object uploads as three 8 MiB-target parts and downloads as three
/// ranged `GetObject` requests, with requests in flight together.
async fn multipart_round_trip(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config.part_size(PartSize::Target(8 * MIB as u64)));
    let data = pattern(20 * MIB);
    upload(&client, "multipart", data.clone()).await?;
    ensure_same(&data, &download(&client, "multipart").await?)?;
    let requests = store.requests();
    ensure!(
        requests.upload_part == 3 && requests.multipart_control == 2,
        "unexpected upload requests: {requests:?}"
    );
    let mut ranges = store.get_ranges();
    ranges.sort_unstable();
    let expected = [0, 8 * MIB as u64, 16 * MIB as u64].map(Some);
    ensure!(ranges == expected, "unexpected download ranges: {ranges:?}");
    let concurrent = store.max_in_flight();
    ensure!(
        concurrent >= 2,
        "at most {concurrent} requests were in flight at once, expected 2"
    );
    let summary = shut_down(client, before, &store).await?;
    Ok(format!("up to {concurrent} requests in flight; {summary}"))
}

/// The server answers the first `PutObject` for `retried` with `503
/// SlowDown`. The error reaches the transfer manager, which waits through the
/// configured sleep implementation and re-sends the request.
async fn outer_retry(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    let data = pattern(64 * KIB);
    upload(&client, "retried", data.clone()).await?;
    let requests = store.requests();
    ensure!(
        requests.throttled == 1 && requests.put_object == 2,
        "expected one throttled and one accepted PutObject, which needs the server's \
         `--slow-down-once bucket/retried`: {requests:?}"
    );
    let sleeps = host::stats().fired_sleeps - before.host.fired_sleeps;
    ensure!(sleeps == 1, "expected one backoff sleep, observed {sleeps}");
    let stored = store.object("retried").await?;
    ensure_same(&data, stored.as_deref().unwrap_or_default())?;
    shut_down(client, before, &store).await
}

/// After warmup, a ranged `GetObject` that outlives its latency deadline is
/// dropped and re-sent once.
async fn hedge_deadline(endpoint: String) -> Result<String, String> {
    const PART: usize = 5 * MIB;
    const PARTS: usize = 15;
    let before = snapshot();
    let store = Store::new(&endpoint);
    let data = pattern(PARTS * PART);
    store.insert("hedged", data.clone()).await?;
    // Two workers keep requests in order, so the last range starts after the
    // ten successful requests that warm the deadline.
    let client = store.client(|config| {
        config
            .part_size(PartSize::Target(PART as u64))
            .concurrency(ConcurrencyMode::Explicit(2))
    });
    let held_start = ((PARTS - 1) * PART) as u64;
    store.hold(HoldRule {
        method: "GET",
        range_start: Some(held_start),
    });
    ensure_same(&data, &download(&client, "hedged").await?)?;
    ensure!(
        store.held() == 1 && store.held_dropped() == 1,
        "held {} requests and dropped {}, expected one of each",
        store.held(),
        store.held_dropped()
    );
    let sent = store
        .get_ranges()
        .iter()
        .filter(|start| **start == Some(held_start))
        .count();
    ensure!(
        sent == 2,
        "held range was sent {sent} times, expected once held and once re-sent"
    );
    shut_down(client, before, &store).await
}

/// Aborting an upload whose `PutObject` never completes drops the request and
/// stores nothing.
async fn cancellation(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    store.hold(HoldRule {
        method: "PUT",
        range_start: None,
    });
    let handle = client
        .upload()
        .bucket("bucket")
        .key("cancelled")
        .body(InputStream::from(pattern(64 * KIB)))
        .initiate()
        .map_err(|e| format!("initiate upload: {e}"))?;
    host::idle().await;
    ensure!(store.held() == 1, "the PutObject was not held");
    handle.abort().await.map_err(|e| format!("abort: {e}"))?;
    ensure!(
        store.held_dropped() == 1,
        "the held PutObject future was not dropped"
    );
    ensure!(
        store.object("cancelled").await?.is_none(),
        "aborted upload stored an object"
    );
    shut_down(client, before, &store).await
}

/// Aborting an upload while the host performs its `PutObject` cancels the
/// host's request.
///
/// The host answers only between calls into the module, so the abort, which
/// completes within one call, always precedes the response.
async fn host_cancellation(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    let handle = client
        .upload()
        .bucket("bucket")
        .key("cancelled-in-flight")
        .body(InputStream::from(pattern(64 * KIB)))
        .initiate()
        .map_err(|e| format!("initiate upload: {e}"))?;
    host::idle().await;
    let sent = host_http::stats().sent - before.http.sent;
    ensure!(
        sent == 1,
        "{sent} requests were sent, expected one PutObject"
    );
    handle.abort().await.map_err(|e| format!("abort: {e}"))?;
    let cancelled = host_http::stats().cancelled - before.http.cancelled;
    ensure!(
        cancelled == 1,
        "{cancelled} requests were cancelled, expected the PutObject"
    );
    shut_down(client, before, &store).await
}

/// Hundreds of concurrent uploads cross many 64-entry submission rounds. A
/// threadless engine traps if submission ever waits on its condition variable.
async fn high_fan_out(endpoint: String) -> Result<String, String> {
    const UPLOADS: usize = 300;
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config.concurrency(ConcurrencyMode::Explicit(128)));
    let mut handles = Vec::with_capacity(UPLOADS);
    for index in 0..UPLOADS {
        let handle = client
            .upload()
            .bucket("bucket")
            .key(format!("fan-out-{index}"))
            .body(InputStream::from(pattern(4 * KIB)))
            .initiate()
            .map_err(|e| format!("initiate upload {index}: {e}"))?;
        handles.push(handle);
    }
    for (index, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .await
            .map_err(|e| format!("upload {index}: {e}"))?;
    }
    let requests = store.requests();
    ensure!(
        requests.put_object == UPLOADS,
        "expected {UPLOADS} PutObject requests: {requests:?}"
    );
    let summary = shut_down(client, before, &store).await?;
    Ok(format!(
        "up to {} requests in flight; {summary}",
        store.max_in_flight()
    ))
}

/// File-backed inputs and directory uploads fail with errors rather than
/// panicking, and send no request.
async fn filesystem_unavailable(endpoint: String) -> Result<String, String> {
    let before = snapshot();
    let store = Store::new(&endpoint);
    let client = store.client(|config| config);
    ensure!(
        InputStream::from_path("/data/object").is_err(),
        "a file-backed input stream was created without a filesystem"
    );
    let uploaded = async {
        client
            .upload_objects()
            .bucket("bucket")
            .source("/data")
            .initiate()?
            .join()
            .await
    }
    .await;
    ensure!(
        uploaded.is_err(),
        "a directory upload succeeded without a filesystem"
    );
    ensure!(
        store.requests() == Default::default(),
        "requests were sent: {}",
        store.requests()
    );
    shut_down(client, before, &store).await
}

/// Constructs a client whose runtime mode is the default, which this target
/// does not support.
pub(crate) fn client_without_runtime() {
    let sdk = sdk_config(UNUSED_ENDPOINT, SharedHttpConnector::new(HostHttp), true);
    drop(Client::new(Config::builder().s3_config(&sdk).build()));
}

/// Constructs a client whose S3 configuration has no sleep implementation.
///
/// Timeouts, stalled-stream protection, and identity caching are disabled
/// because the SDK itself rejects a configuration that enables them, or
/// retries, without a sleep implementation.
pub(crate) fn client_without_sleep() {
    let sdk = sdk_config(UNUSED_ENDPOINT, SharedHttpConnector::new(HostHttp), false)
        .into_builder()
        .timeout_config(TimeoutConfig::disabled())
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .identity_cache(IdentityCache::no_cache())
        .build();
    let config = Config::builder()
        .s3_config(&sdk)
        .runtime_config(RuntimeConfig::wasm(HostRuntime))
        .build();
    drop(Client::new(config));
}
