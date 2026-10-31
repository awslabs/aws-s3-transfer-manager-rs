/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer scenarios run under the module's own host.
//!
//! Each scenario uses a fresh connector and client, then drops the client and
//! drives the executor until idle. Every worker the client spawned must have
//! exited by then.
//!
//! The host round trip instead sends its requests to the embedding host
//! through [`HostHttp`], and the host drives it one call at a time.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, IdentityCache, Region};
use aws_sdk_s3_transfer_manager::config::Builder as ConfigBuilder;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize};
use aws_sdk_s3_transfer_manager::wasm::{self, BoxFuture, RuntimeAlreadyInstalled, WasmRuntime};
use aws_sdk_s3_transfer_manager::{Client, Config};
use aws_smithy_runtime_api::client::http::{http_client_fn, SharedHttpConnector};
use aws_types::sdk_config::{
    RetryConfig, SharedCredentialsProvider, StalledStreamProtectionConfig, TimeoutConfig,
};
use aws_types::SdkConfig;
use bytes::Bytes;

use crate::host::{
    self, block_on, run_until_idle, Driver, LogicalRuntime, LogicalSleep, LogicalWallClock,
    Progress,
};
use crate::host_http::{self, HostHttp};
use crate::s3::{HoldRule, MemoryS3};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// The automatic memory limit on this target.
const AUTO_MEMORY_BYTES: u64 = 64 * MIB as u64;

/// Epoch flushes that advance a single-participant collector far enough to
/// destroy garbage deferred before the first flush.
const EPOCH_FLUSHES: usize = 4;

/// Returns `Err(message)` from the enclosing scenario when `condition` fails.
macro_rules! ensure {
    ($condition:expr, $($message:tt)+) => {
        if !$condition {
            return Err(format!($($message)+));
        }
    };
}

type Scenario = fn() -> Result<String, String>;

/// Scenarios in execution order. The first installs the runtime that the
/// others use.
const SCENARIOS: &[(&str, Scenario)] = &[
    ("installation", installation),
    ("client construction", client_construction),
    ("single-part round trip", single_part_round_trip),
    ("multipart round trip", multipart_round_trip),
    ("outer retry", outer_retry),
    ("hedge deadline", hedge_deadline),
    ("cancellation", cancellation),
    ("high fan-out", high_fan_out),
    ("filesystem unavailable", filesystem_unavailable),
];

/// Number of scenarios that [`run_all`] runs.
pub(crate) fn scenario_count() -> usize {
    SCENARIOS.len()
}

/// Runs every scenario in order. Returns how many passed, which is
/// [`scenario_count`], or the first failure.
///
/// Reports `PASS <scenario>: <summary>` for each scenario, where the summary
/// lists the requests the connector served and the workers that exited.
pub(crate) fn run_all() -> Result<usize, String> {
    let mut passed = 0;
    for (name, scenario) in SCENARIOS {
        let summary = scenario().map_err(|message| format!("FAIL {name}: {message}"))?;
        crate::report(&format!("PASS {name}: {summary}"));
        passed += 1;
    }
    Ok(passed)
}

/// Builds shared configuration for `connector` with every capability the
/// target requires, omitting the sleep implementation when `with_sleep` is
/// false.
fn sdk_config(connector: SharedHttpConnector, with_sleep: bool) -> SdkConfig {
    let builder = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "akid", "secret", None, None, "harness",
        )))
        .endpoint_url("http://s3.harness.invalid")
        .http_client(http_client_fn(move |_, _| connector.clone()))
        .time_source(LogicalWallClock)
        // The SDK makes one attempt, so every retry observed here is the
        // transfer manager's.
        .retry_config(RetryConfig::standard().with_max_attempts(1));
    let builder = if with_sleep {
        builder.sleep_impl(LogicalSleep)
    } else {
        builder
    };
    builder.build()
}

fn client(store: &MemoryS3, configure: impl FnOnce(ConfigBuilder) -> ConfigBuilder) -> Client {
    client_on(SharedHttpConnector::new(store.clone()), configure)
}

fn client_on(
    connector: SharedHttpConnector,
    configure: impl FnOnce(ConfigBuilder) -> ConfigBuilder,
) -> Client {
    let config = Config::builder().s3_config(&sdk_config(connector, true));
    Client::new(configure(config).build())
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

/// Drops `client`, drives the executor, and verifies that every worker the
/// scenario spawned has exited and that requests waited on host I/O. Returns a
/// summary of the requests `store` served and the workers that exited.
fn shut_down(client: Client, before: host::Stats, store: &MemoryS3) -> Result<String, String> {
    let workers = release(client, before)?;
    let after = host::stats();
    let requests = store.requests();
    let sent = requests.put_object
        + requests.upload_part
        + requests.multipart_control
        + requests.get_object
        + requests.throttled;
    ensure!(
        after.io_pending_polls - before.io_pending_polls >= sent,
        "{} requests completed but only {} I/O waits returned Pending",
        sent,
        after.io_pending_polls - before.io_pending_polls
    );
    Ok(format!(
        "{requests}; {workers} tasks exited after the client was dropped"
    ))
}

/// Drops `client`, drives the executor, and verifies that every worker spawned
/// since `before` has exited. Returns how many of those workers were live when
/// the client was dropped.
///
/// Workers exit when the client's shared state drops. The scheduler's
/// lock-free sets hand removed transfer descriptors, which reference that
/// state, to crossbeam-epoch, which destroys them only after the epoch
/// advances. Flushing the epoch here makes the drop deterministic; worker exit
/// itself still depends on the host waking each worker's queue notification.
fn release(client: Client, before: host::Stats) -> Result<usize, String> {
    let workers = host::stats().live_tasks.saturating_sub(before.live_tasks);
    drop(client);
    for _ in 0..EPOCH_FLUSHES {
        crossbeam_epoch::pin().flush();
    }
    run_until_idle()?;
    let after = host::stats();
    ensure!(
        after.live_tasks == before.live_tasks,
        "{} spawned futures remain live after the client was dropped",
        after.live_tasks - before.live_tasks
    );
    Ok(workers)
}

static REJECTED_RUNTIME_DROPPED: AtomicBool = AtomicBool::new(false);

/// A runtime that must never be used.
#[derive(Debug)]
struct RejectedRuntime;

impl WasmRuntime for RejectedRuntime {
    fn monotonic_now(&self) -> Duration {
        panic!("the rejected runtime's clock was read")
    }

    fn spawn(&self, _future: BoxFuture) {
        panic!("the rejected runtime spawned a future")
    }
}

impl Drop for RejectedRuntime {
    fn drop(&mut self) {
        REJECTED_RUNTIME_DROPPED.store(true, Ordering::SeqCst);
    }
}

/// The first installation succeeds. A second fails, drops its runtime, and
/// leaves the first active; later scenarios would trap if the rejected
/// runtime were used.
fn installation() -> Result<String, String> {
    ensure!(
        wasm::install_runtime(LogicalRuntime).is_ok(),
        "first installation failed"
    );
    ensure!(
        matches!(
            wasm::install_runtime(RejectedRuntime),
            Err(RuntimeAlreadyInstalled { .. })
        ),
        "second installation succeeded"
    );
    ensure!(
        REJECTED_RUNTIME_DROPPED.load(Ordering::SeqCst),
        "rejected runtime was retained"
    );
    Ok("a second runtime was rejected and dropped".to_owned())
}

/// A client builds from explicit capabilities and default transfer-manager
/// configuration, and its automatic memory limit is the target's fallback.
fn client_construction() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| config);
    let capacity = client.metrics().memory().configured_capacity_bytes();
    ensure!(
        capacity == AUTO_MEMORY_BYTES,
        "automatic memory capacity is {capacity} bytes, expected {AUTO_MEMORY_BYTES}"
    );
    shut_down(client, before, &store)
}

/// A 64 KiB object uploads with one `PutObject` and downloads byte for byte.
fn single_part_round_trip() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| config);
    let data = pattern(64 * KIB);
    let received = block_on(async {
        upload(&client, "single", data.clone()).await?;
        download(&client, "single").await
    })??;
    ensure_same(&data, &received)?;
    let requests = store.requests();
    ensure!(
        requests.put_object == 1 && requests.upload_part == 0,
        "unexpected upload requests: {requests:?}"
    );
    ensure!(
        host::stats().spawned_tasks > before.spawned_tasks,
        "no worker ran on the installed runtime"
    );
    shut_down(client, before, &store)
}

/// A 20 MiB object uploads as three 8 MiB-target parts and downloads as three
/// ranged `GetObject` requests.
fn multipart_round_trip() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| {
        config.part_size(PartSize::Target(8 * MIB as u64))
    });
    let data = pattern(20 * MIB);
    let received = block_on(async {
        upload(&client, "multipart", data.clone()).await?;
        download(&client, "multipart").await
    })??;
    ensure_same(&data, &received)?;
    let requests = store.requests();
    ensure!(
        requests.upload_part == 3 && requests.multipart_control == 2,
        "unexpected upload requests: {requests:?}"
    );
    let mut ranges = store.get_ranges();
    ranges.sort_unstable();
    let expected = [0, 8 * MIB as u64, 16 * MIB as u64].map(Some);
    ensure!(ranges == expected, "unexpected download ranges: {ranges:?}");
    shut_down(client, before, &store)
}

/// A throttled `PutObject` reaches the transfer manager, which waits through
/// the configured sleep implementation and re-sends it.
fn outer_retry() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| config);
    let data = pattern(64 * KIB);
    store.throttle_next(1);
    block_on(upload(&client, "retried", data.clone()))??;
    let requests = store.requests();
    ensure!(
        requests.throttled == 1 && requests.put_object == 1,
        "expected one throttled and one accepted PutObject: {requests:?}"
    );
    let sleeps = host::stats().fired_sleeps - before.fired_sleeps;
    ensure!(sleeps == 1, "expected one backoff sleep, observed {sleeps}");
    ensure!(
        store.object("/retried").as_deref() == Some(&data[..]),
        "stored object differs from the upload"
    );
    shut_down(client, before, &store)
}

/// After warmup, a ranged `GetObject` that outlives its latency deadline is
/// dropped and re-sent once.
fn hedge_deadline() -> Result<String, String> {
    const PART: usize = 5 * MIB;
    const PARTS: usize = 15;
    let before = host::stats();
    let store = MemoryS3::default();
    let data = pattern(PARTS * PART);
    store.insert("/hedged", data.clone());
    // Two workers keep requests in order, so the last range starts after the
    // ten successful requests that warm the deadline.
    let client = client(&store, |config| {
        config
            .part_size(PartSize::Target(PART as u64))
            .concurrency(ConcurrencyMode::Explicit(2))
    });
    let held_start = ((PARTS - 1) * PART) as u64;
    store.hold(HoldRule {
        method: "GET",
        range_start: Some(held_start),
    });
    let received = block_on(download(&client, "hedged"))??;
    ensure_same(&data, &received)?;
    ensure!(
        store.held_dropped() == 1,
        "held request was not dropped by its deadline"
    );
    let reissued = store
        .get_ranges()
        .iter()
        .filter(|start| **start == Some(held_start))
        .count();
    ensure!(
        reissued == 1,
        "held range was re-sent {reissued} times, expected once"
    );
    shut_down(client, before, &store)
}

/// Aborting an upload whose `PutObject` never completes drops the request and
/// returns.
fn cancellation() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| config);
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
    block_on(async {
        store.wait_held().await;
        handle.abort().await
    })?
    .map_err(|e| format!("abort: {e}"))?;
    ensure!(
        store.held_dropped() == 1,
        "the held PutObject future was not dropped"
    );
    ensure!(
        store.object("/cancelled").is_none(),
        "aborted upload stored an object"
    );
    shut_down(client, before, &store)
}

/// Hundreds of concurrent uploads cross many 64-entry submission rounds. A
/// threadless engine traps if submission ever waits on its condition variable.
fn high_fan_out() -> Result<String, String> {
    const UPLOADS: usize = 300;
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| {
        config.concurrency(ConcurrencyMode::Explicit(128))
    });
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
    block_on(async {
        for (index, handle) in handles.into_iter().enumerate() {
            handle
                .join()
                .await
                .map_err(|e| format!("upload {index}: {e}"))?;
        }
        Ok::<_, String>(())
    })??;
    let requests = store.requests();
    ensure!(
        requests.put_object == UPLOADS,
        "expected {UPLOADS} PutObject requests: {requests:?}"
    );
    shut_down(client, before, &store)
}

/// File-backed inputs and directory uploads fail with errors rather than
/// panicking, and send no request.
fn filesystem_unavailable() -> Result<String, String> {
    let before = host::stats();
    let store = MemoryS3::default();
    let client = client(&store, |config| config);
    ensure!(
        InputStream::from_path("/data/object").is_err(),
        "a file-backed input stream was created without a filesystem"
    );
    let uploaded = block_on(async {
        client
            .upload_objects()
            .bucket("bucket")
            .source("/data")
            .initiate()?
            .join()
            .await
    })?;
    ensure!(
        uploaded.is_err(),
        "a directory upload succeeded without a filesystem"
    );
    ensure!(
        store.requests() == Default::default(),
        "requests were sent: {:?}",
        store.requests()
    );
    shut_down(client, before, &store)
}

/// A round trip whose requests the embedding host answers.
struct HostRoundTrip {
    driver: Driver<Result<Vec<u8>, String>>,
    client: Client,
    object: Bytes,
    before: host::Stats,
}

static HOST_ROUND_TRIP: Mutex<Option<HostRoundTrip>> = Mutex::new(None);

/// Installs the runtime and starts uploading `object` to `bucket/object` with
/// 8 MiB-target parts, then downloading it, over [`HostHttp`].
pub(crate) fn host_round_trip_start(object: Bytes) -> Result<(), String> {
    ensure!(
        wasm::install_runtime(LogicalRuntime).is_ok(),
        "a runtime was already installed"
    );
    let before = host::stats();
    let client = client_on(SharedHttpConnector::new(HostHttp), |config| {
        config.part_size(PartSize::Target(8 * MIB as u64))
    });
    let transfers = client.clone();
    let upload_data = object.clone();
    let driver = Driver::new(async move {
        upload(&transfers, "object", upload_data).await?;
        download(&transfers, "object").await
    });
    *HOST_ROUND_TRIP.lock().unwrap() = Some(HostRoundTrip {
        driver,
        client,
        object,
        before,
    });
    Ok(())
}

/// Drives the round trip. Returns `Ok(false)` while requests await host
/// responses, and `Ok(true)` once the downloaded bytes match the upload and
/// every worker has exited.
pub(crate) fn host_round_trip_step() -> Result<bool, String> {
    let mut slot = HOST_ROUND_TRIP.lock().unwrap();
    let run = slot.as_mut().ok_or("no round trip was started")?;
    let received = match run.driver.drive(&host_http::awaiting_host)? {
        Progress::AwaitingHost => return Ok(false),
        Progress::Complete(received) => received?,
    };
    let HostRoundTrip {
        driver,
        client,
        object,
        before,
    } = slot.take().expect("checked above");
    drop(slot);
    drop(driver);
    ensure_same(&object, &received)?;
    ensure!(
        !host_http::awaiting_host(),
        "requests remain outstanding after the round trip"
    );
    release(client, before)?;
    Ok(true)
}

/// Constructs a client before installing a runtime.
pub(crate) fn client_without_runtime() {
    let store = MemoryS3::default();
    drop(client(&store, |config| config));
}

/// Installs the runtime, then constructs a client whose S3 configuration has
/// no sleep implementation.
///
/// Timeouts, stalled-stream protection, and identity caching are disabled
/// because the SDK itself rejects a configuration that enables them, or
/// retries, without a sleep implementation.
pub(crate) fn client_without_sleep() {
    let _ = wasm::install_runtime(LogicalRuntime);
    let store = MemoryS3::default();
    let sdk = sdk_config(SharedHttpConnector::new(store), false)
        .into_builder()
        .timeout_config(TimeoutConfig::disabled())
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .identity_cache(IdentityCache::no_cache())
        .build();
    let config = Config::builder().s3_config(&sdk).build();
    drop(Client::new(config));
}
