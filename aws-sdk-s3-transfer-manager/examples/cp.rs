/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_config::BehaviorVersion;
use aws_sdk_s3_transfer_manager::config::{ConcurrencyMode, PartSize, TargetThroughput};
use aws_sdk_s3_transfer_manager::config::{MemoryBudgetConfig, MemoryConfig};
use aws_sdk_s3_transfer_manager::config::{RuntimeConfig, S3ClientConfig};
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::memory::BufferPool;
use aws_sdk_s3_transfer_manager::operation::download::Body;
use aws_sdk_s3_transfer_manager::types::{ByteSize, Throughput};
use aws_sdk_s3_transfer_manager::Config;
use aws_smithy_execution::{ManagedRuntime, ManagedRuntimeBuilder, ManagedRuntimeHandle};
use aws_smithy_http_client::pool::{ConnectionPool, DriverSpawner, Partition, PartitionId};
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::{rustls_provider::CryptoMode, Provider};
use aws_smithy_managed_http::client::ManagedHttpPool;
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns};
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use aws_smithy_runtime_api::shared::IntoShared;
use aws_smithy_types::date_time::{DateTime, Format};
use clap::{CommandFactory, Parser};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{self, SystemTime};
use tokio::fs;

type BoxError = Box<dyn Error + Send + Sync>;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// Compiled-in jemalloc runtime config. A saturated download frees and reallocates
// part-sized (8 MiB) receive buffers at line rate; with the default config the
// allocator returns those pages to the OS and refaults them on the next read, so
// page-fault and purge cost dominates the receive path and resident size runs
// several times the live working set. These options keep freed buffers mapped for
// reuse, bounding both costs. Overridden by the _RJEM_MALLOC_CONF environment
// variable (jemalloc config precedence).
//
//   background_thread:true  Run page purging on a dedicated thread instead of
//                           inline on the freeing thread, where the madvise call
//                           would block the receive path. pthread platforms only
//                           (Linux); ignored with a startup warning elsewhere.
//   narenas:32              Cap allocation arenas well below the default (~4x
//                           CPUs). Fewer arenas concentrate freed buffers into a
//                           shared dirty pool, so a free on one thread can satisfy
//                           an allocation on another; the default scatters them
//                           per-arena and forces a fresh mmap. A fixed count rather
//                           than machine-scaled or percpu_arena.
//   dirty_decay_ms:1000     Hold a freed page mapped and reusable for 1s before
//                           purging. This retention is the reuse window: a buffer
//                           freed at delivery serves the next read without a fault.
//                           0 disables reuse; larger values raise resident size.
//   muzzy_decay_ms:0        Purge muzzy (already madvise(FREE)'d) pages immediately
//                           rather than holding a second retention tier; the dirty
//                           pool above provides the reuse.
#[cfg(not(target_env = "msvc"))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] =
    b"background_thread:true,narenas:32,dirty_decay_ms:1000,muzzy_decay_ms:0\0";

#[derive(Debug, Clone, Default, clap::ValueEnum)]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// Execution runtime selection, mapped to `RuntimeConfig`.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
enum RuntimeArg {
    /// Managed OS threads owned by the transfer manager (default).
    #[default]
    Managed,
    /// Run on the ambient tokio multi-threaded runtime.
    MultiThreadTokio,
}

impl RuntimeArg {
    fn mode(&self) -> RuntimeConfig {
        match self {
            RuntimeArg::Managed => RuntimeConfig::default(),
            RuntimeArg::MultiThreadTokio => RuntimeConfig::multi_thread_tokio(),
        }
    }
}

#[derive(Debug, Clone, clap::Parser)]
#[command(name = "cp")]
#[command(about = "Copies a local file or S3 object to another location locally or in S3.")]
pub struct Args {
    /// Source to copy from <S3Uri | Local>
    #[arg(required = true)]
    source: TransferUri,

    /// Destination to copy to <S3Uri | Local>
    #[arg(required = true)]
    dest: TransferUri,

    #[command(flatten)]
    concurrency: ConcurrencyModeArg,

    /// Execution runtime: managed threads (default) or the ambient tokio runtime
    #[arg(long, value_enum, default_value_t = RuntimeArg::Managed)]
    runtime: RuntimeArg,

    /// Part size to use
    #[arg(long, default_value_t = 8388608)]
    part_size: u64,

    /// Enable tokio console (requires RUSTFLAGS="--cfg tokio_unstable")
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    tokio_console: bool,

    /// Command is performed on all files or objects under the specified directory or prefix
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    recursive: bool,

    /// Number of iterations to run (reuses TM client across iterations)
    #[arg(long, default_value_t = 1)]
    iterations: u32,

    /// Output format
    #[arg(long, default_value = "text")]
    output: OutputFormat,

    /// Directory to write output files (e.g. iterations.json)
    #[arg(long)]
    output_dir: Option<String>,

    /// Path to manifest JSON for concurrent transfers
    #[arg(long)]
    manifest: Option<String>,

    /// Directory to write dial9 runtime traces (enables tracing when set)
    #[arg(long)]
    trace_dir: Option<String>,

    /// Enable CPU profiling in dial9 traces (Linux only, requires --trace-dir)
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue, requires = "trace_dir")]
    cpu_profiling: bool,

    /// Network interfaces to bind S3 connections to, comma-separated (e.g. `ens5,ens6`).
    ///
    /// Managed worker threads are assigned interfaces round-robin. Requires the
    /// managed runtime; supported on Linux, Android, Apple platforms, illumos,
    /// Solaris, and Fuchsia.
    #[arg(long, value_delimiter = ',')]
    network_interfaces: Vec<String>,

    /// Maximum connections to one host across all worker threads.
    ///
    /// This option or `--network-interfaces` replaces the transfer manager's HTTP client with
    /// one cp builds, which requires the managed runtime. When unset, that client's cap is 4096,
    /// the transfer manager's ceiling (`MAX_CONNECTIONS_PER_HOST` in `src/runtime/platform.rs`);
    /// the transfer manager's own client derives its cap from the process descriptor limit.
    #[arg(long)]
    max_connections_per_host: Option<NonZeroUsize>,
}

/// Per-host connection cap of the HTTP client cp builds when `--max-connections-per-host` is
/// unset: the transfer manager's `MAX_CONNECTIONS_PER_HOST` in `src/runtime/platform.rs`.
const DEFAULT_MAX_CONNECTIONS_PER_HOST: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

#[derive(Debug, Clone, clap::Args)]
#[group(multiple = false)]
struct ConcurrencyModeArg {
    /// ConcurrencyMode::Auto
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    concurrency_auto: bool,

    /// ConcurrencyMode::TargetThroughput(Gbps)
    #[arg(long, value_parser = parse_target_throughput)]
    target_throughput_gbps: Option<TargetThroughput>,

    /// ConcurrencyMode::Explicit(n)
    #[arg(long)]
    concurrency: Option<usize>,
}

impl ConcurrencyModeArg {
    fn mode(&self) -> ConcurrencyMode {
        if self.concurrency_auto {
            return ConcurrencyMode::Auto;
        }

        match (self.target_throughput_gbps, self.concurrency) {
            (Some(target), _) => ConcurrencyMode::TargetThroughput(target),
            (None, Some(concurrency)) => ConcurrencyMode::Explicit(concurrency),
            (None, None) => ConcurrencyMode::Auto,
        }
    }
}

/// Parses `--target-throughput-gbps`, rejecting values that are not finite and positive.
fn parse_target_throughput(s: &str) -> Result<TargetThroughput, String> {
    let gbps: f64 = s.parse().map_err(|e| format!("{e}"))?;
    TargetThroughput::gbps(gbps).map_err(|e| e.to_string())
}

#[derive(Clone, Debug)]
enum TransferUri {
    /// Local filesystem source/destination
    Local(PathBuf),

    /// S3 source/destination
    S3(S3Uri),
}

impl TransferUri {
    fn expect_s3(&self) -> &S3Uri {
        match self {
            TransferUri::S3(s3_uri) => s3_uri,
            _ => panic!("expected S3Uri"),
        }
    }

    fn expect_local(&self) -> &PathBuf {
        match self {
            TransferUri::Local(path) => path,
            _ => panic!("expected Local"),
        }
    }
}

impl FromStr for TransferUri {
    type Err = BoxError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let uri = if s.starts_with("s3://") {
            TransferUri::S3(S3Uri(s.to_owned()))
        } else {
            let path = PathBuf::from_str(s).unwrap();
            TransferUri::Local(path)
        };
        Ok(uri)
    }
}

#[derive(Clone, Debug)]
struct S3Uri(String);

impl S3Uri {
    /// Split the URI into it's component parts '(bucket, key)'
    fn parts(&self) -> (&str, &str) {
        let bucket = self.0.strip_prefix("s3://").expect("valid s3 uri prefix");
        bucket.split_once('/').unwrap_or((bucket, ""))
    }
}

#[derive(serde::Serialize)]
struct IterationResult {
    iteration: u32,
    start: String,
    end: String,
    duration_secs: f64,
    bytes_transferred: u64,
    throughput_gbps: f64,
}

#[derive(serde::Deserialize)]
struct ManifestEntry {
    key: String,
    local: String,
    #[allow(dead_code)]
    size: u64,
}

/// An invalid argument or argument combination, reported as a usage error.
#[derive(Debug)]
struct ArgError(&'static str);

impl fmt::Display for ArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for ArgError {}

/// Direction of a transfer between the local filesystem and S3.
#[derive(Clone, Copy, Debug)]
enum Direction {
    Upload,
    Download,
}

impl Direction {
    /// Returns the direction of a transfer from `source` to `dest`.
    ///
    /// Errors unless exactly one side is an S3 URI.
    fn new(source: &TransferUri, dest: &TransferUri) -> Result<Self, ArgError> {
        use TransferUri::*;
        match (source, dest) {
            (Local(_), S3(_)) => Ok(Direction::Upload),
            (S3(_), Local(_)) => Ok(Direction::Download),
            (Local(_), Local(_)) => Err(ArgError("local to local transfer not supported")),
            (S3(_), S3(_)) => Err(ArgError("s3 to s3 transfer not supported")),
        }
    }
}

async fn do_single_download(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key: &str,
    dest: &Path,
) -> Result<u64, BoxError> {
    let is_dev_null = dest == Path::new("/dev/null");

    if is_dev_null {
        let mut handle = tm.download().bucket(bucket).key(key).initiate()?;
        drain_body(handle.body_mut()).await?;
        let obj_size_bytes = handle.object_meta().await?.total_object_size();
        handle.join().await?;
        Ok(obj_size_bytes)
    } else {
        let handle = tm
            .download()
            .bucket(bucket)
            .key(key)
            .write_to_path(dest)
            .await?;
        let obj_size_bytes = handle.object_meta().await?.total_object_size();
        handle.join().await?;
        Ok(obj_size_bytes)
    }
}

async fn do_recursive_download(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key_prefix: &str,
    dest: &Path,
) -> Result<u64, BoxError> {
    fs::create_dir_all(dest).await?;

    let handle = tm
        .download_objects()
        .bucket(bucket)
        .walker(
            aws_sdk_s3_transfer_manager::io::walk::S3Walker::builder()
                .prefix(key_prefix)
                .build(),
        )
        .destination(dest)
        .initiate()?;

    let output = handle.join().await?;
    tracing::info!("download output: {output:?}");

    let transfer_size_bytes = output.metrics.network_rx;
    println!(
        "downloaded {} objects totalling {transfer_size_bytes} bytes ({})",
        output.objects_downloaded(),
        ByteSize::new(transfer_size_bytes)
    );
    Ok(transfer_size_bytes)
}

async fn do_single_upload(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key: &str,
    source: &Path,
) -> Result<u64, BoxError> {
    let file_meta = fs::metadata(source).await.expect("file metadata");
    let stream = InputStream::from_path(source)?;

    let handle = tm
        .upload()
        .bucket(bucket)
        .key(key)
        .body(stream)
        .initiate()?;

    let _resp = handle.join().await?;
    Ok(file_meta.len())
}

async fn do_recursive_upload(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key_prefix: &str,
    source: &Path,
) -> Result<u64, BoxError> {
    let handle = tm
        .upload_objects()
        .source(source)
        .bucket(bucket)
        .key_prefix(key_prefix)
        .walker(
            aws_sdk_s3_transfer_manager::io::walk::FsWalker::builder()
                .recursive(true)
                .build(),
        )
        .initiate()?;

    let output = handle.join().await?;
    tracing::info!("recursive upload output: {output:?}");

    let transfer_size_bytes = output.metrics.network_tx;
    println!(
        "uploaded {} objects totalling {transfer_size_bytes} bytes ({})",
        output.objects_uploaded(),
        ByteSize::new(transfer_size_bytes)
    );
    Ok(transfer_size_bytes)
}

async fn do_manifest_download(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    entries: &[ManifestEntry],
    dest_dir: &Path,
) -> Result<u64, BoxError> {
    use tokio::task::JoinSet;
    let mut set = JoinSet::new();
    for entry in entries {
        let tm = tm.clone();
        let bucket = bucket.to_string();
        let key = entry.key.clone();
        let dest = if dest_dir == Path::new("/dev/null") {
            PathBuf::from("/dev/null")
        } else {
            let p = dest_dir.join(&entry.local);
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).await?;
            }
            p
        };
        set.spawn(async move { do_single_download(&tm, &bucket, &key, &dest).await });
    }
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        total += result??;
    }
    Ok(total)
}

async fn do_manifest_upload(
    tm: &aws_sdk_s3_transfer_manager::Client,
    bucket: &str,
    key_prefix: &str,
    entries: &[ManifestEntry],
    source_dir: &Path,
) -> Result<u64, BoxError> {
    use tokio::task::JoinSet;
    let mut set = JoinSet::new();
    for entry in entries {
        let tm = tm.clone();
        let bucket = bucket.to_string();
        // Match `upload_objects`: no separator after an empty prefix or one that
        // already ends with `/`.
        let upload_key = if key_prefix.is_empty() || key_prefix.ends_with('/') {
            format!("{key_prefix}{}", entry.local)
        } else {
            format!("{key_prefix}/{}", entry.local)
        };
        let source = source_dir.join(&entry.local);
        set.spawn(async move { do_single_upload(&tm, &bucket, &upload_key, &source).await });
    }
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        total += result??;
    }
    Ok(total)
}

fn main() {
    let args = Args::parse();
    // Runtime flavor must match the selected execution runtime. `Managed` spawns
    // and owns its own worker threads, so main only drives orchestration — a
    // current-thread runtime is correct and matches prior behavior.
    // `MultiThreadTokio` runs the transfer workers on THIS runtime via
    // `tokio::spawn`, so it needs a multi-threaded runtime or all workers
    // serialize onto one thread.
    let rt = match args.runtime {
        RuntimeArg::Managed => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime"),
        RuntimeArg::MultiThreadTokio => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build multi-thread runtime"),
    };
    rt.block_on(async_main(args));
}

/// Runs the command and exits the process on error: with a usage error for an [`ArgError`],
/// otherwise with status 1 after printing the error and its source chain.
async fn async_main(args: Args) {
    if let Err(e) = run(args).await {
        if let Some(e) = e.downcast_ref::<ArgError>() {
            Args::command()
                .error(clap::error::ErrorKind::InvalidValue, e)
                .exit();
        }
        // The top-level Display already carries a TM error's operation, code, and
        // request ids; the source chain carries the underlying detail (SdkError
        // message, checksum values).
        eprintln!("Error: {e}");
        let mut source = e.source();
        while let Some(s) = source {
            eprintln!("  caused by: {s}");
            source = s.source();
        }
        std::process::exit(1);
    }
}

/// Builds the client `args` configure and runs the transfer.
async fn run(args: Args) -> Result<(), BoxError> {
    init_tracing(args.tokio_console);
    tracing::debug!("using concurrency mode: {:?}", args.concurrency.mode());
    let direction = Direction::new(&args.source, &args.dest)?;
    let runtime_tracing = RuntimeTracing::from_args(&args);
    // The recorder's `Drop` stops its pipeline without draining queued events, so every
    // fallible step runs inside `result`, ahead of the graceful shutdown.
    let result = async {
        let config = client_config(&args, &runtime_tracing).await?;
        transfer(
            aws_sdk_s3_transfer_manager::Client::new(config),
            &args,
            direction,
        )
        .await
    }
    .await;
    runtime_tracing.shutdown();
    result
}

fn init_tracing(tokio_console: bool) {
    if tokio_console {
        console_subscriber::init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_thread_ids(true)
            .init();
    }
}

/// Returns the transfer manager configuration `args` select.
///
/// Each component is built from `args` when an option needs it; otherwise the transfer
/// manager's default for that component applies.
async fn client_config(args: &Args, runtime_tracing: &RuntimeTracing) -> Result<Config, BoxError> {
    let mut loader = aws_sdk_s3_transfer_manager::from_env()
        .concurrency(args.concurrency.mode())
        .part_size(PartSize::Target(ByteSize::new(args.part_size)))
        .runtime_config(args.runtime.mode());
    if let Some(pool) = memory_pool()? {
        loader = loader.memory(MemoryConfig::Explicit(pool));
    }
    let http_options = http_options(args)?;
    if let Some(runtime) = managed_runtime(args, runtime_tracing, http_options.is_some())? {
        if let Some(options) = &http_options {
            let http_client = http_client(runtime.handle(), options)?;
            // The same shared AWS configuration sources `ConfigLoader::load` reads when no S3
            // configuration is set.
            let sdk_config = aws_config::load_defaults(BehaviorVersion::latest())
                .await
                .into_builder()
                .http_client(http_client)
                .build();
            loader = loader.s3_config(S3ClientConfig::new(&sdk_config));
        }
        loader = loader.runtime_config(RuntimeConfig::managed(
            aws_sdk_s3_transfer_manager_ext::managed(runtime),
        ));
    }
    Ok(loader.load().await)
}

/// Returns a memory pool capped at `S3FIO_MEMORY_LIMIT` GiB, or `None` when that variable is
/// unset or not an integer.
///
/// The cap bounds in-flight plus buffered transfer data; transfers backpressure at the cap.
fn memory_pool() -> Result<Option<BufferPool>, BoxError> {
    let Some(gib) = std::env::var("S3FIO_MEMORY_LIMIT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    else {
        return Ok(None);
    };
    tracing::info!(gib, "memory budget limit (S3FIO_MEMORY_LIMIT, GiB)");
    let pool = BufferPool::builder()
        .memory_budget(MemoryBudgetConfig::Limit(ByteSize::gib(gib)))
        .build()?;
    Ok(Some(pool))
}

/// Returns a managed runtime for the transfer manager to adopt, or `None` when no option needs
/// one: runtime tracing with `--runtime managed`, or a replacement HTTP client (`http_client`).
///
/// The runtime has the thread naming and placement the transfer manager uses for the runtime
/// it builds itself. Errors when the runtime cannot be built.
fn managed_runtime(
    args: &Args,
    runtime_tracing: &RuntimeTracing,
    http_client: bool,
) -> Result<Option<ManagedRuntime>, BoxError> {
    let traced = runtime_tracing.is_enabled() && matches!(args.runtime, RuntimeArg::Managed);
    if !traced && !http_client {
        return Ok(None);
    }
    let builder = ManagedRuntime::builder()
        .thread_name_prefix("s3-tm")
        .pin_threads(false);
    Ok(Some(runtime_tracing.instrument(builder).build()?))
}

/// Options for an HTTP client that replaces the transfer manager's.
#[derive(Debug)]
struct HttpOptions {
    /// Interfaces assigned to managed runtime threads round-robin; empty leaves connections
    /// unbound.
    interfaces: Vec<String>,
    max_connections_per_host: NonZeroUsize,
}

/// Returns the HTTP client options from `args`, or `None` when neither `--network-interfaces`
/// (after trimming names and dropping empty ones) nor `--max-connections-per-host` is set.
///
/// Errors when an option is set with `--runtime multi-thread-tokio`, where the S3 client keeps
/// the SDK's HTTP client, or when interfaces are named on a platform that cannot bind a
/// connection to one.
fn http_options(args: &Args) -> Result<Option<HttpOptions>, ArgError> {
    let interfaces: Vec<String> = args
        .network_interfaces
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .collect();
    if interfaces.is_empty() && args.max_connections_per_host.is_none() {
        return Ok(None);
    }
    if matches!(args.runtime, RuntimeArg::MultiThreadTokio) {
        return Err(ArgError(
            "--network-interfaces and --max-connections-per-host require --runtime managed",
        ));
    }
    if !interfaces.is_empty() && !INTERFACE_BINDING_SUPPORTED {
        return Err(ArgError(
            "--network-interfaces is not supported on this platform",
        ));
    }
    Ok(Some(HttpOptions {
        interfaces,
        max_connections_per_host: args
            .max_connections_per_host
            .unwrap_or(DEFAULT_MAX_CONNECTIONS_PER_HOST),
    }))
}

/// Returns an HTTP client over `runtime` configured by `options`.
///
/// The client is configured as the transfer manager's own managed transport: one connection
/// pool with a partition per `runtime` thread, with partition `i` bound to
/// `interfaces[i % interfaces.len()]` when interfaces are named. Pool partition indices match
/// execution partition indices, and each partition spawns its connection tasks on its thread,
/// so a connection's I/O stays on the thread that opened it.
///
/// Errors when the pool or client cannot be built.
fn http_client(
    runtime: ManagedRuntimeHandle,
    options: &HttpOptions,
) -> Result<SharedHttpClient, BoxError> {
    tracing::info!(?options, "building the S3 HTTP client");

    #[cfg(target_os = "android")]
    let dns_resolver = ShufflingDnsResolver::new(
        // Hickory reads /etc/resolv.conf on Unix. Android routes libc
        // resolution through netd and does not provide that file.
        aws_smithy_runtime::client::dns::TokioDnsResolver::new(),
    );
    #[cfg(not(target_os = "android"))]
    let dns_resolver = ShufflingDnsResolver::new(aws_smithy_dns::HickoryDnsResolver::default());

    let partitions = runtime.partitions().iter().map(|target| {
        let index = target.id().index();
        let target = target.clone();
        let spawner = DriverSpawner::from_fn(move |task| {
            drop(target.spawn(task));
        });
        let partition = Partition::new(PartitionId::from_index(index), spawner);
        match options.interfaces.len() {
            0 => partition,
            n => bind_interface(partition, &options.interfaces[index % n]),
        }
    });
    let pool = ConnectionPool::builder()
        .dns_resolver(dns_resolver)
        // S3 closes idle connections server-side. Retiring them first avoids reusing a
        // connection the server is closing, which fails before the request is accepted and
        // costs a retry on a fresh connection.
        .idle_timeout(time::Duration::from_secs(15))
        .max_connections_per_host(options.max_connections_per_host.get())
        .proxy_config(ProxyConfig::from_env())
        .partitions(partitions)
        .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
        .build_https()?;
    Ok(ManagedHttpPool::from_pool(runtime, pool)?.into_shared())
}

/// Whether this platform can bind a connection to a network interface.
const INTERFACE_BINDING_SUPPORTED: bool = cfg!(any(
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
));

/// Binds `partition`'s connections to `interface`.
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
fn bind_interface(partition: Partition, interface: &str) -> Partition {
    partition.interface(interface)
}

/// Interface binding is unavailable on this platform, where [`http_options`] rejects
/// interface names: `partition` is returned unbound.
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
fn bind_interface(partition: Partition, _interface: &str) -> Partition {
    partition
}

/// Optional dial9 runtime tracing of the managed runtime's threads.
///
/// Enabled by `--trace-dir` or `S3FIO_TRACE_DIR` in builds with `cfg(s3_tm_dial9)`; in other
/// builds it is never enabled and every operation is a no-op.
struct RuntimeTracing {
    #[cfg(s3_tm_dial9)]
    recorder: Option<dial9::Recorder>,
}

#[cfg(s3_tm_dial9)]
impl RuntimeTracing {
    /// Returns tracing to a recorder writing to `--trace-dir` or `S3FIO_TRACE_DIR`, disabled
    /// when neither is set. Panics if the trace writer cannot be created.
    fn from_args(args: &Args) -> Self {
        use dial9::DiskBuffer;
        let Some(trace_dir) = args
            .trace_dir
            .clone()
            .or_else(|| std::env::var("S3FIO_TRACE_DIR").ok())
        else {
            return Self { recorder: None };
        };
        let _ = std::fs::create_dir_all(&trace_dir);
        let writer = DiskBuffer::builder()
            .base_path(&trace_dir)
            .max_file_size(64 * 1024 * 1024)
            .max_total_size(512 * 1024 * 1024)
            .build()
            .expect("failed to create trace writer");
        let mut builder = dial9::recorder(writer);
        if args.cpu_profiling {
            use dial9::RecorderPerfExt;
            builder = builder.with_cpu_profiling(dial9::cpu::CpuProfilingConfig::default());
        }
        Self {
            recorder: Some(builder.build()),
        }
    }

    /// Whether a recorder is present.
    fn is_enabled(&self) -> bool {
        self.recorder.is_some()
    }

    /// Returns `builder` with the recorder attached, so the runtime it builds is traced.
    fn instrument(&self, builder: ManagedRuntimeBuilder) -> ManagedRuntimeBuilder {
        match &self.recorder {
            Some(recorder) => builder.dial9_handle(recorder.handle().clone()),
            None => builder,
        }
    }

    /// Drains queued events and stops the recorder, waiting up to 10 seconds.
    ///
    /// Events a traced runtime has not flushed are lost, so every traced runtime must be dropped
    /// first.
    fn shutdown(self) {
        if let Some(recorder) = self.recorder {
            recorder.graceful_shutdown(time::Duration::from_secs(10));
        }
    }
}

#[cfg(not(s3_tm_dial9))]
impl RuntimeTracing {
    /// Returns disabled tracing.
    fn from_args(_args: &Args) -> Self {
        Self {}
    }

    /// Returns `false`.
    fn is_enabled(&self) -> bool {
        false
    }

    /// Returns `builder` unchanged.
    fn instrument(&self, builder: ManagedRuntimeBuilder) -> ManagedRuntimeBuilder {
        builder
    }

    /// Does nothing.
    fn shutdown(self) {}
}

// Copy of the transfer manager's private `ShufflingDnsResolver` in `src/runtime/managed.rs`.
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

/// Runs the configured iterations and writes their results.
///
/// Takes the client by value so it is dropped, and its worker runtimes flush their trace
/// events, before the caller shuts runtime tracing down.
async fn transfer(
    tm: aws_sdk_s3_transfer_manager::Client,
    args: &Args,
    direction: Direction,
) -> Result<(), BoxError> {
    let json_output = matches!(args.output, OutputFormat::Json);

    let mut iteration_results = Vec::new();
    for i in 1..=args.iterations {
        let start = SystemTime::now();
        let wall_start = time::Instant::now();

        let manifest: Option<Vec<ManifestEntry>> = match &args.manifest {
            Some(path) => Some(serde_json::from_str(&fs::read_to_string(path).await?)?),
            None => None,
        };
        let bytes = match direction {
            Direction::Download => {
                let (bucket, key) = args.source.expect_s3().parts();
                let dest = args.dest.expect_local();
                match (&manifest, args.recursive) {
                    (Some(entries), _) => do_manifest_download(&tm, bucket, entries, dest).await?,
                    (None, true) => do_recursive_download(&tm, bucket, key, dest).await?,
                    (None, false) => do_single_download(&tm, bucket, key, dest).await?,
                }
            }
            Direction::Upload => {
                let (bucket, key) = args.dest.expect_s3().parts();
                let source = args.source.expect_local();
                match (&manifest, args.recursive) {
                    (Some(entries), _) => {
                        do_manifest_upload(&tm, bucket, key, entries, source).await?
                    }
                    (None, true) => do_recursive_upload(&tm, bucket, key, source).await?,
                    (None, false) => do_single_upload(&tm, bucket, key, source).await?,
                }
            }
        };

        let elapsed = wall_start.elapsed();
        let end = SystemTime::now();
        let duration_secs = elapsed.as_secs_f64();
        let throughput_gbps = (bytes as f64 * 8.0) / (duration_secs * 1_000_000_000.0);

        if !json_output {
            let throughput = Throughput::new(bytes, elapsed);
            println!(
                "iteration {i}/{}: {} ({}) {throughput} in {elapsed:?}",
                args.iterations,
                bytes,
                ByteSize::new(bytes),
            );
        }

        iteration_results.push(IterationResult {
            iteration: i,
            start: format_time(start),
            end: format_time(end),
            duration_secs,
            bytes_transferred: bytes,
            throughput_gbps,
        });
    }

    if json_output {
        let json = serde_json::to_string_pretty(&iteration_results)?;
        if let Some(dir) = &args.output_dir {
            fs::write(format!("{dir}/iterations.json"), &json).await?;
        } else {
            println!("{json}");
        }
    }

    Ok(())
}

async fn drain_body(body: &mut Body) -> Result<(), BoxError> {
    while let Some(chunk) = body.next().await {
        let _chunk = chunk?;
    }
    Ok(())
}

fn format_time(time: SystemTime) -> String {
    DateTime::from(time)
        .fmt(Format::DateTime)
        .expect("valid time")
}
