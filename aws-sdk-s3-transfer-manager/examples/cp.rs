/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::metrics::Throughput;
use aws_sdk_s3_transfer_manager::operation::download::Body;
use aws_sdk_s3_transfer_manager::types::{
    ConcurrencyMode, MemoryBudgetConfig, PartSize, TargetThroughput, TopologyConfig,
};
use aws_smithy_types::date_time::{DateTime, Format};
use clap::{CommandFactory, Parser};
use std::error::Error;
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

#[derive(Debug, Clone, Default, clap::ValueEnum)]
enum OutputFormat {
    #[default]
    Text,
    Json,
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
}

#[derive(Debug, Clone, clap::Args)]
#[group(multiple = false)]
struct ConcurrencyModeArg {
    /// ConcurrencyMode::Auto
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    concurrency_auto: bool,

    /// ConcurrencyMode::TargetThroughput(Gbps)
    #[arg(long)]
    target_throughput_gbps: Option<u64>,

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
            (None, Some(concurrency)) => ConcurrencyMode::Explicit(concurrency),
            (Some(gbps), None) => {
                ConcurrencyMode::TargetThroughput(TargetThroughput::new_gigabits_per_sec(gbps))
            }
            _ => ConcurrencyMode::Auto,
        }
    }
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

fn invalid_arg(message: &str) -> ! {
    Args::command()
        .error(clap::error::ErrorKind::InvalidValue, message)
        .exit()
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
        Ok(obj_size_bytes as u64)
    } else {
        let handle = tm
            .download()
            .bucket(bucket)
            .key(key)
            .write_to_path(dest)
            .await?;
        let obj_size_bytes = handle.object_meta().await?.total_object_size();
        handle.join().await?;
        Ok(obj_size_bytes as u64)
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
        .key_prefix(key_prefix)
        .destination(dest)
        .initiate()?;

    let output = handle.join().await?;
    tracing::info!("download output: {output:?}");

    let transfer_size_bytes = output.metrics.network_rx;
    println!(
        "downloaded {} objects totalling {transfer_size_bytes} bytes ({})",
        output.objects_downloaded(),
        ByteUnit::display(transfer_size_bytes)
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
        ByteUnit::display(transfer_size_bytes)
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
        let upload_key = format!("{key_prefix}/{}", entry.local);
        let source = source_dir.join(&entry.local);
        set.spawn(async move { do_single_upload(&tm, &bucket, &upload_key, &source).await });
    }
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        total += result??;
    }
    Ok(total)
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(e) = run().await {
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

async fn run() -> Result<(), BoxError> {
    let args = Args::parse();
    if args.tokio_console {
        console_subscriber::init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_thread_ids(true)
            .init();
    }

    tracing::debug!("using concurrency mode: {:?}", args.concurrency.mode());

    // Validate direction early
    use TransferUri::*;
    let is_download = match (&args.source, &args.dest) {
        (Local(_), S3(_)) => false,
        (S3(_), Local(_)) => true,
        (Local(_), Local(_)) => invalid_arg("local to local transfer not supported"),
        (S3(_), S3(_)) => invalid_arg("s3 to s3 transfer not supported"),
    };

    #[cfg(feature = "dial9")]
    let trace_dir = args
        .trace_dir
        .or_else(|| std::env::var("S3FIO_TRACE_DIR").ok());
    #[cfg(feature = "dial9")]
    let telemetry_guard = if let Some(ref trace_dir) = trace_dir {
        use dial9_tokio_telemetry::telemetry::{RotatingWriter, TelemetryCore};
        let _ = std::fs::create_dir_all(trace_dir);
        let writer = RotatingWriter::builder()
            .base_path(format!("{trace_dir}/trace.bin"))
            .max_file_size(64 * 1024 * 1024)
            .max_total_size(512 * 1024 * 1024)
            .build()
            .expect("failed to create trace writer");
        let cpu_profiling = if args.cpu_profiling {
            Some(dial9_tokio_telemetry::telemetry::cpu_profile::CpuProfilingConfig::default())
        } else {
            None
        };
        let guard = TelemetryCore::builder()
            .writer(writer)
            .trace_path(format!("{trace_dir}/trace.bin"))
            .maybe_cpu_profiling(cpu_profiling)
            .build()
            .expect("failed to create telemetry session");
        guard.enable();
        Some(guard)
    } else {
        None
    };

    #[allow(unused_mut)]
    let mut config_loader = aws_sdk_s3_transfer_manager::from_env()
        .concurrency(args.concurrency.mode())
        .part_size(PartSize::Target(args.part_size));

    // S3FIO_INTERFACES=ens5,ens6 binds the managed-thread connection pool to the
    // listed NICs (NUMA-aware via Topology::detect). Unset = default interface.
    if let Ok(interfaces) = std::env::var("S3FIO_INTERFACES") {
        let nics: Vec<String> = interfaces
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !nics.is_empty() {
            tracing::info!(?nics, "binding NICs from S3FIO_INTERFACES");
            config_loader = config_loader.topology(TopologyConfig::AutoWithNics(nics));
        }
    }

    // S3FIO_PIN_THREADS=1 pins each managed thread to its core (Topology::detect
    // assigns the cores). No effect on the synthetic uniform fallback.
    if matches!(
        std::env::var("S3FIO_PIN_THREADS").ok().as_deref(),
        Some("1") | Some("true")
    ) {
        tracing::info!("pinning managed threads (S3FIO_PIN_THREADS)");
        config_loader = config_loader.pin_threads(true);
    }

    // S3FIO_DOWNLOAD_WINDOW=N sets the per-download prefetch window (parts) — how
    // many parts to fetch ahead of the consumer. Bounds per-transfer memory.
    if let Some(parts) = std::env::var("S3FIO_DOWNLOAD_WINDOW")
        .ok()
        .and_then(|w| w.parse::<usize>().ok())
    {
        tracing::info!(parts, "download prefetch window (S3FIO_DOWNLOAD_WINDOW)");
        config_loader = config_loader.download_prefetch_window(parts);
    }

    // S3FIO_MEMORY_LIMIT=N caps the transfer manager's memory budget at N GiB
    // (in-flight plus buffered transfer data; transfers backpressure at the cap).
    if let Some(gib) = std::env::var("S3FIO_MEMORY_LIMIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
    {
        let bytes = gib * ByteUnit::Gibibyte.as_bytes_usize();
        tracing::info!(gib, "memory budget limit (S3FIO_MEMORY_LIMIT, GiB)");
        config_loader = config_loader.memory_budget(MemoryBudgetConfig::Limit(bytes));
    }

    #[cfg(feature = "dial9")]
    if let Some(guard) = telemetry_guard {
        config_loader = config_loader.telemetry_guard(guard);
    }

    let tm_config = config_loader.load().await;

    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    let json_output = matches!(args.output, OutputFormat::Json);

    let mut iteration_results = Vec::new();
    for i in 1..=args.iterations {
        let start = SystemTime::now();
        let wall_start = time::Instant::now();

        let bytes = if let Some(ref manifest_path) = args.manifest {
            let content = fs::read_to_string(manifest_path).await?;
            let entries: Vec<ManifestEntry> = serde_json::from_str(&content)?;
            if is_download {
                let (bucket, _key) = args.source.expect_s3().parts();
                let dest = args.dest.expect_local();
                do_manifest_download(&tm, bucket, &entries, dest).await?
            } else {
                let (bucket, key) = args.dest.expect_s3().parts();
                let source = args.source.expect_local();
                do_manifest_upload(&tm, bucket, key, &entries, source).await?
            }
        } else if is_download {
            let (bucket, key) = args.source.expect_s3().parts();
            let dest = args.dest.expect_local();
            if args.recursive {
                do_recursive_download(&tm, bucket, key, dest).await?
            } else {
                do_single_download(&tm, bucket, key, dest).await?
            }
        } else {
            let (bucket, key) = args.dest.expect_s3().parts();
            let source = args.source.expect_local();
            if args.recursive {
                do_recursive_upload(&tm, bucket, key, source).await?
            } else {
                do_single_upload(&tm, bucket, key, source).await?
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
                ByteUnit::display(bytes),
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
