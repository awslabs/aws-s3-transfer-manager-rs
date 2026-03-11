/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::metrics::Throughput;
use aws_sdk_s3_transfer_manager::operation::download::Body;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, TargetThroughput};
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_types::date_time::{DateTime, Format};
use bytes::Buf;
use clap::{CommandFactory, Parser};
use std::error::Error;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{self, SystemTime};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::Instrument;

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

    if !is_dev_null {
        println!("dest file opened, starting download");
    }

    // TODO(aws-sdk-rust#1159) - rewrite this less naively,
    //      likely abstract this into performant utils for single file download. Higher level
    //      TM will handle it's own thread pool for filesystem work
    let mut handle = tm.download().bucket(bucket).key(key).initiate()?;

    if is_dev_null {
        drain_body(handle.body_mut()).await?;
    } else {
        let dest_file = fs::File::create(dest).await?;
        write_body(handle.body_mut(), dest_file)
            .instrument(tracing::debug_span!("write-output"))
            .await?;
    }

    let obj_size_bytes = handle.object_meta().await?.content_length();
    handle.join().await?;
    Ok(obj_size_bytes as u64)
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
        .send()
        .await?;

    let output = handle.join().await?;
    tracing::info!("download output: {output:?}");

    let transfer_size_bytes = output.total_bytes_transferred();
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
        .recursive(true)
        .send()
        .await?;

    let output = handle.join().await?;
    tracing::info!("recursive upload output: {output:?}");

    let transfer_size_bytes = output.total_bytes_transferred();
    println!(
        "uploaded {} objects totalling {transfer_size_bytes} bytes ({})",
        output.objects_uploaded(),
        ByteUnit::display(transfer_size_bytes)
    );
    Ok(transfer_size_bytes)
}

fn dump_threads(label: &str) {
    match std::fs::read_dir("/proc/self/task") {
        Ok(entries) => {
            let mut names: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for entry in entries.flatten() {
                let status_path = entry.path().join("status");
                let name = std::fs::read_to_string(status_path)
                    .ok()
                    .and_then(|s| {
                        s.lines()
                            .find(|l| l.starts_with("Name:"))
                            .map(|l| l.trim_start_matches("Name:").trim().to_string())
                    })
                    .unwrap_or_else(|| "unknown".into());
                *names.entry(name).or_default() += 1;
            }
            let total: usize = names.values().sum();
            let mut sorted: Vec<_> = names.into_iter().collect();
            sorted.sort_by(|a, b| b.1.cmp(&a.1));
            eprintln!("[THREADS] {label}: {total} total");
            for (name, count) in &sorted {
                eprintln!("[THREADS]   {count:>4} x {name}");
            }
        }
        Err(_) => {
            eprintln!("[THREADS] {label}: /proc not available");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
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

    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let dns_resolver = aws_smithy_dns::HickoryDnsResolver::default();
    let http_client = aws_smithy_http_client::Builder::new()
        .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
            CryptoMode::AwsLc,
        ))
        .build_with_resolver(dns_resolver);
    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared_config)
            .http_client(http_client)
            .build(),
    );
    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .concurrency(args.concurrency.mode())
        .part_size(PartSize::Target(args.part_size))
        .build();
    dump_threads("before Client::new");
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);
    dump_threads("after Client::new");

    let json_output = matches!(args.output, OutputFormat::Json);

    let mut iteration_results = Vec::new();
    for i in 1..=args.iterations {
        dump_threads(&format!("iteration {i} start"));
        let start = SystemTime::now();
        let wall_start = time::Instant::now();

        let bytes = if is_download {
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
        dump_threads(&format!("iteration {i} end"));
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

        // Clean up dest file between download iterations (not needed for /dev/null or uploads)
        if is_download && !args.recursive && i < args.iterations {
            let dest = args.dest.expect_local();
            if dest != Path::new("/dev/null") {
                let _ = fs::remove_file(dest).await;
            }
        }
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

async fn write_body(body: &mut Body, mut dest: fs::File) -> Result<(), BoxError> {
    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap().data;
        tracing::trace!("recv'd chunk remaining={}", chunk.remaining());
        let mut segment_cnt = 1;
        for segment in chunk.into_segments() {
            dest.write_all(segment.as_ref()).await?;
            tracing::trace!("wrote segment size: {}", segment.remaining());
            segment_cnt += 1;
        }
        tracing::trace!("chunk had {segment_cnt} segments");
    }
    Ok(())
}

fn format_time(time: SystemTime) -> String {
    DateTime::from(time)
        .fmt(Format::DateTime)
        .expect("valid time")
}
