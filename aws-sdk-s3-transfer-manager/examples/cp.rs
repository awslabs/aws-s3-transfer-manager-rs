/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::time;

use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::metrics::Throughput;
use aws_sdk_s3_transfer_manager::operation::download::Body;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, TargetThroughput};
use bytes::Buf;
use clap::{CommandFactory, Parser};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::Instrument;

type BoxError = Box<dyn Error + Send + Sync>;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

fn invalid_arg(message: &str) -> ! {
    Args::command()
        .error(clap::error::ErrorKind::InvalidValue, message)
        .exit()
}

async fn do_recursive_download(
    args: Args,
    tm: aws_sdk_s3_transfer_manager::Client,
) -> Result<(), BoxError> {
    let (bucket, key_prefix) = args.source.expect_s3().parts();
    let dest = args.dest.expect_local();
    fs::create_dir_all(dest).await?;

    let start = time::Instant::now();
    let handle = tm
        .download_objects()
        .bucket(bucket)
        .key_prefix(key_prefix)
        .destination(dest)
        .send()
        .await?;

    let output = handle.join().await?;
    tracing::info!("download output: {output:?}");

    let elapsed = start.elapsed();
    let transfer_size_bytes = output.total_bytes_transferred();
    let throughput = Throughput::new(transfer_size_bytes, elapsed);

    println!(
        "downloaded {} objects totalling {transfer_size_bytes} bytes ({}) in {elapsed:?}; {throughput}",
        output.objects_downloaded(),
        ByteUnit::display(transfer_size_bytes)
    );
    Ok(())
}

async fn do_download(args: Args) -> Result<(), BoxError> {
    let (bucket, _) = args.source.expect_s3().parts();

    let tm_config = aws_sdk_s3_transfer_manager::from_env()
        .concurrency(args.concurrency.mode())
        .part_size(PartSize::Target(args.part_size))
        .load()
        .await;

    warmup(&tm_config, bucket).await?;

    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    if args.recursive {
        return do_recursive_download(args, tm).await;
    }

    let (bucket, key) = args.source.expect_s3().parts();
    let dest = fs::File::create(args.dest.expect_local()).await?;
    println!("dest file opened, starting download");

    let start = time::Instant::now();

    // TODO(aws-sdk-rust#1159) - rewrite this less naively,
    //      likely abstract this into performant utils for single file download. Higher level
    //      TM will handle it's own thread pool for filesystem work
    let mut handle = tm.download().bucket(bucket).key(key).initiate()?;

    write_body(handle.body_mut(), dest)
        .instrument(tracing::debug_span!("write-output"))
        .await?;

    let elapsed = start.elapsed();
    let obj_size_bytes = handle.object_meta().await?.content_length();
    let throughput = Throughput::new(obj_size_bytes, elapsed);

    println!(
        "downloaded {obj_size_bytes} bytes ({}) in {elapsed:?}; {throughput}",
        ByteUnit::display(obj_size_bytes)
    );

    Ok(())
}

async fn do_recursive_upload(
    args: Args,
    tm: aws_sdk_s3_transfer_manager::Client,
) -> Result<(), BoxError> {
    let Args { source, dest, .. } = args;
    let source_dir = source.expect_local();
    let (bucket, key_prefix) = dest.expect_s3().parts();

    let start = time::Instant::now();
    let handle = tm
        .upload_objects()
        .source(source_dir)
        .bucket(bucket)
        .key_prefix(key_prefix)
        .recursive(true)
        .send()
        .await?;

    let output = handle.join().await?;
    tracing::info!("recursive upload output: {output:?}");

    let elapsed = start.elapsed();
    let transfer_size_bytes = output.total_bytes_transferred();
    let throughput = Throughput::new(transfer_size_bytes, elapsed);

    println!(
        "uploaded {} objects totalling {transfer_size_bytes} bytes ({}) in {elapsed:?}; {throughput}",
        output.objects_uploaded(),
        ByteUnit::display(transfer_size_bytes)
    );
    Ok(())
}

async fn do_upload(args: Args) -> Result<(), BoxError> {
    let (bucket, key) = args.dest.expect_s3().parts();

    let tm_config = aws_sdk_s3_transfer_manager::from_env()
        .concurrency(args.concurrency.mode())
        .part_size(PartSize::Target(args.part_size))
        .load()
        .await;

    warmup(&tm_config, bucket).await?;

    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    if args.recursive {
        return do_recursive_upload(args, tm).await;
    }

    let path = args.source.expect_local();
    let file_meta = fs::metadata(path).await.expect("file metadata");

    let stream = InputStream::from_path(path)?;

    println!("starting upload");
    let start = time::Instant::now();

    let handle = tm
        .upload()
        .bucket(bucket)
        .key(key)
        .body(stream)
        .initiate()?;

    let _resp = handle.join().await?;
    let elapsed = start.elapsed();

    let obj_size_bytes = file_meta.len();
    let obj_size_mebibytes = ByteUnit::Mebibyte.convert(obj_size_bytes);
    let throughput = Throughput::new(obj_size_bytes, elapsed);

    println!(
        "uploaded {obj_size_bytes} bytes ({obj_size_mebibytes} MiB) in {elapsed:?}; {throughput}"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = dbg!(Args::parse());
    if args.tokio_console {
        console_subscriber::init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_thread_ids(true)
            .init();
    }

    tracing::debug!("using concurrency mode: {:?}", args.concurrency.mode());

    use TransferUri::*;
    let result = match (&args.source, &args.dest) {
        (Local(_), S3(_)) => do_upload(args).await,
        (Local(_), Local(_)) => invalid_arg("local to local transfer not supported"),
        (S3(_), Local(_)) => do_download(args).await,
        (S3(_), S3(_)) => invalid_arg("s3 to s3 transfer not supported"),
    };

    if let Err(ref err) = result {
        tracing::error!("transfer failed: {}", DisplayErrorContext(err.as_ref()));
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

async fn warmup(
    config: &aws_sdk_s3_transfer_manager::Config,
    bucket: &str,
) -> Result<(), BoxError> {
    println!("warming up client...");
    let s3 = config.client();

    let mut handles = Vec::new();
    for _ in 0..16 {
        let s3 = s3.clone();
        let bucket = bucket.to_owned();
        let warmup_task = async move { s3.head_bucket().bucket(bucket).send().await };
        let handle = tokio::spawn(warmup_task);
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await?;
    }

    println!("warming up complete");
    Ok(())
}
