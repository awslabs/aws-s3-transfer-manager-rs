/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::error::Error;
use std::path::PathBuf;
use std::str::FromStr;
use std::{mem, time};

use aws_s3_transfer_manager::download::Downloader;

use aws_s3_transfer_manager::download::body::Body;
use aws_s3_transfer_manager::io::InputStream;
use aws_s3_transfer_manager::types::{ConcurrencySetting, TargetPartSize};
use aws_s3_transfer_manager::upload::{UploadRequest, Uploader};
use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_types::SdkConfig;
use bytes::Buf;
use clap::{CommandFactory, Parser};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug_span, Instrument};

type BoxError = Box<dyn Error + Send + Sync>;

const ONE_MEGABYTE: u64 = 1000 * 1000;

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

    /// Number of concurrent uploads/downloads to perform.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Part size to use
    #[arg(long, default_value_t = 8388608)]
    part_size: u64,
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
        self.0
            .strip_prefix("s3://")
            .expect("valid s3 uri prefix")
            .split_once('/')
            .expect("invalid s3 uri, missing '/' between bucket and key")
    }
}

fn invalid_arg(message: &str) -> ! {
    Args::command()
        .error(clap::error::ErrorKind::InvalidValue, message)
        .exit()
}

async fn do_download(args: Args) -> Result<(), Box<dyn Error>> {
    let config = aws_config::from_env().load().await;
    warmup(&config).await?;

    let tm = Downloader::builder()
        .sdk_config(config)
        .concurrency(args.concurrency)
        .target_part_size(args.part_size)
        .build();

    let (bucket, key) = args.source.expect_s3().parts();
    let input = GetObjectInputBuilder::default().bucket(bucket).key(key);

    let dest = fs::File::create(args.dest.expect_local()).await?;
    println!("dest file opened, starting download");

    let start = time::Instant::now();

    // TODO(aws-sdk-rust#1159) - rewrite this less naively,
    //      likely abstract this into performant utils for single file download. Higher level
    //      TM will handle it's own thread pool for filesystem work
    let mut handle = tm.download(input.into()).await?;
    let body = mem::replace(&mut handle.body, Body::empty());

    write_body(body, dest)
        .instrument(debug_span!("write-output"))
        .await?;

    let elapsed = start.elapsed();
    let obj_size_bytes = handle.object_meta.total_size();
    let obj_size_megabytes = obj_size_bytes as f64 / ONE_MEGABYTE as f64;
    let obj_size_megabits = obj_size_megabytes * 8f64;

    println!(
        "downloaded {obj_size_bytes} bytes ({obj_size_megabytes} MB) in {elapsed:?}; Mb/s: {}",
        obj_size_megabits / elapsed.as_secs_f64(),
    );

    Ok(())
}

async fn do_upload(args: Args) -> Result<(), Box<dyn Error>> {
    let config = aws_config::from_env().load().await;
    warmup(&config).await?;

    let tm = Uploader::builder()
        .sdk_config(config)
        .concurrency(ConcurrencySetting::Explicit(args.concurrency))
        .multipart_threshold_part_size(TargetPartSize::Explicit(args.part_size))
        .build();

    let path = args.source.expect_local();
    let file_meta = fs::metadata(path).await.expect("file metadata");

    let stream = InputStream::from_path(path)?;
    let (bucket, key) = args.dest.expect_s3().parts();

    let request = UploadRequest::builder()
        .bucket(bucket)
        .key(key)
        .body(stream)
        .build()?;

    println!("starting upload");
    let start = time::Instant::now();

    let handle = tm.upload(request).await?;
    let _resp = handle.join().await?;
    let elapsed = start.elapsed();

    let obj_size_bytes = file_meta.len();
    let obj_size_megabytes = obj_size_bytes as f64 / ONE_MEGABYTE as f64;
    let obj_size_megabits = obj_size_megabytes * 8f64;

    println!(
        "uploaded {obj_size_bytes} bytes ({obj_size_megabytes} MB) in {elapsed:?}; Mb/s: {}",
        obj_size_megabits / elapsed.as_secs_f64()
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .init();
    // FIXME - requires --cfg tokio_unstable flag, make opt-in via cmdline
    // console_subscriber::init();
    let args = dbg!(Args::parse());

    use TransferUri::*;
    match (&args.source, &args.dest) {
        (Local(_), S3(_)) => do_upload(args).await?,
        (Local(_), Local(_)) => invalid_arg("local to local transfer not supported"),
        (S3(_), Local(_)) => do_download(args).await?,
        (S3(_), S3(_)) => invalid_arg("s3 to s3 transfer not supported"),
    }

    Ok(())
}

async fn write_body(mut body: Body, mut dest: fs::File) -> Result<(), Box<dyn Error>> {
    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap();
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

async fn warmup(config: &SdkConfig) -> Result<(), Box<dyn Error>> {
    println!("warming up client...");
    let s3 = aws_sdk_s3::Client::new(config);
    s3.list_buckets().send().await?;
    println!("warming up complete");
    Ok(())
}
