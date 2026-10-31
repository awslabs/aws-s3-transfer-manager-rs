/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Serves a mock S3 with in-memory storage on a loopback port.
//!
//! Usage: `s3-mock-server [--slow-down-once <bucket>/<key>]... [<bucket>...]`
//!
//! The server creates each `<bucket>`. For each `--slow-down-once` object, it
//! answers the first GetObject, PutObject, or UploadPart request with `503
//! SlowDown`. It listens on an ephemeral port of `127.0.0.1`, and prints the
//! address, such as `127.0.0.1:49152`, as the first line of standard output
//! once it accepts connections. It serves until it receives SIGINT. Requests
//! must be signed with access key `mock-akid` and secret key `mock-secret`, and
//! address buckets path-style.

use s3_mock_server::{FaultType, Occurrence, S3MockServer};

const USAGE: &str = "usage: s3-mock-server [--slow-down-once <bucket>/<key>]... [<bucket>...]";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut buckets = Vec::new();
    let mut slow_down = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--slow-down-once" => match args.next().as_deref().and_then(|o| o.split_once('/')) {
                Some((bucket, key)) => slow_down.push((bucket.to_owned(), key.to_owned())),
                None => usage(),
            },
            option if option.starts_with('-') => usage(),
            _ => buckets.push(arg),
        }
    }
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    for bucket in &buckets {
        server.create_bucket(bucket).await?;
    }
    for (bucket, key) in &slow_down {
        server.insert_fault(bucket, key, FaultType::slow_down(), 0, Occurrence::Once);
    }
    let handle = server.start().await?;
    println!("{}", handle.socket_addr());
    tokio::signal::ctrl_c().await?;
    handle.shutdown().await?;
    Ok(())
}

fn usage() -> ! {
    eprintln!("{USAGE}");
    std::process::exit(2);
}
