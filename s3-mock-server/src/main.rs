/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Serves a mock S3 with in-memory storage on a loopback port.
//!
//! Usage: `s3-mock-server [<bucket>...]`
//!
//! The server creates each `<bucket>`, listens on an ephemeral port of
//! `127.0.0.1`, and prints the address, such as `127.0.0.1:49152`, as the first
//! line of standard output once it accepts connections. It serves until it
//! receives SIGINT. Requests must be signed with access key `mock-akid` and
//! secret key `mock-secret`, and address buckets path-style.

use s3_mock_server::S3MockServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let buckets: Vec<String> = std::env::args().skip(1).collect();
    if buckets.iter().any(|bucket| bucket.starts_with('-')) {
        eprintln!("usage: s3-mock-server [<bucket>...]");
        std::process::exit(2);
    }
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    for bucket in &buckets {
        server.create_bucket(bucket).await?;
    }
    let handle = server.start().await?;
    println!("{}", handle.socket_addr());
    tokio::signal::ctrl_c().await?;
    handle.shutdown().await?;
    Ok(())
}
