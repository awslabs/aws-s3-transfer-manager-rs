/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Runs an in-memory mock S3 server for tests outside this workspace.
//!
//! Prints the server's endpoint URL, then serves until stdin is closed.

use s3_mock_server::S3MockServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    println!("http://{}", handle.socket_addr());
    tokio::io::copy(&mut tokio::io::stdin(), &mut tokio::io::sink()).await?;
    handle.shutdown().await?;
    Ok(())
}
