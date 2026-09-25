/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Binding managed-runtime connections to network interfaces.
//!
//! The mock server listens on loopback, so binding every connection to the
//! loopback interface must still reach it, while binding to an interface that
//! does not exist must fail the request rather than silently falling back to
//! OS routing. Runs on Linux (`SO_BINDTODEVICE`) and macOS (`IP_BOUND_IF`),
//! whose loopback interfaces are named differently.

use std::time::Duration;

use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize, RuntimeMode};

use crate::harness::{mock_tm_with_s3_config, MockTm};

#[cfg(target_os = "linux")]
const LOOPBACK: &str = "lo";
#[cfg(target_os = "macos")]
const LOOPBACK: &str = "lo0";

async fn setup(interfaces: &[&str]) -> MockTm {
    let interfaces: Vec<String> = interfaces.iter().map(|s| s.to_string()).collect();
    mock_tm_with_s3_config(
        RuntimeMode::Managed,
        |s3| s3.network_interfaces(interfaces),
        |b| {
            b.part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
                .concurrency(ConcurrencyMode::Explicit(8))
        },
    )
    .await
}

async fn download(
    m: &MockTm,
    key: &str,
) -> Result<Vec<u8>, aws_sdk_s3_transfer_manager::error::Error> {
    let mut handle = m
        .client
        .download()
        .bucket("test-bucket")
        .key(key)
        .initiate()
        .expect("initiate download");
    let mut body = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        body.extend_from_slice(&chunk?.data.into_contiguous());
    }
    Ok(body)
}

/// Every managed thread bound to loopback still reaches the loopback mock
/// server, across a multi-part download spread over several threads.
#[tokio::test]
async fn test_download_bound_to_loopback() {
    let m = setup(&[LOOPBACK]).await;
    let content: Vec<u8> = (0..12 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 251) as u8)
        .collect();
    m.server
        .add_object("test-bucket", "bound", content.clone(), None)
        .await
        .expect("add object");

    let body = tokio::time::timeout(Duration::from_secs(30), download(&m, "bound"))
        .await
        .expect("download timed out")
        .expect("download over loopback-bound connections");
    assert!(body == content, "data integrity check failed");

    m.handle.shutdown().await.expect("shutdown");
}

/// Binding to a nonexistent interface fails the transfer: the binding is
/// applied, not ignored in favor of OS routing.
#[tokio::test]
async fn test_download_bound_to_missing_interface_fails() {
    let m = setup(&["s3tm-no-such0"]).await;
    m.server
        .add_object("test-bucket", "unreachable", vec![0u8; 1024], None)
        .await
        .expect("add object");

    let result = tokio::time::timeout(Duration::from_secs(30), download(&m, "unreachable"))
        .await
        .expect("download timed out");
    assert!(
        result.is_err(),
        "download must fail when the interface does not exist"
    );

    m.handle.shutdown().await.expect("shutdown");
}
