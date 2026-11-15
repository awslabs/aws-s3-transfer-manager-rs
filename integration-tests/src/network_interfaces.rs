/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Binding connections to network interfaces through a caller-built runtime and
//! HTTP client.
//!
//! The transfer manager adopts a managed runtime and uses an HTTP client over
//! one connection pool whose per-thread partitions bind their connections to an
//! interface. The mock server listens on loopback, so binding every connection
//! to the loopback interface must still reach it, while binding to an interface
//! that does not exist must fail the request rather than silently falling back
//! to OS routing. Runs on Linux (`SO_BINDTODEVICE`) and macOS (`IP_BOUND_IF`),
//! whose loopback interfaces are named differently.

use std::time::Duration;

use aws_sdk_s3_transfer_manager::config::RuntimeConfig;
use aws_sdk_s3_transfer_manager::config::{ConcurrencyMode, PartSize};
use aws_sdk_s3_transfer_manager::types::ByteSize;

use crate::harness::{mock_tm_with_s3_config, MockTm};

#[cfg(target_os = "linux")]
const LOOPBACK: &str = "lo";
#[cfg(target_os = "macos")]
const LOOPBACK: &str = "lo0";

async fn setup(interfaces: &[&str]) -> MockTm {
    let runtime = ManagedRuntime::builder()
        .pin_threads(false)
        .build()
        .expect("build managed runtime");
    let http_client = interface_bound_http_client(runtime.handle(), interfaces);
    mock_tm_with_s3_config(
        RuntimeConfig::managed(aws_sdk_s3_transfer_manager_ext::managed(runtime)),
        |shared| shared.http_client(http_client),
        |b| {
            b.part_size(PartSize::Target(ByteSize::mib(5)))
                .concurrency(ConcurrencyMode::Explicit(8))
        },
    )
    .await
}

/// An HTTP client over one connection pool with a partition per `runtime`
/// thread, partition `i` bound to `interfaces[i % interfaces.len()]`.
fn interface_bound_http_client(
    runtime: ManagedRuntimeHandle,
    interfaces: &[&str],
) -> SharedHttpClient {
    let partitions = runtime.partitions().iter().map(|target| {
        let index = target.id().index();
        let target = target.clone();
        let spawner = DriverSpawner::from_fn(move |task| {
            drop(target.spawn(task));
        });
        Partition::new(PartitionId::from_index(index), spawner)
            .interface(interfaces[index % interfaces.len()])
    });
    let pool = ConnectionPool::builder()
        .partitions(partitions)
        .build_http()
        .expect("build connection pool");
    ManagedHttpPool::from_pool(runtime, pool)
        .expect("build managed HTTP client")
        .into_shared()
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
    let content: Vec<u8> = (0..ByteSize::mib(12).as_usize())
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
