/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download integration tests.

use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{ConcurrencyMode, PartSize};
use s3_mock_server::S3MockServer;

/// Setup transfer manager with mock server
async fn setup() -> (
    S3MockServer,
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");

    let handle = server.start().await.expect("start mock server");
    let s3_client = handle.client().await;

    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (server, handle, tm)
}

/// Setup with custom part size for testing ranged downloads
async fn setup_with_part_size(
    part_size: usize,
) -> (
    S3MockServer,
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");

    let handle = server.start().await.expect("start mock server");
    let s3_client = handle.client().await;

    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencyMode::Explicit(1)) // Sequential for predictable behavior
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (server, handle, tm)
}

/// Helper to drain download body
async fn drain_body(
    handle: &mut aws_sdk_s3_transfer_manager::operation::download::DownloadHandle,
) -> Result<Vec<u8>, aws_sdk_s3_transfer_manager::error::Error> {
    let mut result = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        let chunk = chunk?;
        result.extend_from_slice(&chunk.data.to_vec());
    }
    Ok(result)
}

/// Test basic download with multiple ranges and verify data integrity
#[tokio::test]
async fn test_download_basic() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_with_part_size(part_size).await;

    // Create test data that spans multiple parts (12MB = 3 parts at 5MB)
    let content: Vec<u8> = (0..12 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect();
    let expected = content.clone();

    server
        .add_object("test-key", content, None)
        .await
        .expect("add object");

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    let body = drain_body(&mut handle).await.expect("download body");

    assert_eq!(body.len(), expected.len(), "size mismatch");
    assert_eq!(body, expected, "data integrity check failed");

    server_handle.shutdown().await.expect("shutdown");
}

/// Test that handle can be dropped without consuming body
#[tokio::test]
async fn test_download_body_not_consumed() {
    let (server, server_handle, tm) = setup().await;

    let content = vec![0u8; 16 * ByteUnit::Mebibyte.as_bytes_usize()];
    server
        .add_object("test-key", content, None)
        .await
        .expect("add object");

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    // Only consume first chunk, then drop
    let _ = handle.body_mut().next().await;
    drop(handle);

    // If we get here without hanging/panicking, test passes
    server_handle.shutdown().await.expect("shutdown");
}

/// Test abort cancels in-flight work
#[tokio::test]
#[ignore = "TODO(redux): Implement DownloadTransfer"]
async fn test_download_abort() {
    let part_size = ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_with_part_size(part_size).await;

    let content = vec![0u8; 25 * ByteUnit::Mebibyte.as_bytes_usize()];
    server
        .add_object("test-key", content, None)
        .await
        .expect("add object");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    // Wait for discovery to complete
    let _ = handle.object_meta().await;

    // Abort before completion
    handle.abort().await;

    // If we get here without hanging, abort worked
    server_handle.shutdown().await.expect("shutdown");
}

/// Test download of non-existent object returns error
#[tokio::test]
async fn test_download_not_found() {
    let (_server, server_handle, tm) = setup().await;

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("non-existent-key")
        .initiate()
        .expect("initiate download");

    let result = drain_body(&mut handle).await;
    assert!(result.is_err(), "should fail for non-existent object");

    server_handle.shutdown().await.expect("shutdown");
}

/// Test object metadata is available after discovery
#[tokio::test]
#[ignore = "TODO(redux): object_meta() needs to wait for discovery"]
async fn test_download_object_meta() {
    let (server, server_handle, tm) = setup().await;

    let content = vec![42u8; ByteUnit::Mebibyte.as_bytes_usize()];
    server
        .add_object("test-key", content.clone(), None)
        .await
        .expect("add object");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-key")
        .initiate()
        .expect("initiate download");

    let meta = handle.object_meta().await.expect("get object meta");
    assert_eq!(
        meta.content_length(),
        content.len() as u64,
        "content length should match"
    );

    server_handle.shutdown().await.expect("shutdown");
}

/// Test concurrent downloads
#[tokio::test]
async fn test_download_concurrent() {
    let (server, server_handle, tm) = setup().await;

    // Add multiple objects directly to server
    for i in 0..5 {
        let content: Vec<u8> = (0..2 * ByteUnit::Mebibyte.as_bytes_usize())
            .map(|j| ((i + j) % 256) as u8)
            .collect();
        server
            .add_object(&format!("concurrent-key-{}", i), content, None)
            .await
            .expect("add object");
    }

    // Start concurrent downloads
    let mut handles = Vec::new();
    for i in 0..5 {
        let handle = tm
            .download()
            .bucket("test-bucket")
            .key(&format!("concurrent-key-{}", i))
            .initiate()
            .expect("initiate download");
        handles.push((i, handle));
    }

    // Wait for all downloads
    for (i, mut handle) in handles {
        let body = drain_body(&mut handle).await;
        assert!(
            body.is_ok(),
            "download {} should succeed: {:?}",
            i,
            body.err()
        );
    }

    server_handle.shutdown().await.expect("shutdown");
}
