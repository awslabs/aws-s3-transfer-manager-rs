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

/// Setup with custom part size and concurrency
async fn setup_concurrent(
    part_size: usize,
    concurrency: usize,
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
        .concurrency(ConcurrencyMode::Explicit(concurrency))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (server, handle, tm)
}

/// Setup with custom part size for testing ranged downloads (sequential)
async fn setup_with_part_size(
    part_size: usize,
) -> (
    S3MockServer,
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    setup_concurrent(part_size, 1).await
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
        meta.total_object_size(),
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

/// Generate deterministic data using prime 251 to avoid alignment patterns.
fn deterministic_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Test download to file path with concurrent multi-part download (100 MB, 5 MB parts, 8 workers).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_concurrent(part_size, 8).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    server
        .add_object("write-to-path-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("write-to-path-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    // No .s3tmp files should remain
    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().to_string_lossy().contains(".s3tmp"))
        .collect();
    assert!(tmp_files.is_empty(), "leftover temp files: {:?}", tmp_files);

    server_handle.shutdown().await.expect("shutdown");
}

/// Test download to caller-provided file handle (50 MB, 5 MB parts, 8 workers).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_file() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_concurrent(part_size, 8).await;

    let size = 50 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    server
        .add_object("write-to-file-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("file_output.dat");
    let file = std::fs::File::create(&file_path).unwrap();

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("write-to-file-key")
        .write_to_file(file)
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&file_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    server_handle.shutdown().await.expect("shutdown");
}

/// Test ranged download to file path (bytes 10000000-59999999 of 100 MB object).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_ranged() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_concurrent(part_size, 8).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    server
        .add_object("ranged-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("ranged_output.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("ranged-key")
        .range("bytes=10000000-59999999")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    let expected_len = 59_999_999 - 10_000_000 + 1;
    assert_eq!(written.len(), expected_len, "ranged file size mismatch");
    assert_eq!(
        &written[..],
        &content[10_000_000..=59_999_999],
        "ranged data integrity check failed"
    );

    server_handle.shutdown().await.expect("shutdown");
}

/// Test single-part download to file (2 MB object, 5 MB part size — no range splitting).
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_single_part() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_concurrent(part_size, 8).await;

    let size = 2 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    server
        .add_object("single-part-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("single_part.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("single-part-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "data integrity check failed");

    server_handle.shutdown().await.expect("shutdown");
}

/// Integrity stress test: 100 MB, 5 MB parts, 16 concurrent workers.
/// Exercises the batched flush path under high concurrency.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_integrity() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (server, server_handle, tm) = setup_concurrent(part_size, 16).await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = deterministic_data(size);
    server
        .add_object("integrity-key", content.clone(), None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("integrity.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("integrity-key")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(written.len(), content.len(), "size mismatch");
    assert_eq!(written, content, "byte-for-byte integrity check failed");

    server_handle.shutdown().await.expect("shutdown");
}

// TODO(vnext): integration tests to add
//
// Data integrity:
// - test_download_write_to_path_auto_concurrency: Auto concurrency mode, 50 MB
//   Verifies adaptive controller works end-to-end without crash/deadlock.
//
// Scale:
// - test_download_many_transfers: 100+ concurrent downloads, all complete with
//   correct checksums. Exercises scheduler fairness and slot buffer under load.
// - test_download_whale_and_small: one 200 MB transfer + 50 × 2 MB transfers
//   running simultaneously. Verifies large transfers don't starve small ones.
// - test_mixed_upload_download: concurrent uploads and downloads against same
//   mock server. Exercises scheduler with mixed workload types.
//
// Cancellation:
// - test_download_abort_one_of_many: start 20 transfers, abort 5 mid-flight,
//   verify the other 15 complete with correct data and no temp files leak.
// - test_download_cancel_half: start 100 transfers, cancel 50, verify rest complete.
//
// Scheduler stress:
// - test_download_high_transfer_count_limited_concurrency: 100+ transfers with
//   low explicit concurrency (e.g. 4). Verifies no starvation, all complete.
//
// Infrastructure improvements:
// - Switch all large data assertions from assert_eq! to checksum comparison
//   (e.g. aws-smithy-checksums CRC32) for better failure output and efficiency.
// - Add priority change tests when priority API is exposed on handles.
