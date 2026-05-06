/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Integration tests for transfer metrics and status.

use std::time::Duration;

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::TransferStatus;
use s3_mock_server::S3MockServer;
use tokio::time::timeout;

/// Setup transfer manager with mock server (upload-style: returns server + handle + tm).
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

/// Poll handle status until terminal, with timeout.
async fn wait_for_terminal(status_fn: impl Fn() -> TransferStatus) {
    timeout(Duration::from_secs(30), async {
        loop {
            if status_fn().is_terminal() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("transfer did not reach terminal state within 30s");
}

#[tokio::test]
async fn test_upload_metrics_and_status() {
    let (_server, server_handle, tm) = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("metrics-upload")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    wait_for_terminal(|| handle.status()).await;

    assert_eq!(handle.status(), TransferStatus::Completed);
    let metrics = handle.metrics();
    assert_eq!(metrics.network_tx, size as u64);
    assert_eq!(metrics.total_bytes, Some(size as u64));
    assert!(metrics.finished_at.is_some());
    assert!(metrics.started_at <= metrics.finished_at.unwrap());

    let output = handle.join().await.expect("join");
    assert_eq!(output.metrics.network_tx, size as u64);

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_upload_from_file_metrics() {
    let (_server, server_handle, tm) = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("upload_src.dat");
    std::fs::write(&file_path, vec![0u8; size]).unwrap();

    let stream = InputStream::from_path(&file_path).expect("open file");

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("metrics-file-upload")
        .body(stream)
        .initiate()
        .expect("initiate upload");

    wait_for_terminal(|| handle.status()).await;

    let metrics = handle.metrics();
    assert_eq!(metrics.network_tx, size as u64);
    assert_eq!(metrics.total_bytes, Some(size as u64));

    let output = handle.join().await.expect("join");
    assert_eq!(output.metrics.network_tx, size as u64);
    assert_eq!(output.metrics.disk_read, size as u64);

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_download_metrics_and_status() {
    let (server, server_handle, tm) = setup().await;

    let size = 25 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    server
        .add_object("metrics-download", content, None)
        .await
        .expect("add object");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("metrics-download")
        .initiate()
        .expect("initiate download");

    wait_for_terminal(|| handle.status()).await;

    assert_eq!(handle.status(), TransferStatus::Completed);
    let metrics = handle.metrics();
    assert_eq!(metrics.network_rx, size as u64);
    assert_eq!(metrics.total_bytes, Some(size as u64));
    assert!(metrics.finished_at.is_some());
    assert!(metrics.started_at <= metrics.finished_at.unwrap());

    let output = timeout(Duration::from_secs(30), handle.join())
        .await
        .expect("join timed out")
        .expect("join");

    assert_eq!(output.metrics.network_rx, size as u64);
    assert_eq!(output.metrics.total_bytes, Some(size as u64));
    assert!(output.metrics.finished_at.is_some());

    server_handle.shutdown().await.expect("shutdown");
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_to_file_metrics() {
    let (server, server_handle, tm) = setup().await;

    let size = 25 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    server
        .add_object("metrics-file-download", content, None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("metrics-file-download")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    let output = timeout(Duration::from_secs(30), handle.join())
        .await
        .expect("join timed out")
        .expect("join");

    assert_eq!(output.metrics.network_rx, size as u64);
    assert_eq!(output.metrics.disk_write, size as u64);
    assert_eq!(output.metrics.total_bytes, Some(size as u64));

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_upload_abort_metrics() {
    let (_server, server_handle, tm) = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];

    let handle = tm
        .upload()
        .bucket("test-bucket")
        .key("metrics-abort-upload")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    // total_bytes is set at initiation, before any I/O
    assert_eq!(handle.metrics().total_bytes, Some(size as u64));

    // Let CreateMPU and some parts start
    tokio::time::sleep(Duration::from_millis(200)).await;

    timeout(Duration::from_secs(30), handle.abort())
        .await
        .expect("abort timed out")
        .expect("abort");

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_download_abort_status() {
    let (server, server_handle, tm) = setup().await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    server
        .add_object("metrics-abort-download", content, None)
        .await
        .expect("add object");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("metrics-abort-download")
        .initiate()
        .expect("initiate download");

    // Give some time for bytes to flow
    tokio::time::sleep(Duration::from_millis(100)).await;

    timeout(Duration::from_secs(30), handle.abort())
        .await
        .expect("abort timed out");

    server_handle.shutdown().await.expect("shutdown");
}
