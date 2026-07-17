/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Integration tests for transfer metrics and status.

use std::time::Duration;

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{RuntimeMode, TransferStatus};
use tokio::time::timeout;

use crate::harness::{mock_tm, MockTm};

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
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
    let m = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_upload_from_file_metrics() {
    let m = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("upload_src.dat");
    std::fs::write(&file_path, vec![0u8; size]).unwrap();

    let stream = InputStream::from_path(&file_path).expect("open file");

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_download_metrics_and_status() {
    let m = setup().await;

    let size = 25 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    m.server
        .add_object("test-bucket", "metrics-download", content, None)
        .await
        .expect("add object");

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}

#[cfg(any(unix, windows))]
async fn test_download_to_file_metrics(rt: RuntimeMode) {
    let m = mock_tm(rt).await;

    let size = 25 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    m.server
        .add_object("test-bucket", "metrics-file-download", content, None)
        .await
        .expect("add object");

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}

#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_to_file_metrics_mock_gp() {
    test_download_to_file_metrics(RuntimeMode::Managed).await;
}

#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread")]
async fn test_download_to_file_metrics_tokio_mt() {
    test_download_to_file_metrics(RuntimeMode::MultiThreadTokio).await;
}

#[tokio::test]
async fn test_upload_abort_metrics() {
    let m = setup().await;

    let size = 16 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_download_abort_status() {
    let m = setup().await;

    let size = 100 * ByteUnit::Mebibyte.as_bytes_usize();
    let content = vec![0u8; size];
    m.server
        .add_object("test-bucket", "metrics-abort-download", content, None)
        .await
        .expect("add object");

    let handle = m
        .client
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

    m.handle.shutdown().await.expect("shutdown");
}
