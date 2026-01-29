/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integration tests.

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use s3_mock_server::S3MockServer;

/// Setup transfer manager with mock server
async fn setup() -> (s3_mock_server::ServerHandle, aws_sdk_s3_transfer_manager::Client) {
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

    (handle, tm)
}

#[tokio::test]
async fn test_mpu_upload_small_file() {
    let (server_handle, tm) = setup().await;

    let content = vec![0u8; 16 * ByteUnit::Mebibyte.as_bytes_usize()]; // 16MB = 2 parts at 8MB default
    let expected_content = content.clone();

    let upload_handle = tm
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    let result = upload_handle.join().await.expect("upload complete");
    assert!(result.e_tag().is_some(), "should have etag");
    assert!(result.upload_id().is_some(), "should have upload_id for MPU");

    // TODO(redux): Use mock server's get_object API instead of going through S3 client
    // once that API is available on ServerHandle
    let s3_client = server_handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("test-key")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(body.to_vec(), expected_content);

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_mpu_upload_concurrent() {
    let (server_handle, tm) = setup().await;

    let mut handles = Vec::new();

    // Start multiple concurrent uploads
    for i in 0..5 {
        let content = vec![i as u8; 8 * ByteUnit::Mebibyte.as_bytes_usize()];
        let key = format!("concurrent-key-{}", i);

        let upload_handle = tm
            .upload()
            .bucket("test-bucket")
            .key(&key)
            .body(InputStream::from(content))
            .initiate()
            .expect("initiate upload");

        handles.push((key, upload_handle));
    }

    // Wait for all uploads to complete
    for (key, handle) in handles {
        let result = handle.join().await;
        assert!(result.is_ok(), "upload {} should succeed: {:?}", key, result);
    }

    server_handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_upload_verify_data_integrity() {
    let (server_handle, tm) = setup().await;

    // Create content with recognizable pattern
    let content: Vec<u8> = (0..24 * ByteUnit::Mebibyte.as_bytes_usize()) // 24MB = 3 parts
        .map(|i| (i % 256) as u8)
        .collect();
    let expected_content = content.clone();

    let upload_handle = tm
        .upload()
        .bucket("test-bucket")
        .key("integrity-test")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    upload_handle.join().await.expect("upload complete");

    // TODO(redux): Use mock server's get_object API instead of going through S3 client
    // once that API is available on ServerHandle
    let s3_client = server_handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("integrity-test")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(body.to_vec(), expected_content, "data integrity check failed");

    server_handle.shutdown().await.expect("shutdown");
}
