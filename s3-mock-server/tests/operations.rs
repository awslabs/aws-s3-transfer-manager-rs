/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::primitives::ByteStream;
use s3_mock_server::{Result, S3MockServer};
use uuid::Uuid;

#[tokio::test]
async fn test_get_object() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");

    let s3 = handle.client().await;

    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let bucket_str = bucket.as_str();

    let key = "test-file.txt";
    let content = "This is a test file content";

    s3.put_object()
        .bucket(bucket_str)
        .key(key)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object");

    let resp = s3
        .get_object()
        .bucket(bucket_str)
        .key(key)
        .send()
        .await
        .expect("Failed to get object");

    let body = resp.body.collect().await.expect("Failed to read body");

    let received_content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");

    // Verify the content matches
    assert_eq!(received_content, content, "Content doesn't match");

    // Verify the content length
    assert_eq!(
        resp.content_length,
        Some(content.len() as i64),
        "Content length doesn't match"
    );

    // Verify ETag exists
    assert!(resp.e_tag.is_some(), "ETag is missing");

    // Shutdown the server
    handle.shutdown().await?;

    Ok(())
}
