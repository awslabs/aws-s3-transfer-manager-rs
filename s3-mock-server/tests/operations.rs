/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
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

#[tokio::test]
async fn test_head_object() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-head-object.txt";
    let content = "Content for head object test";

    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object");

    let resp = s3
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        resp.content_length,
        Some(content.len() as i64),
        "Content length doesn't match"
    );

    assert!(resp.e_tag.is_some(), "ETag is missing");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());

    let prefix = "test-prefix/";
    let key1 = format!("{}file1.txt", prefix);
    let key2 = format!("{}file2.txt", prefix);
    let content = "Test content";

    s3.put_object()
        .bucket(&bucket)
        .key(&key1)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object 1");

    s3.put_object()
        .bucket(&bucket)
        .key(&key2)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object 2");

    let resp = s3
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(prefix)
        .send()
        .await
        .expect("Failed to list objects");

    let keys: Vec<String> = resp
        .contents()
        .iter()
        .filter_map(|obj| obj.key().map(|k| k.to_string()))
        .collect();

    assert_eq!(keys.len(), 2, "Expected 2 objects");
    assert!(keys.contains(&key1), "First object not found");
    assert!(keys.contains(&key2), "Second object not found");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_multipart_upload() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-multipart.txt";

    let create_resp = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_resp.upload_id().expect("No upload ID returned");

    let part1_content = "This is part 1 content.";
    let part2_content = "This is part 2 content.";

    let part1_resp = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(part1_content.as_bytes()))
        .send()
        .await
        .expect("Failed to upload part 1");

    let part2_resp = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from_static(part2_content.as_bytes()))
        .send()
        .await
        .expect("Failed to upload part 2");

    // Complete multipart upload
    let completed_parts = vec![
        CompletedPart::builder()
            .e_tag(part1_resp.e_tag().unwrap())
            .part_number(1)
            .build(),
        CompletedPart::builder()
            .e_tag(part2_resp.e_tag().unwrap())
            .part_number(2)
            .build(),
    ];

    let complete_resp = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .expect("Failed to complete multipart upload");

    assert!(complete_resp.e_tag().is_some(), "ETag is missing");

    let get_resp = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to get object");

    let body = get_resp.body.collect().await.expect("Failed to read body");
    let received_content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");

    let expected_content = format!("{}{}", part1_content, part2_content);
    assert_eq!(received_content, expected_content, "Content doesn't match");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_abort_multipart_upload() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-abort-multipart.txt";

    let create_resp = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_resp.upload_id().expect("No upload ID returned");

    let part_content = "This is part 1 content.";
    s3.upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(part_content.as_bytes()))
        .send()
        .await
        .expect("Failed to upload part");

    s3.abort_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("Failed to abort multipart upload");

    let result = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().build())
        .send()
        .await;

    assert!(
        result.is_err(),
        "Expected error when completing aborted upload"
    );
    assert_eq!(
        result.unwrap_err().into_service_error().code(),
        Some("NoSuchUpload")
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_put_object_with_metadata() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-metadata.txt";
    let content = "Content with metadata";

    // Put object with metadata
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content.as_bytes()))
        .content_type("text/plain")
        .metadata("custom-key", "custom-value")
        .metadata("another-key", "another-value")
        .send()
        .await
        .expect("Failed to put object with metadata");

    let resp = s3
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(
        resp.content_type(),
        Some("text/plain"),
        "Content type doesn't match"
    );

    let metadata = resp.metadata();
    assert!(metadata.is_some(), "Metadata is missing");
    let metadata = metadata.unwrap();

    assert_eq!(
        metadata.get("custom-key"),
        Some(&"custom-value".to_string()),
        "Custom metadata doesn't match"
    );

    assert_eq!(
        metadata.get("another-key"),
        Some(&"another-value".to_string()),
        "Another metadata doesn't match"
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_get_object_range() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-range.txt";
    let content = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // Put object
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object");

    // Get a range of the object (bytes 5-10)
    let resp = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=5-10")
        .send()
        .await
        .expect("Failed to get object range");

    // Verify content length
    assert_eq!(
        resp.content_length,
        Some(6), // 6 bytes (inclusive range)
        "Content length doesn't match"
    );

    // Verify content
    let body = resp.body.collect().await.expect("Failed to read body");
    let received_content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");

    assert_eq!(received_content, "FGHIJK", "Range content doesn't match");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_filesystem_storage() -> Result<()> {
    use tempfile::tempdir;

    // Create a temporary directory for the test
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let temp_path = temp_dir.path();

    // Create a server with filesystem storage
    let server = S3MockServer::builder()
        .with_local_dir_store(temp_path)
        .await?
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "test-file.txt";
    let content = "This is a test file content";

    // Put an object
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("Failed to put object");

    // Get the object
    let resp = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("Failed to get object");

    // Verify content
    let body = resp.body.collect().await.expect("Failed to read body");
    let received_content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");

    assert_eq!(received_content, content, "Content doesn't match");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_error_handling_no_such_key() -> Result<()> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    let key = "non-existent-key.txt";

    // Try to get a non-existent object
    let result = s3.get_object().bucket(&bucket).key(key).send().await;

    // Verify the error
    assert!(result.is_err(), "Expected error for non-existent key");

    // Check the error type
    let error = result.unwrap_err();
    let error_str = format!("{:?}", error);
    assert!(
        error_str.contains("NoSuchKey"),
        "Expected NoSuchKey error, got: {:?}",
        error_str
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_concurrent_operations() -> Result<()> {
    use futures_util::future::join_all;

    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("Failed to build server");

    let handle = server.start().await.expect("Failed to start mock server");
    let s3 = handle.client().await;
    let bucket = format!("test-bucket-{}", Uuid::new_v4());

    // Create multiple upload futures
    let mut upload_futures = Vec::new();
    for i in 0..10 {
        let key = format!("concurrent-file-{}.txt", i);
        let content = format!("Content for file {}", i);
        let content_bytes = content.into_bytes();

        let s3_clone = s3.clone();
        let bucket_clone = bucket.clone();

        let upload_future = async move {
            s3_clone
                .put_object()
                .bucket(&bucket_clone)
                .key(&key)
                .body(ByteStream::from(content_bytes))
                .send()
                .await
                .expect("Failed to put object");

            key
        };

        upload_futures.push(upload_future);
    }

    // Execute all uploads concurrently
    let uploaded_keys = join_all(upload_futures).await;

    // Create multiple download futures
    let mut download_futures = Vec::new();
    for key in &uploaded_keys {
        let s3_clone = s3.clone();
        let bucket_clone = bucket.clone();
        let key_clone = key.clone();

        let download_future = async move {
            let resp = s3_clone
                .get_object()
                .bucket(&bucket_clone)
                .key(&key_clone)
                .send()
                .await
                .expect("Failed to get object");

            let body = resp.body.collect().await.expect("Failed to read body");
            String::from_utf8(body.to_vec()).expect("Invalid UTF-8")
        };

        download_futures.push(download_future);
    }

    // Execute all downloads concurrently
    let downloaded_contents = join_all(download_futures).await;

    // Verify all contents
    for (i, content) in downloaded_contents.iter().enumerate() {
        let expected_content = format!("Content for file {}", i);
        assert_eq!(
            content, &expected_content,
            "Content doesn't match for file {}",
            i
        );
    }

    handle.shutdown().await?;
    Ok(())
}
