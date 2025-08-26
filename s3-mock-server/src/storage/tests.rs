// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Comprehensive tests for storage backend implementations

use crate::storage::filesystem::FilesystemStorage;
use crate::storage::in_memory::InMemoryStorage;
use crate::storage::models::ObjectMetadata;
use crate::storage::{
    CompleteMultipartUploadRequest, CreateMultipartUploadRequest, GetObjectRequest, StorageBackend,
    StoreObjectRequest, UploadPartRequest,
};
use crate::types::ObjectIntegrityChecks;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::tempdir;

/// Helper function to collect stream data into bytes
async fn collect_stream_data(
    mut stream: Box<
        dyn futures::Stream<Item = std::result::Result<Bytes, std::io::Error>>
            + Send
            + Sync
            + Unpin,
    >,
) -> Result<Bytes, std::io::Error> {
    let mut collected = Vec::new();
    while let Some(chunk) = stream.next().await {
        collected.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(collected))
}

/// Test multipart upload with range requests to ensure no blocking issues
async fn test_multipart_streaming_comprehensive<S: StorageBackend>(storage: &S) {
    let key = "test-multipart-streaming";
    let upload_id = "test-upload-streaming";
    let metadata = ObjectMetadata {
        content_length: 0,
        content_type: Some("application/octet-stream".to_string()),
        etag: "streaming-etag".to_string(),
        last_modified: SystemTime::now(),
        user_metadata: HashMap::new(),
        ..Default::default()
    };

    // Create multipart upload
    let request = CreateMultipartUploadRequest {
        key,
        upload_id,
        metadata,
        checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
    };
    storage.create_multipart_upload(request).await.unwrap();

    // Upload multiple parts with different sizes to test streaming across part boundaries
    let part1 = Bytes::from("A".repeat(1000)); // 1KB
    let part2 = Bytes::from("B".repeat(2000)); // 2KB
    let part3 = Bytes::from("C".repeat(1500)); // 1.5KB

    let request1 = UploadPartRequest {
        upload_id,
        part_number: 1,
        content: part1.clone(),
    };
    let etag1 = storage.upload_part(request1).await.unwrap().etag;

    let request2 = UploadPartRequest {
        upload_id,
        part_number: 2,
        content: part2.clone(),
    };
    let etag2 = storage.upload_part(request2).await.unwrap().etag;

    let request3 = UploadPartRequest {
        upload_id,
        part_number: 3,
        content: part3.clone(),
    };
    let etag3 = storage.upload_part(request3).await.unwrap().etag;

    // Complete multipart upload
    let complete_request = CompleteMultipartUploadRequest {
        upload_id,
        parts: vec![(1, etag1), (2, etag2), (3, etag3)],
    };
    storage
        .complete_multipart_upload(complete_request)
        .await
        .unwrap();

    // Test streaming the completed object with various ranges that span multiple parts
    let test_cases = vec![
        (None, 4500),                   // Full object
        (Some(0u64..1000u64), 1000),    // First part only
        (Some(500u64..1500u64), 1000),  // Spans first and second parts
        (Some(1000u64..3000u64), 2000), // Entire second part
        (Some(2000u64..3500u64), 1500), // Spans second and third parts
        (Some(3000u64..4500u64), 1500), // Entire third part
        (Some(4000u64..4500u64), 500),  // End of third part
        (Some(0u64..4500u64), 4500),    // Full range explicitly
    ];

    for (range, expected_size) in test_cases {
        let get_request = GetObjectRequest {
            key,
            range: range.clone(),
        };
        let response = storage.get_object(get_request).await.unwrap().unwrap();

        // Collect the stream data - this tests that streaming works without blocking
        let collected_data = collect_stream_data(response.stream).await.unwrap();
        assert_eq!(
            collected_data.len(),
            expected_size,
            "Range {:?} failed",
            range
        );

        // Verify content correctness for key test cases
        match &range {
            None => {
                // Full object should be all parts concatenated
                let expected = [part1.clone(), part2.clone(), part3.clone()].concat();
                assert_eq!(collected_data, expected);
            }
            Some(r) if r == &(0u64..4500u64) => {
                // Full range should be all parts concatenated
                let expected = [part1.clone(), part2.clone(), part3.clone()].concat();
                assert_eq!(collected_data, expected);
            }
            Some(r) if r == &(0u64..1000u64) => {
                // First 1000 bytes should be all A's
                assert!(collected_data.iter().all(|&b| b == b'A'));
            }
            Some(r) if r == &(1000u64..3000u64) => {
                // Second part should be all B's
                assert!(collected_data.iter().all(|&b| b == b'B'));
            }
            Some(r) if r == &(3000u64..4500u64) => {
                // Third part should be all C's
                assert!(collected_data.iter().all(|&b| b == b'C'));
            }
            Some(r) if r == &(500u64..1500u64) => {
                // Should be 500 A's followed by 500 B's
                assert!(collected_data[0..500].iter().all(|&b| b == b'A'));
                assert!(collected_data[500..1000].iter().all(|&b| b == b'B'));
            }
            _ => {
                // For other ranges, just verify the size is correct
            }
        }
    }

    // Clean up
    storage.delete_object(key).await.unwrap();
}

/// Test concurrent operations to ensure thread safety
async fn test_concurrent_operations<S: StorageBackend>(storage: &S) {
    let tasks = (0..10).map(|i| {
        async move {
            let key = format!("concurrent-object-{}", i);
            let content = Bytes::from(format!("Content for object {}", i));

            // Create a streaming request
            let content_for_stream = content.clone();
            let stream = futures::stream::once(async move { Ok(content_for_stream) });
            let request = StoreObjectRequest {
                key: key.clone(),
                body: Box::pin(stream),
                integrity_checks: ObjectIntegrityChecks::new().with_md5(),
                content_type: Some("text/plain".to_string()),
                user_metadata: HashMap::new(),
            };

            // Put object
            storage.put_object(request).await.unwrap();

            // Get object
            let get_request = GetObjectRequest {
                key: &key,
                range: None,
            };
            let response = storage.get_object(get_request).await.unwrap().unwrap();
            let retrieved_content = collect_stream_data(response.stream).await.unwrap();
            assert_eq!(retrieved_content, content);

            // Delete object
            storage.delete_object(&key).await.unwrap();
        }
    });

    futures::future::join_all(tasks).await;
}

#[tokio::test]
async fn test_in_memory_storage_comprehensive() {
    let storage = InMemoryStorage::new();
    test_multipart_streaming_comprehensive(&storage).await;
    test_concurrent_operations(&storage).await;
}

#[tokio::test]
async fn test_filesystem_storage_comprehensive() {
    let temp_dir = tempdir().unwrap();
    let storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
    test_multipart_streaming_comprehensive(&storage).await;
    test_concurrent_operations(&storage).await;
}

/// Test that both storage backends behave identically for the same operations
#[tokio::test]
async fn test_storage_backend_consistency() {
    let temp_dir = tempdir().unwrap();
    let fs_storage = FilesystemStorage::new(temp_dir.path()).await.unwrap();
    let mem_storage = InMemoryStorage::new();

    let key = "consistency-test";
    let content = Bytes::from("Test content for consistency");

    // Test both storages with the same operations
    for storage in [
        &fs_storage as &dyn StorageBackend,
        &mem_storage as &dyn StorageBackend,
    ] {
        // Store object
        let content_for_stream = content.clone();
        let stream = futures::stream::once(async move { Ok(content_for_stream) });
        let request = StoreObjectRequest {
            key: key.to_string(),
            body: Box::pin(stream),
            integrity_checks: ObjectIntegrityChecks::new().with_md5(),
            content_type: Some("text/plain".to_string()),
            user_metadata: HashMap::new(),
        };
        let stored_metadata = storage.put_object(request).await.unwrap();

        // Retrieve object
        let get_request = GetObjectRequest { key, range: None };
        let response = storage.get_object(get_request).await.unwrap().unwrap();
        let retrieved_content = collect_stream_data(response.stream).await.unwrap();

        // Verify consistency
        assert_eq!(retrieved_content, content);
        assert_eq!(response.metadata.content_length, content.len() as u64);
        assert_eq!(stored_metadata.content_length, content.len() as u64);

        // Clean up
        storage.delete_object(key).await.unwrap();
    }
}

#[tokio::test]
async fn test_checksum_storage_and_retrieval() {
    let in_memory = InMemoryStorage::new();
    let temp_dir = tempfile::tempdir().unwrap();
    let filesystem = FilesystemStorage::new(temp_dir.path()).await.unwrap();

    for storage in [&in_memory as &dyn StorageBackend, &filesystem] {
        test_checksum_storage_backend(storage).await;
    }
}

async fn test_checksum_storage_backend<S: StorageBackend + ?Sized>(storage: &S) {
    let key = "test-checksum-object";
    let test_data = Bytes::from("Hello, checksum world!");

    // Create integrity checks with multiple algorithms
    let integrity_checks = ObjectIntegrityChecks::new()
        .with_md5()
        .with_crc32()
        .with_sha256();

    let stream = Box::pin(futures::stream::once(async move { Ok(test_data) }));
    let request = StoreObjectRequest::new(key, stream, integrity_checks);

    // Store the object
    let stored_meta = storage.put_object(request).await.unwrap();

    // Verify checksums were calculated
    assert!(stored_meta.object_integrity.etag().is_some());
    assert!(stored_meta.object_integrity.crc32.is_some());
    assert!(stored_meta.object_integrity.sha256.is_some());

    // Retrieve object metadata
    let retrieved_meta = storage.head_object(key).await.unwrap().unwrap();

    // Verify checksums are stored in metadata
    assert!(retrieved_meta.crc32.is_some());
    assert!(retrieved_meta.sha256.is_some());
    assert_eq!(
        retrieved_meta.etag,
        stored_meta.object_integrity.etag().unwrap()
    );
}
