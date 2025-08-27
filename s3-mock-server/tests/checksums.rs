// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for S3 checksum compliance

use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use s3_mock_server::S3MockServer;
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Storage backend types for parameterized testing
#[derive(Debug, Clone, Copy)]
enum StorageType {
    InMemory,
    Filesystem,
}

/// Test helper to create a mock server with the specified storage backend
async fn create_server(storage_type: StorageType) -> Result<(S3MockServer, String)> {
    let server = match storage_type {
        StorageType::InMemory => S3MockServer::builder().with_in_memory_store().build()?,
        StorageType::Filesystem => {
            let temp_dir = tempfile::tempdir()?;
            S3MockServer::builder()
                .with_local_dir_store(temp_dir.path())
                .await?
                .build()?
        }
    };

    let bucket = format!("test-bucket-{}", Uuid::new_v4());
    Ok((server, bucket))
}

/// Calculate expected checksum for given data and algorithm
fn calculate_checksum(data: &[u8], algorithm: ChecksumAlgorithm) -> String {
    use base64::Engine;
    use crc_fast::{CrcAlgorithm, Digest};

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut digest = Digest::new(CrcAlgorithm::Crc32IsoHdlc);
            digest.update(data);
            let checksum = digest.finalize();
            base64::engine::general_purpose::STANDARD.encode((checksum as u32).to_be_bytes())
        }
        ChecksumAlgorithm::Crc32C => {
            let mut digest = Digest::new(CrcAlgorithm::Crc32Iscsi);
            digest.update(data);
            let checksum = digest.finalize();
            base64::engine::general_purpose::STANDARD.encode((checksum as u32).to_be_bytes())
        }
        ChecksumAlgorithm::Crc64Nvme => {
            let mut digest = Digest::new(CrcAlgorithm::Crc64Nvme);
            digest.update(data);
            let checksum = digest.finalize();
            base64::engine::general_purpose::STANDARD.encode(checksum.to_be_bytes())
        }
        ChecksumAlgorithm::Sha1 => {
            use sha1::{Digest, Sha1};
            let mut hasher = Sha1::new();
            hasher.update(data);
            base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
        }
        ChecksumAlgorithm::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(data);
            base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
        }
        _ => panic!("Unsupported algorithm for test: {:?}", algorithm),
    }
}

// ============================================================================
// SINGLE PART UPLOAD TESTS
// ============================================================================

/// Test single-part upload with correct checksum
async fn run_single_part_correct_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-single-part.txt";
    let content = b"Hello, S3 checksum world!";
    let expected_checksum = calculate_checksum(content, algorithm.clone());

    let mut put_request = s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(content).into());

    // Set the appropriate checksum field
    put_request = match algorithm {
        ChecksumAlgorithm::Crc32 => put_request.checksum_crc32(expected_checksum.clone()),
        ChecksumAlgorithm::Crc64Nvme => put_request.checksum_crc64_nvme(expected_checksum.clone()),
        ChecksumAlgorithm::Sha1 => put_request.checksum_sha1(expected_checksum.clone()),
        ChecksumAlgorithm::Sha256 => put_request.checksum_sha256(expected_checksum.clone()),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let response = put_request.send().await?;

    // Verify the response contains the expected checksum
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert_eq!(response.checksum_crc32(), Some(expected_checksum.as_str()))
        }
        ChecksumAlgorithm::Crc64Nvme => {
            assert_eq!(
                response.checksum_crc64_nvme(),
                Some(expected_checksum.as_str())
            )
        }
        ChecksumAlgorithm::Sha1 => {
            assert_eq!(response.checksum_sha1(), Some(expected_checksum.as_str()))
        }
        ChecksumAlgorithm::Sha256 => {
            assert_eq!(response.checksum_sha256(), Some(expected_checksum.as_str()))
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test single-part upload with AWS SDK default behavior
async fn run_single_part_default_checksum(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-single-part-default.txt";
    let content = b"Hello, default checksum world!";
    let expected_crc32 = calculate_checksum(content, ChecksumAlgorithm::Crc32);

    let response = s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(content).into())
        .send()
        .await?;

    // AWS SDK automatically adds CRC32 by default - verify it's correct
    assert_eq!(response.checksum_crc32(), Some(expected_crc32.as_str()));

    // Should not have other checksums unless explicitly requested
    assert!(response.checksum_crc64_nvme().is_none());
    assert!(response.checksum_sha1().is_none());

    handle.shutdown().await?;
    Ok(())
}

// ============================================================================
// MULTIPART UPLOAD TESTS
// ============================================================================

/// Test multipart upload with part number validation
async fn run_multipart_non_consecutive_parts(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-non-consecutive.txt";
    let part_data = b"Test part data";

    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with non-consecutive numbers (1, 3)
    let part1_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part_data).into())
        .send()
        .await?;

    let part3_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(3) // Non-consecutive!
        .body(Bytes::from_static(part_data).into())
        .send()
        .await?;

    // Try to complete with non-consecutive parts
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(3)
            .e_tag(part3_response.e_tag().unwrap())
            .build(),
    ];

    let result = s3
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
        .await;

    // Should fail with InvalidPartOrder
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("InvalidPartOrder") || error_msg.contains("consecutive"));

    handle.shutdown().await?;
    Ok(())
}

// ============================================================================
// CONCRETE TEST IMPLEMENTATIONS
// ============================================================================

#[tokio::test]
async fn test_single_part_crc32_correct_in_memory() -> Result<()> {
    run_single_part_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_single_part_crc64nvme_correct_in_memory() -> Result<()> {
    run_single_part_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc64Nvme).await
}

#[tokio::test]
async fn test_single_part_sha256_correct_in_memory() -> Result<()> {
    run_single_part_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_single_part_default_checksum_in_memory() -> Result<()> {
    run_single_part_default_checksum(StorageType::InMemory).await
}

#[tokio::test]
async fn test_multipart_non_consecutive_parts_in_memory() -> Result<()> {
    run_multipart_non_consecutive_parts(StorageType::InMemory).await
}
