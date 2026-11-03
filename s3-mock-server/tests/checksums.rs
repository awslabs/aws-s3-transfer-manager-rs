// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for S3 checksum compliance

mod common;

use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use common::{assert_failure, failure};
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
    server.create_bucket(&bucket).await?;
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

/// S3-faithful composite value: hash the RAW (decoded) part checksum bytes,
/// base64-encode, and append `-<part_count>`.
fn expected_composite(part_checksums: &[String], algorithm: ChecksumAlgorithm) -> String {
    s3_mock_server::composite_checksum(part_checksums, algorithm)
}

// ============================================================================
// SINGLE PART UPLOAD TESTS
// ============================================================================

/// Test single-part upload with correct checksum
async fn run_put_object_correct_checksum(
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
async fn run_put_object_default_checksum(storage_type: StorageType) -> Result<()> {
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

/// Test single-part upload with incorrect checksum (should fail with BadDigest)
async fn run_put_object_incorrect_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-single-part-bad.txt";
    let content = b"Hello, S3 checksum world!";
    // Well-formed, but the checksum of other data: a malformed value fails with InvalidRequest.
    let wrong_checksum = &calculate_checksum(b"other data", algorithm.clone());

    let mut put_request = s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(content).into());

    // Set the wrong checksum
    put_request = match algorithm {
        ChecksumAlgorithm::Crc32 => put_request.checksum_crc32(wrong_checksum),
        ChecksumAlgorithm::Crc64Nvme => put_request.checksum_crc64_nvme(wrong_checksum),
        ChecksumAlgorithm::Sha1 => put_request.checksum_sha1(wrong_checksum),
        ChecksumAlgorithm::Sha256 => put_request.checksum_sha256(wrong_checksum),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let err = put_request
        .send()
        .await
        .expect_err("wrong checksum must fail");
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
        Some("BadDigest")
    );

    handle.shutdown().await?;
    Ok(())
}

/// Test multipart upload with full object checksum validation
async fn run_multipart_full_object_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-checksum.txt";
    let part1_data = b"First part of multipart upload ";
    let part2_data = b"Second part of multipart upload";

    // Create multipart upload with checksum algorithm and full object type
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    // Set part checksums
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum);
            part2_request = part2_request.checksum_crc32(part2_checksum);
        }
        ChecksumAlgorithm::Crc64Nvme => {
            part1_request = part1_request.checksum_crc64_nvme(part1_checksum);
            part2_request = part2_request.checksum_crc64_nvme(part2_checksum);
        }
        ChecksumAlgorithm::Sha1 => {
            part1_request = part1_request.checksum_sha1(part1_checksum);
            part2_request = part2_request.checksum_sha1(part2_checksum);
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum);
            part2_request = part2_request.checksum_sha256(part2_checksum);
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload with full object checksum
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    // Calculate expected full object checksum
    let mut full_data = Vec::new();
    full_data.extend_from_slice(part1_data);
    full_data.extend_from_slice(part2_data);
    let expected_checksum = calculate_checksum(&full_data, algorithm.clone());

    let mut complete_request = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        );

    // Send full object checksum with complete request
    complete_request = match algorithm {
        ChecksumAlgorithm::Crc32 => complete_request.checksum_crc32(expected_checksum.clone()),
        ChecksumAlgorithm::Crc64Nvme => {
            complete_request.checksum_crc64_nvme(expected_checksum.clone())
        }
        ChecksumAlgorithm::Sha1 => complete_request.checksum_sha1(expected_checksum.clone()),
        ChecksumAlgorithm::Sha256 => complete_request.checksum_sha256(expected_checksum.clone()),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let response = complete_request.send().await?;

    // Should complete successfully with ETag
    assert!(response.e_tag().is_some());

    // Should validate client checksum and echo it back in response
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert_eq!(response.checksum_crc32(), Some(expected_checksum.as_str()));
        }
        ChecksumAlgorithm::Crc64Nvme => {
            assert_eq!(
                response.checksum_crc64_nvme(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Sha1 => {
            assert_eq!(response.checksum_sha1(), Some(expected_checksum.as_str()));
        }
        ChecksumAlgorithm::Sha256 => {
            assert_eq!(response.checksum_sha256(), Some(expected_checksum.as_str()));
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test that GetObject returns stored checksums
async fn run_get_object_checksum_retrieval(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-get-object-checksum.txt";
    let content = b"Hello, checksum retrieval test!";
    let expected_checksum = calculate_checksum(content, algorithm.clone());

    // First, put object with checksum
    let mut put_request = s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(content).into());

    put_request = match algorithm {
        ChecksumAlgorithm::Crc32 => put_request.checksum_crc32(expected_checksum.clone()),
        ChecksumAlgorithm::Crc64Nvme => put_request.checksum_crc64_nvme(expected_checksum.clone()),
        ChecksumAlgorithm::Sha1 => put_request.checksum_sha1(expected_checksum.clone()),
        ChecksumAlgorithm::Sha256 => put_request.checksum_sha256(expected_checksum.clone()),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    put_request.send().await?;

    // Now get the object and verify checksum is returned
    let get_response = s3.get_object().bucket(&bucket).key(key).send().await?;

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert_eq!(
                get_response.checksum_crc32(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Crc64Nvme => {
            assert_eq!(
                get_response.checksum_crc64_nvme(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Sha1 => {
            assert_eq!(
                get_response.checksum_sha1(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Sha256 => {
            assert_eq!(
                get_response.checksum_sha256(),
                Some(expected_checksum.as_str())
            );
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test that HeadObject returns stored checksums
async fn run_head_object_checksum_retrieval(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-head-object-checksum.txt";
    let content = b"Hello, head object checksum test!";
    let expected_checksum = calculate_checksum(content, algorithm.clone());

    // First, put object with checksum
    let mut put_request = s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(Bytes::from_static(content).into());

    put_request = match algorithm {
        ChecksumAlgorithm::Crc32 => put_request.checksum_crc32(expected_checksum.clone()),
        ChecksumAlgorithm::Crc64Nvme => put_request.checksum_crc64_nvme(expected_checksum.clone()),
        ChecksumAlgorithm::Sha1 => put_request.checksum_sha1(expected_checksum.clone()),
        ChecksumAlgorithm::Sha256 => put_request.checksum_sha256(expected_checksum.clone()),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    put_request.send().await?;

    // Now head the object and verify checksum is returned
    let head_response = s3
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert_eq!(
                head_response.checksum_crc32(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Crc64Nvme => {
            assert_eq!(
                head_response.checksum_crc64_nvme(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Sha1 => {
            assert_eq!(
                head_response.checksum_sha1(),
                Some(expected_checksum.as_str())
            );
        }
        ChecksumAlgorithm::Sha256 => {
            assert_eq!(
                head_response.checksum_sha256(),
                Some(expected_checksum.as_str())
            );
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test that GetObject returns checksums for multipart-uploaded objects
async fn run_get_object_multipart_checksum_retrieval(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-get-multipart-checksum.txt";
    let part1_data = b"First part of multipart ";
    let part2_data = b"Second part of multipart";

    // Create and complete multipart upload with checksums
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum);
            part2_request = part2_request.checksum_crc32(part2_checksum);
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum);
            part2_request = part2_request.checksum_sha256(part2_checksum);
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await?;

    // Now get the multipart object and verify checksum is returned
    let get_response = s3.get_object().bucket(&bucket).key(key).send().await?;

    // Should have some checksum (exact value depends on implementation - full object vs composite)
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert!(get_response.checksum_crc32().is_some());
        }
        ChecksumAlgorithm::Sha256 => {
            assert!(get_response.checksum_sha256().is_some());
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test that HeadObject returns checksums for multipart-uploaded objects
async fn run_head_object_multipart_checksum_retrieval(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-head-multipart-checksum.txt";
    let part1_data = b"First part for head test ";
    let part2_data = b"Second part for head test";

    // Create and complete multipart upload with checksums
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum);
            part2_request = part2_request.checksum_crc32(part2_checksum);
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum);
            part2_request = part2_request.checksum_sha256(part2_checksum);
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await?;

    // Now head the multipart object and verify checksum is returned
    let head_response = s3
        .head_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;

    // Should have some checksum (exact value depends on implementation - full object vs composite)
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            assert!(head_response.checksum_crc32().is_some());
        }
        ChecksumAlgorithm::Sha256 => {
            assert!(head_response.checksum_sha256().is_some());
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    handle.shutdown().await?;
    Ok(())
}

/// Test UploadPart with incorrect checksum (should fail with BadDigest)
async fn run_upload_part_incorrect_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-bad-part.txt";
    let part_data = b"Test part with wrong checksum";
    // Well-formed, but the checksum of other data: a malformed value fails with InvalidRequest.
    let wrong_checksum = &calculate_checksum(b"other data", algorithm.clone());

    // Create multipart upload
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Try to upload part with wrong checksum
    let mut part_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part_data).into());

    part_request = match algorithm {
        ChecksumAlgorithm::Crc32 => part_request.checksum_crc32(wrong_checksum),
        ChecksumAlgorithm::Crc64Nvme => part_request.checksum_crc64_nvme(wrong_checksum),
        ChecksumAlgorithm::Sha1 => part_request.checksum_sha1(wrong_checksum),
        ChecksumAlgorithm::Sha256 => part_request.checksum_sha256(wrong_checksum),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let err = part_request
        .send()
        .await
        .expect_err("wrong checksum must fail");
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
        Some("BadDigest")
    );

    handle.shutdown().await?;
    Ok(())
}

/// Test multipart upload with composite checksum validation
async fn run_multipart_composite_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-composite-checksum.txt";
    let part1_data = b"First part for composite ";
    let part2_data = b"Second part for composite";

    // Create multipart upload with composite checksum type
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        // Note: AWS SDK may not have explicit composite type parameter
        // This would be specified via checksum_type if available
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum.clone());
            part2_request = part2_request.checksum_crc32(part2_checksum.clone());
        }
        ChecksumAlgorithm::Sha1 => {
            part1_request = part1_request.checksum_sha1(part1_checksum.clone());
            part2_request = part2_request.checksum_sha1(part2_checksum.clone());
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum.clone());
            part2_request = part2_request.checksum_sha256(part2_checksum.clone());
        }
        _ => panic!("Unsupported algorithm for composite: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload - composite checksum should be calculated from part checksums
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    let response = s3
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
        .await?;

    // Should complete successfully with ETag
    assert!(response.e_tag().is_some());

    // Should validate composite checksum and return the S3-faithful value:
    // hash of the raw part-checksum bytes, with a `-<part_count>` suffix.
    let expected = expected_composite(
        &[part1_checksum.clone(), part2_checksum.clone()],
        algorithm.clone(),
    );
    let actual = match algorithm {
        ChecksumAlgorithm::Crc32 => response.checksum_crc32(),
        ChecksumAlgorithm::Sha1 => response.checksum_sha1(),
        ChecksumAlgorithm::Sha256 => response.checksum_sha256(),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };
    assert_eq!(actual, Some(expected.as_str()));

    handle.shutdown().await?;
    Ok(())
}

/// A composite MPU object: an aligned ranged GET returns that part's individual
/// checksum (no `-N`); an unaligned range returns none; whole-object returns `-N`.
async fn run_ranged_get_checksum_fidelity(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-ranged-get-fidelity.bin";
    // Two distinct parts; sizes need not be >=5MiB (mock doesn't enforce it).
    let part1 = vec![0xABu8; 4096];
    let part2 = vec![0xCDu8; 2048];
    let c1 = calculate_checksum(&part1, ChecksumAlgorithm::Crc32);
    let c2 = calculate_checksum(&part2, ChecksumAlgorithm::Crc32);

    let upload_id = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await?
        .upload_id()
        .unwrap()
        .to_string();

    let e1 = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(Bytes::from(part1.clone()).into())
        .checksum_crc32(c1.clone())
        .send()
        .await?
        .e_tag()
        .unwrap()
        .to_string();
    let e2 = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(Bytes::from(part2.clone()).into())
        .checksum_crc32(c2.clone())
        .send()
        .await?
        .e_tag()
        .unwrap()
        .to_string();

    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(e1)
                        .checksum_crc32(c1.clone())
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(e2)
                        .checksum_crc32(c2.clone())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let whole = expected_composite(&[c1.clone(), c2.clone()], ChecksumAlgorithm::Crc32);
    let len1 = part1.len() as u64;
    let len2 = part2.len() as u64;

    // Aligned to part 1 → part1's own checksum, NO `-N`.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range(format!("bytes=0-{}", len1 - 1))
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), Some(c1.as_str()), "aligned part1 range");

    // Aligned to part 2 → part2's own checksum.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range(format!("bytes={}-{}", len1, len1 + len2 - 1))
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), Some(c2.as_str()), "aligned part2 range");

    // Unaligned range → no checksum.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .range("bytes=10-100")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), None, "unaligned range");

    // Whole-object GET → composite `-N` value.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(
        r.checksum_crc32(),
        Some(whole.as_str()),
        "whole-object composite"
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ranged_get_checksum_fidelity_in_memory() -> Result<()> {
    run_ranged_get_checksum_fidelity(StorageType::InMemory).await
}

#[tokio::test]
async fn test_ranged_get_checksum_fidelity_filesystem() -> Result<()> {
    run_ranged_get_checksum_fidelity(StorageType::Filesystem).await
}

/// A `partNumber=N` GET returns exactly that stored part's bytes, the part's own
/// checksum (no `-N`), `parts_count`, and a `Content-Range` with the total object
/// size. This is the contract the TM's download range-alignment relies on; pin it
/// so the mock cannot silently diverge from S3.
async fn run_part_number_get_fidelity(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-part-number-fidelity.bin";
    // Uniform interior + smaller tail (the ragged-tail case).
    let part1 = vec![0x11u8; 4096];
    let part2 = vec![0x22u8; 4096];
    let part3 = vec![0x33u8; 1024];
    let c1 = calculate_checksum(&part1, ChecksumAlgorithm::Crc32);
    let c2 = calculate_checksum(&part2, ChecksumAlgorithm::Crc32);
    let c3 = calculate_checksum(&part3, ChecksumAlgorithm::Crc32);
    let total = (part1.len() + part2.len() + part3.len()) as u64;

    let upload_id = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await?
        .upload_id()
        .unwrap()
        .to_string();

    let mut etags = Vec::new();
    for (i, (part, c)) in [(&part1, &c1), (&part2, &c2), (&part3, &c3)]
        .iter()
        .enumerate()
    {
        let etag = s3
            .upload_part()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(i as i32 + 1)
            .body(Bytes::from((*part).clone()).into())
            .checksum_crc32((*c).clone())
            .send()
            .await?
            .e_tag()
            .unwrap()
            .to_string();
        etags.push(etag);
    }

    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(&etags[0])
                        .checksum_crc32(&c1)
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(&etags[1])
                        .checksum_crc32(&c2)
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(3)
                        .e_tag(&etags[2])
                        .checksum_crc32(&c3)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    // partNumber=1 → part 1's bytes + checksum + parts_count + Content-Range/total.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), Some(c1.as_str()), "part1 checksum");
    assert_eq!(r.parts_count(), Some(3), "parts_count");
    assert_eq!(r.content_length(), Some(part1.len() as i64), "part1 length");
    assert_eq!(
        r.content_range(),
        Some(format!("bytes 0-{}/{}", part1.len() - 1, total).as_str()),
        "part1 content-range carries total size"
    );

    // partNumber=3 → the ragged tail's bytes + checksum.
    let r = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(3)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), Some(c3.as_str()), "tail part checksum");
    assert_eq!(r.content_length(), Some(part3.len() as i64), "tail length");
    let body = r.body.collect().await?.to_vec();
    assert_eq!(body, part3, "tail part bytes");

    // A partNumber HEAD describes the part, as the GET returns it.
    let r = s3
        .head_object()
        .bucket(&bucket)
        .key(key)
        .part_number(1)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(r.checksum_crc32(), Some(c1.as_str()), "HEAD part1 checksum");
    assert_eq!(r.parts_count(), Some(3), "HEAD parts_count");
    assert_eq!(
        r.content_length(),
        Some(part1.len() as i64),
        "HEAD part1 length"
    );
    assert_eq!(
        r.content_range(),
        Some(format!("bytes 0-{}/{}", part1.len() - 1, total).as_str()),
    );

    // A part number the object does not have fails with 416 InvalidPartNumber, as in S3.
    for part_number in [4, 99] {
        let err = s3
            .get_object()
            .bucket(&bucket)
            .key(key)
            .part_number(part_number)
            .send()
            .await
            .expect_err("out-of-range partNumber must fail");
        assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(416));
        assert_eq!(
            aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
            Some("InvalidPartNumber")
        );
    }

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_part_number_get_fidelity_in_memory() -> Result<()> {
    run_part_number_get_fidelity(StorageType::InMemory).await
}

#[tokio::test]
async fn test_part_number_get_fidelity_filesystem() -> Result<()> {
    run_part_number_get_fidelity(StorageType::Filesystem).await
}

/// Test CompleteMultipartUpload with incorrect composite checksum (should fail with BadDigest)
async fn run_complete_multipart_incorrect_composite_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-complete-composite-bad.txt";
    let part1_data = b"First part data ";
    let part2_data = b"Second part data";

    // Create multipart upload for composite checksum
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with correct checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum);
            part2_request = part2_request.checksum_crc32(part2_checksum);
        }
        ChecksumAlgorithm::Sha1 => {
            part1_request = part1_request.checksum_sha1(part1_checksum);
            part2_request = part2_request.checksum_sha1(part2_checksum);
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum);
            part2_request = part2_request.checksum_sha256(part2_checksum);
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload with WRONG composite checksum
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    let wrong_composite_checksum = "wrong-composite-checksum";

    let mut complete_request = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        );

    // Send wrong composite checksum
    complete_request = match algorithm {
        ChecksumAlgorithm::Crc32 => complete_request.checksum_crc32(wrong_composite_checksum),
        ChecksumAlgorithm::Sha1 => complete_request.checksum_sha1(wrong_composite_checksum),
        ChecksumAlgorithm::Sha256 => complete_request.checksum_sha256(wrong_composite_checksum),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let result = complete_request.send().await;

    // Should fail with BadDigest error
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("BadDigest") || error_msg.contains("checksum"));

    handle.shutdown().await?;
    Ok(())
}

/// Test CompleteMultipartUpload with incorrect full object checksum (should fail with BadDigest)
async fn run_complete_multipart_incorrect_full_object_checksum(
    storage_type: StorageType,
    algorithm: ChecksumAlgorithm,
) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-complete-multipart-bad.txt";
    let part1_data = b"First part data ";
    let part2_data = b"Second part data";

    // Create multipart upload
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with correct checksums
    let part1_checksum = calculate_checksum(part1_data, algorithm.clone());
    let part2_checksum = calculate_checksum(part2_data, algorithm.clone());

    let mut part1_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part1_data).into());

    let mut part2_request = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part2_data).into());

    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            part1_request = part1_request.checksum_crc32(part1_checksum);
            part2_request = part2_request.checksum_crc32(part2_checksum);
        }
        ChecksumAlgorithm::Sha256 => {
            part1_request = part1_request.checksum_sha256(part1_checksum);
            part2_request = part2_request.checksum_sha256(part2_checksum);
        }
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    }

    let part1_response = part1_request.send().await?;
    let part2_response = part2_request.send().await?;

    // Complete multipart upload with WRONG full object checksum
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
    ];

    let wrong_checksum = "wrong-full-object-checksum";

    let mut complete_request = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        );

    // Send wrong full object checksum
    complete_request = match algorithm {
        ChecksumAlgorithm::Crc32 => complete_request.checksum_crc32(wrong_checksum),
        ChecksumAlgorithm::Sha256 => complete_request.checksum_sha256(wrong_checksum),
        _ => panic!("Unsupported algorithm: {:?}", algorithm),
    };

    let result = complete_request.send().await;

    // Should fail with BadDigest error
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("BadDigest") || error_msg.contains("checksum"));

    handle.shutdown().await?;
    Ok(())
}

// ============================================================================
// MULTIPART UPLOAD TESTS
// ============================================================================

/// Test multipart upload with part number validation
/// A gapped but ascending part list (1, 3) completes successfully.
///
/// General-purpose S3 requires only that the parts list be in ascending order
/// by part number
async fn run_multipart_gapped_parts_succeed(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-gapped.txt";
    let part_data = b"Test part data";

    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with non-contiguous numbers (1, 3) — a gap, but ascending.
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
        .part_number(3)
        .body(Bytes::from_static(part_data).into())
        .send()
        .await?;

    // Complete with the gapped, ascending list.
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

    // Should succeed — ascending order with a gap is valid for general-purpose buckets.
    assert!(
        result.is_ok(),
        "a gapped but ascending part list must complete: {:?}",
        result.err()
    );

    handle.shutdown().await?;
    Ok(())
}

/// Under a composite (part-level) checksum, a non-contiguous part list must be rejected.
///
/// The part numbers must be consecutive and begin with 1 if multipart upload with Checksums for composite (or part-level) checksums.
/// A gapped list (1, 3) carrying per-part checksums must therefore fail. S3 returns an HTTP 500 Internal Server error
/// for this case.
///
/// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
async fn run_multipart_composite_checksum_gap_rejected(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-composite-gap.txt";
    let part1_data = b"First part of a composite-checksum upload.......";
    let part3_data = b"Third part of a composite-checksum upload.......";

    // Create the upload with a CRC32 composite checksum.
    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(aws_sdk_s3::types::ChecksumType::Composite)
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload parts with non-contiguous numbers (1, 3), each carrying a per-part checksum.
    let part1_checksum = calculate_checksum(part1_data, ChecksumAlgorithm::Crc32);
    let part3_checksum = calculate_checksum(part3_data, ChecksumAlgorithm::Crc32);

    let part1_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .checksum_crc32(part1_checksum.clone())
        .body(Bytes::from_static(part1_data).into())
        .send()
        .await?;

    let part3_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(3)
        .checksum_crc32(part3_checksum.clone())
        .body(Bytes::from_static(part3_data).into())
        .send()
        .await?;

    // Complete with the gapped list, carrying per-part checksums (composite regime).
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
            .checksum_crc32(part1_checksum)
            .build(),
        CompletedPart::builder()
            .part_number(3)
            .e_tag(part3_response.e_tag().unwrap())
            .checksum_crc32(part3_checksum)
            .build(),
    ];

    let result = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .checksum_type(aws_sdk_s3::types::ChecksumType::Composite)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await;

    // Must fail: composite checksum requires contiguous part numbers. S3 returns
    // an HTTP 500 Internal Server error for this. Assert on the handler's specific
    // message so the test fails if the rule is removed (rather than passing on some
    // other internal error from a deeper layer).
    let error = result
        .expect_err("a composite-checksum completion with a part-number gap must be rejected");
    let error_msg = format!("{error:?}");
    assert!(
        error_msg.contains("InternalError"),
        "expected an InternalError (HTTP 500), got: {error_msg}"
    );
    assert!(
        error_msg.contains("consecutive part numbers beginning with 1"),
        "expected the composite-checksum contiguity message, got: {error_msg}"
    );

    handle.shutdown().await?;
    Ok(())
}

/// A complete part set listed out of ascending order must fail with InvalidPartOrder.
///
/// The parts here form a full `1..=2` set but are listed descending (`2, 1`). S3 requires
/// the `<Part>` entries to appear in ascending part-number order and returns InvalidPartOrder.
async fn run_multipart_out_of_order_parts(storage_type: StorageType) -> Result<()> {
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    let key = "test-multipart-out-of-order.txt";
    let part_data = b"Test part data";

    let create_response = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await?;

    let upload_id = create_response.upload_id().unwrap();

    // Upload a complete, consecutive set of parts (1, 2).
    let part1_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(Bytes::from_static(part_data).into())
        .send()
        .await?;

    let part2_response = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(Bytes::from_static(part_data).into())
        .send()
        .await?;

    // Complete with the parts listed in descending order (2, 1).
    let completed_parts = vec![
        CompletedPart::builder()
            .part_number(2)
            .e_tag(part2_response.e_tag().unwrap())
            .build(),
        CompletedPart::builder()
            .part_number(1)
            .e_tag(part1_response.e_tag().unwrap())
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
    assert!(error_msg.contains("InvalidPartOrder"));

    handle.shutdown().await?;
    Ok(())
}

// ============================================================================
// CONCRETE TEST IMPLEMENTATIONS
// ============================================================================

#[tokio::test]
async fn test_put_object_crc32_correct_in_memory() -> Result<()> {
    run_put_object_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_put_object_crc64nvme_correct_in_memory() -> Result<()> {
    run_put_object_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc64Nvme).await
}

#[tokio::test]
async fn test_put_object_sha1_correct_in_memory() -> Result<()> {
    run_put_object_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha1).await
}

#[tokio::test]
async fn test_put_object_sha256_correct_in_memory() -> Result<()> {
    run_put_object_correct_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_put_object_default_checksum_in_memory() -> Result<()> {
    run_put_object_default_checksum(StorageType::InMemory).await
}

#[tokio::test]
async fn test_put_object_crc32_incorrect_in_memory() -> Result<()> {
    run_put_object_incorrect_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_put_object_sha256_incorrect_in_memory() -> Result<()> {
    run_put_object_incorrect_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_put_object_crc32_correct_filesystem() -> Result<()> {
    run_put_object_correct_checksum(StorageType::Filesystem, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_multipart_gapped_parts_succeed_in_memory() -> Result<()> {
    run_multipart_gapped_parts_succeed(StorageType::InMemory).await
}

#[tokio::test]
async fn test_multipart_composite_checksum_gap_rejected_in_memory() -> Result<()> {
    run_multipart_composite_checksum_gap_rejected(StorageType::InMemory).await
}

#[tokio::test]
async fn test_multipart_out_of_order_parts_in_memory() -> Result<()> {
    run_multipart_out_of_order_parts(StorageType::InMemory).await
}

#[tokio::test]
async fn test_multipart_full_object_crc32_in_memory() -> Result<()> {
    run_multipart_full_object_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_multipart_full_object_sha256_in_memory() -> Result<()> {
    run_multipart_full_object_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_multipart_full_object_crc32_filesystem() -> Result<()> {
    run_multipart_full_object_checksum(StorageType::Filesystem, ChecksumAlgorithm::Crc32).await
}

/// S3 reads `x-amz-checksum-type` case-insensitively: a lowercase `full_object` selects the same
/// full-object checksum as `FULL_OBJECT`, so the upload completes with the whole-object CRC32.
/// Fails if the type is matched case-sensitively, which stores `full_object` as an unknown type.
#[tokio::test]
async fn test_multipart_checksum_type_is_case_insensitive() -> Result<()> {
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    let key = "lowercase-checksum-type";
    let part = b"part content";

    let create = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .checksum_type(aws_sdk_s3::types::ChecksumType::from("full_object"))
        .send()
        .await?;
    assert_eq!(
        create.checksum_type(),
        Some(&aws_sdk_s3::types::ChecksumType::FullObject)
    );
    let upload_id = create.upload_id().unwrap();
    let uploaded = s3
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .checksum_crc32(calculate_checksum(part, ChecksumAlgorithm::Crc32))
        .body(Bytes::from_static(part).into())
        .send()
        .await?;
    let complete = s3
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload_id)
        .checksum_crc32(calculate_checksum(part, ChecksumAlgorithm::Crc32))
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(uploaded.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    assert_eq!(
        complete.checksum_crc32(),
        Some(calculate_checksum(part, ChecksumAlgorithm::Crc32).as_str())
    );

    handle.shutdown().await?;
    Ok(())
}

/// S3 rejects an `x-amz-checksum-type` other than `COMPOSITE` or `FULL_OBJECT`, in any case,
/// with 400 InvalidRequest.
#[tokio::test]
async fn test_multipart_invalid_checksum_type_fails() -> Result<()> {
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    for checksum_type in ["none", "bogus"] {
        let err = s3
            .create_multipart_upload()
            .bucket(&bucket)
            .key("invalid-checksum-type")
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_type(aws_sdk_s3::types::ChecksumType::from(checksum_type))
            .send()
            .await
            .expect_err(checksum_type);
        assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
        assert_eq!(
            aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
            Some("InvalidRequest")
        );
        assert_eq!(
            aws_sdk_s3::error::ProvideErrorMetadata::message(&err),
            Some("Value for x-amz-checksum-type header is invalid.")
        );
    }
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_put_object_sha1_correct_filesystem() -> Result<()> {
    run_put_object_correct_checksum(StorageType::Filesystem, ChecksumAlgorithm::Sha1).await
}

#[tokio::test]
async fn test_put_object_sha256_correct_filesystem() -> Result<()> {
    run_put_object_correct_checksum(StorageType::Filesystem, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_put_object_crc64nvme_correct_filesystem() -> Result<()> {
    run_put_object_correct_checksum(StorageType::Filesystem, ChecksumAlgorithm::Crc64Nvme).await
}

#[tokio::test]
async fn test_put_object_default_checksum_filesystem() -> Result<()> {
    run_put_object_default_checksum(StorageType::Filesystem).await
}

#[tokio::test]
async fn test_put_object_crc32_incorrect_filesystem() -> Result<()> {
    run_put_object_incorrect_checksum(StorageType::Filesystem, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_multipart_gapped_parts_succeed_filesystem() -> Result<()> {
    run_multipart_gapped_parts_succeed(StorageType::Filesystem).await
}

#[tokio::test]
async fn test_multipart_composite_checksum_gap_rejected_filesystem() -> Result<()> {
    run_multipart_composite_checksum_gap_rejected(StorageType::Filesystem).await
}

#[tokio::test]
async fn test_multipart_out_of_order_parts_filesystem() -> Result<()> {
    run_multipart_out_of_order_parts(StorageType::Filesystem).await
}

#[tokio::test]
async fn test_get_object_crc32_checksum_in_memory() -> Result<()> {
    run_get_object_checksum_retrieval(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_get_object_sha256_checksum_filesystem() -> Result<()> {
    run_get_object_checksum_retrieval(StorageType::Filesystem, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_head_object_crc64nvme_checksum_in_memory() -> Result<()> {
    run_head_object_checksum_retrieval(StorageType::InMemory, ChecksumAlgorithm::Crc64Nvme).await
}

#[tokio::test]
async fn test_head_object_sha1_checksum_filesystem() -> Result<()> {
    run_head_object_checksum_retrieval(StorageType::Filesystem, ChecksumAlgorithm::Sha1).await
}

#[tokio::test]
async fn test_upload_part_incorrect_crc32_in_memory() -> Result<()> {
    run_upload_part_incorrect_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_upload_part_incorrect_sha256_filesystem() -> Result<()> {
    run_upload_part_incorrect_checksum(StorageType::Filesystem, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_complete_multipart_incorrect_full_object_crc32_in_memory() -> Result<()> {
    run_complete_multipart_incorrect_full_object_checksum(
        StorageType::InMemory,
        ChecksumAlgorithm::Crc32,
    )
    .await
}

#[tokio::test]
async fn test_complete_multipart_incorrect_full_object_sha256_filesystem() -> Result<()> {
    run_complete_multipart_incorrect_full_object_checksum(
        StorageType::Filesystem,
        ChecksumAlgorithm::Sha256,
    )
    .await
}

#[tokio::test]
async fn test_multipart_composite_crc32_in_memory() -> Result<()> {
    run_multipart_composite_checksum(StorageType::InMemory, ChecksumAlgorithm::Crc32).await
}

#[tokio::test]
async fn test_multipart_composite_sha1_filesystem() -> Result<()> {
    run_multipart_composite_checksum(StorageType::Filesystem, ChecksumAlgorithm::Sha1).await
}

#[tokio::test]
async fn test_multipart_composite_sha256_in_memory() -> Result<()> {
    run_multipart_composite_checksum(StorageType::InMemory, ChecksumAlgorithm::Sha256).await
}

#[tokio::test]
async fn test_complete_multipart_incorrect_composite_crc32_filesystem() -> Result<()> {
    run_complete_multipart_incorrect_composite_checksum(
        StorageType::Filesystem,
        ChecksumAlgorithm::Crc32,
    )
    .await
}

#[tokio::test]
async fn test_complete_multipart_incorrect_composite_sha1_in_memory() -> Result<()> {
    run_complete_multipart_incorrect_composite_checksum(
        StorageType::InMemory,
        ChecksumAlgorithm::Sha1,
    )
    .await
}

#[tokio::test]
async fn test_get_object_multipart_crc32_in_memory() -> Result<()> {
    run_get_object_multipart_checksum_retrieval(StorageType::InMemory, ChecksumAlgorithm::Crc32)
        .await
}

#[tokio::test]
async fn test_get_object_multipart_sha256_filesystem() -> Result<()> {
    run_get_object_multipart_checksum_retrieval(StorageType::Filesystem, ChecksumAlgorithm::Sha256)
        .await
}

#[tokio::test]
async fn test_head_object_multipart_crc32_in_memory() -> Result<()> {
    run_head_object_multipart_checksum_retrieval(StorageType::InMemory, ChecksumAlgorithm::Crc32)
        .await
}

#[tokio::test]
async fn test_head_object_multipart_sha256_filesystem() -> Result<()> {
    run_head_object_multipart_checksum_retrieval(StorageType::Filesystem, ChecksumAlgorithm::Sha256)
        .await
}

/// A PutObject whose body does not match its checksum fails with `BadDigest` and leaves the key
/// as it was: an existing object keeps its content, and a missing key is not created. Fails if
/// the backend stores the body before validating it.
async fn run_put_object_bad_digest_leaves_key_unchanged(storage_type: StorageType) -> Result<()> {
    use aws_sdk_s3::error::ProvideErrorMetadata;

    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;

    s3.put_object()
        .bucket(&bucket)
        .key("existing")
        .body(Bytes::from_static(b"original").into())
        .send()
        .await?;

    for key in ["existing", "missing"] {
        let error = s3
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(Bytes::from_static(b"replacement").into())
            .checksum_crc32(calculate_checksum(b"original", ChecksumAlgorithm::Crc32))
            .send()
            .await
            .expect_err("PutObject with a mismatching checksum should fail");
        assert_eq!(error.code(), Some("BadDigest"), "{error:?}");
    }

    let object = s3
        .get_object()
        .bucket(&bucket)
        .key("existing")
        .send()
        .await?;
    assert_eq!(&object.body.collect().await?.into_bytes()[..], b"original");
    let listing = s3.list_objects_v2().bucket(&bucket).send().await?;
    let keys: Vec<_> = listing.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["existing"]);

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_put_object_bad_digest_leaves_key_unchanged_in_memory() -> Result<()> {
    run_put_object_bad_digest_leaves_key_unchanged(StorageType::InMemory).await
}

#[tokio::test]
async fn test_put_object_bad_digest_leaves_key_unchanged_filesystem() -> Result<()> {
    run_put_object_bad_digest_leaves_key_unchanged(StorageType::Filesystem).await
}

/// Objects report the checksum type S3 reports: COMPOSITE for a multipart object whose checksum
/// combines its parts' checksums, also on a range that returns one part's checksum, and
/// FULL_OBJECT otherwise. The type appears on GetObject, HeadObject, PutObject and
/// CompleteMultipartUpload responses and in ListObjectsV2 entries.
#[tokio::test]
async fn test_checksum_type_reporting() -> Result<()> {
    use aws_sdk_s3::types::{ChecksumMode, ChecksumType};
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    let parts: [&[u8]; 2] = [b"first part", b"second part"];

    let put = s3
        .put_object()
        .bucket(&bucket)
        .key("single")
        .checksum_crc32(calculate_checksum(b"single", ChecksumAlgorithm::Crc32))
        .body(Bytes::from_static(b"single").into())
        .send()
        .await?;
    assert_eq!(put.checksum_type(), Some(&ChecksumType::FullObject));

    for (key, algorithm, checksum_type) in [
        (
            "composite",
            ChecksumAlgorithm::Crc32,
            ChecksumType::Composite,
        ),
        (
            "full",
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumType::FullObject,
        ),
    ] {
        let upload_id = s3
            .create_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .checksum_algorithm(algorithm.clone())
            .send()
            .await?
            .upload_id
            .unwrap();
        let mut completed = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            let out = s3
                .upload_part()
                .bucket(&bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(i as i32 + 1)
                .checksum_algorithm(algorithm.clone())
                .body(Bytes::from_static(part).into())
                .send()
                .await?;
            completed.push(
                CompletedPart::builder()
                    .part_number(i as i32 + 1)
                    .e_tag(out.e_tag.unwrap())
                    .set_checksum_crc32(out.checksum_crc32)
                    .set_checksum_crc64_nvme(out.checksum_crc64_nvme)
                    .build(),
            );
        }
        let complete = s3
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .send()
            .await?;
        assert_eq!(complete.checksum_type(), Some(&checksum_type), "{key}");

        let get = s3
            .get_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(get.checksum_type(), Some(&checksum_type), "{key}");
        let head = s3
            .head_object()
            .bucket(&bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(head.checksum_type(), Some(&checksum_type), "{key}");
        // A range matching the first part of a composite object returns that part's checksum,
        // with the object's type.
        if checksum_type == ChecksumType::Composite {
            let first_part = s3
                .get_object()
                .bucket(&bucket)
                .key(key)
                .range(format!("bytes=0-{}", parts[0].len() - 1))
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await?;
            assert_eq!(first_part.checksum_type(), Some(&checksum_type), "{key}");
        }
        let listed = s3
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(key)
            .send()
            .await?;
        assert_eq!(
            listed.contents()[0].checksum_type(),
            Some(&checksum_type),
            "{key}"
        );
    }

    handle.shutdown().await?;
    Ok(())
}

/// Digest headers are checked as S3 checks them. A checksum that is not the base64 encoding of
/// a digest of its algorithm's size fails PutObject and UploadPart with 400 InvalidRequest
/// before the body is read; a malformed Content-MD5 fails with 400 InvalidDigest, and one that
/// does not match the body with 400 BadDigest. A body that does not match a checksum fails with
/// BadDigest and S3's message. No failed PutObject stores anything.
async fn run_digest_header_validation(storage_type: StorageType) -> Result<()> {
    use aws_sdk_s3::error::ProvideErrorMetadata;
    use base64::Engine;
    let (server, bucket) = create_server(storage_type).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    let body = || Bytes::from_static(b"body").into();
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);

    type Customize = fn(
        aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder,
        String,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder;
    let cases: [(&str, Customize, usize); 5] = [
        ("crc32", |r, v| r.checksum_crc32(v), 4),
        ("crc32c", |r, v| r.checksum_crc32_c(v), 4),
        ("crc64nvme", |r, v| r.checksum_crc64_nvme(v), 8),
        ("sha1", |r, v| r.checksum_sha1(v), 20),
        ("sha256", |r, v| r.checksum_sha256(v), 32),
    ];
    for (name, customize, len) in cases {
        for value in [b64(&vec![0; len - 1]), "not base64!".to_owned()] {
            let request = s3.put_object().bucket(&bucket).key(name).body(body());
            let error = customize(request, value)
                .send()
                .await
                .expect_err("malformed checksum");
            assert_eq!(error.code(), Some("InvalidRequest"), "{name}: {error:?}");
            assert_eq!(
                error.message(),
                Some(format!("Value for x-amz-checksum-{name} header is invalid.").as_str()),
            );
        }
    }

    // The request fails before its body is read: a stalled body read does not delay it.
    server.insert_fault(
        &bucket,
        "stalled",
        s3_mock_server::FaultType::StallRequestRead { after_bytes: 0 },
        0,
        s3_mock_server::Occurrence::Always,
    );
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        s3.put_object()
            .bucket(&bucket)
            .key("stalled")
            .checksum_crc32(b64(&[0; 3]))
            .body(body())
            .send(),
    )
    .await
    .expect("the request is answered without reading the body")
    .expect_err("malformed checksum");
    assert_eq!(error.code(), Some("InvalidRequest"));

    let error = s3
        .put_object()
        .bucket(&bucket)
        .key("crc32")
        .checksum_crc32(calculate_checksum(b"other", ChecksumAlgorithm::Crc32))
        .body(body())
        .send()
        .await
        .expect_err("mismatching checksum");
    assert_eq!(error.code(), Some("BadDigest"));
    assert_eq!(
        error.message(),
        Some("The CRC32 you specified did not match the calculated checksum.")
    );

    let error = s3
        .put_object()
        .bucket(&bucket)
        .key("md5")
        .content_md5(b64(b"short"))
        .body(body())
        .send()
        .await
        .expect_err("malformed Content-MD5");
    assert_eq!(error.code(), Some("InvalidDigest"));
    let error = s3
        .put_object()
        .bucket(&bucket)
        .key("md5")
        .content_md5(b64(&md5::compute(b"other").0))
        .body(body())
        .send()
        .await
        .expect_err("mismatching Content-MD5");
    assert_eq!(error.code(), Some("BadDigest"));
    assert_eq!(
        error.message(),
        Some("The Content-MD5 you specified did not match what we received.")
    );
    let listing = s3.list_objects_v2().bucket(&bucket).send().await?;
    assert_eq!(listing.key_count(), Some(0));

    s3.put_object()
        .bucket(&bucket)
        .key("md5")
        .content_md5(b64(&md5::compute(b"body").0))
        .body(body())
        .send()
        .await?;

    let upload_id = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key("mpu")
        .send()
        .await?
        .upload_id
        .unwrap();
    let error = s3
        .upload_part()
        .bucket(&bucket)
        .key("mpu")
        .upload_id(&upload_id)
        .part_number(1)
        .checksum_crc32(b64(&[0; 3]))
        .body(body())
        .send()
        .await
        .expect_err("malformed part checksum");
    assert_eq!(error.code(), Some("InvalidRequest"));
    let error = s3
        .upload_part()
        .bucket(&bucket)
        .key("mpu")
        .upload_id(&upload_id)
        .part_number(1)
        .content_md5(b64(&md5::compute(b"other").0))
        .body(body())
        .send()
        .await
        .expect_err("mismatching part Content-MD5");
    assert_eq!(error.code(), Some("BadDigest"));

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_digest_header_validation_in_memory() -> Result<()> {
    run_digest_header_validation(StorageType::InMemory).await
}

#[tokio::test]
async fn test_digest_header_validation_filesystem() -> Result<()> {
    run_digest_header_validation(StorageType::Filesystem).await
}

/// S3 treats an object uploaded in one request as a single part: partNumber=1 on GetObject and
/// HeadObject returns the whole object with its Content-Range (none for an empty object), and
/// partNumber=2 fails with 416 InvalidPartNumber. Part numbers outside 1 to 10,000 fail with 400
/// InvalidArgument, and partNumber with a Range with 400 InvalidRequest. A HEAD with a Range
/// reports the range, as the GET.
#[tokio::test]
async fn test_part_number_and_ranged_head_of_single_part_object() -> Result<()> {
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    s3.put_object()
        .bucket(&bucket)
        .key("single")
        .body(Bytes::from_static(b"0123456789").into())
        .send()
        .await?;
    s3.put_object()
        .bucket(&bucket)
        .key("empty")
        .body(Bytes::new().into())
        .send()
        .await?;

    let get = s3
        .get_object()
        .bucket(&bucket)
        .key("single")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(get.content_range(), Some("bytes 0-9/10"));
    assert_eq!(get.parts_count(), None);
    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("single")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(head.content_range(), Some("bytes 0-9/10"));
    assert_eq!(head.content_length(), Some(10));

    for (range, content_range) in [
        ("bytes=2-5", "bytes 2-5/10"),
        ("bytes=-3", "bytes 7-9/10"),
        ("bytes=7-", "bytes 7-9/10"),
    ] {
        let head = s3
            .head_object()
            .bucket(&bucket)
            .key("single")
            .range(range)
            .send()
            .await?;
        assert_eq!(head.content_range(), Some(content_range), "{range}");
    }
    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("single")
        .send()
        .await?;
    assert_eq!(head.content_range(), None);

    let get = s3
        .get_object()
        .bucket(&bucket)
        .key("empty")
        .part_number(1)
        .send()
        .await?;
    assert_eq!(get.content_range(), None);

    let fail = failure(
        s3.get_object()
            .bucket(&bucket)
            .key("single")
            .part_number(2)
            .send()
            .await,
    );
    assert_failure(&fail, 416, "InvalidPartNumber");

    // Part numbers outside 1 to 10,000, and partNumber with a Range, are invalid requests.
    for part_number in [0, 10_001] {
        let fail = failure(
            s3.get_object()
                .bucket(&bucket)
                .key("single")
                .part_number(part_number)
                .send()
                .await,
        );
        assert_failure(&fail, 400, "InvalidArgument");
    }
    // HEAD errors have no body, so the SDK only sees a 400.
    let fail = failure(
        s3.head_object()
            .bucket(&bucket)
            .key("single")
            .part_number(1)
            .range("bytes=0-1")
            .send()
            .await,
    );
    assert_eq!(fail.status, 400);
    let fail = failure(
        s3.get_object()
            .bucket(&bucket)
            .key("single")
            .part_number(1)
            .range("bytes=0-1")
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidRequest");
    handle.shutdown().await?;
    Ok(())
}

/// A partNumber request on a multipart object of one part reports the part count and the part's
/// checksum, although the part is the whole object; without partNumber the object's composite
/// checksum is reported, without a part count, as S3 does.
#[tokio::test]
async fn test_part_number_of_one_part_multipart_object() -> Result<()> {
    use aws_sdk_s3::types::{ChecksumMode, ChecksumType};
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    let upload_id = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key("one")
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await?
        .upload_id
        .unwrap();
    let part = s3
        .upload_part()
        .bucket(&bucket)
        .key("one")
        .upload_id(&upload_id)
        .part_number(1)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .body(Bytes::from_static(b"only part").into())
        .send()
        .await?;
    let part_checksum = part.checksum_crc32.clone().unwrap();
    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key("one")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag.unwrap())
                        .checksum_crc32(&part_checksum)
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;

    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("one")
        .part_number(1)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.parts_count(), Some(1));
    assert_eq!(head.checksum_crc32(), Some(part_checksum.as_str()));
    assert_eq!(head.checksum_type(), Some(&ChecksumType::Composite));
    let get = s3
        .get_object()
        .bucket(&bucket)
        .key("one")
        .part_number(1)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(get.parts_count(), Some(1));
    assert_eq!(get.checksum_crc32(), Some(part_checksum.as_str()));

    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("one")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.parts_count(), None);
    assert!(head.checksum_crc32().unwrap().ends_with("-1"));
    handle.shutdown().await?;
    Ok(())
}

/// A range or partNumber read of part of a multipart object with a full-object checksum returns
/// no checksum, as S3 general purpose buckets do; a read of the whole object returns the object's
/// checksum.
#[tokio::test]
async fn test_part_checksums_of_full_object_multipart_object() -> Result<()> {
    use aws_sdk_s3::types::ChecksumType;
    let (server, bucket) = create_server(StorageType::InMemory).await?;
    let handle = server.start().await?;
    let s3 = handle.client().await;
    let parts: [&'static [u8]; 2] = [b"first part", b"second"];
    let upload_id = s3
        .create_multipart_upload()
        .bucket(&bucket)
        .key("mpu")
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await?
        .upload_id
        .unwrap();
    let mut completed = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let out = s3
            .upload_part()
            .bucket(&bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(i as i32 + 1)
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .body(Bytes::from_static(part).into())
            .send()
            .await?;
        completed.push(
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(out.e_tag.unwrap())
                .set_checksum_crc64_nvme(out.checksum_crc64_nvme)
                .build(),
        );
    }
    s3.complete_multipart_upload()
        .bucket(&bucket)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;

    let range = s3
        .get_object()
        .bucket(&bucket)
        .key("mpu")
        .range(format!("bytes=0-{}", parts[0].len() - 1))
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(range.checksum_crc64_nvme, None);
    assert_eq!(range.checksum_type, None);
    let get = s3
        .get_object()
        .bucket(&bucket)
        .key("mpu")
        .part_number(1)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(get.checksum_crc64_nvme, None);
    let head = s3
        .head_object()
        .bucket(&bucket)
        .key("mpu")
        .part_number(1)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(head.checksum_crc64_nvme, None);
    assert_eq!(head.checksum_type, None);

    let whole = s3
        .get_object()
        .bucket(&bucket)
        .key("mpu")
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert!(whole.checksum_crc64_nvme.is_some());
    assert_eq!(whole.checksum_type, Some(ChecksumType::FullObject));
    handle.shutdown().await?;
    Ok(())
}
