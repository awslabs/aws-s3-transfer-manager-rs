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
    let wrong_checksum = "wrong-checksum-value";

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

    let result = put_request.send().await;

    // Should fail with BadDigest error
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("BadDigest") || error_msg.contains("checksum"));

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
    let head_response = s3.head_object().bucket(&bucket).key(key).send().await?;

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
    let head_response = s3.head_object().bucket(&bucket).key(key).send().await?;

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
    let wrong_checksum = "wrong-checksum-value";

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

    let result = part_request.send().await;

    // Should fail with BadDigest error
    assert!(result.is_err());
    let error_msg = format!("{:?}", result.unwrap_err());
    assert!(error_msg.contains("BadDigest") || error_msg.contains("checksum"));

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

    // Out-of-range part number → error (not a panic, not wrong data).
    let err = s3
        .get_object()
        .bucket(&bucket)
        .key(key)
        .part_number(99)
        .send()
        .await;
    assert!(err.is_err(), "out-of-range partNumber must error");

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
async fn test_multipart_non_consecutive_parts_in_memory() -> Result<()> {
    run_multipart_non_consecutive_parts(StorageType::InMemory).await
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
async fn test_multipart_non_consecutive_parts_filesystem() -> Result<()> {
    run_multipart_non_consecutive_parts(StorageType::Filesystem).await
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
