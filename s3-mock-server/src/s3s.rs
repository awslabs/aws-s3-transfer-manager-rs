/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use async_trait::async_trait;
use base64::Engine;
use bytes::BytesMut;
use futures_util::StreamExt;
use s3s::dto::Timestamp;
use s3s::dto::{ChecksumAlgorithm, StreamingBlob};
use s3s::{S3Request, S3Response, S3Result};

use crate::error::Error;
use crate::storage::models::ObjectMetadata;
use crate::storage::StorageBackend;

/// Inner implementation of the s3s::S3 trait.
///
/// This struct implements the s3s::S3 trait, delegating all operations to the storage backend.
/// It is stateless and focuses purely on S3 API semantics and validation.
#[derive(Debug, Clone)]
pub(crate) struct Inner<S: StorageBackend + 'static> {
    /// The storage backend.
    storage: S,
}

impl<S: StorageBackend + 'static> Inner<S> {
    /// Create a new Inner with the given storage backend.
    pub(crate) fn new(storage: S) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl<S: StorageBackend + 'static> s3s::S3 for Inner<S> {
    #[tracing::instrument(level = "debug")]
    async fn get_object(
        &self,
        req: S3Request<s3s::dto::GetObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::GetObjectOutput>> {
        let input = req.input;
        let key = &input.key;

        // Get metadata first to validate range
        let metadata = match self.storage.head_object(key).await? {
            Some(metadata) => metadata,
            None => return Err(Error::NoSuchKey.into()),
        };

        // Convert s3s Range to std Range if present
        let range = input
            .range
            .as_ref()
            .map(|range_dto| {
                range_dto
                    .check(metadata.content_length)
                    .map_err(|_| Error::InvalidRange)
            })
            .transpose()?;

        // Get object stream with validated range
        let request = crate::storage::GetObjectRequest {
            key,
            range: range.clone(),
        };
        let response = match self.storage.get_object(request).await? {
            Some(response) => response,
            None => return Err(Error::NoSuchKey.into()),
        };

        let (stream, stream_metadata) = (response.stream, response.metadata);

        let mut output = s3s::dto::GetObjectOutput::default();

        // Set content length and range headers
        if let Some(range) = range {
            // For range requests, content_length is the size of the range
            let range_size = range.end - range.start;
            output.content_length = Some(range_size as i64);
            // Set content_range header to indicate what range is being returned
            output.content_range = Some(format!(
                "bytes {}-{}/{}",
                range.start,
                range.end - 1,
                metadata.content_length
            ));
        } else {
            // For full object requests, content_length is the full object size
            output.content_length = Some(metadata.content_length as i64);
        }

        // Use the stream directly
        output.body = Some(StreamingBlob::wrap(stream));

        output.e_tag = Some(stream_metadata.etag);

        let timestamp = Timestamp::from(stream_metadata.last_modified);
        output.last_modified = Some(timestamp);

        if let Some(content_type) = stream_metadata.content_type {
            if let Ok(mime) = content_type.parse() {
                output.content_type = Some(mime);
            }
        }

        output.metadata = Some(stream_metadata.user_metadata);
        // FIXME - add checksum support/storage

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn head_object(
        &self,
        req: S3Request<s3s::dto::HeadObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::HeadObjectOutput>> {
        let input = req.input;
        let key = &input.key;

        // Get object metadata from storage
        let metadata = match self.storage.head_object(key).await? {
            Some(metadata) => metadata,
            None => return Err(Error::NoSuchKey.into()),
        };

        // Build response
        let content_type = metadata.content_type.and_then(|ct| ct.parse().ok());
        let output = s3s::dto::HeadObjectOutput {
            content_length: Some(metadata.content_length as i64),
            e_tag: Some(metadata.etag),
            last_modified: Some(Timestamp::from(metadata.last_modified)),
            content_type,
            metadata: Some(metadata.user_metadata),
            ..Default::default()
        };
        // FIXME - add checksum support

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn put_object(
        &self,
        req: S3Request<s3s::dto::PutObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let input = req.input;
        if input.key.is_empty() {
            return Err(s3s::s3_error!(InvalidRequest));
        }

        let client_checksums = crate::types::ClientChecksums::from(&input);
        let integrity_checks = crate::types::ObjectIntegrityChecks::from(&client_checksums);

        // Create storage request with configured checksums
        let mut request = crate::storage::StoreObjectRequest::from(input);
        request.integrity_checks = integrity_checks;

        let stored_meta = self.storage.put_object(request).await?;

        // Validate client-provided checksums
        if let Err(msg) = stored_meta.object_integrity.validate(&client_checksums) {
            return Err(s3s::s3_error!(BadDigest, "{}", msg));
        }

        let output = s3s::dto::PutObjectOutput {
            e_tag: stored_meta.object_integrity.etag(),
            checksum_crc32: stored_meta.object_integrity.crc32,
            checksum_crc32c: stored_meta.object_integrity.crc32c,
            checksum_sha1: stored_meta.object_integrity.sha1,
            checksum_sha256: stored_meta.object_integrity.sha256,
            checksum_crc64nvme: stored_meta.object_integrity.crc64nvme,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        let input = req.input;
        let key = &input.key;

        // Extract checksum algorithm if provided
        let checksum_algorithm = input.checksum_algorithm.as_ref().map(|alg| alg.as_str());

        // Validate algorithm restrictions for multipart uploads
        if let Some(algorithm) = checksum_algorithm {
            match algorithm {
                "MD5" => {
                    // MD5 doesn't support full object checksums in multipart uploads
                    // This is only enforced when a full object checksum is provided in CompleteMultipartUpload
                }
                "CRC64NVME" | "CRC32" | "CRC32C" | "SHA1" | "SHA256" => {
                    // These are all valid for multipart uploads
                }
                _ => {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "Unsupported checksum algorithm"
                    ));
                }
            }
        }

        // Generate a unique upload ID
        let upload_id = format!("{}", uuid::Uuid::new_v4());

        // Extract content type and metadata
        let content_type = input.content_type.map(|mime| mime.to_string());
        let user_metadata = input.metadata.unwrap_or_default();

        // Create metadata for the multipart upload
        let metadata = ObjectMetadata {
            content_type,
            content_length: 0,   // Will be updated when the upload is completed
            etag: String::new(), // Will be updated when the upload is completed
            last_modified: std::time::SystemTime::now(),
            user_metadata,
            checksum_algorithm: checksum_algorithm.map(|s| s.to_string()),
            ..Default::default()
        };

        // Create the multipart upload in storage
        let request = crate::storage::CreateMultipartUploadRequest {
            key,
            upload_id: &upload_id,
            metadata,
        };
        self.storage.create_multipart_upload(request).await?;

        // Build response
        let output = s3s::dto::CreateMultipartUploadOutput {
            upload_id: Some(upload_id),
            key: Some(key.to_string()),
            bucket: Some("mock-bucket".to_string()),
            checksum_algorithm: input.checksum_algorithm,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;
        let part_number = input.part_number;

        let client_checksums = crate::types::ClientChecksums::from(&input);
        let mut integrity_checks = crate::types::ObjectIntegrityChecks::from(&client_checksums);

        // Read the part content and calculate checksums
        let mut content = BytesMut::new();
        if let Some(mut body) = input.body {
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => {
                        integrity_checks.update(&chunk);
                        content.extend_from_slice(&chunk);
                    }
                    Err(_) => return Err(s3s::s3_error!(InternalError, "Failed to read body")),
                }
            }
        }
        let content = content.freeze();
        let calculated_integrity = integrity_checks.finalize();

        // Validate client-provided checksums
        if let Err(msg) = calculated_integrity.validate(&client_checksums) {
            return Err(s3s::s3_error!(BadDigest, "{}", msg));
        }

        // Store the part and get its ETag
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number,
            content,
        };
        let response = self.storage.upload_part(request).await?;

        // Build response with checksums
        let output = s3s::dto::UploadPartOutput {
            e_tag: Some(response.etag),
            checksum_crc32: calculated_integrity.crc32,
            checksum_crc32c: calculated_integrity.crc32c,
            checksum_sha1: calculated_integrity.sha1,
            checksum_sha256: calculated_integrity.sha256,
            checksum_crc64nvme: calculated_integrity.crc64nvme,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CompleteMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;

        // Extract part information
        let mut parts = Vec::new();
        if let Some(completed_parts) = input.multipart_upload.and_then(|u| u.parts) {
            for part in completed_parts {
                let part_number = part
                    .part_number
                    .ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
                let etag = part.e_tag.ok_or_else(|| s3s::s3_error!(InvalidRequest))?;
                parts.push((part_number, etag));
            }
        }

        // Sort parts by part number
        parts.sort_by_key(|&(part_number, _)| part_number);

        // Verify part numbers are consecutive starting from 1
        for (i, (part_number, _)) in parts.iter().enumerate() {
            if *part_number != (i + 1) as i32 {
                return Err(Error::InvalidPartOrder.into());
            }
        }

        // Complete the multipart upload
        let request = crate::storage::CompleteMultipartUploadRequest { upload_id, parts };
        let response = self.storage.complete_multipart_upload(request).await?;

        // Build response
        let output = s3s::dto::CompleteMultipartUploadOutput {
            key: Some(response.key),
            e_tag: Some(response.etag),
            bucket: Some("mock-bucket".to_string()),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        let input = req.input;
        let upload_id = &input.upload_id;

        // Abort the multipart upload
        self.storage.abort_multipart_upload(upload_id).await?;

        Ok(S3Response::new(
            s3s::dto::AbortMultipartUploadOutput::default(),
        ))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects_v2(
        &self,
        req: S3Request<s3s::dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectsV2Output>> {
        let input = req.input;
        let prefix = input.prefix.as_deref();
        let delimiter = input.delimiter.as_deref();
        let max_keys = input.max_keys.unwrap_or(1000).min(1000) as usize;
        let start_after = input.start_after.as_deref();

        // List objects from storage
        let request = crate::storage::ListObjectsRequest {
            prefix,
            max_keys: Some(max_keys as i32),
            continuation_token: None,
        };
        let mut response = self.storage.list_objects(request).await?;

        // Apply start_after if specified
        if let Some(start_after) = start_after {
            response
                .objects
                .retain(|obj| obj.key.as_str() > start_after);
        }

        // Sort by key for consistent ordering
        response.objects.sort_by(|a, b| a.key.cmp(&b.key));

        // Process delimiter if specified
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();

        if let Some(delimiter) = delimiter {
            let mut seen_prefixes = std::collections::HashSet::new();

            for obj in response.objects {
                if let Some(rest) = obj.key.strip_prefix(prefix.unwrap_or("")) {
                    if let Some(index) = rest.find(delimiter) {
                        // This is a common prefix
                        let common_prefix = format!("{}{}", prefix.unwrap_or(""), &rest[..=index]);
                        if seen_prefixes.insert(common_prefix.clone()) {
                            let prefix_obj = s3s::dto::CommonPrefix {
                                prefix: Some(common_prefix),
                            };
                            common_prefixes.push(prefix_obj);
                        }
                        continue;
                    }
                }

                // This is a regular object
                let object = s3s::dto::Object {
                    key: Some(obj.key),
                    size: Some(obj.metadata.content_length as i64),
                    e_tag: Some(obj.metadata.etag),
                    last_modified: Some(Timestamp::from(obj.metadata.last_modified)),
                    ..Default::default()
                };

                contents.push(object);
            }
        } else {
            // No delimiter - just convert all objects
            for obj in response.objects {
                let object = s3s::dto::Object {
                    key: Some(obj.key),
                    size: Some(obj.metadata.content_length as i64),
                    e_tag: Some(obj.metadata.etag),
                    last_modified: Some(Timestamp::from(obj.metadata.last_modified)),
                    ..Default::default()
                };

                contents.push(object);
            }
        }

        // Handle pagination
        let total_objects = contents.len() + common_prefixes.len();
        let is_truncated = total_objects > max_keys;

        // Calculate continuation token if truncated
        let next_continuation_token = if is_truncated {
            contents
                .last()
                .and_then(|obj| obj.key.as_ref())
                .map(|last_key| base64::engine::general_purpose::STANDARD.encode(last_key))
        } else {
            None
        };

        let output = s3s::dto::ListObjectsV2Output {
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            is_truncated: Some(is_truncated),
            key_count: Some(total_objects as i32),
            max_keys: Some(max_keys as i32),
            prefix: prefix.map(|s| s.to_string()),
            delimiter: delimiter.map(|s| s.to_string()),
            name: Some("mock-bucket".to_string()),
            next_continuation_token,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;
    use futures::stream;
    use s3s::S3;

    fn create_get_object_input(bucket: &str, key: &str) -> s3s::dto::GetObjectInput {
        s3s::dto::GetObjectInput::builder()
            .bucket(s3s::dto::BucketName::from(bucket))
            .key(s3s::dto::ObjectKey::from(key))
            .build()
            .unwrap()
    }

    // helper function to create an object
    async fn create_object(
        inner: &Inner<InMemoryStorage>,
        key: &str,
        content: impl Into<bytes::Bytes>,
        _metadata_opt: Option<ObjectMetadata>,
    ) -> S3Response<s3s::dto::PutObjectOutput> {
        let content_bytes = content.into();
        let content_length = content_bytes.len() as i64;

        let stream = stream::once(async move { Ok::<_, std::io::Error>(content_bytes) });
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .body(Some(StreamingBlob::wrap(stream)))
            .key(s3s::dto::ObjectKey::from(key))
            .content_length(Some(s3s::dto::ContentLength::from(content_length)))
            .build()
            .unwrap();

        let req = S3Request::new(input);
        inner.put_object(req).await.expect("Failed to put object")
    }

    #[tokio::test]
    async fn test_get_object_range() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);
        let content = "This is a test content that is exactly 100 bytes long to make our range test work properly..........";
        let _ = create_object(&inner, "test-key", content, None).await;

        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=0-49").unwrap());

        let req = S3Request::new(input);

        // Call the method
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        // For range request "bytes=0-49", we expect:
        // - content_length to be 50 (the size of the range)
        // - content_range to indicate the range being returned
        assert_eq!(output.content_length, Some(50));
        assert_eq!(output.content_range, Some("bytes 0-49/100".to_string()));

        // The etag should be the MD5 hash of the content (calculated during put_object)
        let expected_etag = format!("\"{}\"", hex::encode(md5::compute(content).0));
        assert_eq!(output.e_tag.unwrap().as_str(), &expected_etag);

        let mut body = output.body.unwrap();

        let mut buf = BytesMut::with_capacity(output.content_length.unwrap() as usize);
        while let Some(result) = body.next().await {
            let bytes = result.expect("Failed to read body");
            buf.extend_from_slice(&bytes);
        }
        let range_contents = buf.freeze();

        // Verify we got exactly the requested range (bytes 0-49, which is 50 bytes)
        assert_eq!(range_contents.len(), 50);
        assert_eq!(&range_contents[..], &content.as_bytes()[0..50]);
    }

    #[tokio::test]
    async fn test_get_object_range_edge_cases() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);
        let content = "0123456789"; // 10 bytes
        let _ = create_object(&inner, "test-key", content, None).await;

        // Test case 1: Range at the beginning
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=0-4").unwrap());
        let req = S3Request::new(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(5));
        assert_eq!(output.content_range, Some("bytes 0-4/10".to_string()));

        // Test case 2: Range at the end
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=5-9").unwrap());
        let req = S3Request::new(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(5));
        assert_eq!(output.content_range, Some("bytes 5-9/10".to_string()));

        // Test case 3: Single byte range
        let mut input = create_get_object_input("test-bucket", "test-key");
        input.range = Some(s3s::dto::Range::parse("bytes=3-3").unwrap());
        let req = S3Request::new(input);
        let result = inner.get_object(req).await.expect("Get object failed");
        let output = result.output;

        assert_eq!(output.content_length, Some(1));
        assert_eq!(output.content_range, Some("bytes 3-3/10".to_string()));
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        let content = b"Hello, checksum world!";

        // Calculate expected CRC32 using our ObjectIntegrityChecks
        let mut checks = crate::types::ObjectIntegrityChecks::new().with_crc32();
        checks.update(content);
        let integrity = checks.finalize();
        let expected_crc32 = integrity.crc32.unwrap();

        // Test with correct checksum - should succeed
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some(expected_crc32.clone()))
            .build()
            .unwrap();

        let result = inner
            .put_object(S3Request::new(input))
            .await
            .expect("Put should succeed");
        assert_eq!(result.output.checksum_crc32, Some(expected_crc32));

        // Test with wrong checksum - should fail
        let input_wrong = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-wrong"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some("wrong-checksum".to_string()))
            .build()
            .unwrap();

        let result_wrong = inner.put_object(S3Request::new(input_wrong)).await;
        assert!(result_wrong.is_err());
    }

    #[tokio::test]
    async fn test_default_crc64nvme() {
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        let content = b"Test default checksum";

        // Test without any client checksums - should default to CRC64NVME
        let input = s3s::dto::PutObjectInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .build()
            .unwrap();

        let result = inner
            .put_object(S3Request::new(input))
            .await
            .expect("Put should succeed");

        // Should have CRC64NVME checksum by default
        assert!(result.output.checksum_crc64nvme.is_some());
        // Should not have other checksums
        assert!(result.output.checksum_crc32.is_none());
        assert!(result.output.checksum_sha1.is_none());
    }

    #[tokio::test]
    async fn test_upload_part_checksum_validation() {
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // First create a multipart upload
        let create_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .build()
            .unwrap();

        let create_result = inner
            .create_multipart_upload(S3Request::new(create_input))
            .await
            .expect("Create multipart upload failed");
        let upload_id = create_result.output.upload_id.unwrap();

        let content = b"Part content for checksum test";

        // Calculate expected CRC32 using our ObjectIntegrityChecks
        let mut checks = crate::types::ObjectIntegrityChecks::new().with_crc32();
        checks.update(content);
        let integrity = checks.finalize();
        let expected_crc32 = integrity.crc32.unwrap();

        // Test with correct checksum - should succeed
        let input = s3s::dto::UploadPartInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id.clone())
            .part_number(1)
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some(expected_crc32.clone()))
            .build()
            .unwrap();

        let result = inner
            .upload_part(S3Request::new(input))
            .await
            .expect("Upload part should succeed");
        assert_eq!(result.output.checksum_crc32, Some(expected_crc32));

        // Test with wrong checksum - should fail
        let input_wrong = s3s::dto::UploadPartInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id)
            .part_number(2)
            .body(Some(s3s::dto::StreamingBlob::wrap(
                futures_util::stream::once(async {
                    Ok::<_, std::io::Error>(bytes::Bytes::from(&content[..]))
                }),
            )))
            .checksum_crc32(Some("wrong-checksum".to_string()))
            .build()
            .unwrap();

        let result_wrong = inner.upload_part(S3Request::new(input_wrong)).await;
        assert!(result_wrong.is_err());
    }

    #[tokio::test]
    async fn test_part_number_validation() {
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Create multipart upload
        let create_input = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .build()
            .unwrap();

        let create_result = inner
            .create_multipart_upload(S3Request::new(create_input))
            .await
            .expect("Create multipart upload failed");
        let upload_id = create_result.output.upload_id.unwrap();

        // Upload parts 1 and 2 and collect their ETags
        let mut part_etags = Vec::new();
        for part_num in 1..=2 {
            let content = format!("Part {} content", part_num);
            let input = s3s::dto::UploadPartInput::builder()
                .bucket(s3s::dto::BucketName::from("test-bucket"))
                .key(s3s::dto::ObjectKey::from("test-key"))
                .upload_id(upload_id.clone())
                .part_number(part_num)
                .body(Some(s3s::dto::StreamingBlob::wrap(
                    futures_util::stream::once(async move {
                        Ok::<_, std::io::Error>(bytes::Bytes::from(content))
                    }),
                )))
                .build()
                .unwrap();

            let result = inner
                .upload_part(S3Request::new(input))
                .await
                .expect("Upload part failed");
            part_etags.push(result.output.e_tag.unwrap());
        }

        // Test valid consecutive parts [1,2] - should succeed
        let valid_parts = vec![
            s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some(part_etags[0].clone()),
                ..Default::default()
            },
            s3s::dto::CompletedPart {
                part_number: Some(2),
                e_tag: Some(part_etags[1].clone()),
                ..Default::default()
            },
        ];
        let valid_input = s3s::dto::CompleteMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key"))
            .upload_id(upload_id.clone())
            .multipart_upload(Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(valid_parts),
                ..Default::default()
            }))
            .build()
            .unwrap();

        let valid_result = inner
            .complete_multipart_upload(S3Request::new(valid_input))
            .await;
        assert!(valid_result.is_ok());

        // Create new upload for invalid test
        let create_input2 = s3s::dto::CreateMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-2"))
            .build()
            .unwrap();

        let create_result2 = inner
            .create_multipart_upload(S3Request::new(create_input2))
            .await
            .expect("Create multipart upload failed");
        let upload_id2 = create_result2.output.upload_id.unwrap();

        // Test invalid non-consecutive parts [1,3] - should fail
        let invalid_parts = vec![
            s3s::dto::CompletedPart {
                part_number: Some(1),
                e_tag: Some("etag1".to_string()),
                ..Default::default()
            },
            s3s::dto::CompletedPart {
                part_number: Some(3),
                e_tag: Some("etag3".to_string()),
                ..Default::default()
            },
        ];
        let invalid_input = s3s::dto::CompleteMultipartUploadInput::builder()
            .bucket(s3s::dto::BucketName::from("test-bucket"))
            .key(s3s::dto::ObjectKey::from("test-key-2"))
            .upload_id(upload_id2)
            .multipart_upload(Some(s3s::dto::CompletedMultipartUpload {
                parts: Some(invalid_parts),
                ..Default::default()
            }))
            .build()
            .unwrap();

        let invalid_result = inner
            .complete_multipart_upload(S3Request::new(invalid_input))
            .await;
        assert!(invalid_result.is_err());
    }
}
