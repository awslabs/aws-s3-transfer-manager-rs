/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! In-memory implementation of the StorageBackend trait.

use std::collections::HashMap;

use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::models::{MultipartUploadMetadata, ObjectMetadata, PartMetadata};
use crate::storage::StorageBackend;
use crate::streaming::{apply_range, VecByteStream};
use crate::types::StoredObjectMetadata;

/// Type alias for complex part storage structure
type PartStorage = HashMap<i32, (Bytes, PartMetadata)>;

/// An in-memory implementation of the StorageBackend trait.
///
/// This implementation stores all objects, metadata, and multipart uploads in memory,
/// making it suitable for testing and benchmarking. All data is lost when the
/// instance is dropped.
#[derive(Debug)]
pub(crate) struct InMemoryStorage {
    /// Objects stored as (key -> (data, metadata))
    objects: RwLock<HashMap<String, (Bytes, ObjectMetadata)>>,

    /// Active multipart uploads stored as (upload_id -> upload_metadata)
    multipart_uploads: RwLock<HashMap<String, MultipartUploadMetadata>>,

    /// Parts for multipart uploads stored as (upload_id -> (part_number -> (data, metadata)))
    parts: RwLock<HashMap<String, PartStorage>>,
}

impl InMemoryStorage {
    /// Create a new in-memory storage backend.
    pub(crate) fn new() -> Self {
        Self {
            objects: RwLock::new(HashMap::new()),
            multipart_uploads: RwLock::new(HashMap::new()),
            parts: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn put_object(
        &self,
        request: crate::storage::StoreObjectRequest,
    ) -> Result<StoredObjectMetadata> {
        let mut body = request.body;
        let mut integrity_checks = request.integrity_checks;
        let mut content = BytesMut::new();

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| Error::Internal(format!("Stream error: {}", e)))?;
            integrity_checks.update(&chunk);
            content.extend_from_slice(&chunk);
        }

        let content = content.freeze();
        let content_length = content.len() as u64;
        let object_integrity = integrity_checks.finalize();
        let last_modified = SystemTime::now();

        // Store with checksum metadata
        let metadata = ObjectMetadata {
            content_type: request.content_type,
            content_length,
            etag: object_integrity.etag().unwrap_or_default(),
            last_modified,
            user_metadata: request.user_metadata,
            checksum_algorithm: None,
            crc32: object_integrity.crc32.clone(),
            crc32c: object_integrity.crc32c.clone(),
            crc64nvme: object_integrity.crc64nvme.clone(),
            sha1: object_integrity.sha1.clone(),
            sha256: object_integrity.sha256.clone(),
        };

        let mut objects = self.objects.write().await;
        objects.insert(request.key.clone(), (content, metadata));

        Ok(StoredObjectMetadata { object_integrity })
    }

    async fn get_object(
        &self,
        request: crate::storage::GetObjectRequest<'_>,
    ) -> Result<Option<crate::storage::GetObjectResponse>> {
        let objects = self.objects.read().await;
        let (data, metadata) = match objects.get(request.key) {
            Some((data, metadata)) => (data, metadata),
            None => return Ok(None),
        };

        let is_range_request = request.range.is_some();
        let data = if let Some(range) = request.range {
            apply_range(data, range)
        } else {
            data.clone()
        };

        let stream = VecByteStream::new(data);
        let boxed_stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
        > = Box::new(stream);

        // Clear checksums for range requests since they apply to full object
        let mut response_metadata = metadata.clone();
        if is_range_request {
            response_metadata.clear_checksums();
        }

        Ok(Some(crate::storage::GetObjectResponse {
            stream: boxed_stream,
            metadata: response_metadata,
        }))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        if objects.remove(key).is_none() {
            return Err(Error::NoSuchKey);
        }
        Ok(())
    }

    async fn list_objects(
        &self,
        request: crate::storage::ListObjectsRequest<'_>,
    ) -> Result<crate::storage::ListObjectsResponse> {
        let objects = self.objects.read().await;
        let mut matching_objects = Vec::new();

        for (key, (_, metadata)) in objects.iter() {
            if let Some(prefix) = request.prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }
            matching_objects.push(crate::storage::ObjectInfo {
                key: key.clone(),
                metadata: metadata.clone(),
            });
        }

        // Sort by key for consistent ordering
        matching_objects.sort_by(|a, b| a.key.cmp(&b.key));

        Ok(crate::storage::ListObjectsResponse {
            objects: matching_objects,
        })
    }

    async fn create_multipart_upload(
        &self,
        request: crate::storage::CreateMultipartUploadRequest<'_>,
    ) -> Result<()> {
        let mut uploads = self.multipart_uploads.write().await;
        let upload_metadata = MultipartUploadMetadata {
            key: request.key.to_string(),
            upload_id: request.upload_id.to_string(),
            metadata: request.metadata,
            parts: HashMap::new(),
            checksum_type: Some(request.checksum_type),
        };
        uploads.insert(request.upload_id.to_string(), upload_metadata);

        // Initialize parts storage for this upload
        let mut parts = self.parts.write().await;
        parts.insert(request.upload_id.to_string(), HashMap::new());

        Ok(())
    }

    async fn upload_part(
        &self,
        request: crate::storage::UploadPartRequest<'_>,
    ) -> Result<crate::storage::UploadPartResponse> {
        // Get upload metadata to determine checksum algorithm
        let checksum_algorithm = {
            let uploads = self.multipart_uploads.read().await;
            let upload = uploads.get(request.upload_id).ok_or(Error::NoSuchUpload)?;
            upload.metadata.checksum_algorithm
        };

        // Calculate ETag (MD5 hash)
        let etag = format!("\"{:x}\"", md5::compute(&request.content));

        // Calculate part checksums if algorithm is specified
        let mut part_metadata = PartMetadata {
            etag: etag.clone(),
            size: request.content.len() as u64,
            ..Default::default()
        };

        if let Some(algorithm) = checksum_algorithm {
            use crate::types::ObjectIntegrityChecks;

            // Calculate checksum for the specified algorithm
            let mut integrity_checks =
                ObjectIntegrityChecks::new().with_checksum_algorithm(algorithm);
            integrity_checks.update(&request.content);
            let calculated_integrity = integrity_checks.finalize();

            // Store the calculated checksum in part metadata
            match algorithm {
                aws_smithy_checksums::ChecksumAlgorithm::Crc32 => {
                    part_metadata.crc32 = calculated_integrity.crc32;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Crc32c => {
                    part_metadata.crc32c = calculated_integrity.crc32c;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Crc64Nvme => {
                    part_metadata.crc64nvme = calculated_integrity.crc64nvme;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Sha1 => {
                    part_metadata.sha1 = calculated_integrity.sha1;
                }
                aws_smithy_checksums::ChecksumAlgorithm::Sha256 => {
                    part_metadata.sha256 = calculated_integrity.sha256;
                }
                _ => {} // Ignore unsupported algorithms
            }
        }

        // Store the part data
        let mut parts = self.parts.write().await;
        let upload_parts = parts
            .get_mut(request.upload_id)
            .ok_or(Error::NoSuchUpload)?;

        upload_parts.insert(
            request.part_number,
            (request.content.clone(), part_metadata.clone()),
        );

        // Update the upload metadata with part info
        {
            let mut uploads = self.multipart_uploads.write().await;
            if let Some(upload) = uploads.get_mut(request.upload_id) {
                upload.parts.insert(request.part_number, part_metadata);
            }
        }

        Ok(crate::storage::UploadPartResponse { etag })
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<crate::storage::PartInfo>> {
        let uploads = self.multipart_uploads.read().await;
        let upload = uploads.get(upload_id).ok_or(Error::NoSuchUpload)?;

        let mut result = Vec::new();
        for part_number in upload.parts.keys() {
            result.push(crate::storage::PartInfo {
                part_number: *part_number,
            });
        }

        // Sort by part number for consistent ordering
        result.sort_by_key(|part| part.part_number);
        Ok(result)
    }

    async fn complete_multipart_upload(
        &self,
        request: crate::storage::CompleteMultipartUploadRequest<'_>,
    ) -> Result<crate::storage::CompleteMultipartUploadResponse> {
        // Get the upload metadata
        let (key, mut final_metadata, checksum_algorithm, checksum_type) = {
            let mut uploads = self.multipart_uploads.write().await;
            let upload = uploads
                .remove(request.upload_id)
                .ok_or(Error::NoSuchUpload)?;
            (
                upload.key,
                upload.metadata.clone(),
                upload.metadata.checksum_algorithm,
                upload.checksum_type,
            )
        };

        // Get the parts data
        let upload_parts = {
            let mut parts_storage = self.parts.write().await;
            parts_storage
                .remove(request.upload_id)
                .ok_or(Error::NoSuchUpload)?
        };

        // Verify all parts exist and ETags match
        for (part_number, expected_etag) in &request.parts {
            match upload_parts.get(part_number) {
                Some((_, part_metadata)) => {
                    if part_metadata.etag != *expected_etag {
                        return Err(Error::InvalidPart);
                    }
                }
                None => return Err(Error::NoSuchPart),
            }
        }

        // Combine all parts in the specified order
        let mut combined = BytesMut::new();
        let mut etags = Vec::new();
        let mut total_size = 0u64;

        for (part_number, _) in &request.parts {
            if let Some((part_data, part_metadata)) = upload_parts.get(part_number) {
                combined.extend_from_slice(part_data);
                etags.push(part_metadata.etag.clone());
                total_size += part_metadata.size;
            }
        }

        // Calculate the final ETag for multipart upload
        let combined_etag = if etags.len() > 1 {
            let etags_concat = etags.join("");
            format!("\"{:x}-{}\"", md5::compute(etags_concat), etags.len())
        } else if !etags.is_empty() {
            etags[0].clone()
        } else {
            format!("\"{:x}\"", md5::compute(""))
        };

        // Calculate checksums if algorithm is specified
        let mut integrity_checks = checksum_algorithm
            .as_ref()
            .map(|algorithm| {
                crate::types::ObjectIntegrityChecks::new().with_checksum_algorithm(*algorithm)
            })
            .unwrap_or_else(crate::types::ObjectIntegrityChecks::new);

        match checksum_type {
            Some(aws_sdk_s3::types::ChecksumType::FullObject) => {
                // Update with complete object data
                integrity_checks.update(&combined);
            }
            Some(aws_sdk_s3::types::ChecksumType::Composite) => {
                // Update with part checksum bytes for composite calculation
                if let Some(algorithm) = checksum_algorithm.as_ref() {
                    for (part_number, _) in &request.parts {
                        if let Some((_, part_metadata)) = upload_parts.get(part_number) {
                            let part_checksum = match algorithm {
                                aws_smithy_checksums::ChecksumAlgorithm::Crc32 => {
                                    &part_metadata.crc32
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Crc32c => {
                                    &part_metadata.crc32c
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Sha1 => {
                                    &part_metadata.sha1
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Sha256 => {
                                    &part_metadata.sha256
                                }
                                _ => &None,
                            };

                            if let Some(checksum) = part_checksum {
                                integrity_checks.update(checksum.as_bytes());
                            }
                        }
                    }
                }
            }
            None => {} // No checksum calculation
            Some(_) => {
                // Future checksum types - default to composite behavior
                if let Some(algorithm) = checksum_algorithm.as_ref() {
                    for (part_number, _) in &request.parts {
                        if let Some((_, part_metadata)) = upload_parts.get(part_number) {
                            let part_checksum = match algorithm {
                                aws_smithy_checksums::ChecksumAlgorithm::Crc32 => {
                                    &part_metadata.crc32
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Crc32c => {
                                    &part_metadata.crc32c
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Sha1 => {
                                    &part_metadata.sha1
                                }
                                aws_smithy_checksums::ChecksumAlgorithm::Sha256 => {
                                    &part_metadata.sha256
                                }
                                _ => &None,
                            };

                            if let Some(checksum) = part_checksum {
                                integrity_checks.update(checksum.as_bytes());
                            }
                        }
                    }
                }
            }
        }

        let object_integrity = integrity_checks.finalize();

        // Validate against client-provided checksum if present
        if let Some(client_checksums) = request.client_checksums {
            if let Err(error_msg) = object_integrity.validate(client_checksums) {
                return Err(Error::ChecksumMismatch(error_msg.to_string()));
            }
        }

        // Update the final metadata
        final_metadata.content_length = total_size;
        final_metadata.etag = combined_etag.clone();
        final_metadata.last_modified = SystemTime::now();

        // Store calculated checksums in metadata
        final_metadata.crc32 = object_integrity.crc32.clone();
        final_metadata.crc32c = object_integrity.crc32c.clone();
        final_metadata.sha1 = object_integrity.sha1.clone();
        final_metadata.sha256 = object_integrity.sha256.clone();
        final_metadata.crc64nvme = object_integrity.crc64nvme.clone();

        // Store the final object
        let combined_data = combined.freeze();
        let mut objects = self.objects.write().await;
        objects.insert(key.clone(), (combined_data, final_metadata.clone()));

        Ok(crate::storage::CompleteMultipartUploadResponse {
            key: key.clone(),
            etag: combined_etag,
            object_integrity,
        })
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        // Remove the upload metadata
        {
            let mut uploads = self.multipart_uploads.write().await;
            if uploads.remove(upload_id).is_none() {
                return Err(Error::NoSuchUpload);
            }
        }

        // Remove all parts for this upload
        {
            let mut parts = self.parts.write().await;
            parts.remove(upload_id);
        }

        Ok(())
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let objects = self.objects.read().await;
        Ok(objects.get(key).map(|(_, metadata)| metadata.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ObjectIntegrityChecks;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::pin::Pin;

    // Helper function to collect stream data into bytes
    async fn collect_stream_data(
        mut stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
        >,
    ) -> Bytes {
        let mut collected_data = Vec::new();
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.unwrap();
            collected_data.extend_from_slice(&chunk);
        }
        Bytes::from(collected_data)
    }

    fn create_test_metadata(content_length: u64) -> ObjectMetadata {
        ObjectMetadata {
            content_type: Some("text/plain".to_string()),
            content_length,
            etag: format!("\"{:x}\"", md5::compute("test")),
            last_modified: SystemTime::now(),
            user_metadata: HashMap::new(),
            ..Default::default()
        }
    }

    // Helper function to convert Bytes to a stream for testing
    fn bytes_to_stream(
        data: Bytes,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>> {
        Box::pin(futures::stream::once(async move { Ok(data) }))
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content.clone());
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get object
        let request = crate::storage::GetObjectRequest { key, range: None };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_stream = response.stream;
        let retrieved_metadata = response.metadata;
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, content);
        assert_eq!(retrieved_metadata.content_length, content.len() as u64);
        // Content type is not preserved in the new streaming API
        assert_eq!(retrieved_metadata.content_type, None);
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("0123456789");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Get range
        let range = Some(2..5);
        let request = crate::storage::GetObjectRequest { key, range };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        let retrieved_stream = response.stream;
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, Bytes::from("234"));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");
        let integrity_checks = ObjectIntegrityChecks::new().with_md5();

        // Put object
        let stream = bytes_to_stream(content);
        storage
            .put_object(crate::storage::StoreObjectRequest::new(
                key,
                stream,
                integrity_checks,
            ))
            .await
            .unwrap();

        // Verify it exists
        let request = crate::storage::GetObjectRequest { key, range: None };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());

        // Delete object
        storage.delete_object(key).await.unwrap();

        // Verify it's gone
        let request = crate::storage::GetObjectRequest { key, range: None };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let storage = InMemoryStorage::new();
        let content = Bytes::from("test content");

        // Put multiple objects
        for i in 0..3 {
            let key = format!("test-key-{}", i);
            let integrity_checks = ObjectIntegrityChecks::new().with_md5();
            let stream = bytes_to_stream(content.clone());
            storage
                .put_object(crate::storage::StoreObjectRequest::new(
                    &key,
                    stream,
                    integrity_checks,
                ))
                .await
                .unwrap();
        }

        // List all objects
        let request = crate::storage::ListObjectsRequest { prefix: None };
        let objects = storage.list_objects(request).await.unwrap();
        assert_eq!(objects.objects.len(), 3);

        // List with prefix
        let request = crate::storage::ListObjectsRequest {
            prefix: Some("test-key-1"),
        };
        let objects = storage.list_objects(request).await.unwrap();
        assert_eq!(objects.objects.len(), 1);
        assert_eq!(objects.objects[0].key, "test-key-1");
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0); // Will be updated on completion

        // Create multipart upload
        let request = crate::storage::CreateMultipartUploadRequest {
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        // Upload parts
        let part1 = Bytes::from("part1");
        let part2 = Bytes::from("part2");

        let request1 = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1.clone(),
        };
        let etag1 = storage.upload_part(request1).await.unwrap();
        let request2 = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 2,
            content: part2.clone(),
        };
        let etag2 = storage.upload_part(request2).await.unwrap();

        // List parts
        let parts = storage.list_parts(upload_id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, 1); // part number

        // Complete multipart upload
        let parts_to_complete = vec![(1, etag1.etag), (2, etag2.etag)];
        let request = crate::storage::CompleteMultipartUploadRequest {
            upload_id,
            parts: parts_to_complete,
            client_checksums: None,
        };
        let response = storage.complete_multipart_upload(request).await.unwrap();

        assert_eq!(response.key, key);

        // Verify the final object exists and has correct content length
        let request = crate::storage::GetObjectRequest { key, range: None };
        let result = storage.get_object(request).await.unwrap();
        assert!(result.is_some());
        let response = result.unwrap();
        assert_eq!(
            response.metadata.content_length,
            (part1.len() + part2.len()) as u64
        );
        let final_stream = response.stream;
        let final_content = collect_stream_data(final_stream).await;
        assert_eq!(final_content, Bytes::from("part1part2"));
    }

    #[tokio::test]
    async fn test_multipart_upload_missing_part() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        // Create multipart upload
        let request = crate::storage::CreateMultipartUploadRequest {
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        // Upload only one part
        let part1 = Bytes::from("part1");
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1,
        };
        let etag1 = storage.upload_part(request).await.unwrap();

        // Try to complete with a missing part
        let parts_to_complete = vec![(1, etag1.etag), (2, "missing-etag".to_string())];
        let request = crate::storage::CompleteMultipartUploadRequest {
            upload_id,
            parts: parts_to_complete,
            client_checksums: None,
        };
        let result = storage.complete_multipart_upload(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        // Create multipart upload
        let request = crate::storage::CreateMultipartUploadRequest {
            key,
            upload_id,
            metadata,
            checksum_type: aws_sdk_s3::types::ChecksumType::Composite,
        };
        storage.create_multipart_upload(request).await.unwrap();

        // Upload a part
        let part1 = Bytes::from("part1");
        let request = crate::storage::UploadPartRequest {
            upload_id,
            part_number: 1,
            content: part1,
        };
        storage.upload_part(request).await.unwrap();

        // Abort the upload
        storage.abort_multipart_upload(upload_id).await.unwrap();

        // Verify we can't list parts anymore
        let result = storage.list_parts(upload_id).await;
        assert!(result.is_err());
    }
}
