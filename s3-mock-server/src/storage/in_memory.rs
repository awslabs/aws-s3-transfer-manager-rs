/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! In-memory implementation of the StorageBackend trait.

use std::collections::HashMap;
use std::ops::Range;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::storage::models::{MultipartUploadMetadata, ObjectMetadata, PartMetadata};
use crate::storage::StorageBackend;
use crate::streaming::{apply_range, VecByteStream};

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
    parts: RwLock<HashMap<String, HashMap<i32, (Bytes, PartMetadata)>>>,
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
    async fn put_object(&self, key: &str, content: Bytes, metadata: ObjectMetadata) -> Result<()> {
        let mut objects = self.objects.write().await;
        objects.insert(key.to_string(), (content, metadata));
        Ok(())
    }

    async fn get_object(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<
        Option<(
            Box<
                dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
            >,
            ObjectMetadata,
        )>,
    > {
        let objects = self.objects.read().await;
        let (data, metadata) = match objects.get(key) {
            Some((data, metadata)) => (data, metadata),
            None => return Ok(None),
        };

        let data = if let Some(range) = range {
            apply_range(data, range)
        } else {
            data.clone()
        };

        let stream = VecByteStream::new(data);
        let boxed_stream: Box<
            dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin,
        > = Box::new(stream);

        Ok(Some((boxed_stream, metadata.clone())))
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        if objects.remove(key).is_none() {
            return Err(Error::NoSuchKey);
        }
        Ok(())
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<(String, ObjectMetadata)>> {
        let objects = self.objects.read().await;
        let mut result = Vec::new();

        for (key, (_, metadata)) in objects.iter() {
            if let Some(prefix) = prefix {
                if !key.starts_with(prefix) {
                    continue;
                }
            }
            result.push((key.clone(), metadata.clone()));
        }

        // Sort by key for consistent ordering
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }

    async fn create_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        metadata: ObjectMetadata,
    ) -> Result<()> {
        let mut uploads = self.multipart_uploads.write().await;
        let upload_metadata = MultipartUploadMetadata {
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            metadata,
            parts: HashMap::new(),
        };
        uploads.insert(upload_id.to_string(), upload_metadata);

        // Initialize parts storage for this upload
        let mut parts = self.parts.write().await;
        parts.insert(upload_id.to_string(), HashMap::new());

        Ok(())
    }

    async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<String> {
        // Verify the upload exists
        {
            let uploads = self.multipart_uploads.read().await;
            if !uploads.contains_key(upload_id) {
                return Err(Error::NoSuchUpload);
            }
        }

        // Calculate ETag (MD5 hash)
        let etag = format!("\"{:x}\"", md5::compute(&content));

        // Store the part data
        let mut parts = self.parts.write().await;
        let upload_parts = parts.get_mut(upload_id).ok_or(Error::NoSuchUpload)?;

        let part_metadata = PartMetadata {
            etag: etag.clone(),
            size: content.len() as u64,
        };

        upload_parts.insert(part_number, (content, part_metadata.clone()));

        // Update the upload metadata with part info
        {
            let mut uploads = self.multipart_uploads.write().await;
            if let Some(upload) = uploads.get_mut(upload_id) {
                upload.parts.insert(part_number, part_metadata);
            }
        }

        Ok(etag)
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(i32, String, u64)>> {
        let uploads = self.multipart_uploads.read().await;
        let upload = uploads.get(upload_id).ok_or(Error::NoSuchUpload)?;

        let mut result = Vec::new();
        for (part_number, part_metadata) in &upload.parts {
            result.push((*part_number, part_metadata.etag.clone(), part_metadata.size));
        }

        // Sort by part number for consistent ordering
        result.sort_by_key(|&(part_number, _, _)| part_number);
        Ok(result)
    }

    async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(String, ObjectMetadata)> {
        // Get the upload metadata
        let (key, mut final_metadata) = {
            let mut uploads = self.multipart_uploads.write().await;
            let upload = uploads.remove(upload_id).ok_or(Error::NoSuchUpload)?;
            (upload.key, upload.metadata)
        };

        // Get the parts data
        let upload_parts = {
            let mut parts_storage = self.parts.write().await;
            parts_storage.remove(upload_id).ok_or(Error::NoSuchUpload)?
        };

        // Verify all parts exist and ETags match
        for (part_number, expected_etag) in &parts {
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

        for (part_number, _) in &parts {
            if let Some((part_data, part_metadata)) = upload_parts.get(part_number) {
                combined.extend_from_slice(part_data);
                etags.push(part_metadata.etag.clone());
                total_size += part_metadata.size;
            }
        }

        // Calculate the final ETag for multipart upload
        // For multipart uploads, S3 uses a special format: "{md5-of-etags}-{part-count}"
        let combined_etag = if etags.len() > 1 {
            let etags_concat = etags.join("");
            format!("\"{:x}-{}\"", md5::compute(etags_concat), etags.len())
        } else if !etags.is_empty() {
            // For single part uploads, just use the ETag of the part
            etags[0].clone()
        } else {
            format!("\"{:x}\"", md5::compute(""))
        };

        // Update the final metadata
        final_metadata.content_length = total_size;
        final_metadata.etag = combined_etag;
        final_metadata.last_modified = SystemTime::now();

        // Store the final object
        let combined_data = combined.freeze();
        let mut objects = self.objects.write().await;
        objects.insert(key.clone(), (combined_data, final_metadata.clone()));

        Ok((key, final_metadata))
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
    use futures::StreamExt;
    use std::collections::HashMap;

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
        }
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");
        let metadata = create_test_metadata(content.len() as u64);

        // Put object
        storage
            .put_object(key, content.clone(), metadata.clone())
            .await
            .unwrap();

        // Get object
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, retrieved_metadata) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, content);
        assert_eq!(retrieved_metadata.content_length, metadata.content_length);
        assert_eq!(retrieved_metadata.content_type, metadata.content_type);
    }

    #[tokio::test]
    async fn test_get_object_with_range() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("0123456789");
        let metadata = create_test_metadata(content.len() as u64);

        // Put object
        storage
            .put_object(key, content.clone(), metadata)
            .await
            .unwrap();

        // Get range
        let range = Some(2..5);
        let result = storage.get_object(key, range).await.unwrap();
        assert!(result.is_some());
        let (retrieved_stream, _) = result.unwrap();
        let retrieved_content = collect_stream_data(retrieved_stream).await;
        assert_eq!(retrieved_content, Bytes::from("234"));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let storage = InMemoryStorage::new();
        let key = "test-key";
        let content = Bytes::from("test content");
        let metadata = create_test_metadata(content.len() as u64);

        // Put object
        storage.put_object(key, content, metadata).await.unwrap();

        // Verify it exists
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());

        // Delete object
        storage.delete_object(key).await.unwrap();

        // Verify it's gone
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_objects() {
        let storage = InMemoryStorage::new();
        let content = Bytes::from("test content");

        // Put multiple objects
        for i in 0..3 {
            let key = format!("test-key-{}", i);
            let metadata = create_test_metadata(content.len() as u64);
            storage
                .put_object(&key, content.clone(), metadata)
                .await
                .unwrap();
        }

        // List all objects
        let objects = storage.list_objects(None).await.unwrap();
        assert_eq!(objects.len(), 3);

        // List with prefix
        let objects = storage.list_objects(Some("test-key-1")).await.unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].0, "test-key-1");
    }

    #[tokio::test]
    async fn test_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0); // Will be updated on completion

        // Create multipart upload
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload parts
        let part1 = Bytes::from("part1");
        let part2 = Bytes::from("part2");

        let etag1 = storage
            .upload_part(upload_id, 1, part1.clone())
            .await
            .unwrap();
        let etag2 = storage
            .upload_part(upload_id, 2, part2.clone())
            .await
            .unwrap();

        // List parts
        let parts = storage.list_parts(upload_id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].0, 1); // part number
        assert_eq!(parts[0].1, etag1); // etag
        assert_eq!(parts[0].2, part1.len() as u64); // size

        // Complete multipart upload
        let parts_to_complete = vec![(1, etag1), (2, etag2)];
        let (final_key, final_metadata) = storage
            .complete_multipart_upload(upload_id, parts_to_complete)
            .await
            .unwrap();

        assert_eq!(final_key, key);
        assert_eq!(
            final_metadata.content_length,
            (part1.len() + part2.len()) as u64
        );

        // Verify the final object exists
        let result = storage.get_object(key, None).await.unwrap();
        assert!(result.is_some());
        let (final_stream, _) = result.unwrap();
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
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload only one part
        let part1 = Bytes::from("part1");
        let etag1 = storage.upload_part(upload_id, 1, part1).await.unwrap();

        // Try to complete with a missing part
        let parts_to_complete = vec![(1, etag1), (2, "missing-etag".to_string())];
        let result = storage
            .complete_multipart_upload(upload_id, parts_to_complete)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_abort_multipart_upload() {
        let storage = InMemoryStorage::new();
        let upload_id = "test-upload-123";
        let key = "test-multipart-key";
        let metadata = create_test_metadata(0);

        // Create multipart upload
        storage
            .create_multipart_upload(key, upload_id, metadata)
            .await
            .unwrap();

        // Upload a part
        let part1 = Bytes::from("part1");
        storage.upload_part(upload_id, 1, part1).await.unwrap();

        // Abort the upload
        storage.abort_multipart_upload(upload_id).await.unwrap();

        // Verify we can't list parts anymore
        let result = storage.list_parts(upload_id).await;
        assert!(result.is_err());
    }
}
