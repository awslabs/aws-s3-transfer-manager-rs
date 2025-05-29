/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use async_trait::async_trait;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use futures_util::stream;
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::{S3Request, S3Response, S3Result};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use crate::error::Error;
use crate::storage::models::ObjectMetadata;
use crate::storage::StorageBackend;

/// Inner implementation of the s3s::S3 trait.
///
/// This struct implements the s3s::S3 trait, delegating to the storage backend
/// for data operations and managing metadata.
#[derive(Debug, Clone)]
pub(crate) struct Inner<S: StorageBackend + 'static> {
    /// The storage backend.
    storage: S,

    /// Object metadata, keyed by object key.
    pub(crate) metadata: Arc<tokio::sync::RwLock<HashMap<String, ObjectMetadata>>>,

    /// Multipart uploads in progress, keyed by upload ID.
    /// The value is a tuple of (object key, metadata).
    multipart_uploads: Arc<tokio::sync::RwLock<HashMap<String, (String, ObjectMetadata)>>>,

    /// Parts for multipart uploads, keyed by upload ID.
    /// The value is a map of part number to (ETag, data).
    parts: Arc<tokio::sync::RwLock<HashMap<String, HashMap<i32, (String, Bytes)>>>>,
}

impl<S: StorageBackend + 'static> Inner<S> {
    /// Create a new Inner with the given storage backend.
    pub(crate) fn new(storage: S) -> Self {
        Self {
            storage,
            metadata: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            multipart_uploads: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            parts: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
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
        let key = input.key;

        // Get metadata first to determine content length
        let metadata = {
            let metadata_map = self.metadata.read().await;
            match metadata_map.get(&key) {
                Some(metadata) => metadata.clone(),
                None => return Err(Error::NoSuchKey.into()),
            }
        };

        // For now, we don't support range requests in the s3s integration
        // This would require more work to properly parse the Range header from the s3s::dto::Range type
        let range = None;

        let data = match self.storage.get_object_data(&key, range).await {
            Ok(data) => data,
            Err(err) => return Err(err.into()),
        };

        let mut output = s3s::dto::GetObjectOutput::default();

        // Create a streaming blob from the bytes
        let stream = stream::once(async move { Ok::<_, std::io::Error>(data) });
        output.body = Some(StreamingBlob::wrap(stream));

        output.content_length = Some(metadata.content_length as i64);
        output.e_tag = Some(metadata.etag);

        let timestamp = Timestamp::from(metadata.last_modified);
        output.last_modified = Some(timestamp);

        if let Some(content_type) = metadata.content_type {
            if let Ok(mime) = content_type.parse() {
                output.content_type = Some(mime);
            }
        }

        output.metadata = Some(metadata.user_metadata);
        // FIXME - add checksum support/storage

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn head_object(
        &self,
        req: S3Request<s3s::dto::HeadObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::HeadObjectOutput>> {
        let input = req.input;
        let key = input.key;

        // Get metadata
        let metadata = {
            let metadata_map = self.metadata.read().await;
            match metadata_map.get(&key) {
                Some(metadata) => metadata.clone(),
                None => return Err(Error::NoSuchKey.into()),
            }
        };

        // Build response
        let mut output = s3s::dto::HeadObjectOutput::default();

        output.content_length = Some(metadata.content_length as i64);
        output.e_tag = Some(metadata.etag);

        let timestamp = Timestamp::from(metadata.last_modified);
        output.last_modified = Some(timestamp);

        if let Some(content_type) = metadata.content_type {
            if let Ok(mime) = content_type.parse() {
                output.content_type = Some(mime);
            }
        }

        output.metadata = Some(metadata.user_metadata);

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn put_object(
        &self,
        req: S3Request<s3s::dto::PutObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let input = req.input;
        let key = input.key;

        let body = match input.body {
            Some(body) => body,
            None => return Err(s3s::s3_error!(InvalidRequest, "Missing request body")),
        };

        let mut content = Bytes::new();
        let mut stream = body;

        // FIXME - optimize this - we're pulling everything into memory
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    // Append the chunk to our content
                    let mut new_content = BytesMut::with_capacity(content.len() + chunk.len());
                    new_content.extend_from_slice(&content);
                    new_content.extend_from_slice(&chunk);
                    content = new_content.freeze();
                }
                Err(_) => return Err(s3s::s3_error!(InternalError, "Failed to read body")),
            }
        }

        let content_type = input.content_type.map(|mime| mime.to_string());
        let user_metadata = input.metadata.unwrap_or_default();

        // Calculate ETag (MD5 hash)
        let etag = format!("\"{:x}\"", md5::compute(&content));

        if let Err(err) = self.storage.put_object_data(&key, content.clone()).await {
            return Err(err.into());
        }

        let metadata = ObjectMetadata {
            content_type,
            content_length: content.len() as u64,
            etag: etag.clone(),
            last_modified: std::time::SystemTime::now(),
            user_metadata,
        };

        {
            let mut metadata_map = self.metadata.write().await;
            metadata_map.insert(key, metadata);
        }

        // Build response
        let mut output = s3s::dto::PutObjectOutput::default();
        output.e_tag = Some(etag);

        // FIXME - add checksum support

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn create_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CreateMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key;

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
        };

        // Store the metadata in a temporary location
        {
            let mut metadata_map = self.multipart_uploads.write().await;
            metadata_map.insert(upload_id.clone(), (key.clone(), metadata));
        }

        // Initialize the parts map for this upload
        {
            let mut parts_map = self.parts.write().await;
            parts_map.insert(upload_id.clone(), HashMap::new());
        }

        // Build response
        let mut output = s3s::dto::CreateMultipartUploadOutput::default();
        output.upload_id = Some(upload_id);
        output.key = Some(key);
        output.bucket = Some("mock-bucket".to_string()); // Mock bucket name

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn upload_part(
        &self,
        req: S3Request<s3s::dto::UploadPartInput>,
    ) -> S3Result<S3Response<s3s::dto::UploadPartOutput>> {
        let input = req.input;
        let key = input.key;
        let upload_id = input.upload_id;
        let part_number = input.part_number;

        if part_number <= 0 {
            return Err(s3s::s3_error!(InvalidPart, "Invalid part number"));
        }

        // Verify the upload exists
        {
            let uploads = self.multipart_uploads.read().await;
            if !uploads.contains_key(&upload_id) {
                return Err(Error::NoSuchUpload.into());
            }

            // Verify the key matches
            if let Some((stored_key, _)) = uploads.get(&upload_id) {
                if *stored_key != key {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "Key does not match upload ID"
                    ));
                }
            }
        }

        // Extract the body
        let body = match input.body {
            Some(body) => body,
            None => return Err(s3s::s3_error!(InvalidRequest, "Missing request body")),
        };

        // Collect the body into bytes
        let mut content = Bytes::new();
        let mut stream = body;

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    // Append the chunk to our content
                    let mut new_content = BytesMut::with_capacity(content.len() + chunk.len());
                    new_content.extend_from_slice(&content);
                    new_content.extend_from_slice(&chunk);
                    content = new_content.freeze();
                }
                Err(_) => return Err(s3s::s3_error!(InternalError, "Failed to read body")),
            }
        }

        // Calculate ETag (MD5 hash)
        let etag = format!("\"{:x}\"", md5::compute(&content));

        // Store the part data
        if let Err(err) = self
            .storage
            .store_part_data(&upload_id, part_number, content.clone())
            .await
        {
            return Err(err.into());
        }

        // Store the part metadata
        {
            let mut parts_map = self.parts.write().await;
            if let Some(parts) = parts_map.get_mut(&upload_id) {
                parts.insert(part_number, (etag.clone(), content));
            } else {
                return Err(Error::NoSuchUpload.into());
            }
        }

        // Build response
        let mut output = s3s::dto::UploadPartOutput::default();
        output.e_tag = Some(etag);

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<s3s::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CompleteMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key;
        let upload_id = input.upload_id;

        // Get the multipart upload metadata
        let (stored_key, mut metadata) = {
            let uploads = self.multipart_uploads.read().await;
            match uploads.get(&upload_id) {
                Some((k, m)) => (k.clone(), m.clone()),
                None => return Err(Error::NoSuchUpload.into()),
            }
        };

        // Verify the key matches
        if stored_key != key {
            return Err(s3s::s3_error!(
                InvalidRequest,
                "Key does not match upload ID"
            ));
        }

        // Get the completed parts
        let completed_parts = match input.multipart_upload {
            Some(upload) => upload.parts.unwrap_or_default(),
            None => {
                return Err(s3s::s3_error!(
                    InvalidRequest,
                    "Missing multipart upload data"
                ))
            }
        };

        // Verify we have all the parts
        let mut part_numbers = Vec::new();
        {
            let parts_map = self.parts.read().await;
            let parts = match parts_map.get(&upload_id) {
                Some(p) => p,
                None => return Err(Error::NoSuchUpload.into()),
            };

            // Verify all parts exist and ETags match
            for part in &completed_parts {
                let part_number = match part.part_number {
                    Some(num) => num,
                    None => return Err(s3s::s3_error!(InvalidRequest, "Missing part number")),
                };

                let etag = match &part.e_tag {
                    Some(tag) => tag,
                    None => return Err(s3s::s3_error!(InvalidRequest, "Missing ETag")),
                };

                match parts.get(&part_number) {
                    Some((stored_etag, _)) => {
                        if stored_etag != etag {
                            return Err(s3s::s3_error!(InvalidPart, "ETag does not match"));
                        }
                    }
                    None => return Err(s3s::s3_error!(InvalidPart, "Part does not exist")),
                }

                part_numbers.push(part_number);
            }

            // Verify part numbers are consecutive starting from 1
            if part_numbers.is_empty() || part_numbers[0] != 1 {
                return Err(s3s::s3_error!(
                    InvalidPartOrder,
                    "Part numbers must start with 1"
                ));
            }

            for i in 1..part_numbers.len() {
                if part_numbers[i] != part_numbers[i - 1] + 1 {
                    return Err(s3s::s3_error!(
                        InvalidPartOrder,
                        "Part numbers must be consecutive"
                    ));
                }
            }
        }

        // Combine the parts
        if let Err(err) = self
            .storage
            .complete_multipart_data(&upload_id, &key, &part_numbers)
            .await
        {
            return Err(err.into());
        }

        // Calculate the total size and combined ETag
        let mut total_size = 0;
        let mut etags = Vec::new();
        {
            let parts_map = self.parts.read().await;
            if let Some(parts) = parts_map.get(&upload_id) {
                for part_number in &part_numbers {
                    if let Some((etag, data)) = parts.get(part_number) {
                        total_size += data.len();
                        etags.push(etag.clone());
                    }
                }
            }
        }

        // Calculate the final ETag (this is a simplified version)
        // In real S3, the ETag for a multipart upload is calculated differently
        let combined_etag = if etags.len() > 1 {
            // For multipart uploads, S3 uses a special format: "{md5-of-etags}-{part-count}"
            let etags_concat = etags.join("");
            format!("\"{:x}-{}\"", md5::compute(etags_concat), etags.len())
        } else if !etags.is_empty() {
            // For single part uploads, just use the ETag of the part
            etags[0].clone()
        } else {
            format!("\"{:x}\"", md5::compute(""))
        };

        // Update the metadata
        metadata.content_length = total_size as u64;
        metadata.etag = combined_etag.clone();
        metadata.last_modified = std::time::SystemTime::now();

        // Store the final metadata
        {
            let mut metadata_map = self.metadata.write().await;
            metadata_map.insert(key.clone(), metadata);
        }

        // Clean up the multipart upload data
        {
            let mut uploads = self.multipart_uploads.write().await;
            uploads.remove(&upload_id);

            let mut parts_map = self.parts.write().await;
            parts_map.remove(&upload_id);
        }

        // Build response
        let mut output = s3s::dto::CompleteMultipartUploadOutput::default();
        output.key = Some(key);
        output.e_tag = Some(combined_etag);
        output.bucket = Some(input.bucket);

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key;
        let upload_id = input.upload_id;

        // Verify the upload exists
        {
            let uploads = self.multipart_uploads.read().await;
            if !uploads.contains_key(&upload_id) {
                return Err(Error::NoSuchUpload.into());
            }

            // Verify the key matches
            if let Some((stored_key, _)) = uploads.get(&upload_id) {
                if *stored_key != key {
                    return Err(s3s::s3_error!(
                        InvalidRequest,
                        "Key does not match upload ID"
                    ));
                }
            }
        }

        // Clean up the multipart upload data
        {
            let mut uploads = self.multipart_uploads.write().await;
            uploads.remove(&upload_id);

            let mut parts_map = self.parts.write().await;
            parts_map.remove(&upload_id);
        }

        // Abort the multipart upload
        if let Err(err) = self.storage.abort_multipart_data(&upload_id).await {
            return Err(err.into());
        }

        // Build response
        let output = s3s::dto::AbortMultipartUploadOutput::default();

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn list_objects_v2(
        &self,
        req: S3Request<s3s::dto::ListObjectsV2Input>,
    ) -> S3Result<S3Response<s3s::dto::ListObjectsV2Output>> {
        let input = req.input;
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter;
        let max_keys = input.max_keys.unwrap_or(1000);
        let start_after = input.start_after;
        let continuation_token = input.continuation_token;

        // Determine the starting point
        let start_key = if let Some(token) = continuation_token {
            // Decode the continuation token to get the starting key
            match base64::engine::general_purpose::STANDARD.decode(&token) {
                Ok(decoded) => {
                    if let Ok(token_str) = String::from_utf8(decoded) {
                        if let Some(key) = token_str.strip_prefix("next-key:") {
                            Some(key.to_string())
                        } else {
                            return Err(s3s::s3_error!(
                                InvalidToken,
                                "Invalid continuation token format"
                            ));
                        }
                    } else {
                        return Err(s3s::s3_error!(
                            InvalidToken,
                            "Invalid continuation token encoding"
                        ));
                    }
                }
                Err(_) => {
                    return Err(s3s::s3_error!(InvalidToken, "Invalid continuation token"));
                }
            }
        } else if let Some(after) = start_after {
            Some(after)
        } else {
            None
        };

        // Get all object keys from metadata
        let metadata_map = self.metadata.read().await;

        // Filter objects based on prefix and other criteria
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();

        // Track prefixes we've already seen
        let mut seen_prefixes = std::collections::HashSet::new();

        // Filter and collect objects
        let mut all_keys: Vec<_> = metadata_map.keys().cloned().collect();
        all_keys.sort();

        let filtered_keys = all_keys.iter().filter(|key| {
            // Apply prefix filter
            if !key.starts_with(&prefix) {
                return false;
            }

            // Apply start_after/continuation_token filter
            if let Some(ref start) = start_key {
                if key.as_str() <= start.as_str() {
                    return false;
                }
            }

            true
        });

        // Process keys
        for key in filtered_keys {
            // If we've reached max_keys, stop processing
            if contents.len() + common_prefixes.len() >= max_keys as usize {
                break;
            }

            // Handle delimiter logic
            if let Some(ref delimiter) = delimiter {
                // Check if there's a delimiter after the prefix
                if let Some(suffix_pos) = key[prefix.len()..].find(delimiter) {
                    let prefix_end = prefix.len() + suffix_pos + delimiter.len();
                    let common_prefix = key[..prefix_end].to_string();

                    // Only add each common prefix once
                    if !seen_prefixes.contains(&common_prefix) {
                        seen_prefixes.insert(common_prefix.clone());
                        let mut common_prefix_obj = s3s::dto::CommonPrefix::default();
                        common_prefix_obj.prefix = Some(common_prefix);
                        common_prefixes.push(common_prefix_obj);
                    }

                    // Skip adding this key to contents since it's represented by a common prefix
                    continue;
                }
            }

            // Add the object to contents
            if let Some(metadata) = metadata_map.get(key) {
                let mut object = s3s::dto::Object::default();
                object.key = Some(key.clone());
                object.size = Some(metadata.content_length as i64);
                object.e_tag = Some(metadata.etag.clone());
                object.last_modified = Some(Timestamp::from(metadata.last_modified));

                contents.push(object);
            }
        }

        // Determine if the results are truncated
        let is_truncated = all_keys.len() > contents.len() + common_prefixes.len()
            && contents.len() + common_prefixes.len() == max_keys as usize;

        // Calculate key count before moving contents and common_prefixes
        let key_count = contents.len() as i32 + common_prefixes.len() as i32;

        // Generate continuation token if truncated
        let next_continuation_token = if is_truncated && !contents.is_empty() {
            // Create an opaque token by base64 encoding the last key
            // This simulates how S3 creates continuation tokens
            if let Some(obj) = contents.last() {
                if let Some(key) = &obj.key {
                    let token_data = format!("next-key:{}", key);
                    Some(base64::engine::general_purpose::STANDARD.encode(token_data))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Build response
        let mut output = s3s::dto::ListObjectsV2Output::default();
        output.contents = Some(contents);
        output.common_prefixes = Some(common_prefixes);
        output.is_truncated = Some(is_truncated);
        output.key_count = Some(key_count);
        output.max_keys = Some(max_keys);
        output.prefix = Some(prefix);
        output.delimiter = delimiter;
        output.next_continuation_token = next_continuation_token;

        // Set the bucket name from the request
        output.name = Some(input.bucket);

        Ok(S3Response::new(output))
    }
}

#[async_trait]
impl StorageBackend for Arc<dyn StorageBackend + '_> {
    async fn get_object_data(&self, key: &str, range: Option<Range<u64>>) -> crate::Result<Bytes> {
        (**self).get_object_data(key, range).await
    }

    async fn put_object_data(&self, key: &str, content: Bytes) -> crate::Result<()> {
        (**self).put_object_data(key, content).await
    }

    async fn delete_object_data(&self, key: &str) -> crate::Result<()> {
        (**self).delete_object_data(key).await
    }

    async fn store_part_data(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> crate::Result<()> {
        (**self)
            .store_part_data(upload_id, part_number, content)
            .await
    }

    async fn get_part_data(&self, upload_id: &str, part_number: i32) -> crate::Result<Bytes> {
        (**self).get_part_data(upload_id, part_number).await
    }

    async fn complete_multipart_data(
        &self,
        upload_id: &str,
        key: &str,
        part_numbers: &[i32],
    ) -> crate::Result<()> {
        (**self)
            .complete_multipart_data(upload_id, key, part_numbers)
            .await
    }

    async fn abort_multipart_data(&self, upload_id: &str) -> crate::Result<()> {
        (**self).abort_multipart_data(upload_id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::s3s::Inner;
    use crate::storage::in_memory::InMemoryStorage;
    use crate::storage::models::ObjectMetadata;
    use s3s::{S3Request, S3};
    use std::collections::HashMap;
    use std::time::SystemTime;

    // Helper function to create a ListObjectsV2Input
    fn create_list_objects_input(bucket: &str) -> s3s::dto::ListObjectsV2Input {
        s3s::dto::ListObjectsV2Input {
            bucket: bucket.to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: None,
            prefix: None,
            request_payer: None,
            start_after: None,
            optional_object_attributes: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_list_objects_v2_basic() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add objects with different keys
        metadata_map.insert("key1".to_string(), create_test_metadata(100));
        metadata_map.insert("key2".to_string(), create_test_metadata(200));
        metadata_map.insert("key3".to_string(), create_test_metadata(300));

        drop(metadata_map);

        // Create a request with no filters
        let input = create_list_objects_input("test-bucket");
        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        assert_eq!(output.name, Some("test-bucket".to_string()));

        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 3, "Expected 3 objects");

        // Verify keys are returned and sorted
        let keys: Vec<String> = contents.iter().filter_map(|obj| obj.key.clone()).collect();

        assert_eq!(keys, vec!["key1", "key2", "key3"]);

        // Verify other fields
        assert_eq!(output.key_count, Some(3));
        assert_eq!(output.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_prefix() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add objects with different prefixes
        metadata_map.insert("prefix1/key1".to_string(), create_test_metadata(100));
        metadata_map.insert("prefix1/key2".to_string(), create_test_metadata(200));
        metadata_map.insert("prefix2/key3".to_string(), create_test_metadata(300));

        drop(metadata_map);

        // Create a request with prefix filter
        let mut input = create_list_objects_input("test-bucket");
        input.prefix = Some("prefix1/".to_string());

        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 2, "Expected 2 objects with prefix1/");

        // Verify keys are returned and sorted
        let keys: Vec<String> = contents.iter().filter_map(|obj| obj.key.clone()).collect();

        assert_eq!(keys, vec!["prefix1/key1", "prefix1/key2"]);

        // Verify prefix is returned
        assert_eq!(output.prefix, Some("prefix1/".to_string()));
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_delimiter() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add objects with nested structure
        metadata_map.insert(
            "folder1/subfolder1/key1".to_string(),
            create_test_metadata(100),
        );
        metadata_map.insert(
            "folder1/subfolder1/key2".to_string(),
            create_test_metadata(200),
        );
        metadata_map.insert(
            "folder1/subfolder2/key3".to_string(),
            create_test_metadata(300),
        );
        metadata_map.insert("folder1/key4".to_string(), create_test_metadata(400));
        metadata_map.insert("folder2/key5".to_string(), create_test_metadata(500));

        drop(metadata_map);

        // Create a request with prefix and delimiter
        let mut input = create_list_objects_input("test-bucket");
        input.prefix = Some("folder1/".to_string());
        input.delimiter = Some("/".to_string());

        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 1, "Expected 1 object at folder1/ level");

        // Verify the object is the one directly under folder1/
        let keys: Vec<String> = contents.iter().filter_map(|obj| obj.key.clone()).collect();

        assert_eq!(keys, vec!["folder1/key4"]);

        // Verify common prefixes
        let common_prefixes = output.common_prefixes.expect("No common prefixes returned");
        assert_eq!(common_prefixes.len(), 2, "Expected 2 common prefixes");

        let prefixes: Vec<String> = common_prefixes
            .iter()
            .filter_map(|cp| cp.prefix.clone())
            .collect();

        assert!(prefixes.contains(&"folder1/subfolder1/".to_string()));
        assert!(prefixes.contains(&"folder1/subfolder2/".to_string()));
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_max_keys() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add multiple objects
        for i in 1..=10 {
            metadata_map.insert(format!("key{}", i), create_test_metadata(i * 100));
        }

        drop(metadata_map);

        // Create a request with max_keys=5
        let mut input = create_list_objects_input("test-bucket");
        input.max_keys = Some(5);

        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 5, "Expected 5 objects (max_keys)");

        // Verify truncation and continuation token
        assert_eq!(output.is_truncated, Some(true));
        assert!(
            output.next_continuation_token.is_some(),
            "Expected continuation token"
        );

        // Verify max_keys is returned
        assert_eq!(output.max_keys, Some(5));
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_continuation_token() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add multiple objects
        for i in 1..=10 {
            metadata_map.insert(format!("key{}", i), create_test_metadata(i * 100));
        }

        drop(metadata_map);

        // First request with max_keys=5
        let mut input1 = create_list_objects_input("test-bucket");
        input1.max_keys = Some(5);

        let req1 = S3Request::new(input1);

        // Call the method
        let result1 = inner
            .list_objects_v2(req1)
            .await
            .expect("First list objects failed");
        let output1 = result1.output;

        // Get the continuation token
        let continuation_token = output1
            .next_continuation_token
            .expect("No continuation token");

        // Second request with continuation token
        let mut input2 = create_list_objects_input("test-bucket");
        input2.max_keys = Some(5);
        input2.continuation_token = Some(continuation_token);

        let req2 = S3Request::new(input2);

        // Call the method
        let result2 = inner
            .list_objects_v2(req2)
            .await
            .expect("Second list objects failed");
        let output2 = result2.output;

        // Verify the results
        let contents1 = output1
            .contents
            .expect("No contents returned in first request");
        let contents2 = output2
            .contents
            .expect("No contents returned in second request");

        assert_eq!(contents1.len(), 5, "Expected 5 objects in first request");
        assert_eq!(contents2.len(), 5, "Expected 5 objects in second request");

        // Verify we got different objects in each request
        let keys1: Vec<String> = contents1.iter().filter_map(|obj| obj.key.clone()).collect();

        let keys2: Vec<String> = contents2.iter().filter_map(|obj| obj.key.clone()).collect();

        // Check no overlap between the two sets of keys
        for key in &keys1 {
            assert!(
                !keys2.contains(key),
                "Key {} should not appear in second request",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_start_after() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add objects with different keys
        metadata_map.insert("key1".to_string(), create_test_metadata(100));
        metadata_map.insert("key2".to_string(), create_test_metadata(200));
        metadata_map.insert("key3".to_string(), create_test_metadata(300));
        metadata_map.insert("key4".to_string(), create_test_metadata(400));
        metadata_map.insert("key5".to_string(), create_test_metadata(500));

        drop(metadata_map);

        // Create a request with start_after=key2
        let mut input = create_list_objects_input("test-bucket");
        input.start_after = Some("key2".to_string());

        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 3, "Expected 3 objects after key2");

        // Verify keys are returned and sorted
        let keys: Vec<String> = contents.iter().filter_map(|obj| obj.key.clone()).collect();

        assert_eq!(keys, vec!["key3", "key4", "key5"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_empty_result() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Add some test objects
        let mut metadata_map = inner.metadata.write().await;

        // Add objects with different keys
        metadata_map.insert("key1".to_string(), create_test_metadata(100));
        metadata_map.insert("key2".to_string(), create_test_metadata(200));

        drop(metadata_map);

        // Create a request with non-matching prefix
        let mut input = create_list_objects_input("test-bucket");
        input.prefix = Some("nonexistent/".to_string());

        let req = S3Request::new(input);

        // Call the method
        let result = inner
            .list_objects_v2(req)
            .await
            .expect("List objects failed");
        let output = result.output;

        // Verify the results
        let contents = output.contents.expect("No contents returned");
        assert_eq!(contents.len(), 0, "Expected 0 objects");

        // Verify other fields
        assert_eq!(output.key_count, Some(0));
        assert_eq!(output.is_truncated, Some(false));
        assert_eq!(output.next_continuation_token, None);
    }

    #[tokio::test]
    async fn test_list_objects_v2_invalid_continuation_token() {
        // Create a test instance
        let storage = InMemoryStorage::new();
        let inner = Inner::new(storage);

        // Create a request with invalid continuation token
        let mut input = create_list_objects_input("test-bucket");
        input.continuation_token = Some("invalid-token".to_string());

        let req = S3Request::new(input);

        // Call the method
        let result = inner.list_objects_v2(req).await;

        // Verify the error
        assert!(result.is_err(), "Expected error for invalid token");
    }

    // Helper function to create test metadata
    fn create_test_metadata(size: u64) -> ObjectMetadata {
        ObjectMetadata {
            content_type: Some("text/plain".to_string()),
            content_length: size,
            etag: format!(
                "\"{}\"",
                hex::encode(md5::compute(format!("test-content-{}", size)).0)
            ),
            last_modified: SystemTime::now(),
            user_metadata: HashMap::new(),
        }
    }
}
