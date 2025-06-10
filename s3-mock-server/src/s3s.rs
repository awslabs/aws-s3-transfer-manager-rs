/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use async_trait::async_trait;
use base64::Engine;
use bytes::BytesMut;
use futures_util::stream;
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::{S3Request, S3Response, S3Result, S3};
use uuid;

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

        // Get object data with validated range
        let (data, _metadata) = match self.storage.get_object(key, range.clone()).await? {
            Some((data, metadata)) => (data, metadata),
            None => return Err(Error::NoSuchKey.into()),
        };

        let mut output = s3s::dto::GetObjectOutput::default();

        // Set content length and range headers
        if let Some(range) = range {
            // For range requests, content_length is the size of the range
            output.content_length = Some(data.len() as i64);
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

        // Create a streaming blob from the bytes
        let stream = stream::once(async move { Ok::<_, std::io::Error>(data) });
        output.body = Some(StreamingBlob::wrap(stream));

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
        let key = &input.key;

        // Get object metadata from storage
        let metadata = match self.storage.head_object(key).await? {
            Some(metadata) => metadata,
            None => return Err(Error::NoSuchKey.into()),
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
        // FIXME - add checksum support

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(level = "debug")]
    async fn put_object(
        &self,
        req: S3Request<s3s::dto::PutObjectInput>,
    ) -> S3Result<S3Response<s3s::dto::PutObjectOutput>> {
        let input = req.input;
        let key = &input.key;

        // Read the body content
        let mut content = BytesMut::new();
        if let Some(mut body) = input.body {
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => {
                        content.extend_from_slice(&chunk);
                    }
                    Err(_) => return Err(s3s::s3_error!(InternalError, "Failed to read body")),
                }
            }
        }
        let content = content.freeze();

        let content_type = input.content_type.map(|mime| mime.to_string());
        let user_metadata = input.metadata.unwrap_or_default();

        // Calculate ETag (MD5 hash)
        let etag = format!("\"{:x}\"", md5::compute(&content));

        let metadata = ObjectMetadata {
            content_type,
            content_length: content.len() as u64,
            etag: etag.clone(),
            last_modified: std::time::SystemTime::now(),
            user_metadata,
        };

        // Store object in storage backend
        self.storage.put_object(key, content, metadata).await?;

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
        let key = &input.key;

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

        // Create the multipart upload in storage
        self.storage
            .create_multipart_upload(key, &upload_id, metadata)
            .await?;

        // Build response
        let mut output = s3s::dto::CreateMultipartUploadOutput::default();
        output.upload_id = Some(upload_id);
        output.key = Some(key.to_string());
        output.bucket = Some("mock-bucket".to_string());

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

        // Read the part content
        let mut content = BytesMut::new();
        if let Some(mut body) = input.body {
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => {
                        content.extend_from_slice(&chunk);
                    }
                    Err(_) => return Err(s3s::s3_error!(InternalError, "Failed to read body")),
                }
            }
        }
        let content = content.freeze();

        // Store the part and get its ETag
        let etag = self
            .storage
            .upload_part(upload_id, part_number, content)
            .await?;

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
        let (key, metadata) = self
            .storage
            .complete_multipart_upload(upload_id, parts)
            .await?;

        // Build response
        let mut output = s3s::dto::CompleteMultipartUploadOutput::default();
        output.key = Some(key);
        output.e_tag = Some(metadata.etag);
        output.bucket = Some("mock-bucket".to_string());

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
        let mut objects = self.storage.list_objects(prefix).await?;

        // Apply start_after if specified
        if let Some(start_after) = start_after {
            objects.retain(|(key, _)| key.as_str() > start_after);
        }

        // Sort by key for consistent ordering
        objects.sort_by(|a, b| a.0.cmp(&b.0));

        // Process delimiter if specified
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();

        if let Some(delimiter) = delimiter {
            let mut seen_prefixes = std::collections::HashSet::new();

            for (key, metadata) in objects {
                if let Some(rest) = key.strip_prefix(prefix.unwrap_or("")) {
                    if let Some(index) = rest.find(delimiter) {
                        // This is a common prefix
                        let common_prefix = format!("{}{}", prefix.unwrap_or(""), &rest[..=index]);
                        if seen_prefixes.insert(common_prefix.clone()) {
                            let mut prefix_obj = s3s::dto::CommonPrefix::default();
                            prefix_obj.prefix = Some(common_prefix);
                            common_prefixes.push(prefix_obj);
                        }
                        continue;
                    }
                }

                // This is a regular object
                let mut object = s3s::dto::Object::default();
                object.key = Some(key);
                object.size = Some(metadata.content_length as i64);
                object.e_tag = Some(metadata.etag);
                object.last_modified = Some(Timestamp::from(metadata.last_modified));

                contents.push(object);
            }
        } else {
            // No delimiter - just convert all objects
            for (key, metadata) in objects {
                let mut object = s3s::dto::Object::default();
                object.key = Some(key);
                object.size = Some(metadata.content_length as i64);
                object.e_tag = Some(metadata.etag);
                object.last_modified = Some(Timestamp::from(metadata.last_modified));

                contents.push(object);
            }
        }

        // Handle pagination
        let total_objects = contents.len() + common_prefixes.len();
        let is_truncated = total_objects > max_keys;

        let mut output = s3s::dto::ListObjectsV2Output::default();
        output.contents = Some(contents);
        output.common_prefixes = Some(common_prefixes);
        output.is_truncated = Some(is_truncated);
        output.key_count = Some(total_objects as i32);
        output.max_keys = Some(max_keys as i32);
        output.prefix = prefix.map(|s| s.to_string());
        output.delimiter = delimiter.map(|s| s.to_string());
        output.name = Some("mock-bucket".to_string());

        // Set continuation token if truncated
        if is_truncated {
            if let Some(last_key) = output
                .contents
                .as_ref()
                .and_then(|c| c.last())
                .and_then(|obj| obj.key.as_ref())
            {
                let token = base64::engine::general_purpose::STANDARD.encode(last_key);
                output.next_continuation_token = Some(token);
            }
        }

        Ok(S3Response::new(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;
    use std::collections::HashMap;

    fn create_test_metadata(size: u64) -> ObjectMetadata {
        ObjectMetadata {
            content_type: Some("text/plain".to_string()),
            content_length: size,
            etag: format!(
                "\"{}\"",
                hex::encode(md5::compute(format!("test-content-{}", size)).0)
            ),
            last_modified: std::time::SystemTime::now(),
            user_metadata: HashMap::new(),
        }
    }

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
}
