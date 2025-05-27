/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! s3s integration layer.

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::stream;
use futures_util::StreamExt;
use s3s::dto::StreamingBlob;
use s3s::dto::Timestamp;
use s3s::{S3Request, S3Response, S3Result};

use crate::error::Error;
use crate::storage::models::ObjectMetadata;
use crate::storage::StorageBackend;

/// Inner implementation of the s3s::S3 trait.
///
/// This struct implements the s3s::S3 trait, delegating to the storage backend
/// for data operations and managing metadata.
pub(crate) struct Inner<S: StorageBackend + 'static> {
    /// The storage backend.
    storage: S,

    /// Object metadata, keyed by object key.
    metadata: tokio::sync::RwLock<HashMap<String, ObjectMetadata>>,

    /// Multipart uploads in progress, keyed by upload ID.
    /// The value is a tuple of (object key, metadata).
    multipart_uploads: tokio::sync::RwLock<HashMap<String, (String, ObjectMetadata)>>,

    /// Parts for multipart uploads, keyed by upload ID.
    /// The value is a map of part number to (ETag, data).
    parts: tokio::sync::RwLock<HashMap<String, HashMap<i32, (String, Bytes)>>>,
}

impl<S: StorageBackend + 'static> Inner<S> {
    /// Create a new Inner with the given storage backend.
    pub(crate) fn new(storage: S) -> Self {
        Self {
            storage,
            metadata: tokio::sync::RwLock::new(HashMap::new()),
            multipart_uploads: tokio::sync::RwLock::new(HashMap::new()),
            parts: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl<S: StorageBackend + 'static> s3s::S3 for Inner<S> {
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

        let range = input
            .range
            .as_ref()
            .map(|range_dto| {
                range_dto
                    .check(metadata.content_length)
                    .map_err(|_| Error::InvalidRange)
            })
            .transpose()?;

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
}

#[cfg(test)]
mod tests {
    // Tests will be added as needed
}
