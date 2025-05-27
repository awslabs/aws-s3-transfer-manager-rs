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

        let metadata = {
            let metadata_map = self.metadata.read().await;
            match metadata_map.get(&key) {
                Some(metadata) => metadata.clone(),
                None => return Err(Error::NoSuchKey.into()),
            }
        };

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

        // TODO - checksum support

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
        output.bucket = Some(input.bucket);

        Ok(S3Response::new(output))
    }
}
