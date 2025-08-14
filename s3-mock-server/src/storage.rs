/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Storage backends for the S3 Mock Server.
//!
//! This module provides the `StorageBackend` trait and its implementations.
//! The trait defines the interface for storing and retrieving both object data
//! and metadata, as well as managing multipart upload operations.

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::pin::Pin;

use crate::storage::models::ObjectMetadata;
use crate::types::{ObjectIntegrityChecks, StoredObjectMetadata};
use crate::Result;

/// Request for storing an object with all necessary metadata and options.
pub struct StoreObjectRequest {
    pub key: String,
    pub body: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>>,
    pub integrity_checks: ObjectIntegrityChecks,
    pub content_type: Option<String>,
    pub user_metadata: HashMap<String, String>,
}

/// Request for retrieving an object.
pub(crate) struct GetObjectRequest<'a> {
    pub key: &'a str,
    pub range: Option<Range<u64>>,
}

/// Response for retrieving an object.
pub(crate) struct GetObjectResponse {
    pub stream:
        Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + Sync + Unpin>,
    pub metadata: ObjectMetadata,
}

/// Request for listing objects.
pub(crate) struct ListObjectsRequest<'a> {
    pub prefix: Option<&'a str>,
    pub max_keys: Option<i32>,
    pub continuation_token: Option<&'a str>,
}

/// Response for listing objects.
pub(crate) struct ListObjectsResponse {
    pub objects: Vec<ObjectInfo>,
    pub next_continuation_token: Option<String>,
    pub is_truncated: bool,
}

/// Information about an object.
pub(crate) struct ObjectInfo {
    pub key: String,
    pub metadata: ObjectMetadata,
}

/// Request for creating a multipart upload.
pub(crate) struct CreateMultipartUploadRequest<'a> {
    pub key: &'a str,
    pub upload_id: &'a str,
    pub metadata: ObjectMetadata,
}

/// Request for uploading a part.
pub(crate) struct UploadPartRequest<'a> {
    pub upload_id: &'a str,
    pub part_number: i32,
    pub content: Bytes,
}

/// Response for uploading a part.
#[derive(Debug)]
pub(crate) struct UploadPartResponse {
    pub etag: String,
}

/// Information about a part.
#[derive(Debug)]
pub(crate) struct PartInfo {
    pub part_number: i32,
    pub etag: String,
    pub size: u64,
}

/// Request for completing a multipart upload.
pub(crate) struct CompleteMultipartUploadRequest<'a> {
    pub upload_id: &'a str,
    pub parts: Vec<(i32, String)>,
}

/// Response for completing a multipart upload.
#[derive(Debug)]
pub(crate) struct CompleteMultipartUploadResponse {
    pub key: String,
    pub etag: String,
    pub metadata: ObjectMetadata,
}

impl StoreObjectRequest {
    pub fn new(
        key: impl Into<String>,
        body: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send>>,
        integrity_checks: ObjectIntegrityChecks,
    ) -> Self {
        Self {
            key: key.into(),
            body,
            integrity_checks,
            content_type: None,
            user_metadata: HashMap::new(),
        }
    }

    pub fn with_content_type(mut self, content_type: Option<String>) -> Self {
        self.content_type = content_type;
        self
    }

    pub fn with_user_metadata(mut self, user_metadata: HashMap<String, String>) -> Self {
        self.user_metadata = user_metadata;
        self
    }
}

impl From<s3s::dto::PutObjectInput> for StoreObjectRequest {
    fn from(input: s3s::dto::PutObjectInput) -> Self {
        let stream = input
            .body
            .unwrap_or_else(|| {
                let empty_stream = futures_util::stream::empty::<
                    std::result::Result<bytes::Bytes, std::io::Error>,
                >();
                s3s::dto::StreamingBlob::wrap(empty_stream)
            })
            .map_err(std::io::Error::other);

        Self {
            key: input.key,
            body: Box::pin(stream),
            integrity_checks: ObjectIntegrityChecks::new().with_md5(),
            content_type: input.content_type.map(|mime| mime.to_string()),
            user_metadata: input.metadata.unwrap_or_default(),
        }
    }
}

pub(crate) mod filesystem;
pub(crate) mod in_memory;
pub(crate) mod models;

/// A storage backend for the S3 Mock Server.
///
/// This trait defines the interface for storing and retrieving both object data
/// and metadata, as well as managing multipart upload operations. Storage backends
/// are responsible for all persistence concerns, allowing the S3 API layer to
/// remain stateless and focused on S3 semantics.
///
/// # Design Philosophy
///
/// The trait combines data and metadata operations to ensure consistency and
/// enable storage backends to manage their own persistence strategy. For example:
/// - In-memory storage can keep everything in RAM
/// - File-based storage can persist metadata alongside data files
/// - Database storage could store metadata in tables and data as BLOBs
///
/// # Object Operations
///
/// Objects are the primary entities in S3, consisting of both data and metadata.
/// All object operations work with complete objects, not just raw data.
///
/// # Multipart Upload Operations
///
/// Multipart uploads allow large objects to be uploaded in parts. The storage
/// backend manages the entire multipart upload lifecycle, including:
/// - Creating uploads with initial metadata
/// - Storing individual parts with their ETags
/// - Completing uploads by combining parts into final objects
/// - Aborting uploads and cleaning up partial data
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + Debug {
    // Object operations - combine data and metadata

    /// Store an object with integrity checking.
    async fn put_object(&self, request: StoreObjectRequest) -> Result<StoredObjectMetadata>;

    /// Retrieve an object as a stream with metadata, optionally with a byte range.
    ///
    /// This method is preferred for large objects as it avoids loading the entire
    /// object into memory. The stream yields chunks of bytes that can be processed
    /// incrementally.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `range` - Optional byte range to retrieve
    ///
    /// # Returns
    ///
    /// A stream of bytes and the object metadata, or None if the object doesn't exist
    async fn get_object(&self, request: GetObjectRequest<'_>) -> Result<Option<GetObjectResponse>>;

    /// Delete an object and its metadata.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    ///
    /// # Returns
    ///
    /// Success or an error if the operation fails
    async fn delete_object(&self, key: &str) -> Result<()>;

    /// List all objects with a given prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional prefix to filter objects
    ///
    /// # Returns
    ///
    /// A vector of (key, metadata) pairs for matching objects
    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ListObjectsResponse>;

    /// Get object metadata without fetching the data.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    ///
    /// # Returns
    ///
    /// The object metadata, or None if the object doesn't exist
    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>>;

    // Multipart upload operations

    /// Create a new multipart upload.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key for the final object
    /// * `upload_id` - The unique upload identifier
    /// * `metadata` - Initial metadata for the object being uploaded
    ///
    /// # Returns
    ///
    /// Success or an error if the operation fails
    async fn create_multipart_upload(
        &self,
        request: CreateMultipartUploadRequest<'_>,
    ) -> Result<()>;

    /// Upload a part for a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload identifier
    /// * `part_number` - The part number (1-based)
    /// * `content` - The part data
    ///
    /// # Returns
    ///
    /// The ETag for the uploaded part
    async fn upload_part(&self, request: UploadPartRequest<'_>) -> Result<UploadPartResponse>;

    /// List all parts for a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload identifier
    ///
    /// # Returns
    ///
    /// A vector of (part_number, etag, size) tuples for all uploaded parts
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>>;

    /// Complete a multipart upload by combining parts into a final object.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload identifier
    /// * `parts` - Vector of (part_number, etag) pairs in the order they should be combined
    ///
    /// # Returns
    ///
    /// A tuple of (final_object_key, final_object_metadata) for the completed object
    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUploadRequest<'_>,
    ) -> Result<CompleteMultipartUploadResponse>;

    /// Abort a multipart upload and clean up all associated data.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload identifier
    ///
    /// # Returns
    ///
    /// Success or an error if the operation fails
    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()>;
}

// Implement the trait for Arc<dyn StorageBackend> to allow for dynamic dispatch
#[async_trait]
impl StorageBackend for std::sync::Arc<dyn StorageBackend + '_> {
    async fn put_object(&self, request: StoreObjectRequest) -> Result<StoredObjectMetadata> {
        (**self).put_object(request).await
    }

    async fn get_object(&self, request: GetObjectRequest<'_>) -> Result<Option<GetObjectResponse>> {
        (**self).get_object(request).await
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        (**self).delete_object(key).await
    }

    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ListObjectsResponse> {
        (**self).list_objects(request).await
    }

    async fn create_multipart_upload(
        &self,
        request: CreateMultipartUploadRequest<'_>,
    ) -> Result<()> {
        (**self).create_multipart_upload(request).await
    }

    async fn upload_part(&self, request: UploadPartRequest<'_>) -> Result<UploadPartResponse> {
        (**self).upload_part(request).await
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<PartInfo>> {
        (**self).list_parts(upload_id).await
    }

    async fn complete_multipart_upload(
        &self,
        request: CompleteMultipartUploadRequest<'_>,
    ) -> Result<CompleteMultipartUploadResponse> {
        (**self).complete_multipart_upload(request).await
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        (**self).abort_multipart_upload(upload_id).await
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        (**self).head_object(key).await
    }
}
