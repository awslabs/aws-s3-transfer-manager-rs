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
use std::fmt::Debug;
use std::ops::Range;

use crate::Result;

pub(crate) mod filesystem;
pub(crate) mod in_memory;
pub(crate) mod models;

use models::ObjectMetadata;

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

    /// Store an object with its data and metadata.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `content` - The object data
    /// * `metadata` - The object metadata
    ///
    /// # Returns
    ///
    /// Success or an error if the operation fails
    async fn put_object(&self, key: &str, content: Bytes, metadata: ObjectMetadata) -> Result<()>;

    /// Retrieve an object's data and metadata, optionally with a byte range.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `range` - Optional byte range to retrieve
    ///
    /// # Returns
    ///
    /// The object data and metadata, or None if the object doesn't exist
    async fn get_object(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<(Bytes, ObjectMetadata)>>;

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
    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<(String, ObjectMetadata)>>;

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
        key: &str,
        upload_id: &str,
        metadata: ObjectMetadata,
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
    async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<String>;

    /// List all parts for a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload identifier
    ///
    /// # Returns
    ///
    /// A vector of (part_number, etag, size) tuples for all uploaded parts
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(i32, String, u64)>>;

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
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(String, ObjectMetadata)>;

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
    async fn put_object(&self, key: &str, content: Bytes, metadata: ObjectMetadata) -> Result<()> {
        (**self).put_object(key, content, metadata).await
    }

    async fn get_object(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<(Bytes, ObjectMetadata)>> {
        (**self).get_object(key, range).await
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        (**self).delete_object(key).await
    }

    async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<(String, ObjectMetadata)>> {
        (**self).list_objects(prefix).await
    }

    async fn create_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        metadata: ObjectMetadata,
    ) -> Result<()> {
        (**self)
            .create_multipart_upload(key, upload_id, metadata)
            .await
    }

    async fn upload_part(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<String> {
        (**self).upload_part(upload_id, part_number, content).await
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(i32, String, u64)>> {
        (**self).list_parts(upload_id).await
    }

    async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> Result<(String, ObjectMetadata)> {
        (**self).complete_multipart_upload(upload_id, parts).await
    }

    async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        (**self).abort_multipart_upload(upload_id).await
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        (**self).head_object(key).await
    }
}
