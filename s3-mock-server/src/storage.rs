/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Storage backends for the S3 Mock Server.
//!
//! This module provides the `StorageBackend` trait and its implementations.
//! The trait defines the interface for storing and retrieving object data
//! and multipart upload parts.

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use std::ops::Range;

use crate::Result;

pub(crate) mod filesystem;
pub(crate) mod in_memory;
pub(crate) mod models;

/// A storage backend for the S3 Mock Server.
///
/// This trait defines the interface for storing and retrieving object data
/// and multipart upload parts. It is focused solely on data storage operations,
/// with metadata management and S3 API logic handled separately.
#[async_trait]
pub(crate) trait StorageBackend: Send + Sync + Debug {
    /// Get object data for the specified key and optional range.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `range` - Optional byte range to retrieve
    ///
    /// # Returns
    ///
    /// The object data as bytes
    async fn get_object_data(&self, key: &str, range: Option<Range<u64>>) -> Result<Bytes>;

    /// Store object data for the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    /// * `content` - The object data
    async fn put_object_data(&self, key: &str, content: Bytes) -> Result<()>;

    /// Delete object data for the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The object key
    async fn delete_object_data(&self, key: &str) -> Result<()>;

    /// Store part data for a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload ID
    /// * `part_number` - The part number
    /// * `content` - The part data
    async fn store_part_data(
        &self,
        upload_id: &str,
        part_number: i32,
        content: Bytes,
    ) -> Result<()>;

    /// Get part data for a multipart upload.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload ID
    /// * `part_number` - The part number
    ///
    /// # Returns
    ///
    /// The part data as bytes
    async fn get_part_data(&self, upload_id: &str, part_number: i32) -> Result<Bytes>;

    /// Complete a multipart upload by combining the parts.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload ID
    /// * `key` - The object key to associate the combined parts with
    /// * `part_numbers` - The part numbers to include in the final object, in order
    async fn complete_multipart_data(
        &self,
        upload_id: &str,
        key: &str,
        part_numbers: &[i32],
    ) -> Result<()>;

    /// Abort a multipart upload and delete all parts.
    ///
    /// # Arguments
    ///
    /// * `upload_id` - The upload ID
    async fn abort_multipart_data(&self, upload_id: &str) -> Result<()>;
}
