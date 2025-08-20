/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Data models for storage operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Metadata for an S3 object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ObjectMetadata {
    /// Content type of the object.
    pub content_type: Option<String>,

    /// Size of the object in bytes.
    pub content_length: u64,

    /// ETag of the object.
    pub etag: String,

    /// Last modified time of the object.
    pub last_modified: SystemTime,

    /// User-defined metadata.
    pub user_metadata: HashMap<String, String>,

    /// Checksum values for the object.
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

impl Default for ObjectMetadata {
    fn default() -> Self {
        Self {
            content_type: None,
            content_length: 0,
            etag: String::new(),
            last_modified: SystemTime::UNIX_EPOCH,
            user_metadata: HashMap::new(),
            crc32: None,
            crc32c: None,
            crc64nvme: None,
            sha1: None,
            sha256: None,
        }
    }
}

/// Metadata for a part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PartMetadata {
    /// ETag of the part.
    pub etag: String,

    /// Size of the part in bytes.
    pub size: u64,

    /// Checksum values for the part.
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

/// Metadata for a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultipartUploadMetadata {
    /// Key of the object being uploaded.
    pub key: String,

    /// ID of the upload.
    pub upload_id: String,

    /// Metadata for the object being uploaded.
    pub metadata: ObjectMetadata,

    /// Parts that have been uploaded.
    pub parts: HashMap<i32, PartMetadata>,
}
