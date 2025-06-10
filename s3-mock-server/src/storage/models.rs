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
}

/// Metadata for a part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PartMetadata {
    /// ETag of the part.
    pub etag: String,

    /// Size of the part in bytes.
    pub size: u64,
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
