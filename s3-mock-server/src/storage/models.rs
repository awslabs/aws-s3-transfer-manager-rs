/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Data models for storage operations.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::SystemTime;

// Custom serialization for ChecksumAlgorithm
fn serialize_checksum_algorithm<S>(
    algorithm: &Option<aws_smithy_checksums::ChecksumAlgorithm>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match algorithm {
        Some(alg) => serializer.serialize_some(alg.as_str()),
        None => serializer.serialize_none(),
    }
}

// Custom deserialization for ChecksumAlgorithm
fn deserialize_checksum_algorithm<'de, D>(
    deserializer: D,
) -> Result<Option<aws_smithy_checksums::ChecksumAlgorithm>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_str: Option<String> = Option::deserialize(deserializer)?;
    match opt_str {
        Some(s) => aws_smithy_checksums::ChecksumAlgorithm::from_str(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

// Custom serialization for ChecksumType
fn serialize_checksum_type<S>(
    checksum_type: &Option<aws_sdk_s3::types::ChecksumType>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match checksum_type {
        Some(ct) => serializer.serialize_some(ct.as_str()),
        None => serializer.serialize_none(),
    }
}

// Custom deserialization for ChecksumType
fn deserialize_checksum_type<'de, D>(
    deserializer: D,
) -> Result<Option<aws_sdk_s3::types::ChecksumType>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt_str: Option<String> = Option::deserialize(deserializer)?;
    match opt_str {
        Some(s) => aws_sdk_s3::types::ChecksumType::from_str(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

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

    /// Checksum algorithm specified for multipart uploads.
    #[serde(
        serialize_with = "serialize_checksum_algorithm",
        deserialize_with = "deserialize_checksum_algorithm"
    )]
    pub checksum_algorithm: Option<aws_smithy_checksums::ChecksumAlgorithm>,

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
            checksum_algorithm: None,
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

    /// Checksum type specified during upload creation (FULL_OBJECT vs COMPOSITE).
    #[serde(
        serialize_with = "serialize_checksum_type",
        deserialize_with = "deserialize_checksum_type"
    )]
    pub checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
}
