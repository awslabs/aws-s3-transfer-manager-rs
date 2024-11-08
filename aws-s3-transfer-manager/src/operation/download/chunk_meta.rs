/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::operation::RequestIdExt;

// TODO(aws-sdk-rust#1159,design): how many of these fields should we expose?
// TODO(aws-sdk-rust#1159,docs): Document fields

/// Object metadata other than the body
#[derive(Debug, Clone, Default)]
pub struct ChunkMetadata {
    pub request_id: Option<String>,
    pub extended_request_id: Option<String>,
    pub content_length: Option<i64>,
    pub content_range: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32_c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,
}

impl From<GetObjectOutput> for ChunkMetadata {
    fn from(value: GetObjectOutput) -> Self {
        Self {
            // TODO: waahm7 should we just impl the traits? why traits?
            request_id: value.request_id().map(|s| s.to_string()),
            extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            content_length: value.content_length,
            content_range: value.content_range,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            request_charged: value.request_charged,
        }
    }
}

impl From<HeadObjectOutput> for ChunkMetadata {
    fn from(value: HeadObjectOutput) -> Self {
        Self {
            request_id: value.request_id().map(|s| s.to_string()),
            extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            // Do not include Content-Length as it will not be accurate as chunk metadata
            content_length: None,
            content_range: None,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            request_charged: value.request_charged,
        }
    }
}
