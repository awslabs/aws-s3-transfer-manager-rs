/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::operation::RequestIdExt;

/// request metadata other than the body that will be set from `GetObject`
// TODO: Document fields
#[derive(Debug, Clone, Default)]
pub struct ChunkMetadata {
    pub request_id: Option<String>,
    pub extended_request_id: Option<String>,
    pub delete_marker: Option<bool>,
    pub accept_ranges: Option<String>,
    pub expiration: Option<String>,
    pub restore: Option<String>,
    pub last_modified: Option<::aws_smithy_types::DateTime>,
    pub content_length: Option<i64>,
    pub e_tag: Option<String>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32_c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub missing_meta: Option<i32>,
    pub version_id: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_range: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<::aws_smithy_types::DateTime>,
    pub expires_string: Option<String>,
    pub website_redirect_location: Option<String>,
    pub server_side_encryption: Option<aws_sdk_s3::types::ServerSideEncryption>,
    pub metadata: Option<::std::collections::HashMap<String, String>>,
    pub sse_customer_algorithm: Option<String>,
    pub sse_customer_key_md5: Option<String>,
    pub ssekms_key_id: Option<String>,
    pub bucket_key_enabled: Option<bool>,
    pub storage_class: Option<aws_sdk_s3::types::StorageClass>,
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,
    pub replication_status: Option<aws_sdk_s3::types::ReplicationStatus>,
    pub parts_count: Option<i32>,
    pub tag_count: Option<i32>,
    pub object_lock_mode: Option<aws_sdk_s3::types::ObjectLockMode>,
    pub object_lock_retain_until_date: Option<::aws_smithy_types::DateTime>,
    pub object_lock_legal_hold_status: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,
}

impl From<GetObjectOutput> for ChunkMetadata {
    fn from(value: GetObjectOutput) -> Self {
        Self {
            request_id: value.request_id().map(|s| s.to_string()),
            extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges,
            expiration: value.expiration,
            restore: value.restore,
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag,
            checksum_crc32: value.checksum_crc32,
            checksum_crc32_c: value.checksum_crc32_c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            missing_meta: value.missing_meta,
            version_id: value.version_id,
            cache_control: value.cache_control,
            content_disposition: value.content_disposition,
            content_encoding: value.content_encoding,
            content_language: value.content_language,
            content_range: value.content_range,
            content_type: value.content_type,
            #[allow(deprecated)]
            expires: value.expires,
            expires_string: value.expires_string,
            website_redirect_location: value.website_redirect_location,
            server_side_encryption: value.server_side_encryption,
            metadata: value.metadata,
            sse_customer_algorithm: value.sse_customer_algorithm,
            sse_customer_key_md5: value.sse_customer_key_md5,
            ssekms_key_id: value.ssekms_key_id,
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class,
            request_charged: value.request_charged,
            replication_status: value.replication_status,
            parts_count: value.parts_count,
            tag_count: value.tag_count,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
        }
    }
}