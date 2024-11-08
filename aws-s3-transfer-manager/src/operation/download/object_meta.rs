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
pub struct ObjectMetadata {
    pub delete_marker: Option<bool>,
    pub accept_ranges: Option<String>,
    pub expiration: Option<String>,
    pub restore: Option<String>,
    pub last_modified: Option<::aws_smithy_types::DateTime>,
    pub(crate) content_length: Option<i64>,
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
    pub(crate) content_range: Option<String>,
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
    pub replication_status: Option<aws_sdk_s3::types::ReplicationStatus>,
    pub parts_count: Option<i32>,
    pub tag_count: Option<i32>,
    pub object_lock_mode: Option<aws_sdk_s3::types::ObjectLockMode>,
    pub object_lock_retain_until_date: Option<::aws_smithy_types::DateTime>,
    pub object_lock_legal_hold_status: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,

    // request_id if the client made a request to get object metadata, like HeadObject.
    pub request_id: Option<String>,
    // extended_request_id if the client made a request to get object metadata, like HeadObject.
    pub extended_request_id: Option<String>,
    // whether the request that client made only to get object metadata, like HeadObject.
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,
}

impl ObjectMetadata {
    /// The total object size
    pub fn total_size(&self) -> u64 {
        match (self.content_length, self.content_range.as_ref()) {
            (_, Some(range)) => {
                let total = range.split_once('/').map(|x| x.1).expect("content range total");
                total.parse().expect("valid range total")
            }
            (Some(length), None) => {
                debug_assert!(length > 0, "content length invalid");
                length as u64
            },
            (None, None) => panic!("total object size cannot be calculated without either content length or content range headers")
        }
    }
}

impl From<&GetObjectOutput> for ObjectMetadata {
    fn from(value: &GetObjectOutput) -> Self {
        Self {
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges.clone(),
            expiration: value.expiration.clone(),
            restore: value.restore.clone(),
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag.clone(),
            checksum_crc32: value.checksum_crc32.clone(),
            checksum_crc32_c: value.checksum_crc32_c.clone(),
            checksum_sha1: value.checksum_sha1.clone(),
            checksum_sha256: value.checksum_sha256.clone(),
            missing_meta: value.missing_meta,
            version_id: value.version_id.clone(),
            cache_control: value.cache_control.clone(),
            content_disposition: value.content_disposition.clone(),
            content_encoding: value.content_encoding.clone(),
            content_language: value.content_language.clone(),
            content_range: value.content_range.clone(),
            content_type: value.content_type.clone(),
            #[allow(deprecated)]
            expires: value.expires,
            expires_string: value.expires_string.clone(),
            website_redirect_location: value.website_redirect_location.clone(),
            server_side_encryption: value.server_side_encryption.clone(),
            metadata: value.metadata.clone(),
            sse_customer_algorithm: value.sse_customer_algorithm.clone(),
            sse_customer_key_md5: value.sse_customer_key_md5.clone(),
            ssekms_key_id: value.ssekms_key_id.clone(),
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class.clone(),
            replication_status: value.replication_status.clone(),
            parts_count: value.parts_count,
            tag_count: value.tag_count,
            object_lock_mode: value.object_lock_mode.clone(),
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status.clone(),
            request_id: None,
            extended_request_id: None,
            request_charged: None,
        }
    }
}

impl From<&HeadObjectOutput> for ObjectMetadata {
    fn from(value: &HeadObjectOutput) -> Self {
        Self {
            delete_marker: value.delete_marker,
            accept_ranges: value.accept_ranges.clone(),
            expiration: value.expiration.clone(),
            restore: value.restore.clone(),
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag.clone(),
            checksum_crc32: value.checksum_crc32.clone(),
            checksum_crc32_c: value.checksum_crc32_c.clone(),
            checksum_sha1: value.checksum_sha1.clone(),
            checksum_sha256: value.checksum_sha256.clone(),
            missing_meta: value.missing_meta,
            version_id: value.version_id.clone(),
            cache_control: value.cache_control.clone(),
            content_disposition: value.content_disposition.clone(),
            content_encoding: value.content_encoding.clone(),
            content_language: value.content_language.clone(),
            content_range: None,
            content_type: value.content_type.clone(),
            #[allow(deprecated)]
            expires: value.expires,
            expires_string: value.expires_string.clone(),
            website_redirect_location: value.website_redirect_location.clone(),
            server_side_encryption: value.server_side_encryption.clone(),
            metadata: value.metadata.clone(),
            sse_customer_algorithm: value.sse_customer_algorithm.clone(),
            sse_customer_key_md5: value.sse_customer_key_md5.clone(),
            ssekms_key_id: value.ssekms_key_id.clone(),
            bucket_key_enabled: value.bucket_key_enabled,
            storage_class: value.storage_class.clone(),
            replication_status: value.replication_status.clone(),
            parts_count: value.parts_count,
            tag_count: None,
            object_lock_mode: value.object_lock_mode.clone(),
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status.clone(),
            request_id: value.request_id().map(|s| s.to_string()),
            extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            request_charged: value.request_charged.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::ObjectMetadata;

    // #[test]
    // fn test_inferred_content_length() {
    //     let meta = ObjectMetadata {
    //         content_length: Some(4),
    //         content_range: Some("should ignore".to_owned()),
    //         ..Default::default()
    //     };

    //     assert_eq!(4, meta.content_length());

    //     let meta = ObjectMetadata {
    //         content_length: None,
    //         content_range: Some("bytes 0-499/900".to_owned()),
    //         ..Default::default()
    //     };
    //     assert_eq!(500, meta.content_length());
    // }
}
