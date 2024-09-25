/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::str::FromStr;

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;

use super::header;

// TODO(aws-sdk-rust#1159,design): how many of these fields should we expose?
// TODO(aws-sdk-rust#1159,docs): Document fields

/// Object metadata other than the body that can be set from either `GetObject` or `HeadObject`
#[derive(Debug, Clone, Default)]
pub struct ObjectMetadata {
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

    /// Content length from either the `Content-Range` or `Content-Length` headers
    pub(crate) fn content_length(&self) -> u64 {
        match (self.content_length, self.content_range.as_ref()) {
            (Some(length), _) => {
                debug_assert!(length > 0, "content length invalid");
                length as u64
            },
            (None, Some(range)) => {
                let byte_range_str = range
                    .strip_prefix("bytes ")
                    .expect("content range bytes-unit recognized")
                    .split_once('/')
                    .map(|x| x.0)
                    .expect("content range valid");

                match header::ByteRange::from_str(byte_range_str).expect("valid byte range") {
                    header::ByteRange::Inclusive(start, end) => end - start + 1,
                    _ => unreachable!("Content-Range header invalid")
                }
            }
            (None, None) => panic!("total object size cannot be calculated without either content length or content range headers")
        }
    }
}

impl From<GetObjectOutput> for ObjectMetadata {
    fn from(value: GetObjectOutput) -> Self {
        Self {
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

impl From<HeadObjectOutput> for ObjectMetadata {
    fn from(value: HeadObjectOutput) -> Self {
        Self {
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
            content_range: None,
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
            tag_count: None,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectMetadata;

    #[test]
    fn test_inferred_content_length() {
        let meta = ObjectMetadata {
            content_length: Some(4),
            content_range: Some("should ignore".to_owned()),
            ..Default::default()
        };

        assert_eq!(4, meta.content_length());

        let meta = ObjectMetadata {
            content_length: None,
            content_range: Some("bytes 0-499/900".to_owned()),
            ..Default::default()
        };
        assert_eq!(500, meta.content_length());
    }
}
