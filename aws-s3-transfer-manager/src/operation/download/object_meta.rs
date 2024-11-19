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

/// Object metadata other than the body that can be set from either `GetObject` or `HeadObject`
#[derive(Debug, Clone, Default)]
pub struct ObjectMetadata {
    _request_id: Option<String>,
    _extended_request_id: Option<String>,
    pub delete_marker: Option<bool>,
    pub expiration: Option<String>,
    pub restore: Option<String>,
    pub last_modified: Option<::aws_smithy_types::DateTime>,
    pub(crate) content_length: Option<i64>,
    pub e_tag: Option<String>,
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
    pub object_lock_mode: Option<aws_sdk_s3::types::ObjectLockMode>,
    pub object_lock_retain_until_date: Option<::aws_smithy_types::DateTime>,
    pub object_lock_legal_hold_status: Option<aws_sdk_s3::types::ObjectLockLegalHoldStatus>,
    pub request_charged: Option<aws_sdk_s3::types::RequestCharged>,
}

impl ObjectMetadata {
    /// The total object size
    pub fn content_length(&self) -> u64 {
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
            _request_id: value.request_id().map(|s| s.to_string()),
            _extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            delete_marker: value.delete_marker,
            expiration: value.expiration.clone(),
            restore: value.restore.clone(),
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag.clone(),
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
            object_lock_mode: value.object_lock_mode.clone(),
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status.clone(),
            request_charged: value.request_charged.clone(),
        }
    }
}

impl From<HeadObjectOutput> for ObjectMetadata {
    fn from(value: HeadObjectOutput) -> Self {
        Self {
            _request_id: value.request_id().map(|s| s.to_string()),
            _extended_request_id: value.extended_request_id().map(|s| s.to_string()),
            delete_marker: value.delete_marker,
            expiration: value.expiration,
            restore: value.restore,
            last_modified: value.last_modified,
            content_length: value.content_length,
            e_tag: value.e_tag,
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
            replication_status: value.replication_status,
            parts_count: value.parts_count,
            object_lock_mode: value.object_lock_mode,
            object_lock_retain_until_date: value.object_lock_retain_until_date,
            object_lock_legal_hold_status: value.object_lock_legal_hold_status,
            request_charged: value.request_charged,
        }
    }
}

impl RequestIdExt for ObjectMetadata {
    fn extended_request_id(&self) -> Option<&str> {
        self._extended_request_id.as_deref()
    }
}
impl RequestId for ObjectMetadata {
    fn request_id(&self) -> Option<&str> {
        self._request_id.as_deref()
    }
}


#[cfg(test)]
mod tests {
    use super::ObjectMetadata;

    #[test]
    fn test_inferred_total_size() {
        let meta = ObjectMetadata {
            content_length: Some(15),
            ..Default::default()
        };

        assert_eq!(15, meta.content_length());

        let meta = ObjectMetadata {
            content_range: Some("bytes 0-499/900".to_owned()),
            content_length: Some(500),
            ..Default::default()
        };
        assert_eq!(900, meta.content_length());
    }
}
