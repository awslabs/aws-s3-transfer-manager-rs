/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::model::{ChecksumMode, DownloadInput, RequestPayer};
use crate::sdk_v1::{
    copy_download_input_fields_to_get_object, copy_download_input_fields_to_head_object,
};
use aws_smithy_mocks::mock_client;
use aws_smithy_types::DateTime;

/// Exhaustive destructuring makes a new field a compile-time classification decision.
fn assert_all_input_fields_classified(input: &DownloadInput) {
    let DownloadInput {
        // Forwarded to both GET and HEAD.
        bucket: _,
        if_match: _,
        if_modified_since: _,
        if_none_match: _,
        if_unmodified_since: _,
        key: _,
        range: _,
        response_cache_control: _,
        response_content_disposition: _,
        response_content_encoding: _,
        response_content_language: _,
        response_content_type: _,
        response_expires: _,
        version_id: _,
        sse_customer_algorithm: _,
        sse_customer_key: _,
        sse_customer_key_md5: _,
        request_payer: _,
        expected_bucket_owner: _,
        checksum_mode: _,
        // Discovery controls request-local part numbers. Read-ahead is TM-only.
        part_number: _,
        read_ahead: _,
    } = input;
}

fn request_with_all_forwarded_fields() -> DownloadInput {
    let mut input = DownloadInput::builder()
        .bucket("bucket")
        .if_match("if-match")
        .if_modified_since(DateTime::from_secs(1))
        .if_none_match("if-none-match")
        .if_unmodified_since(DateTime::from_secs(2))
        .key("key")
        .range("bytes=10-19")
        .response_cache_control("no-cache")
        .response_content_disposition("attachment")
        .response_content_encoding("gzip")
        .response_content_language("en-US")
        .response_content_type("application/octet-stream")
        .response_expires(DateTime::from_secs(3))
        .version_id("version")
        .sse_customer_algorithm("AES256")
        .sse_customer_key("secret")
        .sse_customer_key_md5("key-md5")
        .request_payer(RequestPayer::Requester)
        .expected_bucket_owner("owner")
        .checksum_mode(ChecksumMode::Enabled)
        .read_ahead(crate::types::ReadAhead::Parts(3))
        .build()
        .unwrap();
    input.part_number = Some(7);
    input
}

macro_rules! assert_forwarded_fields {
    ($input:expr, $request:expr) => {
        assert_eq!($input.bucket(), $request.bucket());
        assert_eq!($input.if_match(), $request.if_match());
        assert_eq!($input.if_modified_since(), $request.if_modified_since());
        assert_eq!($input.if_none_match(), $request.if_none_match());
        assert_eq!($input.if_unmodified_since(), $request.if_unmodified_since());
        assert_eq!($input.key(), $request.key());
        assert_eq!($input.range(), $request.range());
        assert_eq!(
            $input.response_cache_control(),
            $request.response_cache_control()
        );
        assert_eq!(
            $input.response_content_disposition(),
            $request.response_content_disposition()
        );
        assert_eq!(
            $input.response_content_encoding(),
            $request.response_content_encoding()
        );
        assert_eq!(
            $input.response_content_language(),
            $request.response_content_language()
        );
        assert_eq!(
            $input.response_content_type(),
            $request.response_content_type()
        );
        assert_eq!($input.response_expires(), $request.response_expires());
        assert_eq!($input.version_id(), $request.version_id());
        assert_eq!(
            $input.sse_customer_algorithm(),
            $request.sse_customer_algorithm()
        );
        assert_eq!($input.sse_customer_key(), $request.sse_customer_key());
        assert_eq!(
            $input.sse_customer_key_md5(),
            $request.sse_customer_key_md5()
        );
        assert_eq!(
            $input.request_payer().map(|v| v.as_str()),
            $request.request_payer().map(|v| v.as_str())
        );
        assert_eq!(
            $input.expected_bucket_owner(),
            $request.expected_bucket_owner()
        );
        assert_eq!(
            $input.checksum_mode().map(|v| v.as_str()),
            $request.checksum_mode().map(|v| v.as_str())
        );
        // A preselected request-local part number must not be replaced by the copier.
        assert_eq!(Some(2), $request.part_number());
    };
}

#[test]
fn test_all_fields_copied_to_get_object_request() {
    let input = request_with_all_forwarded_fields();
    assert_all_input_fields_classified(&input);
    let client = mock_client!(aws_sdk_s3, []);
    let request = copy_download_input_fields_to_get_object(&input, client.get_object())
        .as_input()
        .clone()
        .build()
        .unwrap();
    assert_eq!(None, request.part_number());
    let request =
        copy_download_input_fields_to_get_object(&input, client.get_object().part_number(2))
            .as_input()
            .clone()
            .build()
            .unwrap();
    assert_forwarded_fields!(input, request);
}

#[test]
fn test_all_fields_copied_to_head_object_request() {
    let input = request_with_all_forwarded_fields();
    assert_all_input_fields_classified(&input);
    let client = mock_client!(aws_sdk_s3, []);
    let request = copy_download_input_fields_to_head_object(&input, client.head_object())
        .as_input()
        .clone()
        .build()
        .unwrap();
    assert_eq!(None, request.part_number());
    let request =
        copy_download_input_fields_to_head_object(&input, client.head_object().part_number(2))
            .as_input()
            .clone()
            .build()
            .unwrap();
    assert_forwarded_fields!(input, request);
}

#[test]
fn test_from_get_object_output() {
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::types::{
        ChecksumType, ObjectLockLegalHoldStatus, ObjectLockMode, ReplicationStatus, RequestCharged,
        ServerSideEncryption, StorageClass,
    };
    let get_object_output = GetObjectOutput::builder()
        .accept_ranges("bytes=0-999")
        .bucket_key_enabled(true)
        .cache_control("no-cache")
        .checksum_crc32("AAAAAA==")
        .checksum_type(ChecksumType::FullObject)
        .content_disposition("attachment")
        .content_encoding("gzip")
        .content_language("en")
        .content_length(1024)
        .content_range("bytes 0-1023/1024")
        .content_type("application/octet-stream")
        .delete_marker(false)
        .e_tag("test-etag")
        .expiration("test-expiration")
        .expires_string("test-expires")
        .last_modified(DateTime::from_secs(1234567890))
        .missing_meta(0)
        .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(DateTime::from_secs(1234567890))
        .parts_count(1)
        .replication_status(ReplicationStatus::Complete)
        .request_charged(RequestCharged::Requester)
        .restore("test-restore")
        .server_side_encryption(ServerSideEncryption::Aes256)
        .sse_customer_algorithm("AES256")
        .sse_customer_key_md5("test-md5")
        .ssekms_key_id("test-kms-key")
        .storage_class(StorageClass::Standard)
        .tag_count(2)
        .version_id("test-version")
        .website_redirect_location("https://example.com")
        .build();
    let chunk_metadata = crate::sdk_v1::chunk_metadata_from_get_object(&get_object_output);
    assert_eq!(
        Some("bytes=0-999".to_string()),
        chunk_metadata.accept_ranges
    );
    assert_eq!(Some(true), chunk_metadata.bucket_key_enabled);
    assert_eq!(Some("no-cache".to_string()), chunk_metadata.cache_control);
    assert_eq!(Some("AAAAAA==".to_string()), chunk_metadata.checksum_crc32);
    assert_eq!(None, chunk_metadata.checksum_crc32_c);
    assert_eq!(None, chunk_metadata.checksum_crc64_nvme);
    assert_eq!(None, chunk_metadata.checksum_sha1);
    assert_eq!(None, chunk_metadata.checksum_sha256);
    assert_eq!(
        Some(crate::model::ChecksumType::FullObject),
        chunk_metadata.checksum_type
    );
    assert_eq!(
        Some("attachment".to_string()),
        chunk_metadata.content_disposition
    );
    assert_eq!(Some("gzip".to_string()), chunk_metadata.content_encoding);
    assert_eq!(Some("en".to_string()), chunk_metadata.content_language);
    assert_eq!(Some(1024), chunk_metadata.content_length);
    assert_eq!(
        Some("bytes 0-1023/1024".to_string()),
        chunk_metadata.content_range
    );
    assert_eq!(
        Some("application/octet-stream".to_string()),
        chunk_metadata.content_type
    );
    assert_eq!(Some(false), chunk_metadata.delete_marker);
    assert_eq!(Some("test-etag".to_string()), chunk_metadata.e_tag);
    assert_eq!(
        Some("test-expiration".to_string()),
        chunk_metadata.expiration
    );
    assert_eq!(
        Some("test-expires".to_string()),
        chunk_metadata.expires_string
    );
    assert_eq!(
        Some(DateTime::from_secs(1234567890)),
        chunk_metadata.last_modified
    );
    assert_eq!(Some(0), chunk_metadata.missing_meta);
    assert_eq!(
        Some(crate::model::ObjectLockLegalHoldStatus::On),
        chunk_metadata.object_lock_legal_hold_status
    );
    assert_eq!(
        Some(crate::model::ObjectLockMode::Governance),
        chunk_metadata.object_lock_mode
    );
    assert_eq!(
        Some(DateTime::from_secs(1234567890)),
        chunk_metadata.object_lock_retain_until_date
    );
    assert_eq!(Some(1), chunk_metadata.parts_count);
    assert_eq!(
        Some(crate::model::ReplicationStatus::Complete),
        chunk_metadata.replication_status
    );
    assert_eq!(
        Some(crate::model::RequestCharged::Requester),
        chunk_metadata.request_charged
    );
    assert_eq!(Some("test-restore".to_string()), chunk_metadata.restore);
    assert_eq!(
        Some(crate::model::ServerSideEncryption::Aes256),
        chunk_metadata.server_side_encryption
    );
    assert_eq!(
        Some("AES256".to_string()),
        chunk_metadata.sse_customer_algorithm
    );
    assert_eq!(
        Some("test-md5".to_string()),
        chunk_metadata.sse_customer_key_md5
    );
    assert_eq!(
        Some("test-kms-key".to_string()),
        chunk_metadata.ssekms_key_id
    );
    assert_eq!(
        Some(crate::model::StorageClass::Standard),
        chunk_metadata.storage_class
    );
    assert_eq!(Some(2), chunk_metadata.tag_count);
    assert_eq!(Some("test-version".to_string()), chunk_metadata.version_id);
    assert_eq!(
        Some("https://example.com".to_string()),
        chunk_metadata.website_redirect_location
    );
}
