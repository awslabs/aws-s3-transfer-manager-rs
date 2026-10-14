/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
#![cfg(feature = "sdk-v1")]

use aws_sdk_s3_transfer_manager::model;
use aws_smithy_types::DateTime;

macro_rules! enum_roundtrip {
    ($name:ident) => {
        for name in model::$name::values()
            .iter()
            .copied()
            .chain(["FUTURE_VALUE"])
        {
            let tm = model::$name::from(name);
            let sdk = aws_sdk_s3::types::$name::from(&tm);
            assert_eq!(sdk.as_str(), name);
            assert_eq!(model::$name::from(&sdk), tm);
            let sdk: aws_sdk_s3::types::$name = tm.clone().into();
            let roundtrip: model::$name = sdk.into();
            assert_eq!(roundtrip, tm);
        }
    };
}

#[test]
fn owned_and_borrowed_enum_conversions_preserve_known_and_unknown_values() {
    enum_roundtrip!(ChecksumAlgorithm);
    enum_roundtrip!(ChecksumType);
    enum_roundtrip!(ChecksumMode);
    enum_roundtrip!(ObjectCannedAcl);
    enum_roundtrip!(ObjectLockLegalHoldStatus);
    enum_roundtrip!(ObjectLockMode);
    enum_roundtrip!(ObjectStorageClass);
    enum_roundtrip!(ReplicationStatus);
    enum_roundtrip!(RequestCharged);
    enum_roundtrip!(RequestPayer);
    enum_roundtrip!(ServerSideEncryption);
    enum_roundtrip!(StorageClass);
}

#[test]
fn owned_and_borrowed_nested_values_preserve_storage_not_getter_defaults() {
    let time = DateTime::from_secs(42);
    let object = model::Object::builder()
        .key("key")
        .size(0)
        .last_modified(time)
        .set_checksum_algorithm(Some(Vec::new()))
        .owner(model::Owner::builder().id("owner").build())
        .restore_status(
            model::RestoreStatus::builder()
                .is_restore_in_progress(false)
                .restore_expiry_date(time)
                .build(),
        )
        .build();
    let borrowed = aws_sdk_s3::types::Object::from(&object);
    let owned = aws_sdk_s3::types::Object::from(object.clone());
    for sdk in [borrowed, owned] {
        let tm = model::Object::from(&sdk);
        assert_eq!(tm.last_modified(), Some(&time));
        assert_eq!(tm.size(), Some(0));
        assert_eq!(tm.checksum_algorithm, Some(Vec::new()));
        assert_eq!(
            tm.restore_status().unwrap().is_restore_in_progress(),
            Some(false)
        );
        assert_eq!(tm.owner().unwrap().id(), Some("owner"));
        assert_eq!(model::Object::from(sdk).key(), Some("key"));
    }
    let absent = model::Object::from(aws_sdk_s3::types::Object::builder().build());
    assert_eq!(absent.checksum_algorithm, None);
    assert_eq!(absent.size(), None);
    assert_eq!(absent.restore_status(), None);
}

#[test]
fn put_response_conversion_preserves_values_without_claiming_a_complete_transfer() {
    let sdk = aws_sdk_s3::operation::put_object::PutObjectOutput::builder()
        .e_tag("etag")
        .size(0)
        .bucket_key_enabled(false)
        .checksum_sha512("checksum")
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::from("FUTURE"))
        .ssekms_key_id("key")
        .build();
    let borrowed = model::builders::UploadOutputBuilder::from(&sdk);
    let owned = model::builders::UploadOutputBuilder::from(sdk);
    for builder in [borrowed, owned] {
        assert_eq!(builder.get_e_tag().as_deref(), Some("etag"));
        assert_eq!(builder.get_size(), &Some(0));
        assert_eq!(builder.get_bucket_key_enabled(), &Some(false));
        assert_eq!(builder.get_checksum_sha512().as_deref(), Some("checksum"));
        assert_eq!(builder.get_sse_kms_key_id().as_deref(), Some("key"));
        assert_eq!(
            builder
                .get_server_side_encryption()
                .as_ref()
                .unwrap()
                .as_str(),
            "FUTURE"
        );
        assert_eq!(builder.get_upload_id(), &None);
        assert!(builder.build().is_err());
    }
}

#[test]
fn multipart_response_conversion_preserves_create_members_during_completion() {
    let create =
        aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput::builder()
            .upload_id("upload")
            .abort_rule_id("rule")
            .ssekms_encryption_context("context")
            .bucket_key_enabled(true)
            .build();
    let borrowed = model::builders::UploadOutputBuilder::from(&create);
    let owned = model::builders::UploadOutputBuilder::from(create);
    let complete =
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput::builder()
            .location("location")
            .e_tag("etag")
            .checksum_xxhash128("checksum")
            .build();
    for builder in [borrowed, owned] {
        let builder = builder.update_from_complete_mpu(&complete);
        assert_eq!(builder.get_upload_id().as_deref(), Some("upload"));
        assert_eq!(builder.get_abort_rule_id().as_deref(), Some("rule"));
        assert_eq!(
            builder.get_sse_kms_encryption_context().as_deref(),
            Some("context")
        );
        assert_eq!(builder.get_bucket_key_enabled(), &None);
        assert_eq!(builder.get_location().as_deref(), Some("location"));
        assert_eq!(builder.get_e_tag().as_deref(), Some("etag"));
        assert_eq!(
            builder.get_checksum_xxhash128().as_deref(),
            Some("checksum")
        );
        assert!(builder.build().is_err());
    }
}

#[test]
fn completion_response_can_populate_a_builder_without_create_response_state() {
    let sdk =
        aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput::builder()
            .bucket("bucket")
            .key("key")
            .location("location")
            .checksum_md5("checksum")
            .build();
    let borrowed = model::builders::UploadOutputBuilder::from(&sdk);
    let owned = model::builders::UploadOutputBuilder::from(sdk);
    for builder in [borrowed, owned] {
        assert_eq!(builder.get_bucket().as_deref(), Some("bucket"));
        assert_eq!(builder.get_key().as_deref(), Some("key"));
        assert_eq!(builder.get_location().as_deref(), Some("location"));
        assert_eq!(builder.get_checksum_md5().as_deref(), Some("checksum"));
        assert_eq!(builder.get_upload_id(), &None);
        assert_eq!(builder.get_abort_date(), &None);
        assert!(builder.build().is_err());
    }
}

#[test]
fn borrowed_get_metadata_preserves_the_body_and_owned_conversion_preserves_values() {
    let sdk = aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .content_length(7)
        .content_range("bytes 0-6/42")
        .delete_marker(false)
        .bucket_key_enabled(false)
        .parts_count(0)
        .expires_string("invalid HTTP date")
        .set_metadata(Some(Default::default()))
        .checksum_md5("md5")
        .checksum_sha512("sha512")
        .storage_class(aws_sdk_s3::types::StorageClass::from("FUTURE_STORAGE"))
        .ssekms_key_id("kms-key")
        .build();
    let object = model::ObjectMetadata::from(&sdk);
    let chunk = model::ChunkMetadata::from(&sdk);
    assert_eq!(sdk.body.bytes(), Some(&b"payload"[..]));
    assert_eq!(chunk.content_length(), Some(7));
    assert_eq!(chunk.content_range(), Some("bytes 0-6/42"));
    assert_eq!(chunk.expires_string(), Some("invalid HTTP date"));
    assert_eq!(chunk.checksum_md5(), Some("md5"));
    assert_eq!(chunk.checksum_sha512(), Some("sha512"));
    let owned = model::ObjectMetadata::from(sdk);
    assert_eq!(object, owned);
    assert_eq!(owned.delete_marker(), Some(false));
    assert_eq!(owned.bucket_key_enabled(), Some(false));
    assert_eq!(owned.parts_count(), Some(0));
    assert_eq!(owned.metadata, Some(Default::default()));
    assert_eq!(owned.storage_class().unwrap().as_str(), "FUTURE_STORAGE");
    assert_eq!(owned.ssekms_key_id(), Some("kms-key"));
}

#[test]
fn owned_get_chunk_metadata_and_head_metadata_support_the_same_value_conversions() {
    let get = aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .content_length(7)
        .last_modified(DateTime::from_secs(123))
        .checksum_xxhash128("checksum")
        .build();
    let borrowed = model::ChunkMetadata::from(&get);
    let owned = model::ChunkMetadata::from(get);
    assert_eq!(borrowed, owned);
    assert_eq!(owned.last_modified(), Some(&DateTime::from_secs(123)));
    assert_eq!(owned.checksum_xxhash128(), Some("checksum"));
    let head = aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
        .archive_status(aws_sdk_s3::types::ArchiveStatus::from("FUTURE_ARCHIVE"))
        .expires_string("not a date")
        .delete_marker(false)
        .missing_meta(0)
        .checksum_xxhash128("head-checksum")
        .build();
    let borrowed = model::ObjectMetadata::from(&head);
    let owned = model::ObjectMetadata::from(head);
    assert_eq!(borrowed, owned);
    assert_eq!(owned.archive_status().unwrap().as_str(), "FUTURE_ARCHIVE");
    assert_eq!(owned.expires_string(), Some("not a date"));
    assert_eq!(owned.delete_marker(), Some(false));
    assert_eq!(owned.missing_meta(), Some(0));
    assert_eq!(owned.checksum_xxhash128(), Some("head-checksum"));
}

#[test]
fn sdk_request_identifier_traits_agree_with_inherent_metadata_getters() {
    fn check<T: aws_sdk_s3::operation::RequestId + aws_sdk_s3::operation::RequestIdExt>(
        metadata: &T,
    ) {
        assert_eq!(metadata.request_id(), None);
        assert_eq!(metadata.extended_request_id(), None);
    }
    let object = model::ObjectMetadata::builder().build();
    let chunk = model::ChunkMetadata::builder().build();
    check(&object);
    check(&chunk);
    assert_eq!(
        aws_sdk_s3::operation::RequestId::request_id(&object),
        object.request_id()
    );
    assert_eq!(
        aws_sdk_s3::operation::RequestIdExt::extended_request_id(&object),
        object.extended_request_id()
    );
    assert_eq!(
        aws_sdk_s3::operation::RequestId::request_id(&chunk),
        chunk.request_id()
    );
    assert_eq!(
        aws_sdk_s3::operation::RequestIdExt::extended_request_id(&chunk),
        chunk.extended_request_id()
    );
}
