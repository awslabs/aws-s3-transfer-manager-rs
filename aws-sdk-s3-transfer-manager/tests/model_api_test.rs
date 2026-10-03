/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3_transfer_manager::{
    io::InputStream,
    model::{
        builders::{DownloadInputBuilder, UploadInputBuilder},
        ChecksumAlgorithm, ChunkMetadata, DownloadInput, Object, ObjectMetadata, Owner,
        RestoreStatus, UploadInput,
    },
    operation::upload::ChecksumStrategy,
    types::{FailedMultipartUploadPolicy, ReadAhead},
};
use aws_smithy_types::DateTime;

#[test]
fn modeled_inputs_use_real_tm_runtime_types_and_validate_required_fields() {
    let _: DownloadInputBuilder = DownloadInput::builder();
    let _: UploadInputBuilder = UploadInput::builder();
    assert!(DownloadInput::builder().build().is_err());
    assert!(UploadInput::builder().bucket("bucket").build().is_err());
    let download = DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .read_ahead(ReadAhead::Parts(2))
        .build()
        .unwrap();
    assert_eq!(download.read_ahead(), Some(&ReadAhead::Parts(2)));
    let upload = UploadInput::builder()
        .bucket("bucket")
        .key("key")
        .body(InputStream::from_static(b"payload"))
        .checksum_strategy(ChecksumStrategy::default())
        .failed_multipart_upload_policy(FailedMultipartUploadPolicy::Retain)
        .build()
        .unwrap();
    assert_eq!(upload.body().size_hint().upper(), Some(7));
    assert!(upload.checksum_strategy().is_some());
    assert!(matches!(
        upload.failed_multipart_upload_policy(),
        Some(FailedMultipartUploadPolicy::Retain)
    ));
}

#[test]
fn expiration_types_preserve_upload_timestamps_and_raw_response_strings() {
    let time = DateTime::from_secs(123);
    let builder = UploadInput::builder().expires(time);
    assert_eq!(builder.get_expires(), &Some(time));
    let upload = builder.bucket("bucket").key("key").build().unwrap();
    let expires: Option<&DateTime> = upload.expires();
    assert_eq!(expires, Some(&time));
    let download = DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .response_expires(time)
        .build()
        .unwrap();
    assert_eq!(download.response_expires(), Some(&time));
    assert_eq!(
        ObjectMetadata::builder()
            .expires_string("invalid date")
            .build()
            .expires_string(),
        Some("invalid date")
    );
    assert_eq!(
        ChunkMetadata::builder()
            .expires_string("also invalid")
            .build()
            .expires_string(),
        Some("also invalid")
    );
}

#[test]
fn nested_values_preserve_absence_false_zero_and_unknown_strings() {
    assert_eq!(Object::builder().build().size(), None);
    assert_eq!(Object::builder().size(0).build().size(), Some(0));
    assert_eq!(
        Object::builder().size(i64::MAX).build().size(),
        Some(i64::MAX)
    );
    assert_eq!(
        RestoreStatus::builder().build().is_restore_in_progress(),
        None
    );
    assert_eq!(
        RestoreStatus::builder()
            .is_restore_in_progress(false)
            .build()
            .is_restore_in_progress(),
        Some(false)
    );
    let future = ChecksumAlgorithm::from("FUTURE_ALGORITHM");
    let object = Object::builder()
        .owner(Owner::builder().id("owner").build())
        .checksum_algorithm(future)
        .build();
    assert_eq!(object.owner().unwrap().id(), Some("owner"));
    assert_eq!(object.checksum_algorithm()[0].as_str(), "FUTURE_ALGORITHM");
}

#[test]
fn metadata_identifiers_are_accessible_without_sdk_traits() {
    let object = ObjectMetadata::builder().build();
    let chunk = ChunkMetadata::builder().build();
    assert_eq!(object.request_id(), None);
    assert_eq!(object.extended_request_id(), None);
    assert_eq!(chunk.request_id(), None);
    assert_eq!(chunk.extended_request_id(), None);
}

#[test]
fn sensitive_input_and_builder_debug_redacts_real_modeled_members() {
    let builder = UploadInput::builder().sse_kms_key_id("secret");
    assert!(!format!("{builder:?}").contains("secret"));
    let input = builder.bucket("bucket").key("key").build().unwrap();
    assert!(!format!("{input:?}").contains("secret"));
    let input = DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .sse_customer_key("secret")
        .build()
        .unwrap();
    assert!(!format!("{input:?}").contains("secret"));
}

#[test]
fn existing_operation_input_paths_remain_available() {
    let _: aws_sdk_s3_transfer_manager::operation::download::DownloadInput =
        aws_sdk_s3_transfer_manager::operation::download::DownloadInput::builder()
            .bucket("bucket")
            .key("key")
            .build()
            .unwrap();
    let _: aws_sdk_s3_transfer_manager::operation::upload::UploadInput =
        aws_sdk_s3_transfer_manager::operation::upload::UploadInput::builder()
            .bucket("bucket")
            .key("key")
            .build()
            .unwrap();
}
