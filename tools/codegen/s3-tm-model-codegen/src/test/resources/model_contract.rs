// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use aws_smithy_types::DateTime;
use s3_tm_model::model::{
    builders::DownloadInputBuilder, ChecksumAlgorithm, ChecksumType, DownloadInput, Object,
    ObjectMetadata, ObjectStorageClass, Owner, RestoreStatus, StorageClass,
};

#[test]
fn enums_preserve_known_and_unknown_wire_values() {
    assert_eq!(ChecksumAlgorithm::from("CRC32"), ChecksumAlgorithm::Crc32);
    let future = ChecksumAlgorithm::from("FUTURE_CHECKSUM");
    assert_eq!(future.as_str(), "FUTURE_CHECKSUM");
    assert_eq!(future.to_string(), "FUTURE_CHECKSUM");
    assert_eq!(
        "FUTURE_CHECKSUM".parse::<ChecksumAlgorithm>().unwrap(),
        future
    );
    let _: s3_tm_model::model::error::UnknownVariantError =
        ChecksumAlgorithm::try_parse("FUTURE_CHECKSUM").unwrap_err();
    assert_eq!(ChecksumType::from("FULL_OBJECT").as_str(), "FULL_OBJECT");
    assert_eq!(ObjectStorageClass::from("GLACIER").as_str(), "GLACIER");
}

#[test]
fn nested_values_have_optional_fields_and_collection_builders() {
    let empty = Object::builder().build();
    assert!(empty.key().is_none());
    assert!(empty.checksum_algorithm().is_empty());
    assert!(empty.checksum_algorithm.is_none());
    let time = DateTime::from_secs(123);
    let object = Object::builder()
        .key("key")
        .last_modified(time)
        .owner(Owner::builder().id("owner").build())
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .restore_status(RestoreStatus::builder().restore_expiry_date(time).build())
        .build();
    assert_eq!(object.last_modified(), Some(&time));
    assert_eq!(object.owner().unwrap().id(), Some("owner"));
    assert_eq!(object.checksum_algorithm(), &[ChecksumAlgorithm::Crc32]);
    assert_eq!(
        object.restore_status().unwrap().restore_expiry_date(),
        Some(&time)
    );
    assert!(!RestoreStatus::builder().build().is_restore_in_progress());
    assert!(Object::builder()
        .set_checksum_algorithm(None)
        .build()
        .checksum_algorithm
        .is_none());
}

#[test]
fn download_builder_validates_required_fields_and_redacts_sensitive_values() {
    let _: DownloadInputBuilder = DownloadInput::builder();
    assert!(DownloadInput::builder().build().is_err());
    assert!(DownloadInput::builder().bucket("bucket").build().is_err());
    assert!(DownloadInput::builder().key("key").build().is_err());
    let input = DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .sse_customer_key("secret-customer-key")
        .inherited_option("from-mixin")
        .if_modified_since(DateTime::from_secs(123))
        .build()
        .unwrap();
    assert_eq!(input.bucket(), Some("bucket"));
    assert_eq!(input.key(), Some("key"));
    assert_eq!(input.inherited_option(), Some("from-mixin"));
    // Client input defaults may be left unset for the service to supply.
    assert_eq!(input.part_number(), None);
    assert_eq!(
        DownloadInput::builder()
            .bucket("bucket")
            .key("key")
            .part_number(3)
            .build()
            .unwrap()
            .part_number(),
        Some(3)
    );
    assert_eq!(input.sse_customer_key(), Some("secret-customer-key"));
    assert!(DownloadInput::builder()
        .bucket("bucket")
        .key("key")
        .set_key(None)
        .build()
        .is_err());
    assert!(!format!("{input:?}").contains("secret-customer-key"));
    let builder = DownloadInput::builder().sse_customer_key("secret-builder-key");
    assert!(!format!("{builder:?}").contains("secret-builder-key"));
}

#[test]
fn metadata_has_maps_timestamps_and_no_response_body() {
    let time = DateTime::from_secs(123);
    let output = ObjectMetadata::builder()
        .last_modified(time)
        .metadata("user-key", "value")
        .content_length(42)
        .get_only("GET metadata")
        .head_only("HEAD metadata")
        .storage_class(StorageClass::Standard)
        .build();
    assert_eq!(
        output
            .metadata()
            .unwrap()
            .get("user-key")
            .map(String::as_str),
        Some("value")
    );
    assert_eq!(output.last_modified(), Some(&time));
    assert_eq!(output.content_length(), Some(42));
    assert_eq!(output.get_only(), Some("GET metadata"));
    assert_eq!(output.head_only(), Some("HEAD metadata"));
    assert_eq!(output.storage_class(), Some(&StorageClass::Standard));
}
