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
