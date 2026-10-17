// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! RestoreObject and the archival storage classes that restrict reads.

mod common;

use std::time::{Duration, SystemTime};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{RestoreRequest, StorageClass};
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::{Operation, RestoreStatus};

/// Put `content` at `key` with a storage class.
async fn put_in_class(f: &Fixture, key: &str, content: &'static str, class: StorageClass) {
    f.s3.put_object()
        .bucket(&f.bucket)
        .key(key)
        .storage_class(class)
        .body(ByteStream::from_static(content.as_bytes()))
        .send()
        .await
        .expect("put object");
}

/// An object in GLACIER or DEEP_ARCHIVE without a restored copy reports its storage class on
/// HEAD and fails GetObject with 403 InvalidObjectState, the code clients branch on; a failing
/// condition is reported first. Other storage classes are readable.
#[tokio::test]
async fn test_archived_object() -> Result {
    let f = Fixture::new().await;
    for (key, class) in [
        ("glacier", StorageClass::Glacier),
        ("deep", StorageClass::DeepArchive),
    ] {
        put_in_class(&f, key, "cold", class.clone()).await;
        let head = f.s3.head_object().bucket(&f.bucket).key(key).send().await?;
        assert_eq!(head.storage_class, Some(class));
        assert_eq!(head.restore, None);

        let fail = failure(f.s3.get_object().bucket(&f.bucket).key(key).send().await);
        assert_failure(&fail, 403, "InvalidObjectState");
        // Conditions are evaluated before the storage class.
        let fail = failure(
            f.s3.get_object()
                .bucket(&f.bucket)
                .key(key)
                .if_match("\"other\"")
                .send()
                .await,
        );
        assert_failure(&fail, 412, "PreconditionFailed");
    }

    put_in_class(&f, "ia", "warm", StorageClass::StandardIa).await;
    f.s3.get_object().bucket(&f.bucket).key("ia").send().await?;
    f.shutdown().await;
    Ok(())
}

/// RestoreObject on an archived object makes it readable at once, with an `x-amz-restore` expiry
/// at the first midnight UTC `Days` from now, and a restored copy can be restored again. A restore
/// of an object in another storage class fails with 403 InvalidObjectState, and of a missing key
/// with NoSuchKey.
#[tokio::test]
async fn test_restore_object() -> Result {
    let f = Fixture::new().await;
    put_in_class(&f, "a", "cold", StorageClass::Glacier).await;
    let restore_a = || {
        f.s3.restore_object()
            .bucket(&f.bucket)
            .key("a")
            .restore_request(RestoreRequest::builder().days(2).build())
    };

    restore_a().send().await?;
    assert_eq!(f.server.request_count(Operation::RestoreObject), 1);

    let head = f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    let restore = head.restore.expect("x-amz-restore header");
    assert!(
        restore.starts_with(r#"ongoing-request="false", expiry-date=""#),
        "{restore}"
    );
    assert!(restore.ends_with(" GMT\""), "{restore}");
    assert_eq!(head.storage_class, Some(StorageClass::Glacier));

    let data = f.server.get_object(&f.bucket, "a").await?.unwrap();
    let Some(RestoreStatus::Restored { expiry }) = data.restore_status else {
        panic!("unexpected restore status {:?}", data.restore_status);
    };
    // The expiry is the first midnight UTC at least two days from now.
    let since_epoch = expiry.duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
    assert_eq!(since_epoch % 86_400, 0);
    let earliest = SystemTime::now() + Duration::from_secs(2 * 86_400);
    assert!(expiry >= earliest - Duration::from_secs(60), "{expiry:?}");
    assert!(
        expiry < earliest + Duration::from_secs(86_400),
        "{expiry:?}"
    );

    // GetObject of the restored copy reports the restore status as HeadObject does.
    let get = f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(get.restore.as_deref(), Some(restore.as_str()));
    let body = get.body.collect().await?.into_bytes();
    assert_eq!(&body[..], b"cold");

    // A restored copy can be restored again.
    restore_a().send().await?;

    f.put("standard", "warm").await;
    let fail = failure(
        f.s3.restore_object()
            .bucket(&f.bucket)
            .key("standard")
            .send()
            .await,
    );
    assert_failure(&fail, 403, "InvalidObjectState");
    assert_eq!(
        fail.message.as_deref(),
        Some("Restore is not allowed for the object's current storage class")
    );
    let fail = failure(
        f.s3.restore_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}

/// `set_restore_status` sets the `x-amz-restore` header and whether an archived object can be
/// read: in progress, restored until a given date, or not restored.
#[tokio::test]
async fn test_set_restore_status() -> Result {
    let f = Fixture::new().await;
    put_in_class(&f, "a", "cold", StorageClass::Glacier).await;

    f.server
        .set_restore_status(&f.bucket, "a", Some(RestoreStatus::InProgress))
        .await?;
    let head = f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(head.restore.as_deref(), Some(r#"ongoing-request="true""#));
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 403, "InvalidObjectState");

    let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000);
    f.server
        .set_restore_status(&f.bucket, "a", Some(RestoreStatus::Restored { expiry }))
        .await?;
    let head = f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(
        head.restore.as_deref(),
        Some(r#"ongoing-request="false", expiry-date="Wed, 18 May 2033 03:33:20 GMT""#)
    );
    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;

    f.server.set_restore_status(&f.bucket, "a", None).await?;
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 403, "InvalidObjectState");

    assert!(f
        .server
        .set_restore_status(&f.bucket, "missing", None)
        .await
        .is_err());
    f.shutdown().await;
    Ok(())
}

/// A restore of an object whose restore is in progress fails with 409 RestoreAlreadyInProgress
/// and leaves it unreadable.
#[tokio::test]
async fn test_restore_object_in_progress() -> Result {
    let f = Fixture::new().await;
    put_in_class(&f, "a", "cold", StorageClass::Glacier).await;
    f.server
        .set_restore_status(&f.bucket, "a", Some(RestoreStatus::InProgress))
        .await?;
    let fail = failure(
        f.s3.restore_object()
            .bucket(&f.bucket)
            .key("a")
            .send()
            .await,
    );
    assert_failure(&fail, 409, "RestoreAlreadyInProgress");
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 403, "InvalidObjectState");
    f.shutdown().await;
    Ok(())
}

/// GetObject and HeadObject omit the storage class of a STANDARD object, also when STANDARD was
/// requested explicitly, and report any other class, as S3 does.
#[tokio::test]
async fn test_storage_class_reporting() -> Result {
    let f = Fixture::new().await;
    f.put("default", "x").await;
    put_in_class(&f, "standard", "x", StorageClass::Standard).await;
    put_in_class(&f, "ia", "x", StorageClass::StandardIa).await;
    for (key, expected) in [
        ("default", None),
        ("standard", None),
        ("ia", Some(StorageClass::StandardIa)),
    ] {
        let head = f.s3.head_object().bucket(&f.bucket).key(key).send().await?;
        assert_eq!(head.storage_class, expected, "{key}");
        let get = f.s3.get_object().bucket(&f.bucket).key(key).send().await?;
        assert_eq!(get.storage_class, expected, "{key}");
    }
    f.shutdown().await;
    Ok(())
}
