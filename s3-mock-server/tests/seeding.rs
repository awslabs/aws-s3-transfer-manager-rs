// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Seeding objects through `add_object_with` with an explicit ETag, storage class, restore
//! status and checksums, and inspecting multipart uploads in progress.

mod common;

use std::time::{Duration, SystemTime};

use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, StorageClass};
use common::{assert_failure, crc32, crc32c, crc64nvme, failure, md5_etag, Fixture, Result};
use s3_mock_server::{AddObjectRequest, MultipartUploadEntry, RestoreStatus};

/// A seeded ETag is stored quoted, reported by HeadObject, GetObject and ListObjectsV2, and used
/// by conditional requests.
#[tokio::test]
async fn test_seed_etag() -> Result {
    let f = Fixture::new().await;
    f.server
        .add_object_with(
            &f.bucket,
            "a",
            AddObjectRequest::new("hello").etag("custom-etag"),
        )
        .await?;
    f.server
        .add_object_with(
            &f.bucket,
            "b",
            AddObjectRequest::new("hello").etag("\"quoted\""),
        )
        .await?;

    let head = f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(head.e_tag.as_deref(), Some("\"custom-etag\""));
    let get = f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    assert_eq!(get.e_tag.as_deref(), Some("\"custom-etag\""));
    let list = f.s3.list_objects_v2().bucket(&f.bucket).send().await?;
    assert_eq!(list.contents()[0].e_tag(), Some("\"custom-etag\""));
    assert_eq!(list.contents()[1].e_tag(), Some("\"quoted\""));
    assert_eq!(
        f.server.get_object(&f.bucket, "b").await?.unwrap().etag,
        "\"quoted\""
    );

    // Conditions use the seeded ETag.
    f.s3.get_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match("\"custom-etag\"")
        .send()
        .await?;
    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match(md5_etag(b"hello"))
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    f.shutdown().await;
    Ok(())
}

/// Seeding stores checksums of the requested algorithms, none for an empty list, or CRC64NVME by
/// default, and the requested storage class; the SDK validates the seeded checksums on GET. An
/// unsupported algorithm fails without storing the object.
#[tokio::test]
async fn test_seed_checksum_algorithms_and_storage_class() -> Result {
    let f = Fixture::new().await;
    f.server
        .add_object_with(
            &f.bucket,
            "multi",
            AddObjectRequest::new("hello")
                .checksum_algorithms([ChecksumAlgorithm::Crc32, ChecksumAlgorithm::Crc32C])
                .storage_class(StorageClass::StandardIa),
        )
        .await?;
    f.server
        .add_object_with(
            &f.bucket,
            "none",
            AddObjectRequest::new("hello").checksum_algorithms([]),
        )
        .await?;
    f.server
        .add_object(&f.bucket, "default", "hello", None)
        .await?;

    let head = |key: &'static str| {
        f.s3.head_object()
            .bucket(&f.bucket)
            .key(key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
    };
    let multi = head("multi").await?;
    assert_eq!(
        multi.checksum_crc32.as_deref(),
        Some(crc32(b"hello").as_str())
    );
    assert_eq!(
        multi.checksum_crc32_c.as_deref(),
        Some(crc32c(b"hello").as_str())
    );
    assert_eq!(multi.checksum_crc64_nvme, None);
    assert_eq!(
        multi.storage_class.as_ref().map(|s| s.as_str()),
        Some("STANDARD_IA")
    );
    let none = head("none").await?;
    assert_eq!(none.checksum_crc32, None);
    assert_eq!(none.checksum_crc32_c, None);
    assert_eq!(none.checksum_crc64_nvme, None);
    assert_eq!(none.e_tag.as_deref(), Some(md5_etag(b"hello").as_str()));
    let default = head("default").await?;
    assert_eq!(
        default.checksum_crc64_nvme.as_deref(),
        Some(crc64nvme(b"hello").as_str())
    );
    assert_eq!(default.storage_class, None);

    // The SDK validates the seeded checksums on GET.
    for key in ["multi", "none", "default"] {
        let body =
            f.s3.get_object()
                .bucket(&f.bucket)
                .key(key)
                .send()
                .await?
                .body
                .collect()
                .await?
                .into_bytes();
        assert_eq!(&body[..], b"hello");
    }

    let data = f.server.get_object(&f.bucket, "multi").await?.unwrap();
    assert_eq!(data.storage_class.as_deref(), Some("STANDARD_IA"));
    assert_eq!(data.restore_status, None);

    assert!(matches!(
        f.server
            .add_object_with(
                &f.bucket,
                "bad",
                AddObjectRequest::new("hello")
                    .checksum_algorithms([ChecksumAlgorithm::from("MD4")]),
            )
            .await,
        Err(s3_mock_server::Error::InvalidConfiguration(_))
    ));
    assert!(matches!(
        f.server
            .add_object_with(
                &f.bucket,
                "bad",
                AddObjectRequest::new("hello").checksum_algorithms([ChecksumAlgorithm::Md5]),
            )
            .await,
        Err(s3_mock_server::Error::InvalidConfiguration(_))
    ));
    assert!(!f.exists("bad").await);
    f.shutdown().await;
    Ok(())
}

/// `list_multipart_uploads` reports the uploads in progress in a bucket, ordered by key, until
/// they are aborted.
#[tokio::test]
async fn test_list_multipart_uploads() -> Result {
    let f = Fixture::new().await;
    assert!(f.server.list_multipart_uploads(&f.bucket).await?.is_empty());
    let mut expected = Vec::new();
    for key in ["b", "a"] {
        let id =
            f.s3.create_multipart_upload()
                .bucket(&f.bucket)
                .key(key)
                .send()
                .await?
                .upload_id
                .unwrap();
        expected.push(MultipartUploadEntry {
            key: key.to_string(),
            upload_id: id,
        });
    }
    expected.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(f.server.list_multipart_uploads(&f.bucket).await?, expected);
    assert!(f.server.list_multipart_uploads("other").await?.is_empty());

    f.s3.abort_multipart_upload()
        .bucket(&f.bucket)
        .key("a")
        .upload_id(&expected[0].upload_id)
        .send()
        .await?;
    assert_eq!(
        f.server.list_multipart_uploads(&f.bucket).await?,
        expected[1..]
    );
    f.shutdown().await;
    Ok(())
}

/// A seeded restore status is reported in `x-amz-restore` and decides whether an archived object
/// can be read.
#[tokio::test]
async fn test_seed_restore_status() -> Result {
    let f = Fixture::new().await;
    f.server
        .add_object_with(
            &f.bucket,
            "in-progress",
            AddObjectRequest::new("cold")
                .storage_class(StorageClass::Glacier)
                .restore_status(RestoreStatus::InProgress),
        )
        .await?;
    let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000_000);
    f.server
        .add_object_with(
            &f.bucket,
            "restored",
            AddObjectRequest::new("cold")
                .storage_class(StorageClass::Glacier)
                .restore_status(RestoreStatus::Restored { expiry }),
        )
        .await?;

    let head =
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("in-progress")
            .send()
            .await?;
    assert_eq!(head.restore.as_deref(), Some(r#"ongoing-request="true""#));
    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("in-progress")
            .send()
            .await,
    );
    assert_failure(&fail, 403, "InvalidObjectState");
    f.s3.get_object()
        .bucket(&f.bucket)
        .key("restored")
        .send()
        .await?;
    assert_eq!(
        f.server
            .get_object(&f.bucket, "restored")
            .await?
            .unwrap()
            .restore_status,
        Some(RestoreStatus::Restored { expiry })
    );
    f.shutdown().await;
    Ok(())
}
