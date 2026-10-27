// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Directory buckets: the responses that differ from general purpose buckets.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, ObjectStorageClass, StorageClass,
};
use aws_sdk_s3::Client;
use common::{assert_failure, failure, no_retry, Result};
use s3_mock_server::{S3MockServer, ServerHandle};

const DIRECTORY: &str = "bucket--usw2-az1--x-s3";
const GENERAL_PURPOSE: &str = "bucket";

async fn setup(
    builder: s3_mock_server::S3MockServerBuilder,
) -> (S3MockServer, ServerHandle, Client) {
    let server = builder
        .with_in_memory_store()
        .build()
        .expect("build server");
    for bucket in [DIRECTORY, GENERAL_PURPOSE] {
        server.create_bucket(bucket).await.expect("create bucket");
    }
    let handle = server.start().await.expect("start server");
    let s3 = no_retry(&handle.client().await);
    (server, handle, s3)
}

async fn put(s3: &Client, bucket: &str, key: &str) {
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .expect("put object");
}

/// Objects in a directory bucket are EXPRESS_ONEZONE: GetObject and HeadObject report it, as do
/// listings. Other storage classes fail with 400 InvalidStorageClass in directory buckets, and
/// EXPRESS_ONEZONE does in general purpose buckets, as in S3.
#[tokio::test]
async fn test_storage_classes() -> Result {
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    put(&s3, DIRECTORY, "a").await;
    let express = Some(StorageClass::ExpressOnezone);
    let head = s3.head_object().bucket(DIRECTORY).key("a").send().await?;
    assert_eq!(head.storage_class, express);
    let get = s3.get_object().bucket(DIRECTORY).key("a").send().await?;
    assert_eq!(get.storage_class, express);
    let listed = s3.list_objects_v2().bucket(DIRECTORY).send().await?;
    assert_eq!(
        listed.contents()[0].storage_class,
        Some(ObjectStorageClass::ExpressOnezone)
    );
    s3.put_object()
        .bucket(DIRECTORY)
        .key("b")
        .storage_class(StorageClass::ExpressOnezone)
        .body(ByteStream::from_static(b"x"))
        .send()
        .await?;

    for class in [StorageClass::Standard, StorageClass::Glacier] {
        let fail = failure(
            s3.put_object()
                .bucket(DIRECTORY)
                .key("c")
                .storage_class(class.clone())
                .body(ByteStream::from_static(b"x"))
                .send()
                .await,
        );
        assert_failure(&fail, 400, "InvalidStorageClass");
        let fail = failure(
            s3.create_multipart_upload()
                .bucket(DIRECTORY)
                .key("c")
                .storage_class(class)
                .send()
                .await,
        );
        assert_failure(&fail, 400, "InvalidStorageClass");
    }
    let fail = failure(
        s3.put_object()
            .bucket(GENERAL_PURPOSE)
            .key("c")
            .storage_class(StorageClass::ExpressOnezone)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidStorageClass");
    Ok(())
}

/// Directory buckets do not support Content-MD5: PutObject and UploadPart with it fail with 501
/// NotImplemented, as in S3, and store nothing.
#[tokio::test]
async fn test_content_md5_not_implemented() -> Result {
    use base64::Engine;
    let (server, _handle, s3) = setup(S3MockServer::builder()).await;
    let md5 = base64::engine::general_purpose::STANDARD.encode(md5::compute(b"hello").0);
    let fail = failure(
        s3.put_object()
            .bucket(DIRECTORY)
            .key("a")
            .content_md5(&md5)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    assert!(!server.object_exists(DIRECTORY, "a").await?);
    let upload_id = s3
        .create_multipart_upload()
        .bucket(DIRECTORY)
        .key("a")
        .send()
        .await?
        .upload_id
        .unwrap();
    let fail = failure(
        s3.upload_part()
            .bucket(DIRECTORY)
            .key("a")
            .upload_id(upload_id)
            .part_number(1)
            .content_md5(&md5)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    Ok(())
}

/// CompleteMultipartUpload with a part listed twice fails with 400 InvalidPartOrder, with the
/// message of the bucket's type.
#[tokio::test]
async fn test_invalid_part_order_message() -> Result {
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    for (bucket, message) in [
        (
            DIRECTORY,
            "The list of parts was not in ascending order or did not have consecutive part \
             numbers. The parts list must be specified in order by part number, and must not \
             have any gaps.",
        ),
        (
            GENERAL_PURPOSE,
            "The list of parts was not in ascending order. Parts must be ordered by part number.",
        ),
    ] {
        let upload_id = s3
            .create_multipart_upload()
            .bucket(bucket)
            .key("a")
            .send()
            .await?
            .upload_id
            .unwrap();
        let etag = s3
            .upload_part()
            .bucket(bucket)
            .key("a")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"hello"))
            .send()
            .await?
            .e_tag
            .unwrap();
        let parts = [1, 1]
            .map(|number| {
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(&etag)
                    .build()
            })
            .to_vec();
        let fail = failure(
            s3.complete_multipart_upload()
                .bucket(bucket)
                .key("a")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
                .send()
                .await,
        );
        assert_failure(&fail, 400, "InvalidPartOrder");
        assert_eq!(fail.message.as_deref(), Some(message));
    }
    Ok(())
}

/// Directory buckets serve RenameObject and appends; general purpose buckets fail them with 501,
/// as in S3.
#[tokio::test]
async fn test_renames_and_appends() -> Result {
    let (server, _handle, s3) = setup(S3MockServer::builder()).await;
    put(&s3, DIRECTORY, "a").await;
    s3.rename_object()
        .bucket(DIRECTORY)
        .key("b")
        .rename_source("a")
        .send()
        .await?;
    s3.put_object()
        .bucket(DIRECTORY)
        .key("b")
        .write_offset_bytes(5)
        .body(ByteStream::from_static(b" world"))
        .send()
        .await?;
    let data = server.get_object(DIRECTORY, "b").await?.unwrap();
    assert_eq!(&data.body[..], b"hello world");

    put(&s3, GENERAL_PURPOSE, "a").await;
    let fail = failure(
        s3.rename_object()
            .bucket(GENERAL_PURPOSE)
            .key("b")
            .rename_source("a")
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    let fail = failure(
        s3.put_object()
            .bucket(GENERAL_PURPOSE)
            .key("a")
            .write_offset_bytes(5)
            .body(ByteStream::from_static(b" world"))
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    Ok(())
}

/// ListObjectsV2 in a directory bucket lists every key, in no particular order, and fails as S3
/// does for the parameters directory buckets do not support: a delimiter other than `/` or a
/// prefix that does not end in `/` with 400 InvalidRequest, `start-after` with 501 NotSupported.
#[tokio::test]
async fn test_listing() -> Result {
    let (server, _handle, s3) = setup(S3MockServer::builder()).await;
    let keys: Vec<String> = (0..20).map(|i| format!("dir/key-{i:02}")).collect();
    for key in &keys {
        server.add_object(DIRECTORY, key, "x", None).await?;
    }
    let listed: Vec<String> = s3
        .list_objects_v2()
        .bucket(DIRECTORY)
        .prefix("dir/")
        .send()
        .await?
        .contents()
        .iter()
        .map(|object| object.key().unwrap().to_owned())
        .collect();
    assert_ne!(listed, keys, "keys should not be in lexicographic order");
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(sorted, keys);
    s3.list_objects_v2()
        .bucket(DIRECTORY)
        .delimiter("/")
        .send()
        .await?;

    let fail = failure(
        s3.list_objects_v2()
            .bucket(DIRECTORY)
            .prefix("dir/key")
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidRequest");
    let fail = failure(
        s3.list_objects_v2()
            .bucket(DIRECTORY)
            .prefix("dir/")
            .delimiter("-")
            .send()
            .await,
    );
    assert_failure(&fail, 400, "InvalidRequest");
    assert_eq!(
        fail.message.as_deref(),
        Some("This bucket only supports '/' as a delimiter")
    );
    let fail = failure(
        s3.list_objects_v2()
            .bucket(DIRECTORY)
            .start_after("dir/key-01")
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotSupported");

    // General purpose buckets list in order, and accept all three.
    server
        .add_object(GENERAL_PURPOSE, "dir/key", "x", None)
        .await?;
    s3.list_objects_v2()
        .bucket(GENERAL_PURPOSE)
        .prefix("dir/k")
        .delimiter("-")
        .start_after("a")
        .send()
        .await?;
    Ok(())
}

/// A range or partNumber read of part of a multipart object with a full-object checksum, uploaded
/// with part checksums, returns the part's checksum in a directory bucket, as S3 does (general
/// purpose buckets return none); a read of the whole object returns the object's checksum.
#[tokio::test]
async fn test_part_checksums_of_full_object_multipart_objects() -> Result {
    use aws_sdk_s3::types::{
        ChecksumAlgorithm, ChecksumMode, ChecksumType, CompletedMultipartUpload, CompletedPart,
    };
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let parts: [&'static [u8]; 2] = [b"first part", b"second"];
    let upload_id = s3
        .create_multipart_upload()
        .bucket(DIRECTORY)
        .key("mpu")
        .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        .checksum_type(ChecksumType::FullObject)
        .send()
        .await?
        .upload_id
        .unwrap();
    let mut completed = Vec::new();
    for (i, part) in parts.iter().enumerate() {
        let out = s3
            .upload_part()
            .bucket(DIRECTORY)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(i as i32 + 1)
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .body(ByteStream::from_static(part))
            .send()
            .await?;
        completed.push(
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(out.e_tag.unwrap())
                .set_checksum_crc64_nvme(out.checksum_crc64_nvme)
                .build(),
        );
    }
    s3.complete_multipart_upload()
        .bucket(DIRECTORY)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    let part = s3
        .get_object()
        .bucket(DIRECTORY)
        .key("mpu")
        .range(format!("bytes=0-{}", parts[0].len() - 1))
        .send()
        .await?;
    assert_eq!(
        part.checksum_crc64_nvme.as_deref(),
        Some(common::crc64nvme(parts[0]).as_str())
    );
    let head = s3
        .head_object()
        .bucket(DIRECTORY)
        .key("mpu")
        .part_number(1)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    assert_eq!(
        head.checksum_crc64_nvme.as_deref(),
        Some(common::crc64nvme(parts[0]).as_str())
    );
    let whole = s3.get_object().bucket(DIRECTORY).key("mpu").send().await?;
    assert!(whole.checksum_crc64_nvme.is_some());
    Ok(())
}

/// A read of part of a multipart object whose parts were uploaded without checksums returns no
/// checksum, as in S3, although the object has a full-object checksum.
#[tokio::test]
async fn test_no_part_checksums_for_parts_uploaded_without_them() -> Result {
    use aws_sdk_s3::config::RequestChecksumCalculation;
    use aws_sdk_s3::types::{ChecksumMode, CompletedMultipartUpload, CompletedPart};
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = aws_sdk_s3::Client::from_conf(
        s3.config()
            .to_builder()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build(),
    );
    let upload_id = s3
        .create_multipart_upload()
        .bucket(DIRECTORY)
        .key("mpu")
        .send()
        .await?
        .upload_id
        .unwrap();
    let mut completed = Vec::new();
    for (i, part) in [&b"first part"[..], b"second"].into_iter().enumerate() {
        let out = s3
            .upload_part()
            .bucket(DIRECTORY)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(i as i32 + 1)
            .body(ByteStream::from_static(part))
            .send()
            .await?;
        completed.push(
            CompletedPart::builder()
                .part_number(i as i32 + 1)
                .e_tag(out.e_tag.unwrap())
                .build(),
        );
    }
    s3.complete_multipart_upload()
        .bucket(DIRECTORY)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    let head = |part_number: Option<i32>| {
        s3.head_object()
            .bucket(DIRECTORY)
            .key("mpu")
            .set_part_number(part_number)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
    };
    assert_eq!(head(Some(1)).await?.checksum_crc64_nvme, None);
    assert!(head(None).await?.checksum_crc64_nvme.is_some());
    Ok(())
}
