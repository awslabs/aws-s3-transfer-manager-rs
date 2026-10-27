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
