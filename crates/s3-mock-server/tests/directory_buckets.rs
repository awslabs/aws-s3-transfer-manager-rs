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
/// EXPRESS_ONEZONE does in general purpose buckets.
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
/// NotImplemented and store nothing.
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

/// ListObjectsV2 in a directory bucket lists every key, in no particular order. It fails for the
/// parameters directory buckets do not support: with 400 InvalidRequest for a delimiter other than
/// `/` or a prefix that does not end in `/`, and with 501 NotSupported for `start-after`.
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

/// In a directory bucket, a range or partNumber read of part of a multipart object with a
/// full-object checksum, whose upload was created with a checksum algorithm, returns the part's
/// checksum. A read of the whole object returns the object's checksum.
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

/// Upload `parts` to `key` of `bucket` in a multipart upload created with `algorithm` (none if
/// `None`), each part with a CRC64NVME checksum if its flag is set.
async fn upload_mixed(
    s3: &Client,
    bucket: &str,
    key: &str,
    algorithm: Option<aws_sdk_s3::types::ChecksumAlgorithm>,
    parts: &[(&'static [u8], bool)],
) -> Result {
    use aws_sdk_s3::config::RequestChecksumCalculation;
    use aws_sdk_s3::types::ChecksumAlgorithm;
    let unchecked = Client::from_conf(
        s3.config()
            .to_builder()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build(),
    );
    let upload_id = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .set_checksum_algorithm(algorithm)
        .send()
        .await?
        .upload_id
        .unwrap();
    let mut completed = Vec::new();
    for (i, (part, with_checksum)) in parts.iter().enumerate() {
        let request = if *with_checksum {
            s3.upload_part()
                .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
        } else {
            unchecked.upload_part()
        };
        let out = request
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(i as i32 + 1)
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
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await?;
    Ok(())
}

/// In a directory bucket, an UploadPart without a checksum to an upload created with a checksum
/// algorithm fails with 400 InvalidRequest, so the parts of such an object all have checksums.
/// General purpose buckets accept the part.
#[tokio::test]
async fn test_part_without_checksum_fails_in_upload_with_algorithm() -> Result {
    use aws_sdk_s3::types::ChecksumAlgorithm;
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let parts: [(&'static [u8], bool); 2] = [(b"first part", true), (b"second", false)];
    let result = upload_mixed(
        &s3,
        DIRECTORY,
        "mpu",
        Some(ChecksumAlgorithm::Crc64Nvme),
        &parts,
    )
    .await;
    let error = result.expect_err("the part without a checksum should fail");
    let error = error
        .downcast::<aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::upload_part::UploadPartError>>()
        .expect("UploadPart fails");
    let fail = failure::<(), _>(Err(*error));
    assert_failure(&fail, 400, "InvalidRequest");
    assert_eq!(
        fail.message.as_deref(),
        Some(
            "Checksum Type mismatch occurred, expected checksum Type: crc64nvme, actual checksum \
             Type: None."
        )
    );
    upload_mixed(
        &s3,
        GENERAL_PURPOSE,
        "mpu",
        Some(ChecksumAlgorithm::Crc64Nvme),
        &parts,
    )
    .await?;
    Ok(())
}

/// A read of part of a multipart object whose upload was created without a checksum algorithm
/// returns no checksum, also when its parts were uploaded with checksums.
#[tokio::test]
async fn test_no_part_checksums_without_upload_algorithm() -> Result {
    use aws_sdk_s3::types::ChecksumMode;
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    upload_mixed(
        &s3,
        DIRECTORY,
        "mpu",
        None,
        &[(b"first part", true), (b"second", true)],
    )
    .await?;
    for part_number in [1, 2] {
        let head = s3
            .head_object()
            .bucket(DIRECTORY)
            .key("mpu")
            .part_number(part_number)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(head.checksum_crc64_nvme, None, "part {part_number}");
    }
    Ok(())
}

/// A client like `s3` that sends a checksum only when a request asks for one.
fn without_default_checksums(s3: &Client) -> Client {
    use aws_sdk_s3::config::RequestChecksumCalculation;
    Client::from_conf(
        s3.config()
            .to_builder()
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .build(),
    )
}

/// Create a multipart upload of `key` in `bucket` with `algorithm` and `checksum_type` (none if
/// `None`), and return its upload ID.
async fn create_upload(
    s3: &Client,
    bucket: &str,
    key: &str,
    algorithm: Option<aws_sdk_s3::types::ChecksumAlgorithm>,
    checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
) -> Result<String> {
    Ok(s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .set_checksum_algorithm(algorithm)
        .set_checksum_type(checksum_type)
        .send()
        .await?
        .upload_id
        .unwrap())
}

/// UploadPart reports:
/// - the part's checksum in the algorithm its upload was created with, computed if the part carried
///   none;
/// - in a general purpose bucket only, a checksum the part carried to an upload created without an
///   algorithm.
///
/// A part without a checksum to an upload created without an algorithm reports none.
#[tokio::test]
async fn test_upload_part_reported_checksums() -> Result {
    use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = without_default_checksums(&s3);
    // CRC32 of "data", base64.
    let crc32 = "rfPzYw==";
    for bucket in [GENERAL_PURPOSE, DIRECTORY] {
        let upload_id = create_upload(&s3, bucket, "mpu", None, None).await?;
        let part = |number: i32| {
            s3.upload_part()
                .bucket(bucket)
                .key("mpu")
                .upload_id(&upload_id)
                .part_number(number)
                .body(ByteStream::from_static(b"data"))
        };
        let sent = part(1).checksum_crc32(crc32).send().await?;
        let echoed = (bucket == GENERAL_PURPOSE).then_some(crc32);
        assert_eq!(sent.checksum_crc32.as_deref(), echoed, "{bucket}");
        let plain = part(2).send().await?;
        assert_eq!(plain.checksum_crc32, None, "{bucket}");
        assert_eq!(plain.checksum_crc64_nvme, None, "{bucket}");
    }
    let upload_id = create_upload(
        &s3,
        GENERAL_PURPOSE,
        "full",
        Some(ChecksumAlgorithm::Crc32),
        Some(ChecksumType::FullObject),
    )
    .await?;
    let part = s3
        .upload_part()
        .bucket(GENERAL_PURPOSE)
        .key("full")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await?;
    assert_eq!(part.checksum_crc32.as_deref(), Some(crc32));
    assert_eq!(part.checksum_crc64_nvme, None);
    Ok(())
}

/// In a general purpose bucket, an UploadPart without a checksum to an upload created with a
/// checksum algorithm and the COMPOSITE checksum type fails with 400 InvalidRequest.
#[tokio::test]
async fn test_part_without_checksum_fails_in_composite_upload() -> Result {
    use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = without_default_checksums(&s3);
    let upload_id = create_upload(
        &s3,
        GENERAL_PURPOSE,
        "mpu",
        Some(ChecksumAlgorithm::Crc32),
        Some(ChecksumType::Composite),
    )
    .await?;
    let result = s3
        .upload_part()
        .bucket(GENERAL_PURPOSE)
        .key("mpu")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await;
    let fail = failure(result);
    assert_failure(&fail, 400, "InvalidRequest");
    assert_eq!(
        fail.message.as_deref(),
        Some(
            "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum \
             Type: null"
        )
    );
    Ok(())
}

/// A CompleteMultipartUpload that lists a part checksum for an upload created without a checksum
/// algorithm fails with 400 InvalidPart in a general purpose bucket, and with 400 InvalidRequest in
/// a directory bucket. Listed without it, the part completes the upload.
#[tokio::test]
async fn test_complete_lists_part_checksum_of_upload_without_algorithm() -> Result {
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = without_default_checksums(&s3);
    for bucket in [GENERAL_PURPOSE, DIRECTORY] {
        let upload_id = create_upload(&s3, bucket, "mpu", None, None).await?;
        let part = s3
            .upload_part()
            .bucket(bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"data"))
            .checksum_crc32("rfPzYw==")
            .send()
            .await?;
        let complete = |checksum: Option<&str>| {
            let part = CompletedPart::builder()
                .part_number(1)
                .e_tag(part.e_tag.clone().unwrap())
                .set_checksum_crc32(checksum.map(str::to_owned))
                .build();
            s3.complete_multipart_upload()
                .bucket(bucket)
                .key("mpu")
                .upload_id(&upload_id)
                .multipart_upload(CompletedMultipartUpload::builder().parts(part).build())
                .send()
        };
        let fail = failure(complete(Some("rfPzYw==")).await);
        if bucket == GENERAL_PURPOSE {
            assert_failure(&fail, 400, "InvalidPart");
        } else {
            assert_failure(&fail, 400, "InvalidRequest");
            assert_eq!(
                fail.message.as_deref(),
                Some(
                    "Checksum Type mismatch occurred, expected checksum Type: None, actual \
                     checksum Type: crc32."
                )
            );
        }
        complete(None).await?;
    }
    Ok(())
}

/// In a general purpose bucket, a partNumber=1 read of a one-part multipart object with a
/// full-object checksum reports the object's checksum. A directory bucket reports none for an
/// upload created without a checksum algorithm.
#[tokio::test]
async fn test_part_number_read_of_one_part_object() -> Result {
    use aws_sdk_s3::types::{ChecksumMode, ChecksumType};
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = without_default_checksums(&s3);
    for bucket in [GENERAL_PURPOSE, DIRECTORY] {
        let upload_id = create_upload(&s3, bucket, "mpu", None, None).await?;
        let part = s3
            .upload_part()
            .bucket(bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await?;
        let completed = CompletedPart::builder()
            .part_number(1)
            .e_tag(part.e_tag.unwrap())
            .build();
        let object = s3
            .complete_multipart_upload()
            .bucket(bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .multipart_upload(CompletedMultipartUpload::builder().parts(completed).build())
            .send()
            .await?;
        let expected =
            (bucket == GENERAL_PURPOSE).then(|| object.checksum_crc64_nvme.clone().unwrap());
        let head = s3
            .head_object()
            .bucket(bucket)
            .key("mpu")
            .part_number(1)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(head.checksum_crc64_nvme, expected, "{bucket}");
        let get = s3
            .get_object()
            .bucket(bucket)
            .key("mpu")
            .part_number(1)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;
        assert_eq!(get.checksum_crc64_nvme, expected, "{bucket}");
        if bucket == GENERAL_PURPOSE {
            assert_eq!(get.checksum_type, Some(ChecksumType::FullObject));
        }
    }
    Ok(())
}

/// An UploadPart to an upload created with a checksum algorithm, with a checksum of another
/// algorithm only, fails with 400 InvalidRequest in either bucket type.
#[tokio::test]
async fn test_part_with_another_algorithm_fails() -> Result {
    use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
    let (_server, _handle, s3) = setup(S3MockServer::builder()).await;
    let s3 = without_default_checksums(&s3);
    for (bucket, period) in [(GENERAL_PURPOSE, ""), (DIRECTORY, ".")] {
        let upload_id = create_upload(
            &s3,
            bucket,
            "mpu",
            Some(ChecksumAlgorithm::Crc64Nvme),
            Some(ChecksumType::FullObject),
        )
        .await?;
        let result = s3
            .upload_part()
            .bucket(bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"data"))
            .checksum_crc32("rfPzYw==")
            .send()
            .await;
        let fail = failure(result);
        assert_failure(&fail, 400, "InvalidRequest");
        let message = format!(
            "Checksum Type mismatch occurred, expected checksum Type: crc64nvme, actual checksum \
             Type: crc32{period}"
        );
        assert_eq!(fail.message.as_deref(), Some(message.as_str()), "{bucket}");
    }
    Ok(())
}
