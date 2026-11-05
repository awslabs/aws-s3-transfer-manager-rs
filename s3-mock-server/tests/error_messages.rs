// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Error messages: the server answers common errors with S3's messages.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart};
use common::{assert_failure, failure, no_retry, Fixture, Result};
use s3_mock_server::S3MockServer;

/// NoSuchKey, NoSuchBucket, NoSuchUpload, InvalidRange and PreconditionFailed carry S3's
/// messages.
#[tokio::test]
async fn test_common_error_messages() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    let get = || f.s3.get_object().bucket(&f.bucket).key("a");

    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    assert_eq!(
        fail.message.as_deref(),
        Some("The specified key does not exist.")
    );

    let fail = failure(f.s3.delete_object().bucket("missing").key("a").send().await);
    assert_failure(&fail, 404, "NoSuchBucket");
    assert_eq!(
        fail.message.as_deref(),
        Some("The specified bucket does not exist")
    );

    let fail = failure(
        f.s3.upload_part()
            .bucket(&f.bucket)
            .key("a")
            .upload_id("missing")
            .part_number(1)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchUpload");
    assert_eq!(
        fail.message.as_deref(),
        Some("The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.")
    );

    let fail = failure(get().range("bytes=100-200").send().await);
    assert_failure(&fail, 416, "InvalidRange");
    assert_eq!(
        fail.message.as_deref(),
        Some("The requested range is not satisfiable")
    );

    let fail = failure(get().if_match("\"other\"").send().await);
    assert_failure(&fail, 412, "PreconditionFailed");
    assert_eq!(
        fail.message.as_deref(),
        Some("At least one of the pre-conditions you specified did not hold")
    );

    f.shutdown().await;
    Ok(())
}

/// CompleteMultipartUpload's errors carry S3's codes and messages, in both backends: a part
/// with another ETag or that was not uploaded fails with InvalidPart, a part listed twice with
/// InvalidPartOrder, a part without a number or ETag with MalformedXML,
/// and a checksum that does not match with BadDigest, whose message names the algorithm in
/// lowercase.
#[tokio::test]
async fn test_complete_multipart_upload_error_messages() -> Result {
    let temp_dir = tempfile::tempdir()?;
    for builder in [
        S3MockServer::builder().with_in_memory_store(),
        S3MockServer::builder()
            .with_local_dir_store(temp_dir.path())
            .await?,
    ] {
        let server = builder.build()?;
        let handle = server.start().await?;
        let s3 = no_retry(&handle.client().await);
        let bucket = "bucket";
        server.create_bucket(bucket).await?;

        let upload_id = s3
            .create_multipart_upload()
            .bucket(bucket)
            .key("a")
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .checksum_type(ChecksumType::FullObject)
            .send()
            .await?
            .upload_id
            .expect("upload ID");
        let etag = s3
            .upload_part()
            .bucket(bucket)
            .key("a")
            .upload_id(&upload_id)
            .part_number(1)
            .checksum_algorithm(ChecksumAlgorithm::Crc32)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await?
            .e_tag
            .expect("ETag");
        let part = |number: i32, etag: &str| {
            CompletedPart::builder()
                .part_number(number)
                .e_tag(etag)
                .build()
        };
        let complete = |parts: Vec<CompletedPart>| {
            s3.complete_multipart_upload()
                .bucket(bucket)
                .key("a")
                .upload_id(&upload_id)
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(parts))
                        .build(),
                )
        };

        for parts in [
            vec![part(1, "\"other\"")],
            vec![part(1, &etag), part(2, &etag)],
        ] {
            let fail = failure(complete(parts).send().await);
            assert_failure(&fail, 400, "InvalidPart");
            assert_eq!(
                fail.message.as_deref(),
                Some("One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.")
            );
        }

        let fail = failure(complete(vec![part(1, &etag), part(1, &etag)]).send().await);
        assert_failure(&fail, 400, "InvalidPartOrder");
        assert_eq!(
            fail.message.as_deref(),
            Some("The list of parts was not in ascending order. Parts must be ordered by part number.")
        );

        for part in [
            CompletedPart::builder().e_tag(&etag).build(),
            CompletedPart::builder().part_number(1).build(),
        ] {
            let fail = failure(complete(vec![part]).send().await);
            assert_failure(&fail, 400, "MalformedXML");
            assert_eq!(
                fail.message.as_deref(),
                Some("The XML you provided was not well-formed or did not validate against our published schema")
            );
        }

        let fail = failure(
            complete(vec![part(1, &etag)])
                .checksum_crc32("AAAAAA==")
                .checksum_type(ChecksumType::FullObject)
                .send()
                .await,
        );
        assert_failure(&fail, 400, "BadDigest");
        assert_eq!(
            fail.message.as_deref(),
            Some("The crc32 you specified did not match the calculated checksum.")
        );

        handle.shutdown().await?;
    }
    Ok(())
}

/// A request the throttle sheds fails with 503 SlowDown and S3's message.
#[tokio::test]
async fn test_throttled_request_error_message() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    // A throttle without burst allowance sheds every request.
    f.server.set_rate_throttle(1.0, 0.0);

    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 503, "SlowDown");
    assert_eq!(
        fail.message.as_deref(),
        Some("Please reduce your request rate.")
    );

    f.shutdown().await;
    Ok(())
}

/// A partNumber GetObject of a part the object does not have fails with 416 InvalidPartNumber
/// and S3's message.
#[tokio::test]
async fn test_missing_part_number_error_message() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;

    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("a")
            .part_number(2)
            .send()
            .await,
    );
    assert_failure(&fail, 416, "InvalidPartNumber");
    assert_eq!(
        fail.message.as_deref(),
        Some("The requested partnumber is not satisfiable")
    );

    f.shutdown().await;
    Ok(())
}
