// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in S3 behaviors: the bucket check.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::{FaultTarget, FaultType, Occurrence, Operation, S3MockServer};

/// With the bucket check, object requests for a missing bucket fail with NoSuchBucket and create
/// nothing, while a missing key in an existing bucket still fails with NoSuchKey.
#[tokio::test]
async fn test_bucket_check() -> Result {
    let f = Fixture::with(S3MockServer::builder().with_bucket_check(true)).await;
    f.put("a", "hello").await;
    let missing = "missing-bucket";

    let fail = failure(f.s3.get_object().bucket(missing).key("a").send().await);
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(
        f.s3.put_object()
            .bucket(missing)
            .key("a")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(
        f.s3.create_multipart_upload()
            .bucket(missing)
            .key("a")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(f.s3.delete_object().bucket(missing).key("a").send().await);
    assert_failure(&fail, 404, "NoSuchBucket");
    // HEAD errors have no body, so the SDK only sees a 404.
    let fail = failure(f.s3.head_object().bucket(missing).key("a").send().await);
    assert_eq!(fail.status, 404);
    // The failed writes did not create the bucket.
    let fail = failure(f.s3.head_bucket().bucket(missing).send().await);
    assert_eq!(fail.status, 404);
    // The bucket is checked after the request is counted and its fault drawn.
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::GetObject).bucket(missing),
        FaultType::ServiceError {
            status: 403,
            code: "AccessDenied",
        },
        0,
        Occurrence::Once,
    );
    let fail = failure(f.s3.get_object().bucket(missing).key("a").send().await);
    assert_failure(&fail, 403, "AccessDenied");

    // Missing keys in an existing bucket are still NoSuchKey.
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("b").send().await);
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}

/// Without the bucket check, a read from a missing bucket fails with NoSuchKey and a write
/// creates the bucket.
#[tokio::test]
async fn test_no_bucket_check_by_default() -> Result {
    let f = Fixture::new().await;
    let fail = failure(
        f.s3.get_object()
            .bucket("missing-bucket")
            .key("a")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    f.s3.put_object()
        .bucket("created-bucket")
        .key("a")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await?;
    assert!(f.server.object_exists("created-bucket", "a").await?);
    f.shutdown().await;
    Ok(())
}

/// With the bucket check, the requests of a multipart upload whose bucket was deleted fail with
/// NoSuchBucket, and completing the upload does not create the bucket again.
#[tokio::test]
async fn test_bucket_check_on_multipart_upload() -> Result {
    let f = Fixture::with(S3MockServer::builder().with_bucket_check(true)).await;
    f.s3.create_bucket().bucket("doomed").send().await?;
    let upload_id =
        f.s3.create_multipart_upload()
            .bucket("doomed")
            .key("a")
            .send()
            .await?
            .upload_id
            .unwrap();
    f.s3.delete_bucket().bucket("doomed").send().await?;

    let fail = failure(
        f.s3.upload_part()
            .bucket("doomed")
            .key("a")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(
        f.s3.complete_multipart_upload()
            .bucket("doomed")
            .key("a")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag("\"x\"")
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(
        f.s3.abort_multipart_upload()
            .bucket("doomed")
            .key("a")
            .upload_id(&upload_id)
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchBucket");
    let fail = failure(f.s3.head_bucket().bucket("doomed").send().await);
    assert_eq!(fail.status, 404);
    f.shutdown().await;
    Ok(())
}
