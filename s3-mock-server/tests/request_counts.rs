// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Per-operation request counts, observed through the SDK.

mod common;

use std::collections::BTreeMap;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::{FaultType, Occurrence, Operation};

/// Every request is counted under its operation, including one that fails with NoSuchKey; the
/// counts restart from zero after `reset_request_counts` and are kept by `reset`.
#[tokio::test]
async fn test_request_counts_per_operation() -> Result {
    let f = Fixture::new().await;
    assert!(f.server.request_counts().is_empty());

    f.put("a", "hello").await;
    for _ in 0..2 {
        f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    }
    for _ in 0..3 {
        f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    }
    f.s3.list_objects_v2().bucket(&f.bucket).send().await?;
    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");

    assert_eq!(f.server.request_count(Operation::GetObject), 3);
    assert_eq!(f.server.request_count(Operation::DeleteObject), 0);
    assert_eq!(
        f.server.request_counts(),
        BTreeMap::from([
            (Operation::GetObject, 3),
            (Operation::HeadObject, 3),
            (Operation::ListObjectsV2, 1),
            (Operation::PutObject, 1),
        ])
    );

    f.server.reset_request_counts();
    assert!(f.server.request_counts().is_empty());
    f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    f.server.reset().await?;
    assert_eq!(
        f.server.request_counts(),
        BTreeMap::from([(Operation::HeadObject, 1)])
    );
    f.shutdown().await;
    Ok(())
}

/// Multipart, delete and bucket operations each count under their own operation.
#[tokio::test]
async fn test_request_counts_multipart_and_bucket_operations() -> Result {
    let f = Fixture::new().await;
    let mpu =
        f.s3.create_multipart_upload()
            .bucket(&f.bucket)
            .key("mpu")
            .send()
            .await?;
    let upload_id = mpu.upload_id.unwrap();
    let part =
        f.s3.upload_part()
            .bucket(&f.bucket)
            .key("mpu")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"part"))
            .send()
            .await?;
    f.s3.complete_multipart_upload()
        .bucket(&f.bucket)
        .key("mpu")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag.unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await?;
    let aborted =
        f.s3.create_multipart_upload()
            .bucket(&f.bucket)
            .key("aborted")
            .send()
            .await?;
    f.s3.abort_multipart_upload()
        .bucket(&f.bucket)
        .key("aborted")
        .upload_id(aborted.upload_id.unwrap())
        .send()
        .await?;
    f.s3.delete_object()
        .bucket(&f.bucket)
        .key("mpu")
        .send()
        .await?;
    f.s3.create_bucket().bucket("other-bucket").send().await?;
    f.s3.head_bucket().bucket("other-bucket").send().await?;
    f.s3.list_buckets().send().await?;
    f.s3.delete_bucket().bucket("other-bucket").send().await?;

    assert_eq!(
        f.server.request_counts(),
        BTreeMap::from([
            (Operation::AbortMultipartUpload, 1),
            (Operation::CompleteMultipartUpload, 1),
            (Operation::CreateBucket, 1),
            (Operation::CreateMultipartUpload, 2),
            (Operation::DeleteBucket, 1),
            (Operation::DeleteObject, 1),
            (Operation::HeadBucket, 1),
            (Operation::ListBuckets, 1),
            (Operation::UploadPart, 1),
        ])
    );
    f.shutdown().await;
    Ok(())
}

/// Counts are per HTTP request: an SDK retry after a 500 counts as a second GetObject.
#[tokio::test]
async fn test_request_counts_include_retries() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server.reset_request_counts();
    f.server.insert_fault(
        &f.bucket,
        "a",
        FaultType::internal_error(),
        0,
        Occurrence::Once,
    );

    let retrying = f.handle.client().await;
    let body = retrying
        .get_object()
        .bucket(&f.bucket)
        .key("a")
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(&body[..], b"hello");
    assert_eq!(f.server.request_count(Operation::GetObject), 2);
    f.shutdown().await;
    Ok(())
}

/// A request shed by the rate throttle is still counted.
#[tokio::test]
async fn test_request_counts_include_throttled_requests() -> Result {
    let f = Fixture::new().await;
    // An empty bucket that never refills sheds every object request.
    f.server.set_rate_throttle(0.0, 0.0);
    let fail = failure(f.s3.head_object().bucket(&f.bucket).key("a").send().await);
    assert_eq!(fail.status, 503);
    assert_eq!(f.server.request_count(Operation::HeadObject), 1);
    f.shutdown().await;
    Ok(())
}
