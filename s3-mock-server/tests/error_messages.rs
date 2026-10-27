// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Error messages: the server answers common errors with S3's messages.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::S3MockServer;

/// NoSuchKey, NoSuchBucket, NoSuchUpload, InvalidRange and PreconditionFailed carry S3's
/// messages.
#[tokio::test]
async fn test_common_error_messages() -> Result {
    let f = Fixture::with(S3MockServer::builder().with_bucket_check(true)).await;
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
