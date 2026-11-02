// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Conditional requests: `If-Match` on GetObject, HeadObject and DeleteObject, and `If-Match` and
//! `If-None-Match: *` on PutObject.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use common::{assert_failure, failure, md5_etag, Fixture, Result};

/// GetObject and HeadObject with `If-Match` are served when the ETag matches, quoted or not, or
/// is `*`, fail with 412 when it does not, and fail with NoSuchKey for a missing key.
#[tokio::test]
async fn test_if_match_get_and_head() -> Result {
    let f = Fixture::new().await;
    let etag = f.put("a", "hello").await;

    f.s3.get_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match(&etag)
        .send()
        .await?;
    f.s3.get_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match(etag.trim_matches('"'))
        .send()
        .await?;
    f.s3.get_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match("*")
        .send()
        .await?;
    f.s3.head_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match(&etag)
        .send()
        .await?;

    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match("\"nope\"")
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    let fail = failure(
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match("\"nope\"")
            .send()
            .await,
    );
    assert_eq!(fail.status, 412);

    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("missing")
            .if_match(&etag)
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}

/// PutObject with `If-Match` replaces the object only while its ETag matches, and fails with
/// NoSuchKey, without creating the key, when the key does not exist.
#[tokio::test]
async fn test_if_match_put() -> Result {
    let f = Fixture::new().await;
    let etag = f.put("a", "v1").await;

    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match("\"nope\"")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    assert_eq!(f.content("a").await, b"v1");

    let new_etag =
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match(&etag)
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await?
            .e_tag
            .unwrap();
    assert_eq!(new_etag, md5_etag(b"v2"));
    assert_eq!(f.content("a").await, b"v2");

    // The old ETag no longer matches.
    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match(&etag)
            .body(ByteStream::from_static(b"v3"))
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");

    // If-Match on a missing key fails with NoSuchKey, as S3 does, and does not create it.
    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("missing")
            .if_match(&etag)
            .body(ByteStream::from_static(b"v1"))
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    assert!(!f.exists("missing").await);
    f.shutdown().await;
    Ok(())
}

/// PutObject with `If-None-Match: *` creates a missing key and fails with 412, leaving the object
/// unchanged, when the key exists.
#[tokio::test]
async fn test_if_none_match_put() -> Result {
    let f = Fixture::new().await;
    f.s3.put_object()
        .bucket(&f.bucket)
        .key("a")
        .if_none_match("*")
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await?;
    assert_eq!(f.content("a").await, b"v1");

    let fail = failure(
        f.s3.put_object()
            .bucket(&f.bucket)
            .key("a")
            .if_none_match("*")
            .body(ByteStream::from_static(b"v2"))
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    assert_eq!(f.content("a").await, b"v1");
    f.shutdown().await;
    Ok(())
}

/// DeleteObject with `If-Match` deletes the object only while its ETag matches, or for `*` if it
/// exists: a different ETag fails with 412 and keeps the object, and a missing key fails with
/// NoSuchKey, as in S3.
#[tokio::test]
async fn test_if_match_delete() -> Result {
    let f = Fixture::new().await;
    let etag = f.put("a", "hello").await;
    let fail = failure(
        f.s3.delete_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match("\"nope\"")
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");
    assert!(f.exists("a").await);

    f.s3.delete_object()
        .bucket(&f.bucket)
        .key("a")
        .if_match(&etag)
        .send()
        .await?;
    assert!(!f.exists("a").await);

    let fail = failure(
        f.s3.delete_object()
            .bucket(&f.bucket)
            .key("a")
            .if_match(&etag)
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");

    f.put("b", "hello").await;
    f.s3.delete_object()
        .bucket(&f.bucket)
        .key("b")
        .if_match("*")
        .send()
        .await?;
    assert!(!f.exists("b").await);
    let fail = failure(
        f.s3.delete_object()
            .bucket(&f.bucket)
            .key("b")
            .if_match("*")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}
