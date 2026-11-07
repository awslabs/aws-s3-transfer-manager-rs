// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! RenameObject, served in directory buckets.

mod common;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;
use common::{assert_failure, condition, failure, Fixture, Result};
use s3_mock_server::{Error, FaultTarget, FaultType, Occurrence, Operation};

#[allow(clippy::result_large_err)]
async fn rename(
    f: &Fixture,
    src: &str,
    dst: &str,
) -> std::result::Result<
    aws_sdk_s3::operation::rename_object::RenameObjectOutput,
    SdkError<aws_sdk_s3::operation::rename_object::RenameObjectError, HttpResponse>,
> {
    f.s3.rename_object()
        .bucket(&f.bucket)
        .key(dst)
        .rename_source(src)
        .send()
        .await
}

/// A rename in a general purpose bucket fails with 501 NotImplemented, as in S3, and changes
/// nothing; the request is still counted.
#[tokio::test]
async fn test_rename_not_implemented_in_general_purpose_buckets() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    let fail = failure(rename(&f, "a", "b").await);
    assert_failure(&fail, 501, "NotImplemented");
    assert!(f.exists("a").await);
    assert!(!f.exists("b").await);
    assert_eq!(f.server.request_count(Operation::RenameObject), 1);
    f.shutdown().await;
    Ok(())
}

/// A rename retried with the client token of a rename that succeeded succeeds without renaming
/// again, although its source is gone; with other parameters it fails with 400
/// IdempotentParameterMismatch and renames nothing. A token longer than 64 characters, or with
/// characters outside ASCII 33 to 126, fails with 400 InvalidRequest. `reset` forgets the tokens.
#[tokio::test]
async fn test_rename_client_token() -> Result {
    let mut f = Fixture::directory().await;
    let rename_with_token = |src: &str, dst: &str, token: &str| {
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key(dst)
            .rename_source(src)
            .client_token(token)
            .send()
    };
    f.put("a", "hello").await;
    rename_with_token("a", "b", "token-1").await?;
    rename_with_token("a", "b", "token-1").await?;
    assert!(!f.exists("a").await);
    assert_eq!(f.content("b").await, b"hello");

    f.put("c", "other").await;
    let fail = failure(rename_with_token("c", "d", "token-1").await);
    assert_failure(&fail, 400, "IdempotentParameterMismatch");
    assert_eq!(
        fail.message.as_deref(),
        Some(
            "Parameters on this idempotent request are inconsistent with parameters used in \
             previous request(s)."
        )
    );
    assert!(f.exists("c").await);
    assert!(!f.exists("d").await);

    // With another token, the rename runs again and finds no source.
    let fail = failure(rename_with_token("a", "b", "token-2").await);
    assert_failure(&fail, 404, "NoSuchKey");

    // A token longer than 64 characters, or with characters outside ASCII 33 to 126, fails.
    for (token, message) in [
        (
            "t".repeat(65),
            "Invalid header: Value for x-amz-client-token header is too long.",
        ),
        (
            "a token".to_owned(),
            "Invalid header: Value for x-amz-client-token header is invalid.",
        ),
    ] {
        let fail = failure(rename_with_token("c", "d", &token).await);
        assert_failure(&fail, 400, "InvalidRequest");
        assert_eq!(fail.message.as_deref(), Some(message));
    }
    assert!(f.exists("c").await);
    rename_with_token("c", "d", &"t".repeat(64)).await?;
    assert!(f.exists("d").await);

    // `reset` forgets the tokens. It also forgets the client's session, so use a new client.
    f.server.reset().await?;
    f.server.create_bucket(&f.bucket).await?;
    f.s3 = common::no_retry(&f.handle.client().await);
    f.put("c", "other").await;
    f.s3.rename_object()
        .bucket(&f.bucket)
        .key("d")
        .rename_source("c")
        .client_token("token-1")
        .send()
        .await?;
    assert_eq!(f.content("d").await, b"other");
    f.shutdown().await;
    Ok(())
}

/// Under `WithholdResponseHeaders`, a rename is applied but never answered: a retry with its
/// client token succeeds, and a retry without one fails with NoSuchKey.
#[tokio::test]
async fn test_rename_withhold_response_headers() -> Result {
    let f = Fixture::directory().await;
    f.put("a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::RenameObject).key("b"),
        FaultType::WithholdResponseHeaders,
        0,
        Occurrence::Once,
    );
    let rename_with_token = || {
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("b")
            .rename_source("a")
            .client_token("token-1")
            .send()
    };
    let outcome =
        tokio::time::timeout(std::time::Duration::from_secs(2), rename_with_token()).await;
    assert!(outcome.is_err(), "the rename must not be answered");
    assert!(!f.exists("a").await);
    assert_eq!(f.content("b").await, b"hello");
    rename_with_token().await?;
    let fail = failure(rename(&f, "a", "b").await);
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}

/// A rename moves the object and keeps its ETag. The source is a key, which may be
/// percent-encoded, and an existing destination is replaced.
#[tokio::test]
async fn test_rename() -> Result {
    let f = Fixture::directory().await;
    let etag = f.put("dir/a", "hello").await;

    rename(&f, "dir/a", "other/b").await?;
    assert!(!f.exists("dir/a").await);
    assert_eq!(f.content("other/b").await, b"hello");
    let head =
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("other/b")
            .send()
            .await?;
    assert_eq!(head.e_tag.as_deref(), Some(etag.as_str()));

    // As in S3, `/bucket/key` names the key `/bucket/key`, and percent-encoded sources are
    // decoded.
    let fail = failure(rename(&f, &format!("/{}/other/b", f.bucket), "c d").await);
    assert_failure(&fail, 404, "NoSuchKey");
    rename(&f, "other/b", "c d").await?;
    rename(&f, "c%20d", "e+f").await?;
    assert_eq!(f.content("e+f").await, b"hello");

    // Rename overwrites an existing destination.
    f.put("g", "old").await;
    rename(&f, "e+f", "g").await?;
    assert_eq!(f.content("g").await, b"hello");
    f.shutdown().await;
    Ok(())
}

/// A rename fails with 412 and changes nothing when `If-None-Match: *` finds a destination, or an
/// `If-Match` on the destination or the source does not match; it succeeds when they hold.
#[tokio::test]
async fn test_rename_preconditions() -> Result {
    let f = Fixture::directory().await;
    let src_etag = f.put("src", "source").await;
    let dst_etag = f.put("dst", "destination").await;

    // If-None-Match: * on an existing destination.
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("src")
            .destination_if_none_match("*")
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");

    // If-Match on the destination.
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("src")
            .destination_if_match("\"nope\"")
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");

    // If-Match on the source.
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("src")
            .source_if_match("\"nope\"")
            .send()
            .await,
    );
    assert_failure(&fail, 412, "PreconditionFailed");

    // Nothing changed.
    assert_eq!(f.content("src").await, b"source");
    assert_eq!(f.content("dst").await, b"destination");

    f.s3.rename_object()
        .bucket(&f.bucket)
        .key("dst")
        .rename_source("src")
        .destination_if_match(&dst_etag)
        .source_if_match(&src_etag)
        .send()
        .await?;
    assert!(!f.exists("src").await);
    assert_eq!(f.content("dst").await, b"source");

    // If-None-Match: * on a missing destination succeeds.
    f.s3.rename_object()
        .bucket(&f.bucket)
        .key("new")
        .rename_source("dst")
        .destination_if_none_match("*")
        .send()
        .await?;
    assert_eq!(f.content("new").await, b"source");
    f.shutdown().await;
    Ok(())
}

/// A failed rename condition is named in the error's `Condition` element, which clients parse:
/// `If-None-Match` for an existing destination, `If-Match` for a destination or source ETag that
/// does not match, and `If-Match` when an ETag condition and `If-None-Match` both fail, as S3
/// names them.
#[tokio::test]
async fn test_rename_precondition_failure_reports_condition() -> Result {
    let f = Fixture::directory().await;
    f.put("src", "source").await;
    f.put("dst", "destination").await;
    type Customize =
        fn(
            aws_sdk_s3::operation::rename_object::builders::RenameObjectFluentBuilder,
        ) -> aws_sdk_s3::operation::rename_object::builders::RenameObjectFluentBuilder;
    let cases: [(Customize, &str); 5] = [
        (|r| r.destination_if_none_match("*"), "If-None-Match"),
        (|r| r.destination_if_match("\"nope\""), "If-Match"),
        (|r| r.source_if_match("\"nope\""), "If-Match"),
        (
            |r| {
                r.destination_if_none_match("*")
                    .destination_if_match("\"nope\"")
            },
            "If-Match",
        ),
        (
            |r| r.destination_if_none_match("*").source_if_match("\"nope\""),
            "If-Match",
        ),
    ];
    for (customize, expected) in cases {
        let request =
            f.s3.rename_object()
                .bucket(&f.bucket)
                .key("dst")
                .rename_source("src");
        let err = customize(request)
            .send()
            .await
            .expect_err("rename should fail");
        assert_eq!(
            err.raw_response().expect("raw response").status().as_u16(),
            412
        );
        assert_eq!(condition(&err).as_deref(), Some(expected), "{expected}");
    }
    assert_eq!(f.content("dst").await, b"destination");
    f.shutdown().await;
    Ok(())
}

/// A missing source fails with NoSuchKey before any destination condition is evaluated, and
/// `If-Match` on a missing destination fails with 412 naming `If-Match`, as in S3.
#[tokio::test]
async fn test_rename_precondition_order() -> Result {
    let f = Fixture::directory().await;
    f.put("dst", "destination").await;
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("missing")
            .destination_if_none_match("*")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("missing")
            .destination_if_match("\"nope\"")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");

    f.put("src", "source").await;
    let err =
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("new")
            .rename_source("src")
            .destination_if_match("\"nope\"")
            .send()
            .await
            .expect_err("rename should fail");
    assert_eq!(condition(&err).as_deref(), Some("If-Match"));
    assert_failure(&failure::<(), _>(Err(err)), 412, "PreconditionFailed");
    assert!(f.exists("src").await);
    assert!(!f.exists("new").await);
    f.shutdown().await;
    Ok(())
}

/// Conditions the server does not evaluate fail the rename with 501 instead of being ignored,
/// and a source key that is not ASCII is accepted.
#[tokio::test]
async fn test_rename_unsupported_conditions_and_unicode_source() -> Result {
    let f = Fixture::directory().await;
    f.put("src", "source").await;
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("src")
            .destination_if_none_match("\"etag\"")
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    let fail = failure(
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("dst")
            .rename_source("src")
            .source_if_none_match("\"etag\"")
            .send()
            .await,
    );
    assert_failure(&fail, 501, "NotImplemented");
    assert!(f.exists("src").await);

    f.put("caf\u{e9}", "unicode").await;
    let response =
        f.s3.rename_object()
            .bucket(&f.bucket)
            .key("renamed")
            .rename_source("src")
            .customize()
            .mutate_request(|req| {
                req.headers_mut().insert(
                    "x-amz-rename-source",
                    http::HeaderValue::from_bytes("caf\u{e9}".as_bytes()).unwrap(),
                );
            })
            .send()
            .await;
    response?;
    assert_eq!(f.content("renamed").await, b"unicode");
    f.shutdown().await;
    Ok(())
}

/// A missing source fails with NoSuchKey. A destination key over 1,024 bytes fails with 400
/// `KeyTooLongError` reporting the key's size and the limit, as S3 answers it, and is still
/// counted and subject to faults; a 1,024-byte key is accepted.
#[tokio::test]
async fn test_rename_errors() -> Result {
    let f = Fixture::directory().await;
    let fail = failure(rename(&f, "missing", "b").await);
    assert_failure(&fail, 404, "NoSuchKey");

    f.put("a", "hello").await;
    let long_key = "k".repeat(1025);
    let err = rename(&f, "a", &long_key)
        .await
        .expect_err("rename should fail");
    let body = std::str::from_utf8(err.raw_response().unwrap().body().bytes().unwrap())?;
    assert!(
        body.contains("<Size>1025</Size><MaxSizeAllowed>1024</MaxSizeAllowed>"),
        "{body}"
    );
    assert_failure(&failure::<(), _>(Err(err)), 400, "KeyTooLongError");
    assert!(f.exists("a").await);

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::RenameObject).key(&long_key),
        FaultType::Error(Error::AccessDenied),
        0,
        Occurrence::Once,
    );
    let fail = failure(rename(&f, "a", &long_key).await);
    assert_failure(&fail, 403, "AccessDenied");
    assert_eq!(f.server.request_count(Operation::RenameObject), 3);

    rename(&f, "a", &"k".repeat(1024)).await?;
    f.shutdown().await;
    Ok(())
}

/// A rename fault targets the destination key and fails the rename before it moves anything.
#[tokio::test]
async fn test_rename_fault_targets_destination_key() -> Result {
    let f = Fixture::directory().await;
    f.put("dir/a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::RenameObject).key("dir/b"),
        FaultType::Error(Error::Internal("injected fault".to_string())),
        0,
        Occurrence::Once,
    );
    let fail = failure(rename(&f, "dir/a", "dir/b").await);
    assert_failure(&fail, 500, "InternalError");
    assert!(f.exists("dir/a").await);
    rename(&f, "dir/a", "dir/b").await?;
    assert_eq!(f.content("dir/b").await, b"hello");
    f.shutdown().await;
    Ok(())
}

/// A rename to a too-long key that is not signed fails with 403 AccessDenied without being counted
/// or drawing a fault, so the next signed rename still draws it.
#[tokio::test]
async fn test_unsigned_rename_to_long_key_leaves_counts_and_faults() -> Result {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let f = Fixture::directory().await;
    f.put("a", "hello").await;
    let long_key = "k".repeat(1025);
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::RenameObject).key(&long_key),
        FaultType::Error(Error::SlowDown),
        0,
        Occurrence::Once,
    );
    let request = format!(
        "PUT /{}/{long_key}?renameObject HTTP/1.1\r\nHost: localhost\r\nx-amz-rename-source: a\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n",
        f.bucket
    );
    let mut stream = tokio::net::TcpStream::connect(f.handle.socket_addr()).await?;
    stream.write_all(request.as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    assert!(response.starts_with("HTTP/1.1 403"), "{response}");
    assert!(response.contains("<Code>AccessDenied</Code>"), "{response}");
    assert_eq!(f.server.request_count(Operation::RenameObject), 0);

    let fail = failure(rename(&f, "a", &long_key).await);
    assert_failure(&fail, 503, "SlowDown");
    assert_eq!(f.server.request_count(Operation::RenameObject), 1);
    f.shutdown().await;
    Ok(())
}

/// Sends the request with the `/` after the bucket in its path percent-encoded,
/// `/{bucket}%2F{key}`.
#[derive(Debug)]
struct EncodeSlashAfterBucket(String);

impl aws_sdk_s3::config::Intercept for EncodeSlashAfterBucket {
    fn name(&self) -> &'static str {
        "EncodeSlashAfterBucket"
    }

    fn modify_before_transmit(
        &self,
        context: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &aws_sdk_s3::config::RuntimeComponents,
        _cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> std::result::Result<(), aws_sdk_s3::error::BoxError> {
        let request = context.request_mut();
        let uri = request
            .uri()
            .replacen(&format!("/{}/", self.0), &format!("/{}%2F", self.0), 1);
        request.set_uri(uri)?;
        Ok(())
    }
}

/// A rename's path is read as for any other request: decoded, then split after the bucket, so
/// `/{bucket}%2F{key}` renames to `key` of the bucket.
#[tokio::test]
async fn test_rename_path_decoded_before_split() -> Result {
    let f = Fixture::directory().await;
    f.put("source", "hello").await;
    f.s3.rename_object()
        .bucket(&f.bucket)
        .key("destination")
        .rename_source("source")
        .customize()
        .interceptor(EncodeSlashAfterBucket(f.bucket.clone()))
        .send()
        .await?;
    assert!(!f.exists("source").await);
    assert_eq!(f.content("destination").await, b"hello");
    f.shutdown().await;
    Ok(())
}
