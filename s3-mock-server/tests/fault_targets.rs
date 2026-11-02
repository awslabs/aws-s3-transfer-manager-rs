// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Faults registered for a [`FaultTarget`], observed through the SDK: which requests a target
//! matches, how `skip` and `Occurrence` schedule the fault, and the error responses the client
//! sees. Registry state-machine semantics are unit-tested in `src/faults.rs`.

mod common;

use aws_sdk_s3::config::ResponseChecksumValidation;
use aws_sdk_s3::primitives::ByteStream;
use common::{assert_failure, failure, Fixture, Result};
use s3_mock_server::{FaultTarget, FaultType, Occurrence, Operation};

const ACCESS_DENIED: FaultType = FaultType::ServiceError {
    status: 403,
    code: "AccessDenied",
};

/// The first `skip` matching requests are served; the next one fails with the fault's status,
/// code and message; with `Once`, later requests are served again.
#[tokio::test]
async fn test_error_response_skip_then_once() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::GetObject).key("a"),
        ACCESS_DENIED,
        1,
        Occurrence::Once,
    );

    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 403, "AccessDenied");
    assert_eq!(fail.message.as_deref(), Some("Injected fault"));
    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    f.shutdown().await;
    Ok(())
}

/// Requests for another key or another operation neither fail nor count towards `skip`.
#[tokio::test]
async fn test_target_scopes_by_operation_and_key() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.put("b", "world").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::GetObject)
            .bucket(&f.bucket)
            .key("a"),
        FaultType::ServiceError {
            status: 404,
            code: "NoSuchKey",
        },
        1,
        Occurrence::NTimes(2),
    );

    f.s3.get_object().bucket(&f.bucket).key("b").send().await?;
    f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    for _ in 0..2 {
        let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
        assert_failure(&fail, 404, "NoSuchKey");
    }
    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    f.shutdown().await;
    Ok(())
}

/// A target without a key matches every key. An error response is returned before the request
/// reaches storage, so a failed PutObject neither replaces nor creates an object. After
/// `clear_faults`, requests are served.
#[tokio::test]
async fn test_error_response_precedes_storage_and_clear_faults() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::PutObject),
        FaultType::ServiceError {
            status: 400,
            code: "SomethingNew",
        },
        0,
        Occurrence::Always,
    );
    for key in ["a", "b"] {
        let fail = failure(
            f.s3.put_object()
                .bucket(&f.bucket)
                .key(key)
                .body(ByteStream::from_static(b"new"))
                .send()
                .await,
        );
        assert_failure(&fail, 400, "SomethingNew");
    }
    assert_eq!(f.content("a").await, b"hello");
    assert!(!f.exists("b").await);

    f.server.clear_faults();
    f.put("b", "now").await;
    assert_eq!(f.content("b").await, b"now");
    f.shutdown().await;
    Ok(())
}

/// A fault fails a request for a key that does not exist, because it is drawn before the
/// request is served.
#[tokio::test]
async fn test_error_response_on_missing_key() -> Result {
    let f = Fixture::new().await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::DeleteObject).key("missing"),
        ACCESS_DENIED,
        0,
        Occurrence::Once,
    );
    let fail = failure(
        f.s3.delete_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 403, "AccessDenied");
    f.shutdown().await;
    Ok(())
}

/// HeadObject faults fail with the fault's status; a HEAD response has no body, so the client
/// sees no error code. ListObjectsV2 targets are scoped by prefix.
#[tokio::test]
async fn test_error_response_on_head_and_list() -> Result {
    let f = Fixture::new().await;
    f.put("dir/a", "hello").await;

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::HeadObject).key("dir/a"),
        ACCESS_DENIED,
        0,
        Occurrence::Once,
    );
    let fail = failure(
        f.s3.head_object()
            .bucket(&f.bucket)
            .key("dir/a")
            .send()
            .await,
    );
    assert_eq!(fail.status, 403);
    f.s3.head_object()
        .bucket(&f.bucket)
        .key("dir/a")
        .send()
        .await?;

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::ListObjectsV2).key("dir/"),
        FaultType::ServiceError {
            status: 503,
            code: "SlowDown",
        },
        0,
        Occurrence::Once,
    );
    f.s3.list_objects_v2()
        .bucket(&f.bucket)
        .prefix("other/")
        .send()
        .await?;
    f.s3.list_objects_v2().bucket(&f.bucket).send().await?;
    let fail = failure(
        f.s3.list_objects_v2()
            .bucket(&f.bucket)
            .prefix("dir/")
            .send()
            .await,
    );
    assert_failure(&fail, 503, "SlowDown");
    f.shutdown().await;
    Ok(())
}

/// Bucket operations draw faults too: a target restricted to a bucket matches only requests for
/// that bucket, and ListBuckets, which names no bucket, matches only unrestricted targets.
#[tokio::test]
async fn test_error_response_on_bucket_operations() -> Result {
    let f = Fixture::new().await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::ListBuckets).bucket(&f.bucket),
        ACCESS_DENIED,
        0,
        Occurrence::Always,
    );
    f.s3.list_buckets().send().await?;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::ListBuckets),
        ACCESS_DENIED,
        0,
        Occurrence::Once,
    );
    let fail = failure(f.s3.list_buckets().send().await);
    assert_failure(&fail, 403, "AccessDenied");
    f.s3.list_buckets().send().await?;

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::HeadBucket).bucket(&f.bucket),
        ACCESS_DENIED,
        0,
        Occurrence::Once,
    );
    f.s3.create_bucket().bucket("new-bucket").send().await?;
    f.s3.head_bucket().bucket("new-bucket").send().await?;
    let fail = failure(f.s3.head_bucket().bucket(&f.bucket).send().await);
    assert_eq!(fail.status, 403);
    f.s3.head_bucket().bucket(&f.bucket).send().await?;

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::CreateBucket),
        FaultType::ServiceError {
            status: 409,
            code: "BucketAlreadyExists",
        },
        0,
        Occurrence::Once,
    );
    let fail = failure(f.s3.create_bucket().bucket("third-bucket").send().await);
    assert_failure(&fail, 409, "BucketAlreadyExists");

    f.server.insert_fault_for(
        FaultTarget::operation(Operation::DeleteBucket),
        FaultType::ServiceError {
            status: 409,
            code: "BucketNotEmpty",
        },
        0,
        Occurrence::Once,
    );
    let fail = failure(f.s3.delete_bucket().bucket("new-bucket").send().await);
    assert_failure(&fail, 409, "BucketNotEmpty");
    f.s3.delete_bucket().bucket("new-bucket").send().await?;
    f.shutdown().await;
    Ok(())
}

/// A body fault registered for an operation target shapes the GetObject response it fires on.
#[tokio::test]
async fn test_body_fault_for_operation_target() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::GetObject),
        FaultType::CorruptBody,
        0,
        Occurrence::Once,
    );
    // Read the corrupted bytes rather than have the SDK reject them by checksum.
    let s3 = aws_sdk_s3::Client::from_conf(
        f.s3.config()
            .to_builder()
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .build(),
    );
    for expected in [b"\x97ello", b"hello"] {
        let response = s3.get_object().bucket(&f.bucket).key("a").send().await?;
        assert_eq!(&response.body.collect().await?.into_bytes()[..], expected);
    }
    f.shutdown().await;
    Ok(())
}

/// `insert_fault` keeps targeting only the GetObject, PutObject and UploadPart requests of its
/// key: HeadObject requests for the key do not count towards `skip`.
#[tokio::test]
async fn test_insert_fault_targets_object_data_requests() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server
        .insert_fault(&f.bucket, "a", FaultType::slow_down(), 1, Occurrence::Once);
    for _ in 0..3 {
        f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    }
    f.s3.get_object().bucket(&f.bucket).key("a").send().await?;
    let fail = failure(f.s3.get_object().bucket(&f.bucket).key("a").send().await);
    assert_failure(&fail, 503, "SlowDown");
    f.shutdown().await;
    Ok(())
}

/// `insert_fault` draws its fault when a GetObject for the key arrives, before the key is looked
/// up, so a fault scheduled for a missing key fails the request instead of NoSuchKey.
#[tokio::test]
async fn test_insert_fault_fires_before_the_key_is_looked_up() -> Result {
    let f = Fixture::new().await;
    f.server.insert_fault(
        &f.bucket,
        "missing",
        FaultType::slow_down(),
        0,
        Occurrence::Once,
    );
    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 503, "SlowDown");
    let fail = failure(
        f.s3.get_object()
            .bucket(&f.bucket)
            .key("missing")
            .send()
            .await,
    );
    assert_failure(&fail, 404, "NoSuchKey");
    f.shutdown().await;
    Ok(())
}

/// A request the rate throttle sheds draws no fault: the fault fires on the next admitted
/// request.
#[tokio::test]
async fn test_throttled_request_draws_no_fault() -> Result {
    let f = Fixture::new().await;
    f.put("a", "hello").await;
    f.server.insert_fault_for(
        FaultTarget::operation(Operation::HeadObject).key("a"),
        ACCESS_DENIED,
        0,
        Occurrence::Once,
    );
    // An empty bucket that never refills sheds every object request.
    f.server.set_rate_throttle(0.0, 0.0);
    let fail = failure(f.s3.head_object().bucket(&f.bucket).key("a").send().await);
    assert_eq!(fail.status, 503);
    // A throttle that admits every request of this test.
    f.server.set_rate_throttle(1000.0, 1000.0);
    let fail = failure(f.s3.head_object().bucket(&f.bucket).key("a").send().await);
    assert_eq!(fail.status, 403);
    f.s3.head_object().bucket(&f.bucket).key("a").send().await?;
    f.shutdown().await;
    Ok(())
}
