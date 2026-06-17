// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end fault-injection tests: a registered fault makes the validating
//! SDK GET fail. Registry state-machine semantics (skip/occurrence/queue) are
//! unit-tested in `src/faults.rs`; these assert the observable download outcome.

use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode};
use bytes::Bytes;
use s3_mock_server::{FaultType, Occurrence, S3MockServer, ServerHandle};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const B: &str = "faults";
const K: &str = "obj";
const BODY: &[u8] = b"hello world, this is the body";

async fn seed(handle: &ServerHandle) -> Result<()> {
    let s3 = handle.client().await;
    s3.create_bucket().bucket(B).send().await.ok();
    s3.put_object()
        .bucket(B)
        .key(K)
        .body(Bytes::copy_from_slice(BODY).into())
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await?;
    Ok(())
}

/// A validating GET that fully reads the body. Ok(()) = validated clean,
/// Err = the SDK rejected the (faulted) response.
async fn validating_get(handle: &ServerHandle) -> Result<()> {
    let s3 = handle.client().await;
    let resp = s3
        .get_object()
        .bucket(B)
        .key(K)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    let _ = resp.body.collect().await?; // checksum validated on full read
    Ok(())
}

/// Same but with a whole-object RANGE (bytes=0-(len-1)), like the TM's discovery GET.
async fn validating_ranged_get(handle: &ServerHandle) -> Result<()> {
    let s3 = handle.client().await;
    let resp = s3
        .get_object()
        .bucket(B)
        .key(K)
        .range(format!("bytes=0-{}", BODY.len() - 1))
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    let _ = resp.body.collect().await?;
    Ok(())
}

/// A ranged whole-object GET (as the transfer manager issues for discovery) of a
/// corrupted body must fail the SDK's checksum validation.
#[tokio::test]
async fn test_ranged_get_corrupt_body_fails_validation() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;
    server.insert_fault(B, K, FaultType::CorruptBody, 0, Occurrence::Always);
    assert!(
        validating_ranged_get(&handle).await.is_err(),
        "ranged GET corrupt body should fail validation"
    );
    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_wrong_stored_checksum_fails_validating_get() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    validating_get(&handle).await.expect("clean before fault");
    server.insert_fault(B, K, FaultType::WrongStoredChecksum, 0, Occurrence::Always);
    assert!(
        validating_get(&handle).await.is_err(),
        "wrong checksum must fail the GET"
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_corrupt_body_fails_validating_get() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    server.insert_fault(B, K, FaultType::CorruptBody, 0, Occurrence::Always);
    // Body bytes are tampered but the checksum header is intact → validation
    // catches the corruption (proves we validate content, not just compare headers).
    assert!(
        validating_get(&handle).await.is_err(),
        "corrupt body must fail the GET"
    );

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_skip_warms_up_then_fails() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    // Two clean GETs (warm-up), then fail.
    server.insert_fault(B, K, FaultType::WrongStoredChecksum, 2, Occurrence::Always);
    validating_get(&handle).await.expect("req 1 clean");
    validating_get(&handle).await.expect("req 2 clean");
    assert!(validating_get(&handle).await.is_err(), "req 3 faulted");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_once_then_recovers() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    server.insert_fault(B, K, FaultType::WrongStoredChecksum, 0, Occurrence::Once);
    assert!(validating_get(&handle).await.is_err(), "first GET faulted");
    validating_get(&handle).await.expect("second GET recovers");

    handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_clear_fault_recovers() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    server.insert_fault(B, K, FaultType::WrongStoredChecksum, 0, Occurrence::Always);
    assert!(validating_get(&handle).await.is_err());
    server.clear_fault(B, K);
    validating_get(&handle).await.expect("cleared → clean");

    handle.shutdown().await?;
    Ok(())
}

const BIG_KEY: &str = "big-obj";

/// Seed a larger object (no checksum needed; the point is the body stream).
async fn seed_big(handle: &ServerHandle, len: usize) -> Result<()> {
    let s3 = handle.client().await;
    s3.create_bucket().bucket(B).send().await.ok();
    s3.put_object()
        .bucket(B)
        .key(BIG_KEY)
        .body(Bytes::from(vec![0xABu8; len]).into())
        .send()
        .await?;
    Ok(())
}

/// `TruncateBody` yields `after_bytes` body bytes then errors the stream. With an
/// object large enough that the SDK does not buffer the whole response during
/// `send()`, the headers (full Content-Length) succeed, the body delivers a prefix
/// of bytes, then the stream errors before the full length. (The exact prefix the
/// consumer sees before the error propagates is timing-dependent, so it is not
/// asserted precisely.)
#[tokio::test]
async fn test_truncate_body_fails_mid_stream() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    let len = 24 * 1024 * 1024; // large enough that send() does not buffer the whole body
    seed_big(&handle, len).await?;

    let after_bytes = 1024 * 1024;
    server.insert_fault(
        B,
        BIG_KEY,
        FaultType::TruncateBody { after_bytes },
        0,
        Occurrence::Always,
    );

    let s3 = handle.client().await;
    let mut resp = s3
        .get_object()
        .bucket(B)
        .key(BIG_KEY)
        .send()
        .await
        .expect("send/headers succeed; truncation is mid-body");

    let mut read = 0u64;
    loop {
        match resp.body.next().await {
            Some(Ok(chunk)) => read += chunk.len() as u64,
            Some(Err(_)) => break,
            None => panic!("body completed without error; expected mid-stream failure"),
        }
    }

    assert!(
        read > 0,
        "body should deliver a prefix before erroring, got 0"
    );
    assert!(
        read < len as u64,
        "stream should error before the full {len} bytes, read {read}"
    );

    handle.shutdown().await?;
    Ok(())
}

/// Build an S3 client against the mock with stalled-stream protection set to a
/// short grace period, so a stalled body surfaces quickly and deterministically.
/// Uses the mock's real test credentials (mock-akid/mock-secret) — wrong creds
/// would be rejected at auth (403) before the body streams, masking the stall.
async fn client_with_short_ssp(handle: &ServerHandle) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{Credentials, Region, StalledStreamProtectionConfig};
    let endpoint_url = format!("http://127.0.0.1:{}", handle.socket_addr().port());
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "mock-akid",
            "mock-secret",
            None,
            None,
            "mock",
        ))
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .load()
        .await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .stalled_stream_protection(
            StalledStreamProtectionConfig::enabled()
                .grace_period(std::time::Duration::from_secs(1))
                .build(),
        )
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// `StallBody` yields some bytes then stalls. With stalled-stream protection
/// enabled, the SDK aborts the read (`ThroughputBelowMinimum`) rather than
/// hanging. Bounded by an outer timeout so a non-firing-SSP regression fails fast
/// instead of hanging.
#[tokio::test]
async fn test_stall_body_triggers_stalled_stream_protection() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    let len = 1024 * 1024; // 1 MiB
    seed_big(&handle, len).await?;

    // Sanity: a clean GET with this client succeeds (proves creds/SSP config are
    // right, so the stall error below is the stall — not auth or misconfig).
    let s3 = client_with_short_ssp(&handle).await;
    s3.get_object()
        .bucket(B)
        .key(BIG_KEY)
        .send()
        .await
        .expect("clean GET before fault")
        .body
        .collect()
        .await
        .expect("clean body read before fault");

    server.insert_fault(
        B,
        BIG_KEY,
        FaultType::StallBody {
            after_bytes: 64 * 1024,
        },
        0,
        Occurrence::Always,
    );

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        match s3.get_object().bucket(B).key(BIG_KEY).send().await {
            Ok(resp) => resp.body.collect().await.is_err(),
            Err(_) => true,
        }
    })
    .await;

    match outcome {
        Ok(errored) => assert!(errored, "stalled body must surface an error, got Ok"),
        Err(_) => panic!("stalled body hung past the timeout — SSP did not fire"),
    }

    handle.shutdown().await?;
    Ok(())
}

/// `ShortBody` ends the body cleanly after `actual_bytes`, fewer than the
/// advertised Content-Length. The response headers (full Content-Length) succeed;
/// the body delivers a prefix then errors with an incomplete-body / unexpected-EOF
/// (the client detects the length mismatch). Uses an object large enough that the
/// body is not fully buffered during `send()`.
#[tokio::test]
async fn test_short_body_fails_length_mismatch() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    let len = 24 * 1024 * 1024; // large enough that send() does not buffer the whole body
    seed_big(&handle, len).await?;

    let actual_bytes = 1024 * 1024;
    server.insert_fault(
        B,
        BIG_KEY,
        FaultType::ShortBody { actual_bytes },
        0,
        Occurrence::Always,
    );

    let s3 = handle.client().await;
    let mut resp = s3
        .get_object()
        .bucket(B)
        .key(BIG_KEY)
        .send()
        .await
        .expect("send/headers succeed; the short read is mid-body");

    // Content-Length advertises the full object even though the body is short.
    assert_eq!(resp.content_length(), Some(len as i64));

    let mut read = 0u64;
    loop {
        match resp.body.next().await {
            Some(Ok(chunk)) => read += chunk.len() as u64,
            Some(Err(_)) => break,
            None => panic!("short body ended cleanly; client should detect the length mismatch"),
        }
    }

    assert!(
        read > 0,
        "body should deliver a prefix before erroring, got 0"
    );
    assert!(
        read < len as u64,
        "short body must deliver fewer than the full {len} bytes, read {read}"
    );

    handle.shutdown().await?;
    Ok(())
}

/// `set_socket_drop` aborts the connection (RST) after a byte threshold. The
/// `FaultType::ConnectionReset` aborts the connection (RST) after a byte budget,
/// armed for the specific connection serving the faulted key's GET. The response
/// headers succeed; the body delivers a prefix then errors with a real
/// `ConnectionReset` (the OS-level reset, distinct from a body-stream error).
#[tokio::test]
async fn test_connection_reset_mid_stream() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    let len = 24 * 1024 * 1024; // large enough that send() does not buffer the whole body
    seed_big(&handle, len).await?;

    server.insert_fault(
        B,
        BIG_KEY,
        FaultType::ConnectionReset {
            after_bytes: 1024 * 1024,
        },
        0,
        Occurrence::Always,
    );

    let s3 = handle.client().await;
    let mut resp = s3
        .get_object()
        .bucket(B)
        .key(BIG_KEY)
        .send()
        .await
        .expect("send/headers succeed; the reset is mid-body");

    let mut read = 0u64;
    loop {
        match resp.body.next().await {
            Some(Ok(chunk)) => read += chunk.len() as u64,
            Some(Err(_)) => break,
            None => panic!("body completed without error; expected a connection reset"),
        }
    }

    assert!(
        read > 0,
        "body should deliver a prefix before the reset, got 0"
    );
    assert!(
        read < len as u64,
        "connection should reset before the full {len} bytes, read {read}"
    );

    handle.shutdown().await?;
    Ok(())
}

/// Build a client with its own connection pool and retries disabled, so a
/// connect-time fault forces a new connection and is not transparently retried.
async fn fresh_client_no_retry(handle: &ServerHandle) -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{retry::RetryConfig, Credentials, Region};
    let endpoint_url = format!("http://127.0.0.1:{}", handle.socket_addr().port());
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(Credentials::new(
            "mock-akid",
            "mock-secret",
            None,
            None,
            "mock",
        ))
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .retry_config(RetryConfig::disabled())
        .load()
        .await;
    let config = aws_sdk_s3::config::Builder::from(&shared)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

/// `reset_next_connections` aborts a freshly accepted connection before any
/// request is served, simulating a connect-time reset. Uses a fresh client (to
/// force a new connection) with retries disabled (so the SDK does not
/// transparently recover); the request fails at dispatch, not mid-body.
#[tokio::test]
async fn test_connect_time_reset_fails_dispatch() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed_big(&handle, 4096).await?;

    server.reset_next_connections(1);

    let s3 = fresh_client_no_retry(&handle).await;
    let result = s3.get_object().bucket(B).key(BIG_KEY).send().await;
    assert!(
        result.is_err(),
        "a connect-time reset must fail the request at dispatch"
    );

    handle.shutdown().await?;
    Ok(())
}

/// A `ServiceError` fault makes the GET return the configured HTTP error status
/// at send time (before any body), reaching the SDK's dispatch/retry layer
/// rather than the body-read path. Retry is disabled so the fault is observed
/// directly. `Once` fires for the first GET only; the next recovers.
#[tokio::test]
async fn test_service_error_fault_returns_status() -> Result<()> {
    let server = S3MockServer::builder().with_in_memory_store().build()?;
    let handle = server.start().await?;
    seed(&handle).await?;

    server.insert_fault(
        B,
        K,
        FaultType::ServiceError { status: 503 },
        0,
        Occurrence::Once,
    );

    let s3 = fresh_client_no_retry(&handle).await;
    let err = s3
        .get_object()
        .bucket(B)
        .key(K)
        .send()
        .await
        .expect_err("faulted GET returns a service error");
    let service_err = err.into_service_error();
    assert_eq!(
        service_err.meta().code(),
        Some("SlowDown"),
        "503 maps to SlowDown",
    );

    // Once consumed → the next GET succeeds.
    s3.get_object()
        .bucket(B)
        .key(K)
        .send()
        .await
        .expect("second GET recovers");

    handle.shutdown().await?;
    Ok(())
}
