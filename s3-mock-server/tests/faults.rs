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
