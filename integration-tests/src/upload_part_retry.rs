/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload-part transient-transport retry contract.
//!
//! Reproduces the macOS CI `ENOBUFS`-on-`UploadPart` failure deterministically
//! by injecting a dispatch-layer IO error at the client connector (see
//! `fault_connector`) — the faithful client-side shape, not a server fault. The
//! contract: an isolated transient recovers; a correlated transient burst (which
//! starves the SDK's shared retry token bucket) recovers via the TM's outer
//! retry + full-jittered backoff; a persistent outage fails cleanly without
//! hanging. Full analysis: `networking/retry-ownership-and-token-bucket.md`.

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::PartSize;
use s3_mock_server::S3MockServer;

use crate::fault_connector::{io_fault_http_client, is_upload_part, FailCount};

/// 5 MiB parts, matching the failing CI test's `ALIGNED_PART_SIZE`.
const PART_SIZE: u64 = 5 * 1024 * 1024;

/// A multi-part object: enough parts that at least one `UploadPart` is in flight
/// to receive the injected fault.
fn many_part_body() -> Vec<u8> {
    vec![0u8; 8 * PART_SIZE as usize] // 8 parts
}

/// Matches the failing CI test (`MANY_PARTS = 64`): a 64-part object. 64 retries
/// at 14 tokens each (896) exceeds the SDK token bucket's 500 capacity, so a
/// correlated burst over all 64 parts cannot all retry — the bucket starves.
const CI_MANY_PARTS: usize = 64;
fn ci_scale_body() -> Vec<u8> {
    vec![0u8; CI_MANY_PARTS * PART_SIZE as usize]
}

/// Build a TM client wired to the mock server through the IO-fault connector.
async fn setup_with_fault(
    fail: FailCount,
) -> (
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
    crate::fault_connector::InjectionTally,
) {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");

    let (http_client, tally) = io_fault_http_client(is_upload_part, fail);

    // Rebuild the mock's S3 client with our fault connector as the http_client.
    let base = handle.client().await;
    let conf = base.config().to_builder().http_client(http_client).build();
    let s3_client = aws_sdk_s3::Client::from_conf(conf);

    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .part_size(PartSize::Target(PART_SIZE))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);
    (handle, tm, tally)
}

/// A single injected `UploadPart` dispatch IO error recovers. (The SDK's own
/// retry handles an isolated transient when its quota is not contended; this
/// pins that baseline so the burst test's failure is attributable to contention,
/// not to the fault shape itself.)
#[tokio::test]
async fn upload_part_single_dispatch_io_error_recovers() {
    let (_server, tm, tally) = setup_with_fault(FailCount::First(1)).await;

    let body = many_part_body();
    let result = tm
        .upload()
        .bucket("test-bucket")
        .key("obj")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from(bytes::Bytes::from(body)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert_eq!(
        tally.count(),
        1,
        "expected exactly one injected UploadPart dispatch failure"
    );
    result.expect("a single transient UploadPart dispatch IO error must recover");
}

/// Correlated transient burst: fault every part's first dispatch at once (the
/// macOS CI ENOBUFS scenario). The SDK's shared retry token bucket starves under
/// the simultaneous draw and surfaces some parts un-recovered; the TM outer retry
/// + full-jittered backoff must then recover the whole transfer. Reproduced the
/// CI failure before the fix; teeth-checked (fails when the transient-transport
/// retry arm is removed).
#[tokio::test]
async fn upload_part_correlated_burst_dispatch_io_error() {
    // CI scale: 64 parts. Fault each part's FIRST dispatch once (transient
    // ENOBUFS burst: every part's initial write fails, pressure then clears).
    // This drives the SDK's shared retry quota to starvation (some parts surface
    // un-recovered), which the TM outer retry + backoff must then recover.
    let (_server, tm, tally) = setup_with_fault(FailCount::EachPartOnce).await;

    let body = ci_scale_body();
    let result = tm
        .upload()
        .bucket("test-bucket")
        .key("obj")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from(bytes::Bytes::from(body)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert!(
        tally.count() >= CI_MANY_PARTS as u32,
        "expected every part's first dispatch to be faulted (got {})",
        tally.count()
    );
    // Regression: this exact configuration reproduced the macOS CI ENOBUFS
    // failure before the TM transient-transport retry existed (the SDK's shared
    // retry token bucket starved under the correlated burst). With the TM outer
    // retry + full-jittered backoff, the re-issues de-correlate and recover.
    result.expect("correlated UploadPart dispatch-IO burst must recover via TM retry");
}

/// Persistent injected dispatch IO error: the upload must fail cleanly (no hang),
/// not retry forever. A sustained transport outage is terminal once both the SDK
/// inner retry and the TM outer retry are exhausted.
#[tokio::test]
async fn upload_part_persistent_dispatch_io_error_fails() {
    let (_server, tm, tally) = setup_with_fault(FailCount::Always).await;

    let body = many_part_body();
    let result = tm
        .upload()
        .bucket("test-bucket")
        .key("obj")
        .checksum_strategy(ChecksumStrategy::with_calculated_crc32())
        .body(InputStream::from(bytes::Bytes::from(body)))
        .initiate()
        .expect("initiate upload")
        .join()
        .await;

    assert!(tally.count() >= 1, "expected the fault to fire");
    assert!(
        result.is_err(),
        "a persistent dispatch IO error must fail the upload, not hang or succeed"
    );
}
