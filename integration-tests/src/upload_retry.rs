/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload failure-handling contract tests, driven through the real SDK send path
//! against the mock server (real HTTP, production client config incl. stalled-
//! stream protection). Faults are injected two ways:
//!
//! - **Client-side dispatch faults** (this module) — a local IO error on the
//!   `UploadPart` send, injected by a fault *connector* before the request
//!   reaches the server. Models socket-buffer exhaustion (`ENOBUFS`) that starves
//!   the SDK's shared retry token bucket; the mock server cannot reproduce a send
//!   that never leaves the client. The contract: an isolated transient recovers;
//!   a correlated transient burst recovers via the TM's outer retry + full-
//!   jittered backoff; a persistent outage fails cleanly without hanging.
//! - **Server-side faults** ([`server_faults`]) — service errors and a mid-body
//!   read stall injected by the mock server on requests that reached it, across
//!   both the `PutObject` and `UploadPart` upload paths.
//!
//! The upload path carries no adaptive latency deadline (unlike download's time-
//! to-first-byte deadline); these pin the recovery that is in place and document
//! the response-wait gap that remains.

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::PartSize;
use s3_mock_server::S3MockServer;

mod fault_connector;

use fault_connector::{io_fault_http_client, is_upload_part, FailCount};

/// 5 MiB parts (the minimum S3 multipart part size).
const PART_SIZE: u64 = 5 * 1024 * 1024;

/// A multi-part object: enough parts that at least one `UploadPart` is in flight
/// to receive the injected fault.
fn many_part_body() -> Vec<u8> {
    vec![0u8; 8 * PART_SIZE as usize] // 8 parts
}

/// A 64-part object, sized to starve the SDK's shared retry token bucket under a
/// correlated burst: 64 retries at 14 tokens each (896) exceeds the bucket's 500
/// capacity, so not all parts can retry through the SDK's inner retry alone.
const MANY_PARTS: usize = 64;
fn many_parts_burst_body() -> Vec<u8> {
    vec![0u8; MANY_PARTS * PART_SIZE as usize]
}

/// Build a TM client wired to the mock server through the IO-fault connector.
async fn setup_with_fault(
    fail: FailCount,
) -> (
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
    fault_connector::InjectionTally,
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

/// Correlated transient burst: fault every part's first dispatch at once. The
/// SDK's shared retry token bucket starves under the simultaneous draw and
/// surfaces some parts un-recovered; the TM outer retry + full-jittered backoff
/// must then recover the whole transfer. Teeth-checked: fails when the
/// transient-transport retry arm is removed.
#[tokio::test]
async fn upload_part_correlated_burst_dispatch_io_error() {
    // Fault each of the 64 parts' FIRST dispatch once (transient burst: every
    // part's initial write fails, pressure then clears). This drives the SDK's
    // shared retry quota to starvation (some parts surface un-recovered), which
    // the TM outer retry + backoff must then recover.
    let (_server, tm, tally) = setup_with_fault(FailCount::EachPartOnce).await;

    let body = many_parts_burst_body();
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
        tally.count() >= MANY_PARTS as u32,
        "expected every part's first dispatch to be faulted (got {})",
        tally.count()
    );
    // Without the TM's outer transient-transport retry, the SDK's shared retry
    // token bucket starves under the correlated burst and the transfer fails.
    // With the TM outer retry + full-jittered backoff, the re-issues de-correlate
    // and recover.
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

/// Server-side faults injected by the mock server (service errors and a mid-body
/// read stall) on requests that reached it, exercised on both the `PutObject` and
/// `UploadPart` upload paths.
mod server_faults {
    use std::time::Duration;

    use aws_sdk_s3_transfer_manager::io::InputStream;
    use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
    use aws_sdk_s3_transfer_manager::types::PartSize;
    use aws_sdk_s3_transfer_manager::Client as TmClient;
    use s3_mock_server::{FaultType, Occurrence, S3MockServer};

    /// 5 MiB parts (the S3 multipart minimum).
    const PART_SIZE: PartSize = PartSize::Target(5 * 1024 * 1024);

    /// Which upload path a test drives. The transfer manager chooses multipart
    /// when the object is at least the multipart threshold, else a single
    /// `PutObject`.
    #[derive(Clone, Copy)]
    enum Path {
        /// One `PutObject`. Threshold above the object size.
        PutObject,
        /// Multipart: `CreateMultipartUpload` + `UploadPart`s + `CompleteMultipartUpload`.
        /// Threshold at the part size so a two-part object splits.
        Multipart,
    }

    /// A 10 MiB object — two 5 MiB parts under `Path::Multipart`, one `PutObject`
    /// under `Path::PutObject`.
    fn upload_data() -> Vec<u8> {
        vec![0u8; 2 * 5 * ByteUnit::Mebibyte.as_bytes_usize()]
    }

    /// Byte offset at which `StallRequestRead` makes the mock stop reading — the
    /// client finishes writing the body, then waits on a response that never comes.
    const STALL_AFTER_BYTES: u64 = 1024 * 1024;

    /// Start a mock server and a transfer manager wired to it via the production
    /// client-config path (`handle.client()` → `aws_config::defaults(latest())`),
    /// so stalled-stream protection is enabled exactly as in the field. `path`
    /// selects the multipart threshold so the object exercises the intended path.
    async fn setup(path: Path) -> (S3MockServer, s3_mock_server::ServerHandle, TmClient) {
        let server = S3MockServer::builder()
            .with_in_memory_store()
            .build()
            .expect("build mock server");
        let handle = server.start().await.expect("start mock server");
        server
            .create_bucket("test-bucket")
            .await
            .expect("create bucket");
        // 10 MiB object: threshold below it → multipart; above it → single PutObject.
        let threshold = match path {
            Path::Multipart => PART_SIZE,
            Path::PutObject => PartSize::Target(64 * 1024 * 1024),
        };
        let tm = TmClient::new(
            aws_sdk_s3_transfer_manager::Config::builder()
                .client(handle.client().await)
                .part_size(PART_SIZE)
                .multipart_threshold(threshold)
                .build(),
        );
        (server, handle, tm)
    }

    async fn upload(
        tm: &TmClient,
        key: &str,
        data: Vec<u8>,
    ) -> Result<
        aws_sdk_s3_transfer_manager::operation::upload::UploadOutput,
        aws_sdk_s3_transfer_manager::error::Error,
    > {
        tm.upload()
            .bucket("test-bucket")
            .key(key)
            .body(InputStream::from(data))
            .initiate()
            .expect("initiate upload")
            .join()
            .await
    }

    /// A single 503 on the first send is recovered by retry: the request body is
    /// re-sent on a fresh attempt and the upload succeeds. Runs on both upload
    /// paths so the `PutObject` and `UploadPart` retry wiring (and the body's
    /// `try_clone` rewind on each) are both exercised.
    async fn service_error_once_recovers(path: Path) {
        let (server, handle, tm) = setup(path).await;
        server.insert_fault(
            "test-bucket",
            "obj",
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Once,
        );

        let result = upload(&tm, "obj", upload_data()).await;
        assert!(
            result.is_ok(),
            "a single 503 must recover via retry: {:?}",
            result.err()
        );

        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn put_object_service_error_once_recovers() {
        service_error_once_recovers(Path::PutObject).await;
    }

    #[tokio::test]
    async fn upload_part_service_error_once_recovers() {
        service_error_once_recovers(Path::Multipart).await;
    }

    /// A persistent 503 exhausts retries and fails the upload cleanly — the
    /// inverse of the `Once` recovery, proving the retry is bounded and surfaces
    /// an error rather than looping. The timeout guards against a regression to
    /// unbounded retry hanging the suite. Both upload paths.
    async fn service_error_always_fails(path: Path) {
        let (server, handle, tm) = setup(path).await;
        server.insert_fault(
            "test-bucket",
            "obj",
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Always,
        );

        let result =
            tokio::time::timeout(Duration::from_secs(30), upload(&tm, "obj", upload_data()))
                .await
                .expect("retries must be bounded, not hang");
        assert!(
            result.is_err(),
            "a persistent 503 must fail the upload, not succeed"
        );

        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn put_object_persistent_service_error_fails() {
        service_error_always_fails(Path::PutObject).await;
    }

    #[tokio::test]
    async fn upload_part_persistent_service_error_fails() {
        service_error_always_fails(Path::Multipart).await;
    }

    /// KNOWN GAP (ignored): an in-memory upload whose server reads the full
    /// request body then never responds hangs effectively indefinitely.
    ///
    /// This is the "body sent, response never arrives" case — the response-first-
    /// byte interval, which nothing bounds:
    ///   - Stalled-stream protection watches request-body *throughput*. An in-
    ///     memory body is a single frame; hyper reports end-of-stream when it is
    ///     pulled, so SSP marks the request complete before the response wait even
    ///     begins and never observes the stall. (A multi-frame file-backed body
    ///     backpressures when the server stops reading, so SSP *does* catch that
    ///     case — the gap is specific to single-frame in-memory bodies.)
    ///   - The SDK default sets only `connect_timeout`; there is no operation or
    ///     response timeout, and hyper imposes no response read-timeout.
    ///
    /// The bound this needs is a response-first-byte timeout measured from send
    /// completion (as CRT does); `read_timeout` is measured from request
    /// initiation, so it includes the body upload and is size-blind — the wrong
    /// primitive.
    ///
    /// `#[ignore]`d because it hangs: there is no bound to catch. When a response-
    /// first-byte timeout exists, un-ignore it as the regression guard that the
    /// upload response-wait is bounded; the outer timeout keeps a manual
    /// (`--ignored`) run from hanging forever.
    #[tokio::test]
    #[ignore = "documents the unbounded in-memory upload response-wait; un-ignore \
                when a response-first-byte timeout bounds it"]
    async fn stalled_upload_response_wait_is_unbounded_known_gap() {
        // PutObject: the in-memory single-frame body is the case with no bound.
        let (server, handle, tm) = setup(Path::PutObject).await;
        server.insert_fault(
            "test-bucket",
            "obj",
            FaultType::StallRequestRead {
                after_bytes: STALL_AFTER_BYTES,
            },
            0,
            Occurrence::Always,
        );

        // Once a bound exists the upload fails well within this; today it hangs
        // and the outer timeout trips, demonstrating the gap.
        let outcome =
            tokio::time::timeout(Duration::from_secs(15), upload(&tm, "obj", upload_data())).await;
        assert!(
            matches!(outcome, Ok(Err(_))),
            "upload response-wait should be bounded and fail; a timeout here is the \
             unbounded-hang gap this test documents"
        );

        handle.shutdown().await.expect("shutdown");
    }
}
