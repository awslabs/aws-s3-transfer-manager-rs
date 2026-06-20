/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download body-read retry contract tests.
//!
//! A chunk's body is consumed after the SDK's GetObject orchestration completes,
//! so the SDK's own retry does not cover a mid-stream body failure. The transfer
//! manager wraps each ranged GET in its own retry loop
//! (`crate::retry::classify_body_retry`): a transient body-read failure re-issues
//! the GET; a checksum mismatch does not retry.
//!
//! These tests inject real transport faults via the mock server and assert the
//! transfer outcome. A fault injected `Once` lets a single download succeed (the
//! chunk is re-issued within the download); the same fault injected `Always`
//! fails the download. `Occurrence::NTimes` brackets the attempt budget by
//! outcome (see `range_retries_*`).
//!
//! Sizing: a transport fault on a small/buffered response surfaces at `send()`
//! as an `SdkError`, which the classifier treats as terminal (the SDK already
//! retried it). The body-read retry path runs only when the fault surfaces
//! mid-body, which needs a response large enough that the SDK streams rather than
//! buffers it. These tests use multi-megabyte objects and place the fault well
//! into the stream.
//!
//! `corrupt_body_once_is_not_retried` and `corrupt_body_always_fails` cover the
//! checksum path: a corrupt body fails the download and is not retried. Both
//! assert the failure is `ErrorKind::IntegrityError`, not merely that it failed.
//!
//! That assertion is also the standing guard for the classifier's source-chain
//! downcast (`error::checksum_mismatch_values`), which is fragile across
//! `aws-smithy-checksums` versions (a `0.x` minor bump changes the `TypeId` and
//! the downcast misses — see https://github.com/smithy-lang/smithy-rs/issues/4718).
//! These run against the version `aws-sdk-s3` actually links, so a bump that broke
//! the downcast turns the mismatch into a retryable I/O error and fails the
//! `IntegrityError` assertion here — making the break CI-visible rather than silent.

use crate::assertions::{assert_integrity_error, assert_io_error, assert_same_content};
use crate::harness::Target;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::PartSize;
use aws_sdk_s3_transfer_manager::Client as TmClient;
use s3_mock_server::{FaultType, Occurrence, S3MockServer};

/// Part size pinned equally for upload and download so multipart download ranges
/// align to the uploaded part boundaries (the precondition for per-part
/// validation, and for a deterministic chunk layout the fault can target).
const PART_SIZE: PartSize = PartSize::Target(8 * 1024 * 1024);

/// A multipart object large enough that a fault placed mid-stream surfaces during
/// body iteration rather than at `send()`.
fn multipart_data() -> Vec<u8> {
    (0..40 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

/// Byte offset into a chunk at which a transport fault fires: past the point the
/// SDK buffers a response, so the failure lands mid-body and reaches the TM's
/// body-read retry rather than the SDK's send-time retry.
const FAULT_AFTER_BYTES: u64 = 6 * 1024 * 1024;

/// Skip the discovery GET (always the first GET) so a fault lands on a RANGE
/// chunk. The discovery chunk and range chunks retry through separate code paths
/// (`execute_read_discovery_body` vs `execute_get_range`); these targeted faults
/// exercise the range path, and `discovery_chunk_*` below exercise chunk 0.
const SKIP_DISCOVERY: u32 = 1;

/// The transfer manager's per-chunk attempt budget, mirrored from the (crate-
/// private) `retry::MAX_ATTEMPTS`. The `NTimes` bracket tests below pin the budget
/// by outcome: a fault firing `BUDGET - 1` times is recovered by the final clean
/// attempt; firing `BUDGET` times exhausts and fails. If the TM's budget changes,
/// the bracket tests fail loudly and this must be updated to match.
const BUDGET: u32 = 3;

/// An object that downloads as discovery (8 MiB) + exactly ONE range chunk
/// (4 MiB). With a single range chunk, an `NTimes` fault (skipping discovery) can
/// only fire against that chunk's attempts, so the number of fires maps directly
/// to retry attempts — no ambiguity about which chunk a fire hit.
fn single_range_chunk_data() -> Vec<u8> {
    (0..12 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

/// Fault offset for the single-range-chunk object. The lone range chunk is 4 MiB,
/// so the fault must fire below that; it still lands mid-body (past the send
/// buffer) so it surfaces as a body read.
const SINGLE_RANGE_FAULT_AFTER: u64 = 2 * 1024 * 1024;

// transient transport fault, Once -> retry recovers ----------------------------

/// A truncated body on one chunk, injected `Once`, is recovered by re-issuing the
/// ranged GET: a single download succeeds with correct bytes.
#[tokio::test]
async fn truncate_body_once_recovers() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: FAULT_AFTER_BYTES,
        },
        SKIP_DISCOVERY,
        Occurrence::Once,
    );

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download recovers via retry");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

/// A connection reset on one chunk, injected `Once`, is recovered by retry.
#[tokio::test]
async fn connection_reset_once_recovers() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::ConnectionReset {
            after_bytes: FAULT_AFTER_BYTES,
        },
        SKIP_DISCOVERY,
        Occurrence::Once,
    );

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download recovers via retry");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

/// A short body on one chunk, injected `Once`, is recovered by retry.
#[tokio::test]
async fn short_body_once_recovers() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::ShortBody {
            actual_bytes: FAULT_AFTER_BYTES,
        },
        SKIP_DISCOVERY,
        Occurrence::Once,
    );

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download recovers via retry");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

// persistent transport fault, Always -> retries exhaust, transfer fails ---------

/// A truncated body injected `Always` is retried to the cap and then fails the
/// transfer (the inverse of the `Once` recovery, proving the retry is bounded).
#[tokio::test]
async fn truncate_body_always_exhausts_and_fails() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: FAULT_AFTER_BYTES,
        },
        SKIP_DISCOVERY,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_io_error(result);

    t.shutdown().await;
}

// checksum mismatch -> not retried --------------------------------------------

/// A corrupt body (bytes altered, checksum intact) injected `Always` fails the
/// download. The classifier walks the source chain to a checksum
/// `validate::Error`, so this also exercises that downcast against the linked
/// `aws-smithy-checksums` version.
#[tokio::test]
async fn corrupt_body_always_fails() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        SKIP_DISCOVERY,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

/// A corrupt body injected `Once` fails the download: a checksum mismatch is not
/// retried. If the classifier treated a mismatch as a retryable I/O error, the
/// `Once` fault would clear on the retry and the download would succeed; it must
/// fail instead. The `Once`/transport-fault tests above recover under the same
/// setup, so this distinguishes the checksum path from the transport path.
#[tokio::test]
async fn corrupt_body_once_is_not_retried() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        SKIP_DISCOVERY,
        Occurrence::Once,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

// discovery chunk (chunk 0) faults -> retried through the discovery path --------
//
// The discovery GET fetches metadata AND the first chunk's body in one request.
// A mid-body failure there is recovered by `execute_read_discovery_body`'s retry:
// the first read uses the body discovery already fetched (no extra request), and
// a retry re-issues a ranged GET for the discovery chunk's byte range. No `skip`,
// so the fault lands on chunk 0.

/// A transport fault on the discovery chunk, injected `Once`, is recovered: a
/// single download succeeds. Proves the discovery body read retries, not just the
/// range chunks.
#[tokio::test]
async fn discovery_chunk_truncate_once_recovers() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: FAULT_AFTER_BYTES,
        },
        0, // chunk 0 = the discovery chunk
        Occurrence::Once,
    );

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("discovery chunk recovers via retry");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

/// A persistent transport fault on the discovery chunk exhausts the retries and
/// fails the transfer (the discovery path's retry is bounded, like the range
/// path's).
#[tokio::test]
async fn discovery_chunk_truncate_always_fails() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: FAULT_AFTER_BYTES,
        },
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_io_error(result);

    t.shutdown().await;
}

// retry budget bracketed by outcome via Occurrence::NTimes --------------------
//
// Asserting an exact request COUNT is fragile under concurrent ranged GETs.
// Instead these bracket the budget by deterministic OUTCOME on a single-range
// object: a fault firing `BUDGET - 1` times leaves a final clean attempt (the
// download must SUCCEED), while firing `BUDGET` times exhausts every attempt
// (the download must FAIL). Together they pin the budget at exactly `BUDGET`
// without observing a count. `SKIP_DISCOVERY` so the fires target the lone range
// chunk's retries.

/// `BUDGET - 1` fires: the final (BUDGET-th) attempt is clean, so the range chunk
/// recovers and the download succeeds. Proves the budget is at least `BUDGET`.
#[tokio::test]
async fn range_retries_up_to_budget_recover() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = single_range_chunk_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: SINGLE_RANGE_FAULT_AFTER,
        },
        SKIP_DISCOVERY,
        Occurrence::NTimes(BUDGET - 1),
    );

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("final attempt within budget recovers");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

/// `BUDGET` fires: every attempt fails, so retries exhaust and the download
/// fails. Proves the budget is at most `BUDGET` (the chunk is not retried beyond
/// it). Paired with the test above, this pins the budget at exactly `BUDGET`.
#[tokio::test]
async fn range_retries_exhaust_at_budget_fail() {
    let t = Target::mock_gp().connect_with(Some(PART_SIZE)).await;
    let data = single_range_chunk_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::TruncateBody {
            after_bytes: SINGLE_RANGE_FAULT_AFTER,
        },
        SKIP_DISCOVERY,
        Occurrence::NTimes(BUDGET),
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_io_error(result);

    t.shutdown().await;
}

// --- Per-bucket retry partition -------------------------------------------

use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::retry::RetryPartition;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};

/// Captures the `RetryPartition` the SDK resolves per request, keyed by URI.
#[derive(Debug, Clone)]
struct PartitionRecorder {
    seen: Arc<Mutex<Vec<(String, String)>>>,
}

impl PartitionRecorder {
    fn new() -> Self {
        Self {
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn partitions_for(&self, bucket: &str) -> Vec<String> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(uri, _)| uri.contains(bucket))
            .map(|(_, partition)| partition.clone())
            .collect()
    }
}

impl Intercept for PartitionRecorder {
    fn name(&self) -> &'static str {
        "PartitionRecorder"
    }

    // Reads after `config_override` has merged the partition into the bag.
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(partition) = cfg.load::<RetryPartition>() {
            self.seen
                .lock()
                .unwrap()
                .push((context.request().uri().to_string(), partition.to_string()));
        }
        Ok(())
    }
}

/// The TM keys the SDK retry partition per bucket (`s3-tm-{bucket}`) via a
/// per-operation `config_override`, so each bucket has an independent retry
/// token bucket. This asserts the resolved partition directly: distinct
/// partitions are distinct token buckets by the SDK's guarantee, so budget
/// isolation follows. (The runtime drain-and-observe effect is not asserted —
/// the adaptive bucket recovers between retries, so it can't be pinned without
/// races.)
#[tokio::test]
async fn requests_carry_per_bucket_retry_partition() {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");

    let recorder = PartitionRecorder::new();
    let s3 = {
        let base = handle.client().await;
        let conf = base
            .config()
            .to_builder()
            .interceptor(recorder.clone())
            .build();
        aws_sdk_s3::Client::from_conf(conf)
    };
    let tm = TmClient::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .client(s3)
            .part_size(PART_SIZE)
            .build(),
    );

    let run = uuid::Uuid::new_v4();
    let bucket_a = format!("alpha-{run}");
    let bucket_b = format!("bravo-{run}");
    let key = "obj";
    let data = single_range_chunk_data();
    for bucket in [&bucket_a, &bucket_b] {
        server.create_bucket(bucket).await.expect("create bucket");
        server
            .add_object(bucket, key, data.clone(), None)
            .await
            .expect("seed object");
    }

    tm_download(&tm, &bucket_a, key).await.expect("download A");
    tm_download(&tm, &bucket_b, key).await.expect("download B");

    let a_parts = recorder.partitions_for(&bucket_a);
    let b_parts = recorder.partitions_for(&bucket_b);
    let expected_a = format!("s3-tm-{bucket_a}");
    let expected_b = format!("s3-tm-{bucket_b}");

    assert!(!a_parts.is_empty(), "no requests recorded for bucket A");
    assert!(!b_parts.is_empty(), "no requests recorded for bucket B");
    assert!(
        a_parts.iter().all(|p| *p == expected_a),
        "bucket A used {a_parts:?}, expected all {expected_a}",
    );
    assert!(
        b_parts.iter().all(|p| *p == expected_b),
        "bucket B used {b_parts:?}, expected all {expected_b}",
    );
    assert_ne!(expected_a, expected_b, "partitions must differ per bucket");

    handle.shutdown().await.expect("shutdown mock server");
    drop(server);
}

async fn tm_download(tm: &TmClient, bucket: &str, key: &str) -> Result<Vec<u8>, ()> {
    let mut handle = tm
        .download()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .initiate()
        .map_err(|_| ())?;
    let mut data = Vec::new();
    while let Some(chunk) = handle.body_mut().next().await {
        match chunk {
            Ok(c) => data.extend_from_slice(&c.data.into_bytes()),
            Err(_) => {
                let _ = handle.join().await;
                return Err(());
            }
        }
    }
    handle.join().await.map(|_| data).map_err(|_| ())
}
