/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload multi-object integration tests.
//!
//! Unlike `tests/upload_objects_test.rs` in the transfer-manager crate,
//! these tests drive the upload through a real HTTP mock server. That
//! exercises hyper, the connection pool, request signing, and the full
//! SDK request/response path — coverage the smithy interceptor mocks
//! cannot provide.

use std::path::{Path, PathBuf};
use std::time::Duration;

use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::RuntimeMode;
use s3_mock_server::S3MockServer;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::harness::{mock_tm, MockTm};

/// Default test timeout. Any body exceeding this almost certainly
/// reveals a hang or deadlock rather than legitimate slow work.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
}

/// Setup with an explicit concurrency limit, bounding the sustained offered load
/// an installed throttle sees (so a throttled object's re-issues are not drowned
/// out by peers retrying into the same limit).
async fn setup_with_concurrency(
    concurrency: usize,
) -> (
    S3MockServer,
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");

    let handle = server.start().await.expect("start mock server");
    let s3_client = handle.client().await;

    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .concurrency(aws_sdk_s3_transfer_manager::types::ConcurrencyMode::Explicit(concurrency))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (server, handle, tm)
}

/// Create a flat temp directory containing `count` files, each of the
/// same `size` in bytes. Files are named `NNNN.bin` (4-digit zero-padded).
fn make_flat_dataset(count: usize, size: usize) -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for i in 0..count {
        let path = dir.path().join(format!("{i:04}.bin"));
        std::fs::write(&path, vec![0u8; size]).expect("write file");
    }
    dir
}

/// Count objects in the mock server bucket under a given key prefix.
async fn count_objects(server: &S3MockServer, bucket: &str, prefix: &str) -> usize {
    server
        .list_objects(bucket, Some(prefix))
        .await
        .expect("list objects")
        .len()
}

/// Fetch the bytes of a single object from the mock server.
async fn fetch_object_bytes(server: &S3MockServer, bucket: &str, key: &str) -> Vec<u8> {
    server
        .get_object(bucket, key)
        .await
        .expect("get object")
        .expect("object not found")
        .body
        .to_vec()
}

/// Regression coverage for the small-file concurrent-upload hang seen in
/// s3fio benchmarks. The smithy interceptor mocks cannot reproduce this
/// because they short-circuit above hyper; a real HTTP server exposes
/// connection-pool and request-pipeline behavior.
async fn test_upload_objects_many_small_files(rt: RuntimeMode) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let count = 500usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let dataset = make_flat_dataset(count, size);

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("small/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(count as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(
            (count * size) as u64,
            output.metrics.network_tx,
            "network_tx should equal sum of file sizes"
        );

        let landed = count_objects(&m.server, bucket, "small/").await;
        assert_eq!(count, landed, "all files should land in the mock bucket");

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_many_small_files timed out");
}

#[tokio::test]
async fn test_upload_objects_many_small_files_mock_gp() {
    test_upload_objects_many_small_files(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_objects_many_small_files_tokio_mt() {
    test_upload_objects_many_small_files(RuntimeMode::CurrentTokio).await;
}

/// Flat single-directory recursive upload through the real HTTP mock
/// with content verification on every object.
#[tokio::test]
async fn test_upload_objects_flat_directory_content_roundtrip() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let count = 20usize;
        let size = 2 * ByteUnit::Kibibyte.as_bytes_usize();
        let dir = tempfile::tempdir().expect("tempdir");
        // Write a distinct per-index byte pattern so we can verify the
        // mapping from filename to key is correct.
        for i in 0..count {
            let path = dir.path().join(format!("{i:03}.bin"));
            std::fs::write(&path, vec![i as u8; size]).expect("write file");
        }

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("roundtrip/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(count as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());

        // Every key should match its source file's content.
        for i in 0..count {
            let key = format!("roundtrip/{i:03}.bin");
            let got = fetch_object_bytes(&m.server, bucket, &key).await;
            assert_eq!(got, vec![i as u8; size], "content mismatch for key {key}");
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_flat_directory_content_roundtrip timed out");
}

/// Nested directory tree exercises the walker's recursive traversal and
/// the subtree-claim state machine path.
#[tokio::test]
async fn test_upload_objects_nested_tree() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let files: &[(&str, &[u8])] = &[
            ("top.txt", b"top-level"),
            ("a/one.txt", b"a-1"),
            ("a/two.txt", b"a-2"),
            ("b/inner/deep.txt", b"deep-content"),
            ("c/1/2/3/leaf.bin", b"leaf"),
        ];
        for (rel, contents) in files {
            let path: PathBuf = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).expect("mkdirs");
            std::fs::write(&path, contents).expect("write file");
        }

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("tree/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(files.len() as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());

        for (rel, contents) in files {
            let key = format!("tree/{rel}");
            let got = fetch_object_bytes(&m.server, bucket, &key).await;
            assert_eq!(got, contents.to_vec(), "content mismatch for {key}");
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_nested_tree timed out");
}

/// Files above the multipart threshold drive each child through the MPU
/// path. Verifies that the plural state machine + child MPU uploads
/// behave correctly against a real HTTP server.
async fn test_upload_objects_multipart_children(rt: RuntimeMode) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let part = 8 * ByteUnit::Mebibyte.as_bytes_usize();
        let count = 3usize;
        let size = 2 * part; // 16 MiB per file -> 2 parts each at default 8 MiB threshold
        let dataset = make_flat_dataset(count, size);

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("mpu/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(count as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!((count * size) as u64, output.metrics.network_tx);
        assert_eq!((count * size) as u64, output.metrics.disk_read);

        let landed = count_objects(&m.server, bucket, "mpu/").await;
        assert_eq!(count, landed);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_multipart_children timed out");
}

#[tokio::test]
async fn test_upload_objects_multipart_children_mock_gp() {
    test_upload_objects_multipart_children(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_objects_multipart_children_tokio_mt() {
    test_upload_objects_multipart_children(RuntimeMode::CurrentTokio).await;
}

/// Walker filter applied end-to-end: only matching files should land in
/// the bucket.
#[tokio::test]
async fn test_upload_objects_walker_filter_applied() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let entries: &[&str] = &[
            "keep_a.txt",
            "keep_b.txt",
            "skip_a.log",
            "skip_b.log",
            "keep/nested.txt",
            "keep/more.log",
        ];
        for rel in entries {
            let path = dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).expect("mkdirs");
            std::fs::write(&path, b"payload").expect("write file");
        }

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dir.path())
            .walker(
                FsWalker::builder()
                    .recursive(true)
                    .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "txt"))
                    .build(),
            )
            .key_prefix("filtered/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        // keep_a.txt, keep_b.txt, keep/nested.txt -> 3
        assert_eq!(3, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());

        let landed = count_objects(&m.server, bucket, "filtered/").await;
        assert_eq!(3, landed);

        // Confirm none of the .log files made it.
        let s3_client = m.handle.client().await;
        let logs = s3_client
            .list_objects_v2()
            .bucket(bucket)
            .prefix("filtered/")
            .send()
            .await
            .expect("list");
        for obj in logs.contents() {
            let key = obj.key().unwrap_or_default();
            assert!(
                !key.ends_with(".log"),
                "no .log files should have uploaded, found {key}"
            );
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_walker_filter_applied timed out");
}

/// Serial execution (max_concurrent_uploads=1) completes and uploads
/// every file. Exercises the state machine under zero concurrency overlap.
#[tokio::test]
async fn test_upload_objects_serial_execution() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let count = 30usize;
        let size = 512usize;
        let dataset = make_flat_dataset(count, size);

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("serial/")
            .max_concurrent_uploads(1)
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(count as u64, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());

        let landed = count_objects(&m.server, bucket, "serial/").await;
        assert_eq!(count, landed);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_serial_execution timed out");
}

/// Empty source directory terminates cleanly with zero metrics even when
/// driven against a real HTTP server (no requests sent).
#[tokio::test]
async fn test_upload_objects_empty_source() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let bucket = "test-bucket";

        // Create the bucket upfront since no PutObject will fire to
        // auto-create it (empty directory = zero uploads).
        m.server.create_bucket(bucket).await.expect("create bucket");

        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("empty/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle.join().await.expect("join upload_objects");
        assert_eq!(0, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(0, output.metrics.network_tx);
        assert!(
            output.metrics.finished_at.is_some(),
            "finished_at must be set even on zero-work transfer"
        );

        // Create the bucket so count_objects can list it (no PutObject was
        // issued, so the mock server's auto-create-on-put never fired).
        let landed = count_objects(&m.server, bucket, "empty/").await;
        assert_eq!(0, landed);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_empty_source timed out");
}

/// Deep and wide directory tree exercises subtree claiming across many
/// levels. Structure: 4 levels deep, 4 directories per level, 2 files
/// per directory = 4^1 + 4^2 + 4^3 + 4^4 = 340 directories, 680 files.
#[tokio::test]
async fn test_upload_objects_deep_wide_tree() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let dir = tempfile::tempdir().expect("tempdir");
        let mut expected_files = 0u64;

        fn populate(
            base: &Path,
            depth: usize,
            files_per_dir: usize,
            breadth: usize,
            count: &mut u64,
        ) {
            for i in 0..files_per_dir {
                let path = base.join(format!("f{i}.bin"));
                std::fs::write(&path, [0u8; 64]).expect("write");
                *count += 1;
            }
            if depth > 0 {
                for d in 0..breadth {
                    let sub = base.join(format!("d{d}"));
                    std::fs::create_dir(&sub).expect("mkdir");
                    populate(&sub, depth - 1, files_per_dir, breadth, count);
                }
            }
        }

        populate(dir.path(), 3, 2, 4, &mut expected_files);

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("deep/")
            .initiate()
            .expect("initiate");

        let output = handle.join().await.expect("join");
        assert_eq!(expected_files, output.objects_uploaded());
        assert!(output.failed_transfers().is_empty());

        let landed = count_objects(&m.server, bucket, "deep/").await;
        assert_eq!(expected_files as usize, landed);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_deep_wide_tree timed out");
}

/// Abort mid-transfer over real HTTP. Verifies the cancellation cascade
/// propagates through the scheduler to in-flight children without
/// deadlocking or leaking connections.
#[tokio::test]
async fn test_upload_objects_abort_terminates() {
    timeout(TEST_TIMEOUT, async {
        let m = setup().await;

        let count = 200usize;
        let size = 1024usize;
        let dataset = make_flat_dataset(count, size);

        let bucket = "test-bucket";
        let handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("abort/")
            .initiate()
            .expect("initiate");

        // Let some children dispatch before aborting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort().await;

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_objects_abort_terminates timed out");
}

/// Suppress unused warning on `Path` in case the test matrix rotates.
#[allow(dead_code)]
fn _touch(_: &Path) {}

// throttle storm (token-bucket starvation) ------------------------------------

/// Characterizes how `upload_objects` behaves when a concurrent fan-out of small
/// PutObjects is throttled (503 `SlowDown`) across every key — the shape that
/// fails the `256KiB x 10k` benchmark workload.
///
/// Mechanism: throttles ARE retried (throttle backoff + shared refilling bucket),
/// but a PERMANENT storm (`Occurrence::Always`) never relents: the bounded outer
/// retries (`MAX_ATTEMPTS`) exhaust, the child upload fails, and the default
/// `FailedTransferPolicy::Abort` aborts the transfer. The `timeout` guards
/// against a hang regression.
#[tokio::test]
async fn upload_objects_throttle_storm_aborts_transfer() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let count = 200usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let dataset = make_flat_dataset(count, size);
        let bucket = "test-bucket";
        let key_prefix = "storm/";

        // 503 on every object's PutObject, on every attempt: the SDK spends the
        // shared bucket retrying the first objects, then the rest surface the 503
        // un-retried once the bucket is drained.
        for i in 0..count {
            let key = format!("{key_prefix}{i:04}.bin");
            server.insert_fault(
                bucket,
                &key,
                s3_mock_server::FaultType::ServiceError { status: 503 },
                0,
                s3_mock_server::Occurrence::Always,
            );
        }

        let handle = tm
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix(key_prefix)
            .initiate()
            .expect("initiate upload_objects");

        let result = handle.join().await;

        // Current behavior: a persistent throttle storm aborts the whole transfer.
        let err = result
            .expect_err("a persistent 503 storm across the fan-out currently aborts the transfer");
        assert_eq!(
            *err.kind(),
            aws_sdk_s3_transfer_manager::error::ErrorKind::ChildOperationFailed,
            "a throttle storm aborts via a failed child operation, got {err:?}",
        );
        // The top-level Display names the first failing object (not a bare "child
        // operation failed"), so the message points at what to inspect.
        assert!(
            err.to_string().starts_with("child operation failed: "),
            "the abort message must name the failing object, got: {err}",
        );
        // The child's failure is reachable via `failed_uploads()` as a structured
        // `ServiceError`, distinguishing a throttle storm from a transport/IO
        // failure or a generic abort. The service code is `SlowDown` when the SDK
        // surfaces the modeled throttle response, but a drained retry quota can
        // instead surface the terminal 503 as a code-less `ServiceError`, so the
        // reliable assertion is the error KIND. (The root `source()` is only a
        // string today — see the TODO in upload_objects/transfer.rs `abort`.)
        let failed = err
            .failed_uploads()
            .expect("abort under Abort policy attaches the per-object failures");
        assert!(!failed.is_empty(), "at least one child failure recorded");
        let service_failure = failed.iter().any(|f| {
            *f.error().kind() == aws_sdk_s3_transfer_manager::error::ErrorKind::ServiceError
        });
        assert!(
            service_failure,
            "a failed_uploads entry must carry the structured ServiceError from the storm, got: {failed:?}",
        );

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("upload_objects_throttle_storm_aborts_transfer timed out");
}

/// A load-driven throttle storm is recovered: the transfer completes despite the
/// prefix shedding load under the initial fan-out, where the persistent-storm test
/// above aborts. This exercises the throttle classifier (a throttle is retried,
/// not terminal) and the outer throttle-backoff re-issue.
///
/// Uses the mock's load-driven rate throttle (a token bucket: sustained `rate`
/// req/sec with a `burst` allowance, shedding the rest with 503), which models
/// S3's per-prefix request-rate limit and relents *because the client backed off* —
/// not because a fixed request count elapsed:
///
/// - The opening fan-out arrives faster than the bucket refills, so past the
///   `burst` (15) the excess is shed. Across 200 objects that sheds far more than
///   the 100 inner retries the SDK's shared retry bucket can absorb (capacity 500,
///   throttling-retry cost 5), so 503s stop being absorbed by SDK inner retry and
///   surface to the TM's outer loop. That is the machinery under test — with
///   throttles classified `NoRetry` the shed children fail and the transfer aborts
///   (mutation-verified).
/// - The TM's throttle backoff paces re-issues back under `rate`, so the bucket
///   keeps up and requests are served again. Recovery is caused by the backoff.
///   (Rate — not in-flight concurrency — is what accumulates under a burst of
///   near-instant in-memory requests, so it produces real backpressure a
///   concurrency limit would not at this object size.)
/// - Concurrency is fixed (16) so the sustained offered load stays bounded: a
///   throttled object's re-issues are not drowned out by hundreds of peers
///   retrying into the same rate limit, so each recovers within the retry budget.
/// - Which request is shed varies with arrival timing; the behavior (over rate →
///   shed, back off → recover → complete) is deterministic.
///
/// Recovery here is driven by the client's backoff under the rate limit, so it does
/// not by itself depend on the per-bucket token bucket being shared across
/// operations (that isolation + refill fix, and its reuse, is covered by
/// `retry_partition_is_cached_per_bucket` in the transfer-manager crate).
#[tokio::test]
async fn upload_objects_transient_throttle_storm_recovers() {
    timeout(TEST_TIMEOUT, async {
        // Fixed concurrency bounds the sustained offered load so throttled objects'
        // re-issues recover within the retry budget rather than being drowned out by
        // peers retrying into the same rate limit.
        let (server, server_handle, tm) = setup_with_concurrency(16).await;

        let count = 200usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let dataset = make_flat_dataset(count, size);
        let bucket = "test-bucket";

        // Sustained 50 req/sec, burst 15: the fan-out overruns it and sheds well
        // past the 100 inner retries the shared SDK bucket can absorb (draining it
        // so 503s reach the TM loop); the TM's throttle backoff then paces re-issues
        // under 50/sec to recover.
        server.set_rate_throttle(50.0, 15.0);

        let handle = tm
            .upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix("recover/")
            .initiate()
            .expect("initiate upload_objects");

        let output = handle
            .join()
            .await
            .expect("a load-driven throttle storm must be recovered via backoff, not aborted");
        assert_eq!(
            output.objects_uploaded(),
            count as u64,
            "every object must land once backoff relieves the load"
        );
        assert!(
            output.failed_transfers().is_empty(),
            "no object should be left failed after recovery"
        );

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("upload_objects_transient_throttle_storm_recovers timed out");
}

/// Download counterpart to `upload_objects_transient_throttle_storm_recovers`.
/// For a small single-part object, discovery is the sole request, so a
/// throttle storm on discovery must recover via the TM retry loop — parity with
/// the upload path. Drives the same load-driven storm the upload test survives.
#[tokio::test]
async fn download_objects_transient_throttle_storm_recovers() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup_with_concurrency(16).await;
        let count = 200usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "recover-dl/";

        // Seed the store with no throttle installed.
        let dataset = make_flat_dataset(count, size);
        tm.upload_objects()
            .bucket(bucket)
            .source(dataset.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate seed upload")
            .join()
            .await
            .expect("seed upload must succeed");

        // Install the load-driven storm: every request is rate-limited server-wide.
        server.set_rate_throttle(50.0, 15.0);

        let dest = TempDir::new().expect("dest tempdir");
        let handle = tm
            .download_objects()
            .bucket(bucket)
            .key_prefix(prefix)
            .destination(dest.path())
            .initiate()
            .expect("initiate download_objects");

        let output = handle
            .join()
            .await
            .expect("a load-driven throttle storm on download must recover (parity with upload)");
        assert_eq!(output.objects_downloaded(), count as u64);
        assert!(output.failed_transfers().is_empty());
        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("download_objects_transient_throttle_storm_recovers timed out");
}
