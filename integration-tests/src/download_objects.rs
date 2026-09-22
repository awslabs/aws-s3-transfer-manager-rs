/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download multi-object integration tests.
//!
//! Exercises `download_objects` through a real HTTP mock server, covering
//! listing pagination, key-to-path mapping, content integrity, failure
//! policies, and path-safety invariants. Structured as the download-side
//! mirror of `upload_objects.rs`.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use aws_sdk_s3_transfer_manager::io::walk::S3Walker;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::{
    ByteTotal, EntryTotal, FailedTransferPolicy, RuntimeMode,
};
use s3_mock_server::{FaultType, Occurrence, S3MockServer};

use aws_sdk_s3_transfer_manager::events::{Decision, Endpoint, Outcome, TransferEvent};

use crate::harness::mock_tm;
use tokio::time::timeout;

/// Default test timeout.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Seed `count` objects into the mock bucket under `prefix`, each `size` bytes.
/// Objects are named `{prefix}{NNNN}.bin` with distinct per-index byte patterns.
async fn seed_bucket(server: &S3MockServer, bucket: &str, prefix: &str, count: usize, size: usize) {
    server.create_bucket(bucket).await.expect("create bucket");
    for i in 0..count {
        let key = format!("{prefix}{i:04}.bin");
        let body = vec![i as u8; size];
        server
            .add_object(bucket, &key, body, None)
            .await
            .expect("seed object");
    }
}

/// Seed objects with explicit key/content pairs.
async fn seed_objects(server: &S3MockServer, bucket: &str, objects: &[(&str, &[u8])]) {
    server.create_bucket(bucket).await.expect("create bucket");
    for (key, content) in objects {
        server
            .add_object(bucket, key, content.to_vec(), None)
            .await
            .expect("seed object");
    }
}

/// Assert that the downloaded directory contains exactly the expected files
/// with correct byte content. `expected` maps relative paths (within the
/// download directory) to their byte content.
fn verify_dir(dir: &Path, expected: &HashMap<&str, Vec<u8>>) {
    let mut found: HashMap<PathBuf, Vec<u8>> = HashMap::new();
    collect_files(dir, dir, &mut found);

    assert_eq!(
        found.len(),
        expected.len(),
        "file count mismatch: found {} files, expected {}.\nFound: {:?}",
        found.len(),
        expected.len(),
        found.keys().collect::<Vec<_>>()
    );

    for (rel, content) in expected {
        let rel_path = PathBuf::from(rel);
        let got = found
            .get(&rel_path)
            .unwrap_or_else(|| panic!("expected file {rel} not found in download dir"));
        assert_eq!(got, content, "content mismatch for file {rel}");
    }
}

/// Recursively collect files under `base`, storing their paths relative to
/// `root` and their content.
fn collect_files(root: &Path, dir: &Path, out: &mut HashMap<PathBuf, Vec<u8>>) {
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out);
        } else {
            let rel = path.strip_prefix(root).expect("strip prefix").to_path_buf();
            let content = std::fs::read(&path).expect("read file");
            out.insert(rel, content);
        }
    }
}

/// Count files recursively under `dir`.
fn count_files(dir: &Path) -> usize {
    let mut count = 0usize;
    count_files_inner(dir, &mut count);
    count
}

fn count_files_inner(dir: &Path, count: &mut usize) {
    if !dir.exists() {
        return;
    }
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            count_files_inner(&path, count);
        } else {
            *count += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// PARITY SET (mirrors upload_objects.rs)
// ---------------------------------------------------------------------------

/// Seed N small objects, download them all, verify count and disk presence.
async fn test_download_objects_many_small_files(rt: RuntimeMode) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let count = 500usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "small/";

        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        let files_on_disk = count_files(dest.path());
        assert_eq!(count, files_on_disk, "all objects should land on disk");

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_many_small_files timed out");
}

#[tokio::test]
async fn test_download_objects_many_small_files_mock_gp() {
    test_download_objects_many_small_files(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_many_small_files_tokio_mt() {
    test_download_objects_many_small_files(RuntimeMode::MultiThreadTokio).await;
}

/// Per-file byte-content integrity after download.
#[tokio::test]
async fn test_download_objects_flat_content_roundtrip() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 20usize;
        let size = 2 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "roundtrip/";

        m.server.create_bucket(bucket).await.expect("create bucket");
        for i in 0..count {
            let key = format!("{prefix}{i:03}.bin");
            let body = vec![i as u8; size];
            m.server
                .add_object(bucket, &key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        // Verify byte content of every file.
        for i in 0..count {
            let file_path = dest.path().join(format!("{i:03}.bin"));
            let got = std::fs::read(&file_path).unwrap_or_else(|_| {
                panic!("expected file {:03}.bin not found on disk", i);
            });
            assert_eq!(
                got,
                vec![i as u8; size],
                "content mismatch for file {:03}.bin",
                i
            );
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_flat_content_roundtrip timed out");
}

/// Objects under nested prefixes download into the correct directory tree.
#[tokio::test]
async fn test_download_objects_nested_prefixes() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let prefix = "tree/";
        let objects: &[(&str, &[u8])] = &[
            ("tree/top.txt", b"top-level"),
            ("tree/a/one.txt", b"a-1"),
            ("tree/a/two.txt", b"a-2"),
            ("tree/b/inner/deep.txt", b"deep-content"),
            ("tree/c/1/2/3/leaf.bin", b"leaf"),
        ];
        seed_objects(&m.server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(objects.len() as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        // Verify directory structure and content.
        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("top.txt", b"top-level".to_vec());
        expected.insert("a/one.txt", b"a-1".to_vec());
        expected.insert("a/two.txt", b"a-2".to_vec());
        expected.insert("b/inner/deep.txt", b"deep-content".to_vec());
        expected.insert("c/1/2/3/leaf.bin", b"leaf".to_vec());
        verify_dir(dest.path(), &expected);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_nested_prefixes timed out");
}

/// Objects large enough to trigger multipart download, content verified.
async fn test_download_objects_multipart_children(rt: RuntimeMode) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let part = 8 * ByteUnit::Mebibyte.as_bytes_usize();
        let count = 3usize;
        let size = 2 * part; // 16 MiB per file -> 2 parts each
        let bucket = "test-bucket";
        let prefix = "mpu/";

        m.server.create_bucket(bucket).await.expect("create bucket");
        for i in 0..count {
            let key = format!("{prefix}{i:04}.bin");
            // Deterministic content pattern (byte index mod 256).
            let body: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
            m.server
                .add_object(bucket, &key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(
            (count * size) as u64,
            output.metrics.network_rx,
            "network_rx should equal sum of object sizes"
        );
        assert_eq!(
            (count * size) as u64,
            output.metrics.disk_write,
            "disk_write should equal sum of object sizes"
        );

        // Verify content of each downloaded file.
        let expected_body: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
        for i in 0..count {
            let file_path = dest.path().join(format!("{i:04}.bin"));
            let got = std::fs::read(&file_path)
                .unwrap_or_else(|_| panic!("expected file {:04}.bin not found on disk", i));
            assert_eq!(got, expected_body, "content mismatch for {:04}.bin", i);
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_multipart_children timed out");
}

#[tokio::test]
async fn test_download_objects_multipart_children_mock_gp() {
    test_download_objects_multipart_children(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_multipart_children_tokio_mt() {
    test_download_objects_multipart_children(RuntimeMode::MultiThreadTokio).await;
}

/// Empty prefix yields zero downloads with no error.
#[tokio::test]
async fn test_download_objects_empty_prefix() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        m.server.create_bucket(bucket).await.expect("create bucket");

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix("nonexistent/")
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(0, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());
        assert_eq!(0, output.metrics.network_rx);
        assert!(
            output.metrics.finished_at.is_some(),
            "finished_at must be set even on zero-work transfer"
        );

        let files_on_disk = count_files(dest.path());
        assert_eq!(0, files_on_disk);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_empty_prefix timed out");
}

/// Deep and wide prefix tree exercises listing across many keys.
/// Structure: 4 levels deep, 4 prefixes per level, 2 objects per prefix
/// = 4^1 + 4^2 + 4^3 + 4^4 = 340 prefixes, 680 objects.
#[tokio::test]
async fn test_download_objects_deep_wide_tree() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let root_prefix = "deep/";
        m.server.create_bucket(bucket).await.expect("create bucket");

        let mut expected_count = 0u64;

        // Iteratively seed a deep/wide tree to avoid recursive async lifetime issues.
        let mut prefixes_to_seed: Vec<(String, usize)> = vec![(root_prefix.to_string(), 3)];
        while let Some((pfx, depth)) = prefixes_to_seed.pop() {
            for i in 0..2usize {
                let key = format!("{pfx}f{i}.bin");
                m.server
                    .add_object(bucket, &key, vec![0u8; 64], None)
                    .await
                    .expect("seed");
                expected_count += 1;
            }
            if depth > 0 {
                for d in 0..4usize {
                    let sub = format!("{pfx}d{d}/");
                    prefixes_to_seed.push((sub, depth - 1));
                }
            }
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(root_prefix)
            .initiate()
            .expect("initiate");

        let output = handle.join().await.expect("join");
        assert_eq!(expected_count, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        let files_on_disk = count_files(dest.path());
        assert_eq!(expected_count as usize, files_on_disk);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_deep_wide_tree timed out");
}

/// Abort mid-transfer terminates cleanly without deadlock.
#[tokio::test]
async fn test_download_objects_abort_terminates() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 200usize;
        let size = 1024usize;
        let bucket = "test-bucket";
        let prefix = "abort/";

        seed_bucket(&m.server, bucket, prefix, count, size).await;

        // Inject faults on the first key so the download hangs/fails if not aborted.
        m.server.insert_fault(
            bucket,
            &format!("{prefix}0000.bin"),
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Abort)
            .initiate()
            .expect("initiate");

        // The default Abort policy should propagate the error.
        // Whether it auto-aborts or we abort explicitly, it should terminate.
        let result = handle.join().await;
        // Under Abort policy with a faulted key, join returns an error.
        assert!(
            result.is_err(),
            "expected error due to faulted key under Abort policy"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_abort_terminates timed out");
}

/// Under `Abort`, the error `join()` returns must reach the child's real cause.
///
/// Closes the TODO that stood at `download_objects/transfer.rs:902` and `:1264`. Its stated
/// blocker — *"needs a shareable error (`Arc`) since `Error` is not `Clone`"* — went away when
/// `Error` gained `Clone`, but the code did not follow, so the root error's `source()` stayed a
/// formatted string.
///
/// What this rules out: a caller whose bulk download aborts runs `DisplayErrorContext` over what
/// `join()` gave back, and the chain dead-ends at `"download failed for key 'k'"` — the status
/// code, the request id and the service message all unreachable, so the one thing needed to tell a
/// 503 from a 403 is missing from the only error the caller was handed.
///
/// Asserted structurally rather than on message text: the root's immediate `source()` must
/// downcast to the library's own [`Error`], which a `String` source cannot do.
#[tokio::test]
async fn test_download_objects_abort_error_reaches_the_child_cause() {
    use std::error::Error as _;

    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 8usize;
        let size = 1024usize;
        let bucket = "test-bucket";
        let prefix = "abort-cause/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        m.server.insert_fault(
            bucket,
            &format!("{prefix}0000.bin"),
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let err = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Abort)
            .initiate()
            .expect("initiate")
            .join()
            .await
            .expect_err("a faulted key under Abort must fail the operation");

        let source = err.source().expect("the root error owes a source");
        let child = source
            .downcast_ref::<aws_sdk_s3_transfer_manager::error::Error>()
            .unwrap_or_else(|| panic!("root source must be the child's own Error, got: {source}"));
        // And the child's own chain continues past it, which is where the status code lives.
        assert!(
            child.source().is_some(),
            "the child error must keep its own source: {child}"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_abort_error_reaches_the_child_cause timed out");
}

// ---------------------------------------------------------------------------
// DOWNLOAD-SPECIFIC TESTS
// ---------------------------------------------------------------------------

/// Seed enough objects to force ListObjectsV2 pagination, verify all objects
/// across pages are discovered and downloaded. Uses `S3Walker::builder().page_size()`
/// to force a small page size rather than seeding >1000 objects.
async fn test_download_objects_listing_pagination(rt: RuntimeMode) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let count = 50usize;
        let size = 128usize;
        let bucket = "test-bucket";
        let prefix = "paginated/";

        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        // Force page_size=5 so that 50 objects require 10 list pages.
        let walker = S3Walker::builder().page_size(5).build();
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .walker(walker)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        let files_on_disk = count_files(dest.path());
        assert_eq!(
            count, files_on_disk,
            "all paginated objects should land on disk"
        );

        // Verify content of a sample file to ensure no mix-up across pages.
        let sample_path = dest.path().join("0025.bin");
        let got = std::fs::read(&sample_path).expect("read sample file");
        assert_eq!(got, vec![25u8; size], "sample content should match");

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_listing_pagination timed out");
}

#[tokio::test]
async fn test_download_objects_listing_pagination_mock_gp() {
    test_download_objects_listing_pagination(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_listing_pagination_tokio_mt() {
    test_download_objects_listing_pagination(RuntimeMode::MultiThreadTokio).await;
}

/// FailedTransferPolicy::Continue with one faulted key: the other objects
/// succeed and the faulted key appears in `failed_transfers()`.
#[tokio::test]
async fn test_download_objects_continue_policy_partial_failure() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let prefix = "partial/";
        let good_count = 10usize;
        let size = 512usize;

        seed_bucket(&m.server, bucket, prefix, good_count, size).await;
        // Add one extra object that will be faulted.
        let bad_key = format!("{prefix}bad.bin");
        m.server
            .add_object(bucket, &bad_key, vec![0xFFu8; size], None)
            .await
            .expect("seed bad object");

        // Inject a permanent service error on the bad key.
        m.server.insert_fault(
            bucket,
            &bad_key,
            FaultType::ServiceError { status: 500 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        // The good objects should succeed.
        assert_eq!(good_count as u64, output.objects_downloaded());
        // Exactly one failure: the bad key.
        assert_eq!(
            1,
            output.failed_transfers().len(),
            "expected exactly one failed transfer"
        );
        let failed = &output.failed_transfers()[0];
        let failed_key = failed.input().key().expect("failed input should have key");
        assert_eq!(
            failed_key, bad_key,
            "failed key should be the faulted object"
        );

        // Verify good files are on disk.
        let files_on_disk = count_files(dest.path());
        assert_eq!(good_count, files_on_disk);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_continue_policy_partial_failure timed out");
}

/// SECURITY: keys with path traversal components must NOT write files outside
/// the destination directory. The product code uses `path_clean` + validation
/// to reject such keys. If a key resolves outside the root, it should fail
/// rather than escape.
#[tokio::test]
async fn test_download_objects_key_path_safety() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let prefix = "safe/";
        // Include a known-good object alongside traversal keys.
        let objects: &[(&str, &[u8])] = &[
            ("safe/good.txt", b"safe-content"),
            ("safe/../escape.bin", b"escape-attempt-1"),
            ("safe/../../etc/passwd", b"escape-attempt-2"),
            ("safe/sub/../../other.bin", b"escape-attempt-3"),
        ];
        seed_objects(&m.server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let dest_path = dest.path().to_path_buf();

        // Use Continue policy so we can inspect which keys failed vs succeeded.
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(&dest_path)
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");

        // The good file must land on disk inside the destination.
        let good_path = dest_path.join("good.txt");
        assert!(good_path.exists(), "good.txt should exist on disk");
        assert_eq!(
            std::fs::read(&good_path).expect("read good.txt"),
            b"safe-content"
        );

        // CRITICAL SAFETY ASSERTION: no file must exist outside the destination.
        // Walk the parent directory of dest and check that no unexpected files landed.
        let parent = dest_path.parent().expect("dest has parent");
        for entry in std::fs::read_dir(parent).expect("read parent dir") {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            // The only entry under parent that we care about is our dest dir itself.
            if path != dest_path {
                // Check that no file from our traversal keys landed here.
                if path.is_file() {
                    let name = path.file_name().unwrap().to_string_lossy();
                    assert!(
                        name != "escape.bin" && name != "passwd" && name != "other.bin",
                        "SECURITY BUG: path traversal escape detected! File landed at {path:?}"
                    );
                }
            }
        }

        // The traversal keys should appear as failures (InputInvalid).
        assert!(
            !output.failed_transfers().is_empty(),
            "traversal keys should fail rather than silently succeed"
        );
        // At minimum the good object succeeded.
        assert!(
            output.objects_downloaded() >= 1,
            "at least the safe object should download"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_key_path_safety timed out");
}

// ---------------------------------------------------------------------------
// ROUND-TRIP
// ---------------------------------------------------------------------------

/// Build a nested directory tree, upload it, download it to a fresh dir,
/// and assert the two trees are byte-identical.
async fn test_upload_then_download_objects_roundtrip(rt: RuntimeMode) {
    timeout(Duration::from_secs(120), async {
        let m = mock_tm(rt).await;

        let bucket = "test-bucket";
        let prefix = "rt/";

        // Build a source directory tree with varied file sizes.
        let src_dir = tempfile::tempdir().expect("tempdir");
        let files: &[(&str, usize)] = &[
            ("small.txt", 128),
            ("medium.bin", 64 * 1024),
            ("sub/nested.dat", 32 * 1024),
            ("sub/deep/leaf.bin", 256),
            ("another/file.txt", 1024),
        ];
        let mut expected_content: HashMap<&str, Vec<u8>> = HashMap::new();
        for (rel, size) in files {
            let path = src_dir.path().join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).expect("mkdirs");
            let body: Vec<u8> = (0..*size).map(|i| (i % 251) as u8).collect();
            std::fs::write(&path, &body).expect("write source file");
            expected_content.insert(rel, body);
        }

        // Upload the directory tree.
        use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
        let upload_handle = m
            .client
            .upload_objects()
            .bucket(bucket)
            .source(src_dir.path())
            .walker(FsWalker::builder().recursive(true).build())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate upload_objects");

        let upload_output = upload_handle.join().await.expect("join upload_objects");
        assert_eq!(files.len() as u64, upload_output.objects_uploaded());
        assert!(upload_output.failed_transfers().is_empty());

        // Download to a fresh directory.
        let dest_dir = tempfile::tempdir().expect("tempdir");
        let download_handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest_dir.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let download_output = download_handle.join().await.expect("join download_objects");
        assert_eq!(files.len() as u64, download_output.objects_downloaded());
        assert!(download_output.failed_transfers().is_empty());

        // Verify the downloaded tree matches the source tree exactly.
        verify_dir(dest_dir.path(), &expected_content);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_then_download_objects_roundtrip timed out");
}

#[tokio::test]
async fn test_upload_then_download_objects_roundtrip_mock_gp() {
    test_upload_then_download_objects_roundtrip(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_then_download_objects_roundtrip_tokio_mt() {
    test_upload_then_download_objects_roundtrip(RuntimeMode::MultiThreadTokio).await;
}

// ---------------------------------------------------------------------------
// RUST-1224: lifecycle events for `download_objects`.
//
// The download side is where the event's byte count is easiest to get wrong.
// `TransferMetrics` carries four counters and a download populates `network_rx`,
// not `network_tx` — so an emit copy-pasted from `upload_objects` compiles, runs,
// and reports every object as having transferred zero bytes. The byte assertions
// below are the regression guard for exactly that.
// ---------------------------------------------------------------------------

/// One `Decided` per object plus the root, each finishing exactly once, with
/// byte counts that match what was actually downloaded.
#[tokio::test]
async fn test_download_objects_events_pair_and_report_bytes() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 12usize;
        let size = 8 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "events/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        // 2 per transfer (Decided + Settled) plus the root's pair, so nothing
        // is dropped for want of room and the counts below are exact.
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        let collector = tokio::spawn(async move {
            let mut evs = Vec::new();
            while let Some(ev) = stream.next().await {
                evs.push(ev);
            }
            (evs, stream.dropped())
        });

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        let (events, dropped) = collector.await.expect("collector");
        assert_eq!(0, dropped, "capacity 2*(n+1) must lose nothing");

        // Keyed by id, holding the destination each event named, so the two halves
        // of a pair can be checked against each other.
        let mut decided: HashMap<u64, PathBuf> = HashMap::new();
        let mut settled: HashMap<u64, u64> = HashMap::new();
        let mut root_id: Option<u64> = None;

        for ev in &events {
            let dest_path = match ev.transfer().destination() {
                Endpoint::Local { path, .. } => path.to_path_buf(),
                other => panic!("a download writes to a local file, got {other:?}"),
            };
            assert!(
                matches!(ev.decision(), Decision::Transfer { .. }),
                "every event of a download_objects run decides a transfer: {ev:?}"
            );
            match ev {
                TransferEvent::Decided { id, parent, .. } => {
                    assert!(
                        decided.insert(*id, dest_path).is_none(),
                        "id {id} announced twice"
                    );
                    if parent.is_none() {
                        root_id = Some(*id);
                    }
                }
                TransferEvent::Settled { id, outcome, .. } => {
                    *settled.entry(*id).or_default() += 1;
                    assert!(
                        matches!(outcome, Outcome::Succeeded { .. }),
                        "id {id} must succeed, got {outcome:?}"
                    );
                }
                _ => {}
            }
        }

        assert_eq!(
            count + 1,
            decided.len(),
            "one decision per object plus the root"
        );
        assert_eq!(
            decided.len(),
            settled.len(),
            "every announced transfer must settle"
        );
        for (id, n) in &settled {
            assert_eq!(1, *n, "id {id} settled {n} times, expected exactly once");
        }

        // The byte regression guard, read from the operation's own result rather
        // than summed from the stream: delivery is lossy, so a stream-derived total
        // is only ever a lower bound. A download populates `network_rx`; reading
        // `network_tx` yields 0 here.
        let expected = (count * size) as u64;
        assert_eq!(
            expected,
            output.metrics().network_rx,
            "the operation must report the whole directory's downloaded bytes"
        );

        // Each child names the file it wrote, inside the destination directory.
        let root = root_id.expect("root was announced");
        for (id, path) in &decided {
            if *id == root {
                assert_eq!(
                    dest.path(),
                    path.as_path(),
                    "the root's destination is the destination directory"
                );
                continue;
            }
            assert!(
                path.starts_with(dest.path()),
                "child destination {path:?} must be inside {:?}",
                dest.path()
            );
        }

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_events_pair_and_report_bytes timed out");
}

/// The view handed out on `Decided` is what makes per-object progress readable, and it
/// has to keep working after `join(self)` has consumed the operation handle.
///
/// This is the one assertion that exercises the pull half of the design end to end: the
/// stream pushes lifecycle, the view is pulled for bytes. Without it the events carry an
/// id and nothing a caller can read a numerator from, which is how the feature looked
/// while every other test in this file was already green.
///
/// Read after `join()` on purpose. A view holding the `TransferContext` — or anything
/// reaching the client `Handle` — would either fail to compile here or defer the runtime
/// shutdown below; holding only the metrics `Arc` is what makes both fine.
#[tokio::test]
async fn test_download_objects_views_report_per_object_progress() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 10usize;
        let size = 8 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "views/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        // Keep only the views, keyed by id and by whether the entry is the root. The
        // events themselves are covered by the pairing test above.
        let collector = tokio::spawn(async move {
            let mut views = Vec::new();
            while let Some(ev) = stream.next().await {
                if let TransferEvent::Decided {
                    id, parent, view, ..
                } = ev
                {
                    views.push((id, parent, view));
                }
            }
            views
        });

        let output = handle.join().await.expect("join download_objects");
        let views = collector.await.expect("collector");

        assert_eq!(
            count + 1,
            views.len(),
            "one Decided per object plus the root"
        );

        let expected_total = (count * size) as u64;
        let mut roots = 0;
        let mut children = 0;
        for (id, parent, view) in &views {
            let view = view
                .as_ref()
                .unwrap_or_else(|| panic!("id {id} became a real transfer, so it owes a view"));
            let metrics = view.metrics();
            if parent.is_none() {
                roots += 1;
                assert_eq!(
                    ByteTotal::Final(expected_total),
                    view.byte_total(),
                    "the root's denominator is sealed and covers every listed object"
                );
                assert_eq!(
                    expected_total, metrics.network_rx,
                    "the root's numerator reaches its denominator on a clean run"
                );
                // The same fact the joined output reports, from a handle the caller kept
                // across the join rather than from the value join returned.
                assert_eq!(
                    output.metrics().network_rx,
                    metrics.network_rx,
                    "a view and the operation's own result must not disagree"
                );
            } else {
                children += 1;
                assert_eq!(
                    ByteTotal::Final(size as u64),
                    view.byte_total(),
                    "a single-object download knows its length, so its total is final"
                );
                assert_eq!(
                    size as u64, metrics.network_rx,
                    "each child reports exactly the object it downloaded"
                );
            }
            assert!(
                metrics.finished_at.is_some(),
                "id {id} settled before join returned, so its view reports terminal"
            );
        }
        assert_eq!(1, roots, "exactly one entry has no parent");
        assert_eq!(count, children, "every object announced a child view");

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_views_report_per_object_progress timed out");
}

/// A bar drawn off the root view never walks backwards and never exceeds its denominator,
/// and a failed object leaves it short by exactly the bytes that never moved.
///
/// This is what `examples/cp.rs --progress` renders, asserted rather than eyeballed. The
/// root view is read at every child `Settled` — a real mid-flight moment, and one per
/// object, so the sample count is fixed rather than dependent on a tick landing inside the
/// transfer. A time-sampled version of this test would pass vacuously whenever the mock
/// finished between ticks.
///
/// **The root's own counter does not reach its own denominator on a run with failures.**
/// The denominator counts every object that was *listed*; the numerator counts bytes that
/// actually moved. An object rejected with a 403 is refused before a single body byte
/// arrives, so it contributes 0 of its 8 KiB and the root ends short — here at 14/16 of the
/// payload. Bytes from a failure *mid-body* do count (that is what the parent rollup fixed,
/// and what `progress_chaos.rs` pins), so the shortfall is exactly the payload that was
/// never transferred and never more. That exactness is the assertion below, and it is what
/// makes the shortfall reconstructible rather than merely absent.
///
/// A consumer that wants a bar reaching 100% adds the abandoned payload back, per child, as
/// `byte_total() - network_rx` at its `Settled` — the AWS CLI's
/// `ResultRecorder._record_failure_result` arithmetic. Not asserted here: this test pins the
/// root counter's meaning, which is what that reconstruction depends on.
#[tokio::test]
async fn test_download_objects_bar_is_monotonic_and_short_by_what_never_moved() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 16usize;
        let size = 8 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "bar/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        // 403 is non-retryable, so each doomed object fails on its first attempt and the
        // run does not depend on the retry policy.
        for doomed in ["0002.bin", "0011.bin"] {
            m.server.insert_fault(
                bucket,
                &format!("{prefix}{doomed}"),
                FaultType::ServiceError { status: 403 },
                0,
                Occurrence::Always,
            );
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Continue)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        // The drawer, in the shape the example uses: hold the root's view, and read it
        // whenever something happens. Bytes are pulled, never pushed — no event carries a
        // count, which is why a lost event cannot corrupt the bar.
        let collector = tokio::spawn(async move {
            let mut root: Option<aws_sdk_s3_transfer_manager::types::TransferView> = None;
            let mut samples: Vec<(u64, ByteTotal)> = Vec::new();
            while let Some(ev) = stream.next().await {
                match ev {
                    TransferEvent::Decided {
                        parent: None, view, ..
                    } => root = view,
                    TransferEvent::Settled {
                        parent: Some(_), ..
                    } => {
                        if let Some(view) = &root {
                            samples.push((view.metrics().network_rx, view.byte_total()));
                        }
                    }
                    _ => {}
                }
            }
            (root, samples)
        });

        let output = handle.join().await.expect("join download_objects");
        let (root, samples) = collector.await.expect("collector");
        let root = root.expect("the root announced itself with a view");

        assert_eq!(
            count,
            samples.len(),
            "one reading per settled object: {} readings for {count} objects",
            samples.len()
        );

        // Monotonic: a bar that can decrease is worse than no bar, because a caller reads
        // a decrease as data loss.
        for pair in samples.windows(2) {
            assert!(
                pair[1].0 >= pair[0].0,
                "the numerator must never decrease: {} then {}",
                pair[0].0,
                pair[1].0
            );
        }
        // Never over 100% at any sample, and the denominator never shrinks once final.
        for (done, total) in &samples {
            if let ByteTotal::Final(n) = total {
                assert!(
                    done <= n,
                    "a sample must not exceed its own denominator: {done} of {n}"
                );
            }
        }

        let listed = (count * size) as u64;
        assert_eq!(
            ByteTotal::Final(listed),
            root.byte_total(),
            "every listed object belongs in the denominator, including the failed ones"
        );
        assert_eq!(
            (count - 2) as u64,
            output.objects_downloaded(),
            "two objects must actually have failed, or this test proves nothing"
        );
        // The shortfall is the whole of what the two rejected objects would have carried,
        // to the byte. Less than this would mean a successful object's bytes went missing;
        // more would mean a failed object's bytes were counted twice.
        assert_eq!(
            listed - 2 * size as u64,
            root.metrics().network_rx,
            "a 403 is refused before any body byte, so the bar ends short by exactly the \
             payload that never moved"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_bar_is_monotonic_and_short_by_what_never_moved timed out");
}

/// The entry count reaches its total on a run with failures, which is the question bytes
/// cannot answer.
///
/// This is the `N file(s) remaining` the AWS CLI prints on every progress line and the
/// `transferredFiles` the SEP's directory snapshot marks Required. The byte bar on this same
/// run ends at 14/16 of the payload, because two objects moved no bytes — so a consumer with
/// only bytes cannot distinguish "finished with failures" from "still working". The count
/// can: 16 of 16 settled, 2 of them unsuccessfully.
///
/// Both halves matter and both are asserted: the numerator counts endings rather than
/// successes, and the denominator counts every object listed.
#[tokio::test]
async fn test_download_objects_entry_count_reaches_its_total_with_failures() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 16usize;
        let size = 8 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "entries/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        for doomed in ["0002.bin", "0011.bin"] {
            m.server.insert_fault(
                bucket,
                &format!("{prefix}{doomed}"),
                FaultType::ServiceError { status: 403 },
                0,
                Occurrence::Always,
            );
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Continue)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        let collector = tokio::spawn(async move {
            let mut root = None;
            let mut settled_events = 0usize;
            while let Some(ev) = stream.next().await {
                match ev {
                    TransferEvent::Decided {
                        parent: None, view, ..
                    } => root = view,
                    TransferEvent::Settled {
                        parent: Some(_), ..
                    } => settled_events += 1,
                    _ => {}
                }
            }
            (root, settled_events, stream.dropped())
        });

        let output = handle.join().await.expect("join download_objects");
        let (root, settled_events, dropped) = collector.await.expect("collector");
        let root = root.expect("the root announced itself with a view");

        assert_eq!(
            (count - 2) as u64,
            output.objects_downloaded(),
            "two objects must actually have failed, or this test proves nothing"
        );

        // The denominator: every object listed, including the two that failed.
        assert_eq!(
            EntryTotal::Final(count as u64),
            root.entry_total(),
            "the entry denominator counts what listing produced"
        );
        // The numerator: every ending, not every success. 14 succeeded and 2 failed, and all
        // 16 are no longer pending — so "remaining" is 0 and a CLI stops printing work left.
        assert_eq!(
            count as u64,
            root.entries_settled(),
            "a failed entry has still settled; counting only successes would leave 2 \
             objects 'remaining' forever on a finished transfer"
        );

        // The count is published on the view, not tallied from the stream, and this is why:
        // the two agree here only because nothing was dropped. Under loss the view stays
        // exact and the tally goes short.
        assert_eq!(0, dropped, "capacity 2*(n+1) must lose nothing");
        assert_eq!(
            count, settled_events,
            "with no loss the stream's terminal count matches the view's, which is the \
             invariant that makes the view the authority when there is loss"
        );

        // And the contrast that motivates the whole item: bytes stop short on this same run.
        assert_eq!(
            ByteTotal::Final((count * size) as u64),
            root.byte_total(),
            "both denominators seal together"
        );
        assert_eq!(
            ((count - 2) * size) as u64,
            root.metrics().network_rx,
            "the byte numerator is short by the two objects that moved nothing, which is \
             exactly why the entry count is needed"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_entry_count_reaches_its_total_with_failures timed out");
}

/// A run whose listing never completed publishes no entry total, for the same reason it
/// publishes no byte total: nobody knows how many objects there were.
///
/// `EntryTotal::Final(0)` would be the damaging answer — a consumer reads it as "zero objects
/// to do, we are done" at the instant listing failed.
#[tokio::test]
async fn test_download_objects_does_not_seal_an_entry_total_when_listing_never_ran() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let prefix = "unsealed-entries/";
        seed_bucket(&m.server, bucket, prefix, 12, 1024).await;

        // A destination that is a plain file, not a directory: validation fails on the first
        // walker advance, before a single key is listed.
        let dir = tempfile::tempdir().expect("tempdir");
        let not_a_dir = dir.path().join("regular-file");
        std::fs::write(&not_a_dir, b"x").expect("write file");

        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(8).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(&not_a_dir)
            .key_prefix(prefix)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        let collector = tokio::spawn(async move {
            let mut root = None;
            while let Some(ev) = stream.next().await {
                if let TransferEvent::Decided {
                    parent: None, view, ..
                } = ev
                {
                    root = view;
                }
            }
            root
        });

        let _ = handle.join().await;
        let root = collector
            .await
            .expect("collector")
            .expect("the root announced itself with a view");

        assert_eq!(
            EntryTotal::Unknown,
            root.entry_total(),
            "listing never ran, so the object count is unknown and must not read as 0 of 0"
        );
        assert_eq!(
            ByteTotal::Unknown,
            root.byte_total(),
            "and the byte total is unknown for the same reason, from the same seal"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_does_not_seal_an_entry_total_when_listing_never_ran timed out");
}

/// A doomed object must surface as `Outcome::Failed` on its own child event while
/// the rest still report success, under `Continue`.
#[tokio::test]
async fn test_download_objects_events_report_per_object_failure() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 8usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "evfail/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        // 403 rather than 500: non-retryable, so the child fails on its first
        // attempt and the run does not depend on the retry policy.
        let doomed = format!("{prefix}0003.bin");
        m.server.insert_fault(
            bucket,
            &doomed,
            FaultType::ServiceError { status: 403 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Continue)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        let collector = tokio::spawn(async move {
            let mut evs = Vec::new();
            while let Some(ev) = stream.next().await {
                evs.push(ev);
            }
            evs
        });

        let _ = handle.join().await;
        let events = collector.await.expect("collector");

        let mut failed_keys = Vec::new();
        let mut succeeded = 0usize;
        let mut announced = 0usize;
        let mut finished = 0usize;
        for ev in &events {
            // The key comes off the event's own source endpoint, which is where a
            // download reads from -- no side map keyed by id.
            let key = match ev.transfer().source() {
                Endpoint::S3 { key, .. } => key.to_string(),
                other => panic!("a download reads from S3, got {other:?}"),
            };
            match ev {
                TransferEvent::Decided { .. } => announced += 1,
                TransferEvent::Settled {
                    outcome, parent, ..
                } => {
                    finished += 1;
                    match outcome {
                        Outcome::Failed { .. } => failed_keys.push(key),
                        Outcome::Succeeded { .. } if parent.is_some() => succeeded += 1,
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        assert_eq!(
            announced, finished,
            "every announced transfer must reach a terminal event"
        );
        assert!(
            failed_keys.iter().any(|k| k == &doomed),
            "the doomed key must report Failed, got {failed_keys:?}"
        );
        assert_eq!(
            count - 1,
            succeeded,
            "every other object must still report Succeeded"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_events_report_per_object_failure timed out");
}

/// Under `Abort`, a key the walker listed but never got to spawn must still be accounted for.
/// Without the abandoned-entry sweep those keys vanish: the run reports one object where it
/// listed `count`, and a per-object consumer never hears that the files existed.
///
/// The setup makes the gap deterministic. All `count` keys fit in one list page
/// (count < WALK_LOW_WATER), so the walk drains into `pending_entries` before spawn
/// touches it; `max_concurrent_downloads(1)` then spawns only the faulted head key
/// before it fails and Abort cancels, leaving every other listed key unspawned.
///
/// Two things are checked, because two separate mechanisms have to hold. Every listed key must
/// reach the stream, which takes all three settle paths including `announce_child`'s root-gone
/// branch. And `entries_settled` must reach the sealed total, which is what lets a progress bar
/// finish and is the assertion a lost count would break while the stream still looked complete.
#[tokio::test]
async fn test_download_objects_events_abandoned_entries_still_settle() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 60usize;
        let size = 1024usize;
        let bucket = "test-bucket";
        let prefix = "abandon/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        // 503 Always on the first key: it exhausts retries and fails, which under
        // Abort cancels the run with the remaining listed keys still in
        // `pending_entries`.
        m.server.insert_fault(
            bucket,
            &format!("{prefix}0000.bin"),
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        // 2 per object plus the root's pair; the sweep announces-and-finishes every
        // abandoned key, so the stream must have room for all of them to lose nothing.
        let (sink, mut stream) = aws_sdk_s3_transfer_manager::events::channel(
            std::num::NonZeroUsize::new(2 * (count + 1)).expect("capacity > 0"),
        );

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .failure_policy(FailedTransferPolicy::Abort)
            // Serialize spawning so only the faulted head key materializes before
            // the abort; the rest stay listed-but-unspawned for the sweep.
            .max_concurrent_downloads(1)
            .events(sink)
            .initiate()
            .expect("initiate download_objects");

        let collector = tokio::spawn(async move {
            let mut evs = Vec::new();
            while let Some(ev) = stream.next().await {
                evs.push(ev);
            }
            (evs, stream.dropped())
        });

        let result = handle.join().await;
        assert!(
            result.is_err(),
            "Abort with a faulted key must return an error"
        );
        let (events, dropped) = collector.await.expect("collector");
        assert_eq!(0, dropped, "capacity 2*(n+1) must lose nothing");

        // Pairing: every announced id settles exactly once. The sweep announces and
        // finishes an abandoned entry in one step, so a swept key contributes one of
        // each; the two-lock reorder bug (a late Decided landing in a drained map)
        // would surface here as a Decided with no Settled.
        let mut decided: HashMap<u64, bool> = HashMap::new();
        let mut settled: HashMap<u64, usize> = HashMap::new();
        // Completeness: the distinct S3 keys of child events. This is the sweep's
        // guard -- without it, only the head key appears.
        let mut child_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        for ev in &events {
            let key = match ev.transfer().source() {
                Endpoint::S3 { key, .. } => key.to_string(),
                other => panic!("a download reads from S3, got {other:?}"),
            };
            match ev {
                TransferEvent::Decided { id, parent, .. } => {
                    assert!(
                        decided.insert(*id, parent.is_some()).is_none(),
                        "id {id} announced twice"
                    );
                    if parent.is_some() {
                        child_keys.insert(key);
                    }
                }
                TransferEvent::Settled { id, .. } => {
                    *settled.entry(*id).or_default() += 1;
                }
                _ => {}
            }
        }

        for id in decided.keys() {
            assert_eq!(
                Some(&1),
                settled.get(id),
                "announced id {id} must settle exactly once, got {:?}",
                settled.get(id)
            );
        }

        // Every listed key, with no allowance. Three separate paths have to cover the three
        // places an entry can be when Abort lands: `record_abandoned_entries` for the ones
        // still in `pending_entries`, `finish_root`'s orphan drain for the ones in
        // `child_lifecycles`, and `announce_child`'s root-gone branch for the one that can be
        // in neither -- claimed off `pending_entries` and mid-spawn when `on_terminal` takes
        // the root. That last path is why this is `==` and not `>= count - 1`: it needs the
        // sink kept outside `lifecycle`, and without it this assertion fails intermittently
        // under load at `count - 1`.
        assert_eq!(
            count,
            child_keys.len(),
            "every listed key must reach the stream: the three sweeps together are what cover \
             the ones cancelled before they could be reaped"
        );

        // The guarantee that actually protects a consumer, and the one the stream cannot give:
        // the counts reconcile exactly. An entry whose event was lost above is still counted,
        // so `entries_settled` reaches the total and a progress bar completes. Skipping the
        // count instead is what seals a bar below 100% for the life of the process.
        let root_view = events
            .iter()
            .find_map(|ev| match ev {
                TransferEvent::Decided {
                    parent: None, view, ..
                } => view.clone(),
                _ => None,
            })
            .expect("the root announces itself, and it carries a view");
        assert_eq!(
            EntryTotal::Final(count as u64),
            root_view.entry_total(),
            "listing completed, so the denominator must be sealed at the listed count"
        );
        assert_eq!(
            count as u64,
            root_view.entries_settled(),
            "every listed entry must be counted as settled even when its event was lost; \
             short here is the stuck-progress-bar defect"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_events_abandoned_entries_still_settle timed out");
}

/// A composite seals a byte denominator covering every object it listed.
///
/// `set_total_bytes` used to have three call sites, all leaf transfers, so a directory
/// transfer had no denominator at all and a percentage was undefined for the whole run.
/// Both composites now accumulate the size of every entry their walk produces, in the
/// same critical section that publishes the entry, and seal the total once listing is
/// quiescent.
///
/// Asserted on the joined output rather than mid-flight: the seal fires when listing
/// drains, and a timing-based read of the provisional value would be flaky. What this
/// pins is the invariant that matters for a bar — the denominator covers the whole
/// dataset, so the numerator can reach it.
#[tokio::test]
async fn test_download_objects_seals_a_byte_denominator() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let count = 25usize;
        let size = 4096usize;
        let bucket = "test-bucket";
        let prefix = "denominator/";
        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("download_objects");

        assert_eq!(
            Some(count as u64 * size as u64),
            output.metrics.total_bytes,
            "the sealed total must cover every listed object"
        );
        // The numerator reaches the denominator on a clean run. Before the rollup and the
        // seal, one was folded only from successful children and the other did not exist.
        assert_eq!(
            output.metrics.total_bytes,
            Some(output.metrics.network_rx),
            "on a run with no failures the bar must reach exactly 100%"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_seals_a_byte_denominator timed out");
}

/// A run whose listing never completed must not publish a byte total.
///
/// `total_bytes` means "this is the whole payload". Sealing it on a path where enumeration
/// stopped early makes that claim false and `OnceLock` makes it permanent: a consumer sees
/// `None -> Some(partial)` at the instant listing fails and its bar snaps toward 100%
/// immediately before the transfer reports failure.
///
/// The seal is therefore gated on a positive `listing_complete` flag set only where the
/// walker reports itself exhausted, not on "the walk is no longer in state" — which is
/// equally true of a walk that is out for execution, one that finished, and one that was
/// dropped after a failure.
///
/// Exercised through the destination-validation path: a destination that is a file rather
/// than a directory fails on the first walker advance, before a single key is listed. That
/// used to seal `Some(0)` for an arbitrarily large prefix.
#[tokio::test]
async fn test_download_objects_does_not_seal_a_total_when_listing_never_ran() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;

        let bucket = "test-bucket";
        let prefix = "unsealed/";
        seed_bucket(&m.server, bucket, prefix, 12, 4096).await;

        // A file, not a directory: `validate_destination` fails on the first advance.
        let dir = tempfile::tempdir().expect("tempdir");
        let not_a_dir = dir.path().join("regular-file");
        std::fs::write(&not_a_dir, b"x").expect("write file");

        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(&not_a_dir)
            .key_prefix(prefix)
            .initiate()
            .expect("initiate download_objects");

        // Read metrics AFTER the transfer has gone terminal. Reading before it starts
        // would assert `None` on a transfer that had not run yet, which is vacuous.
        loop {
            if handle.status().is_terminal() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let metrics = handle.metrics();
        let result = handle.join().await;

        assert!(
            result.is_err(),
            "a non-directory destination must fail the transfer"
        );
        assert_eq!(
            None, metrics.total_bytes,
            "listing never ran, so no total is known — Some(0) here would claim the \
             prefix was empty"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_does_not_seal_a_total_when_listing_never_ran timed out");
}

/// A caller-supplied walker must still exclude 0-byte folder markers.
///
/// Verified against a real bucket: the marker `markers/sub/` derives the local path `<dest>/sub`,
/// which is the directory `markers/sub/b.bin` already created, so the write fails with `EISDIR` and
/// the default `FailedTransferPolicy::Abort` takes the whole operation down -- exit 1, four of five
/// objects on disk. Which of the pair fails depends on listing order, so the failure is
/// non-deterministic too.
///
/// The exclusion belongs to the operation rather than the walker, which is what makes it survive a
/// supplied walker. `markers/notamarker/` pins the other half: a `/`-terminated key *with* a body is
/// a real object and must still download, so the rule cannot be simplified to a key-suffix test.
#[tokio::test]
async fn test_download_objects_custom_walker_still_excludes_folder_markers() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;
        let bucket = "test-bucket";
        m.server.create_bucket(bucket).await.expect("create bucket");

        for (key, body) in [
            ("markers/a.bin", vec![1u8; 128]),
            ("markers/sub/b.bin", vec![2u8; 128]),
            // Console-style folder markers: 0 bytes, key ends with the delimiter.
            ("markers/sub/", Vec::new()),
            ("markers/emptydir/", Vec::new()),
            // Ends with the delimiter but is not empty, so it is not a marker.
            ("markers/notamarker/", vec![3u8; 128]),
        ] {
            m.server
                .add_object(bucket, key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix("markers/")
            // Both are required: the prefix because a supplied walker owns the listing, and
            // `page_size` because pagination is the reason to supply one at all.
            .walker(S3Walker::builder().prefix("markers/").page_size(2).build())
            .initiate()
            .expect("initiate download_objects");

        let output = handle
            .join()
            .await
            .expect("a folder marker must not fail the operation");

        assert!(
            output.failed_transfers().is_empty(),
            "no child may fail: {:?}",
            output.failed_transfers()
        );
        assert_eq!(
            3,
            output.objects_downloaded(),
            "the two 0-byte markers are excluded; `notamarker/` is not a marker and stays"
        );
        assert_eq!(
            3,
            count_files(dest.path()),
            "and only those three land on disk"
        );
        assert!(
            dest.path().join("sub").is_dir(),
            "`sub` must stay the directory `sub/b.bin` needs, not a 0-byte file"
        );
        assert!(
            !dest.path().join("emptydir").exists(),
            "a marker must not materialize as an empty file"
        );
        assert_eq!(
            vec![3u8; 128],
            std::fs::read(dest.path().join("notamarker")).expect("read notamarker"),
            "a non-empty `/`-terminated key is a real object and must download intact"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_custom_walker_still_excludes_folder_markers timed out");
}

/// A caller's own `filter` must not re-admit folder markers.
///
/// This is the case that decided where the exclusion lives. `S3Walker::filter` replaces nothing and
/// means only what it says, so a caller narrowing the listing writes a predicate about their own
/// business -- and any predicate broad enough to keep real keys also keeps `sub/`. As a walker-level
/// default the exclusion was removable by setting a filter at all, and the run then hit the `EISDIR`
/// abort the default existed to prevent. Dropping markers where the walk is drained puts them out of
/// a predicate's reach.
#[tokio::test]
async fn test_download_objects_a_caller_filter_cannot_re_admit_folder_markers() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeMode::Managed).await;
        let bucket = "test-bucket";
        m.server.create_bucket(bucket).await.expect("create bucket");

        for (key, body) in [
            ("keep/a.bin", vec![1u8; 128]),
            ("keep/sub/b.bin", vec![2u8; 128]),
            ("keep/sub/", Vec::new()),
            ("keep/emptydir/", Vec::new()),
        ] {
            m.server
                .add_object(bucket, key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .key_prefix("keep/")
            // Deliberately permissive: it admits the markers too. A caller writing
            // `|o| o.key().is_some_and(|k| k.ends_with(".parquet"))` would exclude them by luck;
            // this one does not, which is what makes the assertion mean something.
            .walker(
                S3Walker::builder()
                    .prefix("keep/")
                    .filter(|o| o.key().unwrap_or_default().starts_with("keep/"))
                    .build(),
            )
            .initiate()
            .expect("initiate download_objects");

        let output = handle
            .join()
            .await
            .expect("a caller filter must not reintroduce the EISDIR abort");

        assert!(
            output.failed_transfers().is_empty(),
            "no child may fail: {:?}",
            output.failed_transfers()
        );
        assert_eq!(
            2,
            output.objects_downloaded(),
            "only the two real objects; the caller's filter admitted the markers and the \
             operation dropped them anyway"
        );
        assert!(
            dest.path().join("sub").is_dir(),
            "`sub` must stay the directory `sub/b.bin` needs, not a 0-byte file"
        );
        assert!(
            !dest.path().join("emptydir").exists(),
            "a marker must not materialize as an empty file"
        );

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_a_caller_filter_cannot_re_admit_folder_markers timed out");
}
