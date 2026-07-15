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
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use s3_mock_server::{FaultType, Occurrence, S3MockServer};
use tokio::time::timeout;

/// Default test timeout.
const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Setup transfer manager with mock server.
async fn setup() -> (
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
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (server, handle, tm)
}

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
#[tokio::test]
async fn test_download_objects_many_small_files() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let count = 500usize;
        let size = 4 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "small/";

        seed_bucket(&server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_many_small_files timed out");
}

/// Per-file byte-content integrity after download.
#[tokio::test]
async fn test_download_objects_flat_content_roundtrip() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let count = 20usize;
        let size = 2 * ByteUnit::Kibibyte.as_bytes_usize();
        let bucket = "test-bucket";
        let prefix = "roundtrip/";

        server.create_bucket(bucket).await.expect("create bucket");
        for i in 0..count {
            let key = format!("{prefix}{i:03}.bin");
            let body = vec![i as u8; size];
            server
                .add_object(bucket, &key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_flat_content_roundtrip timed out");
}

/// Objects under nested prefixes download into the correct directory tree.
#[tokio::test]
async fn test_download_objects_nested_prefixes() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let bucket = "test-bucket";
        let prefix = "tree/";
        let objects: &[(&str, &[u8])] = &[
            ("tree/top.txt", b"top-level"),
            ("tree/a/one.txt", b"a-1"),
            ("tree/a/two.txt", b"a-2"),
            ("tree/b/inner/deep.txt", b"deep-content"),
            ("tree/c/1/2/3/leaf.bin", b"leaf"),
        ];
        seed_objects(&server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_nested_prefixes timed out");
}

/// Objects large enough to trigger multipart download, content verified.
#[tokio::test]
async fn test_download_objects_multipart_children() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let part = 8 * ByteUnit::Mebibyte.as_bytes_usize();
        let count = 3usize;
        let size = 2 * part; // 16 MiB per file -> 2 parts each
        let bucket = "test-bucket";
        let prefix = "mpu/";

        server.create_bucket(bucket).await.expect("create bucket");
        for i in 0..count {
            let key = format!("{prefix}{i:04}.bin");
            // Deterministic content pattern (byte index mod 256).
            let body: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
            server
                .add_object(bucket, &key, body, None)
                .await
                .expect("seed object");
        }

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_multipart_children timed out");
}

/// Empty prefix yields zero downloads with no error.
#[tokio::test]
async fn test_download_objects_empty_prefix() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let bucket = "test-bucket";
        server.create_bucket(bucket).await.expect("create bucket");

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
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
        let (server, server_handle, tm) = setup().await;

        let bucket = "test-bucket";
        let root_prefix = "deep/";
        server.create_bucket(bucket).await.expect("create bucket");

        let mut expected_count = 0u64;

        // Iteratively seed a deep/wide tree to avoid recursive async lifetime issues.
        let mut prefixes_to_seed: Vec<(String, usize)> = vec![(root_prefix.to_string(), 3)];
        while let Some((pfx, depth)) = prefixes_to_seed.pop() {
            for i in 0..2usize {
                let key = format!("{pfx}f{i}.bin");
                server
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
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_deep_wide_tree timed out");
}

/// Abort mid-transfer terminates cleanly without deadlock.
#[tokio::test]
async fn test_download_objects_abort_terminates() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let count = 200usize;
        let size = 1024usize;
        let bucket = "test-bucket";
        let prefix = "abort/";

        seed_bucket(&server, bucket, prefix, count, size).await;

        // Inject faults on the first key so the download hangs/fails if not aborted.
        server.insert_fault(
            bucket,
            &format!("{prefix}0000.bin"),
            FaultType::ServiceError { status: 503 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_abort_terminates timed out");
}

// ---------------------------------------------------------------------------
// DOWNLOAD-SPECIFIC TESTS
// ---------------------------------------------------------------------------

/// Seed enough objects to force ListObjectsV2 pagination, verify all objects
/// across pages are discovered and downloaded. Uses `S3Walker::builder().page_size()`
/// to force a small page size rather than seeding >1000 objects.
#[tokio::test]
async fn test_download_objects_listing_pagination() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let count = 50usize;
        let size = 128usize;
        let bucket = "test-bucket";
        let prefix = "paginated/";

        seed_bucket(&server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        // Force page_size=5 so that 50 objects require 10 list pages.
        let walker = S3Walker::builder().page_size(5).build();
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_listing_pagination timed out");
}

/// FailedTransferPolicy::Continue with one faulted key: the other objects
/// succeed and the faulted key appears in `failed_transfers()`.
#[tokio::test]
async fn test_download_objects_continue_policy_partial_failure() {
    timeout(TEST_TIMEOUT, async {
        let (server, server_handle, tm) = setup().await;

        let bucket = "test-bucket";
        let prefix = "partial/";
        let good_count = 10usize;
        let size = 512usize;

        seed_bucket(&server, bucket, prefix, good_count, size).await;
        // Add one extra object that will be faulted.
        let bad_key = format!("{prefix}bad.bin");
        server
            .add_object(bucket, &bad_key, vec![0xFFu8; size], None)
            .await
            .expect("seed bad object");

        // Inject a permanent service error on the bad key.
        server.insert_fault(
            bucket,
            &bad_key,
            FaultType::ServiceError { status: 500 },
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
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
        let (server, server_handle, tm) = setup().await;

        let bucket = "test-bucket";
        let prefix = "safe/";
        // Include a known-good object alongside traversal keys.
        let objects: &[(&str, &[u8])] = &[
            ("safe/good.txt", b"safe-content"),
            ("safe/../escape.bin", b"escape-attempt-1"),
            ("safe/../../etc/passwd", b"escape-attempt-2"),
            ("safe/sub/../../other.bin", b"escape-attempt-3"),
        ];
        seed_objects(&server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let dest_path = dest.path().to_path_buf();

        // Use Continue policy so we can inspect which keys failed vs succeeded.
        let handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_key_path_safety timed out");
}

// ---------------------------------------------------------------------------
// ROUND-TRIP
// ---------------------------------------------------------------------------

/// Build a nested directory tree, upload it, download it to a fresh dir,
/// and assert the two trees are byte-identical.
#[tokio::test]
async fn test_upload_then_download_objects_roundtrip() {
    timeout(Duration::from_secs(120), async {
        let (_server, server_handle, tm) = setup().await;

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
        let upload_handle = tm
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
        let download_handle = tm
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

        server_handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_then_download_objects_roundtrip timed out");
}
