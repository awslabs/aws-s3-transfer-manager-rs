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
use aws_sdk_s3_transfer_manager::RuntimeConfig;
use s3_mock_server::{FaultType, Occurrence, S3MockServer};

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
async fn test_download_objects_many_small_files(rt: RuntimeConfig) {
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
            .walker(S3Walker::builder().prefix(prefix).build())
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
    test_download_objects_many_small_files(RuntimeConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_many_small_files_tokio_mt() {
    test_download_objects_many_small_files(RuntimeConfig::multi_thread_tokio()).await;
}

/// Per-file byte-content integrity after download.
#[tokio::test]
async fn test_download_objects_flat_content_roundtrip() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

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
            .walker(S3Walker::builder().prefix(prefix).build())
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(count as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        // Verify byte content of every file.
        for i in 0..count {
            let file_path = dest.path().join(format!("{prefix}{i:03}.bin"));
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

// ref: RUST-1426
/// Objects under nested prefixes download into the directory tree their full
/// keys name, the listing prefix included.
#[tokio::test]
async fn test_download_objects_nested_prefixes() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

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
            .walker(S3Walker::builder().prefix(prefix).build())
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(objects.len() as u64, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        // Verify directory structure and content.
        let expected: HashMap<&str, Vec<u8>> =
            objects.iter().map(|(k, v)| (*k, v.to_vec())).collect();
        verify_dir(dest.path(), &expected);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_nested_prefixes timed out");
}

/// Objects large enough to trigger multipart download, content verified.
async fn test_download_objects_multipart_children(rt: RuntimeConfig) {
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
            .walker(S3Walker::builder().prefix(prefix).build())
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
            let file_path = dest.path().join(format!("{prefix}{i:04}.bin"));
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
    test_download_objects_multipart_children(RuntimeConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_multipart_children_tokio_mt() {
    test_download_objects_multipart_children(RuntimeConfig::multi_thread_tokio()).await;
}

/// Empty prefix yields zero downloads with no error.
#[tokio::test]
async fn test_download_objects_empty_prefix() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

        let bucket = "test-bucket";
        m.server.create_bucket(bucket).await.expect("create bucket");

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .walker(S3Walker::builder().prefix("nonexistent/").build())
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
        let m = mock_tm(RuntimeConfig::default()).await;

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
            .walker(S3Walker::builder().prefix(root_prefix).build())
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
        let m = mock_tm(RuntimeConfig::default()).await;

        let count = 200usize;
        let size = 1024usize;
        let bucket = "test-bucket";
        let prefix = "abort/";

        seed_bucket(&m.server, bucket, prefix, count, size).await;

        // Inject faults on the first key so the download hangs/fails if not aborted.
        m.server.insert_fault(
            bucket,
            &format!("{prefix}0000.bin"),
            FaultType::slow_down(),
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .walker(S3Walker::builder().prefix(prefix).build())
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

// ---------------------------------------------------------------------------
// DOWNLOAD-SPECIFIC TESTS
// ---------------------------------------------------------------------------

/// Seed enough objects to force ListObjectsV2 pagination, verify all objects
/// across pages are discovered and downloaded. Uses `S3Walker::builder().page_size()`
/// to force a small page size rather than seeding >1000 objects.
async fn test_download_objects_listing_pagination(rt: RuntimeConfig) {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(rt).await;

        let count = 50usize;
        let size = 128usize;
        let bucket = "test-bucket";
        let prefix = "paginated/";

        seed_bucket(&m.server, bucket, prefix, count, size).await;

        let dest = tempfile::tempdir().expect("tempdir");
        // Force page_size=5 so that 50 objects require 10 list pages.
        let walker = S3Walker::builder().prefix(prefix).page_size(5).build();
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
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
        let sample_path = dest.path().join(format!("{prefix}0025.bin"));
        let got = std::fs::read(&sample_path).expect("read sample file");
        assert_eq!(got, vec![25u8; size], "sample content should match");

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_listing_pagination timed out");
}

#[tokio::test]
async fn test_download_objects_listing_pagination_mock_gp() {
    test_download_objects_listing_pagination(RuntimeConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_download_objects_listing_pagination_tokio_mt() {
    test_download_objects_listing_pagination(RuntimeConfig::multi_thread_tokio()).await;
}

// ref: RUST-1426
/// A supplied walker's prefix scopes the listing, and each listed object is
/// written at its full key.
#[tokio::test]
async fn test_download_objects_walker_prefix_scopes_listing_and_paths() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

        let bucket = "test-bucket";
        let objects: &[(&str, &[u8])] = &[
            ("other/1.txt", b"other"),
            ("chosen/1.txt", b"chosen-1"),
            ("chosen/sub/2.txt", b"chosen-2"),
        ];
        seed_objects(&m.server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .walker(S3Walker::builder().prefix("chosen/").build())
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(2, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("chosen/1.txt", b"chosen-1".to_vec());
        expected.insert("chosen/sub/2.txt", b"chosen-2".to_vec());
        verify_dir(dest.path(), &expected);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_walker_prefix_scopes_listing_and_paths timed out");
}

/// Seeds `keys`, each holding its own key as content, downloads the objects
/// under `prefix`, and asserts each one is written at its full key beneath the
/// destination.
async fn assert_prefix_keeps_full_keys(prefix: &str, keys: &[&str]) {
    let m = mock_tm(RuntimeConfig::default()).await;
    let bucket = "test-bucket";
    let objects: Vec<(&str, &[u8])> = keys.iter().map(|k| (*k, k.as_bytes())).collect();
    seed_objects(&m.server, bucket, &objects).await;

    let dest = tempfile::tempdir().expect("tempdir");
    let output = m
        .client
        .download_objects()
        .bucket(bucket)
        .destination(dest.path())
        .walker(S3Walker::builder().prefix(prefix).build())
        .initiate()
        .expect("initiate download_objects")
        .join()
        .await
        .expect("join download_objects");
    assert_eq!(keys.len() as u64, output.objects_downloaded());
    assert!(output.failed_transfers().is_empty());

    let expected: HashMap<&str, Vec<u8>> =
        keys.iter().map(|k| (*k, k.as_bytes().to_vec())).collect();
    verify_dir(dest.path(), &expected);

    m.handle.shutdown().await.expect("shutdown");
}

// ref: RUST-1426
/// A prefix that is a key segment without its trailing delimiter selects
/// `photos/readme.txt` and also `photos-other/x.txt`, and both keep their full
/// keys. Removing the prefix as a string from each key would write the second
/// object as `-other/x.txt`.
#[tokio::test]
async fn test_download_objects_prefix_without_delimiter_keeps_full_keys() {
    timeout(
        TEST_TIMEOUT,
        assert_prefix_keeps_full_keys("photos", &["photos/readme.txt", "photos-other/x.txt"]),
    )
    .await
    .expect("test_download_objects_prefix_without_delimiter_keeps_full_keys timed out");
}

// ref: RUST-1426
/// A prefix that ends partway through a file name selects `1.txt`, `10.txt`,
/// and the directory `1/`, and each keeps its full key. Removing the prefix as
/// a string from each key would write them as `.txt`, `0.txt`, and
/// `inner.txt`.
#[tokio::test]
async fn test_download_objects_prefix_ending_mid_name_keeps_full_keys() {
    timeout(
        TEST_TIMEOUT,
        assert_prefix_keeps_full_keys(
            "notes/2021/1",
            &[
                "notes/2021/1.txt",
                "notes/2021/10.txt",
                "notes/2021/1/inner.txt",
            ],
        ),
    )
    .await
    .expect("test_download_objects_prefix_ending_mid_name_keeps_full_keys timed out");
}

/// Folder markers are skipped even when the supplied walker's filter accepts
/// every object.
#[tokio::test]
async fn test_download_objects_skips_folder_markers_with_supplied_walker() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

        let bucket = "test-bucket";
        let objects: &[(&str, &[u8])] = &[
            ("markers/", b""),
            ("markers/sub/", b""),
            ("markers/sub/file.txt", b"content"),
        ];
        seed_objects(&m.server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .walker(
                S3Walker::builder()
                    .prefix("markers/")
                    .filter(|_| true)
                    .build(),
            )
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(1, output.objects_downloaded());
        assert!(output.failed_transfers().is_empty());

        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("markers/sub/file.txt", b"content".to_vec());
        verify_dir(dest.path(), &expected);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_skips_folder_markers_with_supplied_walker timed out");
}

/// A non-empty object whose key maps to the destination directory itself is
/// reported as a failure, and the other files are untouched.
#[tokio::test]
async fn test_download_objects_key_mapping_to_destination_root_fails() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

        let bucket = "test-bucket";
        let objects: &[(&str, &[u8])] = &[("./", b"not-a-marker"), ("ok.txt", b"ok")];
        seed_objects(&m.server, bucket, objects).await;

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");
        assert_eq!(1, output.objects_downloaded());
        assert_eq!(1, output.failed_transfers().len());
        let failed_key = output.failed_transfers()[0]
            .input()
            .key()
            .expect("failed input should have key");
        assert_eq!("./", failed_key);

        assert!(dest.path().is_dir(), "destination must remain a directory");
        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("ok.txt", b"ok".to_vec());
        verify_dir(dest.path(), &expected);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_key_mapping_to_destination_root_fails timed out");
}

/// Seeds `objects`, downloads the bucket to `dest`, and asserts every object
/// downloads with no failures.
async fn download_all(dest: &Path, objects: &[(&str, &[u8])]) {
    let m = mock_tm(RuntimeConfig::default()).await;
    let bucket = "test-bucket";
    seed_objects(&m.server, bucket, objects).await;

    let output = m
        .client
        .download_objects()
        .bucket(bucket)
        .destination(dest)
        .initiate()
        .expect("initiate download_objects")
        .join()
        .await
        .expect("join download_objects");
    assert_eq!(objects.len() as u64, output.objects_downloaded());
    assert!(output.failed_transfers().is_empty());

    m.handle.shutdown().await.expect("shutdown");
}

// ref: RUST-1202
/// A destination written with `.` or `..` components downloads every object
/// into the directory it names lexically. Comparing each object's cleaned path
/// against the uncleaned destination would reject every key as resolving
/// outside it.
#[tokio::test]
async fn test_download_objects_destination_with_dot_components() {
    timeout(TEST_TIMEOUT, async {
        let objects: &[(&str, &[u8])] = &[("a.txt", b"a"), ("nested/b.txt", b"b")];
        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("a.txt", b"a".to_vec());
        expected.insert("nested/b.txt", b"b".to_vec());

        for form in ["./out", "out/../out", "x/./../out/."] {
            let tmp = tempfile::tempdir().expect("tempdir");
            // `tmp` is absolute, so each destination is an absolute path
            // holding `/./` or `/../`.
            let dest = tmp.path().join(form);
            download_all(&dest, objects).await;
            verify_dir(&tmp.path().join("out"), &expected);
        }
    })
    .await
    .expect("test_download_objects_destination_with_dot_components timed out");
}

/// The relative path from the directory `base` to `target`, both absolute:
/// one `..` for each component of `base` below their common ancestor, then
/// the rest of `target`.
///
/// `None` when the two have different roots or prefixes, such as Windows
/// paths on different drives, which no relative path connects.
fn relative_path(base: &Path, target: &Path) -> Option<PathBuf> {
    use std::path::Component;

    let mut base = base.components().peekable();
    let mut target = target.components().peekable();
    while let (Some(b), Some(t)) = (base.peek(), target.peek()) {
        if b != t {
            break;
        }
        base.next();
        target.next();
    }
    let (base, target): (Vec<_>, Vec<_>) = (base.collect(), target.collect());
    if base
        .iter()
        .chain(&target)
        .any(|c| matches!(c, Component::Prefix(_) | Component::RootDir))
    {
        return None;
    }
    Some(
        base.iter()
            .map(|_| Component::ParentDir)
            .chain(target)
            .collect(),
    )
}

// ref: RUST-1202
/// A relative destination with `.` or `..` components, resolved against the
/// working directory, downloads every object into the directory it names
/// lexically. The working directory is not changed: each destination climbs
/// from it with `..` to the temporary directory.
#[tokio::test]
async fn test_download_objects_relative_destination_with_dot_components() {
    timeout(TEST_TIMEOUT, async {
        let objects: &[(&str, &[u8])] = &[("a.txt", b"a"), ("nested/b.txt", b"b")];
        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("a.txt", b"a".to_vec());
        expected.insert("nested/b.txt", b"b".to_vec());
        let cwd = std::env::current_dir().expect("current dir");

        for form in ["./out", "x/../out"] {
            let mut tmp = tempfile::tempdir().expect("tempdir");
            // The system temporary directory can be on another Windows drive
            // than the working directory; use one beneath the working
            // directory instead.
            if relative_path(&cwd, tmp.path()).is_none() {
                tmp = tempfile::tempdir_in(&cwd).expect("tempdir");
            }
            let dest = relative_path(&cwd, tmp.path())
                .expect("a relative path to the temporary directory")
                .join(form);
            assert!(dest.is_relative(), "{dest:?}");
            download_all(&dest, objects).await;
            verify_dir(&tmp.path().join("out"), &expected);
            assert!(!tmp.path().join("x").exists(), "x/.. is resolved lexically");
        }
    })
    .await
    .expect("test_download_objects_relative_destination_with_dot_components timed out");
}

// ref: RUST-1202
/// A destination that does not exist is created, with its missing parents,
/// and the objects are downloaded into it.
#[tokio::test]
async fn test_download_objects_creates_missing_destination() {
    timeout(TEST_TIMEOUT, async {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dest = tmp.path().join("a").join("b").join("out");
        let objects: &[(&str, &[u8])] = &[("a.txt", b"a"), ("nested/b.txt", b"b")];

        download_all(&dest, objects).await;

        let mut expected: HashMap<&str, Vec<u8>> = HashMap::new();
        expected.insert("a.txt", b"a".to_vec());
        expected.insert("nested/b.txt", b"b".to_vec());
        verify_dir(&dest, &expected);
    })
    .await
    .expect("test_download_objects_creates_missing_destination timed out");
}

// ref: RUST-1202
/// A destination that exists and is not a directory fails the download with
/// `InputInvalid`, and the file is left as it was.
#[tokio::test]
async fn test_download_objects_destination_file_is_input_invalid() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;
        let bucket = "test-bucket";
        seed_objects(&m.server, bucket, &[("a.txt", b"a")]).await;

        let tmp = tempfile::tempdir().expect("tempdir");
        let dest = tmp.path().join("out");
        std::fs::write(&dest, b"not a directory").expect("write file");

        let err = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(&dest)
            .initiate()
            .expect("initiate download_objects")
            .join()
            .await
            .expect_err("a file destination must fail");
        assert_eq!(
            &aws_sdk_s3_transfer_manager::error::ErrorKind::InputInvalid,
            err.kind()
        );
        assert_eq!(b"not a directory".as_slice(), std::fs::read(&dest).unwrap());

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_destination_file_is_input_invalid timed out");
}

/// FailedTransferPolicy::Continue with one faulted key: the other objects
/// succeed and the faulted key appears in `failed_transfers()`.
#[tokio::test]
async fn test_download_objects_continue_policy_partial_failure() {
    timeout(TEST_TIMEOUT, async {
        let m = mock_tm(RuntimeConfig::default()).await;

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
            FaultType::internal_error(),
            0,
            Occurrence::Always,
        );

        let dest = tempfile::tempdir().expect("tempdir");
        let handle = m
            .client
            .download_objects()
            .bucket(bucket)
            .destination(dest.path())
            .walker(S3Walker::builder().prefix(prefix).build())
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
        let m = mock_tm(RuntimeConfig::default()).await;

        let bucket = "test-bucket";
        let prefix = "safe/";
        // Include a known-good object alongside traversal keys. Each
        // traversal key climbs one level above the destination.
        let objects: &[(&str, &[u8])] = &[
            ("safe/good.txt", b"safe-content"),
            ("safe/../../escape.bin", b"escape-attempt-1"),
            ("safe/../../etc/passwd", b"escape-attempt-2"),
            ("safe/sub/../../../other.bin", b"escape-attempt-3"),
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
            .walker(S3Walker::builder().prefix(prefix).build())
            .failure_policy(FailedTransferPolicy::Continue)
            .initiate()
            .expect("initiate download_objects");

        let output = handle.join().await.expect("join download_objects");

        // The good file must land on disk inside the destination.
        let good_path = dest_path.join("safe/good.txt");
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

        // Every traversal key fails with InputInvalid; only the safe object
        // downloads.
        assert_eq!(
            3,
            output.failed_transfers().len(),
            "traversal keys should fail rather than silently succeed"
        );
        for failed in output.failed_transfers() {
            assert_eq!(
                &aws_sdk_s3_transfer_manager::error::ErrorKind::InputInvalid,
                failed.error().kind()
            );
        }
        assert_eq!(1, output.objects_downloaded());

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_download_objects_key_path_safety timed out");
}

// ---------------------------------------------------------------------------
// SHARED TEST CORPUS
// ---------------------------------------------------------------------------

/// Part size of every ported corpus case (`targetPartSizeBytes`).
const CORPUS_PART_SIZE: u64 = 8 * ByteUnit::Mebibyte.as_bytes_u64();

/// A `download-directory` case from the shared transfer manager test corpus.
///
/// Paths are written as the corpus states them. The case's destination is
/// recreated beneath a temporary directory, and each expected file beneath it,
/// by keeping only the normal components of the path.
struct CorpusCase {
    /// The destination named by the case.
    destination: &'static str,
    /// The walker carrying the case's `s3Prefix` and `filter`.
    walker: S3Walker,
    failure_policy: FailedTransferPolicy,
    /// The case's `maxConcurrency`, when it sets one.
    max_concurrency: Option<usize>,
    /// Key and size of each object in the bucket (`s3Objects`).
    objects: &'static [(&'static str, usize)],
    /// Path and size of each file the download leaves on disk
    /// (`expectedFiles`).
    expected_files: &'static [(&'static str, u64)],
    objects_downloaded: u64,
    objects_failed: usize,
}

/// `path` beneath `base`, with any root or drive of `path` removed.
fn beneath(base: &Path, path: &str) -> PathBuf {
    use std::path::Component;

    let normal = Path::new(path)
        .components()
        .filter(|c| matches!(c, Component::Normal(_)));
    base.join(normal.collect::<PathBuf>())
}

/// Every file beneath `dir`, by path relative to `dir`, with its size.
fn file_sizes(dir: &Path) -> std::collections::BTreeMap<PathBuf, u64> {
    let mut files = HashMap::new();
    collect_files(dir, dir, &mut files);
    files
        .into_iter()
        .map(|(path, content)| (path, content.len() as u64))
        .collect()
}

/// Runs `case` against the mock server and asserts its outcome counts and
/// that the files it expects, and no others, are written beneath the
/// temporary directory that holds the destination.
///
/// Every failure is asserted to be an `InputInvalid` rejection of the key's
/// local path, the only failure the ported cases expect.
async fn run_corpus_case(case: CorpusCase) {
    let m = crate::harness::mock_tm_with(RuntimeConfig::default(), |b| {
        b.part_size(aws_sdk_s3_transfer_manager::types::PartSize::Target(
            CORPUS_PART_SIZE,
        ))
    })
    .await;
    let bucket = "test-bucket";
    for (i, (key, size)) in case.objects.iter().enumerate() {
        m.server
            .add_object(bucket, key, vec![i as u8; *size], None)
            .await
            .expect("seed object");
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = beneath(tmp.path(), case.destination);
    let mut request = m
        .client
        .download_objects()
        .bucket(bucket)
        .destination(&dest)
        .walker(case.walker)
        .failure_policy(case.failure_policy);
    if let Some(max) = case.max_concurrency {
        request = request.max_concurrent_downloads(max);
    }
    let output = request
        .initiate()
        .expect("initiate download_objects")
        .join()
        .await
        .expect("join download_objects");

    assert_eq!(case.objects_downloaded, output.objects_downloaded());
    assert_eq!(
        case.objects_failed,
        output.failed_transfers().len(),
        "failed: {:?}",
        output
            .failed_transfers()
            .iter()
            .map(|f| f.input().key())
            .collect::<Vec<_>>()
    );
    for failed in output.failed_transfers() {
        assert_eq!(
            &aws_sdk_s3_transfer_manager::error::ErrorKind::InputInvalid,
            failed.error().kind(),
            "key {:?}",
            failed.input().key()
        );
    }

    let expected: std::collections::BTreeMap<PathBuf, u64> = case
        .expected_files
        .iter()
        .map(|(path, size)| (beneath(Path::new(""), path), *size))
        .collect();
    assert_eq!(expected, file_sizes(tmp.path()));

    m.handle.shutdown().await.expect("shutdown");
}

// ref: RUST-1426
/// download-directory.json: "Test download directory - with s3Prefix".
#[tokio::test]
async fn sep_download_directory_with_s3prefix() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder().prefix("photos/").build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: Some(10),
            objects: &[
                ("photos/2023/jan/photo1.jpg", 1048576),
                ("photos/2023/jan/photo2.jpg", 1048576),
                ("photos/readme.txt", 1024),
            ],
            expected_files: &[
                ("/local/downloads/photos/2023/jan/photo1.jpg", 1048576),
                ("/local/downloads/photos/2023/jan/photo2.jpg", 1048576),
                ("/local/downloads/photos/readme.txt", 1024),
            ],
            objects_downloaded: 3,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_with_s3prefix timed out");
}

// ref: RUST-1426
/// download-directory.json: "Test download directory - with filter callback".
///
/// The corpus lists the expected file as `/local/downloads/image.jpg`, with
/// the prefix removed; the derivation rule keeps the full key.
#[tokio::test]
async fn sep_download_directory_with_filter_callback() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder()
                .prefix("mixed/")
                .filter(|obj| obj.key().is_some_and(|key| key.ends_with(".jpg")))
                .build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: Some(10),
            objects: &[("mixed/image.jpg", 1048576), ("mixed/document.txt", 2048)],
            expected_files: &[("/local/downloads/mixed/image.jpg", 1048576)],
            objects_downloaded: 1,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_with_filter_callback timed out");
}

// ref: RUST-1426
/// download-directory.json: "Test download directory - skip folder objects".
///
/// The corpus lists the expected file as `/local/downloads/file1.txt`, with
/// the prefix removed; the derivation rule keeps the full key.
#[tokio::test]
async fn sep_download_directory_skip_folder_objects() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder().prefix("data/").build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: None,
            objects: &[
                ("data/folder1/", 0),
                ("data/file1.txt", 1024),
                ("data/folder2/", 0),
            ],
            expected_files: &[("/local/downloads/data/file1.txt", 1024)],
            objects_downloaded: 1,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_skip_folder_objects timed out");
}

/// download-directory.json: "Test download directory - path traversal security validation".
#[tokio::test]
async fn sep_download_directory_path_traversal_security_validation() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder().build(),
            failure_policy: FailedTransferPolicy::Continue,
            max_concurrency: None,
            objects: &[("../../../etc/passwd", 1024), ("safe-file.txt", 2048)],
            expected_files: &[("/local/downloads/safe-file.txt", 2048)],
            objects_downloaded: 1,
            objects_failed: 1,
        }),
    )
    .await
    .expect("sep_download_directory_path_traversal_security_validation timed out");
}

/// download-directory.json: "Test download directory - path traversal with relative parent directory".
#[tokio::test]
async fn sep_download_directory_path_traversal_with_relative_parent_directory() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder().build(),
            failure_policy: FailedTransferPolicy::Continue,
            max_concurrency: None,
            objects: &[("../2023/Jan/1.png", 1024), ("safe-file.txt", 2048)],
            expected_files: &[("/local/downloads/safe-file.txt", 2048)],
            objects_downloaded: 1,
            objects_failed: 1,
        }),
    )
    .await
    .expect("sep_download_directory_path_traversal_with_relative_parent_directory timed out");
}

/// download-directory.json: "Test download directory - complex path traversal with multiple levels".
#[tokio::test]
async fn sep_download_directory_complex_path_traversal_with_multiple_levels() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/local/downloads",
            walker: S3Walker::builder().build(),
            failure_policy: FailedTransferPolicy::Continue,
            max_concurrency: None,
            objects: &[
                ("foo/../2023/../../Jan/1.png", 1024),
                ("normal/file.txt", 512),
            ],
            expected_files: &[("/local/downloads/normal/file.txt", 512)],
            objects_downloaded: 1,
            objects_failed: 1,
        }),
    )
    .await
    .expect("sep_download_directory_complex_path_traversal_with_multiple_levels timed out");
}

// ref: RUST-1426
/// download-directory-cross-platform-compatibility.json: "Test download directory - Linux happy case".
#[cfg(unix)]
#[tokio::test]
async fn sep_download_directory_linux_happy_case() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/home/user/downloads",
            walker: S3Walker::builder().prefix("projects/").build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: Some(10),
            objects: &[
                ("projects/src/main.py", 4096),
                ("projects/config/settings.json", 1024),
                ("projects/README.md", 2048),
            ],
            expected_files: &[
                ("/home/user/downloads/projects/src/main.py", 4096),
                ("/home/user/downloads/projects/config/settings.json", 1024),
                ("/home/user/downloads/projects/README.md", 2048),
            ],
            objects_downloaded: 3,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_linux_happy_case timed out");
}

// ref: RUST-1426
/// download-directory-cross-platform-compatibility.json: "Test download directory - macOS happy case".
#[cfg(unix)]
#[tokio::test]
async fn sep_download_directory_macos_happy_case() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: "/Users/user/Downloads",
            walker: S3Walker::builder().prefix("photos/").build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: Some(10),
            objects: &[
                ("photos/2023/vacation.jpg", 3145728),
                ("photos/2023/family.jpg", 2621440),
                ("photos/metadata.json", 512),
            ],
            expected_files: &[
                ("/Users/user/Downloads/photos/2023/vacation.jpg", 3145728),
                ("/Users/user/Downloads/photos/2023/family.jpg", 2621440),
                ("/Users/user/Downloads/photos/metadata.json", 512),
            ],
            objects_downloaded: 3,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_macos_happy_case timed out");
}

// ref: RUST-1426
/// download-directory-cross-platform-compatibility.json: "Test download directory - Windows happy case".
#[cfg(windows)]
#[tokio::test]
async fn sep_download_directory_windows_happy_case() {
    timeout(
        TEST_TIMEOUT,
        run_corpus_case(CorpusCase {
            destination: r"C:\Downloads",
            walker: S3Walker::builder().prefix("documents/").build(),
            failure_policy: FailedTransferPolicy::Abort,
            max_concurrency: Some(10),
            objects: &[
                ("documents/report.pdf", 1048576),
                ("documents/data/spreadsheet.xlsx", 2097152),
                ("documents/readme.txt", 1024),
            ],
            expected_files: &[
                (r"C:\Downloads\documents\report.pdf", 1048576),
                (r"C:\Downloads\documents\data\spreadsheet.xlsx", 2097152),
                (r"C:\Downloads\documents\readme.txt", 1024),
            ],
            objects_downloaded: 3,
            objects_failed: 0,
        }),
    )
    .await
    .expect("sep_download_directory_windows_happy_case timed out");
}

// ---------------------------------------------------------------------------
// ROUND-TRIP
// ---------------------------------------------------------------------------

/// Build a nested directory tree, upload it, download it to a fresh dir,
/// and assert the two trees are byte-identical.
async fn test_upload_then_download_objects_roundtrip(rt: RuntimeConfig) {
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
            .walker(S3Walker::builder().prefix(prefix).build())
            .initiate()
            .expect("initiate download_objects");

        let download_output = download_handle.join().await.expect("join download_objects");
        assert_eq!(files.len() as u64, download_output.objects_downloaded());
        assert!(download_output.failed_transfers().is_empty());

        // Verify the downloaded tree, written at the full keys, matches the
        // source tree exactly.
        verify_dir(&dest_dir.path().join(prefix), &expected_content);

        m.handle.shutdown().await.expect("shutdown");
    })
    .await
    .expect("test_upload_then_download_objects_roundtrip timed out");
}

#[tokio::test]
async fn test_upload_then_download_objects_roundtrip_mock_gp() {
    test_upload_then_download_objects_roundtrip(RuntimeConfig::default()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_upload_then_download_objects_roundtrip_tokio_mt() {
    test_upload_then_download_objects_roundtrip(RuntimeConfig::multi_thread_tokio()).await;
}
