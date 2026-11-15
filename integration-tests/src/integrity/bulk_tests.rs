/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Directory transfer integrity: `download_objects` and `upload_objects` with intact, corrupt and
//! rejected children.
//!
//! A `download_objects` child gets only its bucket and key, so it is validated as the client's
//! `ResponseChecksumValidation` decides; these tests use the default, `WhenSupported`. An
//! `upload_objects` child gets only its bucket, key and body, so it uploads with the default
//! checksum strategy. The tests run on the mock, which corrupts and rejects children on request.

use std::path::{Path, PathBuf};

use aws_sdk_s3_transfer_manager::io::walk::S3Walker;
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use s3_mock_server::{FaultTarget, FaultType, Occurrence, Operation};

use super::test_util::vocab::CaseTarget;
use crate::assertions::{check_integrity_error, is_download_temp_file};
use crate::harness::{ClientOptions, Target, TmTestClient};
use crate::test_util::data::object_bytes;
use crate::test_util::object::{
    assert_nothing_stored, assert_stored_bytes, stored_checksum, write_object_file, Alg, Layout,
    Object, MIB,
};

/// The object byte a corrupted child has flipped: inside its second 8 MiB part, so no GET that
/// only reads the first part carries it. A corrupted child must be longer than this.
const CORRUPT_AT: u64 = 12 * MIB + 7;

/// The part size and multipart threshold of the `upload_objects` tests.
const PART_SIZE: u64 = 8 * MIB;

/// Every file under `dir`, recursively.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("list directory") {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// Uploads each `(name, object)` under `prefix/` with a distinct seed from `first_seed`, and
/// returns each object's logical key and seed.
async fn upload_all(
    t: &TmTestClient,
    prefix: &str,
    objects: &[(&str, Object)],
    first_seed: u32,
) -> Vec<(String, u32)> {
    let mut uploaded = Vec::new();
    for (seed, (name, object)) in (first_seed..).zip(objects) {
        let key = format!("{prefix}/{name}");
        object.upload(t, &key, seed).await;
        uploaded.push((key, seed));
    }
    uploaded
}

/// Starts `download_objects` of every object under logical `prefix/` to `dest`, with `policy`.
fn download_prefix(
    t: &TmTestClient,
    prefix: &str,
    dest: &Path,
    policy: FailedTransferPolicy,
) -> aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsHandle {
    t.client_with(ClientOptions::default())
        .download_objects()
        .bucket(t.bucket())
        .destination(dest)
        .walker(
            S3Walker::builder()
                .prefix(t.key(&format!("{prefix}/")))
                .build(),
        )
        .failure_policy(policy)
        .initiate()
        .expect("initiate download_objects")
}

/// Flips byte [`CORRUPT_AT`] of every GET of logical `key` that carries it.
fn corrupt(t: &TmTestClient, key: &str) {
    t.mock().expect("a mock target").insert_fault_for(
        FaultTarget::object(t.bucket(), t.key(key)).covering(CORRUPT_AT),
        FaultType::CorruptByte { offset: CORRUPT_AT },
        0,
        Occurrence::Always,
    );
}

/// Panics unless the file `download_objects` wrote for logical `key` under `dest` holds exactly
/// object `seed`'s `len` bytes.
fn assert_downloaded(t: &TmTestClient, dest: &Path, key: &str, seed: u32, len: u64) {
    let path = dest.join(t.key(key));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        bytes == object_bytes(seed, len as usize),
        "{key} downloaded different bytes from object {seed}, {} long",
        bytes.len()
    );
}

/// One object of each algorithm-test shape that runs in a general purpose bucket, each with its own
/// seed, downloads with `download_objects` to files equal to their sources.
#[tokio::test(flavor = "multi_thread")]
async fn download_objects_mixed_shapes_round_trip() {
    let t = Target::mock_gp().connect().await;
    // The object of each algorithm-test shape that runs on `mock_gp`, with the shape's first
    // algorithm, named by the shape.
    let mut objects: Vec<(&str, Object)> = Vec::new();
    for case in super::table_cases() {
        let shape = case
            .path
            .split("::")
            .skip_while(|s| *s != "algorithm_tests")
            .nth(1);
        if let Some(shape) = shape.filter(|_| case.targets.contains(&CaseTarget::MockGp)) {
            if !objects.iter().any(|(name, _)| *name == shape) {
                objects.push((shape, case.object));
            }
        }
    }
    let uploaded = upload_all(&t, "mixed", &objects, 0x6275_0100).await;

    let dest = tempfile::tempdir().expect("create destination");
    let output = download_prefix(&t, "mixed", dest.path(), FailedTransferPolicy::Abort)
        .join()
        .await
        .expect("download_objects");
    assert!(
        output.failed_transfers().is_empty(),
        "{:?}",
        output.failed_transfers()
    );
    assert_eq!(output.objects_downloaded(), objects.len() as u64);
    assert_eq!(files_under(dest.path()).len(), objects.len());
    for ((key, seed), (_, object)) in uploaded.iter().zip(&objects) {
        assert_downloaded(&t, dest.path(), key, *seed, object.len);
    }
    t.shutdown().await;
}

/// Under `FailedTransferPolicy::Continue`, a corrupted `corrupted` child fails with an integrity
/// error in its algorithm and leaves no file or temporary file, and the other two children
/// download correctly.
async fn corrupt_child_continue(corrupted: Object, first_seed: u32) {
    let t = Target::mock_gp().connect().await;
    let objects = [
        ("corrupted", corrupted),
        ("single_put", Object::single_put(Alg::Crc32).len(5 * MIB)),
        (
            "composite",
            Object::mpu(Layout::Composite, Alg::Sha256)
                .len(20 * MIB + 3)
                .parts(8 * MIB),
        ),
    ];
    let uploaded = upload_all(&t, "dir", &objects, first_seed).await;
    let corrupted_key = &uploaded[0].0;
    corrupt(&t, corrupted_key);

    let dest = tempfile::tempdir().expect("create destination");
    let output = download_prefix(&t, "dir", dest.path(), FailedTransferPolicy::Continue)
        .join()
        .await
        .expect("download_objects");

    let failed = output.failed_transfers();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(failed[0].input().key(), Some(t.key(corrupted_key).as_str()));
    check_integrity_error(
        failed[0].error(),
        &corrupted.alg.tm().expect("an algorithm"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(output.objects_downloaded(), 2);

    let files = files_under(dest.path());
    assert!(
        !dest.path().join(t.key(corrupted_key)).exists(),
        "the failed child's file must not exist"
    );
    assert!(
        !files.iter().any(|f| is_download_temp_file(f)),
        "a temporary file remains: {files:?}"
    );
    assert_eq!(files.len(), 2, "{files:?}");
    for ((key, seed), (_, object)) in uploaded.iter().zip(&objects).skip(1) {
        assert_downloaded(&t, dest.path(), key, *seed, object.len);
    }
    t.shutdown().await;
}

/// A corrupted COMPOSITE child fails alone, with an integrity error, under
/// `FailedTransferPolicy::Continue`.
#[tokio::test(flavor = "multi_thread")]
async fn download_objects_corrupt_composite_child_continue() {
    corrupt_child_continue(
        Object::mpu(Layout::Composite, Alg::Crc32)
            .len(20 * MIB + 3)
            .parts(8 * MIB),
        0x6275_0200,
    )
    .await;
}

/// A corrupted child uploaded with the default strategy, CRC64NVME FULL_OBJECT, fails alone, with
/// an integrity error, under `FailedTransferPolicy::Continue`.
#[tokio::test(flavor = "multi_thread")]
// ref: RUST-1758
#[ignore = "not validated against the full-object checksum"]
async fn download_objects_corrupt_full_object_child_continue() {
    corrupt_child_continue(
        Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
            .len(20 * MIB + 3)
            .parts(8 * MIB),
        0x6275_0300,
    )
    .await;
}

/// Under `FailedTransferPolicy::Abort`, a corrupted COMPOSITE child fails the operation, which
/// reports the child's integrity error, and the child's file is not published.
#[tokio::test(flavor = "multi_thread")]
async fn download_objects_corrupt_child_abort() {
    let t = Target::mock_gp().connect().await;
    let corrupted = Object::mpu(Layout::Composite, Alg::Crc32)
        .len(20 * MIB + 3)
        .parts(8 * MIB);
    let objects = [
        ("corrupted", corrupted),
        ("single_put", Object::single_put(Alg::Crc32).len(5 * MIB)),
        (
            "full_object",
            Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
                .len(20 * MIB + 3)
                .parts(8 * MIB),
        ),
    ];
    let uploaded = upload_all(&t, "dir", &objects, 0x6275_0400).await;
    let corrupted_key = &uploaded[0].0;
    corrupt(&t, corrupted_key);

    let dest = tempfile::tempdir().expect("create destination");
    let result = download_prefix(&t, "dir", dest.path(), FailedTransferPolicy::Abort)
        .join()
        .await;

    let error = result.expect_err("a corrupted child must fail the operation");
    let failed = error.failed_downloads().unwrap_or_default();
    assert_eq!(failed.len(), 1, "{error:?}");
    assert_eq!(failed[0].input().key(), Some(t.key(corrupted_key).as_str()));
    check_integrity_error(
        failed[0].error(),
        &corrupted.alg.tm().expect("an algorithm"),
    )
    .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        !dest.path().join(t.key(corrupted_key)).exists(),
        "the failed child's file must not be published"
    );
    t.shutdown().await;
}

/// `upload_objects` stores each file's bytes with the default checksum: CRC64NVME for a file
/// below the multipart threshold, and CRC64NVME FULL_OBJECT for a file above it, uploaded in
/// parts.
#[tokio::test(flavor = "multi_thread")]
async fn upload_objects_stores_default_checksum() {
    const SMALL_SEED: u32 = 0x6275_0501;
    const LARGE_SEED: u32 = 0x6275_0502;
    let t = Target::mock_gp().connect().await;
    let small = Object::single_put(Alg::Crc64Nvme).len(MIB + 1);
    let large = Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
        .len(20 * MIB + 3)
        .parts(PART_SIZE);
    let source = tempfile::tempdir().expect("create source");
    write_object_file(&source.path().join("small.bin"), SMALL_SEED, small.len);
    write_object_file(&source.path().join("large.bin"), LARGE_SEED, large.len);

    let output = t
        .client_with(ClientOptions {
            part_size: Some(PART_SIZE),
            multipart_threshold: Some(PART_SIZE),
            ..Default::default()
        })
        .upload_objects()
        .bucket(t.bucket())
        .source(source.path())
        .key_prefix(t.key(""))
        .initiate()
        .expect("initiate upload_objects")
        .join()
        .await
        .expect("upload_objects");
    assert!(
        output.failed_transfers().is_empty(),
        "{:?}",
        output.failed_transfers()
    );
    assert_eq!(output.objects_uploaded(), 2);

    for (key, object, seed) in [
        ("small.bin", small, SMALL_SEED),
        ("large.bin", large, LARGE_SEED),
    ] {
        assert_eq!(
            stored_checksum(&t, key).await,
            object.expected_checksum(seed),
            "{key}"
        );
        assert_stored_bytes(&t, key, seed, object.len).await;
    }
    assert_eq!(
        t.head_part("large.bin", 1).await.parts_count(),
        Some(large.stored_part_sizes().len() as i32)
    );
    t.shutdown().await;
}

/// Under `FailedTransferPolicy::Continue`, a file whose UploadPart S3 rejects with `BadDigest`
/// fails alone with that error and stores nothing, and the other two files are stored with their
/// bytes and default checksums.
#[tokio::test(flavor = "multi_thread")]
async fn upload_objects_rejected_child_continue() {
    let t = Target::mock_gp().connect().await;
    let files = [
        (
            "small.bin",
            Object::single_put(Alg::Crc64Nvme).len(MIB + 1),
            0x6275_0601,
        ),
        (
            "rejected.bin",
            Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
                .len(20 * MIB + 3)
                .parts(PART_SIZE),
            0x6275_0602,
        ),
        (
            "large.bin",
            Object::mpu(Layout::FullObject, Alg::Crc64Nvme)
                .len(20 * MIB + 5)
                .parts(PART_SIZE),
            0x6275_0603,
        ),
    ];
    let source = tempfile::tempdir().expect("create source");
    for (name, object, seed) in &files {
        write_object_file(&source.path().join(name), *seed, object.len);
    }
    let rejected = files[1].0;
    t.mock().expect("a mock target").insert_fault_for(
        FaultTarget::operation(Operation::UploadPart)
            .bucket(t.bucket())
            .key(t.key(rejected)),
        FaultType::Error(s3_mock_server::Error::BadDigest {
            algorithm: aws_smithy_checksums::ChecksumAlgorithm::Crc64Nvme,
            completion: false,
        }),
        0,
        Occurrence::Always,
    );

    let output = t
        .client_with(ClientOptions {
            part_size: Some(PART_SIZE),
            multipart_threshold: Some(PART_SIZE),
            ..Default::default()
        })
        .upload_objects()
        .bucket(t.bucket())
        .source(source.path())
        .key_prefix(t.key(""))
        .failure_policy(FailedTransferPolicy::Continue)
        .initiate()
        .expect("initiate upload_objects")
        .join()
        .await
        .expect("upload_objects");

    let failed = output.failed_transfers();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(
        failed[0].source_path(),
        Some(source.path().join(rejected).as_path())
    );
    assert_eq!(failed[0].error().code(), Some("BadDigest"), "{failed:?}");
    assert_eq!(output.objects_uploaded(), 2);
    assert_nothing_stored(&t, rejected).await;

    for (key, object, seed) in files.iter().filter(|(name, ..)| *name != rejected) {
        assert_eq!(
            stored_checksum(&t, key).await,
            object.expected_checksum(*seed),
            "{key}"
        );
        assert_stored_bytes(&t, key, *seed, object.len).await;
    }
    t.shutdown().await;
}
