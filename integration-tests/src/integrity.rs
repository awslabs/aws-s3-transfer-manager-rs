/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Object-integrity contract tests.
//!
//! These tests pin the integrity guarantee a caller gets for a given download,
//! so it cannot change silently. They run the same body across [`Target`]s (see
//! `harness`) against a real HTTP mock server.
//!
//! # How checksums work in the transfer manager
//!
//! Upload: the transfer manager never hashes object bytes. It selects an
//! algorithm via `ChecksumStrategy` and the SDK computes the checksum while
//! streaming. S3 verifies on receipt and stores the value.
//!
//! Download: the SDK validates checksums covering individual response bodies.
//! The transfer manager also hashes accepted ranges and combines their CRCs to
//! verify a stored full-object checksum before destination finalization. Mismatches
//! are reported by `join()`; streamed bytes may arrive before the final check.
//!
//! `DownloadOutput::integrity_checks()` reports two things:
//!   * the object's stored checksum values and type from checksum-mode HEAD for
//!     validated whole-object downloads. Part metadata cannot replace them. A
//!     composite value is never surfaced, only its type.
//!     `DownloadOutput::object_meta` carries the same values, except that it
//!     clears composite values and values from a response for part of the object.
//!   * `ChecksumValidation`: `Validated{algorithm}` when the whole object was
//!     validated, else `NotValidated{reason}`.
//!
//! Whole-object checksum types:
//!   * Full object: one checksum over all bytes. Single-part objects always; a
//!     multipart object when uploaded with a full-object type (CRC only).
//!   * Composite (`<base64>-<parts>`): a checksum of the part checksums. Cannot be
//!     validated against object bytes. SHA in a multipart upload is always
//!     composite.
//!
//! A checksum value being present is not the same as the bytes being validated.
//! The value records what S3 stored; validation records whether the delivered
//! bytes were checked against it.
//!
//! # Coverage
//!
//! Two guarantees, each a table. "Request mode" is the download request's
//! `checksum_mode` (whether the caller asks S3 to return a stored checksum for
//! the SDK to validate against): ENABLED, or unset. "Client validation" is the
//! S3 client's `ResponseChecksumValidation` (which decides whether an unset mode
//! is auto-promoted to ENABLED): WhenSupported is the default. "Backend" is where
//! the body runs: `mock` only, or `mock + real S3` (the same assertions gated
//! under `--cfg e2e_test`, run against both GP and Express buckets). Tamper tests
//! are `mock` only by design: fault injection cannot be done against real S3.
//!
//! ## Positive: a good download round-trips and reports an honest verdict
//!
//! | Object                       | Request mode | Client validation | Verdict asserted                                       | Backend     |
//! | ---------------------------- | ------------ | ----------------- | ------------------------------------------------------ | ----------- |
//! | single-part                  | unset        | WhenSupported     | Validated{Crc32}                                       | mock + real |
//! | single-part                  | unset        | WhenRequired      | NotValidated{Disabled}                                 | mock        |
//! | single-part                  | ENABLED      | default           | object value surfaced; Validated{Crc32}                | mock + real |
//! | multipart full-obj (GP)      | ENABLED      | default           | HEAD's object value, not a part's; Validated{Crc32}    | mock + real |
//! | multipart full-obj (Express) | ENABLED      | default           | HEAD's object value, not a part's; Validated{Crc32}    | real        |
//! | multipart composite          | ENABLED      | default           | COMPOSITE type, no value; Validated{Sha256}            | mock + real |
//! | multipart (Auto sz)          | ENABLED      | default           | round-trip using configured ranges                     | mock + real |
//! | composite MPU (Auto sz)      | ENABLED      | default           | Validated{Crc32}                                       | mock        |
//! | single-part (file)           | ENABLED      | default           | round-trip on disk; Validated{Crc32}                   | mock + real |
//! | composite MPU value          | ENABLED      | default           | object value == composite_checksum                     | real        |
//!
//! ## Negative: a tampered download MUST fail (mock only)
//!
//! | Object              | Request mode | Tamper injected     | Asserts                                 |
//! |---------------------|--------------|---------------------|-----------------------------------------|
//! | single-part         | ENABLED      | CorruptBody         | IntegrityError                          |
//! | single-part (file)  | ENABLED      | CorruptBody         | IntegrityError; temp cleaned, dest absent |
//! | multipart (matched) | ENABLED      | WrongStoredChecksum | IntegrityError (explicit matched size)  |
//! | multipart (Auto sz) | ENABLED      | WrongStoredChecksum | IntegrityError (auto-aligned, caught)   |
//! | single-PUT split    | ENABLED      | CorruptBody         | IntegrityError |
//!
//! Tamper tests assert the specific `ErrorKind::IntegrityError` (the kind a
//! checksum mismatch surfaces as), not merely `is_err()`, so an unrelated
//! failure cannot pass them.
//!
//! For COMPOSITE objects with automatic part sizing, a partNumber=1 GET discovers
//! the stored part size. Aligned ranges carry part checksums that the SDK validates.
//! An explicit download part size is respected, so misaligned ranges can lack those
//! checksums. FULL_OBJECT CRCs use configured ranges and local combination, including
//! general purpose buckets whose ranges carry no checksum.
//!
//! `algorithm_tests` crosses object shapes with every supported upload algorithm;
//! `download_tests` covers faults, destinations and download options. CRC calculation
//! and combination are checked independently against direct SDK hashing.

mod algorithm_tests;
mod bulk_tests;
mod coverage_tests;
mod download_tests;
mod liveness_tests;
mod negative_control_tests;
mod service_tests;
pub(crate) mod test_util;
mod upload_tests;

/// Every case in the two case tables.
fn table_cases() -> test_util::vocab::Cases {
    let mut cases = Vec::new();
    algorithm_tests::cases(&mut cases);
    download_tests::cases(&mut cases);
    cases
}

use crate::assertions::{
    assert_integrity_error, assert_not_validated, assert_same_content, assert_validated,
};
use crate::harness::{BucketKind, Target};
use aws_s3_transfer::config::PartSize;
use aws_s3_transfer::types::checksum::{Crc32, Sha256};
use aws_s3_transfer::types::ByteSize;
use aws_s3_transfer::types::ChecksumMode;
use aws_s3_transfer::types::ChecksumStrategy;
use aws_s3_transfer::types::{ChecksumAlgorithm, NotValidatedReason};
use s3_mock_server::{FaultType, Occurrence};

/// A part size pinned equally for upload and download. Multipart downloads align
/// automatically for an Auto part size; this pins an explicit size to cover the
/// explicit-part-size path (where the user's size must match the upload to
/// validate).
const ALIGNED_PART_SIZE: PartSize = PartSize::Target(ByteSize::new(5 * 1024 * 1024));

fn small() -> Vec<u8> {
    b"hello world, integrity contract".to_vec()
}

/// Data larger than the default part size, forcing a multipart upload.
fn multipart_data() -> Vec<u8> {
    (0..ByteSize::mib(20).as_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

/// Data above the default part size (8 MiB) but below the multipart-upload
/// threshold (16 MiB): uploaded as a SINGLE PUT (full-object checksum, no `-N`),
/// but downloaded as multiple ranged GETs (split for throughput). S3 returns no
/// per-range checksum for a single-stored-part object, so the SDK validates
/// nothing on the split chunks.
fn large_single_put() -> Vec<u8> {
    (0..ByteSize::mib(12).as_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

/// Write `len` bytes of the deterministic `i % 256` pattern to `path` and return
/// their CRC64NVME digest, in a single streaming pass (no full-file buffer). Used
/// to stage a large upload source on disk without holding it in memory.
fn write_pattern_file(path: &std::path::Path, len: usize) -> Vec<u8> {
    use aws_smithy_checksums::ChecksumAlgorithm;
    use std::io::Write;
    let mut hasher = ChecksumAlgorithm::Crc64Nvme.into_impl();
    let mut f = std::io::BufWriter::new(std::fs::File::create(path).expect("create source file"));
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    let mut written = 0;
    while written < len {
        let n = buf.len().min(len - written);
        for (j, b) in buf[..n].iter_mut().enumerate() {
            *b = ((written + j) % 256) as u8;
        }
        f.write_all(&buf[..n]).expect("write source file");
        hasher.update(&buf[..n]);
        written += n;
    }
    f.flush().expect("flush source file");
    hasher.finalize().to_vec()
}

/// CRC64NVME digest of a file, read in bounded chunks (no full-file buffer). Used
/// to verify a downloaded large object matches the uploaded source independently
/// of S3's own checksum.
fn crc64nvme_file(path: &std::path::Path) -> Vec<u8> {
    use aws_smithy_checksums::ChecksumAlgorithm;
    use std::io::Read;
    let mut hasher = ChecksumAlgorithm::Crc64Nvme.into_impl();
    let mut f = std::fs::File::open(path).expect("open file");
    let mut buf = vec![0u8; 8 * 1024 * 1024];
    loop {
        let n = f.read(&mut buf).expect("read file");
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    hasher.finalize().to_vec()
}

// single-part, default checksum_mode (unset) -----------------------------------
//
// Leaving checksum_mode unset does NOT mean validation is off: the SDK's
// GetObject mutator auto-enables ChecksumMode when the client's
// ResponseChecksumValidation resolves to WhenSupported (the default). So a
// default download validates the whole-object checksum.

async fn single_part_mode_default(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let (bytes, output) = t.download("obj", None).await.expect("download");
    assert_same_content(&data, &bytes);
    assert_validated(&output, ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

#[tokio::test]
async fn single_part_mode_default_mock_gp() {
    single_part_mode_default(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn single_part_mode_default_real_gp() {
    single_part_mode_default(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn single_part_mode_default_real_express() {
    single_part_mode_default(Target::real_express()).await;
}

// single-part, client ResponseChecksumValidation=WhenRequired -------------------
//
// The only configuration under which an unset-mode download is genuinely not
// validated: the SDK does not auto-enable ChecksumMode, so the verdict is
// Disabled. Pins that Disabled means "validation will not happen," distinct from
// the default (validation attempted) above.

async fn single_part_when_required_disabled(target: Target) {
    let t = target.connect_mock_when_required().await;
    let data = small();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let (bytes, output) = t.download("obj", None).await.expect("download");
    assert_same_content(&data, &bytes);
    assert_not_validated(&output, NotValidatedReason::Disabled);

    t.shutdown().await;
}

#[tokio::test]
async fn single_part_when_required_disabled_mock_gp() {
    single_part_when_required_disabled(Target::mock_gp()).await;
}

// single-part, checksum_mode on -------------------------------------------------

async fn single_part_mode_on(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    // Whole object in one chunk: the object's checksum is surfaced.
    assert!(
        output.integrity_checks().checksum_crc32().is_some(),
        "whole-object checksum should be surfaced"
    );
    assert_validated(&output, ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

#[tokio::test]
async fn single_part_mode_on_mock_gp() {
    single_part_mode_on(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn single_part_mode_on_real_gp() {
    single_part_mode_on(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn single_part_mode_on_real_express() {
    single_part_mode_on(Target::real_express()).await;
}

// multipart full-object ---------------------------------------------------------

async fn multipart_full_object(target: Target) {
    let t = target.connect().await;
    let data = multipart_data();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    let stored = t
        .s3_client()
        .await
        .head_object()
        .bucket(t.bucket())
        .key(t.key("obj"))
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("stored object checksum");
    assert!(stored.checksum_crc32().is_some());
    assert_eq!(
        output.integrity_checks().checksum_crc32(),
        stored.checksum_crc32()
    );
    assert_eq!(
        output.object_meta.checksum_crc32.as_deref(),
        stored.checksum_crc32(),
        "object metadata must carry HEAD's object checksum, not a part's"
    );

    assert_validated(&output, ChecksumAlgorithm::Crc32);

    // S3 returns the checksum of a part of a FULL_OBJECT multipart object only in directory
    // buckets; a general purpose bucket returns none.
    let part = t.head_part("obj", 1).await;
    match target.bucket_kind {
        BucketKind::GeneralPurpose => {
            assert!(
                part.checksum_crc32().is_none(),
                "a general purpose bucket returns no checksum for a part of a FULL_OBJECT object"
            );
        }
        BucketKind::Express => {
            assert!(
                part.checksum_crc32().is_some(),
                "a directory bucket returns the checksum of a part of a FULL_OBJECT object"
            );
            assert_eq!(
                part.checksum_type(),
                Some(&aws_sdk_s3::types::ChecksumType::FullObject)
            );
        }
    }

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_full_object_mock_gp() {
    multipart_full_object(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_full_object_real_gp() {
    multipart_full_object(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_full_object_real_express() {
    multipart_full_object(Target::real_express()).await;
}

// multipart composite -----------------------------------------------------------

async fn multipart_composite(target: Target) {
    let t = target.connect().await;
    let data = multipart_data();
    t.put("obj", data.clone(), ChecksumStrategy::composite(Sha256))
        .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    let stored = t
        .s3_client()
        .await
        .head_object()
        .bucket(t.bucket())
        .key(t.key("obj"))
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("stored object checksum");
    // A COMPOSITE value is a checksum of part checksums, so only the type is reported.
    assert!(stored.checksum_sha256().is_some());
    assert_eq!(
        stored.checksum_type(),
        Some(&aws_sdk_s3::types::ChecksumType::Composite)
    );
    assert_eq!(output.integrity_checks().checksum_sha256(), None);
    assert_eq!(
        output.integrity_checks().checksum_type(),
        Some(&aws_s3_transfer::types::ChecksumType::Composite)
    );
    assert_eq!(output.object_meta.checksum_sha256, None);
    assert_eq!(
        output.object_meta.checksum_type,
        Some(aws_s3_transfer::types::ChecksumType::Composite)
    );
    assert_validated(&output, ChecksumAlgorithm::Sha256);

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_composite_mock_gp() {
    multipart_composite(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_composite_real_gp() {
    multipart_composite(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_composite_real_express() {
    multipart_composite(Target::real_express()).await;
}

/// Real-S3 authority gate: assert that S3's composite checksum value for a
/// multipart upload exactly equals our `s3_mock_server::composite_checksum`
/// computation. Uses the raw SDK client to control per-part checksums.
#[cfg(e2e_test)]
#[tokio::test]
async fn mpu_composite_value_matches_expected_real_gp() {
    use aws_sdk_s3::types::{
        ChecksumAlgorithm, ChecksumMode, CompletedMultipartUpload, CompletedPart,
    };

    let t = Target::real_gp().connect().await;
    let s3 = t.s3();
    let bucket = t.bucket();
    let key = &format!("upload/mpu-composite-checksum-{}", uuid::Uuid::new_v4());

    let algorithm = ChecksumAlgorithm::Crc32;

    // Two parts, each >= 5 MiB (S3's minimum part size for MPU).
    let part1: Vec<u8> = (0..ByteSize::mib(5).as_usize())
        .map(|i| (i % 251) as u8)
        .collect();
    let part2: Vec<u8> = (0..ByteSize::mib(5).as_usize())
        .map(|i| (i % 239) as u8)
        .collect();

    let create = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .checksum_algorithm(algorithm.clone())
        .send()
        .await
        .expect("create MPU");
    let upload_id = create.upload_id().unwrap();

    let r1 = s3
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .checksum_algorithm(algorithm.clone())
        .body(aws_sdk_s3::primitives::ByteStream::from(part1))
        .send()
        .await
        .expect("upload part 1");

    let r2 = s3
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .checksum_algorithm(algorithm.clone())
        .body(aws_sdk_s3::primitives::ByteStream::from(part2))
        .send()
        .await
        .expect("upload part 2");

    let c1 = r1.checksum_crc32().unwrap().to_owned();
    let c2 = r2.checksum_crc32().unwrap().to_owned();

    s3.complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(r1.e_tag().unwrap())
                        .checksum_crc32(&c1)
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(r2.e_tag().unwrap())
                        .checksum_crc32(&c2)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("complete MPU");

    let head = s3
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .expect("head object");

    let actual = head
        .checksum_crc32()
        .expect("expected composite CRC32 in HeadObject response");
    let expected = s3_mock_server::composite_checksum(&[c1, c2], algorithm);
    assert_eq!(
        actual, expected,
        "real S3 composite value must match our composite_checksum computation"
    );

    // Cleanup
    s3.delete_object().bucket(bucket).key(key).send().await.ok();
    t.shutdown().await;
}

// tamper -> error (the integrity-critical negative; mock only) -------------------

async fn tampered_single_part_errors(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put("obj", data, ChecksumStrategy::full_object(Crc32))
        .await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_single_part_errors_mock_gp() {
    tampered_single_part_errors(Target::mock_gp()).await;
}

// A real checksum mismatch populates every IntegrityError field from the SDK's
// checksum outcome for the response.
#[tokio::test]
async fn integrity_error_carries_extracted_fields_mock_gp() {
    let t = Target::mock_gp().connect().await;
    let data = small();
    t.put("obj", data, ChecksumStrategy::full_object(Crc32))
        .await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    let ie = crate::assertions::expect_integrity_error(result);

    assert_eq!(
        ie.algorithm(),
        Some(&aws_s3_transfer::types::ChecksumAlgorithm::Crc32),
        "the algorithm should come from the SDK's checksum outcome"
    );
    assert!(
        ie.expected().is_some(),
        "expected checksum should be reported by the SDK's checksum outcome"
    );
    assert!(
        ie.computed().is_some(),
        "computed checksum should be reported by the SDK's checksum outcome"
    );
    assert_ne!(
        ie.expected(),
        ie.computed(),
        "a mismatch must report differing expected/computed values"
    );

    t.shutdown().await;
}

// tamper -> error on the FILE path (validate before rename) ---------------------
//
// The in-memory tamper test above drains the body; this exercises the managed
// file-sink path (slot buffer -> pwrite -> temp file -> rename), where the
// integrity contract is "validate before rename": a checksum failure must error
// AND leave no file at the destination (the temp is cleaned up). A corrupt body
// landing at the destination would be silent on-disk corruption.

async fn tampered_single_part_file_errors(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put("obj", data, ChecksumStrategy::full_object(Crc32))
        .await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = dir.path().join("obj.bin");

    let result = t
        .download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await;
    assert_integrity_error(result);
    assert!(
        !dest.exists(),
        "destination must not be created on a failed download"
    );
    // No leftover temp files (dest.s3tmp.*) in the directory.
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read tempdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".s3tmp."))
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_single_part_file_errors_mock_gp() {
    tampered_single_part_file_errors(Target::mock_gp()).await;
}

// successful file download round-trips ------------------------------------------

async fn file_download_round_trips(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = dir.path().join("obj.bin");
    let output = t
        .download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await
        .expect("file download");

    let on_disk = std::fs::read(&dest).expect("read destination");
    assert_same_content(&data, &on_disk);
    assert_validated(&output, ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

#[tokio::test]
async fn file_download_round_trips_mock_gp() {
    file_download_round_trips(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn file_download_round_trips_real_gp() {
    file_download_round_trips(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn file_download_round_trips_real_express() {
    file_download_round_trips(Target::real_express()).await;
}

// multi-chunk file-sink downloads ----------------------------------------------
//
// The file sink (SlotBuffer) coalesces contiguous filled slots and pwrites them,
// then atomically renames the temp file to the destination on success. small()
// is a single chunk; these drive ~64 parts at the aligned part size so the
// multi-slot flush/coalesce path runs and, on tamper, a partially-written temp
// must be discarded across many already-flushed chunks. Source and dest are
// files (streamed, not buffered in memory).

/// Number of aligned parts for the multi-chunk file tests. 64 * 5 MiB = 320 MiB,
/// enough to coalesce and flush across dozens of slots.
///
/// On 32-bit targets, 16 parts (80 MiB). The in-memory mock holds an upload's parts
/// and then one contiguous copy of the object, which a 32-bit address space shared
/// with the other tests can't reliably provide at 320 MiB.
const MANY_PARTS: usize = if cfg!(target_pointer_width = "32") {
    16
} else {
    64
};

fn many_part_len() -> usize {
    MANY_PARTS * ByteSize::mib(5).as_usize()
}

/// A many-part object downloaded to a file round-trips on disk. Ranges align to
/// the stored part size (explicit, matching the upload), so each chunk validates;
/// the coalescing flush writes all parts and renames to the destination.
async fn many_part_file_download_round_trips(target: Target) {
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    let len = many_part_len();
    let src_digest = write_pattern_file(&src, len);

    t.put_from_path("obj", &src, ChecksumStrategy::full_object(Crc32))
        .await;
    t.download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await
        .expect("file download");

    assert_eq!(
        std::fs::metadata(&dest).expect("stat dest").len() as usize,
        len,
        "downloaded size mismatch"
    );
    assert_eq!(crc64nvme_file(&dest), src_digest, "content digest mismatch");

    t.shutdown().await;
}

#[tokio::test]
async fn many_part_file_download_round_trips_mock_gp() {
    many_part_file_download_round_trips(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn many_part_file_download_round_trips_real_gp() {
    many_part_file_download_round_trips(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn many_part_file_download_round_trips_real_express() {
    many_part_file_download_round_trips(Target::real_express()).await;
}

/// A tampered chunk partway through a many-part file download is caught, and the
/// destination is never created even though earlier chunks were already flushed
/// to the temp file. The fault skips the first chunks so the failure lands after
/// real bytes have been written: this exercises discarding a partially-written
/// temp, not a fail-on-first-chunk. Ranged GETs are concurrent, so the moment of
/// failure varies, but the outcome (transfer fails, dest absent, temp cleaned) is
/// deterministic.
async fn tampered_many_part_file_cleans_up(target: Target) {
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    let _ = write_pattern_file(&src, many_part_len());

    t.put_from_path("obj", &src, ChecksumStrategy::composite(Crc32))
        .await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    // Skip the first 8 chunks so several parts flush to the temp before the
    // corruption hits; then corrupt every subsequent body (checksum intact).
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        8,
        Occurrence::Always,
    );

    let result = t
        .download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await;
    assert_integrity_error(result);
    assert!(
        !dest.exists(),
        "destination must not be created on a failed download"
    );
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read tempdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(".s3tmp."))
        .collect();
    assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_many_part_file_cleans_up_mock_gp() {
    tampered_many_part_file_cleans_up(Target::mock_gp()).await;
}

// large-object download (real S3 only) -----------------------------------------
//
// Sized past the 512-slot body buffer window (DEFAULT_BODY_SLOT_CAPACITY) so the
// download crosses slot-index wraparound and exercises seq-window backpressure
// against a real disk-bound file sink, also crossing the 5 GiB single-PUT /
// multipart boundary. Real-S3 only: the data volume has no place in mock CI.
// Source and dest are files; verification re-derives a CRC64NVME over the
// downloaded file independently of S3.

/// Serializes the large-object download tests against each other.
///
/// Each large-object test holds a 6 GiB source and a 6 GiB (preallocated)
/// destination on the runner's temp volume. Two such tests running concurrently
/// exceed the ~14 GiB free on a hosted CI runner and fail with ENOSPC. Every
/// large-object test acquires this gate for its duration so at most one runs at
/// a time, bounding peak temp usage to a single test's footprint.
#[cfg(e2e_test)]
static SERIAL_TEST_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(e2e_test)]
async fn large_object_file_download_validates(target: Target) {
    use aws_s3_transfer::config::PartSize;
    use aws_s3_transfer::types::checksum::Crc64Nvme;

    // Hold the gate for the whole test so peak temp-disk usage stays within one
    // test's footprint (see SERIAL_TEST_GATE).
    let _serial = SERIAL_TEST_GATE.lock().await;

    // 6 GiB at an 8 MiB part size = ~768 ranged GETs, past the 512-slot window.
    let part = ByteSize::mib(8).as_u64();
    let t = target
        .connect_with(Some(PartSize::Target(ByteSize::new(part))))
        .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    let total = ByteSize::gib(6).as_usize();
    let src_digest = write_pattern_file(&src, total);

    t.put_from_path("large-obj", &src, ChecksumStrategy::full_object(Crc64Nvme))
        .await;
    // The source is not needed past upload; remove it before the download
    // preallocates an equal-sized destination, halving peak temp-disk usage.
    std::fs::remove_file(&src).expect("remove source after upload");

    t.download_to_path("large-obj", &dest, Some(ChecksumMode::Enabled))
        .await
        .expect("large file download");

    assert_eq!(
        std::fs::metadata(&dest).expect("stat dest").len() as usize,
        total,
        "downloaded size mismatch"
    );
    assert_eq!(crc64nvme_file(&dest), src_digest, "content digest mismatch");

    t.shutdown().await;
}

#[cfg(e2e_test)]
#[tokio::test]
async fn large_object_file_download_validates_real_gp() {
    large_object_file_download_validates(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn large_object_file_download_validates_real_express() {
    large_object_file_download_validates(Target::real_express()).await;
}

async fn tampered_multipart_errors(target: Target) {
    // Explicit part size, applied to BOTH upload and download, so the download
    // ranges match the uploaded part boundaries without relying on auto-alignment
    // (auto-alignment only fires for an Auto part size). Each aligned chunk carries
    // a per-part checksum the SDK validates, so a tampered checksum fails.
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::composite(Crc32)).await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    // Fail every chunk for a deterministic transfer outcome.
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::WrongStoredChecksum,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_multipart_errors_mock_gp() {
    tampered_multipart_errors(Target::mock_gp()).await;
}

// multipart downloads with the default part size --------------------------------

/// A full-object CRC upload round-trips with the configured download ranges.
/// `multipart_full_object` also asserts the stored values and validation verdict.
async fn multipart_aligned_round_trips(target: Target) {
    let t = target.connect().await;
    let data = multipart_data();
    t.put("obj", data.clone(), ChecksumStrategy::full_object(Crc32))
        .await;

    let (bytes, _output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_aligned_round_trips_mock_gp() {
    multipart_aligned_round_trips(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_aligned_round_trips_real_gp() {
    multipart_aligned_round_trips(Target::real_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn multipart_aligned_round_trips_real_express() {
    multipart_aligned_round_trips(Target::real_express()).await;
}

/// Default part size (Auto): a multipart object uploaded with composite checksums
/// reads `Validated`. Validation re-discovers it with a partNumber=1 GET, whose
/// response is chunk 0, and aligns every other range to a stored part, so each
/// chunk carries its part's checksum.
#[tokio::test]
async fn multipart_auto_part_size_validated_mock_gp() {
    let t = Target::mock_gp().connect().await;
    let data = multipart_data();
    t.put("obj", data.clone(), ChecksumStrategy::composite(Crc32))
        .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    assert_validated(&output, ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

/// Default part size (Auto): a tampered chunk of a multipart download IS caught.
/// The TM aligns download ranges to the stored part boundaries, so each ranged
/// GET carries a per-part checksum the SDK validates; a corrupted part fails the
/// download. This is the integrity-critical guarantee for multipart downloads.
async fn multipart_default_tamper_caught(target: Target) {
    let t = target.connect().await; // default (Auto) part size -> auto-aligned
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::composite(Crc32)).await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::WrongStoredChecksum,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_default_tamper_caught_mock_gp() {
    multipart_default_tamper_caught(Target::mock_gp()).await;
}

// large single-PUT object split for throughput ---------------------------------

/// A corrupted range of a split single-PUT download fails against the stored CRC,
/// even though its individual responses carry no checksum for SDK validation.
#[tokio::test]
// ref: RUST-1758
async fn single_put_split_tamper_caught_mock_gp() {
    let t = Target::mock_gp().connect().await; // default part sizes
    let data = large_single_put();
    t.put("obj", data, ChecksumStrategy::full_object(Crc32))
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert_integrity_error(result);

    t.shutdown().await;
}
