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
//! Download: the transfer manager downloads with ranged GETs. Validation of the
//! delivered bytes is performed by the SDK's response body validator when a
//! stored checksum covers exactly the bytes returned. A mismatch surfaces as an
//! error from the body stream, which the transfer manager propagates as an `Err`
//! from `join()`. A successful download therefore never carries unvalidated-but-
//! corrupt bytes that a stored checksum could have caught.
//!
//! `DownloadOutput::integrity_checks()` reports two things:
//!   * the object's checksum value members and type, surfaced only when the whole
//!     object is covered by a single returned checksum (single-part objects); for
//!     a larger multipart object the discovery chunk carries only part 1's
//!     checksum, which is not the object checksum and is not surfaced.
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
//! ## How the verdict is reached
//!
//! The SDK exposes no per-response validation outcome, so the transfer manager
//! infers coverage per chunk and aggregates: a chunk whose response carried a
//! checksum covering exactly its own bytes was compared against it by the SDK, and
//! a mismatch would have failed the body stream, so a chunk that both carried one
//! and arrived was validated. `Validated` requires *every* delivered chunk to have
//! been covered — one uncovered chunk downgrades the whole transfer, which is the
//! property a per-response outcome could not express.
//!
//! Where no chunk can carry a checksum of its own — a pinned part size that misses
//! the stored boundaries, or a large single-PUT object split into ranges — the
//! transfer manager hashes the delivered bytes itself and folds them into the
//! object's checksum. That fold outranks the per-chunk inference: it is evidence
//! about the bytes rather than about the headers. A mismatch fails the download.
//!
//! So the two shapes agree, and deliberately so: a caller who pins an awkward part
//! size gets the same verdict as the default aligned path, by a different route.
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
//! | Object              | Request mode | Client validation | Verdict asserted                      | Backend     |
//! |---------------------|--------------|-------------------|---------------------------------------|-------------|
//! | single-part         | unset        | WhenSupported     | Validated{Crc32}                      | mock + real |
//! | single-part         | unset        | WhenRequired      | NotValidated{Disabled}                | mock        |
//! | single-part         | ENABLED      | default           | object value surfaced; Validated{Crc32} | mock + real |
//! | multipart full-obj  | ENABLED      | default           | object value is NOT a part checksum   | mock + real |
//! | multipart composite | ENABLED      | default           | Validated{Sha256} — per-part, not the object value | mock + real |
//! | multipart (Auto sz) | ENABLED      | default           | Validated{Crc32} (ranges auto-aligned) | mock + real |
//! | single-part (file)  | ENABLED      | default           | round-trip on disk                    | mock + real |
//! | composite MPU value | ENABLED      | default           | object value == composite_checksum    | real        |
//! | multipart, pinned misaligned size | ENABLED | default | Validated{Crc32} (by the fold) | mock + real |
//! | multipart, explicit byte range    | ENABLED | default | NotValidated{Unavailable}    | mock + real |
//! | single-part, no stored checksum    | ENABLED | default | NotValidated{Unavailable}   | mock        |
//! | single-part, composite value only  | ENABLED | default | NotValidated{CompositeChecksum} | mock    |
//!
//! ## Negative: a tampered download MUST fail (mock only)
//!
//! | Object              | Request mode | Tamper injected     | Asserts                                 |
//! |---------------------|--------------|---------------------|-----------------------------------------|
//! | single-part         | ENABLED      | CorruptBody         | IntegrityError                          |
//! | single-part (file)  | ENABLED      | CorruptBody         | IntegrityError; temp cleaned, dest absent |
//! | multipart (matched) | ENABLED      | WrongStoredChecksum | IntegrityError (explicit matched size)  |
//! | multipart (Auto sz) | ENABLED      | WrongStoredChecksum | IntegrityError (auto-aligned, caught)   |
//! | single-PUT split    | ENABLED      | CorruptBody         | IntegrityError (transfer-computed whole-object hash) |
//! | pinned misaligned size | ENABLED   | CorruptBody         | IntegrityError (transfer-computed whole-object hash) |
//! | pinned misaligned size | unset     | CorruptBody         | IntegrityError — unset is not off, so the default path errors too |
//!
//! The one shape corruption cannot be caught in is an explicit sub-range: the only
//! value S3 holds covers the whole object, and nothing derived from it validates a
//! strict subset of those bytes, so a tampered range download succeeds. What it
//! must never do is claim the bytes were checked —
//! `explicit_range_tamper_not_caught_but_never_validated_mock_gp` pins that, and is
//! the executable statement of the limit.
//!
//! Tamper tests assert the specific `ErrorKind::IntegrityError` (the kind a
//! checksum mismatch surfaces as), not merely `is_err()`, so an unrelated
//! failure cannot pass them.
//!
//! Multipart downloads are validated by default: when validating a multipart
//! object with an Auto part size, the TM discovers the stored part size (a
//! partNumber=1 GET reports part 1's exact length) and slices download ranges to
//! it, so every range aligns to a stored part boundary and the SDK validates each
//! part (including the ragged tail). An explicit user part size is respected as-is
//! (no auto-align), so it validates only when it matches the uploaded part size.
//!
//! Where alignment is impossible the transfer manager validates the bytes itself:
//! a caller-pinned part size that cannot land on stored boundaries, and a large
//! single-PUT object split into ranged GETs, both hash each chunk as it arrives and
//! fold the results into the object's checksum, which S3 reports for the whole
//! object. Only the fold needs byte order, and it happens off the data path, so
//! chunks still complete out of order. Costs one HeadObject to learn the value.
//!
//! Algorithm is a pass-through axis for the transfer manager (it forwards the
//! choice and reads back whatever S3 returns), so per-algorithm value
//! correctness is covered at the unit and mock-server layers, not multiplied
//! through this matrix. Representative algorithms are used: CRC32 for full
//! object, SHA-256 for the composite case.

use crate::assertions::{assert_integrity_error, assert_same_content};
use crate::harness::Target;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::download::DownloadOutput;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::{ChecksumValidation, NotValidatedReason, PartSize};
use s3_mock_server::{FaultType, Occurrence};

/// A part size pinned equally for upload and download. Multipart downloads align
/// automatically for an Auto part size; this pins an explicit size to cover the
/// explicit-part-size path (where the user's size must match the upload to
/// validate).
const ALIGNED_PART_SIZE: PartSize = PartSize::Target(5 * 1024 * 1024);

fn small() -> Vec<u8> {
    b"hello world, integrity contract".to_vec()
}

/// Data larger than the default part size, forcing a multipart upload.
fn multipart_data() -> Vec<u8> {
    (0..20 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

/// Data above the download part size (5 MiB) but below the multipart-upload
/// threshold (16 MiB): uploaded as a SINGLE PUT (full-object checksum, no `-N`),
/// but downloaded as multiple ranged GETs (split for throughput). S3 returns no
/// per-range checksum for a single-stored-part object, so the SDK validates
/// nothing on the split chunks.
fn large_single_put() -> Vec<u8> {
    (0..12 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 256) as u8)
        .collect()
}

fn assert_not_validated(output: &DownloadOutput, expected: NotValidatedReason) {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::NotValidated { reason, .. } => assert_eq!(*reason, expected),
        other => panic!("expected NotValidated{{{expected:?}}}, got {other:?}"),
    }
}

/// Assert the download reports every delivered byte as validated, by `expected`.
fn assert_validated(output: &DownloadOutput, expected: aws_sdk_s3::types::ChecksumAlgorithm) {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::Validated { algorithm, .. } => assert_eq!(*algorithm, expected),
        other => panic!("expected Validated{{{expected:?}}}, got {other:?}"),
    }
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
// default download validates, and the verdict says so — the distinction this pins
// is against `Disabled`, which is what a WhenRequired client gets.

async fn single_part_mode_default(target: Target) {
    let t = target.connect().await;
    let data = small();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let (bytes, output) = t.download("obj", None).await.expect("download");
    assert_same_content(&data, &bytes);
    // Default resolves to validation on (the SDK auto-enables ChecksumMode), and
    // the whole object came back under one checksum, so it was validated.
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Crc32);

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
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
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
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    // Whole object in one chunk: the object's checksum is surfaced, and it covered
    // every delivered byte.
    assert!(
        output.integrity_checks().checksum_crc32().is_some(),
        "whole-object checksum should be surfaced"
    );
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Crc32);

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
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    // A part's checksum must not be surfaced as the object's checksum.
    assert!(
        output.integrity_checks().checksum_crc32().is_none(),
        "must not surface a part checksum as the object checksum"
    );

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
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_sha256_composite_if_multipart(),
    )
    .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    assert!(
        output.integrity_checks().checksum_sha256().is_none(),
        "must not surface a part checksum as the object checksum"
    );
    // A composite object can still be fully validated: the ranges are aligned to
    // the stored parts, and each part's own checksum is a plain value covering
    // exactly that part's bytes. What cannot be validated is the *object* value,
    // and nothing here claims to have used it.
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Sha256);

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
    use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;

    let t = Target::real_gp().connect().await;
    let s3 = t.s3();
    let bucket = t.bucket();
    let key = &format!("upload/mpu-composite-checksum-{}", uuid::Uuid::new_v4());

    let algorithm = ChecksumAlgorithm::Crc32;

    // Two parts, each >= 5 MiB (S3's minimum part size for MPU).
    let part1: Vec<u8> = (0..5 * ByteUnit::Mebibyte.as_bytes_usize())
        .map(|i| (i % 251) as u8)
        .collect();
    let part2: Vec<u8> = (0..5 * ByteUnit::Mebibyte.as_bytes_usize())
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
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
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

// Backwards-compatibility guard: a real checksum mismatch must populate the
// IntegrityError fields. These are extracted from the SDK's
// `aws_smithy_checksums::body::validate::Error` shape at the body-read boundary;
// if the SDK changes that error type, the extraction silently degrades to a bare
// IOError and integrity failures stop being distinguishable. This test fails
// loudly in that case.
#[tokio::test]
async fn integrity_error_carries_extracted_fields_mock_gp() {
    let t = Target::mock_gp().connect().await;
    let data = small();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
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

    // The expected/computed checksums must be recovered from the SDK error.
    assert!(
        ie.expected().is_some(),
        "expected checksum should be extracted from the SDK mismatch error"
    );
    assert!(
        ie.computed().is_some(),
        "computed checksum should be extracted from the SDK mismatch error"
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
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
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
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = dir.path().join("obj.bin");
    t.download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await
        .expect("file download");

    let on_disk = std::fs::read(&dest).expect("read destination");
    assert_same_content(&data, &on_disk);

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
const MANY_PARTS: usize = 64;

fn many_part_len() -> usize {
    MANY_PARTS * 5 * ByteUnit::Mebibyte.as_bytes_usize()
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

    t.put_from_path("obj", &src, ChecksumStrategy::with_calculated_crc32())
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

    t.put_from_path("obj", &src, ChecksumStrategy::with_calculated_crc32())
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
    use aws_sdk_s3_transfer_manager::types::PartSize;

    // Hold the gate for the whole test so peak temp-disk usage stays within one
    // test's footprint (see SERIAL_TEST_GATE).
    let _serial = SERIAL_TEST_GATE.lock().await;

    // 6 GiB at an 8 MiB part size = ~768 ranged GETs, past the 512-slot window.
    let part = 8 * ByteUnit::Mebibyte.as_bytes_u64();
    let t = target.connect_with(Some(PartSize::Target(part))).await;

    let dir = tempfile::tempdir().expect("tempdir");
    let src = dir.path().join("src.bin");
    let dest = dir.path().join("dest.bin");
    let total = 6 * ByteUnit::Gibibyte.as_bytes_usize();
    let src_digest = write_pattern_file(&src, total);

    t.put_from_path(
        "large-obj",
        &src,
        ChecksumStrategy::with_calculated_crc64_nvme(),
    )
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
    //
    // COMPOSITE on purpose: per-part checksums exist only for a composite object.
    // A FULL_OBJECT object has one value over all bytes and none per part, so S3
    // returns no checksum for an aligned range of one and there is nothing for this
    // fault to tamper with -- see `multipart_full_object_default_tamper_caught`,
    // which covers that shape through the mechanism that does apply to it.
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data,
        ChecksumStrategy::with_calculated_sha256_composite_if_multipart(),
    )
    .await;

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

// multipart download is aligned to the stored part size by default ---------------
//
// S3 returns a per-part checksum for a ranged GET only when the range matches a
// stored part boundary. The TM downloads with ranged
// GETs and, when validating a multipart object with an Auto part size, discovers
// the stored part size (via a partNumber=1 GET) and slices download ranges to it
// so every chunk aligns to a stored boundary (including the ragged tail) and the
// SDK validates each part. These tests pin that aligned-by-default behavior.

/// Default part size (Auto): a non-tampered multipart download round-trips. The
/// TM aligns ranges to the stored part size, so the SDK validates each part.
async fn multipart_aligned_round_trips(target: Target) {
    let t = target.connect().await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let (bytes, output) = t
        .download("obj", Some(ChecksumMode::Enabled))
        .await
        .expect("download");
    assert_same_content(&data, &bytes);
    // Ranges are auto-aligned to the stored boundaries here, so every chunk carried
    // its own part checksum and the SDK validated each one. The verdict must be the
    // same as on the misaligned path, which reaches it by folding instead: a caller
    // pinning an awkward part size must not get a *better*-looking verdict than the
    // default.
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Crc32);

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

/// Default part size (Auto): a tampered chunk of a multipart download IS caught.
/// The TM aligns download ranges to the stored part boundaries, so each ranged
/// GET carries a per-part checksum the SDK validates; a corrupted part fails the
/// download. This is the integrity-critical guarantee for multipart downloads.
async fn multipart_default_tamper_caught(target: Target) {
    let t = target.connect().await; // default (Auto) part size -> auto-aligned
    let data = multipart_data();
    // COMPOSITE, so the auto-aligned ranges carry per-part checksums for the fault
    // to tamper with; see `tampered_multipart_errors` for why FULL_OBJECT cannot be
    // tested through this fault.
    t.put(
        "obj",
        data,
        ChecksumStrategy::with_calculated_sha256_composite_if_multipart(),
    )
    .await;

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

/// The default multipart download of a FULL_OBJECT-checksum object — what this
/// crate's own CRC upload strategies produce — fails on corruption.
///
/// This is the shape with no per-part checksums at all, so alignment buys nothing and
/// the transfer's own fold over the delivered bytes is the only thing that can catch
/// it. The sibling tamper tests cannot cover this shape: they corrupt a stored
/// checksum, and this one has none to corrupt per range.
#[tokio::test]
async fn multipart_full_object_default_tamper_caught_mock_gp() {
    let t = Target::mock_gp().connect().await; // Auto part size -> auto-aligned
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
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

// large single-PUT object split for throughput ---------------------------------
//
// An object uploaded as a SINGLE PUT (below the multipart-upload threshold) but
// larger than the download part size is split into parallel ranged GETs for
// throughput. The split already happens today (the slicer cuts by byte range,
// not by stored part count). S3 stores ONE full-object checksum and returns NO
// checksum for any sub-range of a single-stored-part object, so the SDK cannot
// validate the split chunks. Closing this needs the transfer manager to compute
// a whole-object checksum over the delivered bytes itself (HeadObject for the
// expected value + an ordered hash at the slot-buffer consume point), or a wire
// checksum from S3 over arbitrary response bytes.

/// A tampered chunk of a split single-PUT download fails the download. S3 returns
/// no checksum for a sub-range of a single-stored-part object, so the SDK
/// validates nothing; the transfer manager's own whole-object hash catches it.
#[tokio::test]
async fn single_put_split_tamper_caught_mock_gp() {
    let t = Target::mock_gp().connect().await; // default part sizes; 12 MiB -> 3 ranged GETs
    let data = large_single_put();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
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

// caller-driven ranges: pinned part size, and explicit byte range (RUST-1173) ---
//
// Two request shapes defeat the stored-part alignment that makes multipart
// validation work. A pinned part size suppresses auto-alignment (it is gated on
// the size being Auto) and a pinned size that differs from the uploaded one cannot
// align. An explicit range is excluded from alignment outright. In both cases S3
// returns no checksum for the resulting sub-ranges, so the SDK validates nothing
// and the verdict must say so rather than claim a validation that did not happen.

/// A part size that deliberately does NOT divide the uploaded part size, so no
/// download range can land on a stored part boundary.
const MISALIGNED_DOWNLOAD_PART_SIZE: PartSize = PartSize::Target(5 * 1024 * 1024);
/// The size `multipart_data()` is uploaded at for these tests: 20 MiB in 8 MiB
/// parts, whose boundaries (8 MiB, 16 MiB) no multiple of 5 MiB reaches.
const UPLOAD_PART_SIZE: PartSize = PartSize::Target(8 * 1024 * 1024);

/// A caller-pinned part size that misaligns with the stored boundaries: no chunk
/// carries a checksum of its own, so the transfer hashes the delivered bytes and
/// folds them into the object's checksum — the download succeeds with the right
/// bytes and reports them as validated.
async fn misaligned_part_size_validates_delivered_bytes(target: Target) {
    let t = target.connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let downloader = t.tm_with_part_size(MISALIGNED_DOWNLOAD_PART_SIZE);
    let (bytes, output) = t
        .download_range_with(&downloader, "obj", Some(ChecksumMode::Enabled), None)
        .await
        .expect("download");

    assert_same_content(&data, &bytes);
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

#[tokio::test]
async fn misaligned_part_size_validates_delivered_bytes_mock_gp() {
    misaligned_part_size_validates_delivered_bytes(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn misaligned_part_size_validates_delivered_bytes_real_gp() {
    misaligned_part_size_validates_delivered_bytes(Target::real_gp()).await;
}

/// An explicit byte range: the requested bytes arrive intact, and the verdict
/// reports that nothing was validated. The range here sits strictly inside stored
/// part 1, so it cannot match a boundary even by accident.
async fn explicit_range_reports_not_validated(target: Target) {
    let t = target.connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let (bytes, output) = t
        .download_range_with(
            &t.tm_with_part_size(UPLOAD_PART_SIZE),
            "obj",
            Some(ChecksumMode::Enabled),
            Some("bytes=1048576-3145727"),
        )
        .await
        .expect("download");

    assert_eq!(bytes, data[1048576..=3145727], "requested range contents");
    // The reason names the request, not the object: this object does carry a
    // checksum, and the same bytes validate when asked for without a range.
    assert_not_validated(&output, NotValidatedReason::RangeNotCovered);

    t.shutdown().await;
}

#[tokio::test]
async fn explicit_range_reports_not_validated_mock_gp() {
    explicit_range_reports_not_validated(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn explicit_range_reports_not_validated_real_gp() {
    explicit_range_reports_not_validated(Target::real_gp()).await;
}

/// A range that happens to span the whole object is as validatable as no range at
/// all, and is validated.
///
/// The bytes are identical either way, so a verdict that depended on how the caller
/// spelled the request would be reporting on syntax rather than on the data. This is
/// what gating the fold on "does this transfer deliver the whole object" buys over
/// gating it on "did the request carry a range".
async fn whole_object_explicit_range_validates(target: Target) {
    let t = target.connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let last = data.len() - 1;
    let (bytes, output) = t
        .download_range_with(
            &t.tm_with_part_size(MISALIGNED_DOWNLOAD_PART_SIZE),
            "obj",
            Some(ChecksumMode::Enabled),
            Some(&format!("bytes=0-{last}")),
        )
        .await
        .expect("download");

    assert_same_content(&data, &bytes);
    assert_validated(&output, aws_sdk_s3::types::ChecksumAlgorithm::Crc32);

    t.shutdown().await;
}

#[tokio::test]
async fn whole_object_explicit_range_validates_mock_gp() {
    whole_object_explicit_range_validates(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn whole_object_explicit_range_validates_real_gp() {
    whole_object_explicit_range_validates(Target::real_gp()).await;
}

/// A suffix range of every byte but the first delivers the object's tail, which the
/// whole-object checksum does not describe -- so the download reports not-validated
/// and returns those bytes.
///
/// The byte count alone matches nothing here, but it comes within one byte of the
/// object's size, which is what makes this the shape that separates "the delivered
/// bytes are the object's" from "there are as many of them".
async fn tail_suffix_range_is_not_reported_corrupt(target: Target) {
    let t = target.connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
    .await;

    let tail = data.len() - 1;
    let (bytes, output) = t
        .download_range_with(
            &t.tm_with_part_size(MISALIGNED_DOWNLOAD_PART_SIZE),
            "obj",
            Some(ChecksumMode::Enabled),
            Some(&format!("bytes=-{tail}")),
        )
        .await
        .expect("a range the checksum does not cover is not an error");

    assert_same_content(&data[1..], &bytes);
    assert_not_validated(&output, NotValidatedReason::RangeNotCovered);

    t.shutdown().await;
}

#[tokio::test]
async fn tail_suffix_range_is_not_reported_corrupt_mock_gp() {
    tail_suffix_range_is_not_reported_corrupt(Target::mock_gp()).await;
}
#[cfg(e2e_test)]
#[tokio::test]
async fn tail_suffix_range_is_not_reported_corrupt_real_gp() {
    tail_suffix_range_is_not_reported_corrupt(Target::real_gp()).await;
}

/// A tampered chunk of a misaligned-part-size download fails the download. No
/// chunk carries a checksum of its own here, so the SDK validates nothing; the
/// transfer manager hashes the delivered bytes and compares against the object's
/// stored checksum, which is what catches this.
#[tokio::test]
async fn misaligned_part_size_tamper_caught_mock_gp() {
    let t = Target::mock_gp().connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let downloader = t.tm_with_part_size(MISALIGNED_DOWNLOAD_PART_SIZE);
    let result = t
        .download_range_with(&downloader, "obj", Some(ChecksumMode::Enabled), None)
        .await;
    assert_integrity_error(result);

    t.shutdown().await;
}

/// The same corruption on the same shape with `checksum_mode` left unset also
/// fails the download.
///
/// This is the case that carries most downloads: unset does not mean off, because
/// the SDK auto-enables validation whenever the client resolves
/// `ResponseChecksumValidation` to `WhenSupported`, the default. Gating the fold
/// on an explicit opt-in would leave exactly this path returning `Ok` over
/// corrupt bytes, which is the defect the ticket names.
#[tokio::test]
async fn misaligned_part_size_tamper_caught_default_mode_mock_gp() {
    let t = Target::mock_gp().connect_with(Some(UPLOAD_PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        &t.key("obj"),
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let downloader = t.tm_with_part_size(MISALIGNED_DOWNLOAD_PART_SIZE);
    let result = t.download_range_with(&downloader, "obj", None, None).await;
    assert_integrity_error(result);

    t.shutdown().await;
}

/// Corruption inside an explicit sub-range is NOT caught, and the verdict says so
/// rather than claiming the bytes were checked.
///
/// The limit is arithmetic, not an omission: the only value S3 holds covers the
/// whole object, and no part of it validates a strict subset of those bytes. So
/// the range path can be honest but not safe, and this test pins the honest half
/// -- it fails if anything ever reports `Validated` here.
#[tokio::test]
async fn explicit_range_tamper_not_caught_but_never_validated_mock_gp() {
    let t = Target::mock_gp().connect_with(Some(UPLOAD_PART_SIZE)).await;
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
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let (bytes, output) = t
        .download_range_with(
            &t.tm_with_part_size(UPLOAD_PART_SIZE),
            "obj",
            Some(ChecksumMode::Enabled),
            Some("bytes=1048576-3145727"),
        )
        .await
        .expect("a sub-range download cannot detect this, so it succeeds");

    assert_ne!(
        bytes,
        data[1048576..=3145727],
        "the fault must actually have corrupted the delivered bytes"
    );
    assert_not_validated(&output, NotValidatedReason::RangeNotCovered);

    t.shutdown().await;
}
