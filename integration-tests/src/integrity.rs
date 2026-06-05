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
//! ## Current limitation
//!
//! The Rust SDK does not yet expose a per-response validation outcome, so the
//! transfer manager cannot currently confirm a positive result: the verdict is
//! `NotValidated` whenever validation is requested, and `Disabled` when it is
//! not. The integrity-critical negative guarantee (a tampered response fails the
//! download) holds today and is tested here. Positive-result assertions are
//! marked `TODO(vnext)` until the SDK reports the outcome.
//!
//! # Coverage
//!
//! | Object                        | checksum_mode | Asserts                                   |
//! |-------------------------------|---------------|-------------------------------------------|
//! | single-part                   | default       | round-trip; verdict Unavailable (on)      |
//! | single-part (WhenRequired)    | default       | round-trip; verdict Disabled (off)        |
//! | single-part                   | on            | round-trip; value shown; not Validated*   |
//! | multipart full-object         | on            | round-trip; object value not from a part  |
//! | multipart composite           | on            | round-trip; object value not from a part  |
//! | single-part tampered          | on            | download errors (never Ok)                |
//! | single-part tampered (file)   | on            | errors; temp cleaned, dest not created    |
//! | multipart tampered (aligned)  | on            | download errors (never Ok)                |
//! | multipart aligned             | on            | round-trip (download ranges align)        |
//! | multipart unaligned, tampered | on            | NOT caught (no checksum on unaligned GET) |
//!
//! *not Validated until the SDK reports a per-response validation outcome
//! (see the limitation above).
//!
//! Multipart download validation requires the download range to match a stored
//! part boundary. The TM downloads with ranged GETs, so a test pins an explicit
//! part size to align upload and download; the unaligned row pins the current
//! default behavior (upload 8 MiB, download 5 MiB) where validation does not
//! occur. See `harness::TmTestClient::connect_with`.
//!
//! Algorithm is a pass-through axis for the transfer manager (it forwards the
//! choice and reads back whatever S3 returns), so per-algorithm value
//! correctness is covered at the unit and mock-server layers, not multiplied
//! through this matrix. Representative algorithms are used: CRC32 for full
//! object, SHA-256 for the composite case.

use crate::harness::Target;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::operation::download::DownloadOutput;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::{ChecksumValidation, NotValidatedReason, PartSize};
use s3_mock_server::{FaultType, Occurrence};

/// A part size pinned equally for upload and download so multipart download
/// ranges align to the uploaded part boundaries (the precondition for per-part
/// download validation). The TM's default leaves upload (8 MiB) and download
/// (5 MiB) unaligned; see the unaligned boundary test.
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

fn assert_not_validated(output: &DownloadOutput, expected: NotValidatedReason) {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::NotValidated { reason } => assert_eq!(*reason, expected),
        other => panic!("expected NotValidated{{{expected:?}}}, got {other:?}"),
    }
}

/// Assert downloaded bytes match the source without dumping the buffers. A
/// failed `assert_eq!` on multi-megabyte buffers prints both in full; this gates
/// on real byte equality and, on mismatch, reports lengths and the first
/// differing offset.
fn assert_same_content(expected: &[u8], actual: &[u8]) {
    if expected == actual {
        return;
    }
    let first_diff = expected
        .iter()
        .zip(actual.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(expected.len().min(actual.len()));
    panic!(
        "content mismatch: expected len={}, got len={}, first diff at byte {}",
        expected.len(),
        actual.len(),
        first_diff
    );
}

// single-part, default checksum_mode (unset) -----------------------------------
//
// Leaving checksum_mode unset does NOT mean validation is off: the SDK's
// GetObject mutator auto-enables ChecksumMode when the client's
// ResponseChecksumValidation resolves to WhenSupported (the default). So a
// default download attempts validation; the verdict is Unavailable (not
// Disabled) until the SDK reports a confirmed outcome (TODO(vnext) -> Validated).

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
    // Default resolves to validation-attempted (SDK auto-enables ChecksumMode),
    // so the verdict is Unavailable, NOT Disabled.
    assert_not_validated(&output, NotValidatedReason::Unavailable);

    t.shutdown().await;
}

#[tokio::test]
async fn single_part_mode_default_mock_gp() {
    single_part_mode_default(Target::mock_gp()).await;
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
    // Whole object in one chunk: the object's checksum is surfaced.
    assert!(
        output.integrity_checks().checksum_crc32().is_some(),
        "whole-object checksum should be surfaced"
    );
    // TODO(vnext): assert Validated{Crc32} once the SDK reports the outcome.
    assert!(
        !matches!(
            output.integrity_checks().checksum_validation(),
            ChecksumValidation::Validated { .. }
        ),
        "must not report Validated without an SDK-confirmed outcome"
    );

    t.shutdown().await;
}

#[tokio::test]
async fn single_part_mode_on_mock_gp() {
    single_part_mode_on(Target::mock_gp()).await;
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

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_composite_mock_gp() {
    multipart_composite(Target::mock_gp()).await;
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
        "obj",
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert!(result.is_err(), "tampered body must fail the download");

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_single_part_errors_mock_gp() {
    tampered_single_part_errors(Target::mock_gp()).await;
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
        "obj",
        FaultType::CorruptBody,
        0,
        Occurrence::Always,
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let dest = dir.path().join("obj.bin");

    let result = t
        .download_to_path("obj", &dest, Some(ChecksumMode::Enabled))
        .await;
    assert!(result.is_err(), "tampered body must fail the file download");
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

async fn tampered_multipart_errors(target: Target) {
    // Align download ranges to the uploaded part boundaries so each chunk carries
    // a per-part checksum the SDK validates. Without alignment, S3 returns no
    // checksum for the ranges and tampering cannot be caught (see the unaligned
    // boundary test below).
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("tamper faults require the mock backend");
    // Fail every chunk for a deterministic transfer outcome.
    mock.insert_fault(
        t.bucket(),
        "obj",
        FaultType::WrongStoredChecksum,
        0,
        Occurrence::Always,
    );

    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert!(result.is_err(), "tampered checksum must fail the download");

    t.shutdown().await;
}

#[tokio::test]
async fn tampered_multipart_errors_mock_gp() {
    tampered_multipart_errors(Target::mock_gp()).await;
}

// part-size alignment boundary --------------------------------------------------
//
// S3 returns a per-part checksum for a ranged GET only when the range matches a
// stored part boundary (flexible-checksums SEP). The TM downloads with ranged
// GETs, so multipart download validation depends on the download part size
// matching the uploaded part size. These two tests pin both sides of that
// boundary so the behavior cannot drift silently.
//
// TODO(vnext): the TM should align download ranges to the stored part size by
// default (today upload defaults to 8 MiB, download to 5 MiB, so the default is
// unaligned and not validated). When that lands, the unaligned case below stops
// being the default; it remains reachable only with an explicit mismatched size.

/// Aligned (upload part size == download part size): a non-tampered multipart
/// download round-trips. Tampering on this path is caught (see
/// `tampered_multipart_errors`).
async fn multipart_aligned_round_trips(target: Target) {
    let t = target.connect_with(Some(ALIGNED_PART_SIZE)).await;
    let data = multipart_data();
    t.put(
        "obj",
        data.clone(),
        ChecksumStrategy::with_calculated_crc32(),
    )
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

/// Unaligned (TM default: upload 8 MiB, download 5 MiB): the download still
/// round-trips, but a tampered checksum is NOT caught, because S3 returns no
/// checksum for the unaligned ranges. This pins the silent no-validation default
/// so the alignment fix (TODO above) flips it deliberately.
async fn multipart_unaligned_tamper_not_caught(target: Target) {
    let t = target.connect().await; // default unaligned part sizes
    let data = multipart_data();
    t.put("obj", data, ChecksumStrategy::with_calculated_crc32())
        .await;

    let mock = t.mock().expect("requires the mock backend");
    mock.insert_fault(
        t.bucket(),
        "obj",
        FaultType::WrongStoredChecksum,
        0,
        Occurrence::Always,
    );

    // Unaligned ranges carry no checksum, so the tamper is invisible and the
    // download succeeds. This is the gap the alignment fix closes.
    let result = t.download("obj", Some(ChecksumMode::Enabled)).await;
    assert!(
        result.is_ok(),
        "unaligned multipart download is not validated, so it currently succeeds"
    );

    t.shutdown().await;
}

#[tokio::test]
async fn multipart_unaligned_tamper_not_caught_mock_gp() {
    multipart_unaligned_tamper_not_caught(Target::mock_gp()).await;
}
