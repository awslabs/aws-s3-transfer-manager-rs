/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared assertions for the integration tests.
//!
//! Outcome assertions used across more than one test module live here so they
//! are written and maintained once. Module-specific assertions (e.g. ones tied
//! to a particular output type) stay with their module.

use aws_sdk_s3_transfer_manager::error::{Error, ErrorKind};
use aws_sdk_s3_transfer_manager::metrics::TransferMetrics;
use aws_sdk_s3_transfer_manager::operation::download::DownloadOutput;
use aws_sdk_s3_transfer_manager::types::{
    ChecksumAlgorithm, ChecksumValidation, NotValidatedReason,
};
use std::time::Duration;
use tokio::time::timeout;

/// Waits until two `metrics` snapshots taken 10 ms apart report the same
/// `elapsed`, and returns it.
///
/// `TransferMetrics::elapsed` stops advancing only once the transfer records
/// its finish, so a transfer that never records one fails after 30 s.
pub(crate) async fn wait_for_finished_elapsed(metrics: impl Fn() -> TransferMetrics) -> Duration {
    timeout(Duration::from_secs(30), async {
        loop {
            let first = metrics().elapsed;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if metrics().elapsed == first {
                return first;
            }
        }
    })
    .await
    .expect("elapsed kept advancing for 30s: the transfer did not record its finish")
}

/// Assert a download's verdict is `Validated` with `expected`.
pub(crate) fn assert_validated(output: &DownloadOutput, expected: ChecksumAlgorithm) {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::Validated { algorithm, .. } => assert_eq!(*algorithm, expected),
        other => panic!("expected Validated{{{expected:?}}}, got {other:?}"),
    }
}

/// Assert a download's verdict is `NotValidated` with `expected`.
pub(crate) fn assert_not_validated(output: &DownloadOutput, expected: NotValidatedReason) {
    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::NotValidated { reason, .. } => assert_eq!(*reason, expected),
        other => panic!("expected NotValidated{{{expected:?}}}, got {other:?}"),
    }
}

/// Assert a download failed because object integrity validation failed (a
/// checksum mismatch). Pins the [`ErrorKind::IntegrityError`] kind, not merely
/// `is_err()`, so a tamper test cannot pass on some unrelated failure.
pub(crate) fn assert_integrity_error<T>(result: Result<T, Error>) {
    match result {
        Err(e) if matches!(e.kind(), ErrorKind::IntegrityError(_)) => {}
        Err(e) => panic!("expected ErrorKind::IntegrityError, got {:?}", e.kind()),
        Ok(_) => panic!("expected the download to fail with IntegrityError, but it succeeded"),
    }
}

/// Assert a download failed with an integrity error and return it for field
/// inspection. Used by the backwards-compatibility test that pins the SDK
/// checksum-mismatch error shape (algorithm/expected/computed extraction).
pub(crate) fn expect_integrity_error<T: std::fmt::Debug>(
    result: Result<T, Error>,
) -> aws_sdk_s3_transfer_manager::error::IntegrityError {
    match result {
        Err(e) => match e.kind() {
            ErrorKind::IntegrityError(ie) => ie.clone(),
            other => panic!("expected ErrorKind::IntegrityError, got {other:?}"),
        },
        Ok(v) => panic!("expected an integrity error, but the download succeeded: {v:?}"),
    }
}
///
/// A body-stream failure (truncation, reset, short body) that exhausts retries
/// surfaces as [`ErrorKind::IOError`]. Pinning the kind distinguishes a transport
/// failure from an integrity failure (which must not be retried).
pub(crate) fn assert_io_error<T>(result: Result<T, Error>) {
    match result {
        Err(e) if matches!(e.kind(), ErrorKind::IOError) => {}
        Err(e) => panic!("expected ErrorKind::IOError, got {:?}", e.kind()),
        Ok(_) => panic!("expected the download to fail with IOError, but it succeeded"),
    }
}

/// Assert downloaded bytes match the source without dumping the buffers.
///
/// A failed `assert_eq!` on multi-megabyte buffers prints both in full; this
/// gates on byte equality and, on mismatch, reports lengths and the first
/// differing offset.
pub(crate) fn assert_same_content(expected: &[u8], actual: &[u8]) {
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

/// Whether `path` names a temporary file of a path download: the transfer manager writes to a
/// file whose name contains `.s3tmp.` and renames it to the destination on success.
pub(crate) fn is_download_temp_file(path: &std::path::Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().contains(".s3tmp."))
}

/// Checks that `error` is an integrity error in `algorithm` whose expected and computed values
/// are both set and differ. Returns the reason it isn't as `Err`.
pub(crate) fn check_integrity_error(
    error: &Error,
    algorithm: &ChecksumAlgorithm,
) -> Result<(), String> {
    let ErrorKind::IntegrityError(integrity) = error.kind() else {
        return Err(format!("expected an integrity error, got {error:?}"));
    };
    if integrity.algorithm() != Some(algorithm) {
        return Err(format!(
            "integrity error in {:?}, expected {algorithm:?}",
            integrity.algorithm()
        ));
    }
    if integrity.expected().is_none()
        || integrity.computed().is_none()
        || integrity.expected() == integrity.computed()
    {
        return Err(format!(
            "integrity error must carry different expected and computed values: {integrity:?}"
        ));
    }
    Ok(())
}

/// The code and kind of an S3 request error, without its message or `Debug` form.
///
/// An authorization failure's message names the caller's account and role, and tests that run
/// against real S3 print to public CI logs, so they describe a failed request with this instead.
pub(crate) fn describe_sdk_error<E, R>(error: &aws_sdk_s3::error::SdkError<E, R>) -> String
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_sdk_s3::error::ProvideErrorMetadata;
    match error.code() {
        Some(code) => format!("{error} ({code})"),
        None => format!("{error}"),
    }
}

/// The kind, operation and code of a transfer manager error, without its source, for the same
/// reason as [`describe_sdk_error`].
pub(crate) fn describe_error(error: &Error) -> String {
    format!(
        "{:?} from {:?} ({:?})",
        error.kind(),
        error.operation_name(),
        error.code()
    )
}
