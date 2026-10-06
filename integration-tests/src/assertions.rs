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
