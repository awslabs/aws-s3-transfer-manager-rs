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

/// Assert a download failed with a per-chunk failure.
///
/// A checksum mismatch and a transient body-read failure both surface as
/// [`ErrorKind::ChunkFailed`] (the SDK body validator or stream errors on a
/// chunk, which the transfer manager reports as `ChunkFailed`). Asserting the
/// kind, not merely `is_err()`, pins that the failure is the expected per-chunk
/// path and not some unrelated error.
pub(crate) fn assert_chunk_failed<T>(result: Result<T, Error>) {
    match result {
        Err(e) if matches!(e.kind(), ErrorKind::ChunkFailed(_)) => {}
        Err(e) => panic!("expected ErrorKind::ChunkFailed, got {:?}", e.kind()),
        Ok(_) => panic!("expected the download to fail with ChunkFailed, but it succeeded"),
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
