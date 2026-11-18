/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! CRC evidence over immutable bytes and its combination into whole-object coverage.
//!
//! [`ComputedChecksum`] keeps numeric CRC values private; [`CrcFold`] accepts
//! intervals in any completion order and combines only adjacent object bytes.
//! Retained intervals are disjoint and nonadjacent. Invalid evidence clears them
//! and latches a failure that subsequent records cannot repair.

mod combine;

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};

use bytes::Buf;

use super::stored::ExpectedValue;
use super::CompletedRange;
use crate::error::{self, Error, ErrorKind};
use crate::runtime::buffer_pool::SegmentedBytes;

/// CRC algorithms supported by independent range calculation and combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::operation::download) enum CrcAlgorithm {
    /// CRC-32/ISO-HDLC, encoded as four big-endian bytes.
    Crc32,
    /// CRC-32/ISCSI (CRC32C), encoded as four big-endian bytes.
    Crc32c,
    /// CRC-64/NVME, encoded as eight big-endian bytes.
    Crc64Nvme,
}

impl CrcAlgorithm {
    /// Returns the decoded S3 checksum width in bytes.
    pub(super) fn width(self) -> usize {
        match self {
            Self::Crc32 | Self::Crc32c => 4,
            Self::Crc64Nvme => 8,
        }
    }

    /// Returns the algorithm reported in transfer verdicts and integrity errors.
    pub(super) fn model(self) -> crate::model::ChecksumAlgorithm {
        match self {
            Self::Crc32 => crate::model::ChecksumAlgorithm::Crc32,
            Self::Crc32c => crate::model::ChecksumAlgorithm::Crc32C,
            Self::Crc64Nvme => crate::model::ChecksumAlgorithm::Crc64Nvme,
        }
    }

    /// Returns the SDK algorithm with the same CRC parameters and byte encoding.
    fn sdk(self) -> aws_smithy_checksums::ChecksumAlgorithm {
        match self {
            Self::Crc32 => aws_smithy_checksums::ChecksumAlgorithm::Crc32,
            Self::Crc32c => aws_smithy_checksums::ChecksumAlgorithm::Crc32c,
            Self::Crc64Nvme => aws_smithy_checksums::ChecksumAlgorithm::Crc64Nvme,
        }
    }

    /// Returns the CRC of the left bytes followed by `right_len` right bytes.
    ///
    /// Both inputs are finalized CRCs under this algorithm; `right_len` is a byte
    /// count, not an absolute object offset. Inputs need no matching range size.
    /// The SDK's checksum API exposes hashing but no finalized-CRC combination;
    /// [`combine`] costs one multiplication modulo the polynomial per nonzero
    /// hexadecimal digit of `right_len`, at most 16, and is called with the transfer's
    /// state lock held.
    fn combine(self, left: u64, right: u64, right_len: u64) -> u64 {
        let combiner = match self {
            Self::Crc32 => &combine::CRC32,
            Self::Crc32c => &combine::CRC32C,
            Self::Crc64Nvme => &combine::CRC64_NVME,
        };
        combiner.combine(left, right, right_len)
    }

    /// Encodes a finalized CRC as big-endian bytes, matching the SDK's CRC finalizers.
    fn bytes(self, value: u64) -> Vec<u8> {
        value.to_be_bytes()[8 - self.width()..].to_vec()
    }
}

/// A finalized CRC of captured bytes, tagged with the parameters used to calculate it.
///
/// The numeric value is private; its byte coverage is supplied by the accompanying
/// [`CompletedRange`] when the evidence is recorded.
#[derive(Debug)]
pub(in crate::operation::download) struct ComputedChecksum {
    /// Parameters used for hashing and required for combination.
    algorithm: CrcAlgorithm,
    /// Finalized CRC; four-byte algorithms occupy the low 32 bits.
    value: u64,
}

/// Hashes the buffer's remaining bytes without advancing its cursor or copying payloads.
pub(super) fn digest_segments(algorithm: CrcAlgorithm, bytes: &SegmentedBytes) -> ComputedChecksum {
    let mut digest = algorithm.sdk().into_impl();
    let mut cursor = bytes.clone();
    while cursor.has_remaining() {
        let chunk = cursor.chunk();
        digest.update(chunk);
        let len = chunk.len();
        cursor.advance(len);
    }
    let value = digest
        .finalize()
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte));
    ComputedChecksum { algorithm, value }
}

/// CRC coverage of `[0, object_len)`, accepting nonoverlapping ranges in any order.
///
/// Adjacent intervals merge in object-offset order, leaving one entry per contiguous
/// accepted run. Invalid evidence latches the first failure and releases all entries.
/// An empty object is verified against the algorithm's empty-input checksum.
#[derive(Debug)]
pub(super) struct CrcFold {
    /// Algorithm required for all range CRCs and the stored checksum comparison.
    algorithm: CrcAlgorithm,
    /// Complete source-object length in bytes and upper bound for accepted intervals.
    object_len: u64,
    /// Disjoint, nonadjacent intervals mapped to the CRC over each entire interval.
    completed_ranges: BTreeMap<CompletedRange, u64>,
    /// First evidence error; a failed fold retains no intervals and ignores records.
    failure: Option<&'static str>,
}

impl CrcFold {
    /// Creates empty CRC coverage of `object_len` bytes under `algorithm`.
    pub(super) fn new(algorithm: CrcAlgorithm, object_len: u64) -> Self {
        Self {
            algorithm,
            object_len,
            completed_ranges: BTreeMap::new(),
            failure: None,
        }
    }

    /// Latches the first failure, clears retained intervals and makes later records no-ops.
    pub(super) fn fail(&mut self, message: &'static str) {
        if self.failure.is_none() {
            self.failure = Some(message);
            self.completed_ranges.clear();
        }
    }

    /// Records the CRC over exactly `range`, merging its adjacent object intervals.
    ///
    /// Empty, reversed, out-of-bounds, overlapping or wrong-algorithm evidence latches
    /// a failure and clears the map. Duplicate bounds count as overlap. The failure
    /// is returned by [`Self::finish`]; subsequent records cannot repair it.
    ///
    /// Tree operations cost O(log m), with at most two CRC combinations costing
    /// O(log L) each, where m is the retained interval count and L the right-hand
    /// byte length. Lengths are `u64`, so combination examines at most 64 bits.
    /// Clearing the map on failure costs O(m). No payload bytes are retained or read.
    pub(super) fn record(&mut self, range: CompletedRange, checksum: ComputedChecksum) {
        if self.failure.is_some() {
            return;
        }
        if checksum.algorithm != self.algorithm {
            return self.fail("range CRC algorithm disagrees with the validation plan");
        }
        if range.start >= range.end || range.end > self.object_len {
            return self.fail("range CRC has invalid object bounds");
        }
        let bound = CompletedRange {
            start: range.start,
            end: u64::MAX,
        };
        let previous = self
            .completed_ranges
            .range(..=bound)
            .next_back()
            .map(|(r, v)| (*r, *v));
        let next = self
            .completed_ranges
            .range((Excluded(bound), Unbounded))
            .next()
            .map(|(r, v)| (*r, *v));
        if previous.is_some_and(|(previous, _)| previous.end > range.start)
            || next.is_some_and(|(next, _)| next.start < range.end)
        {
            return self.fail("range CRC duplicates or overlaps accepted bytes");
        }

        let mut merged = range;
        let mut value = checksum.value;
        if let Some((previous, previous_crc)) = previous.filter(|(r, _)| r.end == range.start) {
            value = self
                .algorithm
                .combine(previous_crc, value, merged.end - merged.start);
            merged.start = previous.start;
            self.completed_ranges.remove(&previous);
        }
        if let Some((next, next_crc)) = next.filter(|(r, _)| r.start == range.end) {
            value = self
                .algorithm
                .combine(value, next_crc, next.end - next.start);
            merged.end = next.end;
            self.completed_ranges.remove(&next);
        }
        self.completed_ranges.insert(merged, value);
    }

    /// Consumes the fold, requiring exact object coverage and the stored checksum match.
    ///
    /// `expected` must be the stored value for this fold's algorithm. An empty object
    /// requires no recorded intervals and uses the empty-input CRC.
    ///
    /// Returns [`ErrorKind::RuntimeError`] for a latched evidence error or incomplete
    /// coverage. A mismatch returns [`ErrorKind::IntegrityError`] with the algorithm
    /// and the expected and computed values encoded as S3 checksum headers.
    pub(super) fn finish(self, expected: &ExpectedValue) -> Result<(), Error> {
        if let Some(message) = self.failure {
            return Err(Error::new(ErrorKind::RuntimeError, message));
        }
        let value = if self.object_len == 0 && self.completed_ranges.is_empty() {
            let value = self.algorithm.sdk().into_impl().finalize();
            value
                .iter()
                .fold(0u64, |value, byte| (value << 8) | u64::from(*byte))
        } else {
            let range = CompletedRange {
                start: 0,
                end: self.object_len,
            };
            match (
                self.completed_ranges.len(),
                self.completed_ranges.get(&range),
            ) {
                (1, Some(value)) => *value,
                _ => {
                    return Err(Error::new(
                        ErrorKind::RuntimeError,
                        "range CRCs do not cover the complete object",
                    ))
                }
            }
        };
        let computed = self.algorithm.bytes(value);
        if computed != expected.decoded {
            return Err(error::integrity_mismatch(
                self.algorithm.model(),
                expected.encoded.clone(),
                aws_smithy_types::base64::encode(computed),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALGORITHMS: [CrcAlgorithm; 3] = [
        CrcAlgorithm::Crc32,
        CrcAlgorithm::Crc32c,
        CrcAlgorithm::Crc64Nvme,
    ];

    fn digest(algorithm: CrcAlgorithm, bytes: &[u8]) -> ComputedChecksum {
        digest_segments(
            algorithm,
            &SegmentedBytes::from(bytes::Bytes::copy_from_slice(bytes)),
        )
    }

    fn expected(algorithm: CrcAlgorithm, bytes: &[u8]) -> ExpectedValue {
        // The oracle hashes the complete object directly through the SDK.
        let mut checksum = algorithm.sdk().into_impl();
        checksum.update(bytes);
        let decoded = checksum.finalize().to_vec();
        ExpectedValue {
            encoded: aws_smithy_types::base64::encode(&decoded),
            decoded,
        }
    }

    fn permutations(order: &mut [usize], start: usize, visit: &mut impl FnMut(&[usize])) {
        if start == order.len() {
            visit(order);
            return;
        }
        for index in start..order.len() {
            order.swap(start, index);
            permutations(order, start + 1, visit);
            order.swap(start, index);
        }
    }

    #[test]
    fn every_order_of_unequal_intervals_matches_direct_sdk_hashing() {
        let bytes: Vec<_> = (0..113).map(|i| ((i * 37 + 11) % 251) as u8).collect();
        // Three intervals cover left, right and bridging merges under interpretation.
        let boundaries: &[usize] = if cfg!(miri) {
            &[0, 1, 9, 113]
        } else {
            &[0, 1, 9, 24, 53, 87, 113]
        };
        let mut order: Vec<_> = (0..boundaries.len() - 1).collect();
        for algorithm in ALGORITHMS {
            let expected = expected(algorithm, &bytes);
            permutations(&mut order, 0, &mut |order| {
                let mut fold = CrcFold::new(algorithm, bytes.len() as u64);
                for (completed, &part) in order.iter().enumerate() {
                    let (start, end) = (boundaries[part], boundaries[part + 1]);
                    fold.record(
                        CompletedRange {
                            start: start as u64,
                            end: end as u64,
                        },
                        digest(algorithm, &bytes[start..end]),
                    );
                    assert!(fold.completed_ranges.len() <= order.len() - completed);
                }
                fold.finish(&expected).unwrap();
            });
        }
    }

    #[test]
    fn segmented_hashing_preserves_the_original_cursor() {
        let mut bytes = SegmentedBytes::from(bytes::Bytes::from_static(b"prefix"));
        bytes.append(SegmentedBytes::from(bytes::Bytes::from_static(b"payload")));
        bytes.advance(2);
        for algorithm in ALGORITHMS {
            let calculated = digest_segments(algorithm, &bytes);
            assert_eq!(
                algorithm.bytes(calculated.value),
                expected(algorithm, b"efixpayload").decoded
            );
            assert_eq!(bytes.len(), 11);
            assert_eq!(bytes.clone().into_contiguous().as_ref(), b"efixpayload");
        }
    }

    #[test]
    fn invalid_records_latch_failure_and_clear_retained_intervals() {
        for algorithm in ALGORITHMS {
            for range in [
                CompletedRange { start: 0, end: 4 },
                CompletedRange { start: 0, end: 2 },
                CompletedRange { start: 2, end: 6 },
                CompletedRange { start: 4, end: 4 },
                CompletedRange { start: 9, end: 8 },
                CompletedRange { start: 4, end: 9 },
            ] {
                let mut fold = CrcFold::new(algorithm, 8);
                fold.record(
                    CompletedRange { start: 0, end: 4 },
                    digest(algorithm, b"abcd"),
                );
                fold.record(range, digest(algorithm, b"wrong"));
                fold.record(
                    CompletedRange { start: 4, end: 8 },
                    digest(algorithm, b"efgh"),
                );
                assert!(fold.completed_ranges.is_empty());
                assert_eq!(
                    fold.finish(&expected(algorithm, b"abcdefgh"))
                        .unwrap_err()
                        .kind(),
                    &ErrorKind::RuntimeError
                );
            }
        }
    }

    #[test]
    fn overlaps_with_the_successor_are_rejected_before_merging() {
        for algorithm in ALGORITHMS {
            let mut fold = CrcFold::new(algorithm, 8);
            fold.record(
                CompletedRange { start: 4, end: 8 },
                digest(algorithm, b"efgh"),
            );
            fold.record(
                CompletedRange { start: 2, end: 6 },
                digest(algorithm, b"cdef"),
            );
            assert!(fold.completed_ranges.is_empty());
            assert_eq!(
                fold.finish(&expected(algorithm, b"abcdefgh"))
                    .unwrap_err()
                    .kind(),
                &ErrorKind::RuntimeError
            );
        }
    }

    #[test]
    fn missing_coverage_and_mismatched_algorithms_fail() {
        for algorithm in ALGORITHMS {
            let mut fold = CrcFold::new(algorithm, 8);
            fold.record(
                CompletedRange { start: 1, end: 8 },
                digest(algorithm, b"bcdefgh"),
            );
            assert_eq!(
                fold.finish(&expected(algorithm, b"abcdefgh"))
                    .unwrap_err()
                    .kind(),
                &ErrorKind::RuntimeError
            );
        }
        let mut fold = CrcFold::new(CrcAlgorithm::Crc32, 4);
        fold.record(
            CompletedRange { start: 0, end: 4 },
            digest(CrcAlgorithm::Crc32c, b"abcd"),
        );
        assert_eq!(
            fold.finish(&expected(CrcAlgorithm::Crc32, b"abcd"))
                .unwrap_err()
                .kind(),
            &ErrorKind::RuntimeError
        );
    }

    #[test]
    fn mismatch_reports_algorithm_and_encoded_values() {
        for algorithm in ALGORITHMS {
            let stored = expected(algorithm, b"stored");
            let mut fold = CrcFold::new(algorithm, 6);
            fold.record(
                CompletedRange { start: 0, end: 6 },
                digest(algorithm, b"broken"),
            );
            let error = fold.finish(&stored).unwrap_err();
            let ErrorKind::IntegrityError(detail) = error.kind() else {
                panic!("{error:?}")
            };
            assert_eq!(detail.algorithm(), Some(&algorithm.model()));
            assert_eq!(detail.expected(), Some(stored.encoded.as_str()));
            assert_eq!(
                detail.computed(),
                Some(expected(algorithm, b"broken").encoded.as_str())
            );
        }
    }

    #[test]
    fn empty_objects_use_the_sdk_empty_digest() {
        for algorithm in ALGORITHMS {
            CrcFold::new(algorithm, 0)
                .finish(&expected(algorithm, b""))
                .unwrap();
            assert!(CrcFold::new(algorithm, 0)
                .finish(&expected(algorithm, b"wrong"))
                .is_err());
        }
    }

    #[test]
    fn holding_the_first_range_across_many_waves_retains_one_later_interval() {
        let range_count = if cfg!(miri) { 16 } else { 10_000 };
        for algorithm in ALGORITHMS {
            let bytes: Vec<_> = (0..range_count).map(|i| (i % 251) as u8).collect();
            let mut fold = CrcFold::new(algorithm, bytes.len() as u64);
            for start in 1..bytes.len() {
                fold.record(
                    CompletedRange {
                        start: start as u64,
                        end: start as u64 + 1,
                    },
                    digest(algorithm, &bytes[start..start + 1]),
                );
                assert_eq!(fold.completed_ranges.len(), 1);
            }
            fold.record(
                CompletedRange { start: 0, end: 1 },
                digest(algorithm, &bytes[..1]),
            );
            fold.finish(&expected(algorithm, &bytes)).unwrap();
        }
    }
}
