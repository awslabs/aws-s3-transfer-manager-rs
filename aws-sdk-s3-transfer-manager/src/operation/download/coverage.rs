/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-chunk validation coverage, aggregated into one verdict for the transfer.
//!
//! A download's bytes arrive as many ranged GETs. The SDK validates a response
//! body whenever S3 returns a checksum covering exactly the bytes it returned, and
//! a mismatch fails the body stream -- so a chunk that carried such a checksum and
//! arrived without error was validated. Nothing in the response states that
//! outcome directly; the transfer records what the headers imply, per chunk, and
//! resolves the verdict when the last chunk lands.
//!
//! Aggregation is the part a per-response outcome could not replace. "Every
//! delivered byte was covered" is a property of the whole transfer, and one
//! uncovered chunk in the middle of a large download is exactly the case a
//! per-chunk answer cannot express.

use aws_sdk_s3::types::ChecksumAlgorithm;

use super::chunk_meta::ChunkMetadata;
use super::object_crc::{is_composite_value, ChunkCrc};
use crate::types::{ChecksumValidation, NotValidatedReason};

/// What one completed chunk contributes to the transfer's verdict.
///
/// One value per chunk that actually landed, recorded in a single lock
/// acquisition, so the two facts it carries cannot be accounted at different
/// moments: they describe the same bytes.
#[derive(Debug, Clone)]
pub(crate) struct ChunkReport {
    /// The chunk's CRC and extent, when the transfer is hashing bytes itself.
    /// `None` when it is not -- nothing to fold.
    pub(crate) crc: Option<ChunkCrc>,
    /// What the chunk's own response headers implied about these bytes.
    pub(crate) coverage: ChunkCoverage,
}

/// What one chunk's response headers imply about the bytes it delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChunkCoverage {
    /// A checksum covering exactly these bytes was present, so the SDK compared
    /// the body against it -- and the chunk arrived, so the comparison passed.
    Validated(ChecksumAlgorithm),
    /// Only a composite (`-N`) value was present. That is a checksum of part
    /// checksums, which the SDK refuses to validate a body against, so these
    /// bytes were not checked by anything.
    Composite,
    /// No checksum at all: the range did not line up with anything S3 stores a
    /// checksum for, or the object has none.
    Uncovered,
}

/// Classify one chunk response.
///
/// S3 stores one checksum per object, so at most one of these is present in
/// practice; the order matters only for a response that somehow carried several,
/// where the cheapest to verify wins -- the same preference the SDK applies.
pub(crate) fn classify(meta: &ChunkMetadata) -> ChunkCoverage {
    let candidates = [
        (
            ChecksumAlgorithm::Crc64Nvme,
            meta.checksum_crc64_nvme.as_deref(),
        ),
        (ChecksumAlgorithm::Crc32, meta.checksum_crc32.as_deref()),
        (ChecksumAlgorithm::Crc32C, meta.checksum_crc32_c.as_deref()),
        (ChecksumAlgorithm::Sha256, meta.checksum_sha256.as_deref()),
        (ChecksumAlgorithm::Sha1, meta.checksum_sha1.as_deref()),
    ];
    let mut saw_composite = false;
    for (algorithm, value) in candidates {
        match value {
            // A composite value does not disqualify a byte-covering one that may
            // follow, so keep looking rather than returning here.
            Some(v) if is_composite_value(v) => saw_composite = true,
            Some(_) => return ChunkCoverage::Validated(algorithm),
            None => {}
        }
    }
    if saw_composite {
        ChunkCoverage::Composite
    } else {
        ChunkCoverage::Uncovered
    }
}

/// Coverage folded across the chunks a transfer delivered.
///
/// Plain (non-atomic) state, guarded by the transfer's `state` mutex and recorded
/// in the same lock acquisition that decrements `ranges_in_flight` -- the same
/// argument as `object_crc` beside it in
/// [`DownloadState::Transferring`](super::context::DownloadState). An atomic
/// counter would not do: "every chunk was counted before the completing thread
/// read the total" is a claim about ordering between two distinct events, and only
/// the lock that produces both can make it.
#[derive(Debug, Default)]
pub(crate) struct CoverageTally {
    /// Chunks that completed. Every delivered byte belongs to exactly one.
    chunks: u64,
    /// Of those, how many carried a checksum covering their own bytes.
    validated: u64,
    /// How many carried only a composite value.
    composite: u64,
    /// The algorithm the validated chunks used. First one wins; they cannot
    /// disagree, because an object has one stored checksum.
    algorithm: Option<ChecksumAlgorithm>,
}

impl CoverageTally {
    pub(crate) fn record(&mut self, coverage: ChunkCoverage) {
        self.chunks += 1;
        match coverage {
            ChunkCoverage::Validated(algorithm) => {
                self.validated += 1;
                self.algorithm.get_or_insert(algorithm);
            }
            ChunkCoverage::Composite => self.composite += 1,
            ChunkCoverage::Uncovered => {}
        }
    }

    /// The verdict these chunks support.
    ///
    /// `fold` is the algorithm of the transfer's own whole-object hash, present
    /// only when that hash covered every delivered byte and matched. It outranks
    /// the tally because it is evidence about the bytes themselves rather than an
    /// inference from headers -- and it is the only evidence available on the paths
    /// where no chunk carries a checksum.
    ///
    /// `PartialCoverage` precedes the composite check: a transfer with some
    /// validated chunks and some composite ones is more precisely described as
    /// partially covered than as a composite object.
    pub(crate) fn resolve(&self, fold: Option<ChecksumAlgorithm>) -> ChecksumValidation {
        if let Some(algorithm) = fold {
            return ChecksumValidation::Validated { algorithm };
        }
        if self.chunks > 0 && self.validated == self.chunks {
            if let Some(algorithm) = self.algorithm.clone() {
                return ChecksumValidation::Validated { algorithm };
            }
        }
        let reason = if self.validated > 0 {
            NotValidatedReason::PartialCoverage
        } else if self.composite > 0 {
            NotValidatedReason::CompositeChecksum
        } else {
            NotValidatedReason::Unavailable
        };
        ChecksumValidation::NotValidated { reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta_with(field: &str, value: &str) -> ChunkMetadata {
        let mut m = ChunkMetadata::default();
        match field {
            "crc32" => m.checksum_crc32 = Some(value.to_string()),
            "crc32c" => m.checksum_crc32_c = Some(value.to_string()),
            "crc64" => m.checksum_crc64_nvme = Some(value.to_string()),
            "sha256" => m.checksum_sha256 = Some(value.to_string()),
            other => panic!("unknown field {other}"),
        }
        m
    }

    #[test]
    fn plain_value_is_validated_composite_is_not() {
        assert_eq!(
            classify(&meta_with("crc32", "DUoRhQ==")),
            ChunkCoverage::Validated(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(
            classify(&meta_with("crc32", "DUoRhQ==-14")),
            ChunkCoverage::Composite
        );
        assert_eq!(
            classify(&ChunkMetadata::default()),
            ChunkCoverage::Uncovered
        );
    }

    #[test]
    fn each_algorithm_is_reported_as_itself() {
        assert_eq!(
            classify(&meta_with("crc64", "AAAAAAAAAAA=")),
            ChunkCoverage::Validated(ChecksumAlgorithm::Crc64Nvme)
        );
        assert_eq!(
            classify(&meta_with("crc32c", "DUoRhQ==")),
            ChunkCoverage::Validated(ChecksumAlgorithm::Crc32C)
        );
        assert_eq!(
            classify(&meta_with("sha256", "n4bQ...")),
            ChunkCoverage::Validated(ChecksumAlgorithm::Sha256)
        );
    }

    /// Every chunk covered -> Validated, naming the algorithm the chunks used.
    #[test]
    fn all_chunks_covered_is_validated() {
        let mut t = CoverageTally::default();
        for _ in 0..4 {
            t.record(ChunkCoverage::Validated(ChecksumAlgorithm::Crc32));
        }
        assert_eq!(
            t.resolve(None),
            ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc32
            }
        );
    }

    /// The case a per-response outcome cannot express: one uncovered chunk in an
    /// otherwise covered transfer must not read as validated.
    #[test]
    fn one_uncovered_chunk_downgrades_the_whole_transfer() {
        let mut t = CoverageTally::default();
        t.record(ChunkCoverage::Validated(ChecksumAlgorithm::Crc32));
        t.record(ChunkCoverage::Uncovered);
        t.record(ChunkCoverage::Validated(ChecksumAlgorithm::Crc32));
        assert_eq!(
            t.resolve(None),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::PartialCoverage
            }
        );
    }

    #[test]
    fn all_composite_is_reported_as_composite() {
        let mut t = CoverageTally::default();
        t.record(ChunkCoverage::Composite);
        t.record(ChunkCoverage::Composite);
        assert_eq!(
            t.resolve(None),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::CompositeChecksum
            }
        );
    }

    #[test]
    fn nothing_covered_is_unavailable() {
        let mut t = CoverageTally::default();
        t.record(ChunkCoverage::Uncovered);
        assert_eq!(
            t.resolve(None),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Unavailable
            }
        );
    }

    /// No chunks at all yields no claim either way.
    #[test]
    fn empty_tally_is_unavailable() {
        assert_eq!(
            CoverageTally::default().resolve(None),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Unavailable
            }
        );
    }

    /// The fold outranks the tally: it is what validates the paths where no chunk
    /// carries a checksum of its own.
    #[test]
    fn a_matched_fold_validates_uncovered_chunks() {
        let mut t = CoverageTally::default();
        t.record(ChunkCoverage::Uncovered);
        t.record(ChunkCoverage::Uncovered);
        assert_eq!(
            t.resolve(Some(ChecksumAlgorithm::Crc64Nvme)),
            ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc64Nvme
            }
        );
    }
}
