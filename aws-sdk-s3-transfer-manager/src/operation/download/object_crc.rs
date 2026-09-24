/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Whole-object checksum computed from a download's own chunks.
//!
//! S3 returns a per-range checksum only when the requested range matches a stored
//! part boundary, so a download whose ranges are driven by the caller (a pinned
//! part size that does not divide the stored layout) gets nothing to validate
//! against. The transfer manager can still validate those bytes: CRCs combine, so
//! each chunk is hashed independently as it arrives and the results are folded
//! into the object's checksum, which S3 reports for the whole object.
//!
//! Combining is what makes this work without an ordered delivery point. Chunks
//! complete out of order on many threads; only the *fold* needs offset order, and
//! that happens here rather than in the data path.
//!
//! This applies only to algorithms that combine — every CRC, which is every
//! algorithm S3 permits for a full-object checksum ([`ChecksumType::FullObject`]
//! forbids SHA on a multipart upload). A composite (`<base64>-<N>`) value is a
//! checksum of part checksums, not of bytes, so no byte hash can reproduce it and
//! this type is never constructed for one.
//!
//! [`ChecksumType::FullObject`]: aws_sdk_s3::types::ChecksumType

use crc_fast::CrcAlgorithm;
use std::collections::BTreeMap;

/// Whether a checksum value is a composite one, which carries a `-<part count>`
/// suffix (e.g. `aB3..==-14`).
///
/// This mirrors the SDK's own `is_part_level_checksum`, which is what actually
/// decides whether the SDK validates a response body: trailing ASCII digits
/// preceded by exactly one `-`. Matching it exactly is the point -- a value the
/// SDK skips must never be treated here as one it checked. Two dashes are not a
/// part suffix, the same rejection the SDK makes.
pub(crate) fn is_composite_value(value: &str) -> bool {
    let mut digits = false;
    let mut dash = false;
    for ch in value.chars().rev() {
        if ch.is_ascii_digit() && !dash {
            digits = true;
            continue;
        }
        if digits && ch == '-' {
            if dash {
                return false;
            }
            dash = true;
            continue;
        }
        break;
    }
    digits && dash
}

/// One chunk's contribution: its CRC and the byte length it covers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkCrc {
    /// The chunk's offset within the object.
    pub(crate) offset: u64,
    /// CRC over exactly this chunk's delivered bytes.
    pub(crate) crc: u64,
    /// Number of bytes the CRC covers. Required by the combine step, which needs
    /// the right-hand operand's length.
    pub(crate) len: u64,
}

/// Which S3 checksum algorithm a stored value belongs to, and how to compute it.
///
/// Only the combinable (CRC) algorithms appear here. `crc32` is CRC-32/ISO-HDLC
/// and `crc32c` is CRC-32/ISCSI (Castagnoli) -- the names differ between S3 and
/// the CRC catalogue for the same polynomial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectCrcAlgorithm {
    Crc32,
    Crc32C,
    Crc64Nvme,
}

impl ObjectCrcAlgorithm {
    fn crc_algorithm(self) -> CrcAlgorithm {
        match self {
            ObjectCrcAlgorithm::Crc32 => CrcAlgorithm::Crc32IsoHdlc,
            ObjectCrcAlgorithm::Crc32C => CrcAlgorithm::Crc32Iscsi,
            ObjectCrcAlgorithm::Crc64Nvme => CrcAlgorithm::Crc64Nvme,
        }
    }

    /// The SDK algorithm this reports as, for the download's verdict.
    pub(crate) fn sdk_algorithm(self) -> aws_sdk_s3::types::ChecksumAlgorithm {
        match self {
            ObjectCrcAlgorithm::Crc32 => aws_sdk_s3::types::ChecksumAlgorithm::Crc32,
            ObjectCrcAlgorithm::Crc32C => aws_sdk_s3::types::ChecksumAlgorithm::Crc32C,
            ObjectCrcAlgorithm::Crc64Nvme => aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme,
        }
    }

    /// Width of the value in bytes, which fixes how it is base64-encoded.
    fn width(self) -> usize {
        match self {
            ObjectCrcAlgorithm::Crc32 | ObjectCrcAlgorithm::Crc32C => 4,
            ObjectCrcAlgorithm::Crc64Nvme => 8,
        }
    }

    /// A new incremental digest for one chunk.
    pub(crate) fn digest(self) -> crc_fast::Digest {
        crc_fast::Digest::new(self.crc_algorithm())
    }

    /// Encode a computed value the way S3 reports it: base64 of the big-endian
    /// bytes, truncated to the algorithm's width.
    fn encode(self, value: u64) -> String {
        let be = value.to_be_bytes();
        aws_smithy_types::base64::encode(&be[be.len() - self.width()..])
    }
}

/// Folds per-chunk CRCs into the object's checksum as chunks complete.
///
/// Folding needs offset order, but chunks arrive in any order, so a chunk that
/// arrives ahead of the fold point waits in `pending`. Memory is bounded by the
/// read-ahead window rather than the object size: the occupancy gate admits at
/// most `window` chunks concurrently, so at most that many can be waiting.
#[derive(Debug)]
pub(crate) struct ObjectCrc {
    algorithm: ObjectCrcAlgorithm,
    /// What S3 reports for the whole object, base64 as it appears on the wire.
    expected: String,
    /// CRC over the contiguous prefix `[0, folded_to)`. `None` before the first
    /// chunk at offset 0 arrives.
    folded: Option<u64>,
    /// End of the contiguous prefix folded so far.
    folded_to: u64,
    /// Chunks past the fold point, keyed by offset.
    pending: BTreeMap<u64, ChunkCrc>,
}

impl ObjectCrc {
    pub(crate) fn new(algorithm: ObjectCrcAlgorithm, expected: String) -> Self {
        Self {
            algorithm,
            expected,
            folded: None,
            folded_to: 0,
            pending: BTreeMap::new(),
        }
    }

    pub(crate) fn algorithm(&self) -> ObjectCrcAlgorithm {
        self.algorithm
    }

    /// Record a completed chunk, then fold every chunk now contiguous with the
    /// prefix. A duplicate offset is ignored: the first record for an offset wins,
    /// so a chunk cannot be folded twice.
    pub(crate) fn record(&mut self, chunk: ChunkCrc) {
        if chunk.offset < self.folded_to || self.pending.contains_key(&chunk.offset) {
            return;
        }
        self.pending.insert(chunk.offset, chunk);
        while let Some(next) = self.pending.remove(&self.folded_to) {
            self.folded = Some(match self.folded {
                None => next.crc,
                Some(acc) => crc_fast::checksum_combine(
                    self.algorithm.crc_algorithm(),
                    acc,
                    next.crc,
                    next.len,
                ),
            });
            self.folded_to += next.len;
        }
    }

    /// The object's computed checksum, or `None` if the chunks did not cover a
    /// contiguous `[0, total)`.
    ///
    /// `total` is the object's length. Requiring it -- rather than trusting that
    /// the last fold ended at the end -- is what rules out a verdict over a
    /// prefix: a transfer that delivered only the first half folds cleanly and
    /// would otherwise compare a half-object hash against the whole-object value.
    pub(crate) fn computed(&self, total: u64) -> Option<String> {
        if !self.pending.is_empty() || self.folded_to != total {
            return None;
        }
        self.folded.map(|v| self.algorithm.encode(v))
    }

    /// Whether the computed checksum matches what S3 reported. `None` when no
    /// verdict is possible (see [`computed`](Self::computed)).
    pub(crate) fn matches(&self, total: u64) -> Option<bool> {
        self.computed(total).map(|c| c == self.expected)
    }

    pub(crate) fn expected(&self) -> &str {
        &self.expected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALG: ObjectCrcAlgorithm = ObjectCrcAlgorithm::Crc32;

    fn crc_of(data: &[u8]) -> u64 {
        let mut d = ALG.digest();
        d.update(data);
        d.finalize()
    }

    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// Folding chunk CRCs reproduces a single CRC over the whole object, whatever
    /// order the chunks are recorded in.
    #[test]
    fn folds_to_whole_object_crc_in_any_arrival_order() {
        let body = data(10_000);
        let expected = ALG.encode(crc_of(&body));
        let chunks: Vec<ChunkCrc> = body
            .chunks(1_500)
            .scan(0u64, |off, c| {
                let chunk = ChunkCrc {
                    offset: *off,
                    crc: crc_of(c),
                    len: c.len() as u64,
                };
                *off += c.len() as u64;
                Some(chunk)
            })
            .collect();

        // In order.
        let mut fwd = ObjectCrc::new(ALG, expected.clone());
        for c in &chunks {
            fwd.record(*c);
        }
        assert_eq!(fwd.computed(body.len() as u64).as_deref(), Some(&*expected));
        assert_eq!(fwd.matches(body.len() as u64), Some(true));

        // Reversed: every chunk but the first waits in `pending`.
        let mut rev = ObjectCrc::new(ALG, expected.clone());
        for c in chunks.iter().rev() {
            rev.record(*c);
        }
        assert_eq!(rev.computed(body.len() as u64).as_deref(), Some(&*expected));
    }

    /// A gap yields no verdict rather than a wrong one.
    #[test]
    fn no_verdict_while_a_chunk_is_missing() {
        let body = data(3_000);
        let expected = ALG.encode(crc_of(&body));
        let mut acc = ObjectCrc::new(ALG, expected);

        acc.record(ChunkCrc {
            offset: 0,
            crc: crc_of(&body[..1_000]),
            len: 1_000,
        });
        // Skip [1000, 2000) entirely.
        acc.record(ChunkCrc {
            offset: 2_000,
            crc: crc_of(&body[2_000..]),
            len: 1_000,
        });

        assert_eq!(acc.computed(3_000), None, "a gap cannot produce a verdict");
        assert_eq!(acc.matches(3_000), None);
    }

    /// A prefix that folds cleanly still yields no verdict: the whole-object value
    /// it would be compared against covers bytes that were never delivered.
    #[test]
    fn no_verdict_for_a_contiguous_prefix() {
        let body = data(3_000);
        let expected = ALG.encode(crc_of(&body));
        let mut acc = ObjectCrc::new(ALG, expected);
        acc.record(ChunkCrc {
            offset: 0,
            crc: crc_of(&body[..1_000]),
            len: 1_000,
        });
        assert_eq!(acc.computed(3_000), None);
    }

    /// Corruption anywhere changes the folded value.
    #[test]
    fn a_flipped_byte_fails_the_comparison() {
        let body = data(6_000);
        let expected = ALG.encode(crc_of(&body));
        let mut corrupt = body.clone();
        corrupt[4_321] ^= 0xFF;

        let mut acc = ObjectCrc::new(ALG, expected);
        let mut off = 0u64;
        for c in corrupt.chunks(2_000) {
            acc.record(ChunkCrc {
                offset: off,
                crc: crc_of(c),
                len: c.len() as u64,
            });
            off += c.len() as u64;
        }
        assert_eq!(acc.matches(6_000), Some(false));
    }

    /// A chunk re-recorded at an already-folded offset is ignored, so a retry that
    /// reports twice cannot fold the same bytes into the value again.
    #[test]
    fn duplicate_offset_is_ignored() {
        let body = data(4_000);
        let expected = ALG.encode(crc_of(&body));
        let mut acc = ObjectCrc::new(ALG, expected.clone());
        let mut off = 0u64;
        let chunks: Vec<ChunkCrc> = body
            .chunks(2_000)
            .map(|c| {
                let chunk = ChunkCrc {
                    offset: off,
                    crc: crc_of(c),
                    len: c.len() as u64,
                };
                off += c.len() as u64;
                chunk
            })
            .collect();
        for c in &chunks {
            acc.record(*c);
        }
        for c in &chunks {
            acc.record(*c);
        }
        assert_eq!(acc.computed(4_000).as_deref(), Some(&*expected));
    }

    /// Non-uniform chunk lengths (the ragged tail) fold correctly -- combine takes
    /// the right operand's length, so a short final chunk must not be padded.
    #[test]
    fn ragged_tail_folds_correctly() {
        let body = data(5_500);
        let expected = ALG.encode(crc_of(&body));
        let mut acc = ObjectCrc::new(ALG, expected.clone());
        let mut off = 0u64;
        for c in body.chunks(2_000) {
            acc.record(ChunkCrc {
                offset: off,
                crc: crc_of(c),
                len: c.len() as u64,
            });
            off += c.len() as u64;
        }
        assert_eq!(acc.computed(5_500).as_deref(), Some(&*expected));
    }

    #[test]
    fn composite_value_detection() {
        assert!(is_composite_value("DUoRhQ==-14"));
        assert!(is_composite_value("DUoRhQ==-1"));
        // A plain base64 value has no `-` at all.
        assert!(!is_composite_value("DUoRhQ=="));
        // A trailing `-` with no count is not a part suffix.
        assert!(!is_composite_value("DUoRhQ==-"));
        assert!(!is_composite_value("DUoRhQ==-abc"));
        // Two dashes are not a part suffix -- the SDK rejects this too.
        assert!(!is_composite_value("DUoRhQ==--14"));
        // Digits with no dash at all.
        assert!(!is_composite_value("DUoRhQ14"));
    }

    /// Each algorithm's value is encoded at its own width.
    #[test]
    fn encodes_at_the_algorithm_width() {
        // 4 bytes -> 8 base64 chars with padding; 8 bytes -> 12.
        assert_eq!(ObjectCrcAlgorithm::Crc32.encode(0x0000_0000).len(), 8);
        assert_eq!(ObjectCrcAlgorithm::Crc32C.encode(0x0000_0000).len(), 8);
        assert_eq!(ObjectCrcAlgorithm::Crc64Nvme.encode(0).len(), 12);
    }
}
