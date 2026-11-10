/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::checksum_algorithm;
use crate::model::ChecksumAlgorithm;
use crate::operation::download::ChunkMetadata;
use crate::types::{ChecksumValidation, IntegrityChecks, NotValidatedReason};
use aws_smithy_observability::unit_of_work::checksums::{
    ChecksumOutcome, NotValidatedReason as ResponseNotValidatedReason,
};

/// The checksum facts of a download, from which its [`IntegrityChecks`] is built
/// when it completes.
#[derive(Debug, Default)]
pub(super) struct ChecksumTally {
    /// The discovery chunk's metadata when that chunk is the whole object, the one
    /// case where its checksum values are the object's.
    whole_object_meta: Option<ChunkMetadata>,
    /// Bytes the delivered chunks hold.
    delivered_bytes: u64,
    /// Bytes of the delivered chunks whose response was validated.
    validated_bytes: u64,
    /// The offset and algorithm of the lowest-offset validated chunk.
    first_validated: Option<(u64, ChecksumAlgorithm)>,
    /// Whether a delivered chunk's response carried only a part-level checksum.
    part_level: bool,
}

impl ChecksumTally {
    /// Records the discovery chunk's metadata, whose checksum values are the
    /// object's only when `whole_object` says that chunk is the whole object.
    pub(super) fn record_discovery(
        &mut self,
        chunk_meta: Option<&ChunkMetadata>,
        whole_object: bool,
    ) {
        self.whole_object_meta = chunk_meta.filter(|_| whole_object).cloned();
    }

    /// Records `outcome`, the checksum outcome of the response that delivered a
    /// chunk's `len` bytes at `offset`. The bytes count as validated only when
    /// `outcome` is `Validated`.
    pub(super) fn record(&mut self, offset: u64, len: u64, outcome: Option<ChecksumOutcome>) {
        self.delivered_bytes += len;
        match outcome {
            Some(ChecksumOutcome::Validated { algorithm, .. }) => {
                self.validated_bytes += len;
                if self
                    .first_validated
                    .as_ref()
                    .is_none_or(|(first, _)| offset < *first)
                {
                    self.first_validated = Some((offset, checksum_algorithm(algorithm)));
                }
            }
            Some(ChecksumOutcome::NotValidated {
                reason: ResponseNotValidatedReason::PartLevelChecksum,
            }) => self.part_level = true,
            _ => {}
        }
    }

    /// The download's verdict, by the first rule that holds:
    ///
    /// 1. `NotValidated { Disabled }` when validation was not enabled.
    /// 2. `Validated` when more than 0 bytes were delivered and every delivered byte
    ///    was validated, with the algorithm of the lowest-offset chunk.
    /// 3. `NotValidated { PartialCoverage }` when some delivered bytes were validated.
    /// 4. `NotValidated { CompositeChecksum }` when a delivered chunk's response
    ///    carried only a part-level checksum.
    /// 5. `NotValidated { Unavailable }` otherwise, including when no bytes were
    ///    delivered.
    pub(super) fn checksum_validation(&self, validation_enabled: bool) -> ChecksumValidation {
        let not_validated = |reason| ChecksumValidation::NotValidated { reason };
        if !validation_enabled {
            return not_validated(NotValidatedReason::Disabled);
        }
        if self.delivered_bytes > 0 && self.validated_bytes == self.delivered_bytes {
            if let Some((_, algorithm)) = &self.first_validated {
                return ChecksumValidation::Validated {
                    algorithm: algorithm.clone(),
                };
            }
        }
        not_validated(if self.validated_bytes > 0 {
            NotValidatedReason::PartialCoverage
        } else if self.part_level {
            NotValidatedReason::CompositeChecksum
        } else {
            NotValidatedReason::Unavailable
        })
    }

    /// The object-integrity result of the completed download: the object's checksum
    /// values from the whole-object discovery metadata, and the download's verdict.
    pub(super) fn integrity_checks(&self, validation_enabled: bool) -> IntegrityChecks {
        let meta = self.whole_object_meta.as_ref();
        IntegrityChecks::new(
            meta.and_then(|m| m.checksum_crc32.clone()),
            meta.and_then(|m| m.checksum_crc32_c.clone()),
            meta.and_then(|m| m.checksum_crc64_nvme.clone()),
            meta.and_then(|m| m.checksum_sha1.clone()),
            meta.and_then(|m| m.checksum_sha256.clone()),
            meta.and_then(|m| m.checksum_type.clone()),
            self.checksum_validation(validation_enabled),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ChecksumType;
    use aws_smithy_observability::unit_of_work::checksums::ResponseChecksumAlgorithm;

    fn validated(algorithm: ResponseChecksumAlgorithm) -> Option<ChecksumOutcome> {
        Some(ChecksumOutcome::Validated {
            algorithm,
            computed: "AAAAAA==".to_owned(),
        })
    }

    fn not_validated(reason: ResponseNotValidatedReason) -> Option<ChecksumOutcome> {
        Some(ChecksumOutcome::NotValidated { reason })
    }

    fn reason(reason: NotValidatedReason) -> ChecksumValidation {
        ChecksumValidation::NotValidated { reason }
    }

    #[test]
    fn disabled_when_validation_not_enabled() {
        let mut tally = ChecksumTally::default();
        tally.record(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        assert_eq!(
            tally.checksum_validation(false),
            reason(NotValidatedReason::Disabled)
        );
    }

    #[test]
    fn validated_takes_the_lowest_offset_chunks_algorithm() {
        let mut tally = ChecksumTally::default();
        tally.record(8, 8, validated(ResponseChecksumAlgorithm::Sha256));
        tally.record(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        assert_eq!(
            tally.checksum_validation(true),
            ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc32
            }
        );
    }

    #[test]
    fn partial_coverage_when_some_delivered_bytes_were_validated() {
        let mut tally = ChecksumTally::default();
        tally.record(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        tally.record(
            8,
            8,
            not_validated(ResponseNotValidatedReason::PartLevelChecksum),
        );
        assert_eq!(
            tally.checksum_validation(true),
            reason(NotValidatedReason::PartialCoverage)
        );
    }

    #[test]
    fn composite_checksum_when_a_chunk_had_only_a_part_level_checksum() {
        let mut tally = ChecksumTally::default();
        tally.record(
            0,
            8,
            not_validated(ResponseNotValidatedReason::PartLevelChecksum),
        );
        tally.record(8, 8, not_validated(ResponseNotValidatedReason::NoChecksum));
        assert_eq!(
            tally.checksum_validation(true),
            reason(NotValidatedReason::CompositeChecksum)
        );
    }

    #[test]
    fn unavailable_when_no_bytes_were_delivered() {
        assert_eq!(
            ChecksumTally::default().checksum_validation(true),
            reason(NotValidatedReason::Unavailable)
        );
    }

    #[test]
    fn other_outcomes_leave_the_bytes_unvalidated() {
        for outcome in [
            None,
            not_validated(ResponseNotValidatedReason::NoChecksum),
            not_validated(ResponseNotValidatedReason::Reused),
        ] {
            let mut tally = ChecksumTally::default();
            tally.record(0, 8, outcome.clone());
            assert_eq!(
                tally.checksum_validation(true),
                reason(NotValidatedReason::Unavailable),
                "{outcome:?}"
            );
        }
    }

    #[test]
    fn integrity_checks_surface_values_only_for_a_whole_object_chunk() {
        let mut meta = ChunkMetadata::default();
        meta.checksum_crc32 = Some("DUoRhQ==".to_owned());
        meta.checksum_type = Some(ChecksumType::FullObject);

        let mut whole = ChecksumTally::default();
        whole.record_discovery(Some(&meta), true);
        let checks = whole.integrity_checks(true);
        assert_eq!(checks.checksum_crc32(), Some("DUoRhQ=="));
        assert_eq!(checks.checksum_type(), Some(&ChecksumType::FullObject));

        let mut part = ChecksumTally::default();
        part.record_discovery(Some(&meta), false);
        let checks = part.integrity_checks(true);
        assert_eq!(checks.checksum_crc32(), None);
        assert_eq!(checks.checksum_type(), None);
    }
}
