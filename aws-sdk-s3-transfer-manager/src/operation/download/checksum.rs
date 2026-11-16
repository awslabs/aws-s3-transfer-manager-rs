/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Validation requirements and accepted checksum evidence for one download.
//!
//! [`ValidationPlan`](crate::operation::download::checksum::ValidationPlan)
//! selects response checks or additional whole-object verification.
//! [`RangeCompletion`](crate::operation::download::checksum::RangeCompletion)
//! pairs accepted object offsets with the checks over those bytes;
//! [`DownloadIntegrity`](crate::operation::download::checksum::DownloadIntegrity)
//! owns that evidence and consumes it to produce the final verdict.
//! Whole-object success requires nonoverlapping, complete coverage and checksum equality.
//! Stored-checksum classification and CRC calculation remain in the child modules.

mod crc;
pub(super) mod stored;

use crc::{digest_segments, CrcFold};
pub(super) use crc::{ComputedChecksum, CrcAlgorithm};
use stored::ExpectedValue;

use crate::error::{checksum_algorithm, Error, ErrorKind};
use crate::model::{ChecksumAlgorithm, ChecksumType, ObjectMetadata};
use crate::runtime::buffer_pool::SegmentedBytes;
use crate::types::{ChecksumValidation, IntegrityChecks, NotValidatedReason};
use aws_smithy_observability::unit_of_work::checksums::{
    ChecksumOutcome, NotValidatedReason as ResponseNotValidatedReason,
};

/// Checks required for the bytes accepted by one download.
#[derive(Debug)]
pub(super) enum ValidationPlan {
    /// Reports validation as disabled, regardless of recorded response outcomes.
    Disabled,
    /// Derives the verdict solely from SDK checks over accepted response bodies.
    PerResponse,
    /// Requires the stored whole-object checksum to match, in addition to SDK response checks.
    WholeObject(WholeObjectPlan),
}

/// A whole-object calculation, its expected value and the required byte coverage.
///
/// The expected value is decoded and checked against the method's algorithm width.
/// Verification covers exactly `[0, object_len)`, including an empty object.
#[derive(Debug)]
pub(super) struct WholeObjectPlan {
    /// Calculation and algorithm required for the accepted object bytes.
    method: ObjectChecksumMethod,
    /// Stored checksum in decoded and original header form, for comparison and errors.
    expected: ExpectedValue,
    /// Complete source-object length in bytes, independent of download range size.
    object_len: u64,
}

/// The calculation performed over accepted object bytes.
#[derive(Debug, Clone, Copy)]
pub(super) enum ObjectChecksumMethod {
    /// Calculates each range's CRC independently and combines results in object-offset order.
    CombineRanges {
        /// CRC used for every range and the final whole-object comparison.
        algorithm: CrcAlgorithm,
    },
}

impl ObjectChecksumMethod {
    /// Returns the range calculation required to supply evidence for this method.
    fn range_digest_mode(self) -> RangeDigestMode {
        match self {
            Self::CombineRanges { algorithm } => RangeDigestMode::Calculate { algorithm },
        }
    }
}

/// Per-range evidence required by the download's validation plan.
#[derive(Debug, Clone, Copy)]
pub(super) enum RangeDigestMode {
    /// Captures response outcomes and byte bounds without calculating another checksum.
    None,
    /// Calculates a CRC over the frozen bytes accepted for the range.
    Calculate {
        /// Must match the whole-object method that consumes this range's evidence.
        algorithm: CrcAlgorithm,
    },
}

impl RangeDigestMode {
    /// Captures `[offset, offset + bytes.len())`, its SDK outcome and the selected CRC.
    ///
    /// `offset` is the absolute source-object offset of the first remaining byte.
    /// Hashing neither advances the buffer's cursor nor copies payload bytes.
    /// Capturing evidence does not accept it; record it only after publication and
    /// any required destination writes succeed.
    ///
    /// Returns [`ErrorKind::RuntimeError`] if the length or exclusive end exceeds `u64`.
    pub(super) fn capture(
        self,
        offset: u64,
        bytes: &SegmentedBytes,
        response_outcome: Option<ChecksumOutcome>,
    ) -> Result<RangeCompletion, Error> {
        let len = u64::try_from(bytes.len())
            .map_err(|_| Error::new(ErrorKind::RuntimeError, "range length exceeds u64"))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::new(ErrorKind::RuntimeError, "range checksum bounds overflow"))?;
        let computed = match self {
            Self::None => None,
            Self::Calculate { algorithm } => Some(digest_segments(algorithm, bytes)),
        };
        Ok(RangeCompletion {
            range: CompletedRange { start: offset, end },
            response_outcome,
            computed,
        })
    }
}

/// Absolute source-object byte bounds `[start, end)`, ordered by start and then end.
///
/// Whole-object verification rejects empty, reversed, overlapping or out-of-bounds
/// intervals; an empty object requires no range evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CompletedRange {
    /// Absolute offset of the first accepted byte.
    pub(super) start: u64,
    /// Absolute offset immediately after the last accepted byte.
    pub(super) end: u64,
}

/// Byte bounds and checksum evidence to record after a range is successfully accepted.
///
/// Record each range once, after its body is collected, published and any required
/// destination writes succeed. Failed or cancelled attempts contribute no evidence.
#[derive(Debug)]
pub(super) struct RangeCompletion {
    /// Absolute source-object bounds of the accepted bytes.
    pub(super) range: CompletedRange,
    /// SDK result for the body carrying these bytes; `None` supplies no validated coverage.
    pub(super) response_outcome: Option<ChecksumOutcome>,
    /// CRC over exactly `range`; required when the selected method combines range CRCs.
    pub(super) computed: Option<ComputedChecksum>,
}

/// Owns accepted response coverage and the selected whole-object verification.
///
/// Only successful ranges contribute evidence. SDK coverage does not bypass a
/// whole-object check: [`Self::finish`] requires its exact coverage and checksum match.
/// A successful all-bytes SDK verdict retains its algorithm; otherwise a successful
/// whole-object check supplies the validation algorithm.
#[derive(Debug)]
pub(super) struct DownloadIntegrity {
    /// Checks required before the accepted evidence can produce a final verdict.
    plan: ValidationPlan,
    /// Metadata describing the whole object's stored values, never a part's checksum values.
    whole_object_meta: Option<ObjectMetadata>,
    /// Present exactly when the plan requires combining range CRCs.
    fold: Option<CrcFold>,
    /// First bounds or byte-count error; later records are ignored.
    failure: Option<Error>,
    /// Accepted response bytes, whether or not the SDK validated them.
    delivered_bytes: u64,
    /// Accepted response bytes covered by a successful SDK checksum check.
    validated_bytes: u64,
    /// Lowest accepted object offset with successful SDK validation, and its algorithm.
    first_validated: Option<(u64, ChecksumAlgorithm)>,
    /// At least one response carried a composite value the SDK could not validate.
    part_level: bool,
    /// At least one response used an algorithm the SDK could not validate.
    unsupported_algorithm: bool,
}

impl Default for DownloadIntegrity {
    fn default() -> Self {
        Self::new(ValidationPlan::PerResponse, None)
    }
}

impl DownloadIntegrity {
    /// Creates empty accepted-byte accounting for `plan`.
    ///
    /// `object_meta`, when present, must describe the whole object's stored values.
    /// Those values are reported independently of the validation verdict.
    pub(super) fn new(plan: ValidationPlan, object_meta: Option<ObjectMetadata>) -> Self {
        let fold = match &plan {
            ValidationPlan::WholeObject(WholeObjectPlan {
                method: ObjectChecksumMethod::CombineRanges { algorithm },
                object_len,
                ..
            }) => Some(CrcFold::new(*algorithm, *object_len)),
            _ => None,
        };
        Self {
            plan,
            whole_object_meta: object_meta,
            fold,
            failure: None,
            delivered_bytes: 0,
            validated_bytes: 0,
            first_validated: None,
            part_level: false,
            unsupported_algorithm: false,
        }
    }

    /// Derives range evidence from the selected whole-object calculation.
    pub(super) fn range_digest_mode(&self) -> RangeDigestMode {
        match &self.plan {
            ValidationPlan::WholeObject(plan) => plan.method.range_digest_mode(),
            _ => RangeDigestMode::None,
        }
    }

    /// Records `len` accepted bytes at absolute object `offset` with their SDK outcome.
    ///
    /// The range must have been successfully published and written where required.
    /// Invalid bounds latch an error. A whole-object CRC plan also latches missing
    /// calculated evidence; response accounting alone cannot satisfy that plan.
    pub(super) fn record_response(
        &mut self,
        offset: u64,
        len: u64,
        outcome: Option<ChecksumOutcome>,
    ) {
        let Some(end) = offset.checked_add(len) else {
            self.failure.get_or_insert_with(|| {
                Error::new(ErrorKind::RuntimeError, "range checksum bounds overflow")
            });
            return;
        };
        self.record(RangeCompletion {
            range: CompletedRange { start: offset, end },
            response_outcome: outcome,
            computed: None,
        });
    }

    /// Records one successfully accepted range and its checks.
    ///
    /// Reversed bounds and overflowing accepted-byte counts latch a runtime error and
    /// ignore later records. Whole-object CRC checks also latch missing, overlapping,
    /// out-of-bounds or wrong-algorithm evidence; failure cannot be repaired by retries.
    pub(super) fn record(&mut self, completion: RangeCompletion) {
        if self.failure.is_some() {
            return;
        }
        let RangeCompletion {
            range,
            response_outcome,
            computed,
        } = completion;
        let Some(len) = range.end.checked_sub(range.start) else {
            self.failure.get_or_insert_with(|| {
                Error::new(
                    ErrorKind::RuntimeError,
                    "range checksum has reversed bounds",
                )
            });
            return;
        };
        if self.delivered_bytes.checked_add(len).is_none() {
            self.failure.get_or_insert_with(|| {
                Error::new(
                    ErrorKind::RuntimeError,
                    "accepted checksum byte count overflows",
                )
            });
            return;
        }
        self.record_response_outcome(range.start, len, response_outcome);
        if let Some(fold) = &mut self.fold {
            match computed {
                Some(computed) => fold.record(range, computed),
                None => fold.fail("accepted range is missing its calculated CRC"),
            }
        }
    }

    /// Consumes accepted evidence and returns its stored values and final verdict.
    ///
    /// Whole-object checks run even when SDK checks already cover all accepted bytes.
    /// Response-only plans can return `NotValidated`; whole-object plans cannot
    /// downgrade a failed comparison or incomplete coverage to that verdict.
    ///
    /// Returns [`ErrorKind::RuntimeError`] for latched evidence errors or incomplete
    /// coverage, and [`ErrorKind::IntegrityError`] when a calculated checksum differs.
    pub(super) fn finish(mut self) -> Result<IntegrityChecks, Error> {
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        let mut validation = self.response_validation();
        if let ValidationPlan::WholeObject(plan) = &self.plan {
            let fold = self.fold.take().ok_or_else(|| {
                Error::new(
                    ErrorKind::RuntimeError,
                    "whole-object plan has no CRC accumulator",
                )
            })?;
            fold.finish(&plan.expected)?;
            if !matches!(validation, ChecksumValidation::Validated { .. }) {
                let ObjectChecksumMethod::CombineRanges { algorithm } = plan.method;
                validation = ChecksumValidation::Validated {
                    algorithm: algorithm.model(),
                };
            }
        }
        Ok(self.checks(validation))
    }

    /// Records `outcome`, the checksum outcome of the response that delivered a
    /// chunk's `len` bytes at `offset`. The bytes count as validated only when
    /// `outcome` is `Validated`.
    fn record_response_outcome(&mut self, offset: u64, len: u64, outcome: Option<ChecksumOutcome>) {
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
            Some(ChecksumOutcome::NotValidated {
                reason: ResponseNotValidatedReason::UnsupportedAlgorithm,
            }) => self.unsupported_algorithm = true,
            _ => {}
        }
    }

    /// The download's verdict, by the first rule that holds:
    ///
    /// 1. `NotValidated { reason: Disabled }` when validation was not enabled.
    /// 2. `Validated` when more than 0 bytes were delivered and every delivered byte
    ///    was validated, with the algorithm of the lowest-offset chunk.
    /// 3. `NotValidated { reason: PartialCoverage }` when some delivered bytes were
    ///    validated.
    /// 4. `NotValidated { reason: CompositeChecksum }` when a delivered chunk's
    ///    response carried only a part-level checksum.
    /// 5. `NotValidated { reason: UnsupportedAlgorithm }` when a delivered chunk's
    ///    response carried checksums only in algorithms the SDK does not compute.
    /// 6. `NotValidated { reason: Unavailable }` otherwise, including when no bytes
    ///    were delivered.
    fn response_validation(&self) -> ChecksumValidation {
        let not_validated = |reason| ChecksumValidation::NotValidated { reason };
        if matches!(self.plan, ValidationPlan::Disabled) {
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
        } else if self.unsupported_algorithm {
            NotValidatedReason::UnsupportedAlgorithm
        } else {
            NotValidatedReason::Unavailable
        })
    }

    /// Returns stored object values and the verdict derived only from SDK response coverage.
    ///
    /// This does not verify whole-object coverage or surface latched evidence errors.
    /// It cannot establish that a whole-object validation plan succeeded.
    pub(super) fn response_checks(&self) -> IntegrityChecks {
        self.checks(self.response_validation())
    }

    /// Pairs `validation` with the supplied whole-object metadata's stored checksum values.
    ///
    /// A [`ChecksumType::Composite`] value is a checksum of part checksums, which a
    /// checksum over the delivered bytes does not reproduce, so its value members
    /// are `None` and only the type is reported.
    fn checks(&self, validation: ChecksumValidation) -> IntegrityChecks {
        let meta = self.whole_object_meta.as_ref();
        let values = meta.filter(|m| m.checksum_type != Some(ChecksumType::Composite));
        IntegrityChecks::new(
            values.and_then(|m| m.checksum_crc32.clone()),
            values.and_then(|m| m.checksum_crc32_c.clone()),
            values.and_then(|m| m.checksum_crc64_nvme.clone()),
            values.and_then(|m| m.checksum_sha1.clone()),
            values.and_then(|m| m.checksum_sha256.clone()),
            meta.and_then(|m| m.checksum_type.clone()),
            validation,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let mut tally = DownloadIntegrity::new(ValidationPlan::Disabled, None);
        tally.record_response(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::Disabled)
        );
    }

    #[test]
    fn validated_takes_the_lowest_offset_chunks_algorithm() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(8, 8, validated(ResponseChecksumAlgorithm::Sha256));
        tally.record_response(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        assert_eq!(
            tally.response_validation(),
            ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc32
            }
        );
    }

    #[test]
    fn partial_coverage_when_some_delivered_bytes_were_validated() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        tally.record_response(
            8,
            8,
            not_validated(ResponseNotValidatedReason::PartLevelChecksum),
        );
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::PartialCoverage)
        );
    }

    #[test]
    fn composite_checksum_when_a_chunk_had_only_a_part_level_checksum() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(
            0,
            8,
            not_validated(ResponseNotValidatedReason::PartLevelChecksum),
        );
        tally.record_response(8, 8, not_validated(ResponseNotValidatedReason::NoChecksum));
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::CompositeChecksum)
        );
    }

    #[test]
    fn unsupported_algorithm_when_a_chunk_had_only_an_unsupported_checksum() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(
            0,
            8,
            not_validated(ResponseNotValidatedReason::UnsupportedAlgorithm),
        );
        tally.record_response(8, 8, not_validated(ResponseNotValidatedReason::NoChecksum));
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::UnsupportedAlgorithm)
        );
    }

    #[test]
    fn partial_coverage_outranks_an_unsupported_algorithm() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(0, 8, validated(ResponseChecksumAlgorithm::Crc32));
        tally.record_response(
            8,
            8,
            not_validated(ResponseNotValidatedReason::UnsupportedAlgorithm),
        );
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::PartialCoverage)
        );
    }

    #[test]
    fn a_part_level_checksum_outranks_an_unsupported_algorithm() {
        let mut tally = DownloadIntegrity::default();
        tally.record_response(
            0,
            8,
            not_validated(ResponseNotValidatedReason::UnsupportedAlgorithm),
        );
        tally.record_response(
            8,
            8,
            not_validated(ResponseNotValidatedReason::PartLevelChecksum),
        );
        assert_eq!(
            tally.response_validation(),
            reason(NotValidatedReason::CompositeChecksum)
        );
    }

    #[test]
    fn unavailable_when_no_bytes_were_delivered() {
        assert_eq!(
            DownloadIntegrity::default().response_validation(),
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
            let mut tally = DownloadIntegrity::default();
            tally.record_response(0, 8, outcome.clone());
            assert_eq!(
                tally.response_validation(),
                reason(NotValidatedReason::Unavailable),
                "{outcome:?}"
            );
        }
    }

    #[test]
    fn integrity_checks_surface_values_only_for_a_whole_object_chunk() {
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc32 = Some("DUoRhQ==".to_owned());
        meta.checksum_type = Some(ChecksumType::FullObject);

        let whole = DownloadIntegrity::new(ValidationPlan::PerResponse, Some(meta));
        let checks = whole.response_checks();
        assert_eq!(checks.checksum_crc32(), Some("DUoRhQ=="));
        assert_eq!(checks.checksum_type(), Some(&ChecksumType::FullObject));

        let part = DownloadIntegrity::new(ValidationPlan::PerResponse, None);
        let checks = part.response_checks();
        assert_eq!(checks.checksum_crc32(), None);
        assert_eq!(checks.checksum_type(), None);
    }

    #[test]
    fn integrity_checks_omit_composite_values_and_keep_the_type() {
        // A one-part multipart object with a composite checksum fits in the discovery
        // chunk, but its `-1` value is a checksum of part checksums and does not
        // describe the delivered bytes.
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc32 = Some("DUoRhQ==-1".to_owned());
        meta.checksum_crc32_c = Some("yZRlqg==-1".to_owned());
        meta.checksum_sha1 = Some("Kq5sNclPz7QV2+lfQIuc6R7oRu0=-1".to_owned());
        meta.checksum_sha256 = Some("uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=-1".to_owned());
        meta.checksum_type = Some(ChecksumType::Composite);

        let checks =
            DownloadIntegrity::new(ValidationPlan::PerResponse, Some(meta)).response_checks();
        assert_eq!(checks.checksum_crc32(), None);
        assert_eq!(checks.checksum_crc32c(), None);
        assert_eq!(checks.checksum_crc64_nvme(), None);
        assert_eq!(checks.checksum_sha1(), None);
        assert_eq!(checks.checksum_sha256(), None);
        assert_eq!(checks.checksum_type(), Some(&ChecksumType::Composite));
    }

    fn whole_owner(bytes: &[u8]) -> DownloadIntegrity {
        let mut checksum = aws_smithy_checksums::ChecksumAlgorithm::Crc32.into_impl();
        checksum.update(bytes);
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc32 = Some(aws_smithy_types::base64::encode(checksum.finalize()));
        meta.checksum_type = Some(ChecksumType::FullObject);
        let plan = stored::StoredChecksum::from_metadata(&meta)
            .unwrap()
            .into_plan(bytes.len() as u64);
        DownloadIntegrity::new(plan, Some(meta))
    }

    #[test]
    fn whole_object_verification_validates_ranges_without_sdk_checksums() {
        let mut integrity = whole_owner(b"abcdefgh");
        let mode = integrity.range_digest_mode();
        for (offset, bytes) in [(4, b"efgh".as_slice()), (0, b"abcd".as_slice())] {
            let bytes = SegmentedBytes::from(bytes::Bytes::copy_from_slice(bytes));
            integrity.record(mode.capture(offset, &bytes, None).unwrap());
        }
        assert_eq!(
            integrity.finish().unwrap().checksum_validation(),
            &ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc32
            }
        );
    }

    #[test]
    fn sdk_verdict_does_not_bypass_failed_whole_object_verification() {
        let mut integrity = whole_owner(b"stored");
        let bytes = SegmentedBytes::from(bytes::Bytes::from_static(b"broken"));
        integrity.record(
            integrity
                .range_digest_mode()
                .capture(0, &bytes, validated(ResponseChecksumAlgorithm::Sha256))
                .unwrap(),
        );
        assert!(matches!(
            integrity.finish().unwrap_err().kind(),
            ErrorKind::IntegrityError(_)
        ));
    }

    #[test]
    fn successful_sdk_verdict_retains_its_algorithm() {
        let mut integrity = whole_owner(b"stored");
        let bytes = SegmentedBytes::from(bytes::Bytes::from_static(b"stored"));
        integrity.record(
            integrity
                .range_digest_mode()
                .capture(0, &bytes, validated(ResponseChecksumAlgorithm::Sha256))
                .unwrap(),
        );
        assert_eq!(
            integrity.finish().unwrap().checksum_validation(),
            &ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Sha256
            }
        );
    }

    #[test]
    fn missing_calculated_evidence_cannot_downgrade_verification() {
        let mut integrity = whole_owner(b"stored");
        integrity.record_response(0, 6, validated(ResponseChecksumAlgorithm::Crc32));
        assert_eq!(
            integrity.finish().unwrap_err().kind(),
            &ErrorKind::RuntimeError
        );
    }

    #[test]
    fn empty_crc_object_is_verified_without_response_evidence() {
        assert_eq!(
            whole_owner(b"").finish().unwrap().checksum_validation(),
            &ChecksumValidation::Validated {
                algorithm: ChecksumAlgorithm::Crc32
            }
        );
    }

    #[test]
    fn capture_rejects_offset_overflow() {
        let bytes = SegmentedBytes::from(bytes::Bytes::from_static(b"a"));
        assert_eq!(
            RangeDigestMode::None
                .capture(u64::MAX, &bytes, None)
                .unwrap_err()
                .kind(),
            &ErrorKind::RuntimeError
        );
    }
}
