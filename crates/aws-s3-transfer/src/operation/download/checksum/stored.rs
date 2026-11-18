/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Classifies stored S3 checksum metadata into download validation requirements.
//!
//! [`StoredChecksum`] recognizes explicit COMPOSITE metadata and otherwise
//! selects the first supported header in SDK priority order.
//! Only a decodable full-object CRC of the correct width can construct a
//! whole-object plan; composite, SHA and unavailable values select response checks.
//! Metadata the transfer manager doesn't recognize, such as an unsupported
//! algorithm or checksum type, is ignored. A malformed value for a recognized
//! algorithm fails discovery.
//! [`ExpectedValue`] preserves the stored encoding alongside its decoded bytes.

use super::crc::CrcAlgorithm;
use super::{ObjectChecksumMethod, ValidationPlan, WholeObjectPlan};
use crate::error::{self, Error};
use crate::model::{ChecksumAlgorithm, ChecksumType, ObjectMetadata};

/// The discovery error for a malformed stored `algorithm` checksum value.
fn malformed(algorithm: &ChecksumAlgorithm, problem: &str) -> Error {
    error::discovery_failed(format!("stored {} checksum {problem}", algorithm.as_str()))
}

/// A stored checksum's decoded bytes and original S3 header value.
///
/// The decoded bytes are compared with the calculated value. The original
/// encoding is retained for checksum mismatch errors.
#[derive(Debug)]
pub(in crate::operation::download) struct ExpectedValue {
    /// Decoded checksum bytes used for equality with the calculated value.
    pub(super) decoded: Vec<u8>,
    /// Original base64 value reported in the integrity error's expected field.
    pub(super) encoded: String,
}

/// Stored checksum metadata classified before selecting a calculation.
///
/// Header priority is CRC64NVME, CRC32C, CRC32, SHA-1, then SHA-256. A later header
/// does not replace a selected header that cannot be verified, and a malformed
/// selected header fails discovery.
#[derive(Debug)]
pub(in crate::operation::download) enum StoredChecksum {
    /// Plain CRC with valid base64, the correct width and absent or FULL_OBJECT type.
    FullObjectCrc {
        /// CRC parameters used for range calculation and whole-object comparison.
        algorithm: CrcAlgorithm,
        /// Stored value checked against this algorithm's decoded width.
        expected: ExpectedValue,
    },
    /// Metadata explicitly marked COMPOSITE, or a selected value with a positive `-N` suffix.
    Composite,
    /// An unsuffixed SHA-1 or SHA-256 value not marked COMPOSITE; selects response checks.
    Ordered,
    /// No supported header, or a CRC whose checksum type is neither absent nor
    /// FULL_OBJECT, such as a type this version doesn't recognize.
    Unavailable,
}

impl StoredChecksum {
    /// Classifies explicit checksum type and the first present supported header.
    ///
    /// Explicit COMPOSITE metadata selects composite checks even without a supported
    /// header. A positive `-N` suffix also selects composite checks regardless of type.
    /// Classification never falls back to later headers. A CRC whose checksum type
    /// is neither absent nor FULL_OBJECT selects [`Self::Unavailable`].
    ///
    /// Returns [`crate::error::ErrorKind::ObjectNotDiscoverable`] when the selected
    /// value is malformed: a `-` suffix that isn't a positive part count, or a
    /// full-object CRC that isn't valid base64 or has the wrong decoded width for
    /// its algorithm.
    pub(in crate::operation::download) fn from_metadata(
        meta: &ObjectMetadata,
    ) -> Result<Self, Error> {
        if matches!(meta.checksum_type, Some(ChecksumType::Composite)) {
            return Ok(Self::Composite);
        }
        let Some((selected_algorithm, value)) = Self::preferred_checksum_header(meta) else {
            return Ok(Self::Unavailable);
        };
        let algorithm = match selected_algorithm {
            ChecksumAlgorithm::Crc64Nvme => Some(CrcAlgorithm::Crc64Nvme),
            ChecksumAlgorithm::Crc32C => Some(CrcAlgorithm::Crc32c),
            ChecksumAlgorithm::Crc32 => Some(CrcAlgorithm::Crc32),
            _ => None,
        };
        // Base64 has no `-`, so a `-` always introduces a part count suffix.
        if let Some((_, count)) = value.rsplit_once('-') {
            return match count.parse::<u32>() {
                Ok(count) if count > 0 => Ok(Self::Composite),
                _ => Err(malformed(
                    &selected_algorithm,
                    "has a part count suffix that isn't a positive number",
                )),
            };
        }
        let Some(algorithm) = algorithm else {
            return Ok(Self::Ordered);
        };
        if !matches!(meta.checksum_type, None | Some(ChecksumType::FullObject)) {
            return Ok(Self::Unavailable);
        }
        let decoded = aws_smithy_types::base64::decode(value)
            .map_err(|_| malformed(&selected_algorithm, "isn't valid base64"))?;
        if decoded.len() != algorithm.width() {
            return Err(malformed(
                &selected_algorithm,
                "has the wrong decoded width for its algorithm",
            ));
        }
        Ok(Self::FullObjectCrc {
            algorithm,
            expected: ExpectedValue {
                decoded,
                encoded: value.to_owned(),
            },
        })
    }

    /// Returns the first present supported header in SDK priority order:
    /// CRC64NVME, CRC32C, CRC32, SHA-1, then SHA-256.
    ///
    /// Does not decode or validate the value. A malformed preferred header is not
    /// replaced by a lower-priority header.
    fn preferred_checksum_header(meta: &ObjectMetadata) -> Option<(ChecksumAlgorithm, &str)> {
        [
            (
                ChecksumAlgorithm::Crc64Nvme,
                meta.checksum_crc64_nvme.as_deref(),
            ),
            (ChecksumAlgorithm::Crc32C, meta.checksum_crc32_c.as_deref()),
            (ChecksumAlgorithm::Crc32, meta.checksum_crc32.as_deref()),
            (ChecksumAlgorithm::Sha1, meta.checksum_sha1.as_deref()),
            (ChecksumAlgorithm::Sha256, meta.checksum_sha256.as_deref()),
        ]
        .into_iter()
        .find_map(|(algorithm, value)| value.map(|value| (algorithm, value)))
    }

    /// Returns the selected header's algorithm even when its value cannot be verified.
    pub(super) fn algorithm(meta: &ObjectMetadata) -> Option<ChecksumAlgorithm> {
        Self::preferred_checksum_header(meta).map(|(algorithm, _)| algorithm)
    }

    /// Selects a whole-object CRC check of `[0, object_len)` or SDK response checks.
    ///
    /// `object_len` is the complete source-object length in bytes, including zero.
    /// Only [`Self::FullObjectCrc`] supplies a whole-object plan; other classifications
    /// retain response-only validation without claiming whole-object coverage.
    pub(in crate::operation::download) fn into_plan(self, object_len: u64) -> ValidationPlan {
        match self {
            Self::FullObjectCrc {
                algorithm,
                expected,
            } => ValidationPlan::WholeObject(WholeObjectPlan {
                method: ObjectChecksumMethod::CombineRanges { algorithm },
                expected,
                object_len,
            }),
            _ => ValidationPlan::PerResponse,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_crcs_require_their_decoded_width() {
        for (algorithm, value) in [
            (CrcAlgorithm::Crc32, "AAAAAA=="),
            (CrcAlgorithm::Crc32c, "AAAAAA=="),
            (CrcAlgorithm::Crc64Nvme, "AAAAAAAAAAA="),
        ] {
            let mut meta = ObjectMetadata::default();
            match algorithm {
                CrcAlgorithm::Crc32 => meta.checksum_crc32 = Some(value.into()),
                CrcAlgorithm::Crc32c => meta.checksum_crc32_c = Some(value.into()),
                CrcAlgorithm::Crc64Nvme => meta.checksum_crc64_nvme = Some(value.into()),
            }
            for checksum_type in [None, Some(ChecksumType::FullObject)] {
                meta.checksum_type = checksum_type;
                let stored = StoredChecksum::from_metadata(&meta).unwrap();
                assert!(matches!(
                    stored,
                    StoredChecksum::FullObjectCrc { algorithm: a, .. } if a == algorithm
                ));
                assert!(matches!(
                    stored.into_plan(7),
                    ValidationPlan::WholeObject(_)
                ));
            }
        }
    }

    #[test]
    fn composite_type_or_suffix_never_selects_whole_object_verification() {
        for (value, checksum_type) in [
            ("AAAAAA==-3", None),
            ("AAAAAA==-3", Some(ChecksumType::FullObject)),
            ("AAAAAA==", Some(ChecksumType::Composite)),
        ] {
            let mut meta = ObjectMetadata::default();
            meta.checksum_crc32 = Some(value.into());
            meta.checksum_type = checksum_type;
            let stored = StoredChecksum::from_metadata(&meta).unwrap();
            assert!(matches!(stored, StoredChecksum::Composite));
            assert!(matches!(stored.into_plan(7), ValidationPlan::PerResponse));
        }
    }

    #[test]
    fn explicit_composite_type_classifies_sha_and_absent_headers() {
        for algorithm in [
            None,
            Some(ChecksumAlgorithm::Sha1),
            Some(ChecksumAlgorithm::Sha256),
        ] {
            let mut meta = ObjectMetadata::default();
            meta.checksum_type = Some(ChecksumType::Composite);
            match algorithm {
                Some(ChecksumAlgorithm::Sha1) => meta.checksum_sha1 = Some("stored hash".into()),
                Some(ChecksumAlgorithm::Sha256) => {
                    meta.checksum_sha256 = Some("stored hash".into())
                }
                None => {}
                _ => unreachable!("test includes only SHA algorithms"),
            }
            let stored = StoredChecksum::from_metadata(&meta).unwrap();
            assert!(
                matches!(stored, StoredChecksum::Composite),
                "{algorithm:?}: {stored:?}"
            );
            assert!(matches!(stored.into_plan(7), ValidationPlan::PerResponse));
        }
    }

    #[test]
    fn first_present_supported_header_controls_classification() {
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc64_nvme = Some("not base64".into());
        meta.checksum_crc32_c = Some("AAAAAA==".into());
        let error = StoredChecksum::from_metadata(&meta).unwrap_err();
        assert_eq!(
            error.kind(),
            &crate::error::ErrorKind::ObjectNotDiscoverable
        );
        let message = format!("{error}: {error:?}");
        assert!(
            message.contains("stored CRC64NVME checksum isn't valid base64"),
            "a malformed preferred header is not replaced by a later one: {message}"
        );
        meta.checksum_crc64_nvme = Some("AAAAAAAAAAA=".into());
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::FullObjectCrc {
                algorithm: CrcAlgorithm::Crc64Nvme,
                ..
            }
        ));
        meta.checksum_crc64_nvme = None;
        meta.checksum_crc32 = Some("AAAAAA==".into());
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::FullObjectCrc {
                algorithm: CrcAlgorithm::Crc32c,
                ..
            }
        ));
    }

    /// Every malformed value for a recognized algorithm fails discovery, naming the
    /// algorithm and the problem.
    #[test]
    fn malformed_values_fail_discovery() {
        let crc32 = |value: &str| {
            let mut meta = ObjectMetadata::default();
            meta.checksum_crc32 = Some(value.into());
            meta
        };
        let sha256 = |value: &str| {
            let mut meta = ObjectMetadata::default();
            meta.checksum_sha256 = Some(value.into());
            meta
        };
        for (meta, problem) in [
            (crc32(""), "wrong decoded width"),
            (crc32("AA=="), "wrong decoded width"),
            (crc32("AAAAAAAAAAA="), "wrong decoded width"),
            (crc32("!"), "isn't valid base64"),
            (crc32("AAAA AA=="), "isn't valid base64"),
            (crc32("AAAAAA==-0"), "part count suffix"),
            (crc32("AAAAAA==-x"), "part count suffix"),
            (crc32("AAAAAA==-"), "part count suffix"),
            (sha256("stored-hash"), "part count suffix"),
        ] {
            let error = StoredChecksum::from_metadata(&meta).unwrap_err();
            assert_eq!(
                error.kind(),
                &crate::error::ErrorKind::ObjectNotDiscoverable,
                "{meta:?}"
            );
            let message = format!("{error}: {error:?}");
            assert!(message.contains(problem), "{meta:?}: {message}");
        }
    }

    /// Metadata the transfer manager doesn't recognize is ignored rather than failing:
    /// an unsupported algorithm, or a checksum type other than COMPOSITE or FULL_OBJECT.
    #[test]
    fn unrecognized_metadata_is_unavailable() {
        let mut meta = ObjectMetadata::default();
        meta.checksum_xxhash64 = Some("AAAAAAAAAAA=".into());
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::Unavailable
        ));

        let mut meta = ObjectMetadata::default();
        meta.checksum_crc32 = Some("AAAAAA==".into());
        meta.checksum_type = Some(ChecksumType::from("FUTURE_TYPE"));
        let stored = StoredChecksum::from_metadata(&meta).unwrap();
        assert!(matches!(stored, StoredChecksum::Unavailable));
        assert!(matches!(stored.into_plan(7), ValidationPlan::PerResponse));
    }

    #[test]
    fn sha_and_absent_values_remain_response_only() {
        let mut meta = ObjectMetadata::default();
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::Unavailable
        ));
        meta.checksum_sha256 = Some("stored hash".into());
        let stored = StoredChecksum::from_metadata(&meta).unwrap();
        assert!(matches!(stored, StoredChecksum::Ordered));
        assert!(matches!(stored.into_plan(8), ValidationPlan::PerResponse));
    }
}
