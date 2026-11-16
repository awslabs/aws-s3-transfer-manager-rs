/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Classifies stored S3 checksum metadata into download validation requirements.
//!
//! [`StoredChecksum`] selects the first supported header in SDK priority order.
//! Only a decodable full-object CRC of the correct width can construct a
//! whole-object plan; composite, SHA and unavailable values select response checks.
//! [`ExpectedValue`] preserves the stored encoding alongside its decoded bytes.

use super::crc::CrcAlgorithm;
use super::{ObjectChecksumMethod, ValidationPlan, WholeObjectPlan};
use crate::error::{self, Error};
use crate::model::{ChecksumAlgorithm, ChecksumType, ObjectMetadata};

/// A stored CRC's decoded bytes and original S3 header value.
///
/// Classification checks that the decoded width matches the CRC algorithm carried
/// by the whole-object plan. The original encoding is retained for mismatch errors.
#[derive(Debug)]
pub(in crate::operation::download) struct ExpectedValue {
    /// Big-endian checksum bytes used for equality with the calculated CRC.
    pub(super) decoded: Vec<u8>,
    /// Original base64 value reported in the integrity error's expected field.
    pub(super) encoded: String,
}

/// Classification of the first present supported checksum, before selecting a calculation.
///
/// Header priority is CRC64NVME, CRC32C, CRC32, SHA-1, then SHA-256. A later header
/// does not replace a selected header that is malformed or cannot be verified.
#[derive(Debug)]
pub(in crate::operation::download) enum StoredChecksum {
    /// Plain CRC with valid base64, the correct width and absent or FULL_OBJECT type.
    FullObjectCrc {
        /// CRC parameters used for range calculation and whole-object comparison.
        algorithm: CrcAlgorithm,
        /// Stored value checked against this algorithm's decoded width.
        expected: ExpectedValue,
    },
    /// A value with a positive `-N` suffix, or a CRC explicitly marked COMPOSITE.
    Composite,
    /// An unsuffixed SHA-1 or SHA-256 value; selects SDK response checks.
    Ordered,
    /// No supported header, an unknown CRC type or a CRC with invalid base64.
    Unavailable,
}

impl StoredChecksum {
    /// Classifies the first present supported header, without falling back to later headers.
    ///
    /// A positive `-N` suffix selects composite checks regardless of checksum type.
    /// Invalid CRC base64 or an unknown CRC type selects [`Self::Unavailable`].
    /// Returns [`crate::error::ErrorKind::ObjectNotDiscoverable`] when a full-object
    /// CRC decodes successfully but has the wrong width for its algorithm.
    pub(in crate::operation::download) fn from_metadata(
        meta: &ObjectMetadata,
    ) -> Result<Self, Error> {
        let Some((selected_algorithm, value)) = Self::header(meta) else {
            return Ok(Self::Unavailable);
        };
        let algorithm = match selected_algorithm {
            ChecksumAlgorithm::Crc64Nvme => Some(CrcAlgorithm::Crc64Nvme),
            ChecksumAlgorithm::Crc32C => Some(CrcAlgorithm::Crc32c),
            ChecksumAlgorithm::Crc32 => Some(CrcAlgorithm::Crc32),
            _ => None,
        };
        if value
            .rsplit_once('-')
            .is_some_and(|(_, count)| count.parse::<u32>().is_ok_and(|count| count > 0))
        {
            return Ok(Self::Composite);
        }
        let Some(algorithm) = algorithm else {
            return Ok(Self::Ordered);
        };
        if matches!(meta.checksum_type, Some(ChecksumType::Composite)) {
            return Ok(Self::Composite);
        }
        if !matches!(meta.checksum_type, None | Some(ChecksumType::FullObject)) {
            return Ok(Self::Unavailable);
        }
        let decoded = match aws_smithy_types::base64::decode(value) {
            Ok(decoded) => decoded,
            Err(_) => {
                tracing::error!(
                    target: crate::telemetry::TARGET_TRANSFER_DOWNLOAD,
                    ?algorithm,
                    "stored checksum has invalid base64 encoding",
                );
                return Ok(Self::Unavailable);
            }
        };
        if decoded.len() != algorithm.width() {
            return Err(error::discovery_failed(
                "stored CRC checksum has an invalid decoded width",
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

    /// Returns the first present supported header, without decoding or validating it.
    fn header(meta: &ObjectMetadata) -> Option<(ChecksumAlgorithm, &str)> {
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
        Self::header(meta).map(|(algorithm, _)| algorithm)
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
    fn first_present_supported_header_controls_classification() {
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc64_nvme = Some("not base64".into());
        meta.checksum_crc32_c = Some("AAAAAA==".into());
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::Unavailable
        ));
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

    #[test]
    fn malformed_width_fails_but_invalid_base64_is_unavailable() {
        for value in ["", "AA==", "AAAAAAAAAAA="] {
            let mut meta = ObjectMetadata::default();
            meta.checksum_crc32 = Some(value.into());
            assert_eq!(
                StoredChecksum::from_metadata(&meta).unwrap_err().kind(),
                &crate::error::ErrorKind::ObjectNotDiscoverable
            );
        }
        let mut meta = ObjectMetadata::default();
        meta.checksum_crc32 = Some("!".into());
        assert!(matches!(
            StoredChecksum::from_metadata(&meta).unwrap(),
            StoredChecksum::Unavailable
        ));
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
