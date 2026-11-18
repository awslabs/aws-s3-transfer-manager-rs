/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::{invalid_input, Error};
use crate::model::{ChecksumAlgorithm, ChecksumType};

#[doc = include_str!("checksum_strategy.md")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumStrategy {
    algorithm: ChecksumAlgorithm,
    multipart_checksum_type: ChecksumType,
    expected_value: Option<String>,
}

impl ChecksumStrategy {
    /// Returns a strategy whose stored checksum is a full object checksum in `algorithm`, for a
    /// single-request or multipart upload.
    ///
    /// `algorithm` is CRC64NVME, CRC32 or CRC32C.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) when `algorithm`
    /// is SHA1 or SHA256, which S3 supports only as composite checksums for a multipart upload, or
    /// any other algorithm, which the transfer manager cannot calculate.
    pub fn full_object(algorithm: ChecksumAlgorithm) -> Result<Self, Error> {
        match algorithm {
            ChecksumAlgorithm::Crc64Nvme | ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32C => {
                Ok(Self::new(algorithm, ChecksumType::FullObject))
            }
            ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => Err(invalid_input(format!(
                "`{algorithm}` cannot be a full object checksum; use `ChecksumStrategy::composite`"
            ))),
            _ => Err(unsupported(&algorithm)),
        }
    }

    /// Returns a strategy whose stored checksum, for a multipart upload, is a composite checksum
    /// in `algorithm`. A single-request upload sends a full object checksum in `algorithm`.
    ///
    /// `algorithm` is CRC32, CRC32C, SHA1 or SHA256.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) when `algorithm`
    /// is CRC64NVME, which S3 supports only as a full object checksum, or any other algorithm,
    /// which the transfer manager cannot calculate.
    pub fn composite(algorithm: ChecksumAlgorithm) -> Result<Self, Error> {
        match algorithm {
            ChecksumAlgorithm::Crc32
            | ChecksumAlgorithm::Crc32C
            | ChecksumAlgorithm::Sha1
            | ChecksumAlgorithm::Sha256 => Ok(Self::new(algorithm, ChecksumType::Composite)),
            ChecksumAlgorithm::Crc64Nvme => Err(invalid_input(format!(
                "`{algorithm}` cannot be a composite checksum; use `ChecksumStrategy::full_object`"
            ))),
            _ => Err(unsupported(&algorithm)),
        }
    }

    /// Sets the object's expected checksum: the base64 encoding of the big-endian checksum of the
    /// whole object in the strategy's algorithm.
    ///
    /// S3 rejects the upload unless the object matches `value`. The transfer manager sends `value`
    /// as given and does not check it. A single-request upload sends it in place of a checksum
    /// calculated from the body; a multipart upload still calculates each part's checksum and
    /// sends `value` with `CompleteMultipartUpload`.
    ///
    /// A composite strategy with an expected value is valid only for a single-request upload.
    /// `initiate` fails with [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid),
    /// before any request is sent, for an upload sent as a multipart upload: the body's size has no
    /// upper bound, the body is a [`PartStream`](crate::io::PartStream), or its upper bound is at
    /// least the multipart threshold.
    pub fn with_expected_value(mut self, value: impl Into<String>) -> Self {
        self.expected_value = Some(value.into());
        self
    }

    /// The algorithm of every checksum the upload sends and of the checksum S3 stores.
    pub fn algorithm(&self) -> &ChecksumAlgorithm {
        &self.algorithm
    }

    /// The type of the object's checksum when the upload is sent as a multipart upload:
    /// [`ChecksumType::FullObject`] or [`ChecksumType::Composite`].
    ///
    /// A single-request upload always has a full object checksum.
    pub fn multipart_checksum_type(&self) -> &ChecksumType {
        &self.multipart_checksum_type
    }

    /// The expected value set with [`with_expected_value`](Self::with_expected_value), if any.
    ///
    /// A value returned from
    /// [`PartStream::full_object_checksum`](crate::io::PartStream::full_object_checksum) is not
    /// reported here.
    pub fn expected_value(&self) -> Option<&str> {
        self.expected_value.as_deref()
    }

    fn new(algorithm: ChecksumAlgorithm, multipart_checksum_type: ChecksumType) -> Self {
        Self {
            algorithm,
            multipart_checksum_type,
            expected_value: None,
        }
    }
}

impl Default for ChecksumStrategy {
    /// Returns a full object CRC64NVME strategy without an expected value.
    fn default() -> Self {
        Self::new(ChecksumAlgorithm::Crc64Nvme, ChecksumType::FullObject)
    }
}

fn unsupported(algorithm: &ChecksumAlgorithm) -> Error {
    if ChecksumAlgorithm::values().contains(&algorithm.as_str()) {
        invalid_input(format!(
            "the transfer manager cannot calculate `{algorithm}` checksums"
        ))
    } else {
        // `ChecksumAlgorithm::from` matches exact names, so `"crc32"` arrives here as unknown.
        invalid_input(format!(
            "unknown checksum algorithm `{algorithm}`; supported algorithms are `CRC64NVME`, \
             `CRC32`, `CRC32C`, `SHA1` and `SHA256`"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    const UNSUPPORTED: [&str; 5] = ["MD5", "SHA512", "XXHASH64", "XXHASH3", "XXHASH128"];
    const UNKNOWN: [&str; 2] = ["NEW", "crc32"];

    fn assert_input_invalid(result: Result<ChecksumStrategy, Error>, message: &str) {
        let error = result.expect_err("the algorithm must be rejected");
        assert_eq!(error.kind(), &ErrorKind::InputInvalid, "{error}");
        let cause = std::error::Error::source(&error).expect("an input error carries its cause");
        assert_eq!(cause.to_string(), message);
    }

    #[test]
    fn default_is_full_object_crc64_nvme_without_expected_value() {
        let strategy = ChecksumStrategy::default();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc64Nvme);
        assert_eq!(
            strategy.multipart_checksum_type(),
            &ChecksumType::FullObject
        );
        assert_eq!(strategy.expected_value(), None);
        assert_eq!(
            strategy,
            ChecksumStrategy::full_object(ChecksumAlgorithm::Crc64Nvme).unwrap()
        );
    }

    #[test]
    fn full_object_accepts_crc_algorithms() {
        for algorithm in [
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
        ] {
            let strategy = ChecksumStrategy::full_object(algorithm.clone()).unwrap();
            assert_eq!(strategy.algorithm(), &algorithm);
            assert_eq!(
                strategy.multipart_checksum_type(),
                &ChecksumType::FullObject
            );
            assert_eq!(strategy.expected_value(), None);
        }
    }

    #[test]
    fn composite_accepts_crc32_crc32c_and_sha_algorithms() {
        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ] {
            let strategy = ChecksumStrategy::composite(algorithm.clone()).unwrap();
            assert_eq!(strategy.algorithm(), &algorithm);
            assert_eq!(strategy.multipart_checksum_type(), &ChecksumType::Composite);
            assert_eq!(strategy.expected_value(), None);
        }
    }

    #[test]
    fn full_object_rejects_sha_algorithms() {
        assert_input_invalid(
            ChecksumStrategy::full_object(ChecksumAlgorithm::Sha1),
            "`SHA1` cannot be a full object checksum; use `ChecksumStrategy::composite`",
        );
        assert_input_invalid(
            ChecksumStrategy::full_object(ChecksumAlgorithm::Sha256),
            "`SHA256` cannot be a full object checksum; use `ChecksumStrategy::composite`",
        );
    }

    #[test]
    fn composite_rejects_crc64_nvme() {
        assert_input_invalid(
            ChecksumStrategy::composite(ChecksumAlgorithm::Crc64Nvme),
            "`CRC64NVME` cannot be a composite checksum; use `ChecksumStrategy::full_object`",
        );
    }

    #[test]
    fn both_constructors_reject_algorithms_the_transfer_manager_cannot_calculate() {
        for name in UNSUPPORTED {
            let message = format!("the transfer manager cannot calculate `{name}` checksums");
            assert_input_invalid(
                ChecksumStrategy::full_object(ChecksumAlgorithm::from(name)),
                &message,
            );
            assert_input_invalid(
                ChecksumStrategy::composite(ChecksumAlgorithm::from(name)),
                &message,
            );
        }
    }

    #[test]
    fn both_constructors_reject_unknown_algorithm_names() {
        for name in UNKNOWN {
            let message = format!(
                "unknown checksum algorithm `{name}`; supported algorithms are `CRC64NVME`, \
                 `CRC32`, `CRC32C`, `SHA1` and `SHA256`"
            );
            assert_input_invalid(
                ChecksumStrategy::full_object(ChecksumAlgorithm::from(name)),
                &message,
            );
            assert_input_invalid(
                ChecksumStrategy::composite(ChecksumAlgorithm::from(name)),
                &message,
            );
        }
    }

    #[test]
    fn with_expected_value_sets_expected_value() {
        let strategy = ChecksumStrategy::full_object(ChecksumAlgorithm::Crc32)
            .unwrap()
            .with_expected_value("3fRuog==");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32);
        assert_eq!(
            strategy.multipart_checksum_type(),
            &ChecksumType::FullObject
        );
        assert_eq!(strategy.expected_value(), Some("3fRuog=="));

        let strategy = ChecksumStrategy::composite(ChecksumAlgorithm::Sha256)
            .unwrap()
            .with_expected_value("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Sha256);
        assert_eq!(strategy.multipart_checksum_type(), &ChecksumType::Composite);
        assert_eq!(
            strategy.expected_value(),
            Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")
        );
    }
}
