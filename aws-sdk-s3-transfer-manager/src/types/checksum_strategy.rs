/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::model::{ChecksumAlgorithm, ChecksumType};
use crate::types::checksum::{CompositeAlgorithm, FullObjectAlgorithm};

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
    /// [`FullObjectAlgorithm`] lists the supported algorithms.
    pub fn full_object<A: FullObjectAlgorithm>(algorithm: A) -> Self {
        let _ = algorithm;
        Self::new(A::ALGORITHM, ChecksumType::FullObject)
    }

    /// Returns a strategy whose stored checksum, for a multipart upload, is a composite checksum
    /// in `algorithm`. A single-request upload sends a full object checksum in `algorithm`.
    ///
    /// [`CompositeAlgorithm`] lists the supported algorithms.
    pub fn composite<A: CompositeAlgorithm>(algorithm: A) -> Self {
        let _ = algorithm;
        Self::new(A::ALGORITHM, ChecksumType::Composite)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::checksum::{Crc32, Crc32C, Crc64Nvme, Sha1, Sha256};

    fn assert_strategy(
        strategy: &ChecksumStrategy,
        algorithm: ChecksumAlgorithm,
        multipart_checksum_type: ChecksumType,
    ) {
        assert_eq!(strategy.algorithm(), &algorithm);
        assert_eq!(strategy.multipart_checksum_type(), &multipart_checksum_type);
        assert_eq!(strategy.expected_value(), None);
    }

    #[test]
    fn default_is_full_object_crc64_nvme_without_expected_value() {
        let strategy = ChecksumStrategy::default();
        assert_strategy(
            &strategy,
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumType::FullObject,
        );
        assert_eq!(strategy, ChecksumStrategy::full_object(Crc64Nvme));
    }

    #[test]
    fn full_object_stores_the_algorithm_and_full_object_type() {
        for (strategy, algorithm) in [
            (
                ChecksumStrategy::full_object(Crc64Nvme),
                ChecksumAlgorithm::Crc64Nvme,
            ),
            (
                ChecksumStrategy::full_object(Crc32),
                ChecksumAlgorithm::Crc32,
            ),
            (
                ChecksumStrategy::full_object(Crc32C),
                ChecksumAlgorithm::Crc32C,
            ),
        ] {
            assert_strategy(&strategy, algorithm, ChecksumType::FullObject);
        }
    }

    #[test]
    fn composite_stores_the_algorithm_and_composite_type() {
        for (strategy, algorithm) in [
            (ChecksumStrategy::composite(Crc32), ChecksumAlgorithm::Crc32),
            (
                ChecksumStrategy::composite(Crc32C),
                ChecksumAlgorithm::Crc32C,
            ),
            (ChecksumStrategy::composite(Sha1), ChecksumAlgorithm::Sha1),
            (
                ChecksumStrategy::composite(Sha256),
                ChecksumAlgorithm::Sha256,
            ),
        ] {
            assert_strategy(&strategy, algorithm, ChecksumType::Composite);
        }
    }

    #[test]
    fn with_expected_value_sets_expected_value() {
        let strategy = ChecksumStrategy::full_object(Crc32).with_expected_value("3fRuog==");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32);
        assert_eq!(
            strategy.multipart_checksum_type(),
            &ChecksumType::FullObject
        );
        assert_eq!(strategy.expected_value(), Some("3fRuog=="));

        let strategy = ChecksumStrategy::composite(Sha256)
            .with_expected_value("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Sha256);
        assert_eq!(strategy.multipart_checksum_type(), &ChecksumType::Composite);
        assert_eq!(
            strategy.expected_value(),
            Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")
        );
    }
}
