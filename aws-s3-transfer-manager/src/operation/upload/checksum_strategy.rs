use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
use aws_smithy_types::error::operation::BuildError;

#[doc = std::include_str!("checksum_strategy.md")]
#[derive(Debug, Clone)]
pub struct ChecksumStrategy {
    /// The checksum algorithm to use.
    algorithm: ChecksumAlgorithm,

    /// The checksum type to use IF the upload is multipart.
    type_if_multipart: ChecksumType,

    /// The precalculated full object checksum value.
    full_object_checksum: Option<String>,
}

impl ChecksumStrategy {
    /// Use a precalculated `CRC64NVME` full object checksum value.
    pub fn with_crc64_nvme(value: impl Into<String>) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc64Nvme,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: Some(value.into()),
        }
    }

    /// Use a precalculated `CRC32` full object checksum value.
    pub fn with_crc32(value: impl Into<String>) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: Some(value.into()),
        }
    }

    /// Use a precalculated `CRC32C` full object checksum value.
    pub fn with_crc32_c(value: impl Into<String>) -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32C,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: Some(value.into()),
        }
    }

    /// The transfer manager calculates a `CRC64NVME` full object checksum while uploading.
    /// This is the default strategy.
    pub fn with_calculated_crc64_nvme() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc64Nvme,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates a `CRC32` full object checksum while uploading.
    pub fn with_calculated_crc32() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates a `CRC32C` full object checksum while uploading.
    pub fn with_calculated_crc32_c() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32C,
            type_if_multipart: ChecksumType::FullObject,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates `CRC32` checksums while uploading.
    /// If the upload is multipart, the object will have a composite checksum,
    /// otherwise it will have a full object checksum.
    pub fn with_calculated_crc32_composite_if_multipart() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32,
            type_if_multipart: ChecksumType::Composite,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates `CRC32C` checksums while uploading.
    /// If the upload is multipart, the object will have a composite checksum,
    /// otherwise it will have a full object checksum.
    pub fn with_calculated_crc32_c_composite_if_multipart() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Crc32C,
            type_if_multipart: ChecksumType::Composite,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates `SHA1` checksums while uploading.
    /// If the upload is multipart, the object will have a composite checksum,
    /// otherwise it will have a full object checksum.
    pub fn with_calculated_sha1_composite_if_multipart() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Sha1,
            type_if_multipart: ChecksumType::Composite,
            full_object_checksum: None,
        }
    }

    /// The transfer manager calculates `SHA256` checksums while uploading.
    /// If the upload is multipart, the object will have a composite checksum,
    /// otherwise it will have a full object checksum.
    pub fn with_calculated_sha256_composite_if_multipart() -> Self {
        Self {
            algorithm: ChecksumAlgorithm::Sha256,
            type_if_multipart: ChecksumType::Composite,
            full_object_checksum: None,
        }
    }

    /// Builder for [`ChecksumStrategy`].
    ///
    /// Note that [`Builder::build()`] can fail if the strategy is invalid.
    /// E.g. S3 does not allow any `SHA` algorithm to do full object multipart uploads,
    /// and does not allow `CRC64NVME` to do composite multipart uploads.
    /// You should prefer the `with_` constructors, which cannot fail.
    pub fn builder() -> Builder {
        Builder {
            algorithm: None,
            type_if_multipart: None,
            full_object_checksum: None,
        }
    }

    /// The checksum algorithm to use.
    pub fn algorithm(&self) -> &ChecksumAlgorithm {
        &self.algorithm
    }

    /// The checksum type to use IF the upload is multipart.
    ///
    /// Note that if the upload is NOT multipart (e.g. object size is below the multipart threshold),
    /// the object's checksum type will always be [`ChecksumType::FullObject`], regardless of this setting.
    pub fn type_if_multipart(&self) -> &ChecksumType {
        &self.type_if_multipart
    }

    /// The precalculated full object checksum value.
    ///
    /// If specified, this value will be sent to S3 as the full object checksum value.
    /// In the case of a multipart upload, the transfer manager still calculates
    /// checksums for individual parts, but this value will always be sent as the full object checksum value.
    ///
    /// If not specified, the transfer manager will calculate the checksum value.
    ///
    /// You may not specify this when [type_if_multipart](`Self::type_if_multipart`) is [`ChecksumType::Composite`].
    pub fn full_object_checksum(&self) -> Option<&str> {
        self.full_object_checksum.as_deref()
    }
}

impl Default for ChecksumStrategy {
    /// The transfer manager calculates a `CRC64NVME` full object checksum while uploading.
    fn default() -> Self {
        Self::with_calculated_crc64_nvme()
    }
}

/// Builder for [`ChecksumStrategy`].
#[derive(Debug)]
pub struct Builder {
    algorithm: Option<ChecksumAlgorithm>,
    type_if_multipart: Option<ChecksumType>,
    full_object_checksum: Option<String>,
}

impl Builder {
    /// The checksum algorithm to use.
    pub fn algorithm(mut self, input: ChecksumAlgorithm) -> Self {
        self.algorithm = Some(input);
        self
    }

    /// The checksum type to use IF the upload is multipart.
    ///
    /// Note that if the upload is NOT multipart (e.g. object size is below the multipart threshold),
    /// the object's checksum type will always be [`ChecksumType::FullObject`], regardless of this setting.
    pub fn type_if_multipart(mut self, input: ChecksumType) -> Self {
        self.type_if_multipart = Some(input);
        self
    }

    /// The precalculated full object checksum value.
    ///
    /// If specified, this value will be sent to S3 as the full object checksum value.
    /// In the case of a multipart upload, the transfer manager still calculates
    /// checksums for individual parts, but this value will always be sent as the full object checksum value.
    ///
    /// If not specified, the transfer manager will calculate the checksum value.
    ///
    /// You may not specify this when [type_if_multipart](`Self::type_if_multipart`) is [`ChecksumType::Composite`].
    pub fn full_object_checksum(mut self, input: impl Into<String>) -> Self {
        self.full_object_checksum = Some(input.into());
        self
    }

    /// TODO: docs
    pub fn build(self) -> Result<ChecksumStrategy, BuildError> {
        let algorithm = self.algorithm.ok_or_else(|| {
            BuildError::missing_field("algorithm", "Checksum algorithm is required")
        })?;

        // Ensure checksum algorithm is something we know about
        match algorithm {
            ChecksumAlgorithm::Crc64Nvme
            | ChecksumAlgorithm::Crc32
            | ChecksumAlgorithm::Crc32C
            | ChecksumAlgorithm::Sha1
            | ChecksumAlgorithm::Sha256 => (),
            _ => {
                return Err(BuildError::invalid_field(
                    "algorithm",
                    format!("Unknown checksum algorithm: {}", algorithm),
                ));
            }
        }

        // If multipart checksum type not specified, default to FullObject, unless a SHA algorithm is being used
        let type_if_multipart = match self.type_if_multipart {
            Some(type_if_multipart) => type_if_multipart,
            None => match &algorithm {
                &ChecksumAlgorithm::Sha1 | &ChecksumAlgorithm::Sha256 => ChecksumType::Composite,
                _ => ChecksumType::FullObject,
            },
        };

        // Validate multipart checksum type
        match (&type_if_multipart, &algorithm) {
            // Check for illegal combinations...
            (&ChecksumType::Composite, &ChecksumAlgorithm::Crc64Nvme)
            | (&ChecksumType::FullObject, &ChecksumAlgorithm::Sha1)
            | (&ChecksumType::FullObject, &ChecksumAlgorithm::Sha256) => {
                return Err(BuildError::invalid_field("type_if_multipart", format!(
                    "`{algorithm}` checksum algorithm does not support `{type_if_multipart}` multipart checksum type"
                )));
            }
            // Anything else is legal...
            (&ChecksumType::Composite, _) | (&ChecksumType::FullObject, _) => (),
            // Unless we don't even recognize the ChecksumType
            (_, _) => {
                return Err(BuildError::invalid_field(
                    "type_if_multipart",
                    format!("Unknown multipart checksum type: {}", type_if_multipart),
                ));
            }
        };

        if self.full_object_checksum.is_some() && &type_if_multipart != &ChecksumType::FullObject {
            return Err(BuildError::invalid_field("full_object_checksum",
                format!("You cannot provide the full object checksum value up front when the multipart checksum type is `{type_if_multipart}` (algorithm is `{algorithm}`)")
            ));
        }

        Ok(ChecksumStrategy {
            algorithm,
            type_if_multipart,
            full_object_checksum: self.full_object_checksum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_algorithm_is_crc64() {
        assert_eq!(
            ChecksumStrategy::default().algorithm,
            ChecksumAlgorithm::Crc64Nvme
        );
    }

    #[test]
    fn test_with_xyz_constructors() {
        // All ChecksumStrategy::with_XYZ() constructors should create something valid
        let strategy = ChecksumStrategy::default();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc64Nvme);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_crc64_nvme("06PGTl8uMFM=");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc64Nvme);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), Some("06PGTl8uMFM="));

        let strategy = ChecksumStrategy::with_crc32("3fRuog==");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), Some("3fRuog=="));

        let strategy = ChecksumStrategy::with_crc32_c("X9v3eA==");
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32C);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), Some("X9v3eA=="));

        let strategy = ChecksumStrategy::with_calculated_crc64_nvme();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc64Nvme);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_crc32();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_crc32_c();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32C);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::FullObject);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_crc32_composite_if_multipart();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::Composite);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_crc32_c_composite_if_multipart();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32C);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::Composite);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_sha1_composite_if_multipart();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Sha1);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::Composite);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::with_calculated_sha256_composite_if_multipart();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Sha256);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::Composite);
        assert_eq!(strategy.full_object_checksum(), None);
    }

    #[test]
    fn test_builder() {
        // Assert that values passed to builder carry through
        let strategy = ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Crc32C) // non-default value
            .type_if_multipart(ChecksumType::Composite) // non-default value
            .build()
            .unwrap();
        assert_eq!(strategy.algorithm(), &ChecksumAlgorithm::Crc32C);
        assert_eq!(strategy.type_if_multipart(), &ChecksumType::Composite);
        assert_eq!(strategy.full_object_checksum(), None);

        let strategy = ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Crc32)
            .full_object_checksum("3fRuog==")
            .build()
            .unwrap();
        assert_eq!(strategy.full_object_checksum(), Some("3fRuog=="));
    }

    #[test]
    fn test_builder_validation() {
        ChecksumStrategy::builder()
            .build()
            .expect_err("Builder requires algorithm");

        ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Crc64Nvme)
            .type_if_multipart(ChecksumType::Composite)
            .build()
            .expect_err("Composite checksums not allowed with CRC64NVME");

        ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Sha1)
            .type_if_multipart(ChecksumType::FullObject)
            .build()
            .expect_err("FullObject checksums not allowed with SHA-1");

        ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Sha256)
            .type_if_multipart(ChecksumType::FullObject)
            .build()
            .expect_err("FullObject checksums not allowed with SHA-256");

        ChecksumStrategy::builder()
            .algorithm(ChecksumAlgorithm::Crc32)
            .type_if_multipart(ChecksumType::Composite)
            .full_object_checksum("3fRuog==")
            .build()
            .expect_err("full_object_checksum values not allowed with Composite");
    }
}
