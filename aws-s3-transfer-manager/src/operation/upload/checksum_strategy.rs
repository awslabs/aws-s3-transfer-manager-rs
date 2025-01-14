use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};

use crate::error;

#[doc = std::include_str!("checksum_strategy.md")]
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ChecksumStrategy {
    /// The checksum algorithm to use.
    pub algorithm: ChecksumAlgorithm,

    /// The checksum type to use IF the upload is multipart.
    ///
    /// Note that if the upload is NOT multipart (e.g. object size is below the multipart threshold),
    /// the object's checksum type will always be [`ChecksumType::FullObject`], regardless of this setting.
    pub type_if_multipart: ChecksumType,

    /// The precalculated full object checksum value.
    ///
    /// If specified, this value will be sent to S3 as the full object checksum value.
    /// In the case of a multipart upload, the transfer manager still calculates
    /// checksums for individual parts, but this value will always be sent as the full object checksum value.
    ///
    /// If not specified, the transfer manager will calculate the checksum value.
    ///
    /// You may not specify this when `type_if_multipart` is [`ChecksumType::Composite`].
    pub full_object_checksum: Option<String>,
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

    /// Create checksum strategy from parts.
    ///
    /// It is possible to create an invalid strategy this way,
    /// use [`ChecksumStrategy::validate()`] to check. e.g. S3 does not allow
    /// any `SHA` algorithm to do full object multipart uploads, and does not
    /// allow `CRC64NVME` to do composite multipart uploads.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        type_if_multipart: ChecksumType,
        full_object_checksum: Option<String>,
    ) -> Self {
        Self {
            algorithm,
            type_if_multipart,
            full_object_checksum,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), crate::error::Error> {
        // Ensure multipart checksum type is something we know about
        match self.type_if_multipart {
            ChecksumType::Composite | ChecksumType::FullObject => (),
            _ => {
                return Err(error::invalid_input(format!(
                    "Unknown multipart checksum type: {}",
                    self.type_if_multipart
                )));
            }
        }

        // Ensure checksum algorithm is something we know about, and set whether it only supports one checksum type
        let only_one_supported_multipart_checksum_type = match self.algorithm {
            ChecksumAlgorithm::Crc64Nvme => Some(ChecksumType::FullObject),
            ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32C => None,
            ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => Some(ChecksumType::Composite),
            _ => {
                return Err(error::invalid_input(format!(
                    "Unknown checksum algorithm: {}",
                    self.algorithm
                )));
            }
        };

        // If only one multipart checksum type is supported, make sure it's being used
        if only_one_supported_multipart_checksum_type.is_some_and(|x| x != self.type_if_multipart) {
            return Err(error::invalid_input(format!(
                "`{}` checksum algorithm does not support `{}` multipart checksum type",
                self.algorithm, self.type_if_multipart
            )));
        }

        // Ensure full object checksum value is only being provided when type is FullObject
        if self.full_object_checksum.is_some() && self.type_if_multipart != ChecksumType::FullObject
        {
            return Err(error::invalid_input(
                format!("You cannot provide the full object checksum value up front when the multipart upload type is `{}` (algorithm is `{}`)", self.type_if_multipart, self.algorithm)
            ));
        }

        Ok(())
    }
}

impl Default for ChecksumStrategy {
    /// The transfer manager calculates a `CRC64NVME` full object checksum while uploading.
    fn default() -> Self {
        Self::with_calculated_crc64_nvme()
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
    fn test_validation_ok() {
        // All ChecksumStrategy::with_XYZ() constructors should create something valid
        ChecksumStrategy::default().validate().unwrap();
        ChecksumStrategy::with_crc64_nvme("06PGTl8uMFM=")
            .validate()
            .unwrap();
        ChecksumStrategy::with_crc32("3fRuog==").validate().unwrap();
        ChecksumStrategy::with_crc32_c("X9v3eA==")
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_crc64_nvme()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_crc32()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_crc32_c()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_crc32_composite_if_multipart()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_crc32_c_composite_if_multipart()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_sha1_composite_if_multipart()
            .validate()
            .unwrap();
        ChecksumStrategy::with_calculated_sha256_composite_if_multipart()
            .validate()
            .unwrap();
    }

    #[test]
    fn test_validation_errors() {
        // ChecksumStrategy::new() can create something invalid.
        // Use that to test validate()

        ChecksumStrategy::new(ChecksumAlgorithm::Crc64Nvme, ChecksumType::Composite, None)
            .validate()
            .expect_err("Composite checksums not allowed with CRC64NVME");

        ChecksumStrategy::new(ChecksumAlgorithm::Sha1, ChecksumType::FullObject, None)
            .validate()
            .expect_err("FullObject checksums not allowed with SHA-1");

        ChecksumStrategy::new(ChecksumAlgorithm::Sha256, ChecksumType::FullObject, None)
            .validate()
            .expect_err("FullObject checksums not allowed with SHA-256");

        ChecksumStrategy::new(
            ChecksumAlgorithm::Crc32,
            ChecksumType::Composite,
            Some("3fRuog==".to_string()),
        )
        .validate()
        .expect_err("full_object_checksum values not allowed with Composite");
    }
}
