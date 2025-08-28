/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Common types used throughout the S3 mock server.

use aws_smithy_checksums::ChecksumAlgorithm;
use std::collections::HashMap;

/// Client-provided checksums from S3 API requests
#[derive(Debug, Clone)]
pub struct ClientChecksums {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub crc64nvme: Option<String>,
}

impl ClientChecksums {
    pub fn has_any(&self) -> bool {
        self.crc32.is_some()
            || self.crc32c.is_some()
            || self.sha1.is_some()
            || self.sha256.is_some()
            || self.crc64nvme.is_some()
    }
}

impl From<&s3s::dto::PutObjectInput> for ClientChecksums {
    fn from(input: &s3s::dto::PutObjectInput) -> Self {
        Self {
            crc32: input.checksum_crc32.clone(),
            crc32c: input.checksum_crc32c.clone(),
            sha1: input.checksum_sha1.clone(),
            sha256: input.checksum_sha256.clone(),
            crc64nvme: input.checksum_crc64nvme.clone(),
        }
    }
}

impl From<&s3s::dto::UploadPartInput> for ClientChecksums {
    fn from(input: &s3s::dto::UploadPartInput) -> Self {
        Self {
            crc32: input.checksum_crc32.clone(),
            crc32c: input.checksum_crc32c.clone(),
            sha1: input.checksum_sha1.clone(),
            sha256: input.checksum_sha256.clone(),
            crc64nvme: input.checksum_crc64nvme.clone(),
        }
    }
}

impl From<&s3s::dto::CompleteMultipartUploadInput> for ClientChecksums {
    fn from(input: &s3s::dto::CompleteMultipartUploadInput) -> Self {
        Self {
            crc32: input.checksum_crc32.clone(),
            crc32c: input.checksum_crc32c.clone(),
            sha1: input.checksum_sha1.clone(),
            sha256: input.checksum_sha256.clone(),
            crc64nvme: input.checksum_crc64nvme.clone(),
        }
    }
}

/// Configures what integrity checks to perform on streaming data.
#[derive(Default)]
pub struct ObjectIntegrityChecks {
    md5_hasher: Option<md5::Context>,
    checksum_hashers: HashMap<String, Box<dyn aws_smithy_checksums::Checksum>>,
}

impl ObjectIntegrityChecks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_checksum_algorithm(mut self, algorithm: ChecksumAlgorithm) -> Self {
        let algorithm_name = algorithm.as_str();
        if !self.checksum_hashers.contains_key(algorithm_name) {
            self.checksum_hashers
                .insert(algorithm_name.to_string(), algorithm.into_impl());
        }
        self
    }

    pub fn with_md5(mut self) -> Self {
        self.md5_hasher = Some(md5::Context::new());
        self
    }

    pub fn with_crc32(self) -> Self {
        self.with_checksum_algorithm(ChecksumAlgorithm::Crc32)
    }

    pub fn with_crc32c(self) -> Self {
        self.with_checksum_algorithm(ChecksumAlgorithm::Crc32c)
    }

    pub fn with_crc64nvme(self) -> Self {
        self.with_checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
    }

    pub fn with_sha1(self) -> Self {
        self.with_checksum_algorithm(ChecksumAlgorithm::Sha1)
    }

    pub fn with_sha256(self) -> Self {
        self.with_checksum_algorithm(ChecksumAlgorithm::Sha256)
    }

    /// Configure checksums based on client-provided checksums
    pub fn from_client_checksums(client: &ClientChecksums) -> Self {
        let mut checks = Self::new().with_md5();

        if client.crc32.is_some() {
            checks = checks.with_crc32();
        }
        if client.crc32c.is_some() {
            checks = checks.with_crc32c();
        }
        if client.sha1.is_some() {
            checks = checks.with_sha1();
        }
        if client.sha256.is_some() {
            checks = checks.with_sha256();
        }
        if client.crc64nvme.is_some() {
            checks = checks.with_crc64nvme();
        }

        // Default to CRC64NVME if no client checksums
        if !client.has_any() {
            checks = checks.with_crc64nvme();
        }

        checks
    }

    /// Update all configured hash calculations with new data.
    pub fn update(&mut self, data: &[u8]) {
        if let Some(ref mut hasher) = self.md5_hasher {
            hasher.consume(data);
        }

        for hasher in self.checksum_hashers.values_mut() {
            hasher.update(data);
        }
    }

    /// Finalize all calculations and return the results.
    pub fn finalize(mut self) -> ObjectIntegrity {
        let md5_hash = self.md5_hasher.map(|h| format!("\"{:x}\"", h.compute()));

        let mut crc32 = None;
        let mut crc32c = None;
        let mut crc64nvme = None;
        let mut sha1 = None;
        let mut sha256 = None;

        for (algorithm_name, hasher) in self.checksum_hashers.drain() {
            let checksum_bytes = hasher.finalize();
            let checksum_b64 =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &checksum_bytes);

            match algorithm_name.as_str() {
                "crc32" => crc32 = Some(checksum_b64),
                "crc32c" => crc32c = Some(checksum_b64),
                "crc64nvme" => crc64nvme = Some(checksum_b64),
                "sha1" => sha1 = Some(checksum_b64),
                "sha256" => sha256 = Some(checksum_b64),
                _ => {} // Ignore unknown algorithms
            }
        }

        ObjectIntegrity {
            md5_hash,
            crc32,
            crc32c,
            crc64nvme,
            sha1,
            sha256,
        }
    }
}

/// Contains calculated integrity values for an object.
#[derive(Debug, Clone)]
pub struct ObjectIntegrity {
    md5_hash: Option<String>,
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

impl From<&ClientChecksums> for ObjectIntegrityChecks {
    fn from(client: &ClientChecksums) -> Self {
        Self::from_client_checksums(client)
    }
}

impl ObjectIntegrity {
    /// Create an empty ObjectIntegrity with no checksums calculated.
    pub fn empty() -> Self {
        Self {
            md5_hash: None,
            crc32: None,
            crc32c: None,
            crc64nvme: None,
            sha1: None,
            sha256: None,
        }
    }

    /// Returns the ETag (MD5 hash with quotes) if calculated.
    pub fn etag(&self) -> Option<String> {
        self.md5_hash.clone()
    }

    /// Returns the checksum for a specific algorithm if calculated.
    #[allow(dead_code)]
    pub fn checksum(&self, algorithm: ChecksumAlgorithm) -> Option<&String> {
        match algorithm {
            ChecksumAlgorithm::Crc32 => self.crc32.as_ref(),
            ChecksumAlgorithm::Crc32c => self.crc32c.as_ref(),
            ChecksumAlgorithm::Crc64Nvme => self.crc64nvme.as_ref(),
            ChecksumAlgorithm::Sha1 => self.sha1.as_ref(),
            ChecksumAlgorithm::Sha256 => self.sha256.as_ref(),
            _ => None,
        }
    }

    /// Validate against client-provided checksums
    pub fn validate(&self, client: &ClientChecksums) -> Result<(), &'static str> {
        if let Some(expected) = &client.crc32 {
            if self.crc32.as_ref() != Some(expected) {
                return Err("checksum_crc32 mismatch");
            }
        }
        if let Some(expected) = &client.crc32c {
            if self.crc32c.as_ref() != Some(expected) {
                return Err("checksum_crc32c mismatch");
            }
        }
        if let Some(expected) = &client.sha1 {
            if self.sha1.as_ref() != Some(expected) {
                return Err("checksum_sha1 mismatch");
            }
        }
        if let Some(expected) = &client.sha256 {
            if self.sha256.as_ref() != Some(expected) {
                return Err("checksum_sha256 mismatch");
            }
        }
        if let Some(expected) = &client.crc64nvme {
            if self.crc64nvme.as_ref() != Some(expected) {
                return Err("checksum_crc64nvme mismatch");
            }
        }
        Ok(())
    }
}

/// Metadata for a stored object, including integrity information.
#[derive(Debug, Clone)]
pub struct StoredObjectMetadata {
    pub content_length: u64,
    pub object_integrity: ObjectIntegrity,
    pub last_modified: std::time::SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_algorithms() {
        let test_data = b"Hello, World!";

        let mut checks = ObjectIntegrityChecks::new()
            .with_md5()
            .with_crc32()
            .with_crc32c()
            .with_crc64nvme()
            .with_sha1()
            .with_sha256();

        checks.update(test_data);
        let integrity = checks.finalize();

        // Should have all checksums
        assert!(integrity.checksum(ChecksumAlgorithm::Crc32).is_some());
        assert!(integrity.checksum(ChecksumAlgorithm::Crc32c).is_some());
        assert!(integrity.checksum(ChecksumAlgorithm::Crc64Nvme).is_some());
        assert!(integrity.checksum(ChecksumAlgorithm::Sha1).is_some());
        assert!(integrity.checksum(ChecksumAlgorithm::Sha256).is_some());

        // Should have ETag from MD5
        assert!(integrity.etag().is_some());

        // ETag should match expected format
        let expected_etag = format!("\"{:x}\"", md5::compute(test_data));
        assert_eq!(integrity.etag().unwrap(), expected_etag);
    }

    #[test]
    fn test_checksum_calculation() {
        let test_data = b"Hello, World!";

        // Test with CRC32
        let mut checks = ObjectIntegrityChecks::new().with_md5().with_crc32();

        checks.update(test_data);
        let integrity = checks.finalize();

        // Should have MD5 hash (ETag)
        assert!(integrity.etag().is_some());

        // Should have CRC32 checksum
        assert!(integrity.checksum(ChecksumAlgorithm::Crc32).is_some());

        // Should not have SHA256 checksum (not requested)
        assert!(integrity.checksum(ChecksumAlgorithm::Sha256).is_none());
    }

    #[test]
    fn test_multiple_checksums() {
        let test_data = b"Test data for multiple checksums";

        let mut checks = ObjectIntegrityChecks::new().with_crc32().with_sha256();

        checks.update(test_data);
        let integrity = checks.finalize();

        // Should have both checksums
        assert!(integrity.checksum(ChecksumAlgorithm::Crc32).is_some());
        assert!(integrity.checksum(ChecksumAlgorithm::Sha256).is_some());

        // Checksums should be base64 encoded
        let crc32_checksum = integrity.checksum(ChecksumAlgorithm::Crc32).unwrap();
        let sha256_checksum = integrity.checksum(ChecksumAlgorithm::Sha256).unwrap();

        // Should be valid base64
        assert!(
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, crc32_checksum)
                .is_ok()
        );
        assert!(base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            sha256_checksum
        )
        .is_ok());
    }
}
