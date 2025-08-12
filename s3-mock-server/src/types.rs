/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Common types used throughout the S3 mock server.

/// Configures what integrity checks to perform on streaming data.
#[derive(Default)]
pub struct ObjectIntegrityChecks {
    md5_hasher: Option<md5::Context>,
}

impl ObjectIntegrityChecks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_md5(mut self) -> Self {
        self.md5_hasher = Some(md5::Context::new());
        self
    }

    /// Update all configured hash calculations with new data.
    pub fn update(&mut self, data: &[u8]) {
        if let Some(ref mut hasher) = self.md5_hasher {
            hasher.consume(data);
        }
    }

    /// Finalize all calculations and return the results.
    pub fn finalize(self) -> ObjectIntegrity {
        let md5_hash = self.md5_hasher.map(|h| format!("\"{:x}\"", h.compute()));

        ObjectIntegrity { md5_hash }
    }
}

/// Contains calculated integrity values for an object.
#[derive(Debug, Clone)]
pub struct ObjectIntegrity {
    md5_hash: Option<String>,
}

impl ObjectIntegrity {
    /// Returns the ETag (MD5 hash with quotes) if calculated.
    pub fn etag(&self) -> Option<&String> {
        self.md5_hash.as_ref()
    }
}

/// Metadata for a stored object, including integrity information.
#[derive(Debug, Clone)]
pub struct StoredObjectMetadata {
    pub content_length: u64,
    pub object_integrity: ObjectIntegrity,
    pub last_modified: std::time::SystemTime,
}
