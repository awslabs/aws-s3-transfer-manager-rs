/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use super::ChecksumValidationLevel;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
/// Metadata that isn't available until the download completes.
pub struct TrailingMetadata {
    /// The level of checksum validation performed on this download.
    pub checksum_validation_level: ChecksumValidationLevel,
}

pub(crate) type TrailingMetadataOnceLock = Arc<OnceLock<TrailingMetadata>>;

impl TrailingMetadata {
    pub(crate) fn new_oncelock() -> TrailingMetadataOnceLock {
        Arc::new(OnceLock::new())
    }
}
