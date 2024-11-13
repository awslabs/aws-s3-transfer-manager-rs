/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::ChecksumValidationLevel;

/// Output from downloading an object from Amazon S3
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct DownloadOutput {
    /// The level of checksum validation performed on this download.
    pub checksum_validation_level: ChecksumValidationLevel,
}
