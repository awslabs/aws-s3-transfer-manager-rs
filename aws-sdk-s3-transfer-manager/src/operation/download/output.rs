/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::object_meta::ObjectMetadata;

/// Output from a completed download operation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DownloadOutput {
    /// Object metadata from the download
    pub object_meta: ObjectMetadata,
}

impl DownloadOutput {
    pub(crate) fn new(object_meta: ObjectMetadata) -> Self {
        Self { object_meta }
    }
}
