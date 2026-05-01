/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::object_meta::ObjectMetadata;

/// Output from a completed download operation.
// FIXME: join() should return a wrapper type that always carries TransferMetrics
// regardless of success/failure. For now, metrics are only available on success.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DownloadOutput {
    /// Object metadata from the download
    pub object_meta: ObjectMetadata,
    /// Snapshot of transfer metrics at completion.
    pub metrics: crate::types::TransferMetrics,
}

impl DownloadOutput {
    pub(crate) fn new(object_meta: ObjectMetadata, metrics: crate::types::TransferMetrics) -> Self {
        Self {
            object_meta,
            metrics,
        }
    }
}
