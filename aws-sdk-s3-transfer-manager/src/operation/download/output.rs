/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::object_meta::ObjectMetadata;
use crate::types::IntegrityChecks;

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
    /// Object-integrity information: the object's reported checksum values and
    /// whether the delivered bytes were checksum-validated.
    integrity_checks: IntegrityChecks,
}

impl DownloadOutput {
    pub(crate) fn new(
        object_meta: ObjectMetadata,
        metrics: crate::types::TransferMetrics,
        integrity_checks: IntegrityChecks,
    ) -> Self {
        Self {
            object_meta,
            metrics,
            integrity_checks,
        }
    }

    /// Object-integrity information for this download: the object's reported
    /// checksum values and whether the delivered bytes were checksum-validated.
    pub fn integrity_checks(&self) -> &IntegrityChecks {
        &self.integrity_checks
    }
}
