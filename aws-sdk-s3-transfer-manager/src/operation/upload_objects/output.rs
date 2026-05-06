/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::{FailedUpload, TransferMetrics};

/// Output type for uploading multiple objects.
#[non_exhaustive]
#[derive(Debug)]
pub struct UploadObjectsOutput {
    /// Number of objects successfully uploaded.
    pub objects_uploaded: u64,

    /// Details for each upload that failed. Empty on a fully successful transfer.
    pub failed_transfers: Vec<FailedUpload>,

    /// Aggregated metrics across every completed child upload: network and
    /// disk byte counters plus start/finish timestamps. See
    /// [`TransferMetrics`] for the full set of fields.
    pub metrics: TransferMetrics,
}

impl UploadObjectsOutput {
    /// Creates a new builder-style object to manufacture an [`UploadObjectsOutput`].
    pub fn builder() -> UploadObjectsOutputBuilder {
        UploadObjectsOutputBuilder::default()
    }

    /// Number of objects successfully uploaded.
    pub fn objects_uploaded(&self) -> u64 {
        self.objects_uploaded
    }

    /// Details for each upload that failed. Empty on a fully successful transfer.
    pub fn failed_transfers(&self) -> &[FailedUpload] {
        self.failed_transfers.as_slice()
    }

    /// Aggregated transfer metrics across all completed child uploads.
    pub fn metrics(&self) -> &TransferMetrics {
        &self.metrics
    }
}

/// Builder for [`UploadObjectsOutput`].
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct UploadObjectsOutputBuilder {
    pub(crate) objects_uploaded: u64,
    pub(crate) failed_transfers: Vec<FailedUpload>,
    pub(crate) metrics: Option<TransferMetrics>,
}

impl UploadObjectsOutputBuilder {
    /// Set the number of objects successfully uploaded.
    pub fn objects_uploaded(mut self, input: u64) -> Self {
        self.objects_uploaded = input;
        self
    }

    /// Get the number of objects successfully uploaded.
    pub fn get_objects_uploaded(&self) -> u64 {
        self.objects_uploaded
    }

    /// Append a failed transfer. Use [`set_failed_transfers`](Self::set_failed_transfers)
    /// to replace the whole collection.
    pub fn failed_transfers(mut self, input: FailedUpload) -> Self {
        self.failed_transfers.push(input);
        self
    }

    /// Replace the list of failed uploads.
    pub fn set_failed_transfers(mut self, input: Vec<FailedUpload>) -> Self {
        self.failed_transfers = input;
        self
    }

    /// Set the aggregated transfer metrics.
    pub fn metrics(mut self, input: TransferMetrics) -> Self {
        self.metrics = Some(input);
        self
    }

    /// Consume the builder and return the output. Panics if `metrics` has
    /// not been set.
    pub fn build(self) -> UploadObjectsOutput {
        UploadObjectsOutput {
            objects_uploaded: self.objects_uploaded,
            failed_transfers: self.failed_transfers,
            metrics: self.metrics.expect("metrics must be set"),
        }
    }
}
