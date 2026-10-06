/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::{FailedDownload, TransferMetrics};

/// Output type for downloading multiple objects.
///
/// Produced by [`DownloadObjectsHandle::join`](crate::operation::download_objects::DownloadObjectsHandle::join)
/// when the transfer reaches a non-error terminal state.
#[non_exhaustive]
#[derive(Debug)]
pub struct DownloadObjectsOutput {
    /// Number of objects successfully downloaded.
    pub objects_downloaded: u64,

    /// Details for each download that failed. Empty on a fully successful transfer.
    pub failed_transfers: Vec<FailedDownload>,

    /// Aggregated metrics across every completed child download.
    pub metrics: TransferMetrics,
}

impl DownloadObjectsOutput {
    /// Creates a new builder.
    pub fn builder() -> DownloadObjectsOutputBuilder {
        DownloadObjectsOutputBuilder::default()
    }

    /// Number of objects successfully downloaded.
    pub fn objects_downloaded(&self) -> u64 {
        self.objects_downloaded
    }

    /// Details for each download that failed.
    pub fn failed_transfers(&self) -> &[FailedDownload] {
        self.failed_transfers.as_slice()
    }

    /// Aggregated transfer metrics.
    pub fn metrics(&self) -> &TransferMetrics {
        &self.metrics
    }
}

/// Builder for [`DownloadObjectsOutput`].
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct DownloadObjectsOutputBuilder {
    pub(crate) objects_downloaded: u64,
    pub(crate) failed_transfers: Vec<FailedDownload>,
    pub(crate) metrics: Option<TransferMetrics>,
}

impl DownloadObjectsOutputBuilder {
    /// Set the number of objects successfully downloaded.
    pub fn objects_downloaded(mut self, input: u64) -> Self {
        self.objects_downloaded = input;
        self
    }

    /// Append a failed transfer.
    pub fn failed_transfers(mut self, input: FailedDownload) -> Self {
        self.failed_transfers.push(input);
        self
    }

    /// Replace the list of failed downloads.
    pub fn set_failed_transfers(mut self, input: Vec<FailedDownload>) -> Self {
        self.failed_transfers = input;
        self
    }

    /// Set the aggregated transfer metrics.
    pub fn metrics(mut self, input: TransferMetrics) -> Self {
        self.metrics = Some(input);
        self
    }

    /// Consume the builder and return the output.
    pub fn build(self) -> DownloadObjectsOutput {
        DownloadObjectsOutput {
            objects_downloaded: self.objects_downloaded,
            failed_transfers: self.failed_transfers,
            metrics: self.metrics.expect("metrics must be set"),
        }
    }
}
