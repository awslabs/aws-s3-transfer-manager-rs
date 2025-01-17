/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::DownloadObjectsState;
use crate::types::FailedDownload;
use std::sync::atomic::Ordering;

/// Output type for downloading multiple objects
#[non_exhaustive]
#[derive(Debug)]
pub struct DownloadObjectsOutput {
    /// The number of objects that were successfully downloaded
    pub objects_downloaded: u64,

    /// A list of failed object transfers
    pub failed_transfers: Vec<FailedDownload>,

    // FIXME - likely remove when progress is implemented?
    /// Total number of bytes transferred
    pub total_bytes_transferred: u64,
}

impl DownloadObjectsOutput {
    /// Creates a new builder-style object to manufacture [`DownloadObjectsOutput`](crate::operation::download_objects::DownloadObjectsOutput).
    pub fn builder() -> DownloadObjectsOutputBuilder {
        DownloadObjectsOutputBuilder::default()
    }

    /// The number of objects that were successfully downloaded
    pub fn objects_downloaded(&self) -> u64 {
        self.objects_downloaded
    }

    /// A slice of failed object transfers
    pub fn failed_transfers(&self) -> &[FailedDownload] {
        self.failed_transfers.as_slice()
    }

    /// The number of bytes successfully transferred (downloaded)
    pub fn total_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred
    }
}

impl From<&DownloadObjectsState> for DownloadObjectsOutput {
    fn from(state: &DownloadObjectsState) -> Self {
        let failed_downloads = std::mem::take(&mut *state.failed_downloads.lock().unwrap());
        let successful_downloads = state.successful_downloads.load(Ordering::SeqCst);
        let total_bytes_transferred = state.total_bytes_transferred.load(Ordering::SeqCst);

        DownloadObjectsOutput::builder()
            .objects_downloaded(successful_downloads)
            .set_failed_transfers(failed_downloads)
            .total_bytes_transferred(total_bytes_transferred)
            .build()
    }
}

/// A builder for [`DownloadObjectsOutput`](crate::operation::download_objects::DownloadObjectsOutput).
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct DownloadObjectsOutputBuilder {
    pub(crate) objects_downloaded: u64,
    pub(crate) failed_transfers: Vec<FailedDownload>,
    pub(crate) total_bytes_transferred: u64,
}

impl DownloadObjectsOutputBuilder {
    /// The number of objects that were successfully downloaded
    pub fn objects_downloaded(mut self, input: u64) -> Self {
        self.objects_downloaded = input;
        self
    }

    /// The number of objects that were successfully downloaded
    pub fn get_objects_download(&self) -> u64 {
        self.objects_downloaded
    }

    /// Append a failed transfer.
    ///
    /// To override the contents of this collection use
    /// [`set_failed_transfers`](Self::set_failed_transfers)
    pub fn failed_transfers(mut self, input: FailedDownload) -> Self {
        self.failed_transfers.push(input);
        self
    }

    /// Set a list of failed object transfers
    pub fn set_failed_transfers(mut self, input: Vec<FailedDownload>) -> Self {
        self.failed_transfers = input;
        self
    }

    /// Get a list of failed object transfers
    pub fn get_failed_transfers(&self) -> &[FailedDownload] {
        self.failed_transfers.as_slice()
    }

    /// The number of bytes successfully transferred (downloaded)
    pub fn total_bytes_transferred(mut self, input: u64) -> Self {
        self.total_bytes_transferred = input;
        self
    }

    /// The number of bytes successfully transferred (downloaded)
    pub fn get_total_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred
    }

    /// Consume the builder and return the output
    pub fn build(self) -> DownloadObjectsOutput {
        DownloadObjectsOutput {
            objects_downloaded: self.objects_downloaded,
            failed_transfers: self.failed_transfers,
            total_bytes_transferred: self.total_bytes_transferred,
        }
    }
}
