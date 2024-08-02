/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::FailedDownloadTransfer;

/// Output type for downloading multiple objects
#[non_exhaustive]
#[derive(Debug)]
pub struct DownloadObjectsOutput {
    /// The number of objects that were successfully downloaded
    pub objects_downloaded: u64,

    /// A list of failed object transfers
    pub failed_transfers: Option<Vec<FailedDownloadTransfer>>,
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
    ///
    /// If no value was sent for this field, a default will be set. If you want to determine if no value was
    /// set, use `.failed_transfers.is_none()`
    pub fn failed_transfers(&self) -> &[FailedDownloadTransfer] {
        self.failed_transfers.as_deref().unwrap_or_default()
    }
}

/// A builder for [`DownloadObjectsOutput`](crate::operation::download_objects::DownloadObjectsOutput).
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct DownloadObjectsOutputBuilder {
    pub(crate) objects_downloaded: u64,
    pub(crate) failed_transfers: Option<Vec<FailedDownloadTransfer>>,
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
    pub fn failed_transfers(mut self, input: FailedDownloadTransfer) -> Self {
        let mut v = self.failed_transfers.unwrap_or_default();
        v.push(input);
        self.failed_transfers = Some(v);
        self
    }

    /// A list of failed object transfers
    pub fn set_failed_transfers(mut self, input: Option<Vec<FailedDownloadTransfer>>) -> Self {
        self.failed_transfers = input;
        self
    }

    /// A list of failed object transfers
    pub fn get_failed_transfers(&self) -> &Option<Vec<FailedDownloadTransfer>> {
        &self.failed_transfers
    }
}