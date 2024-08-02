/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::FailedUploadTransfer;

/// Output type for uploading multiple objects
#[non_exhaustive]
#[derive(Debug)]
pub struct UploadObjectsOutput {
    /// The number of objects successfully uploaded
    pub objects_uploaded: u64,

    /// The list of failed uploads
    pub failed_transfers: Vec<FailedUploadTransfer>,
    // TODO - DownloadObjectsOutput did Option<Vec<>> instead of just Vec<>
}

impl UploadObjectsOutput {
    /// Creates a new builder-style object to manufacture [`UploadObjectsOutput`]
    pub fn builder() -> UploadObjectsOutputBuilder {
        UploadObjectsOutputBuilder::default()
    }

    /// The number of objects successfully uploaded
    pub fn objects_uploaded(&self) -> u64 {
        self.objects_uploaded
    }

    /// The list of failed uploads
    pub fn failed_transfers(&self) -> &[FailedUploadTransfer] {
        &self.failed_transfers
    }
}

/// Builder for [`UploadObjectsOutput`]
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct UploadObjectsOutputBuilder {
    pub(crate) objects_uploaded: u64,
    pub(crate) failed_transfers: Vec<FailedUploadTransfer>,
}

impl UploadObjectsOutputBuilder {
    /// The number of objects successfully uploaded
    pub fn objects_uploaded(mut self, input: u64) -> Self {
        self.objects_uploaded = input;
        self
    }

    /// The number of objects successfully uploaded
    pub fn get_objects_uploaded(&self) -> u64 {
        self.objects_uploaded
    }

    /// Append a failed transfer.
    ///
    /// To override the contents of this collection use [`set_failed_transfers`](Self::set_failed_transfers)
    pub fn failed_transfers(mut self, input: FailedUploadTransfer) -> Self {
        self.failed_transfers.push(input);
        self
    }

    /// The list of any failed uploads
    pub fn set_failed_transfers(mut self, input: Vec<FailedUploadTransfer>) -> Self {
        self.failed_transfers = input;
        self
    }
}
