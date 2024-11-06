/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::UploadObjectsState;
use crate::types::FailedUploadTransfer;
use std::sync::atomic::Ordering;

/// Output type for uploading multiple objects
#[non_exhaustive]
#[derive(Debug)]
pub struct UploadObjectsOutput {
    /// The number of objects successfully uploaded
    objects_uploaded: u64,

    /// The list of failed uploads
    failed_transfers: Vec<FailedUploadTransfer>,

    // FIXME - likely remove when progress is implemented (let's be consistent with downloads for now)?
    /// Total number of bytes transferred
    total_bytes_transferred: u64,
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
        self.failed_transfers.as_slice()
    }

    /// The number of bytes successfully transferred (uploaded)
    pub fn total_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred
    }
}

impl From<&UploadObjectsState> for UploadObjectsOutput {
    fn from(state: &UploadObjectsState) -> Self {
        let failed_uploads = std::mem::take(&mut *state.failed_uploads.lock().unwrap());
        let successful_uploads = state.successful_uploads.load(Ordering::SeqCst);
        let total_bytes_transferred = state.total_bytes_transferred.load(Ordering::SeqCst);

        UploadObjectsOutput::builder()
            .objects_uploaded(successful_uploads)
            .set_failed_transfers(failed_uploads)
            .total_bytes_transferred(total_bytes_transferred)
            .build()
    }
}

/// Builder for [`UploadObjectsOutput`]
#[non_exhaustive]
#[derive(Debug, Default)]
pub struct UploadObjectsOutputBuilder {
    pub(crate) objects_uploaded: u64,
    pub(crate) failed_transfers: Vec<FailedUploadTransfer>,
    pub(crate) total_bytes_transferred: u64,
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

    /// Set a list of failed uploads
    pub fn set_failed_transfers(mut self, input: Vec<FailedUploadTransfer>) -> Self {
        self.failed_transfers = input;
        self
    }

    /// The number of bytes successfully transferred (uploaded)
    pub fn total_bytes_transferred(mut self, input: u64) -> Self {
        self.total_bytes_transferred = input;
        self
    }

    /// The number of bytes successfully transferred (uploaded)
    pub fn get_total_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred
    }

    /// Consume the builder and return the output
    pub fn build(self) -> UploadObjectsOutput {
        UploadObjectsOutput {
            objects_uploaded: self.objects_uploaded,
            failed_transfers: self.failed_transfers,
            total_bytes_transferred: self.total_bytes_transferred,
        }
    }
}
