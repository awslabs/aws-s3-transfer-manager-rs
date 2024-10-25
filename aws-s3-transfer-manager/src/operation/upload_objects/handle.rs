/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::Ordering;

use super::{UploadObjectsContext, UploadObjectsError, UploadObjectsOutput};
use tokio::task;

/// Handle for `UploadObjects` operation
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: UploadObjectsContext,
}

impl UploadObjectsHandle {
    /// Consume the handle and wait for the upload to complete
    pub async fn join(mut self) -> Result<UploadObjectsOutput, UploadObjectsError> {
        while let Some(join_result) = self.tasks.join_next().await {
            join_result.unwrap().unwrap();
        }

        let failed_uploads = self.ctx.state.failed_uploads.lock().unwrap().take();
        let successful_uploads = self.ctx.state.successful_uploads.load(Ordering::SeqCst);
        let total_bytes_transferred = self
            .ctx
            .state
            .total_bytes_transferred
            .load(Ordering::SeqCst);

        let output = UploadObjectsOutput::builder()
            .objects_uploaded(successful_uploads)
            .set_failed_transfers(failed_uploads)
            .total_bytes_transferred(total_bytes_transferred)
            .build();

        Ok(output)
    }
}
