/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::Ordering;

use tokio::task;

use super::{DownloadObjectsContext, DownloadObjectsOutput};

/// Handle for `DownloadObjects` transfer operation
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: DownloadObjectsContext,
}

impl DownloadObjectsHandle {
    /// Consume the handle and wait for download transfer to complete
    #[tracing::instrument(skip_all, level = "debug", name = "download-objects-join")]
    pub async fn join(mut self) -> Result<DownloadObjectsOutput, crate::error::Error> {
        // join all tasks
        while let Some(join_result) = self.tasks.join_next().await {
            join_result??;
        }

        let failed_downloads = self.ctx.state.failed_downloads.lock().unwrap().take();
        let successful_downloads = self.ctx.state.successful_downloads.load(Ordering::SeqCst);
        let total_bytes_transferred = self
            .ctx
            .state
            .total_bytes_transferred
            .load(Ordering::SeqCst);

        let output = DownloadObjectsOutput::builder()
            .objects_downloaded(successful_downloads)
            .set_failed_transfers(failed_downloads)
            .total_bytes_transferred(total_bytes_transferred)
            .build();

        Ok(output)
    }
}
