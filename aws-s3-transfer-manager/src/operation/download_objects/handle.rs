/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

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
    #[tracing::instrument(skip_all, level = "debug", name = "join-download-objects")]
    pub async fn join(mut self) -> Result<DownloadObjectsOutput, crate::error::Error> {
        // TODO - Consider implementing more sophisticated error handling such as canceling in-progress transfers
        // join all tasks
        while let Some(join_result) = self.tasks.join_next().await {
            join_result??;
        }

        Ok(DownloadObjectsOutput::from(self.ctx.state.as_ref()))
    }
}
