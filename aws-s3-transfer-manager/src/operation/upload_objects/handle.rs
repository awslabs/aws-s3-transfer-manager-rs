/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{UploadObjectsContext, UploadObjectsOutput};
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
    #[tracing::instrument(skip_all, level = "debug", name = "join-upload-objects-")]
    pub async fn join(mut self) -> Result<UploadObjectsOutput, crate::error::Error> {
        // TODO - Consider implementing more sophisticated error handling such as canceling in-progress transfers
        while let Some(join_result) = self.tasks.join_next().await {
            join_result??;
        }

        Ok(UploadObjectsOutput::from(self.ctx.state.as_ref()))
    }
}
