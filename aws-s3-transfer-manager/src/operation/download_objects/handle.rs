/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::task;

use super::DownloadObjectsOutput;

/// Handle for `DownloadObjects` transfer operation
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) _tasks: task::JoinSet<()>,
}

impl DownloadObjectsHandle {
    /// Consume the handle and wait for download transfer to complete
    pub async fn join(self) -> Result<DownloadObjectsOutput, crate::error::Error> {
        unimplemented!()
    }
}
