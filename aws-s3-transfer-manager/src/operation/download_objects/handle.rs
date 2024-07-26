/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::task;

/// Response type for dowloading multiple multiple objects
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) _tasks: task::JoinSet<()>,
}
