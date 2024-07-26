/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;

mod input;
/// Input type for downloading multiple objects from Amazon S3
pub use input::{DownloadObjectsInput, DownloadObjectsInputBuilder};
mod output;
/// Output type for downloading multiple objects from Amazon S3
pub use output::{DownloadObjectsOutput, DownloadObjectsOutputBuilder};

mod handle;
pub use handle::DownloadObjectsHandle;

use std::sync::Arc;

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation
    pub(crate) async fn orchestrate(
        _handle: Arc<crate::client::Handle>,
        _input: crate::operation::download_objects::DownloadObjectsInput,
    ) -> Result<DownloadObjectsHandle, DownloadObjectsError> {
        unimplemented!()
    }
}

/// Error type for `DownloadObjects` operation
#[non_exhaustive]
#[derive(Debug)]
pub enum DownloadObjectsError {}
