/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;

mod input;
/// Request type for uploads to Amazon S3
pub use input::{DownloadObjectsInput, DownloadObjectsInputBuilder};

mod handle;
pub use handle::DownloadObjectsHandle;

use crate::error::TransferError;
use std::sync::Arc;

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download_objects::DownloadObjectsInput,
    ) -> Result<DownloadObjectsHandle, TransferError> {
        unimplemented!()
    }
}
