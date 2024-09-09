/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

/// Operation builders
pub mod builders;

mod input;
pub use input::{UploadObjectsInput, UploadObjectsInputBuilder};

mod handle;
pub use handle::UploadObjectsHandle;

mod output;
pub use output::{UploadObjectsOutput, UploadObjectsOutputBuilder};

/// Operation struct for uploading multiple objects to Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct UploadObjects;

impl UploadObjects {
    /// Execute a single `UploadObjects` transfer operation
    pub(crate) async fn orchestrate(
        _handle: Arc<crate::client::Handle>,
        _input: UploadObjectsInput,
    ) -> Result<UploadObjectsHandle, UploadObjectsError> {
        unimplemented!()
    }
}

/// Error type for `UploadObjects` operation
#[non_exhaustive]
#[derive(Debug)]
pub enum UploadObjectsError {}
