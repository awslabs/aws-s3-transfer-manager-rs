/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{UploadObjectsError, UploadObjectsOutput};

/// Handle for `UploadObjects` operation
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadObjectsHandle {}

impl UploadObjectsHandle {
    /// Consume the handle and wait for the upload to complete
    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn join(self) -> Result<UploadObjectsOutput, UploadObjectsError> {
        unimplemented!()
    }
}
