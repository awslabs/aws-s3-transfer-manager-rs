/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{path::Path, sync::Arc};

use tokio::fs;

use crate::error;

/// Types for single object upload operation
pub mod upload;

/// Types for single object download operation
pub mod download;

/// Types for multiple object download operation
pub mod download_objects;

/// Types for multiple object upload operation
pub mod upload_objects;

pub(crate) const DEFAULT_DELIMITER: &str = "/";

/// Container for maintaining context required to carry out a single operation/transfer.
///
/// `State` is whatever additional operation specific state is required for the operation.
#[derive(Debug)]
pub(crate) struct TransferContext<State> {
    handle: Arc<crate::client::Handle>,
    state: Arc<State>,
}

impl<State> TransferContext<State> {
    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }
}

impl<State> Clone for TransferContext<State> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            state: self.state.clone(),
        }
    }
}

pub(crate) async fn validate_target_is_dir(path: &Path) -> Result<(), error::Error> {
    let meta = fs::metadata(path).await?;

    if !meta.is_dir() {
        return Err(error::invalid_input(format!(
            "destination is not a directory: {path:?}"
        )));
    }

    Ok(())
}
