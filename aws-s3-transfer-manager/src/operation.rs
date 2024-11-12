/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fs::Metadata, path::Path, sync::Arc};

use crate::error;

/// Types for single object upload operation
pub mod upload;

/// Types for single object download operation
pub mod download;

/// Types for multiple object download operation
pub mod download_objects;

/// Types for multiple object upload operation
pub mod upload_objects;

// The default delimiter of the S3 object key
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

// Checks if the target path at `path`, with the provided `metadata`, represents a directory.
//
// The caller is responsible for providing the correct `Metadata`. If the `Metadata` is obtained
// via `fs::metadata`, it can only determine whether the path is a file or a directory, but it cannot
// indicate whether the path is a symbolic link. On the other hand, if `Metadata` is obtained through
// `fs::symlink_metadata`, it can identify symbolic links, but calling `is_dir()` on a symlink will
// return false, even if the symlink points to a directory.
pub(crate) fn validate_target_is_dir(metadata: &Metadata, path: &Path) -> Result<(), error::Error> {
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(error::invalid_input(format!(
            "target is not a directory: {path:?}"
        )))
    }
}
