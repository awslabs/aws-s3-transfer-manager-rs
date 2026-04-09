/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::{Arc, Mutex};
use std::{fs::Metadata, path::Path};

use crate::error;
use crate::transfer::StateMachineStatus;

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

// Type aliases for cancel broadcast channel (one-to-many, handle → workers)
pub(crate) type CancelBroadcastSender = tokio::sync::watch::Sender<()>;
pub(crate) type CancelBroadcastReceiver = tokio::sync::watch::Receiver<()>;

// Keep the old generic version temporarily for migration
// TODO(phase3): Remove after all transfers migrated

/// Container for maintaining context required to carry out a single operation/transfer.
///
/// `State` is whatever additional operation specific state is required for the operation.
///
/// DEPRECATED: Use non-generic `TransferContext` instead. This will be removed.
#[derive(Debug)]
pub(crate) struct LegacyTransferContext<State> {
    /// Access to client handle (scheduler, S3 client, config)
    pub(crate) handle: Arc<crate::client::Handle>,
    /// Operation-specific state
    pub(crate) state: Arc<State>,
    /// Transfer lifecycle status
    status: StateMachineStatus,
    /// Error storage (only used when status == Failed)
    error: Arc<Mutex<Option<Box<error::Error>>>>,
}

impl<State> fmt::Display for LegacyTransferContext<State> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer(status={})", self.status)
    }
}

impl<State> LegacyTransferContext<State> {
    /// Create a new transfer context from pre-built state.
    pub(crate) fn from_state(handle: Arc<crate::client::Handle>, state: Arc<State>) -> Self {
        Self {
            handle,
            state,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
        }
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        &self.handle.s3_client
    }
}

impl<State> Clone for LegacyTransferContext<State> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            state: self.state.clone(),
            status: self.status.clone(),
            error: self.error.clone(),
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
