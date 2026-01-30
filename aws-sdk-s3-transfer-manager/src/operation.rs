/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fs::Metadata, path::Path, sync::Arc};
use std::sync::Mutex;

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

// Type aliases to channel ends to send/receive cancel notification
pub(crate) type CancelNotificationSender = tokio::sync::watch::Sender<bool>;
pub(crate) type CancelNotificationReceiver = tokio::sync::watch::Receiver<bool>;

/// Error state for a failed transfer
#[derive(Debug)]
pub(crate) enum TransferError {
    /// Error is available
    Err(Box<error::Error>),
    /// Error was already taken
    Taken,
}

/// Transfer lifecycle status
#[derive(Debug)]
pub(crate) enum TransferStatus {
    /// Transfer is actively processing
    Active,
    /// Transfer completed successfully
    Completed,
    /// Transfer failed
    Failed(TransferError),
    /// Transfer was cancelled
    Cancelled,
}

impl Default for TransferStatus {
    fn default() -> Self {
        TransferStatus::Active
    }
}

/// Container for maintaining context required to carry out a single operation/transfer.
///
/// `State` is whatever additional operation specific state is required for the operation.
#[derive(Debug)]
pub(crate) struct TransferContext<State> {
    /// Unique identifier for this transfer
    pub(crate) id: crate::scheduler::TransferId,
    /// Access to client handle (scheduler, S3 client, config)
    pub(crate) handle: Arc<crate::client::Handle>,
    /// Operation-specific state
    pub(crate) state: Arc<State>,
    /// Transfer lifecycle status (Arc for Clone)
    status: Arc<Mutex<TransferStatus>>,
}

impl<State> TransferContext<State> {
    /// Create a new transfer context from pre-built state
    pub(crate) fn from_state(
        id: crate::scheduler::TransferId,
        handle: Arc<crate::client::Handle>,
        state: Arc<State>,
    ) -> Self {
        Self {
            id,
            handle,
            state,
            status: Arc::new(Mutex::new(TransferStatus::Active)),
        }
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }

    /// Mark transfer as failed. First-write-wins - returns true if this call set the status.
    pub(crate) fn set_failed(&self, err: error::Error) -> bool {
        let mut status = self.status.lock().unwrap();
        if matches!(*status, TransferStatus::Active) {
            *status = TransferStatus::Failed(TransferError::Err(Box::new(err)));
            true
        } else {
            tracing::debug!("set_failed called but status already {:?}", *status);
            false
        }
    }

    /// Mark transfer as completed. First-write-wins - returns true if this call set the status.
    pub(crate) fn set_completed(&self) -> bool {
        let mut status = self.status.lock().unwrap();
        if matches!(*status, TransferStatus::Active) {
            *status = TransferStatus::Completed;
            true
        } else {
            tracing::debug!("set_completed called but status already {:?}", *status);
            false
        }
    }

    /// Mark transfer as cancelled. First-write-wins - returns true if this call set the status.
    pub(crate) fn set_cancelled(&self) -> bool {
        let mut status = self.status.lock().unwrap();
        if matches!(*status, TransferStatus::Active) {
            *status = TransferStatus::Cancelled;
            true
        } else {
            tracing::debug!("set_cancelled called but status already {:?}", *status);
            false
        }
    }

    /// Take the error if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn take_error(&self) -> Option<error::Error> {
        let mut status = self.status.lock().unwrap();
        match &mut *status {
            TransferStatus::Failed(transfer_err) => {
                match std::mem::replace(transfer_err, TransferError::Taken) {
                    TransferError::Err(err) => Some(*err),
                    TransferError::Taken => None,
                }
            }
            _ => None,
        }
    }

    /// Peek at the error kind if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn error_kind(&self) -> Option<error::ErrorKind> {
        let status = self.status.lock().unwrap();
        match &*status {
            TransferStatus::Failed(TransferError::Err(err)) => Some(err.kind().clone()),
            _ => None,
        }
    }

    /// Check if transfer has failed
    pub(crate) fn is_failed(&self) -> bool {
        matches!(*self.status.lock().unwrap(), TransferStatus::Failed(_))
    }

    /// Check if transfer was cancelled
    pub(crate) fn is_cancelled(&self) -> bool {
        matches!(*self.status.lock().unwrap(), TransferStatus::Cancelled)
    }
}

impl<State> Clone for TransferContext<State> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            handle: self.handle.clone(),
            state: self.state.clone(),
            status: self.status.clone(),
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
