/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::{fs::Metadata, path::Path};

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

// TODO(redux) - why use bool, just use () on cancellation channels.
// Type aliases to channel ends to send/receive cancel notification
pub(crate) type CancelNotificationSender = tokio::sync::watch::Sender<bool>;
pub(crate) type CancelNotificationReceiver = tokio::sync::watch::Receiver<bool>;

/// Completion channel types
pub(crate) type CompletionSender = tokio::sync::oneshot::Sender<()>;
pub(crate) type CompletionReceiver = tokio::sync::oneshot::Receiver<()>;

/// Transfer lifecycle status.
///
/// ## State Machine
///
/// ```text
///              ┌──────────┐
///  (enqueued)──►  Active  │
///              └────┬─────┘
///                   │
///      ┌────────────┼────────────┐
///      │            │            │
///      ▼            ▼            ▼
/// ┌──────────┐ ┌──────────┐ ┌───────────┐
/// │Completed │ │  Failed  │ │ Cancelled │
/// └──────────┘ └──────────┘ └───────────┘
///
/// Terminal states: Completed, Failed, Cancelled
/// ```
///
/// ## Transitions
///
/// - `Active → Completed`: All work finished successfully, output available
/// - `Active → Failed`: A work item failed, error stored in context
/// - `Active → Cancelled`: User requested cancellation or handle dropped
///
/// ## First-Write-Wins Semantics
///
/// Only one transition from `Active` succeeds. Concurrent failures, cancellations,
/// or completion attempts race - the first one wins, others are no-ops.
///
/// ## Terminal State Signaling
///
/// After transitioning to a terminal state, call `signal_terminal()` to notify
/// waiters (e.g., `join()`). This signals "state machine has reached terminal
/// state" - in-flight work may still be completing/draining.
///
/// ## Data Availability
///
/// | Status    | Error Available | Output Available |
/// |-----------|-----------------|------------------|
/// | Active    | No              | No               |
/// | Completed | No              | Yes              |
/// | Failed    | Yes (take once) | No               |
/// | Cancelled | No              | No               |
#[derive(Clone)]
pub(crate) struct StateMachineStatus(Arc<AtomicU8>);

// Status constants
const STATUS_ACTIVE: u8 = 0;
const STATUS_COMPLETED: u8 = 1;
const STATUS_FAILED: u8 = 2;
const STATUS_CANCELLED: u8 = 3;

impl StateMachineStatus {
    /// Create a new status in the Active state
    #[inline]
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(STATUS_ACTIVE)))
    }

    /// Transition to Completed. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_completed(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to Failed. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_failed(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_FAILED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Transition to Cancelled. Returns true if this call made the transition.
    #[inline]
    pub(crate) fn set_cancelled(&self) -> bool {
        self.0
            .compare_exchange(
                STATUS_ACTIVE,
                STATUS_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_ACTIVE
    }

    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_COMPLETED
    }

    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_FAILED
    }

    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire) == STATUS_CANCELLED
    }

    fn as_str(&self) -> &'static str {
        match self.0.load(Ordering::Acquire) {
            STATUS_ACTIVE => "Active",
            STATUS_COMPLETED => "Completed",
            STATUS_FAILED => "Failed",
            STATUS_CANCELLED => "Cancelled",
            _ => "Unknown",
        }
    }
}

impl fmt::Debug for StateMachineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("StateMachineStatus")
            .field(&self.as_str())
            .finish()
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
    /// Transfer lifecycle status
    status: StateMachineStatus,
    /// Error storage (only used when status == Failed)
    error: Arc<Mutex<Option<Box<error::Error>>>>,
    /// Completion signal sender - signals "state machine reached terminal state"
    completion_tx: Arc<Mutex<Option<CompletionSender>>>,
}

impl<State> TransferContext<State> {
    /// Create a new transfer context from pre-built state.
    /// Returns the context and a receiver for terminal state notification.
    pub(crate) fn from_state(
        id: crate::scheduler::TransferId,
        handle: Arc<crate::client::Handle>,
        state: Arc<State>,
    ) -> (Self, CompletionReceiver) {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let ctx = Self {
            id,
            handle,
            state,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
        };
        (ctx, completion_rx)
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }

    /// Mark transfer as failed and store the error.
    /// First-write-wins - returns true if this call set the status.
    pub(crate) fn set_failed(&self, err: error::Error) -> bool {
        if self.status.set_failed() {
            *self.error.lock().unwrap() = Some(Box::new(err));
            true
        } else {
            false
        }
    }

    /// Mark transfer as completed.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_completed(&self) -> bool {
        self.status.set_completed()
    }

    /// Mark transfer as cancelled.
    /// First-write-wins - returns true if this call set the status.
    #[inline]
    pub(crate) fn set_cancelled(&self) -> bool {
        self.status.set_cancelled()
    }

    /// Take the error if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn take_error(&self) -> Option<error::Error> {
        if self.status.is_failed() {
            self.error.lock().unwrap().take().map(|e| *e)
        } else {
            None
        }
    }

    /// Peek at the error kind if transfer failed. Returns None if not failed or already taken.
    pub(crate) fn error_kind(&self) -> Option<error::ErrorKind> {
        if self.status.is_failed() {
            self.error
                .lock()
                .unwrap()
                .as_ref()
                .map(|e| e.kind().clone())
        } else {
            None
        }
    }

    /// Check if transfer has failed
    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.status.is_failed()
    }

    /// Check if transfer was cancelled
    #[inline]
    pub(crate) fn is_cancelled(&self) -> bool {
        self.status.is_cancelled()
    }

    /// Check if transfer is still active (not completed, failed, or cancelled)
    #[inline]
    pub(crate) fn is_active(&self) -> bool {
        self.status.is_active()
    }

    /// Signal that the transfer state machine has reached a terminal state.
    ///
    /// Call this after `set_completed()`/`set_failed()`/`set_cancelled()`.
    /// Wakes any waiters (e.g., `join()`). Note: in-flight work may still
    /// be draining when this is called.
    pub(crate) fn signal_terminal(&self) {
        if let Some(tx) = self.completion_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

impl<State> Clone for TransferContext<State> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            handle: self.handle.clone(),
            state: self.state.clone(),
            status: self.status.clone(),
            error: self.error.clone(),
            completion_tx: self.completion_tx.clone(),
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
