/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::{fs::Metadata, path::Path};

use crate::error;

static NEXT_TRANSFER_ID: AtomicU64 = AtomicU64::new(1);

fn next_transfer_id() -> crate::scheduler::TransferId {
    crate::scheduler::TransferId {
        id: NEXT_TRANSFER_ID.fetch_add(1, Ordering::Relaxed),
        parent: None,
    }
}

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

// Type aliases for state machine completion signal (one-to-one, state machine → handle)
pub(crate) type StateMachineTerminalSender = tokio::sync::oneshot::Sender<()>;
pub(crate) type StateMachineTerminalReceiver = tokio::sync::oneshot::Receiver<()>;

/// Channel for sending download chunks to Body
pub(crate) type ChunkSender = tokio::sync::mpsc::Sender<download::ChunkOutput>;

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

impl fmt::Display for StateMachineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Debug for StateMachineStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("StateMachineStatus")
            .field(&self.as_str())
            .finish()
    }
}

/// Common lifecycle management for transfer state machines.
///
/// Handles status tracking, error storage, completion signaling, and scheduler
/// wake coordination. Each transfer type (upload, download, etc.) owns a
/// `TransferContext` along with its operation-specific state.
///
/// Cheap to clone - all fields are behind `Arc`.
#[derive(Clone)]
pub(crate) struct TransferContext {
    /// Unique identifier for this transfer
    pub(crate) id: crate::scheduler::TransferId,
    /// Access to client handle (scheduler, S3 client, config)
    pub(crate) handle: Arc<crate::client::Handle>,
    /// Transfer lifecycle status
    status: StateMachineStatus,
    /// Error storage (only used when status == Failed)
    error: Arc<Mutex<Option<Box<error::Error>>>>,
    /// Completion signal sender - signals "state machine reached terminal state"
    completion_tx: Arc<Mutex<Option<StateMachineTerminalSender>>>,
    /// Set when poll_work returns Pending, cleared on try_wake
    pending: Arc<std::sync::atomic::AtomicBool>,
    /// Cancellation token for cooperative cancellation
    cancellation_token: tokio_util::sync::CancellationToken,
}

impl fmt::Display for TransferContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Transfer(id={}, status={})", self.id.id, self.status)
    }
}

impl fmt::Debug for TransferContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransferContext")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl TransferContext {
    /// Create a new transfer context.
    /// Returns the context and a receiver for terminal state notification.
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> (Self, StateMachineTerminalReceiver) {
        let id = next_transfer_id();
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let ctx = Self {
            id,
            handle,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        };
        (ctx, completion_rx)
    }

    /// Create a new transfer context with a specific ID (for testing).
    /// Returns the context and a receiver for terminal state notification.
    #[cfg(test)]
    pub(crate) fn with_id(
        id: crate::scheduler::TransferId,
        handle: Arc<crate::client::Handle>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
        let ctx = Self {
            id,
            handle,
            status: StateMachineStatus::new(),
            error: Arc::new(Mutex::new(None)),
            completion_tx: Arc::new(Mutex::new(Some(completion_tx))),
            pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cancellation_token: tokio_util::sync::CancellationToken::new(),
        };
        (ctx, completion_rx)
    }

    /// Mark that poll_work returned Pending. Call while holding state lock.
    #[inline]
    pub(crate) fn set_pending(&self) {
        self.pending
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Wake scheduler if we were pending. Call after state mutation that may unblock.
    #[inline]
    pub(crate) fn try_wake(&self) {
        if self
            .pending
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.handle.scheduler.wake(self.id);
        }
    }

    /// The S3 client to use for SDK operations
    pub(crate) fn s3_client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }

    /// The cancellation token for this transfer
    pub(crate) fn cancellation_token(&self) -> &tokio_util::sync::CancellationToken {
        &self.cancellation_token
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

    /// Get scheduling controls for this transfer.
    pub(crate) fn scheduling(&self) -> SchedulingCtl<'_> {
        SchedulingCtl { ctx: self }
    }
}

/// Scheduling controls for a transfer.
///
/// Provides methods to adjust how this transfer is scheduled relative to others.
/// Obtained via [`UploadHandle::scheduling()`] or [`DownloadHandle::scheduling()`].
///
/// [`UploadHandle::scheduling()`]: crate::operation::upload::UploadHandle::scheduling
/// [`DownloadHandle::scheduling()`]: crate::operation::download::DownloadHandle::scheduling
#[derive(Debug)]
pub struct SchedulingCtl<'a> {
    ctx: &'a TransferContext,
}

impl SchedulingCtl<'_> {
    /// Set the priority of this transfer.
    ///
    /// Priority affects how work is scheduled relative to other transfers:
    /// - Higher priority (255) = more work share, runs more often
    /// - Lower priority (1) = less work share, runs less often
    /// - Default priority is 128
    ///
    /// The scheduler uses CFS-style fair scheduling. Priority affects how fast
    /// "virtual runtime" accumulates - higher priority transfers accumulate slower,
    /// so they stay ahead in scheduling and get more work done.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = tm.download()
    ///     .bucket("my-bucket")
    ///     .key("my-key")
    ///     .initiate()
    ///     .await?;
    ///
    /// // Start as lower priority background transfer
    /// handle.scheduling().set_priority(64);
    ///
    /// // ...
    /// // User requests this data - boost priority
    /// handle.scheduling().set_priority(255);
    /// ```
    pub fn set_priority(&self, priority: u8) {
        self.ctx
            .handle
            .scheduler
            .set_priority(self.ctx.id, priority);
    }
}

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
        self.handle.config.client()
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
