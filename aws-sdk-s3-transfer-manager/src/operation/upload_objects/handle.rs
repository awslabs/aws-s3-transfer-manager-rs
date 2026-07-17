/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{UploadObjectsOutput, UploadObjectsTransfer};
use crate::error::{Error, ErrorKind};
use crate::transfer::StateMachineTerminalReceiver;

/// Handle for an in-progress `UploadObjects` operation.
///
/// This handle is returned when initiating a multi-object upload and provides methods
/// to wait for completion ([`join`](Self::join)) or cancel the operation
/// ([`abort`](Self::abort)).
///
/// # Lifecycle
///
/// **Handles should be kept until the transfer completes or is explicitly cancelled.**
/// The recommended pattern is to call [`join`](Self::join) to wait for completion:
///
/// ```ignore
/// let handle = tm.upload_objects()
///     .bucket("my-bucket")
///     .source("/path/to/dir")
///     .initiate()?;
///
/// let output = handle.join().await?;
/// println!("uploaded {} objects", output.objects_uploaded());
/// ```
///
/// # Cancellation
///
/// The operation can be cancelled by calling [`abort`](Self::abort) or by dropping
/// this handle.
///
/// ## Calling `abort()`
///
/// When [`abort`](Self::abort) is called:
/// - The transfer is marked as cancelled in the scheduler
/// - Queued child uploads are purged
/// - Waits for all in-flight child uploads to complete or be cancelled
/// - Returns only after all cleanup is complete
///
/// ## Dropping the handle
///
/// When the handle is dropped without calling `join()` or `abort()`:
/// - The transfer is marked as cancelled
/// - Queued work is purged from the scheduler
/// - In-flight child uploads may be interrupted at their next await point
/// - Drop returns immediately without waiting for in-flight work to settle
///
/// Because `Drop` cannot be async, any child uploads that are mid-HTTP-
/// request when the handle is dropped will continue briefly until they
/// reach a cancellation point. For a clean shutdown that waits until all
/// cleanup is complete, call [`abort`](Self::abort) instead of dropping.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadObjectsHandle {
    pub(crate) completion_rx: Option<StateMachineTerminalReceiver>,
    pub(crate) transfer: UploadObjectsTransfer,
}

impl UploadObjectsHandle {
    /// Consume the handle and wait for all uploads to complete.
    ///
    /// Returns the aggregated output on success. When the transfer failed (e.g. a child
    /// upload failed under [`FailedTransferPolicy::Abort`](crate::types::FailedTransferPolicy::Abort)),
    /// returns the error that triggered the abort. When the transfer was cancelled,
    /// returns `ErrorKind::OperationCancelled`.
    pub async fn join(mut self) -> Result<UploadObjectsOutput, Error> {
        if let Some(rx) = self.completion_rx.take() {
            let _ = rx.await;
        }

        let ctx = self.transfer.ctx();

        if ctx.is_failed() {
            ctx.handle
                .scheduler
                .cancel_transfer(ctx.id)
                .wait_for_idle()
                .await;
            let err = ctx.take_error().expect("failed transfer must have error");
            // The per-object failures would otherwise be unreachable on the Err
            // path; attach them so a caller can inspect what failed under Abort.
            return Err(err.with_failed_uploads(self.transfer.take_failed()));
        }

        if ctx.is_cancelled() {
            return Err(Error::new(
                ErrorKind::OperationCancelled,
                "upload_objects cancelled",
            ));
        }

        let m = ctx.metrics();
        Ok(UploadObjectsOutput::builder()
            .objects_uploaded(self.transfer.successful_uploads())
            .set_failed_transfers(self.transfer.take_failed())
            .metrics(m)
            .build())
    }

    /// Abort the multi-object upload and cancel all in-progress child uploads.
    ///
    /// This will:
    /// 1. Cancel the transfer in the scheduler
    /// 2. Wait for any in-flight child uploads to complete or be cancelled
    ///
    /// When this method returns, all work for this transfer has been stopped.
    /// No further child uploads will be initiated.
    pub async fn abort(self) {
        let ctx = self.transfer.ctx();
        ctx.handle
            .scheduler
            .cancel_transfer(ctx.id)
            .wait_for_idle()
            .await;
    }

    /// Current status of this transfer.
    pub fn status(&self) -> crate::types::TransferStatus {
        self.transfer.ctx().transfer_status()
    }

    /// Snapshot of aggregated transfer metrics across every completed child.
    pub fn metrics(&self) -> crate::types::TransferMetrics {
        self.transfer.ctx().metrics()
    }

    /// Get scheduling controls for this transfer.
    ///
    /// Allows adjusting priority relative to other concurrent transfers.
    pub fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.transfer.ctx().scheduling()
    }
}

impl Drop for UploadObjectsHandle {
    fn drop(&mut self) {
        let ctx = self.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            ctx.handle.scheduler.cancel_transfer(ctx.id);
        }
    }
}
