/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{DownloadObjectsOutput, DownloadObjectsTransfer};
use crate::error::{Error, ErrorKind};
use crate::transfer::StateMachineTerminalReceiver;

/// Handle for an in-progress `DownloadObjects` operation.
///
/// Returned when initiating a multi-object download. Provides methods to wait
/// for completion ([`join`](Self::join)) or cancel ([`abort`](Self::abort)).
///
/// # Cancellation
///
/// The operation can be cancelled by calling [`abort`](Self::abort) or by
/// dropping this handle. Dropping cancels immediately without waiting for
/// in-flight child downloads to settle; call `abort()` for a clean shutdown.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadObjectsHandle {
    pub(crate) completion_rx: Option<StateMachineTerminalReceiver>,
    pub(crate) transfer: DownloadObjectsTransfer,
}

impl DownloadObjectsHandle {
    /// Consume the handle and wait for all downloads to complete.
    ///
    /// Returns aggregated output on success. Under
    /// [`FailedTransferPolicy::Abort`](crate::types::FailedTransferPolicy::Abort),
    /// returns the error that triggered the abort. When cancelled, returns
    /// `ErrorKind::OperationCancelled`.
    pub async fn join(mut self) -> Result<DownloadObjectsOutput, Error> {
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
            return Err(err.with_failed_downloads(self.transfer.take_failed().unwrap_or_default()));
        }

        if ctx.is_cancelled() {
            return Err(Error::new(
                ErrorKind::OperationCancelled,
                "download_objects cancelled",
            ));
        }

        let m = ctx.metrics();
        Ok(DownloadObjectsOutput::builder()
            .objects_downloaded(self.transfer.successful_downloads())
            .set_failed_transfers(self.transfer.take_failed().unwrap_or_default())
            .metrics(m)
            .build())
    }

    /// Abort the multi-object download and cancel all in-progress child downloads.
    ///
    /// Waits for any in-flight child downloads to complete or be cancelled
    /// before returning.
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
    pub fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.transfer.ctx().scheduling()
    }
}

impl Drop for DownloadObjectsHandle {
    fn drop(&mut self) {
        let ctx = self.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            ctx.handle.scheduler.cancel_transfer(ctx.id);
        }
    }
}
