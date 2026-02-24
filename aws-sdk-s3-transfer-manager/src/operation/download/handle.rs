/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::sync::mpsc;

use crate::error::{self, ErrorKind};
use crate::operation::download::body::Body;
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::output::DownloadOutput;
use crate::operation::download::transfer::DownloadTransfer;
use crate::operation::download::ChunkOutput;
use crate::operation::StateMachineTerminalReceiver;

/// Handle to an in-progress download operation.
///
/// This handle is returned when initiating a download and provides access to the
/// downloaded content ([`body`](Self::body)), object metadata ([`object_meta`](Self::object_meta)),
/// and methods to wait for completion ([`join`](Self::join)) or cancel ([`abort`](Self::abort)).
///
/// # Lifecycle
///
/// **Handles should be kept until the transfer completes or is explicitly cancelled.**
/// The recommended pattern is to consume the body and then call [`join`](Self::join):
///
/// ```ignore
/// let handle = tm.download()
///     .bucket("my-bucket")
///     .key("my-key")
///     .initiate()
///     .await?;
///
/// // Consume the body
/// let mut body = handle.body_mut();
/// while let Some(chunk) = body.next().await {
///     let chunk = chunk?;
///     // Process chunk...
/// }
///
/// // Wait for completion and get final result
/// let output = handle.join().await?;
/// ```
///
/// # Cancellation
///
/// The operation can be cancelled either by dropping this handle or by calling
/// [`abort`](Self::abort). Both methods stop the transfer, but they differ in behavior:
///
/// ## Dropping the handle
///
/// When the handle is dropped without calling `join()` or `abort()`:
/// - The transfer is marked as cancelled
/// - Queued work is purged from the scheduler
/// - In-flight work may be interrupted at await points
/// - Drop returns immediately without waiting for in-flight work
///
/// ## Calling `abort()`
///
/// When [`abort`](Self::abort) is called:
/// - The transfer is marked as cancelled
/// - Queued work is purged from the scheduler
/// - Waits for all in-flight work to complete
/// - Returns only after all cleanup is complete
///
/// ## Calling `join()` after failure
///
/// If the download fails, [`join`](Self::join) will cancel any remaining work,
/// wait for in-flight work to complete, and return the error.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadHandle {
    /// The object content
    pub(crate) body: Body,

    /// Download transfer
    pub(crate) transfer: DownloadTransfer,

    /// Completion signal receiver - signals state machine reached terminal state
    pub(crate) completion_rx: Option<StateMachineTerminalReceiver>,
}

impl DownloadHandle {
    pub(crate) fn new(
        transfer: DownloadTransfer,
        chunk_rx: mpsc::Receiver<Result<ChunkOutput, error::Error>>,
        completion_rx: StateMachineTerminalReceiver,
    ) -> Self {
        Self {
            body: Body::new(chunk_rx, transfer.clone()),
            transfer,
            completion_rx: Some(completion_rx),
        }
    }

    /// Object metadata
    ///
    /// Waits for discovery to complete if metadata is not yet available.
    pub async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        // Fast path: already available
        if let Some(meta) = self.transfer.object_meta() {
            return Ok(meta);
        }

        // Wait for discovery to complete
        self.transfer.discovery_notify().notified().await;

        // Check result
        self.transfer.object_meta().ok_or_else(|| {
            if self.transfer.ctx().is_cancelled() {
                error::from_kind(ErrorKind::OperationCancelled)("download cancelled")
            } else {
                error::from_kind(ErrorKind::ObjectNotDiscoverable)("discovery failed")
            }
        })
    }

    /// The object content
    pub fn body(&self) -> &Body {
        &self.body
    }

    /// Mutable reference to the body
    pub fn body_mut(&mut self) -> &mut Body {
        &mut self.body
    }

    /// Wait for the download to complete and return the result.
    ///
    /// This is the authoritative source for transfer errors. Other methods
    /// (`body.next()`, `object_meta()`) return generic errors; call `join()`
    /// to get the actual error with full context.
    ///
    /// If the body has not been fully consumed, this will cancel the transfer
    /// and wait for outstanding work to complete before returning.
    ///
    /// TODO(redux): Currently, in-flight work is only cancelled when join() is called
    /// after a failure. Consider proactively cancelling on failure so resources are
    /// released without requiring user action.
    pub async fn join(mut self) -> Result<DownloadOutput, error::Error> {
        // Close body - we're done consuming (or never started)
        self.body.close();

        // Wait for transfer state machine to reach terminal state
        if let Some(rx) = self.completion_rx.take() {
            // completion_tx is sent when state machine completes (success/failure)
            // RecvError means sender dropped without signaling - treat as failure
            let _ = rx.await;
        }

        let ctx = self.transfer.ctx();
        let id = self.transfer.id();

        if ctx.is_failed() {
            tracing::debug!(ctx = %ctx, "join: cancelling and waiting for idle");
            ctx.handle.scheduler.cancel_transfer(id);
            ctx.handle.scheduler.wait_for_idle(id).await;
            tracing::debug!(ctx = %ctx, "join: idle, returning error");
            // take the actual error (only we should do this)
            let err = ctx.take_error().expect("error taken outside of join()");
            return Err(err);
        }

        // Success - discovery must have completed
        let object_meta = self
            .transfer
            .object_meta()
            .expect("object_meta must be set on successful completion")
            .clone();
        Ok(DownloadOutput::new(object_meta))
    }

    /// Abort the download and cancel any in-progress work.
    ///
    /// When this method returns, all work for this transfer has been
    /// cancelled or completed. No further work will be executed.
    pub async fn abort(mut self) {
        let ctx = self.transfer.ctx();
        let id = self.transfer.id();

        ctx.set_cancelled();
        self.body.close();

        // Cancel transfer and purge queued work
        ctx.handle.scheduler.cancel_transfer(id);

        // Wait for any executing work to complete
        ctx.handle.scheduler.wait_for_idle(id).await;
    }

    /// Get scheduling controls for this transfer.
    ///
    /// See [`SchedulingCtl`](crate::operation::SchedulingCtl) for available controls.
    ///
    /// <div class="warning">
    /// Scheduling controls are an advanced feature.
    /// </div>
    pub fn scheduling(&self) -> crate::operation::SchedulingCtl<'_> {
        self.transfer.ctx().scheduling()
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        let ctx = self.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            ctx.handle.scheduler.cancel_transfer(self.transfer.id());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DownloadHandle;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_handle_properties() {
        is_send::<DownloadHandle>();
        is_sync::<DownloadHandle>();
    }
}
