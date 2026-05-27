/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::{self, ErrorKind};
use crate::operation::download::body::{Body, SlotBodyConsumer};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::output::DownloadOutput;
use crate::operation::download::transfer::DownloadTransfer;
use crate::transfer::{StateMachineTerminalReceiver, TransferId};

/// Shared core logic for download handles.
///
/// Contains the transfer and completion state but not the body, allowing
/// reuse across different handle types (e.g. `DownloadHandle`, `ManagedDownloadHandle`).
#[derive(Debug)]
pub(crate) struct DownloadHandleInner {
    pub(crate) transfer: DownloadTransfer,
    pub(crate) completion_rx: Option<StateMachineTerminalReceiver>,
}

impl DownloadHandleInner {
    /// Object metadata
    ///
    /// Waits for discovery to complete if metadata is not yet available.
    pub(crate) async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        // Fast path: already available
        if let Some(meta) = self.transfer.object_meta() {
            return Ok(meta);
        }

        // Register interest before checking again
        let notified = self.transfer.discovery_notify().notified();

        // Double-check after registering
        if let Some(meta) = self.transfer.object_meta() {
            return Ok(meta);
        }

        notified.await;

        // Check result
        self.transfer.object_meta().ok_or_else(|| {
            if self.transfer.ctx().is_cancelled() {
                error::from_kind(ErrorKind::OperationCancelled)("download cancelled")
            } else {
                error::from_kind(ErrorKind::ObjectNotDiscoverable)("discovery failed")
            }
        })
    }

    /// Core join logic: wait for completion, handle failure/cancellation/success.
    pub(crate) async fn join(&mut self) -> Result<DownloadOutput, error::Error> {
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
            ctx.handle
                .scheduler
                .cancel_transfer(id)
                .wait_for_idle()
                .await;
            tracing::debug!(ctx = %ctx, "join: idle, returning error");
            // take the actual error (only we should do this)
            let err = ctx.take_error().expect("error taken outside of join()");
            return Err(err);
        }

        if ctx.is_cancelled() {
            return Err(error::Error::new(
                error::ErrorKind::OperationCancelled,
                "download cancelled",
            ));
        }

        // Success - discovery must have completed
        let object_meta = self
            .transfer
            .object_meta()
            .expect("object_meta must be set on successful completion")
            .clone();
        Ok(DownloadOutput::new(object_meta, ctx.metrics()))
    }

    /// Core abort logic: cancel transfer, notify consumer, and wait for idle.
    pub(crate) async fn abort(&self) {
        let ctx = self.transfer.ctx();
        let id = self.transfer.id();

        ctx.set_cancelled();
        self.transfer.writer().notify_consumer();

        // Cancel transfer (purges queued work) and wait for any executing work to complete.
        ctx.handle
            .scheduler
            .cancel_transfer(id)
            .wait_for_idle()
            .await;
    }

    /// Get scheduling controls for this transfer.
    pub(crate) fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.transfer.ctx().scheduling()
    }
}

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
    /// Shared inner state (transfer + completion)
    pub(crate) inner: DownloadHandleInner,

    /// The object content
    pub(crate) body: Body,
}

impl DownloadHandle {
    pub(crate) fn new(
        transfer: DownloadTransfer,
        consumer: SlotBodyConsumer,
        completion_rx: StateMachineTerminalReceiver,
    ) -> Self {
        let body = Body::new(consumer, transfer.clone());
        Self {
            inner: DownloadHandleInner {
                transfer,
                completion_rx: Some(completion_rx),
            },
            body,
        }
    }

    /// Object metadata
    ///
    /// Waits for discovery to complete if metadata is not yet available.
    pub async fn object_meta(&self) -> Result<&ObjectMetadata, error::Error> {
        self.inner.object_meta().await
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
    /// TODO: Currently, in-flight work is only cancelled when join() is called
    /// after a failure. Consider proactively cancelling on failure so resources are
    /// released without requiring user action.
    pub async fn join(mut self) -> Result<DownloadOutput, error::Error> {
        // Close body - we're done consuming (or never started)
        self.body.close();
        self.inner.join().await
    }

    /// Abort the download and cancel any in-progress work.
    ///
    /// When this method returns, all work for this transfer has been
    /// cancelled or completed. No further work will be executed.
    pub async fn abort(mut self) {
        self.body.close();
        self.inner.abort().await;
    }

    /// Get scheduling controls for this transfer.
    ///
    /// See [`SchedulingCtl`](crate::transfer::SchedulingCtl) for available controls.
    ///
    /// <div class="warning">
    /// Scheduling controls are an advanced feature.
    /// </div>
    pub fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.inner.scheduling()
    }

    /// Current status of this transfer.
    pub fn status(&self) -> crate::types::TransferStatus {
        self.inner.transfer.ctx().transfer_status()
    }

    /// Snapshot of current transfer metrics.
    pub fn metrics(&self) -> crate::types::TransferMetrics {
        self.inner.transfer.ctx().metrics()
    }
}

impl Drop for DownloadHandle {
    fn drop(&mut self) {
        let ctx = self.inner.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            self.inner.transfer.writer().notify_consumer();
            ctx.handle
                .scheduler
                .cancel_transfer(self.inner.transfer.id());
        }
    }
}

/// Handle to an in-progress download-to-file operation.
///
/// Returned by `write_to_path` and similar methods. Unlike [`DownloadHandle`],
/// this handle does not expose a body — the transfer manager writes data
/// directly to disk.
///
/// Data is written to a temporary file (`{dest}.s3tmp.{id}`) during the
/// transfer. On successful completion via [`join`](Self::join), the temporary
/// file is atomically renamed to the destination path. On failure,
/// cancellation, or drop, the temporary file is deleted.
#[derive(Debug)]
pub struct ManagedDownloadHandle {
    inner: DownloadHandleInner,
    temp_path: Option<std::path::PathBuf>,
    dest_path: Option<std::path::PathBuf>,
}

impl ManagedDownloadHandle {
    pub(crate) fn new(
        inner: DownloadHandleInner,
        temp_path: std::path::PathBuf,
        dest_path: std::path::PathBuf,
    ) -> Self {
        Self {
            inner,
            temp_path: Some(temp_path),
            dest_path: Some(dest_path),
        }
    }

    pub(crate) fn new_unmanaged(inner: DownloadHandleInner) -> Self {
        Self {
            inner,
            temp_path: None,
            dest_path: None,
        }
    }

    /// The transfer ID for this child download.
    #[allow(dead_code)] // Used by download_objects state machine (chunk 2 wire-up)
    pub(crate) fn transfer_id(&self) -> TransferId {
        self.inner.transfer.id()
    }

    /// Object metadata.
    ///
    /// Waits for discovery to complete if metadata is not yet available.
    pub async fn object_meta(
        &self,
    ) -> Result<&crate::operation::download::object_meta::ObjectMetadata, error::Error> {
        self.inner.object_meta().await
    }

    /// Wait for the download to complete.
    ///
    /// On success, atomically renames the temporary file to the destination
    /// path. On failure or cancellation, deletes the temporary file.
    pub async fn join(
        mut self,
    ) -> Result<crate::operation::download::output::DownloadOutput, error::Error> {
        let result = self.inner.join().await;

        match &result {
            Ok(_) => {
                if let Err(e) = self.finalize().await {
                    self.cleanup().await;
                    return Err(error::from_kind(error::ErrorKind::IOError)(e));
                }
            }
            Err(_) => {
                self.cleanup().await;
            }
        }

        result
    }

    /// Abort the download and cancel any in-progress work.
    ///
    /// Deletes the temporary file. When this method returns, all work for
    /// this transfer has been cancelled or completed.
    pub async fn abort(self) {
        self.inner.abort().await;
        self.cleanup().await;
    }

    /// Get scheduling controls for this transfer.
    pub fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.inner.scheduling()
    }

    /// Current status of this transfer.
    pub fn status(&self) -> crate::types::TransferStatus {
        self.inner.transfer.ctx().transfer_status()
    }

    /// Snapshot of current transfer metrics.
    pub fn metrics(&self) -> crate::types::TransferMetrics {
        self.inner.transfer.ctx().metrics()
    }

    async fn finalize(&self) -> std::io::Result<()> {
        if let (Some(temp), Some(dest)) = (&self.temp_path, &self.dest_path) {
            // TODO: consider optional fsync before rename for durability guarantees.
            // Without fsync, a crash between rename and OS writeback leaves a corrupt
            // file at the destination. CRT does not fsync. Fsync of 32 GiB adds ~8s.
            tokio::fs::rename(temp, dest).await?;
        }
        Ok(())
    }

    async fn cleanup(&self) {
        if let Some(temp) = &self.temp_path {
            let _ = tokio::fs::remove_file(temp).await;
        }
    }
}

impl Drop for ManagedDownloadHandle {
    fn drop(&mut self) {
        let ctx = self.inner.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            self.inner.transfer.writer().notify_consumer();
            ctx.handle
                .scheduler
                .cancel_transfer(self.inner.transfer.id());
        }
        if let Some(temp) = &self.temp_path {
            let _ = std::fs::remove_file(temp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DownloadHandle, DownloadHandleInner, ManagedDownloadHandle};
    use crate::error::ErrorKind;
    use crate::operation::download::body::{
        new_slot_body, SlotBodyConsumer, DEFAULT_BODY_SLOT_CAPACITY,
    };
    use crate::operation::download::transfer::DownloadTransfer;
    use crate::operation::download::DownloadInput;
    use crate::transfer::TransferContext;
    use crate::types::BucketType;
    use crate::DEFAULT_CONCURRENCY;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_handle_properties() {
        is_send::<DownloadHandle>();
        is_sync::<DownloadHandle>();
        is_send::<ManagedDownloadHandle>();
        is_sync::<ManagedDownloadHandle>();
    }

    /// Build a `DownloadHandleInner` whose `TransferContext` is already at
    /// a `Cancelled` terminal state. Callers assemble either a
    /// `DownloadHandle` or `ManagedDownloadHandle` around it to test the
    /// post-cancel `join()` contract.
    fn make_cancelled_download_inner() -> (DownloadHandleInner, SlotBodyConsumer) {
        let handle = crate::client::Handle::new_for_test(
            crate::Config::builder()
                .client(aws_smithy_mocks::mock_client!(
                    aws_sdk_s3,
                    aws_smithy_mocks::RuleMode::MatchAny,
                    &[]
                ))
                .build(),
            DEFAULT_CONCURRENCY,
        );
        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let (writer, consumer) = new_slot_body(DEFAULT_BODY_SLOT_CAPACITY);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx.clone(), BucketType::Standard, input, writer);

        ctx.set_cancelled();
        ctx.signal_terminal();

        let inner = DownloadHandleInner {
            transfer,
            completion_rx: Some(completion_rx),
        };
        (inner, consumer)
    }

    /// Regression: if the transfer reaches a `Cancelled` terminal state
    /// before `join()` is awaited, `DownloadHandle::join()` must return
    /// `Err(OperationCancelled)`. Previously this path panicked through
    /// `object_meta().expect("object_meta must be set on successful completion")`
    /// (or silently returned `Ok` if discovery had run before cancel).
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_download_join_returns_cancelled_error_when_transfer_cancelled() {
        let (inner, consumer) = make_cancelled_download_inner();
        let body = super::Body::new(consumer, inner.transfer.clone());
        let handle = DownloadHandle { inner, body };
        let err = handle
            .join()
            .await
            .expect_err("join on cancelled transfer must return Err");
        assert_eq!(err.kind(), &ErrorKind::OperationCancelled);
    }

    /// Regression: `ManagedDownloadHandle::join()` on a cancelled transfer
    /// must return `Err(OperationCancelled)` and delete the temp file.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_managed_download_join_returns_cancelled_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.dat");
        let temp = dir.path().join("out.dat.s3tmp.abcdefgh");
        std::fs::write(&temp, b"partial").unwrap();

        let (inner, _consumer) = make_cancelled_download_inner();
        let managed = ManagedDownloadHandle::new(inner, temp.clone(), dest.clone());

        let err = managed
            .join()
            .await
            .expect_err("join on cancelled transfer must return Err");
        assert_eq!(err.kind(), &ErrorKind::OperationCancelled);
        assert!(
            !temp.exists(),
            "temp file must be cleaned up on cancellation"
        );
        assert!(
            !dest.exists(),
            "dest file must not be created on cancellation"
        );
    }
}
