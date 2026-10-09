/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tracing::Instrument;

use crate::error::{Error, ErrorKind};
use crate::operation::upload::transfer::UploadTransfer;
use crate::operation::upload::UploadOutput;
use crate::transfer::StateMachineTerminalReceiver;
use crate::types::AbortedUpload;
use crate::types::FailedMultipartUploadPolicy;

/// Handle to an in-progress upload operation.
///
/// This handle is returned when initiating an upload and provides methods to wait for
/// completion ([`join`](Self::join)) or cancel the operation ([`abort`](Self::abort)).
///
/// # Lifecycle
///
/// **Handles should be kept until the transfer completes or is explicitly cancelled.**
/// The recommended pattern is to call [`join`](Self::join) to wait for completion:
///
/// ```ignore
/// let handle = tm.upload()
///     .bucket("my-bucket")
///     .key("my-key")
///     .body(stream)
///     .initiate()?;
///
/// // Wait for upload to complete
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
/// - In-flight work is interrupted at its next await point
/// - `AbortMultipartUpload` is **not** called (multipart uploads may be left incomplete)
/// - Drop returns immediately without waiting for in-flight work
///
/// **Warning:** Dropping a handle for a multipart upload without calling `abort()` may
/// leave incomplete multipart uploads on S3. Use S3 lifecycle rules to clean these up,
/// or call `abort()` explicitly.
///
/// ## Calling `abort()`
///
/// When [`abort`](Self::abort) is called:
/// - The transfer is marked as cancelled
/// - Queued work is purged from the scheduler
/// - In-flight work is interrupted; waits for it to stop
/// - Calls `AbortMultipartUpload` if a multipart upload was started
/// - Returns only after all cleanup is complete
///
/// ## Already completed transfers
///
/// If the upload has already completed before the handle is dropped or aborted,
/// the uploaded object will **not** be deleted from S3.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadHandle {
    completion_rx: Option<StateMachineTerminalReceiver>,
    transfer: UploadTransfer,
}

impl UploadHandle {
    pub(crate) fn new(
        completion_rx: StateMachineTerminalReceiver,
        transfer: UploadTransfer,
    ) -> Self {
        Self {
            completion_rx: Some(completion_rx),
            transfer,
        }
    }

    /// Consume the handle and wait for upload to complete.
    ///
    /// Returns [`UploadOutput`] metadata and transfer metrics on success.
    /// Single-request and multipart uploads populate different optional fields;
    /// see [`UploadOutput`] for response origins and completion merge semantics.
    ///
    /// Returns an error
    /// when the transfer failed (with the recorded failure cause) or when
    /// the transfer was cancelled (with `ErrorKind::OperationCancelled`).
    pub async fn join(mut self) -> Result<UploadOutput, Error> {
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
            return Err(err);
        }

        if ctx.is_cancelled() {
            return Err(Error::new(
                ErrorKind::OperationCancelled,
                "upload cancelled",
            ));
        }

        Ok(self
            .transfer
            .take_result()
            .expect("result must be set on successful completion"))
    }

    /// Abort the upload and cancel any in-progress part uploads.
    ///
    /// This will:
    /// 1. Cancel the transfer in the scheduler
    /// 2. Interrupt in-flight work and wait for it to stop
    /// 3. Call AbortMultipartUpload if MPU was started
    ///
    /// When this method returns, all work for this transfer has been
    /// cancelled or completed. No further work will be executed.
    ///
    // TODO(aws-sdk-rust#1159): Handle already completed upload
    pub async fn abort(self) -> Result<AbortedUpload, Error> {
        let ctx = self.transfer.ctx();

        // Cancel the transfer and wait for idle: queued work is purged, and
        // executing work is either finished or dropped by the time this returns.
        //
        // A CreateMultipartUpload dropped mid-flight leaves no upload ID here even
        // if S3 created the upload; that empty upload remains until a lifecycle
        // rule removes it. A CompleteMultipartUpload dropped mid-flight leaves the
        // multipart upload open, so it is aborted below, but S3 may already
        // have created the object (see #191).
        ctx.handle
            .scheduler
            .cancel_transfer(ctx.id)
            .wait_for_idle()
            .await;

        // Check if we have an open multipart upload to abort
        let upload_id = self.transfer.open_upload_id();

        if let Some(upload_id) = upload_id {
            let abort_policy = self
                .transfer
                .request()
                .failed_multipart_upload_policy
                .clone()
                .unwrap_or_default();

            match abort_policy {
                FailedMultipartUploadPolicy::Retain => Ok(AbortedUpload::default()),
                FailedMultipartUploadPolicy::AbortUpload => {
                    let resp = crate::sdk_v1::copy_upload_input_fields_to_abort_multipart_upload(
                        self.transfer.request(),
                        ctx.s3_client()
                            .abort_multipart_upload()
                            .upload_id(&upload_id),
                    )
                    .customize()
                    .config_override(
                        ctx.handle
                            .bucket_partition_override(self.transfer.request().bucket()),
                    )
                    .send()
                    .instrument(tracing::debug_span!("send-abort-multipart-upload"))
                    .await?;

                    Ok(AbortedUpload {
                        upload_id: Some(upload_id),
                        request_charged: resp
                            .request_charged
                            .as_ref()
                            .map(crate::sdk_v1::request_charged_from_sdk),
                    })
                }
            }
        } else {
            Ok(AbortedUpload::default())
        }
    }

    /// Get the transfer ID for this upload.
    pub(crate) fn id(&self) -> crate::transfer::TransferId {
        self.transfer.ctx().id
    }

    /// Get scheduling controls for this transfer.
    pub fn scheduling(&self) -> crate::transfer::SchedulingCtl<'_> {
        self.transfer.ctx().scheduling()
    }

    /// Current status of this transfer.
    pub fn status(&self) -> crate::types::TransferStatus {
        self.transfer.ctx().transfer_status()
    }

    /// Snapshot of current transfer metrics.
    pub fn metrics(&self) -> crate::types::TransferMetrics {
        self.transfer.ctx().metrics()
    }
}

impl Drop for UploadHandle {
    fn drop(&mut self) {
        let ctx = self.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            ctx.handle.scheduler.cancel_transfer(ctx.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UploadHandle;
    use crate::error::ErrorKind;
    use crate::io::InputStream;
    use crate::operation::upload::transfer::UploadTransfer;
    use crate::operation::upload::UploadInput;
    use crate::transfer::TransferContext;
    use crate::types::BucketType;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_handle_properties() {
        is_send::<UploadHandle>();
        is_sync::<UploadHandle>();
    }

    /// Regression: if the transfer reaches a `Cancelled` terminal state
    /// before `join()` is awaited, `join()` must return
    /// `Err(OperationCancelled)`. Previously this path panicked through
    /// `take_result().expect("result must be set on successful completion")`.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_join_returns_cancelled_error_when_transfer_cancelled() {
        let handle = crate::client::Handle::test_handle_tokio(
            crate::Config::builder()
                .sdk_client(aws_smithy_mocks::mock_client!(
                    aws_sdk_s3,
                    aws_smithy_mocks::RuleMode::MatchAny,
                    &[]
                ))
                .build(),
        );
        let input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let stream = InputStream::from(Vec::<u8>::new());
        let (ctx, completion_rx) = TransferContext::new(handle);
        let transfer =
            UploadTransfer::try_new(ctx.clone(), BucketType::Standard, input, stream).unwrap();

        // Drive to Cancelled terminal state.
        ctx.set_cancelled();
        ctx.signal_terminal();

        let upload_handle = UploadHandle::new(completion_rx, transfer);
        let err = upload_handle
            .join()
            .await
            .expect_err("join on cancelled transfer must return Err");
        assert_eq!(err.kind(), &ErrorKind::OperationCancelled);
    }
}
