/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tracing::Instrument;

use crate::error::Error;
use crate::operation::upload::input::convert::copy_fields_to_abort_mpu_request;
use crate::operation::upload::transfer::{UploadResultReceiver, UploadTransfer};
use crate::operation::upload::UploadOutput;
use crate::types::AbortedUpload;

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
/// - In-flight work may be interrupted at await points
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
/// - Waits for all in-flight work to complete
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
    result_rx: Option<UploadResultReceiver>,
    transfer: UploadTransfer,
}

impl UploadHandle {
    pub(crate) fn new(result_rx: UploadResultReceiver, transfer: UploadTransfer) -> Self {
        Self {
            result_rx: Some(result_rx),
            transfer,
        }
    }

    /// Consume the handle and wait for upload to complete
    pub async fn join(mut self) -> Result<UploadOutput, Error> {
        let rx = self.result_rx.take().expect("result_rx already taken");
        rx.await
            .map_err(|_| Error::new(crate::error::ErrorKind::RuntimeError, "upload cancelled"))?
    }

    /// Abort the upload and cancel any in-progress part uploads.
    ///
    /// This will:
    /// 1. Cancel the transfer in the scheduler
    /// 2. Wait for any in-flight work to complete
    /// 3. Call AbortMultipartUpload if MPU was started
    ///
    /// When this method returns, all work for this transfer has been
    /// cancelled or completed. No further work will be executed.
    ///
    /// TODO(aws-sdk-rust#1159): Handle already completed upload
    pub async fn abort(self) -> Result<AbortedUpload, Error> {
        let ctx = self.transfer.ctx();

        // Register waiter before checking state to avoid missing notification
        let notified = self.transfer.create_mpu_complete_notified();
        // TODO(redux): There's an edge case where CreateMPU HTTP request is in-flight,
        // we cancel, but the server still processes the request. The MPU would be
        // orphaned. This is the same behavior as the old implementation. S3 lifecycle
        // rules can clean up incomplete MPUs. To fully fix this would require waiting
        // for the HTTP response even after cancellation.
        let create_mpu_in_flight = self.transfer.is_create_mpu_in_flight();

        // Cancel the transfer and purge queued work
        ctx.handle.new_scheduler.cancel_transfer(ctx.id);

        // Wait for any executing work to complete
        ctx.handle.new_scheduler.wait_for_idle(ctx.id).await;

        // If CreateMPU was in flight, wait for it to complete or be cancelled
        if create_mpu_in_flight {
            notified.await;
        }

        // Check if we have an upload_id to abort
        let upload_id = self.transfer.upload_id();

        if let Some(upload_id) = upload_id {
            let resp = copy_fields_to_abort_mpu_request(
                self.transfer.request(),
                ctx.s3_client()
                    .abort_multipart_upload()
                    .upload_id(&upload_id),
            )
            .send()
            .instrument(tracing::debug_span!("send-abort-multipart-upload"))
            .await?;

            Ok(AbortedUpload {
                upload_id: Some(upload_id),
                request_charged: resp.request_charged,
            })
        } else {
            Ok(AbortedUpload::default())
        }
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

impl Drop for UploadHandle {
    fn drop(&mut self) {
        let ctx = self.transfer.ctx();
        if ctx.is_active() {
            ctx.set_cancelled();
            ctx.handle.new_scheduler.cancel_transfer(ctx.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UploadHandle;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_handle_properties() {
        is_send::<UploadHandle>();
        is_sync::<UploadHandle>();
    }
}
