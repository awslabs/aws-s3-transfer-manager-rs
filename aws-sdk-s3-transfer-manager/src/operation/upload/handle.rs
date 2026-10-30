/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::{Error, ErrorKind};
use crate::operation::upload::transfer::UploadTransfer;
use crate::operation::upload::UploadOutput;
use crate::transfer::StateMachineTerminalReceiver;
use crate::types::{AbortOutcome, AbortedUpload};

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
/// - No further parts are uploaded, and in-flight part uploads and PutObject
///   are interrupted
/// - In-flight CreateMultipartUpload and CompleteMultipartUpload are not
///   interrupted; their responses decide the outcome
/// - The transfer calls `AbortMultipartUpload` if a multipart upload started and before ending
///   (after CreateMultipartUpload sent and before CompleteMultipartUpload completes)
/// - Returns once the transfer has ended, reporting whether the object was
///   cancelled or already stored in S3 ([`AbortOutcome`](crate::types::AbortOutcome))
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

    /// Consume the handle and abort the upload.
    ///
    /// Asks the transfer to stop and remove the upload from S3 when possible,
    /// then waits for that cleanup to finish. No new parts are uploaded, and
    /// parts or PutObject requests in flight are interrupted.
    ///
    /// What happens depends on how far the upload got:
    ///
    /// - Before CreateMultipartUpload is sent: returns
    ///   [`AbortOutcome::Cancelled`] with no upload ID.
    /// - After CreateMultipartUpload is sent: sends AbortMultipartUpload once
    ///   the upload ID is known. Returns [`AbortOutcome::Cancelled`] with the
    ///   upload ID.
    /// - While CompleteMultipartUpload is in flight: also sends
    ///   AbortMultipartUpload and waits for both. Returns
    ///   [`AbortOutcome::AlreadyCompleted`] if S3 created the object first,
    ///   otherwise [`AbortOutcome::Cancelled`].
    /// - After the object was created: returns
    ///   [`AbortOutcome::AlreadyCompleted`] with the upload output. The object
    ///   is not deleted.
    ///
    /// AbortMultipartUpload is sent at most once, and not at all when the
    /// failed multipart upload policy is to retain the upload.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer failed, before or during the abort.
    /// AbortMultipartUpload is not sent for a transfer that had already
    /// failed, and its multipart upload is left open.
    ///
    /// [`AbortOutcome::Cancelled`]: crate::types::AbortOutcome::Cancelled
    /// [`AbortOutcome::AlreadyCompleted`]: crate::types::AbortOutcome::AlreadyCompleted
    pub async fn abort(mut self) -> Result<AbortedUpload, Error> {
        // TODO: route a caller's abort through the failure path, as a failure
        //   with a cancellation error, and have that path abort the multipart
        //   upload.

        // TODO: check `abort_requested()` here once an abort can already be
        //   running when this is called. Today it cannot: `abort()` consumes
        //   the handle and is the only caller of `request_abort()`. That
        //   changes if `Drop` calls `request_abort()`, if a failure goes
        //   through `request_abort()`, or if the upload can be aborted without
        //   consuming the handle. An abort that is already running should be
        //   awaited and its outcome reported.
        // On a transfer that is no longer active, this changes nothing:
        // `poll_work` returns `Done` before it reads the abort flag.
        self.transfer.request_abort();

        // `signal_terminal()` runs only after every request the transfer sent
        // has finished or been dropped.
        if let Some(rx) = self.completion_rx.take() {
            let _ = rx.await;
        }

        let ctx = self.transfer.ctx();
        if ctx.is_failed() {
            return Err(ctx.take_error().expect("failed transfer must have error"));
        }

        // A stored result means S3 created the object, either before the
        // abort or because Complete or PutObject committed before it took
        // effect.
        if let Some(output) = self.transfer.take_result() {
            return Ok(AbortedUpload {
                outcome: AbortOutcome::AlreadyCompleted,
                output: Some(output),
                ..Default::default()
            });
        }

        // The upload ID and request-charged value are set only when
        // AbortMultipartUpload was sent.
        Ok(self.transfer.take_abort_mpu_output())
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
        let mut input = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(InputStream::from(Vec::<u8>::new()))
            .build()
            .unwrap();
        let stream = input.take_body();
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
