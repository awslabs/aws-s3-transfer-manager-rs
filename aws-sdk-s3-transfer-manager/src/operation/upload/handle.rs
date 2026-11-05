/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::{Error, ErrorKind};
use crate::operation::upload::transfer::{UploadResult, UploadTransfer};
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
/// # Custom stream bodies
///
/// When the upload fails or is cancelled, it drops a body created with
/// [`InputStream::from_part_stream`](crate::io::InputStream::from_part_stream) without waiting for
/// this handle to be dropped. A producer feeding the stream can therefore observe the end of the
/// upload through the stream's own drop, for example a channel's sender returning an error, while
/// the caller keeps the handle to learn the outcome from [`join`](Self::join). Work already holding
/// the stream at that moment, such as a poll in progress, drops it when that work stops. On
/// success, the stream is dropped after the upload completes.
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
/// - The ID of a multipart upload left open is logged at `WARN`
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
///   cancelled or already stored in S3 ([`AbortOutcome`])
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

        match self.transfer.take_result() {
            Some(UploadResult::Completed(output)) => Ok(output),
            other => unreachable!("result must be set on successful completion instead {other:?}"),
        }
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
    /// failed multipart upload policy is to retain the upload. In that case a
    /// multipart upload that was created is left open and this returns
    /// [`AbortOutcome::Retained`] with its upload ID, unless S3 created the
    /// object first.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer failed, before or during the abort.
    /// A failure aborts the multipart upload itself (unless the policy is to
    /// retain it), so this does not send a second AbortMultipartUpload.
    ///
    /// [`AbortOutcome::Cancelled`]: crate::types::AbortOutcome::Cancelled
    /// [`AbortOutcome::AlreadyCompleted`]: crate::types::AbortOutcome::AlreadyCompleted
    /// [`AbortOutcome::Retained`]: crate::types::AbortOutcome::Retained
    pub async fn abort(mut self) -> Result<AbortedUpload, Error> {
        // A failure may have started the abort already. `request_abort()` is
        // idempotent, so this then changes nothing, and the wait below
        // reports the failure. On a transfer that is no longer active it
        // leaves the state alone too.
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

        Ok(match self.transfer.take_result() {
            // S3 created the object, either before the abort or because
            // Complete or PutObject committed before it took effect.
            Some(UploadResult::Completed(output)) => AbortedUpload {
                outcome: AbortOutcome::AlreadyCompleted,
                output: Some(output),
                ..Default::default()
            },
            // AbortMultipartUpload succeeded (`Cancelled`), or the policy is
            // `Retain` and the upload was left open (`Retained`). Both carry
            // the upload ID.
            Some(UploadResult::Aborted(aborted)) => aborted,
            // No multipart upload existed: the abort came before Create was
            // sent, or stopped a PutObject.
            None => AbortedUpload {
                outcome: AbortOutcome::Cancelled,
                ..Default::default()
            },
        })
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
