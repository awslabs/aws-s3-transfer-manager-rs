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
/// - Queued work is purged and in-flight part uploads and PutObject are
///   interrupted
/// - In-flight CreateMultipartUpload and CompleteMultipartUpload are not
///   interrupted; their responses decide the outcome
/// - The transfer calls `AbortMultipartUpload` if a multipart upload exists
/// - Returns once the transfer has ended, reporting whether the object was
///   created ([`AbortOutcome`](crate::types::AbortOutcome))
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

    /// Abort the upload.
    ///
    /// Asks the transfer to stop and waits for it to end. The transfer itself
    /// sends AbortMultipartUpload and decides the outcome from S3's responses:
    ///
    /// - Before CreateMultipartUpload/PutObject is sent: nothing is sent.
    ///   Returns [`AbortOutcome::Cancelled`].
    /// - CreateMultipartUpload in flight: it is not interrupted. If S3 returns
    ///   an upload ID, that upload is aborted. Returns `Cancelled`.
    /// - Uploading parts: in-flight parts are interrupted and their results
    ///   discarded; the upload is aborted. Returns `Cancelled`.
    /// - CompleteMultipartUpload in flight: it is not interrupted.
    ///   AbortMultipartUpload is sent right away, and Complete's response
    ///   decides. Success returns [`AbortOutcome::AlreadyCompleted`] with the
    ///   output. `NoSuchUpload` means the abort won and returns `Cancelled`.
    ///   Any other Complete error is returned as that error.
    /// - PutObject in flight: it is interrupted (including any retries) and
    ///   returns `Cancelled` without waiting for S3's response. If S3 had
    ///   already received the whole body, the object may exist anyway; check
    ///   with HeadObject if that matters. The Java v2 transfer manager behaves
    ///   the same way.
    /// - Already completed: returns `AlreadyCompleted`. The object is not deleted.
    /// - Already failed: the failure left any multipart upload open, and it is
    ///   aborted now. Returns `Cancelled`.
    ///
    /// Any other error from a request still in flight (including
    /// AbortMultipartUpload itself) is returned as that error, as it would be
    /// from [`join`](Self::join).
    ///
    /// Dropping the returned future (for example under a caller's timeout)
    /// does not stop the abort: the transfer keeps going until it ends, and
    /// only the outcome is lost.
    ///
    /// # What can still be left behind
    ///
    /// - An empty multipart upload, if CreateMultipartUpload's response is lost
    ///   after S3 accepted it.
    /// - The upload and its parts, if a request fails while an abort is
    ///   already running (including AbortMultipartUpload itself failing): the
    ///   transfer fails at once rather than waiting for the abort.
    /// - The upload and its parts, under `FailedMultipartUploadPolicy::Retain`.
    ///
    /// A lifecycle rule for incomplete multipart uploads cleans these up.
    ///
    /// [`AbortOutcome::Cancelled`]: crate::types::AbortOutcome::Cancelled
    /// [`AbortOutcome::AlreadyCompleted`]: crate::types::AbortOutcome::AlreadyCompleted
    pub async fn abort(mut self) -> Result<AbortedUpload, Error> {
        // Whether the transfer had already failed before this call. Read before
        // `request_abort()` so a failure caused during the abort is not mistaken
        // for one that happened earlier.
        let failed_before_abort = self.transfer.ctx().is_failed();
        self.transfer.request_abort();

        // Wait for the transfer's one terminal signal. C view: the receiver is a
        // one-shot condition variable; awaiting it parks this task until the
        // transfer calls `signal_terminal()`.
        if let Some(rx) = self.completion_rx.take() {
            let _ = rx.await;
        }

        let ctx = self.transfer.ctx();
        if ctx.is_failed() {
            // Same as `join()`: let work still in flight drain before returning.
            ctx.handle
                .scheduler
                .cancel_transfer(ctx.id)
                .wait_for_idle()
                .await;
            if failed_before_abort {
                // The caller is cleaning up an upload that already failed:
                // abort whatever multipart upload it left open.
                return self.transfer.abort_upload_left_by_failure().await;
            }
            // The abort was running when a request failed: report that failure.
            return Err(ctx.take_error().expect("failed transfer must have error"));
        }

        // A stored result means S3 created the object, whether that happened
        // before this call, while Complete raced the abort, or because
        // PutObject's response arrived just before it could be interrupted.
        if let Some(output) = self.transfer.take_result() {
            return Ok(AbortedUpload {
                outcome: AbortOutcome::AlreadyCompleted,
                output: Some(output),
                ..Default::default()
            });
        }

        // Cancelled. Carries the upload ID and request-charged value when
        // AbortMultipartUpload was sent; empty otherwise.
        Ok(self.transfer.take_aborted())
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
        // Once `abort()` has started, the transfer finishes the abort on its
        // own. A hard cancel here (for example from a dropped `abort()` future)
        // would drop the in-flight Create/Complete/AbortMultipartUpload it is
        // waiting on.
        if ctx.is_active() && !self.transfer.abort_requested() {
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
