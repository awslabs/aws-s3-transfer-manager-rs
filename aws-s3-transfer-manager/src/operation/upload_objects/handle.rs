/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{error::ErrorKind, types::FailedTransferPolicy};

use super::{UploadObjectsContext, UploadObjectsOutput};
use tokio::task;

/// Handle for `UploadObjects` operation
///
/// # Cancellation
///
/// The operation can be cancelled by either by dropping this handle or by calling
/// [`Self::abort`]. In both cases, any ongoing tasks will ignore future work and
/// will not start processing anything new. However, there are subtle differences
/// in how each cancels ongoing tasks.
///
/// When the handle is dropped, in-progress tasks will be cancelled at the await
/// points where their futures are waiting. This means a particular upload
/// operation may be terminated mid-process, without completing the upload or
/// calling `AbortMultipartUpload` for multipart uploads (if the upload is
/// multipart, as opposed to a simple `PutObject`). In the case of `Drop`,
/// tasks will be forcefully terminated, regardless of the `FailedTransferPolicy`
/// associated with the handle.
///
/// Calling [`Self::abort`], on the other hand, provides more deterministic cancellation
/// behavior. If the `FailedTransferPolicy` for the handle is set to `Abort`, the
/// individual upload task can either complete the current upload operation or call
/// `AbortMultipartUpload` in the case of multipart uploads. Errors encountered during
/// `AbortMultipartUpload` will be logged, but will not affect the program's cancellation
/// control flow.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: UploadObjectsContext,
}

impl UploadObjectsHandle {
    /// Consume the handle and wait for the upload to complete
    ///
    /// When the `FailedTransferPolicy` is set to [`FailedTransferPolicy::Abort`], this method
    /// will return the first error if any of the spawned tasks encounter one. The other tasks
    /// will be canceled, but their cancellations will not be reported as errors by this method;
    /// they will be logged as errors, instead.
    ///
    /// If the `FailedTransferPolicy` is set to [`FailedTransferPolicy::Continue`], the
    /// [`UploadObjectsOutput`] will include a detailed breakdown, such as the number of
    /// successful uploads and the number of failed ones.
    ///
    // TODO(aws-sdk-rust#1159) - Consider if we want to return failed `AbortMultipartUpload` during cancellation.
    #[tracing::instrument(skip_all, level = "debug", name = "join-upload-objects-")]
    pub async fn join(mut self) -> Result<UploadObjectsOutput, crate::error::Error> {
        let mut first_error_to_report = None;
        while let Some(join_result) = self.tasks.join_next().await {
            let result = join_result.expect("task completed");
            if let Err(e) = result {
                match self.ctx.state.input.failure_policy() {
                    FailedTransferPolicy::Abort
                        if first_error_to_report.is_none()
                            && e.kind() != &ErrorKind::OperationCancelled =>
                    {
                        first_error_to_report = Some(e);
                    }
                    FailedTransferPolicy::Continue => {
                        tracing::warn!("encountered but dismissed error when the failure policy is `Continue`: {e}")
                    }
                    _ => {}
                }
            }
        }

        if let Some(e) = first_error_to_report {
            Err(e)
        } else {
            Ok(UploadObjectsOutput::from(self.ctx.state.as_ref()))
        }
    }

    /// Aborts all tasks owned by the handle.
    ///
    /// Unlike `Drop`, calling `abort` gracefully shuts down any in-progress tasks.
    /// Specifically, ongoing upload tasks will be allowed to complete their current work,
    /// but any future uploads will be ignored. The task of listing directory contents will
    /// stop yielding new directory contents.
    pub async fn abort(&mut self) -> Result<(), crate::error::Error> {
        if self.ctx.state.input.failure_policy() == &FailedTransferPolicy::Abort {
            if self.ctx.state.cancel_tx.send(true).is_err() {
                tracing::warn!(
                    "all receiver ends have been dropped, unable to send a cancellation signal"
                );
            }
            while (self.tasks.join_next().await).is_some() {}
        }

        Ok(())
    }
}
