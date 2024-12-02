/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tokio::task;

use crate::{error::ErrorKind, types::FailedTransferPolicy};

use super::{DownloadObjectsContext, DownloadObjectsOutput};

/// Handle for `DownloadObjects` transfer operation
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadObjectsHandle {
    /// All child tasks spawned for this download
    pub(crate) tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: DownloadObjectsContext,
}

impl DownloadObjectsHandle {
    /// Consume the handle and wait for download transfer to complete
    ///
    /// When the `FailedTransferPolicy` is set to [`FailedTransferPolicy::Abort`], this method
    /// will return the first error if any of the spawned tasks encounter one. The other tasks
    /// will be canceled, but their cancellations will not be reported as errors by this method;
    /// they will be logged as errors, instead.
    ///
    /// If the `FailedTransferPolicy` is set to [`FailedTransferPolicy::Continue`], the
    /// [`DownloadObjectsOutput`] will include a detailed breakdown, including the number of
    /// successful downloads and the number of failed ones.
    ///
    // TODO(aws-sdk-rust#1159) - Consider if we want to return other all errors encountered during cancellation.
    #[tracing::instrument(skip_all, level = "debug", name = "join-download-objects")]
    pub async fn join(mut self) -> Result<DownloadObjectsOutput, crate::error::Error> {
        let mut first_error_to_report = None;
        // join all tasks
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
            Ok(DownloadObjectsOutput::from(self.ctx.state.as_ref()))
        }
    }

    /// Aborts all tasks owned by the handle.
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
