/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use tracing::Instrument;

use crate::error::Error;
use crate::operation::upload::context::UploadContext;
use crate::operation::upload::input::convert::copy_fields_to_abort_mpu_request;
use crate::operation::upload::transfer::UploadResultReceiver;
use crate::operation::upload::UploadOutput;
use crate::types::AbortedUpload;

/// Response type for a single upload object request.
///
/// # Cancellation
///
/// The operation can be cancelled either by dropping this handle or by calling
/// [`Self::abort`]. In both cases, any ongoing tasks will stop processing future work
/// and will not start processing anything new. However, there are subtle differences in
/// how each method cancels ongoing tasks.
///
/// When the handle is dropped, in-progress tasks are cancelled at their await points,
/// meaning read body tasks may be interrupted mid-processing, or upload parts may be
/// terminated without calling `AbortMultipartUpload` for multipart uploads.
///
/// In contrast, calling [`Self::abort`] explicitly cancels the transfer and calls
/// `AbortMultipartUpload` if a multipart upload was started.
///
/// In either case, if the upload operation has already been completed before the handle is dropped
/// or aborted, the uploaded object will not be deleted from S3.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadHandle {
    result_rx: UploadResultReceiver,
    ctx: UploadContext,
}

impl UploadHandle {
    pub(crate) fn new(result_rx: UploadResultReceiver, ctx: UploadContext) -> Self {
        Self { result_rx, ctx }
    }

    /// Consume the handle and wait for upload to complete
    pub async fn join(self) -> Result<UploadOutput, Error> {
        self.result_rx
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::RuntimeError, "upload cancelled"))?
    }

    /// Abort the upload and cancel any in-progress part uploads.
    ///
    /// This will:
    /// 1. Cancel the transfer in the scheduler (interrupts in-flight work)
    /// 2. Wait for any in-flight CreateMPU to complete
    /// 3. Call AbortMultipartUpload if MPU was started
    ///
    /// TODO(aws-sdk-rust#1159): Handle already completed upload
    pub async fn abort(self) -> Result<AbortedUpload, Error> {
        // TODO(redux): There's an edge case where CreateMPU HTTP request is in-flight,
        // we cancel, but the server still processes the request. The MPU would be
        // orphaned. This is the same behavior as the old implementation. S3 lifecycle
        // rules can clean up incomplete MPUs. To fully fix this would require waiting
        // for the HTTP response even after cancellation.

        // Register waiter before checking state to avoid missing notification
        let notified = self.ctx.state.create_mpu_complete.notified();
        let create_mpu_in_flight = self.ctx.state.is_create_mpu_in_flight();

        // Cancel the transfer (cancels token + removes from scheduler)
        self.ctx.handle.new_scheduler.cancel_transfer(self.ctx.id);

        // If CreateMPU was in flight, wait for it to complete or be cancelled
        if create_mpu_in_flight {
            notified.await;
        }

        // Check if we have an upload_id to abort
        let upload_id = self.ctx.state.upload_id();

        if let Some(upload_id) = upload_id {
            let resp = copy_fields_to_abort_mpu_request(
                self.ctx.state.request(),
                self.ctx
                    .client()
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
}

// TODO(redux): Consider Drop impl for cleanup
// When handle is dropped without join() or abort(), the transfer continues
// running in the scheduler. This may or may not be desired behavior.
