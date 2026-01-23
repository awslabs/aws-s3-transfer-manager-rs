/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::Error;
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
/// TODO(redux): Document cancellation behavior with new scheduler model.
/// - When handle is dropped, what happens to in-flight work?
/// - How does scheduler handle orphaned transfers?
/// - Do we need explicit cleanup or does drop suffice?
///
/// When the handle is dropped, in-progress tasks are cancelled at their await points,
/// meaning read body tasks may be interrupted mid-processing, or upload parts may be
/// terminated without calling `AbortMultipartUpload` for multipart uploads.
///
/// In contrast, calling [`Self::abort`] attempts to cancel ongoing tasks more explicitly.
/// It first calls `.abort_all` on the tasks it owns, and then invokes `AbortMultipartUpload`
/// to abort any in-progress multipart uploads. Errors encountered during `AbortMultipartUpload`
/// are logged, but do not affect the overall cancellation flow.
///
/// In either case, if the upload operation has already been completed before the handle is dropped
/// or aborted, the uploaded object will not be deleted from S3.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadHandle {
    result_rx: UploadResultReceiver,
    // TODO(redux): Do we need to hold ctx for abort/cancellation?
    // Old impl held: ctx: UploadContext
    // TODO(redux): Do we need transfer_id to signal cancellation to scheduler?
}

impl UploadHandle {
    pub(crate) fn new(result_rx: UploadResultReceiver) -> Self {
        Self { result_rx }
    }

    /// Consume the handle and wait for upload to complete
    ///
    /// TODO(redux): Re-add tracing span instrumentation
    /// Old: #[tracing::instrument(skip_all, level = "debug", name = "join-upload")]
    ///
    /// TODO(redux): The old impl had this concern:
    /// > We won't send completeMPU until customers join the future. This can create a
    /// > bottleneck where we have many uploads not making the completeMPU call, waiting for the join
    /// > to happen, and then everyone tries to do completeMPU at the same time. We should investigate doing
    /// > this without waiting for join to happen.
    /// With the new scheduler model, CompleteMPU is driven by scheduler, not by join().
    /// Verify this is actually the case and document the new behavior.
    pub async fn join(self) -> Result<UploadOutput, Error> {
        self.result_rx
            .await
            .map_err(|_| Error::new(crate::error::ErrorKind::RuntimeError, "upload cancelled"))?
    }

    /// Abort the upload and cancel any in-progress part uploads.
    ///
    /// TODO(redux): Re-add tracing span instrumentation
    /// Old: #[tracing::instrument(skip_all, level = "debug", name = "abort-upload")]
    ///
    /// TODO(redux): Implement abort with scheduler cancellation (Phase 5)
    /// Old impl did:
    /// 1. Abort initiate_task
    /// 2. For PutObject: abort the task
    /// 3. For MPU: abort read_body_tasks, abort upload_part_tasks, call AbortMultipartUpload
    ///
    /// New impl needs to:
    /// 1. Signal scheduler to cancel this transfer
    /// 2. Wait for in-flight work to complete or be cancelled
    /// 3. Call AbortMultipartUpload if MPU was started
    ///
    /// TODO(aws-sdk-rust#1159): Handle already completed upload
    pub async fn abort(self) -> Result<AbortedUpload, Error> {
        todo!("abort not yet implemented with new scheduler model")
    }
}

// TODO(redux): Consider Drop impl for cleanup
// Old impl relied on JoinHandle drop behavior. New impl may need explicit cleanup.
// impl Drop for UploadHandle {
//     fn drop(&mut self) {
//         // Signal cancellation to scheduler?
//     }
// }
