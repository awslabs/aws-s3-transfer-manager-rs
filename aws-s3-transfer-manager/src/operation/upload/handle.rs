/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use crate::operation::upload::context::UploadContext;
use crate::operation::upload::{UploadOutput, UploadOutputBuilder};
use crate::types::{AbortedUpload, FailedMultipartUploadPolicy};
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tokio::sync::Mutex;
use tokio::task::{self, JoinHandle};
use tracing::Instrument;

#[derive(Debug)]
pub(crate) enum UploadType {
    MultipartUpload(MultipartUploadData),
    PutObject(JoinHandle<Result<UploadOutput, crate::error::Error>>),
}

#[derive(Debug)]
pub(crate) struct MultipartUploadData {
    /// All child multipart upload tasks spawned for this upload
    pub(crate) upload_part_tasks:
        Arc<Mutex<task::JoinSet<Result<CompletedPart, crate::error::Error>>>>,
    /// All child read body tasks spawned for this upload
    pub(crate) read_body_tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The response that will eventually be yielded to the caller.
    pub(crate) response: Option<UploadOutputBuilder>,
    /// the multipart upload ID
    pub(crate) upload_id: String,
}

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
    /// Initial task which determines the upload type
    initiate_task: JoinHandle<Result<UploadType, crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: UploadContext,
}

impl UploadHandle {
    pub(crate) fn new(
        ctx: UploadContext,
        initiate_task: JoinHandle<Result<UploadType, crate::error::Error>>,
    ) -> Self {
        Self { initiate_task, ctx }
    }

    /// Consume the handle and wait for upload to complete
    #[tracing::instrument(skip_all, level = "debug", name = "join-upload")]
    pub async fn join(self) -> Result<UploadOutput, crate::error::Error> {
        // TODO: We won't send completeMPU until customers join the future. This can create a
        // bottleneck where we have many uploads not making the completeMPU call, waiting for the join
        // to happen, and then everyone tries to do completeMPU at the same time. We should investigate doing
        // this without waiting for join to happen.
        complete_upload(self).await
    }

    /// Abort the upload and cancel any in-progress part uploads.
    #[tracing::instrument(skip_all, level = "debug", name = "abort-upload")]
    pub async fn abort(self) -> Result<AbortedUpload, crate::error::Error> {
        // TODO(aws-sdk-rust#1159) - handle already completed upload
        self.initiate_task.abort();
        if let Ok(Ok(upload_type)) = self.initiate_task.await {
            match upload_type {
                UploadType::PutObject(put_object_task) => {
                    put_object_task.abort();
                    let _ = put_object_task.await?;
                    Ok(AbortedUpload::default())
                }
                UploadType::MultipartUpload(mpu_ctx) => {
                    abort_multipart_upload(self.ctx.clone(), mpu_ctx).await
                }
            }
        } else {
            // Nothing to abort since initiate task was not successful.
            Ok(AbortedUpload::default())
        }
    }
}

/// Abort the multipart upload and cancel any in-progress part uploads.
async fn abort_multipart_upload(
    ctx: UploadContext,
    mut mpu_data: MultipartUploadData,
) -> Result<AbortedUpload, crate::error::Error> {
    // cancel in-progress read_body tasks
    mpu_data.read_body_tasks.abort_all();
    while (mpu_data.read_body_tasks.join_next().await).is_some() {}

    // cancel in-progress upload tasks
    let mut tasks = mpu_data.upload_part_tasks.lock().await;
    tasks.abort_all();

    // join all tasks
    while (tasks.join_next().await).is_some() {}

    let abort_policy = ctx
        .request
        .failed_multipart_upload_policy
        .clone()
        .unwrap_or_default();
    match abort_policy {
        FailedMultipartUploadPolicy::Retain => Ok(AbortedUpload::default()),
        FailedMultipartUploadPolicy::AbortUpload => {
            let abort_mpu_resp = ctx
                .client()
                .abort_multipart_upload()
                .set_bucket(ctx.request.bucket.clone())
                .set_key(ctx.request.key.clone())
                .set_upload_id(Some(mpu_data.upload_id.clone()))
                .set_request_payer(ctx.request.request_payer.clone())
                .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone())
                .send()
                .instrument(tracing::debug_span!("send-abort-multipart-upload"))
                .await?;

            let aborted_upload = AbortedUpload {
                upload_id: Some(mpu_data.upload_id),
                request_charged: abort_mpu_resp.request_charged,
            };

            Ok(aborted_upload)
        }
    }
}

async fn complete_upload(handle: UploadHandle) -> Result<UploadOutput, crate::error::Error> {
    let upload_type = handle.initiate_task.await??;
    match upload_type {
        UploadType::PutObject(put_object_task) => put_object_task.await?,
        UploadType::MultipartUpload(mut mpu_data) => {
            while let Some(join_result) = mpu_data.read_body_tasks.join_next().await {
                if let Err(err) = join_result.expect("task completed") {
                    tracing::error!(
                        "multipart upload failed while trying to read the body, aborting"
                    );
                    // TODO(aws-sdk-rust#1159) - if cancelling causes an error we want to propagate that in the returned error somehow?
                    if let Err(err) = abort_multipart_upload(handle.ctx, mpu_data).await {
                        tracing::error!("failed to abort upload: {}", DisplayErrorContext(err))
                    };
                    return Err(err);
                }
            }

            let mut all_parts = Vec::new();
            // join all the upload tasks. We can safely grab the lock since all the read_tasks are done.
            let mut tasks = mpu_data.upload_part_tasks.lock().await;
            while let Some(join_result) = tasks.join_next().await {
                let result = join_result.expect("task completed");
                match result {
                    Ok(completed_part) => all_parts.push(completed_part),
                    // TODO(aws-sdk-rust#1159, design) - do we want to return first error or collect all errors?
                    Err(err) => {
                        tracing::error!("multipart upload failed, aborting");
                        // TODO(aws-sdk-rust#1159) - if cancelling causes an error we want to propagate that in the returned error somehow?
                        drop(tasks);
                        if let Err(err) = abort_multipart_upload(handle.ctx, mpu_data).await {
                            tracing::error!("failed to abort upload: {}", DisplayErrorContext(err))
                        };
                        return Err(err);
                    }
                }
            }

            tracing::trace!("completing multipart upload");

            // parts must be sorted
            all_parts.sort_by_key(|p| p.part_number.expect("part number set"));

            // complete the multipart upload
            let complete_mpu_resp = handle
                .ctx
                .client()
                .complete_multipart_upload()
                .set_bucket(handle.ctx.request.bucket.clone())
                .set_key(handle.ctx.request.key.clone())
                .set_upload_id(Some(mpu_data.upload_id))
                .multipart_upload(
                    CompletedMultipartUpload::builder()
                        .set_parts(Some(all_parts))
                        .build(),
                )
                // TODO(aws-sdk-rust#1159) - implement checksums
                // .set_checksum_crc32()
                // .set_checksum_crc32_c()
                // .set_checksum_sha1()
                // .set_checksum_sha256()
                .set_request_payer(handle.ctx.request.request_payer.clone())
                .set_expected_bucket_owner(handle.ctx.request.expected_bucket_owner.clone())
                .set_sse_customer_algorithm(handle.ctx.request.sse_customer_algorithm.clone())
                .set_sse_customer_key(handle.ctx.request.sse_customer_key.clone())
                .set_sse_customer_key_md5(handle.ctx.request.sse_customer_key_md5.clone())
                .send()
                .instrument(tracing::debug_span!("send-complete-multipart-upload"))
                .await?;

            // set remaining fields from completing the multipart upload
            let resp = mpu_data
                .response
                .take()
                .expect("response set")
                .set_e_tag(complete_mpu_resp.e_tag.clone())
                .set_expiration(complete_mpu_resp.expiration.clone())
                .set_version_id(complete_mpu_resp.version_id.clone());

            tracing::trace!("upload completed successfully");

            Ok(resp.build().expect("valid response"))
        }
    }
}
