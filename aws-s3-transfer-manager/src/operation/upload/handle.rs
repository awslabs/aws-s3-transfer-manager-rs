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
use tokio::task;

/// Response type for a single upload object request.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadHandle {
    /// All child multipart upload tasks spawned for this upload
    pub(crate) upload_tasks: Arc<Mutex<task::JoinSet<Result<CompletedPart, crate::error::Error>>>>,
    pub(crate) read_tasks: task::JoinSet<Result<(), crate::error::Error>>,
    /// The context used to drive an upload to completion
    pub(crate) ctx: UploadContext,
    /// The response that will eventually be yielded to the caller.
    response: Option<UploadOutputBuilder>,
}

impl UploadHandle {
    /// Create a new upload handle with the given request context
    pub(crate) fn new(ctx: UploadContext) -> Self {
        Self {
            upload_tasks: Arc::new(Mutex::new(task::JoinSet::new())),
            read_tasks: task::JoinSet::new(),
            ctx,
            response: None,
        }
    }

    /// Set the initial response builder once available
    ///
    /// This is usually after `CreateMultipartUpload` is initiated (or
    /// `PutObject` is invoked for uploads less than the required MPU threshold).
    pub(crate) fn set_response(&mut self, builder: UploadOutputBuilder) {
        if builder.upload_id.is_some() {
            let upload_id = builder.upload_id.clone().expect("upload ID present");
            self.ctx.set_upload_id(upload_id);
        }

        self.response = Some(builder);
    }

    /// Consume the handle and wait for upload to complete
    pub async fn join(self) -> Result<UploadOutput, crate::error::Error> {
        complete_upload(self).await
    }

    /// Abort the upload and cancel any in-progress part uploads.
    pub async fn abort(&mut self) -> Result<AbortedUpload, crate::error::Error> {
        // TODO(aws-sdk-rust#1159) - handle already completed upload

        // cancel in-progress uploads
        self.read_tasks.abort_all();
        while (self.read_tasks.join_next().await).is_some() {}

        let mut tasks = self.upload_tasks.lock().await;
        tasks.abort_all();

        // join all tasks
        while (tasks.join_next().await).is_some() {}

        if !self.ctx.is_multipart_upload() {
            return Ok(AbortedUpload::default());
        }

        let abort_policy = self
            .ctx
            .request
            .failed_multipart_upload_policy
            .clone()
            .unwrap_or_default();

        match abort_policy {
            FailedMultipartUploadPolicy::AbortUpload => abort_upload(self).await,
            FailedMultipartUploadPolicy::Retain => Ok(AbortedUpload::default()),
        }
    }
}

async fn abort_upload(handle: &UploadHandle) -> Result<AbortedUpload, crate::error::Error> {
    let abort_mpu_resp = handle
        .ctx
        .client()
        .abort_multipart_upload()
        .set_bucket(handle.ctx.request.bucket.clone())
        .set_key(handle.ctx.request.key.clone())
        .set_upload_id(handle.ctx.upload_id.clone())
        .set_request_payer(handle.ctx.request.request_payer.clone())
        .set_expected_bucket_owner(handle.ctx.request.expected_bucket_owner.clone())
        .send()
        .await?;

    let aborted_upload = AbortedUpload {
        upload_id: handle.ctx.upload_id.clone(),
        request_charged: abort_mpu_resp.request_charged,
    };

    Ok(aborted_upload)
}

async fn complete_upload(mut handle: UploadHandle) -> Result<UploadOutput, crate::error::Error> {
    if !handle.ctx.is_multipart_upload() {
        todo!("non mpu upload not implemented yet")
    }

    let span = tracing::debug_span!("joining upload", upload_id = handle.ctx.upload_id);
    let _enter = span.enter();

    while let Some(join_result) = handle.read_tasks.join_next().await {
        if let Err(err) = join_result.expect("task completed") {
            //TODO: code duplication? one JoinSet?
            tracing::error!("multipart upload failed, aborting");
            // TODO(aws-sdk-rust#1159) - if cancelling causes an error we want to propagate that in the returned error somehow?
            if let Err(err) = handle.abort().await {
                tracing::error!("failed to abort upload: {}", DisplayErrorContext(err))
            };
            return Err(err);
        }
    }

    let mut all_parts = Vec::new();
    // join all the upload tasks
    let mut tasks = handle.upload_tasks.lock().await;
    while let Some(join_result) = tasks.join_next().await {
        let result = join_result.expect("task completed");
        match result {
            Ok(completed_part) => all_parts.push(completed_part),
            // TODO(aws-sdk-rust#1159, design) - do we want to return first error or collect all errors?
            Err(err) => {
                tracing::error!("multipart upload failed, aborting");
                // TODO(aws-sdk-rust#1159) - if cancelling causes an error we want to propagate that in the returned error somehow?
                drop(tasks);
                if let Err(err) = handle.abort().await {
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
        .set_upload_id(handle.ctx.upload_id.clone())
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
        .await?;

    // set remaining fields from completing the multipart upload
    let resp = handle
        .response
        .take()
        .expect("response set")
        .set_e_tag(complete_mpu_resp.e_tag.clone())
        .set_expiration(complete_mpu_resp.expiration.clone())
        .set_version_id(complete_mpu_resp.version_id.clone());

    tracing::trace!("upload completed successfully");

    Ok(resp.build().expect("valid response"))
}
