/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;
mod input;
mod output;

mod context;
mod handle;
mod service;

use crate::error;
use crate::io::InputStream;
use aws_sdk_s3::primitives::ByteStream;
use context::UploadContext;
pub use handle::UploadHandle;
/// Request type for uploads to Amazon S3
pub use input::{UploadInput, UploadInputBuilder};
/// Response type for uploads to Amazon S3
pub use output::{UploadOutput, UploadOutputBuilder};
use service::distribute_work;

use std::cmp;
use std::sync::Arc;

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
pub(crate) struct Upload;

impl Upload {
    /// Execute a single `Upload` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        let min_mpu_threshold = handle.mpu_threshold_bytes();

        let stream = input.take_body();
        let ctx = new_context(handle, input);

        // MPU has max of 10K parts which requires us to know the upper bound on the content length (today anyway)
        let content_length = stream
            .size_hint()
            .upper()
            .ok_or_else(crate::io::error::Error::upper_bound_size_hint_required)?;

        let handle = if content_length < min_mpu_threshold {
            tracing::trace!("upload request content size hint ({content_length}) less than min part size threshold ({min_mpu_threshold}); sending as single PutObject request");
            try_start_put_object(ctx, stream, content_length).await?
        } else {
            try_start_mpu_upload(ctx, stream, content_length).await?
        };

        Ok(handle)
    }
}

async fn try_start_put_object(
    ctx: UploadContext,
    stream: InputStream,
    content_length: u64,
) -> Result<UploadHandle, crate::error::Error> {
    let byte_stream = stream.into_byte_stream().await?;
    let content_length: i64 = content_length.try_into().map_err(|_| {
        error::invalid_input(format!("content_length:{} is invalid.", content_length))
    })?;

    Ok(UploadHandle::new_put_object(
        ctx.clone(),
        tokio::spawn(put_object(ctx.clone(), byte_stream, content_length)),
    ))
}

async fn put_object(
    ctx: UploadContext,
    body: ByteStream,
    content_length: i64,
) -> Result<UploadOutput, error::Error> {
    // FIXME - This affects performance in a lot of small file case in ram workload. We need a way to schedule more
    // work for a lot of small files.
    let _permit = ctx.handle.scheduler.acquire_permit().await.unwrap();
    // TODO: add all the fields
    let resp = ctx
        .client()
        .put_object()
        .set_bucket(ctx.request.bucket.clone())
        .set_key(ctx.request.key.clone())
        .content_length(content_length)
        .body(body)
        .set_sse_customer_algorithm(ctx.request.sse_customer_algorithm.clone())
        .set_sse_customer_key(ctx.request.sse_customer_key.clone())
        .set_sse_customer_key_md5(ctx.request.sse_customer_key_md5.clone())
        .set_request_payer(ctx.request.request_payer.clone())
        .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone())
        .send()
        .await?;
    let upload_output: UploadOutputBuilder = resp.into();
    Ok(upload_output.build()?)
}

/// Start a multipart upload
///
/// # Arguments
///
/// * `handle` - The upload handle
/// * `stream` - The content to upload
/// * `content_length` - The upper bound on the content length
async fn try_start_mpu_upload(
    ctx: UploadContext,
    stream: InputStream,
    content_length: u64,
) -> Result<UploadHandle, crate::error::Error> {
    let part_size = cmp::max(
        ctx.handle.upload_part_size_bytes(),
        content_length / MAX_PARTS,
    );
    tracing::trace!("upload request using multipart upload with part size: {part_size} bytes");

    let mpu = start_mpu(&ctx).await?;
    tracing::trace!(
        "multipart upload started with upload id: {:?}",
        mpu.upload_id
    );

    let mut handle = UploadHandle::new_multipart(ctx);
    handle.set_response(mpu);
    distribute_work(&mut handle, stream, part_size)?;
    Ok(handle)
}

fn new_context(handle: Arc<crate::client::Handle>, req: UploadInput) -> UploadContext {
    UploadContext {
        handle,
        request: Arc::new(req),
        upload_id: None,
    }
}

/// start a new multipart upload by invoking `CreateMultipartUpload`
async fn start_mpu(ctx: &UploadContext) -> Result<UploadOutputBuilder, crate::error::Error> {
    let req = ctx.request();
    let client = ctx.client();

    let resp = client
        .create_multipart_upload()
        .set_acl(req.acl.clone())
        .set_bucket(req.bucket.clone())
        .set_cache_control(req.cache_control.clone())
        .set_content_disposition(req.content_disposition.clone())
        .set_content_encoding(req.content_encoding.clone())
        .set_content_language(req.content_language.clone())
        .set_content_type(req.content_type.clone())
        .set_expires(req.expires)
        .set_grant_full_control(req.grant_full_control.clone())
        .set_grant_read(req.grant_read.clone())
        .set_grant_read_acp(req.grant_read_acp.clone())
        .set_grant_write_acp(req.grant_write_acp.clone())
        .set_key(req.key.clone())
        .set_metadata(req.metadata.clone())
        .set_server_side_encryption(req.server_side_encryption.clone())
        .set_storage_class(req.storage_class.clone())
        .set_website_redirect_location(req.website_redirect_location.clone())
        .set_sse_customer_algorithm(req.sse_customer_algorithm.clone())
        .set_sse_customer_key(req.sse_customer_key.clone())
        .set_sse_customer_key_md5(req.sse_customer_key_md5.clone())
        .set_ssekms_key_id(req.sse_kms_key_id.clone())
        .set_ssekms_encryption_context(req.sse_kms_encryption_context.clone())
        .set_bucket_key_enabled(req.bucket_key_enabled)
        .set_request_payer(req.request_payer.clone())
        .set_tagging(req.tagging.clone())
        .set_object_lock_mode(req.object_lock_mode.clone())
        .set_object_lock_retain_until_date(req.object_lock_retain_until_date)
        .set_object_lock_legal_hold_status(req.object_lock_legal_hold_status.clone())
        .set_expected_bucket_owner(req.expected_bucket_owner.clone())
        .set_checksum_algorithm(req.checksum_algorithm.clone())
        .send()
        .await?;

    Ok(resp.into())
}

#[cfg(test)]
mod test {
    use crate::io::InputStream;
    use crate::operation::upload::UploadInput;
    use crate::types::{ConcurrencySetting, PartSize};
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks_experimental::{mock, mock_client, RuleMode};
    use bytes::Bytes;
    use std::ops::Deref;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_basic_mpu() {
        let expected_upload_id = Arc::new("test-upload".to_owned());
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);

        let upload_id = expected_upload_id.clone();
        let create_mpu =
            mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
                CreateMultipartUploadOutput::builder()
                    .upload_id(upload_id.as_ref().to_owned())
                    .build()
            });

        let upload_id = expected_upload_id.clone();
        let upload_1 = mock!(aws_sdk_s3::Client::upload_part)
            .match_requests(move |r| {
                r.upload_id.as_ref() == Some(&upload_id) && r.content_length == Some(30)
            })
            .then_output(|| UploadPartOutput::builder().build());

        let upload_id = expected_upload_id.clone();
        let upload_2 = mock!(aws_sdk_s3::Client::upload_part)
            .match_requests(move |r| {
                r.upload_id.as_ref() == Some(&upload_id) && r.content_length == Some(9)
            })
            .then_output(|| UploadPartOutput::builder().build());

        let expected_e_tag = Arc::new("test-e-tag".to_owned());
        let upload_id = expected_upload_id.clone();
        let e_tag = expected_e_tag.clone();
        let complete_mpu = mock!(aws_sdk_s3::Client::complete_multipart_upload)
            .match_requests(move |r| {
                r.upload_id.as_ref() == Some(&upload_id)
                    && r.multipart_upload.clone().unwrap().parts.unwrap().len() == 2
            })
            .then_output(move || {
                CompleteMultipartUploadOutput::builder()
                    .e_tag(e_tag.as_ref().to_owned())
                    .build()
            });

        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&create_mpu, &upload_1, &upload_2, &complete_mpu]
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencySetting::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(30))
            .client(client)
            .build();

        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);

        let handle = request.send_with(&tm).await.unwrap();

        let resp = handle.join().await.unwrap();
        assert_eq!(expected_upload_id.deref(), resp.upload_id.unwrap().deref());
        assert_eq!(expected_e_tag.deref(), resp.e_tag.unwrap().deref());
    }

    // TODO: Fix test to not do auto upload
    #[tokio::test]
    async fn test_basic_upload_object() {
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);
        let expected_e_tag = Arc::new("test-etag".to_owned());

        let e_tag = expected_e_tag.clone();
        let put_object = mock!(aws_sdk_s3::Client::put_object).then_output(move || {
            PutObjectOutput::builder().e_tag(e_tag.as_ref().to_owned())
                .build()
        });

        let client = mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&put_object]
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencySetting::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10 * 1024 * 1024))
            .client(client)
            .build();
        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);
        let handle = request.send_with(&tm).await.unwrap();
        let resp = handle.join().await.unwrap();
        assert_eq!(resp.upload_id(), None);
        assert_eq!(expected_e_tag.deref(), resp.e_tag().unwrap());
    }
}
