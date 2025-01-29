/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;
mod checksum_strategy;
mod input;
mod output;

mod context;
mod handle;
mod service;

pub use checksum_strategy::ChecksumStrategy;

use crate::error;
use crate::io::InputStream;
use context::UploadContext;
pub use handle::UploadHandle;
use handle::{MultipartUploadData, UploadType};
/// Request type for uploads to Amazon S3
pub use input::{UploadInput, UploadInputBuilder};
/// Response type for uploads to Amazon S3
pub use output::{UploadOutput, UploadOutputBuilder};
use service::distribute_work;
use tracing::Instrument;

use std::cmp;
use std::sync::Arc;

/// Maximum number of parts that a single S3 multipart upload supports
const MAX_PARTS: u64 = 10_000;

/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
pub(crate) struct Upload;

impl Upload {
    /// Execute a single `Upload` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        if input.checksum_strategy.is_none() {
            // User didn't explicitly set checksum strategy.
            // If SDK is configured to send checksums: use default checksum strategy.
            // Else: continue with no checksums
            if handle
                .config
                .client()
                .config()
                .request_checksum_calculation()
                .cloned()
                .unwrap_or_default()
                == aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported
            {
                input.checksum_strategy = Some(ChecksumStrategy::default());
            }
        }

        let stream = input.take_body();
        let ctx = new_context(handle.clone(), input);
        Ok(UploadHandle::new(
            ctx.clone(),
            tokio::spawn(try_start_upload(handle.clone(), stream, ctx)),
        ))
    }
}

async fn try_start_upload(
    handle: Arc<crate::client::Handle>,
    stream: InputStream,
    ctx: UploadContext,
) -> Result<UploadType, crate::error::Error> {
    let min_mpu_threshold = handle.mpu_threshold_bytes();

    // MPU has max of 10K parts which requires us to know the upper bound on the content length (today anyway)
    // While true for file-based workloads, the upper `size_hint` might not be equal to the actual bytes transferred.
    let content_length = stream
        .size_hint()
        .upper()
        .ok_or_else(crate::io::error::Error::upper_bound_size_hint_required)?;

    let upload_type = if content_length < min_mpu_threshold && !stream.is_mpu_only() {
        tracing::trace!("upload request content size hint ({content_length}) less than min part size threshold ({min_mpu_threshold}); sending as single PutObject request");
        UploadType::PutObject(tokio::spawn(put_object(
            ctx.clone(),
            stream,
            content_length,
        )))
    } else {
        // TODO - to upload a 0 byte object via MPU you have to send [CreateMultipartUpload, UploadPart(part=1, 0 bytes), CompleteMultipartUpload]
        //        we should add tests for this and hide this edge case from the user (e.g. send an empty part when a custom PartStream returns `None` immediately)
        // FIXME - investigate what it would take to allow non mpu uploads for `PartStream` implementations
        try_start_mpu_upload(ctx, stream, content_length).await?
    };
    Ok(upload_type)
}

async fn put_object(
    ctx: UploadContext,
    stream: InputStream,
    content_length: u64,
) -> Result<UploadOutput, error::Error> {
    let body = stream.into_byte_stream().await?;
    let content_length: i64 = content_length.try_into().map_err(|_| {
        error::invalid_input(format!("content_length:{} is invalid.", content_length))
    })?;
    // FIXME - This affects performance in cases with a lot of small files workloads. We need a way to schedule
    // more work for a lot of small files.
    let _permit = ctx.handle.scheduler.acquire_permit().await?;
    let mut req = ctx
        .client()
        .put_object()
        .set_acl(ctx.request.acl.clone())
        .body(body)
        .set_bucket(ctx.request.bucket.clone())
        .set_cache_control(ctx.request.cache_control.clone())
        .set_content_disposition(ctx.request.content_disposition.clone())
        .set_content_encoding(ctx.request.content_encoding.clone())
        .set_content_language(ctx.request.content_language.clone())
        .content_length(content_length)
        .set_content_md5(ctx.request.content_md5.clone())
        .set_content_type(ctx.request.content_type.clone())
        .set_expires(ctx.request.expires)
        .set_grant_full_control(ctx.request.grant_full_control.clone())
        .set_grant_read(ctx.request.grant_read.clone())
        .set_grant_read_acp(ctx.request.grant_read_acp.clone())
        .set_grant_write_acp(ctx.request.grant_write_acp.clone())
        .set_key(ctx.request.key.clone())
        .set_metadata(ctx.request.metadata.clone())
        .set_server_side_encryption(ctx.request.server_side_encryption.clone())
        .set_storage_class(ctx.request.storage_class.clone())
        .set_website_redirect_location(ctx.request.website_redirect_location.clone())
        .set_sse_customer_algorithm(ctx.request.sse_customer_algorithm.clone())
        .set_sse_customer_key(ctx.request.sse_customer_key.clone())
        .set_sse_customer_key_md5(ctx.request.sse_customer_key_md5.clone())
        .set_ssekms_key_id(ctx.request.sse_kms_key_id.clone())
        .set_ssekms_encryption_context(ctx.request.sse_kms_encryption_context.clone())
        .set_bucket_key_enabled(ctx.request.bucket_key_enabled)
        .set_request_payer(ctx.request.request_payer.clone())
        .set_tagging(ctx.request.tagging.clone())
        .set_object_lock_mode(ctx.request.object_lock_mode.clone())
        .set_object_lock_retain_until_date(ctx.request.object_lock_retain_until_date)
        .set_object_lock_legal_hold_status(ctx.request.object_lock_legal_hold_status.clone())
        .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone());

    if let Some(checksum_strategy) = &ctx.request.checksum_strategy {
        if let Some(value) = checksum_strategy.full_object_checksum() {
            // We have the full-object checksum value, so set it
            req = match checksum_strategy.algorithm() {
                aws_sdk_s3::types::ChecksumAlgorithm::Crc32 => req.checksum_crc32(value),
                aws_sdk_s3::types::ChecksumAlgorithm::Crc32C => req.checksum_crc32_c(value),
                aws_sdk_s3::types::ChecksumAlgorithm::Crc64Nvme => req.checksum_crc64_nvme(value),
                algo => unreachable!("unexpected algorithm `{algo}` for full object checksum"),
            };
        } else {
            // Set checksum algorithm, which tells SDK to calculate and add checksum value
            req = req.checksum_algorithm(checksum_strategy.algorithm().clone());
        }
    }

    let resp = req
        .customize()
        .disable_payload_signing()
        .send()
        .instrument(tracing::info_span!(
            "send-upload-part",
            bucket = ctx.request.bucket().unwrap_or_default(),
            key = ctx.request.key().unwrap_or_default()
        ))
        .await?;
    Ok(UploadOutputBuilder::from(resp).build()?)
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
) -> Result<UploadType, crate::error::Error> {
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
    let upload_id = mpu.upload_id.clone().expect("upload_id is present");
    let mut mpu_data = MultipartUploadData {
        upload_part_tasks: Default::default(),
        read_body_tasks: Default::default(),
        response: Some(mpu),
        upload_id: upload_id.clone(),
    };

    distribute_work(&mut mpu_data, ctx, stream, part_size)?;
    Ok(UploadType::MultipartUpload(mpu_data))
}

fn new_context(handle: Arc<crate::client::Handle>, req: UploadInput) -> UploadContext {
    UploadContext {
        handle,
        request: Arc::new(req),
    }
}

/// start a new multipart upload by invoking `CreateMultipartUpload`
async fn start_mpu(ctx: &UploadContext) -> Result<UploadOutputBuilder, crate::error::Error> {
    let req = ctx.request();
    let client = ctx.client();

    let mut req = client
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
        .set_expected_bucket_owner(req.expected_bucket_owner.clone());

    if let Some(checksum_strategy) = &ctx.request.checksum_strategy {
        req = req
            .checksum_algorithm(checksum_strategy.algorithm().clone())
            .checksum_type(checksum_strategy.type_if_multipart().clone());
    }

    let resp = req
        .send()
        .instrument(tracing::debug_span!("send-create-multipart-upload"))
        .await?;

    Ok(resp.into())
}

#[cfg(test)]
mod test {
    use crate::io::InputStream;
    use crate::metrics::unit::ByteUnit;
    use crate::operation::upload::UploadInput;
    use crate::types::{ConcurrencySetting, PartSize};
    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_mocks_experimental::{mock, RuleMode};
    use bytes::Bytes;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::sync::Barrier;
    use test_common::mock_client_with_stubbed_http_client;

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

        let client = mock_client_with_stubbed_http_client!(
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

        let handle = request.initiate_with(&tm).unwrap();

        let resp = handle.join().await.unwrap();
        assert_eq!(expected_upload_id.deref(), resp.upload_id.unwrap().deref());
        assert_eq!(expected_e_tag.deref(), resp.e_tag.unwrap().deref());
    }

    #[tokio::test]
    async fn test_basic_upload_object() {
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);
        let expected_e_tag = Arc::new("test-etag".to_owned());

        let e_tag = expected_e_tag.clone();
        let put_object = mock!(aws_sdk_s3::Client::put_object).then_output(move || {
            PutObjectOutput::builder()
                .e_tag(e_tag.as_ref().to_owned())
                .build()
        });

        let client =
            mock_client_with_stubbed_http_client!(aws_sdk_s3, RuleMode::Sequential, &[&put_object]);

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencySetting::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10 * ByteUnit::Mebibyte.as_bytes_u64()))
            .client(client)
            .build();
        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);
        let handle = request.initiate_with(&tm).unwrap();
        let resp = handle.join().await.unwrap();
        assert_eq!(resp.upload_id(), None);
        assert_eq!(expected_e_tag.deref(), resp.e_tag().unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_abort_multipart_upload() {
        let expected_upload_id = Arc::new("test-upload".to_owned());
        let body = Bytes::from_static(b"every adolescent dog goes bonkers early");
        let stream = InputStream::from(body);
        let bucket = "test-bucket";
        let key = "test-key";
        let wait_till_create_mpu = Arc::new(Barrier::new(2));

        let upload_id = expected_upload_id.clone();
        let create_mpu =
            mock!(aws_sdk_s3::Client::create_multipart_upload).then_output(move || {
                CreateMultipartUploadOutput::builder()
                    .upload_id(upload_id.as_ref().to_owned())
                    .build()
            });

        let upload_part = mock!(aws_sdk_s3::Client::upload_part).then_output({
            let wait_till_create_mpu = wait_till_create_mpu.clone();
            move || {
                wait_till_create_mpu.wait();
                UploadPartOutput::builder().build()
            }
        });

        let abort_mpu = mock!(aws_sdk_s3::Client::abort_multipart_upload)
            .match_requests({
                let upload_id: Arc<String> = expected_upload_id.clone();
                move |input| {
                    input.upload_id.as_ref() == Some(&upload_id)
                        && input.bucket() == Some(bucket)
                        && input.key() == Some(key)
                }
            })
            .then_output(|| AbortMultipartUploadOutput::builder().build());

        let client = mock_client_with_stubbed_http_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[create_mpu, upload_part, abort_mpu]
        );

        let tm_config = crate::Config::builder()
            .concurrency(ConcurrencySetting::Explicit(1))
            .set_multipart_threshold(PartSize::Target(10))
            .set_target_part_size(PartSize::Target(5 * ByteUnit::Mebibyte.as_bytes_u64()))
            .client(client)
            .build();

        let tm = crate::Client::new(tm_config);

        let request = UploadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .body(stream);
        let handle = request.initiate_with(&tm).unwrap();
        wait_till_create_mpu.wait();
        let abort = handle.abort().await.unwrap();
        assert_eq!(abort.upload_id().unwrap(), expected_upload_id.deref());
    }
}
