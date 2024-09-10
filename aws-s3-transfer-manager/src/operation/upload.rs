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
use crate::io::part_reader::{Builder as PartReaderBuilder, PartData, ReadPart};
use crate::io::InputStream;
use context::UploadContext;
pub use handle::UploadHandle;
/// Request type for uploads to Amazon S3
pub use input::{UploadInput, UploadInputBuilder};
/// Response type for uploads to Amazon S3
pub use output::{UploadOutput, UploadOutputBuilder};
use service::distribute_work;
use tokio::sync::mpsc;

use std::cmp;
use std::sync::Arc;
use tracing::Instrument;

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
        let mut handle = UploadHandle::new(ctx);

        // MPU has max of 10K parts which requires us to know the upper bound on the content length (today anyway)
        let content_length = stream
            .size_hint()
            .upper()
            .ok_or_else(crate::io::error::Error::upper_bound_size_hint_required)?;

        if content_length < min_mpu_threshold {
            // TODO - adapt body to ByteStream and send request using `PutObject` for non mpu upload
            // tracing::trace!("upload request content size hint ({content_length}) less than min part size threshold ({min_mpu_threshold}); sending as single PutObject request");
            try_start_mpu_upload(&mut handle, stream, content_length).await?
        } else {
            try_start_mpu_upload(&mut handle, stream, content_length).await?
        }

        Ok(handle)
    }
}

/// Start a multipart upload
///
/// # Arguments
///
/// * `handle` - The upload handle
/// * `stream` - The content to upload
/// * `content_length` - The upper bound on the content length
async fn try_start_mpu_upload(
    handle: &mut UploadHandle,
    stream: InputStream,
    content_length: u64,
) -> Result<(), crate::error::Error> {
    let part_size = cmp::max(
        handle.ctx.handle.upload_part_size_bytes(),
        content_length / MAX_PARTS,
    );
    tracing::trace!("upload request using multipart upload with part size: {part_size} bytes");

    let mpu = start_mpu(handle).await?;
    tracing::trace!(
        "multipart upload started with upload id: {:?}",
        mpu.upload_id
    );

    handle.set_response(mpu);

    let part_reader = Arc::new(
        PartReaderBuilder::new()
            .stream(stream)
            .part_size(part_size.try_into().expect("valid part size"))
            .build(),
    );

    let (sender, data) = mpsc::channel(handle.ctx.handle.num_workers());
    let n_workers = handle.ctx.handle.num_workers();
    for i in 0..n_workers {
        let worker = read_body(part_reader.clone(), sender.clone())
            .instrument(tracing::debug_span!("read_body", worker = i));
        handle.read_body_tasks.spawn(worker);
    }
    drop(sender);
    distribute_work(handle, data).await
}

async fn read_body(
    part_reader: Arc<impl ReadPart>,
    sender: mpsc::Sender<PartData>,
) -> Result<(), error::Error> {
    loop {
        let part_data = part_reader.next_part().await?;
        let part_data = match part_data {
            None => break,
            Some(part_data) => part_data,
        };

        // TODO: is unwrap the right thing to do?
        sender.send(part_data).await.unwrap();
    }
    Ok(())
}

fn new_context(handle: Arc<crate::client::Handle>, req: UploadInput) -> UploadContext {
    UploadContext {
        handle,
        request: Arc::new(req),
        upload_id: None,
    }
}

/// start a new multipart upload by invoking `CreateMultipartUpload`
async fn start_mpu(handle: &UploadHandle) -> Result<UploadOutputBuilder, crate::error::Error> {
    let req = handle.ctx.request();
    let client = handle.ctx.client();

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
}
