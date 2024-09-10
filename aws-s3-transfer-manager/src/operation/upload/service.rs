use crate::{
    error,
    io::part_reader::PartData,
    operation::upload::UploadContext,
};
use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::Buf;
use tokio::sync::mpsc;
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};

use super::UploadHandle;

/// Request/input type for our "upload_part" service.
#[derive(Debug, Clone)]
pub(super) struct UploadPartRequest {
    pub(super) ctx: UploadContext,
    pub(super) part_data: PartData,
}

/// handler (service fn) for a single part
async fn upload_part_handler(request: UploadPartRequest) -> Result<CompletedPart, error::Error> {
    let ctx = request.ctx;
    let part_data = request.part_data;
    let part_number = part_data.part_number as i32;

    // TODO(aws-sdk-rust#1159): disable payload signing
    // TODO(aws-sdk-rust#1159): set checksum fields if applicable
    let resp = ctx
        .client()
        .upload_part()
        .set_bucket(ctx.request.bucket.clone())
        .set_key(ctx.request.key.clone())
        .set_upload_id(ctx.upload_id.clone())
        .part_number(part_number)
        .content_length(part_data.data.remaining() as i64)
        .body(ByteStream::from(part_data.data))
        .set_sse_customer_algorithm(ctx.request.sse_customer_algorithm.clone())
        .set_sse_customer_key(ctx.request.sse_customer_key.clone())
        .set_sse_customer_key_md5(ctx.request.sse_customer_key_md5.clone())
        .set_request_payer(ctx.request.request_payer.clone())
        .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone())
        .send()
        .await?;

    tracing::trace!("completed upload of part number {}", part_number);
    let completed = CompletedPart::builder()
        .part_number(part_number)
        .set_e_tag(resp.e_tag.clone())
        .set_checksum_crc32(resp.checksum_crc32.clone())
        .set_checksum_crc32_c(resp.checksum_crc32_c.clone())
        .set_checksum_sha1(resp.checksum_sha1.clone())
        .set_checksum_sha256(resp.checksum_sha256.clone())
        .build();

    Ok(completed)
}

/// Create a new tower::Service for uploading individual parts of an object to S3
pub(super) fn upload_part_service(
    ctx: &UploadContext,
) -> impl Service<UploadPartRequest, Response = CompletedPart, Error = error::Error, Future: Send>
       + Clone
       + Send {
    let svc = service_fn(upload_part_handler);
    ServiceBuilder::new()
        .concurrency_limit(ctx.handle.num_workers())
        .service(svc)
}

/// Spawn tasks to upload the remaining parts of object
///
/// # Arguments
///
/// * handle - the handle for this upload
/// * reader - the reader to read the body for upload
pub(super) async fn distribute_work(
    handle: &mut UploadHandle,
    mut data: mpsc::Receiver<PartData>,
) -> Result<(), error::Error> {
    let svc = upload_part_service(&handle.ctx);
    while let Some(part_data) = data.recv().await {
        let part_number = part_data.part_number as i32;
        tracing::trace!("recv'd part number {}", part_number);

        let req = UploadPartRequest {
            ctx: handle.ctx.clone(),
            part_data,
        };

        let svc = svc.clone();
        let task = async move { svc.oneshot(req).await };
        handle.tasks.spawn(task);
    }
    tracing::trace!("work distributed for uploading parts");
    Ok(())
}
