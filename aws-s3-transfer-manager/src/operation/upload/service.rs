use std::sync::Arc;

use crate::{error, io::{part_reader::{Builder as PartReaderBuilder, PartData, ReadPart}, InputStream}, operation::upload::UploadContext};
use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::Buf;
use tokio::{sync::Mutex, task};
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};

use super::UploadHandle;

/// Request/input type for our "upload_part" service.
#[derive(Debug, Clone)]
pub(super) struct UploadPartRequest {
    pub(super) ctx: UploadContext,
    pub(super) part_data: PartData,
}

/// handler (service fn) for a single part
async fn upload_part_handler(request: UploadPartRequest) -> Result<Option<CompletedPart>, error::Error> {
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

    Ok(Some(completed))
}

/// Create a new tower::Service for uploading individual parts of an object to S3
pub(super) fn upload_part_service(
    ctx: &UploadContext,
) -> impl Service<UploadPartRequest, Response = Option<CompletedPart>, Error = error::Error, Future: Send>
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
/// * body_rx - the channel to receive the body for upload
pub(super) async fn distribute_work(
    handle: &mut UploadHandle,
    stream: InputStream,
    part_size: u64,
) -> Result<(), error::Error> {
    
    let part_reader = Arc::new(PartReaderBuilder::new()
            .stream(stream)
            .part_size(part_size.try_into().expect("valid part size"))
            .build());
    let svc = upload_part_service(&handle.ctx);
    let n_workers = handle.ctx.handle.num_workers();
    for _i in 0..n_workers {
        let mut tasks = handle.tasks.lock().await;
        let worker = read_body(part_reader.clone(), handle.ctx.clone(), svc.clone(), handle.tasks.clone());
        //.instrument
        tasks.spawn(worker);
    }
    tracing::trace!("work distributed for uploading parts");
    Ok(())
}

pub(super) async fn read_body(
    part_reader: Arc<impl ReadPart>,
    ctx: UploadContext,
    svc: impl Service<UploadPartRequest, Response = Option<CompletedPart>, Error = error::Error, Future: Send>
       + Clone
       + Send
       + 'static,
    tasks: Arc<Mutex<task::JoinSet<Result<Option<CompletedPart>, crate::error::Error>>>>,
) -> Result<Option<CompletedPart>, error::Error> {
    loop {
        let part_data = part_reader.next_part().await?;
        let part_data = match part_data {
            None => break,
            Some(part_data) => part_data,
        };

        let req = UploadPartRequest {
            ctx: ctx.clone(),
            part_data,
        };
        let svc = svc.clone();
        let task = async move { svc.oneshot(req).await };
        let mut tasks = tasks.lock().await;
        tasks.spawn(task);
    }
    Ok(None)
}
