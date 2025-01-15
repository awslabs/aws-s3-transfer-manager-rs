use std::sync::Arc;

use super::MultipartUploadData;
use crate::{
    error,
    io::{
        part_reader::{Builder as PartReaderBuilder, PartReader},
        InputStream, PartData,
    },
    middleware::{hedge, limit::concurrency::ConcurrencyLimitLayer},
    operation::upload::UploadContext,
};
use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::Buf;
use tokio::{sync::Mutex, task};
use tower::{hedge::Policy, service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

/// Request/input type for our "upload_part" service.
#[derive(Debug, Clone)]
pub(super) struct UploadPartRequest {
    pub(super) ctx: UploadContext,
    pub(super) part_data: PartData,
    pub(super) upload_id: String,
}

impl Policy<UploadPartRequest> for hedge::DefaultPolicy {
    fn clone_request(&self, req: &UploadPartRequest) -> Option<UploadPartRequest> {
        if req.ctx.is_s3_express {
            None
        } else {
            Some(req.clone())
        }
    }
    fn can_retry(&self, req: &UploadPartRequest) -> bool {
        // Stop retry for S3 express bucket, since s3 express generates different etag for same content.
        // FIXME - Maybe remove after s3 express fixes this issue.
        !req.ctx.is_s3_express
    }
}

/// handler (service fn) for a single part
async fn upload_part_handler(request: UploadPartRequest) -> Result<CompletedPart, error::Error> {
    let ctx = request.ctx;
    let part_data = request.part_data;
    let part_number = part_data.part_number as i32;

    // TODO(aws-sdk-rust#1159): set checksum fields if applicable
    let resp = ctx
        .client()
        .upload_part()
        .set_bucket(ctx.request.bucket.clone())
        .set_key(ctx.request.key.clone())
        .set_upload_id(Some(request.upload_id))
        .part_number(part_number)
        .content_length(part_data.data.remaining() as i64)
        .body(ByteStream::from(part_data.data))
        .set_sse_customer_algorithm(ctx.request.sse_customer_algorithm.clone())
        .set_sse_customer_key(ctx.request.sse_customer_key.clone())
        .set_sse_customer_key_md5(ctx.request.sse_customer_key_md5.clone())
        .set_request_payer(ctx.request.request_payer.clone())
        .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone())
        .customize()
        .disable_payload_signing()
        .send()
        .instrument(tracing::debug_span!("send-upload-part", part_number))
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
    let concurrency_limit = ConcurrencyLimitLayer::new(ctx.handle.scheduler.clone());

    let svc = ServiceBuilder::new()
        .layer(concurrency_limit)
        // FIXME - This setting will need to be globalized.
        .buffer(ctx.handle.num_workers())
        // FIXME - Hedged request should also get a permit. Currently, it can bypass the
        // concurrency_limit layer.
        .layer(hedge::Builder::default().into_layer())
        .service(svc);
    svc.map_err(|err| {
        let e = err
            .downcast::<error::Error>()
            .unwrap_or_else(|err| Box::new(error::Error::new(error::ErrorKind::RuntimeError, err)));
        *e
    })
}

/// Spawn tasks to read the body and upload the remaining parts of object
///
/// # Arguments
///
/// * handle - the handle for this upload
/// * stream - the body input stream
/// * part_size - the part_size for each part
pub(super) fn distribute_work(
    mpu_data: &mut MultipartUploadData,
    ctx: UploadContext,
    stream: InputStream,
    part_size: u64,
) -> Result<(), error::Error> {
    let part_reader = Arc::new(
        PartReaderBuilder::new()
            .stream(stream)
            .part_size(part_size.try_into().expect("valid part size"))
            .build(),
    );
    // group all spawned tasks together
    let parent_span_for_all_tasks = tracing::debug_span!(
        parent: None, "upload-tasks", // TODO: for upload_objects, parent should be upload-objects-tasks
        bucket = ctx.request.bucket().unwrap_or_default(),
        key = ctx.request.key().unwrap_or_default(),
    );
    parent_span_for_all_tasks.follows_from(tracing::Span::current());

    // it looks nice to group all read-workers under single span
    let parent_span_for_read_tasks = tracing::debug_span!(
        parent: parent_span_for_all_tasks.clone(),
        "upload-read-tasks"
    );

    // it looks nice to group all upload tasks together under single span
    let parent_span_for_upload_tasks = tracing::debug_span!(
        parent: parent_span_for_all_tasks,
        "upload-net-tasks"
    );
    let svc = upload_part_service(&ctx);
    let n_workers = ctx.handle.num_workers();
    for _ in 0..n_workers {
        let worker = read_body(
            part_reader.clone(),
            ctx.clone(),
            mpu_data.upload_id.clone(),
            svc.clone(),
            mpu_data.upload_part_tasks.clone(),
            parent_span_for_upload_tasks.clone(),
        );
        mpu_data
            .read_body_tasks
            .spawn(worker.instrument(parent_span_for_read_tasks.clone()));
    }
    tracing::trace!("work distributed for uploading parts");
    Ok(())
}

/// Worker function that pulls part data from the `part_reader` and spawns tasks to upload each part until the reader
/// is exhausted. If any part fails, the worker will return the error and stop processing.
pub(super) async fn read_body(
    part_reader: Arc<PartReader>,
    ctx: UploadContext,
    upload_id: String,
    svc: impl Service<UploadPartRequest, Response = CompletedPart, Error = error::Error, Future: Send>
        + Clone
        + Send
        + 'static,
    upload_part_tasks: Arc<Mutex<task::JoinSet<Result<CompletedPart, crate::error::Error>>>>,
    parent_span_for_upload_tasks: tracing::Span,
) -> Result<(), error::Error> {
    while let Some(part_data) = part_reader
        .next_part()
        .instrument(tracing::debug_span!("read-upload-body"))
        .await?
    {
        let req = UploadPartRequest {
            ctx: ctx.clone(),
            part_data,
            upload_id: upload_id.clone(),
        };
        let svc = svc.clone();
        let task = svc.oneshot(req);
        upload_part_tasks
            .lock()
            .await
            .spawn(task.instrument(parent_span_for_upload_tasks.clone()));
    }
    Ok(())
}
