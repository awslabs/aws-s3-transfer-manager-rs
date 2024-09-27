use super::UploadHandle;
use crate::{
    error,
    io::{
        part_reader::{Builder as PartReaderBuilder, PartData, ReadPart},
        InputStream,
    },
    operation::upload::UploadContext,
};
use aws_sdk_s3::{primitives::ByteStream, types::CompletedPart};
use bytes::Buf;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::{sync::Mutex, task};
use tokio::{task, time::Instant};
use tower::{
    hedge::{Hedge, Policy},
    service_fn, Service, ServiceBuilder, ServiceExt,
};
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;
use tracing::Instrument;

use super::UploadHandle;
/// Request/input type for our "upload_part" service.
#[derive(Debug, Clone)]
pub(super) struct UploadPartRequest {
    pub(super) ctx: UploadContext,
    pub(super) part_data: PartData,
}

#[derive(Debug, Clone)]
pub(crate) struct UploadPolicy;

impl Policy<UploadPartRequest> for UploadPolicy {
    /// Attempts to clone the request. If cloning is allowed, it returns a new instance.
    fn clone_request(&self, req: &UploadPartRequest) -> Option<UploadPartRequest> {
        Some(req.clone())
    }

    /// Determines if the request can be retried based on custom logic.
    fn can_retry(&self, _req: &UploadPartRequest) -> bool {
        // Example policy: Only allow retry if part_data is not empty
        true
    }
}

/// handler (service fn) for a single part
async fn upload_part_handler(request: UploadPartRequest) -> Result<CompletedPart, error::Error> {
    let ctx = request.ctx;
    let part_data = request.part_data;
    let part_number = part_data.part_number as i32;

    // TODO(aws-sdk-rust#1159): disable payload signing
    // TODO(aws-sdk-rust#1159): set checksum fields if applicable
    let instant = Instant::now();
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
    let elasped = instant.elapsed();
    //eprint!("{0},", elasped.as_nanos());

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
    let hedge = Hedge::new(svc, UploadPolicy, 100, 95.0, Duration::new(1, 0));
    let svc = ServiceBuilder::new()
        // FIXME - This setting will need to be globalized.
        .buffer(60)
        .concurrency_limit(ctx.handle.num_workers())
        .service(hedge);
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
    handle: &mut UploadHandle,
    stream: InputStream,
    part_size: u64,
) -> Result<(), error::Error> {
    let part_reader = Arc::new(
        PartReaderBuilder::new()
            .stream(stream)
            .part_size(part_size.try_into().expect("valid part size"))
            .build(),
    );
    let svc = upload_part_service(&handle.ctx);
    let n_workers = handle.ctx.handle.num_workers();
    for i in 0..n_workers {
        let worker = read_body(
            part_reader.clone(),
            handle.ctx.clone(),
            svc.clone(),
            handle.upload_tasks.clone(),
        )
        .instrument(tracing::debug_span!("read_body", worker = i));
        handle.read_tasks.spawn(worker);
    }
    tracing::trace!("work distributed for uploading parts");
    Ok(())
}

/// Worker function that pulls part data from the `part_reader` and spawns tasks to upload each part until the reader
/// is exhausted. If any part fails, the worker will return the error and stop processing.
pub(super) async fn read_body(
    part_reader: Arc<impl ReadPart>,
    ctx: UploadContext,
    svc: impl Service<UploadPartRequest, Response = CompletedPart, Error = error::Error, Future: Send>
        + Clone
        + Send
        + 'static,
    upload_tasks: Arc<Mutex<task::JoinSet<Result<CompletedPart, crate::error::Error>>>>,
) -> Result<(), error::Error> {
    while let Some(part_data) = part_reader.next_part().await? {
        let part_number = part_data.part_number;
        let req = UploadPartRequest {
            ctx: ctx.clone(),
            part_data,
        };
        let svc = svc.clone();
        let task = svc.oneshot(req).instrument(tracing::trace_span!(
            "upload_part",
            part_number = part_number
        ));
        upload_tasks.lock().await.spawn(task);
    }
    Ok(())
}
