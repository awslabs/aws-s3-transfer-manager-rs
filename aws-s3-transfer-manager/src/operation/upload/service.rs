use std::sync::Arc;

use super::{BucketType, MultipartUploadData, TransferDirection};
use crate::{
    error,
    io::{part_reader::PartReader, PartData},
    middleware::{
        hedge,
        limit::concurrency::{ConcurrencyLimitLayer, ProvideNetworkPermitContext},
    },
    operation::upload::UploadContext,
    runtime::scheduler::NetworkPermitContext,
};
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{ChecksumAlgorithm, CompletedPart},
};
use bytes::Buf;
use tokio::{sync::Mutex, task};
use tower::{hedge::Policy, service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

/// Maximum requests to buffer in the tower::Buffer layer
///
/// This bears some explanation... we introduced the buffer layer solely to
/// make the resulting service cloneable. The addition of the out of the box hedging layer
/// makes a service not cloneable and a buffer layer fixes that without much overhead.
///
/// The actual number of requests in-flight is limited by our scheduler and the concurrency layer.
/// Thus, we pick a relatively large enough number to guarantee the buffer layer isn't a
/// bottleneck or additional limit on concurrency.
const BUFFER_LAYER_LIMIT: usize = 1024;

/// Request/input type for our "upload_part" service.
#[derive(Debug, Clone)]
pub(super) struct UploadPartRequest {
    pub(super) ctx: UploadContext,
    pub(super) part_data: PartData,
    pub(super) upload_id: String,
}

impl ProvideNetworkPermitContext for UploadPartRequest {
    fn network_permit_context(&self) -> NetworkPermitContext {
        NetworkPermitContext {
            payload_size_estimate: self.part_data.data.len() as u64,
            bucket_type: self.ctx.bucket_type(),
            direction: TransferDirection::Upload,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UploadHedgePolicy;

impl Policy<UploadPartRequest> for UploadHedgePolicy {
    fn clone_request(&self, req: &UploadPartRequest) -> Option<UploadPartRequest> {
        if req.ctx.bucket_type() == BucketType::Standard {
            Some(req.clone())
        } else {
            None
        }
    }

    fn can_retry(&self, _req: &UploadPartRequest) -> bool {
        true
    }
}

/// handler (service fn) for a single part
async fn upload_part_handler(request: UploadPartRequest) -> Result<CompletedPart, error::Error> {
    let ctx = request.ctx;
    let part_data = request.part_data;
    let part_number = part_data.part_number as i32;

    let mut req = ctx
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
        .set_expected_bucket_owner(ctx.request.expected_bucket_owner.clone());

    if let Some(checksum_strategy) = &ctx.request.checksum_strategy {
        // If user passed checksum value via PartStream, add it to request
        if let Some(checksum_value) = &part_data.checksum {
            req = match checksum_strategy.algorithm() {
                ChecksumAlgorithm::Crc32 => req.checksum_crc32(checksum_value),
                ChecksumAlgorithm::Crc32C => req.checksum_crc32_c(checksum_value),
                ChecksumAlgorithm::Crc64Nvme => req.checksum_crc64_nvme(checksum_value),
                ChecksumAlgorithm::Sha1 => req.checksum_sha1(checksum_value),
                ChecksumAlgorithm::Sha256 => req.checksum_sha256(checksum_value),
                algo => unreachable!("unexpected checksum algorithm `{algo}`"),
            }
        } else {
            // Otherwise, set checksum algorithm, which tells SDK to calculate and add checksum value
            req = req.checksum_algorithm(checksum_strategy.algorithm().clone());
        }
    } else {
        // Warn if user is passing a checksum value, but the upload isn't doing checksums.
        // We can't just set a checksum header, because we don't know what algorithm to use.
        if part_data.checksum.is_some() {
            tracing::warn!("Ignoring part checksum provided during upload, because no ChecksumStrategy is specified");
        }
    }

    let resp = req
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
        .set_checksum_crc64_nvme(resp.checksum_crc64_nvme.clone())
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
        // TODO: investigate removing the buffer layer by making the hedging layer cloneable
        .buffer(BUFFER_LAYER_LIMIT)
        // FIXME - Hedged request should also get a permit. Currently, it can bypass the
        // concurrency_limit layer.
        .layer(hedge::Builder::new(UploadHedgePolicy).into_layer())
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
pub(super) fn distribute_work(
    mpu_data: &mut MultipartUploadData,
    ctx: UploadContext,
) -> Result<(), error::Error> {
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
            mpu_data.part_reader.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Handle;
    use crate::operation::upload::UploadInput;
    use crate::runtime::scheduler::Scheduler;
    use crate::types::ConcurrencyMode;
    use crate::Config;
    use bytes::Bytes;
    use test_common::mock_client_with_stubbed_http_client;

    fn _mock_upload_part_request_with_bucket_name(
        bucket_name: &str,
        bucket_type: BucketType,
    ) -> UploadPartRequest {
        let s3_client = mock_client_with_stubbed_http_client!(aws_sdk_s3, []);
        UploadPartRequest {
            ctx: UploadContext {
                handle: Arc::new(Handle {
                    config: Config::builder().client(s3_client).build(),
                    scheduler: Scheduler::new(ConcurrencyMode::Explicit(1)),
                }),
                request: Arc::new(UploadInput::builder().bucket(bucket_name).build().unwrap()),
                bucket_type,
            },
            part_data: PartData::new(1, Bytes::default()),
            upload_id: "test-id".to_string(),
        }
    }

    #[test]
    fn test_upload_hedge_policy_operation() {
        let policy = UploadHedgePolicy;

        // Test S3 Express bucket
        let express_req =
            _mock_upload_part_request_with_bucket_name("test--x-s3", BucketType::Express);
        assert!(policy.clone_request(&express_req).is_none());

        // Test regular bucket
        let regular_req = _mock_upload_part_request_with_bucket_name("test", BucketType::Standard);
        assert!(policy.clone_request(&regular_req).is_some());
    }
}
