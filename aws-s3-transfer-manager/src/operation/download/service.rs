/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::error;
use crate::http::header;
use crate::middleware::limit::concurrency::ConcurrencyLimitLayer;
use crate::middleware::retry;
use crate::operation::download::DownloadContext;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::{AggregatedBytes, ByteStream};
use std::cmp;
use std::mem;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

use super::{DownloadHandle, DownloadInput, DownloadInputBuilder};

/// Request/input type for our "chunk" service.
#[derive(Debug, Clone)]
pub(super) struct DownloadChunkRequest {
    pub(super) ctx: DownloadContext,
    pub(super) remaining: RangeInclusive<u64>,
    pub(super) input: DownloadInputBuilder,
    pub(super) start_seq: u64,
}

fn next_chunk(
    seq: u64,
    remaining: RangeInclusive<u64>,
    part_size: u64,
    start_seq: u64,
    input: DownloadInputBuilder,
) -> DownloadInputBuilder {
    let start = remaining.start() + ((seq - start_seq) * part_size);
    let end_inclusive = cmp::min(start + part_size - 1, *remaining.end());
    input.range(header::Range::bytes_inclusive(start, end_inclusive))
}

/// handler (service fn) for a single chunk
async fn download_chunk_handler(
    request: DownloadChunkRequest,
) -> Result<ChunkResponse, error::Error> {
    let ctx = request.ctx;
    let seq = ctx.next_seq();
    let part_size = ctx.handle.download_part_size_bytes();
    let input = next_chunk(
        seq,
        request.remaining,
        part_size,
        request.start_seq,
        request.input,
    );

    let op = input.into_sdk_operation(ctx.client());
    let mut resp = op
        .send()
        .await
        .map_err(error::from_kind(error::ErrorKind::ChunkFailed))?;

    let body = mem::replace(&mut resp.body, ByteStream::new(SdkBody::taken()));

    let bytes = body
        .collect()
        .instrument(tracing::debug_span!("collect-body", seq = seq))
        .await
        .map_err(error::from_kind(error::ErrorKind::ChunkFailed))?;

    Ok(ChunkResponse {
        seq,
        data: Some(bytes),
    })
}

/// Create a new tower::Service for downloading individual chunks of an object from S3
pub(super) fn chunk_service(
    ctx: &DownloadContext,
) -> impl Service<DownloadChunkRequest, Response = ChunkResponse, Error = error::Error, Future: Send>
       + Clone
       + Send {
    let svc = service_fn(download_chunk_handler);
    let concurrency_limit = ConcurrencyLimitLayer::new(ctx.handle.scheduler.clone());

    ServiceBuilder::new()
        .layer(concurrency_limit)
        .retry(retry::RetryPolicy::default())
        .service(svc)
}

#[derive(Debug, Clone)]
pub(crate) struct ChunkResponse {
    // TODO(aws-sdk-rust#1159, design) - consider PartialOrd for ChunkResponse and hiding `seq` as internal only detail
    // the seq number
    pub(crate) seq: u64,
    // chunk data
    pub(crate) data: Option<AggregatedBytes>,
}

/// Spawn tasks to download the remaining chunks of object data
///
/// # Arguments
///
/// * handle - the handle for this download
/// * remaining - the remaining content range that needs to be downloaded
/// * input - the base transfer request input used to build chunk requests from
/// * start_seq - the starting sequence number to use for chunks
/// * comp_tx - the channel to send chunk responses to
pub(super) fn distribute_work(
    handle: &mut DownloadHandle,
    remaining: RangeInclusive<u64>,
    input: DownloadInput,
    start_seq: u64,
    comp_tx: mpsc::Sender<Result<ChunkResponse, error::Error>>,
) {
    let svc = chunk_service(&handle.ctx);
    let part_size = handle.ctx.target_part_size_bytes();
    let input: DownloadInputBuilder = input.into();

    let size = *remaining.end() - *remaining.start() + 1;
    let num_parts = size.div_ceil(part_size);
    for seq in 0..num_parts {
        let req = DownloadChunkRequest {
            ctx: handle.ctx.clone(),
            remaining: remaining.clone(),
            input: input.clone(),
            start_seq,
        };

        let svc = svc.clone();
        let comp_tx = comp_tx.clone();

        let task = async move {
            let resp = svc.oneshot(req).await;
            if let Err(err) = comp_tx.send(resp).await {
                tracing::debug!(error = ?err, "chunk send failed, channel closed");
            }
        }
        .instrument(tracing::debug_span!("download-chunk", seq = seq));
        handle.tasks.spawn(task);
    }

    tracing::trace!("work fully distributed");
}
