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
    pub(super) request: ChunkRequest,
}

/// handler (service fn) for a single chunk
async fn download_chunk_handler(
    request: DownloadChunkRequest,
) -> Result<ChunkResponse, error::Error> {
    let ctx = request.ctx;
    let request = request.request;

    let op = request.input.into_sdk_operation(ctx.client());

    let mut resp = op
        .send()
        .await
        .map_err(error::from_kind(error::ErrorKind::ChunkFailed))?;

    let body = mem::replace(&mut resp.body, ByteStream::new(SdkBody::taken()));

    let bytes = body
        .collect()
        .instrument(tracing::debug_span!("collect-body", seq = request.seq))
        .await
        .map_err(error::from_kind(error::ErrorKind::ChunkFailed))?;

    Ok(ChunkResponse {
        seq: request.seq,
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

// FIXME - should probably be enum ChunkRequest { Range(..), Part(..) } or have an inner field like such
#[derive(Debug, Clone)]
pub(super) struct ChunkRequest {
    // byte range to download
    pub(super) range: RangeInclusive<u64>,
    pub(super) input: DownloadInputBuilder,
    // sequence number
    pub(super) seq: u64,
}

impl ChunkRequest {
    /// Size of this chunk request in bytes
    pub(super) fn size(&self) -> u64 {
        self.range.end() - self.range.start() + 1
    }
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
    let end = *remaining.end();
    let mut pos = *remaining.start();
    let mut remaining = end - pos + 1;
    let mut seq = start_seq;

    let svc = chunk_service(&handle.ctx);

    let part_size = handle.ctx.target_part_size_bytes();
    let input: DownloadInputBuilder = input.into();

    while remaining > 0 {
        let start = pos;
        let end_inclusive = cmp::min(pos + part_size - 1, end);

        let chunk_req = next_chunk(start, end_inclusive, seq, input.clone());
        tracing::trace!(
            "distributing chunk(size={}): {:?}",
            chunk_req.size(),
            chunk_req
        );
        let chunk_size = chunk_req.size();

        let req = DownloadChunkRequest {
            ctx: handle.ctx.clone(),
            request: chunk_req,
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

        seq += 1;
        remaining -= chunk_size;
        tracing::trace!("remaining = {}", remaining);
        pos += chunk_size;
    }

    tracing::trace!("work fully distributed");
}

fn next_chunk(
    start: u64,
    end_inclusive: u64,
    seq: u64,
    input: DownloadInputBuilder,
) -> ChunkRequest {
    let range = start..=end_inclusive;
    let input = input.range(header::Range::bytes_inclusive(start, end_inclusive));
    ChunkRequest { seq, range, input }
}
