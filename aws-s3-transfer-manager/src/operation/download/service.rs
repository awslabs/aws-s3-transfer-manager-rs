/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::error;
use crate::operation::download::context::DownloadContext;
use crate::operation::download::header;
use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::{AggregatedBytes, ByteStream};
use std::cmp;
use std::mem;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

use super::DownloadHandle;

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

    let mut resp = request
        .input
        .send_with(ctx.client())
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

    ServiceBuilder::new()
        .concurrency_limit(ctx.handle.num_workers())
        .service(svc)
}

// FIXME - should probably be enum ChunkRequest { Range(..), Part(..) } or have an inner field like such
#[derive(Debug, Clone)]
pub(super) struct ChunkRequest {
    // byte range to download
    pub(super) range: RangeInclusive<u64>,
    pub(super) input: GetObjectInputBuilder,
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

pub(super) fn distribute_work(
    handle: &mut DownloadHandle,
    remaining: RangeInclusive<u64>,
    input: GetObjectInputBuilder,
    start_seq: u64,
    comp_tx: mpsc::Sender<Result<ChunkResponse, error::Error>>,
) {
    let end = *remaining.end();
    let mut pos = *remaining.start();
    let mut remaining = end - pos + 1;
    let mut seq = start_seq;

    let svc = chunk_service(&handle.ctx);

    let part_size = handle.ctx.target_part_size_bytes;

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
            // FIXME - error should propagate to handle.join()
            if let Err(err) = comp_tx.send(resp).await {
                tracing::error!(error = ?err, "chunk send failed");
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
    input: GetObjectInputBuilder,
) -> ChunkRequest {
    let range = start..=end_inclusive;
    let input = input.range(header::Range::bytes_inclusive(start, end_inclusive));
    ChunkRequest { seq, range, input }
}

#[cfg(test)]
mod tests {}
