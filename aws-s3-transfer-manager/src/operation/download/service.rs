/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::error;
use crate::operation::download::context::DownloadContext;
use crate::operation::download::worker::{ChunkRequest, ChunkResponse};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use std::mem;
use std::time::Duration;
use tower::hedge::Hedge;
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

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

/// Minimum number of data points for hedging decision
const HEDGE_MIN_DATA_POINTS: u64 = 3;

/// Any request that takes longer than this latency percentile will be pre-emptively retried
const HEDGE_LATENCY_PERCENTILE: f32 = 0.90;

/// Period for calculating percentiles
const HEDGE_PERIOD_SECS: u64 = 5;

#[derive(Debug, Clone, Default)]
struct HedgePolicy;

impl tower::hedge::Policy<DownloadChunkRequest> for HedgePolicy {
    fn clone_request(&self, req: &DownloadChunkRequest) -> Option<DownloadChunkRequest> {
        Some(req.clone())
    }

    fn can_retry(&self, _req: &DownloadChunkRequest) -> bool {
        true
    }
}

pub(super) fn chunk_service(
    ctx: &DownloadContext,
) -> impl Service<DownloadChunkRequest, Response = ChunkResponse, Error = error::Error, Future: Send>
       + Clone
       + Send {
    let svc = service_fn(download_chunk_handler);
    let svc = Hedge::new(
        svc,
        HedgePolicy,
        HEDGE_MIN_DATA_POINTS,
        HEDGE_LATENCY_PERCENTILE,
        Duration::from_secs(HEDGE_PERIOD_SECS),
    );

    // TODO - todo iuse Buffer::pair() to not spawn directly onto tokio and spawn onto the JoinSet
    let svc = ServiceBuilder::new()
        .buffer(64)
        .concurrency_limit(ctx.handle.num_workers())
        .service(svc);

    // restore our error type
    svc.map_err(|err| {
        let e = err
            .downcast::<error::Error>()
            .unwrap_or_else(|err| Box::new(error::Error::new(error::ErrorKind::RuntimeError, err)));
        *e
    })
}
