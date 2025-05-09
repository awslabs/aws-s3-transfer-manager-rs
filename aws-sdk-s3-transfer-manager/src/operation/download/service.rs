/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::error;
use crate::error::ErrorKind;
use crate::http::header;
use crate::io::AggregatedBytes;
use crate::middleware::limit::concurrency::ConcurrencyLimitLayer;
use crate::middleware::limit::concurrency::ProvideNetworkPermitContext;
use crate::middleware::retry;
use crate::operation::download::DownloadContext;
use crate::runtime::scheduler::NetworkPermitContext;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use std::cmp;
use std::mem;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tokio::task;
use tower::{service_fn, Service, ServiceBuilder, ServiceExt};
use tracing::Instrument;

use super::body::ChunkOutput;
use super::TransferDirection;
use super::{DownloadInput, DownloadInputBuilder};

/// Request/input type for our "chunk" service.
#[derive(Debug, Clone)]
pub(super) struct DownloadChunkRequest {
    pub(super) ctx: DownloadContext,
    pub(super) remaining: RangeInclusive<u64>,
    pub(super) input: DownloadInputBuilder,
    pub(super) start_seq: u64,
}

impl ProvideNetworkPermitContext for DownloadChunkRequest {
    fn network_permit_context(&self) -> NetworkPermitContext {
        // we can't know the actual size by calling next_seq() as that would modify the
        // state. Instead we give an estimate based on the current sequence.
        let seq = self.ctx.current_seq();
        let remaining = self.remaining.clone();
        let part_size = self.ctx.handle.download_part_size_bytes();
        let range = next_range(seq, remaining, part_size, self.start_seq);
        let payload_estimate = range.end() - range.start() + 1;

        NetworkPermitContext {
            payload_size_estimate: payload_estimate,
            bucket_type: self.ctx.bucket_type(),
            direction: TransferDirection::Download,
        }
    }
}

/// Compute the next byte range to fetch as an inclusive range
fn next_range(
    seq: u64,
    remaining: RangeInclusive<u64>,
    part_size: u64,
    start_seq: u64,
) -> RangeInclusive<u64> {
    let start = remaining.start() + ((seq - start_seq) * part_size);
    let end_inclusive = cmp::min(start + part_size - 1, *remaining.end());
    start..=end_inclusive
}

/// Compute the next input to send for the given sequence, part size, and overall object byte range being fetched
fn next_chunk(
    seq: u64,
    remaining: RangeInclusive<u64>,
    part_size: u64,
    start_seq: u64,
    input: DownloadInputBuilder,
) -> DownloadInputBuilder {
    let range = next_range(seq, remaining, part_size, start_seq);
    input.range(header::Range::bytes_inclusive(*range.start(), *range.end()))
}

/// handler (service fn) for a single chunk
async fn download_chunk_handler(
    request: DownloadChunkRequest,
) -> Result<ChunkOutput, error::Error> {
    let seq: u64 = request.ctx.next_seq();

    // the rest of the work is in its own fn, so we can log `seq` in the tracing span
    download_specific_chunk(request, seq)
        .instrument(tracing::debug_span!("download-chunk", seq))
        .await
}

async fn download_specific_chunk(
    request: DownloadChunkRequest,
    seq: u64,
) -> Result<ChunkOutput, error::Error> {
    let ctx = request.ctx;
    let part_size = ctx.handle.download_part_size_bytes();
    let input = next_chunk(
        seq,
        request.remaining,
        part_size,
        request.start_seq,
        request.input,
    );

    let op = input.into_sdk_operation(ctx.client());
    let mut cancel_rx = ctx.state.cancel_rx.clone();
    tokio::select! {
        _ = cancel_rx.changed() => {
            tracing::debug!("Received cancellating signal, exiting and not downloading chunk#{seq}");
            Err(error::operation_cancelled())
        },
        resp = op.send() => {
            match resp {
                Err(err) => Err(error::from_kind(error::ErrorKind::ChunkFailed)(err)),
                Ok(mut resp) => {
                    let body = mem::replace(&mut resp.body, ByteStream::new(SdkBody::taken()));
                    let body = AggregatedBytes::from_byte_stream(body)
                        .instrument(tracing::debug_span!(
                            "collect-body-from-download-chunk",
                            seq
                        ))
                        .await?;

                    Ok(ChunkOutput {
                        seq,
                        data: body,
                        metadata: resp.into(),
                    })
                },
            }
        }
    }
}

/// Create a new tower::Service for downloading individual chunks of an object from S3
pub(super) fn chunk_service(
    ctx: &DownloadContext,
) -> impl Service<DownloadChunkRequest, Response = ChunkOutput, Error = error::Error, Future: Send>
       + Clone
       + Send {
    let svc = service_fn(download_chunk_handler);
    let concurrency_limit = ConcurrencyLimitLayer::new(ctx.handle.scheduler.clone());

    ServiceBuilder::new()
        .layer(concurrency_limit)
        .retry(retry::RetryPolicy::default())
        .service(svc)
}

/// Spawn tasks to download the remaining chunks of object data
///
/// # Arguments
///
/// * handle - the handle for this download
/// * remaining - the remaining content range that needs to be downloaded
/// * input - the base transfer request input used to build chunk requests from
/// * start_seq - the starting sequence number to use for chunks
/// * chunk_tx - the channel to send chunk responses to
pub(super) fn distribute_work(
    tasks: &mut task::JoinSet<()>,
    ctx: DownloadContext,
    remaining: RangeInclusive<u64>,
    input: DownloadInput,
    start_seq: u64,
    chunk_tx: mpsc::Sender<Result<ChunkOutput, error::Error>>,
    parent_span_for_tasks: tracing::Span,
) {
    let svc = chunk_service(&ctx);
    let part_size = ctx.target_part_size_bytes();
    let input: DownloadInputBuilder = input.into();

    let size = *remaining.end() - *remaining.start() + 1;
    let num_parts = size.div_ceil(part_size);
    for _ in 0..num_parts {
        let req = DownloadChunkRequest {
            ctx: ctx.clone(),
            remaining: remaining.clone(),
            input: input.clone(),
            start_seq,
        };

        let svc = svc.clone();
        let chunk_tx = chunk_tx.clone();
        let cancel_tx = ctx.state.cancel_tx.clone();

        let task = async move {
            let resp = svc.oneshot(req).await;
            // If any chunk fails, send cancel notification, to kill any other in-flight chunks
            if let Err(err) = &resp {
                if *err.kind() == ErrorKind::OperationCancelled {
                    // Ignore any OperationCancelled errors.
                    return;
                }
                if cancel_tx.send(true).is_err() {
                    tracing::debug!(
                        "all receiver ends have dropped, unable to send a cancellation signal"
                    );
                }
            }

            if let Err(err) = chunk_tx.send(resp).await {
                tracing::debug!(error = ?err, "chunk send failed, channel closed");
                if cancel_tx.send(true).is_err() {
                    tracing::debug!(
                        "all receiver ends have dropped, unable to send a cancellation signal"
                    );
                }
            }
        };
        tasks.spawn(task.instrument(parent_span_for_tasks.clone()));
    }

    tracing::trace!("work fully distributed");
}
