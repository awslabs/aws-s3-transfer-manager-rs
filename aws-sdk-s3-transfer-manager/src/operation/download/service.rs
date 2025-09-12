/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error;
use crate::error::ChunkId;
use crate::error::ErrorKind;
use crate::http::header;
use crate::io::AggregatedBytes;
use crate::middleware::limit::concurrency::ConcurrencyLimitLayer;
use crate::middleware::limit::concurrency::ProvideNetworkPermitContext;
use crate::operation::download::DownloadContext;
use crate::operation::download::RetryPolicy;
use crate::runtime::scheduler::NetworkPermitContext;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Buf;
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
    // Initially set to `None`.
    // It will be assigned the sequence when `download_chunk_handler` is invoked through retries
    // to ensure the handler operates on the previously assigned number.
    pub(super) seq: Option<u64>,
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
    // `seq` should be assigned at this point, i.e., after the handler begins execution.
    // Assigning `seq` at the time of `tokio::spawn` could cause issues, as spawn order is not guaranteed.
    // This could result in later `seq`s being processed before earlier ones, leading to inefficient memory usage.
    // Related: https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/60
    let seq: u64 = request.seq.unwrap_or_else(|| request.ctx.next_seq());

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
    let requested_byte_range = op.get_range().clone().expect("range set in `next_chunk");
    let mut cancel_rx = ctx.state.cancel_rx.clone();
    tokio::select! {
        _ = cancel_rx.changed() => {
            tracing::debug!("Received cancellating signal, exiting and not downloading chunk#{seq}");
            Err(error::operation_cancelled())
        },
        resp = op.send() => {
            match resp {
                Err(err) => Err(error::chunk_failed(ChunkId::Download(seq), err)),
                Ok(mut resp) => {
                    validate_content_range(seq, &requested_byte_range, resp.content_range())?;
                    let body = mem::replace(&mut resp.body, ByteStream::new(SdkBody::taken()));
                    let body = AggregatedBytes::from_byte_stream(body)
                        .instrument(tracing::debug_span!(
                            "collect-body-from-download-chunk",
                            seq
                        ))
                        .await
                        .map_err(|err| {
                           error::chunk_failed(ChunkId::Download(seq), err)
                        })?;

                    // Track bytes transferred
                    let bytes_len = body.remaining() as u64;
                    ctx.handle.metrics.add_bytes_transferred(bytes_len);

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
        .retry(RetryPolicy::default())
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
    // Set aside the number of tasks before distributing work
    let num_tasks_before = tasks.len() as u64;

    let size = *remaining.end() - *remaining.start() + 1;
    let num_parts = size.div_ceil(part_size);
    for _ in 0..num_parts {
        let req = DownloadChunkRequest {
            ctx: ctx.clone(),
            remaining: remaining.clone(),
            input: input.clone(),
            start_seq,
            seq: None,
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
                if let Err(e) = cancel_tx.send(true) {
                    tracing::debug!(error = ?e, "all receiver ends have dropped, unable to send a cancellation signal");
                }
            }

            if let Err(err) = chunk_tx.send(resp).await {
                tracing::debug!(error = ?err, "chunk send failed, channel closed");
                if let Err(e) = cancel_tx.send(true) {
                    tracing::debug!(error = ?e, "all receiver ends have dropped, unable to send a cancellation signal");
                }
            }
        };
        tasks.spawn(task.instrument(parent_span_for_tasks.clone()));
    }

    let num_requests = tasks.len() as u64 - num_tasks_before;
    if num_parts != num_requests {
        tracing::error!(
            "The total number of GetObject requests must match the expected number of parts: request count {}, number of parts {}",
            num_requests,
            num_parts,
        );
        if let Err(e) = ctx.state.cancel_tx.send(true) {
            tracing::debug!(error = ?e, "all receiver ends have dropped, unable to send a cancellation signal");
        }
    }

    tracing::trace!("work fully distributed");
}

/// Validates that the response content range matches the requested range.
///
/// Handles both "1024-2047" and "bytes=1024-2047" formats for the requested content range.
/// The response_content_range is of the form Some("bytes 1024-2047/4096").
fn validate_content_range(
    seq: u64,
    requested_content_range: &str,
    response_content_range: Option<&str>,
) -> Result<(), error::Error> {
    // Strip "bytes=" prefix if present to normalize the requested range
    let content_range = requested_content_range
        .strip_prefix("bytes=")
        .unwrap_or(requested_content_range);

    if response_content_range
        .map(|range| range.contains(content_range))
        .unwrap_or_default()
    {
        Ok(())
    } else {
        Err(crate::error::chunk_failed(ChunkId::Download(seq),
            format!(
                "Content range in response must match the requested range: requested range {}, content-range in response {:?}",
                requested_content_range,
                response_content_range,
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMMY_SEQ: u64 = 1;

    #[test]
    fn test_validate_content_range_success() {
        assert!(
            validate_content_range(DUMMY_SEQ, "1024-2047", Some("bytes 1024-2047/4096")).is_ok()
        );
        assert!(
            validate_content_range(DUMMY_SEQ, "bytes=1024-2047", Some("bytes 1024-2047/4096"))
                .is_ok()
        );
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        assert!(
            validate_content_range(DUMMY_SEQ, "1024-2047", Some("bytes 2048-3071/4096")).is_err()
        );
        assert!(
            validate_content_range(DUMMY_SEQ, "bytes=1024-2047", Some("bytes 2048-3071/4096"))
                .is_err()
        );
    }

    #[test]
    fn test_validate_content_range_none_response() {
        assert!(validate_content_range(DUMMY_SEQ, "1024-2047", None).is_err());
        assert!(validate_content_range(DUMMY_SEQ, "bytes=1024-2047", None).is_err());
    }
}
