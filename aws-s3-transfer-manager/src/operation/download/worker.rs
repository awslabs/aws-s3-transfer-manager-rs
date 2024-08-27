/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use crate::error;
use crate::operation::download::context::DownloadContext;
use crate::operation::download::header;
use crate::operation::download::service::{chunk_service, DownloadChunkRequest};
use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::{AggregatedBytes, ByteStream};
use std::ops::RangeInclusive;
use std::{cmp, mem};
use tokio::sync::mpsc;
use tower::ServiceExt;
use tracing::Instrument;

use super::DownloadHandle;

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
mod tests {
    // use crate::operation::download::header;
    // use crate::operation::download::worker::distribute_work;
    // use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
    // use std::ops::RangeInclusive;

    // #[tokio::test]
    // async fn test_distribute_work() {
    //     let rem = 0..=90u64;
    //     let part_size = 20;
    //     let input = GetObjectInputBuilder::default();
    //     let (tx, rx) = async_channel::unbounded();
    //
    //     tokio::spawn(distribute_work(rem, input, part_size, 0, tx));
    //
    //     let mut chunks = Vec::new();
    //     while let Ok(chunk) = rx.recv().await {
    //         chunks.push(chunk);
    //     }
    //
    //     let expected_ranges = vec![0..=19u64, 20..=39u64, 40..=59u64, 60..=79u64, 80..=90u64];
    //
    //     let actual_ranges: Vec<RangeInclusive<u64>> =
    //         chunks.iter().map(|c| c.range.clone()).collect();
    //
    //     assert_eq!(expected_ranges, actual_ranges);
    //     assert!(rx.is_closed());
    //
    //     for (i, chunk) in chunks.iter().enumerate() {
    //         assert_eq!(i as u64, chunk.seq);
    //         let expected_range_header =
    //             header::Range::bytes_inclusive(*chunk.range.start(), *chunk.range.end())
    //                 .to_string();
    //
    //         assert_eq!(
    //             expected_range_header,
    //             chunk.input.get_range().clone().expect("range header set")
    //         );
    //     }
    // }
}
