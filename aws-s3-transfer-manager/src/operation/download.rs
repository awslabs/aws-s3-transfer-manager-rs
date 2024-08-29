/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

/// Request type for dowloading a single object from Amazon S3
pub use input::{DownloadInput, DownloadInputBuilder};

/// Abstractions for response bodies and consuming data streams.
pub mod body;
/// Operation builders
pub mod builders;

mod context;
mod discovery;

mod handle;
pub use handle::DownloadHandle;

mod header;
mod object_meta;
mod service;

use body::Body;
use context::DownloadContext;
use discovery::{discover_obj, ObjectDiscovery};
use service::{distribute_work, ChunkResponse};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download::DownloadInput,
    ) -> Result<DownloadHandle, crate::error::Error> {
        // if there is a part number then just send the default request
        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let target_part_size_bytes = handle.download_part_size_bytes();
        let concurrency = handle.num_workers();

        let ctx = DownloadContext {
            handle,
            target_part_size_bytes,
        };

        // FIXME - discovery network requests should go through hedging, as well as scheduler to
        // prevent hurting throughput by not coordinating the work

        // make initial discovery about the object size, metadata, possibly first chunk
        let mut discovery = discover_obj(&ctx, &input).await?;
        let (comp_tx, comp_rx) = mpsc::channel(concurrency);
        let start_seq = handle_discovery_chunk(&mut discovery, &comp_tx).await;

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let tasks = JoinSet::new();

        let mut handle = DownloadHandle {
            // FIXME(aws-sdk-rust#1159) - initial object discovery for a range/first-part will not
            //   have the correct metadata w.r.t. content-length and maybe others for the whole object.
            object_meta: discovery.meta,
            body: Body::new(comp_rx),
            tasks,
            ctx,
        };

        if !discovery.remaining.is_empty() {
            let remaining = discovery.remaining.clone();
            distribute_work(&mut handle, remaining, input.into(), start_seq, comp_tx)
        }

        Ok(handle)
    }
}

/// Handle possibly sending the first chunk of data received through discovery. Returns
/// the starting sequence number to use for remaining chunks.
async fn handle_discovery_chunk(
    discovery: &mut ObjectDiscovery,
    completed: &mpsc::Sender<Result<ChunkResponse, crate::error::Error>>,
) -> u64 {
    let mut start_seq = 0;
    if let Some(initial_data) = discovery.initial_chunk.take() {
        let chunk = ChunkResponse {
            seq: start_seq,
            data: Some(initial_data),
        };
        completed.send(Ok(chunk)).await.expect("initial chunk");
        start_seq = 1;
    }
    start_seq
}
