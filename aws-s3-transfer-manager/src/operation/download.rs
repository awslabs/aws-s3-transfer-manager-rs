/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

use std::sync::Arc;

/// Request type for uploads to Amazon S3
pub use input::{DownloadInput, DownloadInputBuilder};

mod output;

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
mod worker;

use crate::error::TransferError;
use body::Body;
use context::DownloadContext;
use discovery::{discover_obj, ObjectDiscovery};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::Instrument;
use worker::{distribute_work, download_chunks, ChunkResponse};

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download::DownloadInput,
    ) -> Result<DownloadHandle, TransferError> {
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

        // make initial discovery about the object size, metadata, possibly first chunk
        let mut discovery = discover_obj(&ctx, &input).await?;
        let (comp_tx, comp_rx) = mpsc::channel(concurrency);
        let start_seq = handle_discovery_chunk(&mut discovery, &comp_tx).await;

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let mut tasks = JoinSet::new();

        if !discovery.remaining.is_empty() {
            // start assigning work
            let (work_tx, work_rx) = async_channel::bounded(concurrency);
            let input = input.clone();
            let rem = discovery.remaining.clone();

            // TODO(aws-sdk-rust#1159) - test semaphore based approach where we create all futures at once,
            //        the downside is controlling memory usage as a large download may result in
            //        quite a few futures created. If more performant could be enabled for
            //        objects less than some size.

            tasks.spawn(distribute_work(
                rem,
                input.into(),
                target_part_size_bytes,
                start_seq,
                work_tx,
            ));

            for i in 0..concurrency {
                let worker = download_chunks(ctx.clone(), work_rx.clone(), comp_tx.clone())
                    .instrument(tracing::debug_span!("chunk-downloader", worker = i));
                tasks.spawn(worker);
            }
        }

        // Drop our half of the completion channel. When all workers drop theirs, the channel is closed.
        drop(comp_tx);

        let handle = DownloadHandle {
            // FIXME(aws-sdk-rust#1159) - initial object discovery for a range/first-part will not
            //   have the correct metadata w.r.t. content-length and maybe others for the whole object.
            object_meta: discovery.meta,
            body: Body::new(comp_rx),
            _tasks: tasks,
        };

        Ok(handle)
    }
}

/// Handle possibly sending the first chunk of data received through discovery. Returns
/// the starting sequence number to use for remaining chunks.
async fn handle_discovery_chunk(
    discovery: &mut ObjectDiscovery,
    completed: &mpsc::Sender<Result<ChunkResponse, TransferError>>,
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
