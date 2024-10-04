/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

use aws_sdk_s3::error::DisplayErrorContext;
/// Request type for dowloading a single object from Amazon S3
pub use input::{DownloadInput, DownloadInputBuilder};

/// Abstractions for response bodies and consuming data streams.
pub mod body;
/// Operation builders
pub mod builders;

mod discovery;

mod handle;
pub use handle::DownloadHandle;

mod object_meta;
mod service;

use crate::error;
use crate::runtime::scheduler::OwnedWorkPermit;
use aws_smithy_types::byte_stream::ByteStream;
use body::Body;
use discovery::discover_obj;
use service::{distribute_work, ChunkResponse};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::TransferContext;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download::DownloadInput,
    ) -> Result<DownloadHandle, error::Error> {
        // if there is a part number then just send the default request
        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let concurrency = handle.num_workers();
        let ctx = DownloadContext::new(handle);

        // acquire a permit for discovery
        let permit = ctx.handle.scheduler.acquire_permit().await?;

        // make initial discovery about the object size, metadata, possibly first chunk
        let mut discovery = discover_obj(&ctx, &input).await?;
        let (comp_tx, comp_rx) = mpsc::channel(concurrency);

        let initial_chunk = discovery.initial_chunk.take();
        let mut handle = DownloadHandle {
            // FIXME(aws-sdk-rust#1159) - initial object discovery for a range/first-part will not
            //   have the correct metadata w.r.t. content-length and maybe others for the whole object.
            object_meta: discovery.meta,
            body: Body::new(comp_rx),
            // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
            tasks: JoinSet::new(),
            ctx,
        };

        // spawn a task (if necessary) to handle the discovery chunk. This returns immediately so
        // that we can begin concurrently downloading any reamining chunks/parts ASAP
        let start_seq = handle_discovery_chunk(&mut handle, initial_chunk, &comp_tx, permit);

        if !discovery.remaining.is_empty() {
            let remaining = discovery.remaining.clone();
            distribute_work(&mut handle, remaining, input, start_seq, comp_tx)
        }

        Ok(handle)
    }
}

/// Handle possibly sending the first chunk of data received through discovery. Returns
/// the starting sequence number to use for remaining chunks.
///
/// NOTE: This function does _not_ wait to read the initial chunk from discovery but
/// instead spawns a new task to read the stream and send it over the body channel.
/// This allows remaining work to start immediately (and concurrently) without
/// waiting for the first chunk.
fn handle_discovery_chunk(
    handle: &mut DownloadHandle,
    initial_chunk: Option<ByteStream>,
    completed: &mpsc::Sender<Result<ChunkResponse, crate::error::Error>>,
    permit: OwnedWorkPermit,
) -> u64 {
    let mut start_seq = 0;
    if let Some(stream) = initial_chunk {
        let seq = handle.ctx.next_seq();
        let completed = completed.clone();
        // spawn a task to actually read the discovery chunk without waiting for it so we
        // can get started sooner on any remaining work (if any)
        handle.tasks.spawn(async move {
            let chunk = stream
                .collect()
                .await
                .map(|aggregated| ChunkResponse {
                    seq,
                    data: Some(aggregated),
                })
                .map_err(error::discovery_failed);

            // release the permit _before_ sending the chunk to sequencing so that other requests
            // can make progress
            drop(permit);
            if let Err(send_err) = completed.send(chunk).await {
                tracing::error!(
                    "channel closed, initial chunk from discovery not sent: {}",
                    &DisplayErrorContext(send_err)
                );
            }
        });
        start_seq = 1;
    }
    start_seq
}

/// Download operation specific state
#[derive(Debug)]
pub(crate) struct DownloadState {
    current_seq: u64,
}

type DownloadContext = TransferContext<Mutex<DownloadState>>;

impl DownloadContext {
    fn new(handle: Arc<crate::client::Handle>) -> Self {
        let state = Arc::new(Mutex::new(DownloadState { current_seq: 0 }));
        TransferContext { handle, state }
    }

    /// The target part size to use for this download
    fn target_part_size_bytes(&self) -> u64 {
        self.handle.download_part_size_bytes()
    }

    /// Returns the next part to download
    fn next_seq(&self) -> u64 {
        let state = self.state.clone();
        let mut state = state.lock().unwrap();

        let part_number = state.current_seq;
        state.current_seq += 1;
        part_number
    }
}
