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

mod header;
mod object_meta;
mod service;

use crate::error;
use aws_smithy_types::byte_stream::ByteStream;
use body::Body;
use discovery::discover_obj;
use service::{distribute_work, ChunkResponse};
use std::sync::Arc;
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

        // FIXME - discovery network requests should go through hedging, as well as scheduler to
        // prevent hurting throughput by not coordinating the work

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

        let start_seq = handle_discovery_chunk(&mut handle, initial_chunk, &comp_tx);

        if !discovery.remaining.is_empty() {
            let remaining = discovery.remaining.clone();
            distribute_work(&mut handle, remaining, input, start_seq, comp_tx)
        }

        Ok(handle)
    }
}

/// Handle possibly sending the first chunk of data received through discovery. Returns
/// the starting sequence number to use for remaining chunks.
fn handle_discovery_chunk(
    handle: &mut DownloadHandle,
    initial_chunk: Option<ByteStream>,
    completed: &mpsc::Sender<Result<ChunkResponse, crate::error::Error>>,
) -> u64 {
    let mut start_seq = 0;

    if let Some(stream) = initial_chunk {
        let completed = completed.clone();
        handle.tasks.spawn(async move {
            let chunk = stream
                .collect()
                .await
                .map(|aggregated| ChunkResponse {
                    seq: start_seq,
                    data: Some(aggregated),
                })
                .map_err(error::discovery_failed);

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
pub(crate) struct DownloadState {}

type DownloadContext = TransferContext<DownloadState>;

impl DownloadContext {
    fn new(handle: Arc<crate::client::Handle>) -> Self {
        let state = Arc::new(DownloadState {});
        TransferContext { handle, state }
    }

    /// The target part size to use for this download
    fn target_part_size_bytes(&self) -> u64 {
        self.handle.download_part_size_bytes()
    }
}
