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
use tracing::Instrument;

mod chunk_meta;
mod object_meta;
mod service;

use crate::error;
use crate::io::aggregated_bytes::AggregatedBytes;
use crate::runtime::scheduler::OwnedWorkPermit;
use aws_smithy_types::byte_stream::ByteStream;
use body::{Body, ChunkOutput};
use chunk_meta::ChunkMetadata;
use discovery::discover_obj;
use object_meta::ObjectMetadata;
use service::distribute_work;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, OnceCell};
use tokio::task::{self, JoinSet};

use super::TransferContext;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    ///
    /// If `use_current_span_as_parent_for_tasks` is true, spawned tasks will
    /// be under the current span, and that span will not end until all tasks
    /// complete. Useful for download-objects, which calls this from a task
    /// whose span should last until all individual object downloads complete.
    ///
    /// If `use_current_span_as_parent_for_tasks` is false, spawned tasks will
    /// "follow from" the current span, but be under their own root of the trace tree.
    /// Use this for `TransferManager.download().send()`, where the spawned tasks
    /// should NOT extend the life of the current `send()` span.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download::DownloadInput,
        use_current_span_as_parent_for_tasks: bool,
    ) -> Result<DownloadHandle, error::Error> {
        // if there is a part number then just send the default request
        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let ctx = DownloadContext::new(handle);
        let concurrency = ctx.handle.num_workers();
        let (comp_tx, comp_rx) = mpsc::channel(concurrency);
        let (object_meta_tx, object_meta_rx) = oneshot::channel();

        let tasks = Arc::new(Mutex::new(JoinSet::new()));
        let discovery = tokio::spawn(send_discovery(
            tasks.clone(),
            ctx.clone(),
            comp_tx,
            object_meta_tx,
            input,
            use_current_span_as_parent_for_tasks,
        ));

        Ok(DownloadHandle {
            body: Body::new(comp_rx),
            tasks,
            discovery,
            object_meta_receiver: Mutex::new(Some(object_meta_rx)),
            object_meta: OnceCell::new(),
        })
    }
}

async fn send_discovery(
    tasks: Arc<Mutex<task::JoinSet<()>>>,
    ctx: DownloadContext,
    comp_tx: mpsc::Sender<Result<ChunkOutput, crate::error::Error>>,
    object_meta_tx: oneshot::Sender<ObjectMetadata>,
    input: DownloadInput,
    use_current_span_as_parent_for_tasks: bool,
) -> Result<(), crate::error::Error> {
    // create span to serve as parent of spawned child tasks.
    let parent_span_for_tasks = tracing::debug_span!(
        parent: if use_current_span_as_parent_for_tasks { tracing::Span::current().id() } else { None } ,
        "download-tasks",
        bucket = input.bucket().unwrap_or_default(),
        key = input.key().unwrap_or_default(),
    );
    if !use_current_span_as_parent_for_tasks {
        // if not child of current span, then "follows from" current span
        parent_span_for_tasks.follows_from(tracing::Span::current());
    }

    // acquire a permit for discovery
    let permit = ctx.handle.scheduler.acquire_permit().await?;

    // make initial discovery about the object size, metadata, possibly first chunk
    let mut discovery = discover_obj(&ctx, &input).await?;
    let _ = object_meta_tx.send(discovery.object_meta);

    let initial_chunk = discovery.initial_chunk.take();

    let mut tasks = tasks.lock().await;
    // spawn a task (if necessary) to handle the discovery chunk. This returns immediately so
    // that we can begin concurrently downloading any remaining chunks/parts ASAP
    let start_seq = handle_discovery_chunk(
        &mut tasks,
        ctx.clone(),
        initial_chunk,
        &comp_tx,
        permit,
        parent_span_for_tasks.clone(),
        discovery.chunk_meta,
    );

    if !discovery.remaining.is_empty() {
        distribute_work(
            &mut tasks,
            ctx.clone(),
            discovery.remaining,
            input,
            start_seq,
            comp_tx,
            parent_span_for_tasks,
        );
    }
    Ok(())
}

/// Handle possibly sending the first chunk of data received through discovery. Returns
/// the starting sequence number to use for remaining chunks.
///
/// NOTE: This function does _not_ wait to read the initial chunk from discovery but
/// instead spawns a new task to read the stream and send it over the body channel.
/// This allows remaining work to start immediately (and concurrently) without
/// waiting for the first chunk.
fn handle_discovery_chunk(
    tasks: &mut task::JoinSet<()>,
    ctx: DownloadContext,
    initial_chunk: Option<ByteStream>,
    completed: &mpsc::Sender<Result<ChunkOutput, crate::error::Error>>,
    permit: OwnedWorkPermit,
    parent_span_for_tasks: tracing::Span,
    metadata: Option<ChunkMetadata>,
) -> u64 {
    if let Some(stream) = initial_chunk {
        let seq = ctx.next_seq();
        let completed = completed.clone();
        // spawn a task to actually read the discovery chunk without waiting for it so we
        // can get started sooner on any remaining work (if any)
        tasks.spawn(async move {
            let chunk = AggregatedBytes::from_byte_stream(stream)
                .await
                .map(|aggregated| ChunkOutput {
                    seq,
                    data: aggregated,
                    metadata: metadata.expect("chunk metadata is available"),
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
        }.instrument(tracing::debug_span!(parent: parent_span_for_tasks.clone(), "collect-body-from-discovery", seq)));
    }
    ctx.current_seq()
}

/// Download operation specific state
#[derive(Debug)]
pub(crate) struct DownloadState {
    current_seq: AtomicU64,
}

type DownloadContext = TransferContext<DownloadState>;

impl DownloadContext {
    fn new(handle: Arc<crate::client::Handle>) -> Self {
        let state = Arc::new(DownloadState {
            current_seq: AtomicU64::new(0),
        });
        TransferContext { handle, state }
    }

    /// The target part size to use for this download
    fn target_part_size_bytes(&self) -> u64 {
        self.handle.download_part_size_bytes()
    }

    /// Returns the next seq to download
    fn next_seq(&self) -> u64 {
        self.state.current_seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns the current seq
    fn current_seq(&self) -> u64 {
        self.state.current_seq.load(Ordering::SeqCst)
    }
}
