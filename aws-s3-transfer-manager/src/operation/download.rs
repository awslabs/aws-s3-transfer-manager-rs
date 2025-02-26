/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

use aws_sdk_s3::error::DisplayErrorContext;
/// Request type for dowloading a single object from Amazon S3
pub use input::{DownloadInput, DownloadInputBuilder};

/// Operation builders
pub mod builders;

/// Abstractions for responses and consuming data streams.
mod body;
pub use body::{Body, ChunkOutput};

mod discovery;

mod handle;
pub use handle::DownloadHandle;
use tracing::Instrument;

/// Provides metadata for each chunk during an object download.
mod chunk_meta;
pub use chunk_meta::ChunkMetadata;
/// Provides metadata for a single S3 object during download.
mod object_meta;
pub use object_meta::ObjectMetadata;

mod service;

use crate::error;
use crate::io::AggregatedBytes;
use crate::runtime::scheduler::{
    NetworkPermitContext, OwnedWorkPermit, PermitType, TransferDirection,
};
use crate::types::BucketType;
use aws_smithy_types::byte_stream::ByteStream;
use discovery::discover_obj;
use service::distribute_work;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex, OnceCell};
use tokio::task::{self, JoinSet};

use super::{CancelNotificationReceiver, CancelNotificationSender, TransferContext};

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
    /// Use this for `TransferManager.download().initiate()`, where the spawned tasks
    /// should NOT extend the life of the current `initiate()` span.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download::DownloadInput,
        use_current_span_as_parent_for_tasks: bool,
    ) -> Result<DownloadHandle, error::Error> {
        // if there is a part number then just send the default request
        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type = if input.bucket().unwrap_or("").ends_with("--x-s3") {
            BucketType::Express
        } else {
            BucketType::Standard
        };

        let ctx = DownloadContext::new(handle, bucket_type);
        let concurrency = ctx.handle.num_workers();
        let (chunk_tx, chunk_rx) = mpsc::channel(concurrency);
        let (object_meta_tx, object_meta_rx) = oneshot::channel();

        let tasks = Arc::new(Mutex::new(JoinSet::new()));
        let discovery = tokio::spawn(send_discovery(
            tasks.clone(),
            ctx.clone(),
            chunk_tx,
            object_meta_tx,
            input,
            use_current_span_as_parent_for_tasks,
        ));

        Ok(DownloadHandle {
            body: Body::new(chunk_rx),
            tasks,
            discovery,
            object_meta_rx: Mutex::new(Some(object_meta_rx)),
            object_meta: OnceCell::new(),
        })
    }
}

async fn send_discovery(
    tasks: Arc<Mutex<task::JoinSet<()>>>,
    ctx: DownloadContext,
    chunk_tx: mpsc::Sender<Result<ChunkOutput, crate::error::Error>>,
    object_meta_tx: oneshot::Sender<ObjectMetadata>,
    mut input: DownloadInput,
    use_current_span_as_parent_for_tasks: bool,
) {
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

    // FIXME - move acquire permit to where it's used in discovery (where we know the payload size)
    // acquire a permit for discovery
    let permit = ctx
        .handle
        .scheduler
        .acquire_permit(PermitType::Network(NetworkPermitContext {
            payload_size_estimate: 0,
            bucket_type: ctx.bucket_type(),
            direction: TransferDirection::Download,
        }))
        .await;
    let permit = match permit {
        Ok(permit) => permit,
        Err(err) => {
            if chunk_tx.send(Err(err)).await.is_err() {
                tracing::debug!("Download handle for key({:?}) has been dropped, aborting during the discovery phase", input.key);
            }
            return;
        }
    };

    // make initial discovery about the object size, metadata, possibly first chunk
    let discovery = discover_obj(&ctx, &input).await;
    let mut discovery = match discovery {
        Ok(discovery) => discovery,
        Err(err) => {
            if chunk_tx.send(Err(err)).await.is_err() {
                tracing::debug!("Download handle for key({:?}) has been dropped, aborting during the discovery phase", input.key);
            }
            return;
        }
    };

    // Add if_match to the rest of the requests using the etag
    // we got from discovery to ensure the object stays the same
    // during the download process.
    input.if_match.clone_from(&discovery.object_meta.e_tag);

    if object_meta_tx.send(discovery.object_meta).is_err() {
        tracing::debug!(
            "Download handle for key({:?}) has been dropped, aborting during the discovery phase",
            input.key
        );
        return;
    }

    let initial_chunk = discovery.initial_chunk.take();

    let mut tasks = tasks.lock().await;
    // spawn a task (if necessary) to handle the discovery chunk. This returns immediately so
    // that we can begin concurrently downloading any remaining chunks/parts ASAP
    let start_seq = handle_discovery_chunk(
        &mut tasks,
        ctx.clone(),
        initial_chunk,
        &chunk_tx,
        permit,
        parent_span_for_tasks.clone(),
        discovery.chunk_meta,
    );

    match discovery.remaining {
        Some(remaining) if !remaining.is_empty() => {
            distribute_work(
                &mut tasks,
                ctx.clone(),
                remaining,
                input,
                start_seq,
                chunk_tx,
                parent_span_for_tasks,
            );
        }
        _ => {}
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
    cancel_tx: CancelNotificationSender,
    cancel_rx: CancelNotificationReceiver,
    bucket_type: BucketType,
}

type DownloadContext = TransferContext<DownloadState>;

impl DownloadContext {
    fn new(handle: Arc<crate::client::Handle>, bucket_type: BucketType) -> Self {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let state = Arc::new(DownloadState {
            current_seq: AtomicU64::new(0),
            cancel_tx,
            cancel_rx,
            bucket_type,
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

    /// Returns the type of bucket targeted by this operation
    fn bucket_type(&self) -> BucketType {
        self.state.bucket_type
    }
}
