/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Abstractions for response bodies and consuming data streams.
pub mod body;
mod context;
mod discovery;
mod handle;
mod header;
mod object_meta;
mod worker;

use crate::client::downloader::body::Body;
use crate::client::downloader::discovery::{discover_obj, ObjectDiscovery};
use crate::client::downloader::handle::DownloadHandle;
use crate::client::downloader::worker::{distribute_work, download_chunks, ChunkResponse};
use crate::error::TransferError;
use crate::operation::download::DownloadInput;
use crate::types::{ConcurrencySetting, PartSize};
use crate::{DEFAULT_CONCURRENCY, MEBIBYTE};
use aws_types::SdkConfig;
use context::DownloadContext;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::Instrument;

// TODO(aws-sdk-rust#1159) - need to set User-Agent header value for SEP, e.g. `ft/hll#s3-transfer`

/// Fluent style builder for [Downloader]
#[derive(Debug, Clone, Default)]
pub struct Builder {
    part_size: PartSize,
    concurrency: ConcurrencySetting,
    sdk_config: Option<SdkConfig>,
}

impl Builder {
    fn new() -> Builder {
        Builder::default()
    }

    /// Size of parts the object will be downloaded in, in bytes.
    ///
    /// Defaults is [PartSize::Auto].
    pub fn part_size(mut self, target_size: PartSize) -> Self {
        self.part_size = target_size;
        self
    }

    /// Set the configuration used by the S3 client
    pub fn sdk_config(mut self, config: SdkConfig) -> Self {
        self.sdk_config = Some(config);
        self
    }

    /// Set the concurrency level this component is allowed to use.
    ///
    /// This sets the maximum number of concurrent in-flight requests.
    /// Default is [ConcurrencySetting::Auto].
    pub fn concurrency(mut self, concurrency: ConcurrencySetting) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Consumes the builder and constructs a [Downloader]
    pub fn build(self) -> Downloader {
        self.into()
    }
}

impl From<Builder> for Downloader {
    fn from(value: Builder) -> Self {
        let sdk_config = value
            .sdk_config
            .unwrap_or_else(|| SdkConfig::builder().build());
        let client = aws_sdk_s3::Client::new(&sdk_config);
        Self {
            part_size: value.part_size,
            concurrency: value.concurrency,
            client,
        }
    }
}

/// Download an object in the most efficient way possible by splitting the request into
/// concurrent requests (e.g. using ranged GET or part number).
#[derive(Debug, Clone)]
pub struct Downloader {
    part_size: PartSize,
    concurrency: ConcurrencySetting,
    client: aws_sdk_s3::client::Client,
}

impl Downloader {
    /// Create a new [Builder]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Download a single object from S3.
    ///
    /// A single logical request may be split into many concurrent ranged `GetObject` requests
    /// to improve throughput.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::error::Error;
    /// use aws_sdk_s3::operation::get_object::builders::GetObjectInputBuilder;
    /// use aws_s3_transfer_manager::client::Downloader;
    /// use aws_s3_transfer_manager::operation::download::DownloadInput;
    ///
    /// async fn get_object(client: Downloader) -> Result<(), Box<dyn Error>> {
    ///     let request = GetObjectInputBuilder::default()
    ///         .bucket("my-bucket")
    ///         .key("my-key")
    ///         .into();
    ///
    ///     let handle = client.download(request).await?;
    ///     // process data off handle...
    ///     Ok(())
    /// }
    /// ```
    pub async fn download(&self, req: DownloadInput) -> Result<DownloadHandle, TransferError> {
        // if there is a part number then just send the default request
        if req.input.get_part_number().is_some() {
            todo!("single part download not implemented")
        }

        let target_part_size_bytes = self.part_size();
        let ctx = DownloadContext {
            client: self.client.clone(),
            target_part_size_bytes,
        };

        let concurrency = self.concurrency();

        // make initial discovery about the object size, metadata, possibly first chunk
        let mut discovery = discover_obj(&ctx, &req).await?;
        let (comp_tx, comp_rx) = mpsc::channel(concurrency);
        let start_seq = handle_discovery_chunk(&mut discovery, &comp_tx).await;

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let mut tasks = JoinSet::new();

        if !discovery.remaining.is_empty() {
            // start assigning work
            let (work_tx, work_rx) = async_channel::bounded(concurrency);
            let input = req.input.clone();
            let rem = discovery.remaining.clone();

            // TODO(aws-sdk-rust#1159) - test semaphore based approach where we create all futures at once,
            //        the downside is controlling memory usage as a large download may result in
            //        quite a few futures created. If more performant could be enabled for
            //        objects less than some size.

            tasks.spawn(distribute_work(
                rem,
                input,
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

    /// Get the concrete concurrency setting
    fn concurrency(&self) -> usize {
        match self.concurrency {
            ConcurrencySetting::Auto => DEFAULT_CONCURRENCY,
            ConcurrencySetting::Explicit(explicit) => explicit,
        }
    }

    // Get the concrete part size to use in bytes
    fn part_size(&self) -> u64 {
        match self.part_size {
            PartSize::Auto => 8 * MEBIBYTE,
            PartSize::Target(explicit) => explicit,
        }
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