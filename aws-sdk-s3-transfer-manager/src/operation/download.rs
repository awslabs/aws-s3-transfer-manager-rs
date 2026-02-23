/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod input;

/// Request type for downloading a single object from Amazon S3
pub use input::{DownloadInput, DownloadInputBuilder};

/// Operation builders
pub mod builders;

/// Abstractions for responses and consuming data streams.
mod body;
pub use body::{Body, ChunkOutput};

mod context;

pub(crate) mod discovery;

mod handle;
pub use handle::DownloadHandle;

mod output;
pub use output::DownloadOutput;

pub(crate) mod transfer;
pub(crate) use transfer::DownloadTransfer;

/// Provides metadata for each chunk during an object download.
mod chunk_meta;
pub use chunk_meta::ChunkMetadata;

/// Provides metadata for a single S3 object during download.
mod object_meta;
pub use object_meta::ObjectMetadata;

use crate::error;
use crate::types::BucketType;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        _use_current_span_as_parent_for_tasks: bool,
    ) -> Result<DownloadHandle, error::Error> {
        use crate::operation::TransferContext;
        use crate::scheduler::TransferId;
        use std::sync::atomic::{AtomicU64, Ordering};

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        // TODO(redux): Consider where transfer ID generation should live
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let transfer_id = TransferId {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: None,
        };

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let concurrency = handle.num_workers();
        let (chunk_tx, chunk_rx) = mpsc::channel(concurrency);

        let (ctx, completion_rx) = TransferContext::new(transfer_id, handle.clone());

        let transfer = DownloadTransfer::new(ctx.clone(), bucket_type, input, chunk_tx);
        handle
            .new_scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadHandle::new(transfer, chunk_rx, completion_rx))
    }
}
