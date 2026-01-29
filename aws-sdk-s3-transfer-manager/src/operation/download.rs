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
pub(crate) use context::{DownloadContext, DownloadState, DownloadWorkState};

pub(crate) mod discovery;

mod handle;
pub use handle::DownloadHandle;

/// Provides metadata for each chunk during an object download.
mod chunk_meta;
pub use chunk_meta::ChunkMetadata;

/// Provides metadata for a single S3 object during download.
mod object_meta;
pub use object_meta::ObjectMetadata;

use crate::error;
use crate::types::BucketType;
use std::sync::Arc;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    ///
    /// TODO(redux): Wire to scheduler. Key behaviors from old impl to preserve:
    /// - Discovery via HeadObject or first ranged GET (see discovery.rs)
    /// - if_match on subsequent requests using ETag from discovery
    /// - Cancellation propagates to in-flight requests
    /// - Body read errors (ByteStreamError) need retry (SDK doesn't retry these)
    /// - Chunks sent to Body channel with seq for reordering
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        _use_current_span_as_parent_for_tasks: bool,
    ) -> Result<DownloadHandle, error::Error> {
        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        // TODO(redux): Get real TransferId from scheduler
        let id = crate::scheduler::TransferId {
            id: 0,
            parent: None,
        };

        let ctx = DownloadContext::new(id, handle, bucket_type, input);

        Ok(DownloadHandle::new(ctx))
    }
}
