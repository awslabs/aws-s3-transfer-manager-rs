/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;

mod input;
/// Input type for downloading multiple objects from Amazon S3
pub use input::{DownloadObjectsInput, DownloadObjectsInputBuilder};
mod output;
/// Output type for downloading multiple objects from Amazon S3
pub use output::{DownloadObjectsOutput, DownloadObjectsOutputBuilder};

mod handle;
pub use handle::DownloadObjectsHandle;

pub(crate) mod transfer;
use transfer::DownloadObjectsTransfer;

use std::sync::Arc;

use crate::io::walk::{S3WalkContext, S3Walker};
use crate::transfer::TransferContext;

use super::validate_target_is_dir;

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation.
    ///
    /// Destination validation (exists, is a directory) is performed
    /// synchronously. Walker errors surface from `handle.join()`.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadObjectsInput,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let destination = input
            .destination()
            .ok_or_else(|| crate::error::invalid_input("destination is required"))?;
        // TODO(vnext): revisit sync I/O in initiate path. This runs on the
        // caller's thread — acceptable for a single stat() but worth
        // reconsidering if initiate grows more blocking work.
        let metadata = std::fs::metadata(destination)?;
        validate_target_is_dir(&metadata, destination)?;

        let bucket = input
            .bucket()
            .ok_or_else(|| crate::error::invalid_input("bucket is required"))?
            .to_string();
        let pipeline_depth = input
            .max_concurrent_downloads()
            .unwrap_or(super::DEFAULT_MAX_CONCURRENT_CHILDREN);

        let walker = input.walker().cloned().unwrap_or_else(|| {
            let mut builder =
                S3Walker::builder().filter(crate::io::walk::exclude_s3_folder_markers);
            if let Some(prefix) = input.key_prefix() {
                builder = builder.prefix(prefix);
            }
            builder.build()
        });

        let s3_client = handle.s3_client.clone();
        let walk_ctx = S3WalkContext::builder()
            .client(s3_client)
            .bucket(&bucket)
            .build();
        let walk = walker.walk(walk_ctx);

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let transfer = DownloadObjectsTransfer::new(ctx, &input, walk, pipeline_depth);

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadObjectsHandle {
            completion_rx: Some(completion_rx),
            transfer,
        })
    }
}
