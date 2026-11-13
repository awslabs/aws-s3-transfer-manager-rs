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

mod observability;
pub(crate) mod transfer;
use transfer::DownloadObjectsTransfer;

use std::sync::Arc;

use crate::config::user_agent::OperationFeatures;
use crate::io::walk::S3WalkContext;
use crate::transfer::TransferContext;
use aws_runtime::sdk_feature::AwsSdkFeature;

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation.
    ///
    /// Required inputs are checked synchronously. Preparing the destination
    /// directory (checking it, or creating it) and walker errors surface from
    /// `handle.join()`.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadObjectsInput,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        // Destination presence is validated here (cheap, no I/O); preparing the
        // directory is deferred to the state machine's first walker advance so
        // the blocking filesystem calls run during work-item execution, not on
        // the caller's thread. That error surfaces from `handle.join()`.
        //
        // An empty path counts as unset: `create_dir_all("")` succeeds and
        // keys joined to it are relative, so it would download into the
        // current working directory.
        if input
            .destination()
            .is_none_or(|destination| destination.as_os_str().is_empty())
        {
            return Err(crate::error::invalid_input("destination is required"));
        }

        let bucket = input
            .bucket()
            .ok_or_else(|| crate::error::invalid_input("bucket is required"))?
            .to_string();
        // Clamp to 1: an explicit 0 would wedge the transfer by never
        // admitting children (mirrors the global ConcurrencyMode::Explicit guard).
        let pipeline_depth = input
            .max_concurrent_downloads()
            .map(|n| n.max(1))
            .unwrap_or(super::DEFAULT_MAX_CONCURRENT_CHILDREN);

        let walker = input.walker().cloned().unwrap_or_default();

        let features = OperationFeatures::new([AwsSdkFeature::S3TransferDownloadDirectory]);
        let s3_client = features.apply_to_client(&handle.s3_client);
        let walk_ctx = S3WalkContext::builder()
            .sdk_client(s3_client)
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
