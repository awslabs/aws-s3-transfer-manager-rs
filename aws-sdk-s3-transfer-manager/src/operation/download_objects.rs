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

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation.
    ///
    /// Destination validation (exists, is a directory) is performed
    /// synchronously. Walker errors surface from `handle.join()`.
    ///
    /// `events` is `None` unless a caller registered a sink. The root is announced
    /// before `enqueue_transfer`, so no child event can precede it: children are
    /// only created from `poll_work`, which cannot run until the transfer is in the
    /// scheduler.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadObjectsInput,
        events: Option<crate::events::TransferEventSink>,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        // Destination presence is validated here (cheap, no I/O); directory
        // validation is deferred to the state machine's first walker advance so
        // the blocking stat runs during work-item execution, not on the
        // caller's thread. That error surfaces from `handle.join()`.
        if input.destination().is_none() {
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

        // Only the prefix. Folder markers are dropped where the walk is drained, so the rule
        // holds for a caller-supplied walker too -- see `is_folder_marker`.
        let walker = input.walker().cloned().unwrap_or_else(|| {
            let mut builder = S3Walker::builder();
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

        // The root's ends are the prefix being downloaded and the destination
        // directory — the two halves of what the operation was asked to do, so a
        // consumer can label the operation before any child appears. Its bucket is
        // interned here and cloned per child, not re-interned per entry.
        let lifecycle = events.map(|sink| {
            Arc::new(crate::events::TransferLifecycle::new(
                sink,
                ctx.id.id,
                None,
                crate::events::TransferRef::download(
                    crate::events::Endpoint::S3 {
                        bucket: Arc::from(bucket.as_str()),
                        key: Arc::from(input.key_prefix().unwrap_or_default()),
                    },
                    crate::events::Endpoint::Local {
                        path: Arc::from(input.destination().expect("destination validated above")),
                    },
                ),
                Some(ctx.view()),
            ))
        });

        let transfer =
            DownloadObjectsTransfer::new(ctx, &input, walk, pipeline_depth, lifecycle.clone());

        if let Some(lc) = &lifecycle {
            lc.announce();
        }

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadObjectsHandle {
            completion_rx: Some(completion_rx),
            transfer,
        })
    }
}
