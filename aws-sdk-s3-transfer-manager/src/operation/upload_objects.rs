/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;

mod input;
pub use input::{UploadObjectsInput, UploadObjectsInputBuilder};

mod handle;
pub use handle::UploadObjectsHandle;

mod output;
pub use output::{UploadObjectsOutput, UploadObjectsOutputBuilder};

mod transfer;
pub(crate) use transfer::UploadObjectsTransfer;

use std::sync::Arc;

use crate::io::walk::FsWalkContext;
use crate::transfer::TransferContext;

/// Operation struct for uploading multiple objects to Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct UploadObjects;

impl UploadObjects {
    /// Execute a single `UploadObjects` transfer operation.
    ///
    /// Source validation (not a directory, symlinked root with
    /// `follow_symlinks` disabled, etc.) is handled by the walker. The error
    /// surfaces from `handle.join()` as `ErrorKind::IOError`.
    /// `events` is `None` unless a caller registered a sink.
    ///
    /// The root is announced before `enqueue_transfer`, so no child event can
    /// precede it: children are only created from `poll_work`, which cannot run
    /// until the transfer is in the scheduler.
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: UploadObjectsInput,
        events: Option<crate::events::TransferEventSink>,
    ) -> Result<UploadObjectsHandle, crate::error::Error> {
        let source = input.source().expect("source set");

        let walker = input.walker().cloned().unwrap_or_default();
        let walk = walker.walk(FsWalkContext::builder().root(source).build());

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        // The root's ends are the two halves of what the operation was asked to do
        // -- the source directory and the key prefix -- so a consumer can label the
        // operation before any child appears. Its bucket is interned here and
        // cloned per child, not re-interned per entry.
        let lifecycle = events.map(|sink| {
            Arc::new(crate::events::TransferLifecycle::new(
                sink,
                ctx.id.id,
                None,
                crate::events::TransferRef::upload(
                    crate::events::Endpoint::Local {
                        path: Arc::from(source),
                    },
                    crate::events::Endpoint::S3 {
                        bucket: Arc::from(input.bucket().unwrap_or_default()),
                        key: Arc::from(input.key_prefix().unwrap_or_default()),
                    },
                ),
            ))
        });

        let transfer = UploadObjectsTransfer::new(ctx, input, walk, lifecycle.clone());

        if let Some(lc) = &lifecycle {
            lc.announce();
        }

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(UploadObjectsHandle {
            completion_rx: Some(completion_rx),
            transfer,
        })
    }
}
