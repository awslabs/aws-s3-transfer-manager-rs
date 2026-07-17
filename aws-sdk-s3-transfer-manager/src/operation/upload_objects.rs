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
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: UploadObjectsInput,
    ) -> Result<UploadObjectsHandle, crate::error::Error> {
        let source = input.source().expect("source set");

        let walker = input.walker().cloned().unwrap_or_default();
        let walk = walker.walk(FsWalkContext::builder().root(source).build());

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let transfer = UploadObjectsTransfer::new(ctx, input, walk);

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(UploadObjectsHandle {
            completion_rx: Some(completion_rx),
            transfer,
        })
    }
}
