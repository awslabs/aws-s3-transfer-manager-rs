/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::{atomic::AtomicU64, Arc, Mutex};

/// Operation builders
pub mod builders;

mod input;
pub use input::{UploadObjectsInput, UploadObjectsInputBuilder};

mod handle;
pub use handle::UploadObjectsHandle;

mod output;
pub use output::{UploadObjectsOutput, UploadObjectsOutputBuilder};
use tokio::task::JoinSet;
use tracing::Instrument;

mod worker;

use crate::types::FailedUploadTransfer;

use super::{validate_target_is_dir, TransferContext};

/// Operation struct for uploading multiple objects to Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct UploadObjects;

impl UploadObjects {
    /// Execute a single `UploadObjects` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: UploadObjectsInput,
    ) -> Result<UploadObjectsHandle, crate::error::Error> {
        //  validate existence of source and return error if it's not a directory
        let source = input.source().expect("source set");
        validate_target_is_dir(source).await?;

        let concurrency = handle.num_workers();
        let ctx = UploadObjectsContext::new(handle.clone(), input);

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let mut tasks = JoinSet::new();
        let (work_tx, work_rx) = async_channel::bounded(concurrency);

        // spawn worker to discover/distribute work
        tasks.spawn(worker::list_directory_contents(
            ctx.state.input.clone(),
            work_tx,
        ));

        for i in 0..concurrency {
            let worker = worker::upload_objects(ctx.clone(), work_rx.clone())
                .instrument(tracing::debug_span!("object-uploader", worker = i));
            tasks.spawn(worker);
        }

        let handle = UploadObjectsHandle { tasks, ctx };
        Ok(handle)
    }
}

/// DownloadObjects operation specific state
#[derive(Debug)]
pub(crate) struct UploadObjectsState {
    input: UploadObjectsInput,
    failed_uploads: Mutex<Option<Vec<FailedUploadTransfer>>>,
    successful_uploads: AtomicU64,
    total_bytes_transferred: AtomicU64,
}

type UploadObjectsContext = TransferContext<UploadObjectsState>;

impl UploadObjectsContext {
    fn new(handle: Arc<crate::client::Handle>, input: UploadObjectsInput) -> Self {
        let state = Arc::new(UploadObjectsState {
            input,
            failed_uploads: Mutex::new(None),
            successful_uploads: AtomicU64::default(),
            total_bytes_transferred: AtomicU64::default(),
        });
        TransferContext { handle, state }
    }
}
