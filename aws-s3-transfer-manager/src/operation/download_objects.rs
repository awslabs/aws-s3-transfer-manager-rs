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

mod list_objects;
mod worker;

use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tokio::{fs, task::JoinSet};
use tracing::Instrument;

use crate::{error, types::FailedDownloadTransfer};

use super::TransferContext;

/// Operation struct for downloading multiple objects from Amazon S3
#[derive(Clone, Default, Debug)]
pub(crate) struct DownloadObjects;

impl DownloadObjects {
    /// Execute a single `DownloadObjects` transfer operation
    pub(crate) async fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: crate::operation::download_objects::DownloadObjectsInput,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        //  validate existence of destination and return error if it's not a directory
        let destination = input.destination().expect("destination set");
        validate_destination(destination).await?;

        // create span to serve as parent of spawned child tasks
        let parent_span_for_tasks = tracing::debug_span!(
            parent: None,
            "download-objects-tasks",
            bucket = input.bucket().unwrap_or_default(),
            destination = input.destination().map(|p| p.to_str().unwrap_or_default()).unwrap_or_default(),
            key_prefix = input.key_prefix().unwrap_or_default(),
        );
        parent_span_for_tasks.follows_from(tracing::Span::current());

        let concurrency = handle.num_workers();
        let ctx = DownloadObjectsContext::new(handle.clone(), input);

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let mut tasks = JoinSet::new();
        let (work_tx, work_rx) = async_channel::bounded(concurrency);

        // spawn worker to discover/distribute work
        tasks.spawn(
            worker::discover_objects(ctx.clone(), work_tx)
                .instrument(parent_span_for_tasks.clone()),
        );

        for _ in 0..concurrency {
            let worker = worker::download_objects(ctx.clone(), work_rx.clone());
            tasks.spawn(worker.instrument(parent_span_for_tasks.clone()));
        }

        let handle = DownloadObjectsHandle { tasks, ctx };
        Ok(handle)
    }
}

async fn validate_destination(path: &Path) -> Result<(), error::Error> {
    let meta = fs::metadata(path).await?;

    if !meta.is_dir() {
        return Err(error::invalid_input(format!(
            "destination is not a directory: {path:?}"
        )));
    }

    Ok(())
}

/// DownloadObjects operation specific state
#[derive(Debug)]
pub(crate) struct DownloadObjectsState {
    input: DownloadObjectsInput,
    failed_downloads: Mutex<Option<Vec<FailedDownloadTransfer>>>,
    successful_downloads: AtomicU64,
    total_bytes_transferred: AtomicU64,
}

type DownloadObjectsContext = TransferContext<DownloadObjectsState>;

impl DownloadObjectsContext {
    fn new(handle: Arc<crate::client::Handle>, input: DownloadObjectsInput) -> Self {
        let state = Arc::new(DownloadObjectsState {
            input,
            failed_downloads: Mutex::new(None),
            successful_downloads: AtomicU64::default(),
            total_bytes_transferred: AtomicU64::default(),
        });
        TransferContext { handle, state }
    }
}
