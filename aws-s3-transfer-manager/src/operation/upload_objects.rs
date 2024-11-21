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
use tokio::{
    sync::watch::{self, Receiver, Sender},
    task::JoinSet,
};
use tracing::Instrument;

mod worker;

use crate::{error, types::FailedUpload};

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
        //  validate existence of source, possibly a symlink, and return error if it's not a directory
        let source = input.source().expect("source set");
        let symlink_metadata = tokio::fs::symlink_metadata(source).await?;
        if let Err(e) = validate_target_is_dir(&symlink_metadata, source) {
            if symlink_metadata.is_symlink() {
                if input.follow_symlinks() {
                    let metadata = tokio::fs::metadata(source).await?;
                    validate_target_is_dir(&metadata, source)?;
                } else {
                    return Err(error::invalid_input(
                        format!("{source:?} is a symbolic link to a directory but the current upload operation does not follow symbolic links"))
                    );
                }
            } else {
                return Err(e);
            }
        };
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let concurrency = handle.num_workers();
        let ctx = UploadObjectsContext::new(handle.clone(), input, cancel_tx, cancel_rx);

        // spawn all work into the same JoinSet such that when the set is dropped all tasks are cancelled.
        let mut tasks = JoinSet::new();
        let (list_directory_tx, list_directory_rx) = async_channel::bounded(concurrency);

        // spawn worker to discover/distribute work
        tasks.spawn(worker::list_directory_contents(
            ctx.state.clone(),
            list_directory_tx,
        ));

        for i in 0..concurrency {
            let worker = worker::upload_objects(ctx.clone(), list_directory_rx.clone())
                .instrument(tracing::debug_span!("object-uploader", worker = i));
            tasks.spawn(worker);
        }

        let handle = UploadObjectsHandle { tasks, ctx };
        Ok(handle)
    }
}

/// UploadObjects operation specific state
#[derive(Debug)]
pub(crate) struct UploadObjectsState {
    // TODO - Determine if `input` should be separated from this struct
    // https://github.com/awslabs/aws-s3-transfer-manager-rs/pull/67#discussion_r1821661603
    input: UploadObjectsInput,
    cancel_tx: Sender<bool>,
    cancel_rx: Receiver<bool>,
    failed_uploads: Mutex<Vec<FailedUpload>>,
    successful_uploads: AtomicU64,
    total_bytes_transferred: AtomicU64,
}

impl UploadObjectsState {
    pub(crate) fn new(
        input: UploadObjectsInput,
        cancel_tx: Sender<bool>,
        cancel_rx: Receiver<bool>,
    ) -> Self {
        Self {
            input,
            cancel_tx,
            cancel_rx,
            failed_uploads: Mutex::new(Vec::new()),
            successful_uploads: AtomicU64::default(),
            total_bytes_transferred: AtomicU64::default(),
        }
    }
}

type UploadObjectsContext = TransferContext<UploadObjectsState>;

impl UploadObjectsContext {
    fn new(
        handle: Arc<crate::client::Handle>,
        input: UploadObjectsInput,
        cancel_tx: Sender<bool>,
        cancel_rx: Receiver<bool>,
    ) -> Self {
        let state = Arc::new(UploadObjectsState::new(input, cancel_tx, cancel_rx));
        TransferContext { handle, state }
    }
}
