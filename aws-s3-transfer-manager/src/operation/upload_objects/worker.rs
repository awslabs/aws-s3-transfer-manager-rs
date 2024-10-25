/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::borrow::Cow;
use std::path::MAIN_SEPARATOR;
use std::sync::atomic::Ordering;

use super::{UploadObjectsContext, UploadObjectsInput};
use async_channel::{Receiver, Sender};
use walkdir::WalkDir;

use crate::io::InputStream;
use crate::operation::upload::UploadInputBuilder;
use crate::operation::DEFAULT_DELIMITER;
use crate::types::UploadFilter;
use crate::{error, types::UploadFilterItem};

#[derive(Debug)]
pub(super) struct UploadObjectJob {
    key: String,
    object: InputStream,
}

impl UploadObjectJob {
    fn new(key: String, object: InputStream) -> Self {
        Self { key, object }
    }
}

fn walker(input: &UploadObjectsInput) -> WalkDir {
    let source = input.source().unwrap();
    let mut walker = WalkDir::new(source);
    if input.follow_symlinks() {
        walker = walker.follow_links(true);
    }
    if !input.recursive() {
        walker = walker.max_depth(1);
    }
    walker
}

pub(super) async fn list_directory_contents(
    ctx: UploadObjectsContext,
    work_tx: Sender<UploadObjectJob>,
) -> Result<(), error::Error> {
    let walker = walker(&ctx.state.input);

    let default_filter = &UploadFilter::default();
    let filter = ctx.state.input.filter().unwrap_or(default_filter);

    for entry in walker {
        let entry = entry.unwrap();
        if !(filter.predicate)(&UploadFilterItem::new(
            entry.path(),
            tokio::fs::metadata(entry.path()).await?,
        )) {
            tracing::debug!("skipping object due to filter: {:?}", entry.path());
            continue;
        }

        let recursion_root_dir_path = ctx.state.input.source().expect("source set");
        let entry_path = entry.path();
        let relative_filename = entry_path
            .strip_prefix(recursion_root_dir_path)
            .expect("{entry_path:?} should be a path entry directly or indirectly under {recursion_root_dir_path:?}")
            .to_str()
            .expect("valid utf-8 path");

        let object_key = derive_object_key(
            relative_filename,
            ctx.state.input.key_prefix(),
            ctx.state.input.delimiter(),
        )
        .unwrap();
        tracing::info!("uploading {relative_filename} with object key {object_key}...");

        let job = UploadObjectJob::new(
            object_key.into_owned(),
            InputStream::from_path(entry.path()).unwrap(),
        );
        work_tx.send(job).await.expect("channel valid");
    }

    Ok(())
}

pub(super) async fn upload_objects(
    ctx: UploadObjectsContext,
    work_rx: Receiver<UploadObjectJob>,
) -> Result<(), error::Error> {
    while let Ok(job) = work_rx.recv().await {
        let bytes_transferred: u64 = job
            .object
            .size_hint()
            .upper()
            .ok_or_else(crate::io::error::Error::upper_bound_size_hint_required)
            .unwrap();
        let key = job.key.clone();
        let result = upload_single_obj(&ctx, job).await;
        match result {
            Ok(_) => {
                ctx.state.successful_uploads.fetch_add(1, Ordering::SeqCst);

                ctx.state
                    .total_bytes_transferred
                    .fetch_add(bytes_transferred, Ordering::SeqCst);

                tracing::debug!("worker finished uploading object {:?}", key);
            }
            Err(err) => {
                tracing::debug!("worker failed to upload object {:?}: {}", key, err);
            }
        }
    }

    tracing::trace!("req channel closed, worker finished");
    Ok(())
}

async fn upload_single_obj(
    ctx: &UploadObjectsContext,
    job: UploadObjectJob,
) -> Result<(), error::Error> {
    let input = UploadInputBuilder::default()
        .set_bucket(ctx.state.input.bucket.to_owned())
        .set_body(Some(job.object))
        .set_key(Some(job.key))
        .build()
        .expect("valid input");

    let handle = crate::operation::upload::Upload::orchestrate(ctx.handle.clone(), input).await?;

    handle.join().await?;

    Ok(())
}

fn derive_object_key<'a>(
    relative_filename: &'a str,
    object_key_prefix: Option<&str>,
    object_key_delimiter: Option<&str>,
) -> Result<Cow<'a, str>, error::Error> {
    if let Some(delim) = object_key_delimiter {
        if relative_filename.contains(delim) {
            return Err(error::invalid_input(format!(
                "a custom delimiter {delim} should not appear in {relative_filename}"
            )));
        }
    }

    let delim = object_key_delimiter.unwrap_or(DEFAULT_DELIMITER);

    let relative_filename = if delim.starts_with(MAIN_SEPARATOR) {
        Cow::Borrowed(relative_filename)
    } else {
        Cow::Owned(relative_filename.replace(MAIN_SEPARATOR, delim))
    };

    let object_key = if let Some(prefix) = object_key_prefix {
        if prefix.ends_with(delim) {
            Cow::Owned(format!("{prefix}{relative_filename}"))
        } else {
            Cow::Owned(format!("{prefix}{delim}{relative_filename}"))
        }
    } else {
        relative_filename
    };

    Ok(object_key)
}
