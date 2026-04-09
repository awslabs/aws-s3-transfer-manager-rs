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

pub(crate) mod discovery;

mod handle;
pub use handle::DownloadHandle;
pub(crate) use handle::DownloadHandleInner;
pub use handle::ManagedDownloadHandle;

mod output;
pub use output::DownloadOutput;

pub(crate) mod transfer;
pub(crate) use transfer::DownloadTransfer;

/// Provides metadata for each chunk during an object download.
mod chunk_meta;
pub use chunk_meta::ChunkMetadata;

/// Provides metadata for a single S3 object during download.
mod object_meta;
pub use object_meta::ObjectMetadata;

use crate::error;
use crate::operation::download::body::{new_slot_body, DEFAULT_BODY_SLOT_CAPACITY};
use crate::types::BucketType;
use std::sync::Arc;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

impl Download {
    /// Execute a single `Download` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        _use_current_span_as_parent_for_tasks: bool,
    ) -> Result<DownloadHandle, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, consumer) = new_slot_body(DEFAULT_BODY_SLOT_CAPACITY);

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let transfer = DownloadTransfer::new(ctx.clone(), bucket_type, input, writer);
        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadHandle::new(transfer, consumer, completion_rx))
    }

    /// Orchestrate a download that writes to a file path (temp file + rename).
    #[cfg(any(unix, windows))]
    pub(crate) async fn orchestrate_to_path(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        dest_path: std::path::PathBuf,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        // Generate temp file in the same directory as destination
        let unique_id = fastrand::u32(..);
        let temp_name = format!(
            "{}.s3tmp.{:08x}",
            dest_path.file_name().unwrap_or_default().to_string_lossy(),
            unique_id
        );
        let temp_path = dest_path.with_file_name(temp_name);

        let tokio_file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|e| error::from_kind(error::ErrorKind::IOError)(e))?;
        let file = tokio_file.into_std().await;

        let range_start = object_range_start_from_input(&input);
        let inner = Self::orchestrate_with_sink(handle, input, file, range_start, true)?;
        Ok(ManagedDownloadHandle::new(inner, temp_path, dest_path))
    }

    /// Orchestrate a download that writes to a caller-provided file.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_to_file(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        let range_start = object_range_start_from_input(&input);
        let inner = Self::orchestrate_with_sink(handle, input, file, range_start, false)?;
        // No temp/dest paths — caller manages the file lifecycle
        Ok(ManagedDownloadHandle::new_unmanaged(inner))
    }

    /// Shared orchestration for file-sink downloads.
    #[cfg(any(unix, windows))]
    fn orchestrate_with_sink(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
        object_range_start: u64,
        owns_file: bool,
    ) -> Result<DownloadHandleInner, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, _consumer) = body::new_slot_body_with_sink(
            DEFAULT_BODY_SLOT_CAPACITY,
            file,
            object_range_start,
            owns_file,
        );

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let transfer = DownloadTransfer::new(ctx.clone(), bucket_type, input, writer);
        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadHandleInner {
            transfer,
            completion_rx: Some(completion_rx),
        })
    }
}

/// Extract the byte range start from the user's range header, if present.
/// Returns 0 for no range or non-inclusive ranges (suffix, open-ended).
fn object_range_start_from_input(input: &DownloadInput) -> u64 {
    use crate::http::header;
    use std::str::FromStr;
    input
        .range()
        .and_then(|r| header::Range::from_str(r).ok())
        .map(|r| match r.0 {
            header::ByteRange::Inclusive(start, _) => start,
            _ => 0,
        })
        .unwrap_or(0)
}
