/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download a single S3 object using modeled request and response metadata.
//!
//! [`DownloadInput`](crate::operation::download::DownloadInput) and the metadata
//! types are re-exported from [`crate::model`].
//! Object metadata describes the discovery response (GET or HEAD), while each
//! [`ChunkOutput`](crate::operation::download::ChunkOutput) carries metadata from
//! its own GET response. Optional service
//! values remain absent when the corresponding response does not include them.
//! Reported checksum fields alone do not mean that the downloaded bytes were
//! validated; use
//! [`DownloadOutput::integrity_checks`](crate::operation::download::DownloadOutput::integrity_checks)
//! for that result.

pub use crate::model::builders::DownloadInputBuilder;
pub use crate::model::{ChunkMetadata, DownloadInput, ObjectMetadata};

/// Operation builders
pub mod builders;

/// Abstractions for responses and consuming data streams.
mod body;
pub use body::{Body, ChunkOutput};

/// In-order delivery buffer (out-of-order arrival → in-order stream).
pub(crate) mod recv_buffer;

/// Read-ahead window — the occupancy bound on speculative issuance.
pub(crate) mod read_ahead;

mod context;
mod observability;

pub(crate) mod discovery;

mod handle;
pub use handle::DownloadHandle;
pub(crate) use handle::DownloadHandleInner;
pub use handle::DownloadIoCtl;
pub use handle::ManagedDownloadHandle;

mod output;
pub use output::DownloadOutput;

pub(crate) mod transfer;
pub(crate) use transfer::DownloadTransfer;

mod object_meta;

use crate::error;
use crate::operation::download::body::new_recv_body;
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

        let (writer, consumer) = new_recv_body();

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let transfer = DownloadTransfer::new(ctx.clone(), bucket_type, input, writer);
        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadHandle::new(transfer, consumer, completion_rx))
    }

    /// Orchestrate a download that writes to a file path (temp file + rename).
    ///
    /// When `parent_id` is `Some`, the transfer is linked as a child of the
    /// given composite transfer via [`TransferContext::new_child`](crate::transfer::TransferContext::new_child).
    #[cfg(any(unix, windows))]
    pub(crate) async fn orchestrate_to_path(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        dest_path: std::path::PathBuf,
        parent_id: Option<u64>,
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

        let inner = Self::orchestrate_with_sink(handle, input, file, true, parent_id)?;
        Ok(ManagedDownloadHandle::new(inner, temp_path, dest_path))
    }

    /// Orchestrate a download that writes to a caller-provided file.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_to_file(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        let inner = Self::orchestrate_with_sink(handle, input, file, false, None)?;
        // No temp/dest paths — caller manages the file lifecycle
        Ok(ManagedDownloadHandle::new_unmanaged(inner))
    }

    /// Shared orchestration for file-sink downloads.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_with_sink(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
        owns_file: bool,
        parent_id: Option<u64>,
    ) -> Result<DownloadHandleInner, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, _consumer) = body::new_recv_body_with_sink(file, owns_file);

        let (ctx, completion_rx) = match parent_id {
            Some(pid) => TransferContext::new_child(handle.clone(), pid),
            None => TransferContext::new(handle.clone()),
        };

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
