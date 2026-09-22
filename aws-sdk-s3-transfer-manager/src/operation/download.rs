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

/// In-order delivery buffer (out-of-order arrival → in-order stream).
pub(crate) mod recv_buffer;

/// Read-ahead window — the occupancy bound on speculative issuance.
pub(crate) mod read_ahead;

mod context;

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

/// Provides metadata for each chunk during an object download.
mod chunk_meta;
pub use chunk_meta::ChunkMetadata;

/// Provides metadata for a single S3 object during download.
mod object_meta;
pub use object_meta::ObjectMetadata;

use crate::error;
use crate::operation::download::body::new_recv_body;
use crate::types::BucketType;
use std::sync::Arc;

/// Operation struct for single object download
#[derive(Clone, Default, Debug)]
pub(crate) struct Download;

/// Where a download's events go, and the end it writes to.
///
/// One value rather than two parameters: a sink is only ever useful alongside the
/// destination to report with it, and only the entry point that chose the
/// destination knows what it is — a file for `write_to_path`, the caller's own body
/// for a streaming download.
pub(crate) struct EventRegistration {
    pub(crate) sink: crate::events::TransferEventSink,
    pub(crate) destination: crate::events::Endpoint,
}

/// The file a download writes into, and the two facts that only travel with it.
///
/// Grouped rather than passed as three parameters because none of them is meaningful
/// without the others: the offset says what the file's byte 0 means, and `owns_file`
/// says who closes it. Keeping them together is also what holds
/// [`Download::orchestrate_with_sink`] inside clippy's argument limit now that the
/// listing's `known_size` travels with it.
pub(crate) struct FileSink {
    pub(crate) file: std::fs::File,
    /// Offset in the *object* that this file's offset 0 corresponds to — non-zero only
    /// for a ranged download.
    pub(crate) object_range_start: u64,
    /// `true` when the transfer manager created the file and must clean it up (the
    /// temp-file-then-rename path); `false` when the caller opened it and owns it.
    pub(crate) owns_file: bool,
}

impl Download {
    fn lifecycle_for(
        ctx: &crate::transfer::TransferContext,
        input: &DownloadInput,
        events: Option<EventRegistration>,
    ) -> Option<Arc<crate::events::TransferLifecycle>> {
        events.map(|reg| {
            Arc::new(crate::events::TransferLifecycle::new(
                reg.sink,
                ctx.id.id,
                None,
                crate::events::TransferRef::download(
                    crate::events::Endpoint::S3 {
                        bucket: Arc::from(input.bucket().unwrap_or_default()),
                        key: Arc::from(input.key().unwrap_or_default()),
                    },
                    reg.destination,
                ),
                Some(ctx.view()),
            ))
        })
    }

    /// Execute a single `Download` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        _use_current_span_as_parent_for_tasks: bool,
        events: Option<EventRegistration>,
    ) -> Result<DownloadHandle, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, consumer) = new_recv_body();

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        // A streaming download has no local path: the caller owns the body.
        let lifecycle = Self::lifecycle_for(&ctx, &input, events);
        let transfer =
            DownloadTransfer::new(ctx.clone(), bucket_type, input, writer, lifecycle.clone());
        if let Some(lc) = &lifecycle {
            lc.announce();
        }
        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        Ok(DownloadHandle::new(transfer, consumer, completion_rx))
    }

    /// Orchestrate a download that writes to a file path (temp file + rename).
    ///
    /// When `parent` is `Some`, the transfer is linked as a child of the
    /// given composite transfer via [`TransferContext::new_child`](crate::transfer::TransferContext::new_child).
    #[cfg(any(unix, windows))]
    pub(crate) async fn orchestrate_to_path(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        dest_path: std::path::PathBuf,
        parent: Option<&crate::transfer::TransferContext>,
        events: Option<EventRegistration>,
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
        // No listing behind this entry point, so the size comes from discovery as before.
        let inner = Self::orchestrate_with_sink(
            handle,
            input,
            FileSink {
                file,
                object_range_start: range_start,
                owns_file: true,
            },
            parent,
            events,
            None,
        )?;
        Ok(ManagedDownloadHandle::new(inner, temp_path, dest_path))
    }

    /// Orchestrate a download that writes to a caller-provided file.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_to_file(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
        events: Option<EventRegistration>,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        let range_start = object_range_start_from_input(&input);
        let inner = Self::orchestrate_with_sink(
            handle,
            input,
            FileSink {
                file,
                object_range_start: range_start,
                owns_file: false,
            },
            None,
            events,
            None,
        )?;
        // No temp/dest paths — caller manages the file lifecycle
        Ok(ManagedDownloadHandle::new_unmanaged(inner))
    }

    /// Shared orchestration for file-sink downloads.
    ///
    /// `known_size` is the object's size when the caller already learned it — a composite
    /// has it from its listing. Seeding it here rather than waiting for discovery is what
    /// gives the entry a byte denominator it keeps even if its own `GetObject` never
    /// succeeds; see the note at the `set_total_bytes` call below.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_with_sink(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        sink: FileSink,
        parent: Option<&crate::transfer::TransferContext>,
        events: Option<EventRegistration>,
        known_size: Option<u64>,
    ) -> Result<DownloadHandleInner, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, _consumer) =
            body::new_recv_body_with_sink(sink.file, sink.object_range_start, sink.owns_file);

        let (ctx, completion_rx) = match parent {
            Some(parent) => TransferContext::new_child(handle.clone(), parent),
            None => TransferContext::new(handle.clone()),
        };

        // Before `enqueue_transfer`, so the value is in place by the time the transfer can be
        // polled and no reader sees an entry with no denominator at all.
        //
        // The listed size is what the entry is *expected* to move. An object refused before
        // its first body byte — a 403, an integrity failure — never reaches discovery, so
        // without this its denominator would stay `Unknown` and a consumer could not tell how
        // much that entry failed to transfer. That shortfall is what the AWS CLI banks into
        // `bytes_failed_to_transfer` to reach 100% on a run with failures.
        //
        // `set_expected_bytes` and not `set_total_bytes`: see its doc comment. The short
        // version is that the listed size is an estimate until the object is opened, so it
        // reads `Provisional` and discovery still gets to publish the real total.
        if let Some(size) = known_size {
            ctx.set_expected_bytes(size);
        }

        let lifecycle = Self::lifecycle_for(&ctx, &input, events);
        let transfer =
            DownloadTransfer::new(ctx.clone(), bucket_type, input, writer, lifecycle.clone());
        if let Some(lc) = &lifecycle {
            lc.announce();
        }
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
