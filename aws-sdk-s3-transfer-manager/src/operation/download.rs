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

/// Positioned-write targets for disk downloads.
pub(crate) mod sink;

#[cfg(test)]
pub(crate) mod test_util;

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

/// Most names tried for the temporary file of one path download.
///
/// Bounded so that creation fails, rather than retrying indefinitely, when the
/// suffix source keeps naming entries that already exist.
pub(crate) const TEMP_FILE_ATTEMPTS: usize = 3;

/// Candidate paths for the temporary file a path download writes before
/// renaming it to `dest`.
///
/// Each is `{dest file name}.s3tmp.{suffix}` in `dest`'s directory, with the
/// suffix written as 8 lower-case hex digits, for the first
/// [`TEMP_FILE_ATTEMPTS`] values of `suffixes`. The suffixes are drawn here, on
/// the calling thread, so a source backed by thread-local state, such as
/// `fastrand`'s global generator, draws from the caller's state even when the
/// file is then created on the blocking pool.
pub(crate) fn temp_file_candidates(
    dest: &std::path::Path,
    suffixes: impl IntoIterator<Item = u32>,
) -> Vec<std::path::PathBuf> {
    let file_name = dest.file_name().unwrap_or_default().to_string_lossy();
    suffixes
        .into_iter()
        .take(TEMP_FILE_ATTEMPTS)
        .map(|suffix| dest.with_file_name(format!("{file_name}.s3tmp.{suffix:08x}")))
        .collect()
}

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

impl Download {
    fn lifecycle_for(
        ctx: &crate::transfer::TransferContext,
        input: &DownloadInput,
        events: Option<EventRegistration>,
    ) -> Option<Arc<crate::events::TransferLifecycle>> {
        events.map(|reg| {
            Arc::new(crate::events::TransferLifecycle::new(
                reg.sink,
                ctx.id,
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

        // A streaming download has no local path: the caller owns the body, so there is
        // no destination of its own to commit to.
        let lifecycle = Self::lifecycle_for(&ctx, &input, events);
        let transfer = DownloadTransfer::new(
            ctx.clone(),
            bucket_type,
            input,
            writer,
            lifecycle.clone(),
            None,
        );
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
    /// The destination sink is created by `sinks` over the temp file, which the
    /// transfer manager owns and may therefore preallocate. A failure to create
    /// the temp file or the sink is returned as
    /// [`ErrorKind::IOError`](error::ErrorKind::IOError); a temp file whose sink
    /// could not be created is removed.
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
        sinks: &dyn sink::SinkFactory,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        // Generate temp file in the same directory as destination
        let candidates =
            temp_file_candidates(&dest_path, std::iter::repeat_with(|| fastrand::u32(..)));
        let (file, temp_path) = crate::io::fs::create_first_new_async(candidates)
            .await
            .map_err(|e| error::from_kind(error::ErrorKind::IOError)(e))?;

        let sink = match sinks.create(file, true) {
            Ok(sink) => sink,
            Err(e) => {
                // No handle owns the temp file yet, so remove it here.
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error::from_kind(error::ErrorKind::IOError)(e));
            }
        };
        // No listing behind this entry point, so the size comes from discovery as before.
        let inner = Self::orchestrate_with_sink(
            handle,
            input,
            sink,
            parent,
            events,
            None,
            Some(transfer::CommitTarget {
                temp: temp_path.clone(),
                dest: dest_path,
            }),
        )?;
        Ok(ManagedDownloadHandle::new(inner, temp_path))
    }

    /// Orchestrate a download that writes to a caller-provided file.
    ///
    /// The destination sink is created by `sinks` over `file`. The caller owns
    /// `file`, so it is not preallocated.
    ///
    /// Returns [`ErrorKind::InputInvalid`](error::ErrorKind::InputInvalid)
    /// when `sinks` rejects `file` with [`std::io::ErrorKind::InvalidInput`],
    /// and [`ErrorKind::IOError`](error::ErrorKind::IOError) for any other
    /// failure to create the sink. Either happens before the transfer is
    /// scheduled, so no request is sent and `file` is not written.
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_to_file(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        file: std::fs::File,
        events: Option<EventRegistration>,
        sinks: &dyn sink::SinkFactory,
    ) -> Result<ManagedDownloadHandle, error::Error> {
        let sink = sinks.create(file, false).map_err(|e| {
            if e.kind() == std::io::ErrorKind::InvalidInput {
                error::invalid_input(e)
            } else {
                error::from_kind(error::ErrorKind::IOError)(e)
            }
        })?;
        let inner = Self::orchestrate_with_sink(handle, input, sink, None, events, None, None)?;
        // No temp path and nothing to commit — caller manages the file lifecycle
        Ok(ManagedDownloadHandle::new_unmanaged(inner))
    }

    /// Shared orchestration for disk downloads: the transfer writes the object
    /// through `sink`.
    ///
    /// `known_size` is the object's size when the caller already learned it — a composite
    /// has it from its listing. Seeding it here rather than waiting for discovery is what
    /// gives the entry a byte denominator it keeps even if its own `GetObject` never
    /// succeeds; see the note at the `set_total_bytes` call below.
    ///
    /// When `parent` is `Some`, the transfer is linked as a child of the
    /// given composite transfer via [`TransferContext::new_child`](crate::transfer::TransferContext::new_child).
    #[cfg(any(unix, windows))]
    pub(crate) fn orchestrate_with_sink(
        handle: Arc<crate::client::Handle>,
        input: DownloadInput,
        sink: Box<dyn sink::SinkWrite>,
        parent: Option<&crate::transfer::TransferContext>,
        events: Option<EventRegistration>,
        known_size: Option<u64>,
        commit: Option<transfer::CommitTarget>,
    ) -> Result<DownloadHandleInner, error::Error> {
        use crate::transfer::TransferContext;

        if input.part_number().is_some() {
            todo!("single part download not implemented")
        }

        let bucket_type =
            BucketType::from_bucket_name(input.bucket().expect("bucket is available"));

        let (writer, _consumer) = body::new_recv_body_with_disk_mode(sink);

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
        let transfer = DownloadTransfer::new(
            ctx.clone(),
            bucket_type,
            input,
            writer,
            lifecycle.clone(),
            commit,
        );
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

#[cfg(test)]
mod tests {
    use super::{temp_file_candidates, TEMP_FILE_ATTEMPTS};
    use std::path::{Path, PathBuf};

    #[test]
    fn temp_file_candidates_name_files_beside_the_destination() {
        let candidates = temp_file_candidates(Path::new("dir/out.dat"), [0xaa, 0xbb]);

        assert_eq!(
            candidates,
            [
                PathBuf::from("dir/out.dat.s3tmp.000000aa"),
                PathBuf::from("dir/out.dat.s3tmp.000000bb"),
            ]
        );
    }

    #[test]
    fn temp_file_candidates_stop_at_the_attempt_bound() {
        let candidates = temp_file_candidates(Path::new("out.dat"), 0..100);

        assert_eq!(candidates.len(), TEMP_FILE_ATTEMPTS);
    }
}
