/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The positioned-write target behind a disk download.
//!
//! [`SinkWrite`](crate::operation::download::sink::SinkWrite) is the
//! destination a disk download's drains write through,
//! [`FileSink`](crate::operation::download::sink::FileSink) implements it over
//! a file, and a [`SinkFactory`](crate::operation::download::sink::SinkFactory)
//! builds the sink for one download over its already-open destination file.

use std::io;

use futures_util::future::{BoxFuture, Either, Ready};

use super::body::DiskWriteCursor;
use crate::io::fs::OpenFile;

/// The completion of one sink operation.
///
/// A sink whose operations complete when they are started returns
/// [`Either::Left`], a ready future. A sink whose operations complete later
/// returns [`Either::Right`].
pub(crate) type SinkFuture<'a> = Either<Ready<io::Result<()>>, BoxFuture<'a, io::Result<()>>>;

/// Returns a sink operation that has already completed with `result`.
pub(crate) fn completed(result: io::Result<()>) -> SinkFuture<'static> {
    Either::Left(futures_util::future::ready(result))
}

/// Positioned-write target for download-to-file. Abstracts the file so the drain
/// orchestration (run coalescing, offset translation) can be exercised against an
/// in-memory capture, and so an alternative write strategy (e.g. O_DIRECT/io_uring)
/// can replace the file write without touching the buffer or the drain logic.
///
/// Positions passed to `write_all_at` are relative to the downloaded payload.
/// `prepare` may reserve storage before writes begin, while `finalize` establishes
/// the destination layout after every payload write succeeds. Implementations are
/// shared across the issuer and the drain task, hence `Send + Sync`.
///
/// Each operation returns a [`SinkFuture`]. The run's payloads stay owned until
/// the write's future completes or is dropped. A sink that reports
/// [`completes_inline`](Self::completes_inline) returns only ready futures.
pub(crate) trait SinkWrite: Send + Sync + std::fmt::Debug {
    /// Write the entire run `buf` at `pos` bytes from the payload's destination
    /// start.
    fn write_all_at<'a>(&'a self, buf: DiskWriteCursor<'a>, pos: u64) -> SinkFuture<'a>;

    /// Prepare a target for an expected download of `expected_download_len` bytes.
    fn prepare(&self, _expected_download_len: u64) -> SinkFuture<'_> {
        completed(Ok(()))
    }

    /// Establish the target's successful layout for the complete payload.
    fn finalize(&self, _expected_download_len: u64) -> SinkFuture<'_> {
        completed(Ok(()))
    }

    /// Returns whether every operation completes when it is started.
    fn completes_inline(&self) -> bool {
        true
    }
}

/// File-backed [`SinkWrite`] using the current replace-from-zero policy.
///
/// Payload-relative positions map directly to file positions and successful
/// finalization truncates the file to the payload length. A future append or
/// write-at policy belongs here: it can translate the relative positions and
/// final length without changing transfer scheduling or object-range arithmetic.
///
/// An operating-system file completes each operation on the calling thread. A
/// host file on `wasm32-unknown-unknown` completes them asynchronously.
pub(crate) struct FileSink {
    file: OpenFile,
    /// Whether the transfer manager created this file (vs caller-provided). Only an
    /// owned file is preallocated.
    owns_file: bool,
}

impl FileSink {
    /// Wraps `file`. `owns_file` is true when the transfer manager created the
    /// file, which permits preallocating it.
    pub(crate) fn new(file: OpenFile, owns_file: bool) -> Self {
        Self { file, owns_file }
    }
}

impl std::fmt::Debug for FileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileSink").finish_non_exhaustive()
    }
}

/// Returns whether failure to reserve storage makes the download futile.
///
/// Unsupported preallocation remains best effort because the subsequent
/// positioned writes may still succeed. Linux storage and quota exhaustion
/// cannot recover without external intervention and should fail before the
/// transfer spends network and memory resources on the object body.
fn preallocation_failure_is_fatal(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        matches!(
            error.raw_os_error(),
            Some(libc::ENOSPC) | Some(libc::EDQUOT)
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        false
    }
}

/// Applies the preallocation failure policy: a fatal failure is returned, and
/// any other failure is logged and ignored.
fn preallocation_outcome(result: io::Result<()>) -> io::Result<()> {
    match result {
        Err(e) if preallocation_failure_is_fatal(&e) => Err(e),
        Err(e) => {
            tracing::warn!(error = %e, "failed to preallocate file space");
            Ok(())
        }
        Ok(()) => Ok(()),
    }
}

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
impl SinkWrite for FileSink {
    fn write_all_at<'a>(&'a self, mut buf: DiskWriteCursor<'a>, pos: u64) -> SinkFuture<'a> {
        completed(crate::io::fs::write_all_at(&self.file, &mut buf, pos))
    }

    fn prepare(&self, expected_download_len: u64) -> SinkFuture<'_> {
        if !self.owns_file {
            return completed(Ok(()));
        }
        completed(preallocation_outcome(crate::io::fs::preallocate(
            &self.file,
            expected_download_len,
        )))
    }

    fn finalize(&self, expected_download_len: u64) -> SinkFuture<'_> {
        completed(self.file.set_len(expected_download_len))
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
impl SinkWrite for FileSink {
    /// Sends the run's payloads to the host without copying them. The returned
    /// future retains them until the host confirms the complete write.
    fn write_all_at<'a>(&'a self, buf: DiskWriteCursor<'a>, pos: u64) -> SinkFuture<'a> {
        use crate::wasm::WasmFile;
        Either::Right(self.file.write_all_at(pos, buf.to_segmented()))
    }

    fn prepare(&self, expected_download_len: u64) -> SinkFuture<'_> {
        use crate::wasm::WasmFile;
        if !self.owns_file {
            return completed(Ok(()));
        }
        let preallocation = self.file.preallocate(expected_download_len);
        Either::Right(Box::pin(async move {
            preallocation_outcome(preallocation.await)
        }))
    }

    fn finalize(&self, expected_download_len: u64) -> SinkFuture<'_> {
        use crate::wasm::WasmFile;
        Either::Right(self.file.set_len(expected_download_len))
    }

    fn completes_inline(&self) -> bool {
        false
    }
}

/// Builds the [`SinkWrite`] that receives one disk download's positioned writes,
/// preparation, and finalization.
///
/// Constructing the sink apart from the transfer lets destination I/O be
/// replaced without changing how the transfer schedules and drains its writes.
pub(crate) trait SinkFactory: Send + Sync + std::fmt::Debug {
    /// Builds the sink a disk download writes through, over an already-open
    /// destination `file`. `owns_file` is true when the transfer manager
    /// created the file, which permits preallocating it.
    ///
    /// Fails, without writing to `file`, when `file` cannot serve as this
    /// factory's destination or its handle cannot be inspected. An
    /// [`InvalidInput`](std::io::ErrorKind::InvalidInput) error means the
    /// caller supplied a destination the sink does not support.
    fn create(&self, file: OpenFile, owns_file: bool) -> io::Result<Box<dyn SinkWrite>>;
}

/// [`SinkFactory`] whose sinks write directly to the destination file.
#[derive(Debug, Default)]
pub(crate) struct FileSinkFactory;

impl SinkFactory for FileSinkFactory {
    /// Returns [`InvalidInput`](std::io::ErrorKind::InvalidInput) when
    /// `file`'s handle is in append mode
    /// ([`is_append_only`](crate::io::fs::is_append_only)), since positioned
    /// writes through it would not land at their offsets. An error reading the
    /// handle's mode is returned as is.
    fn create(&self, file: OpenFile, owns_file: bool) -> io::Result<Box<dyn SinkWrite>> {
        if crate::io::fs::is_append_only(&file)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination file is open in append mode, which downloads to a file do not support",
            ));
        }
        Ok(Box::new(FileSink::new(file, owns_file)))
    }
}

#[cfg(test)]
mod tests {
    /// A destination opened in append mode is rejected before a sink is
    /// built, and its contents are left as they were.
    #[cfg(any(unix, windows))]
    #[cfg_attr(miri, ignore)] // the Miri build does not inspect the handle
    #[test]
    fn file_sink_factory_rejects_append_mode() {
        use super::{FileSinkFactory, SinkFactory};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"header").unwrap();
        let file = std::fs::File::options().append(true).open(&path).unwrap();

        let error = FileSinkFactory
            .create(file, false)
            .expect_err("an append-mode destination must be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read(&path).unwrap(), b"header");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_preallocation_fails_only_for_storage_exhaustion() {
        assert!(super::preallocation_failure_is_fatal(
            &std::io::Error::from_raw_os_error(libc::ENOSPC)
        ));
        assert!(super::preallocation_failure_is_fatal(
            &std::io::Error::from_raw_os_error(libc::EDQUOT)
        ));
        assert!(!super::preallocation_failure_is_fatal(
            &std::io::Error::from_raw_os_error(libc::EOPNOTSUPP)
        ));
    }
}
