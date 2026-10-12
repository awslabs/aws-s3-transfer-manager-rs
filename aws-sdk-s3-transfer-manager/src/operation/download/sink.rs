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

use super::body::DiskWriteCursor;

/// Positioned-write target for download-to-file. Abstracts the file so the drain
/// orchestration (run coalescing, offset translation) can be exercised against an
/// in-memory capture, and so an alternative write strategy (e.g. O_DIRECT/io_uring)
/// can replace the file write without touching the buffer or the drain logic.
///
/// Positions passed to `write_all_at` are relative to the downloaded payload.
/// `prepare` may reserve storage before writes begin, while `finalize` establishes
/// the destination layout after every payload write succeeds. Implementations are
/// shared across the issuer and the drain task, hence `Send + Sync`.
pub(crate) trait SinkWrite: Send + Sync + std::fmt::Debug {
    /// Write the entire buffer at `pos` bytes from the payload's destination start.
    fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()>;

    /// Prepare a target for an expected download of `expected_download_len` bytes.
    fn prepare(&self, _expected_download_len: u64) -> std::io::Result<()> {
        Ok(())
    }

    /// Establish the target's successful layout for the complete payload.
    fn finalize(&self, _expected_download_len: u64) -> std::io::Result<()> {
        Ok(())
    }
}

/// File-backed [`SinkWrite`] using the current replace-from-zero policy.
///
/// Payload-relative positions map directly to file positions and successful
/// finalization truncates the file to the payload length. A future append or
/// write-at policy belongs here: it can translate the relative positions and
/// final length without changing transfer scheduling or object-range arithmetic.
pub(crate) struct FileSink {
    file: std::fs::File,
    /// Whether the transfer manager created this file (vs caller-provided). Only an
    /// owned file is preallocated.
    owns_file: bool,
}

impl FileSink {
    /// Wraps `file`. `owns_file` is true when the transfer manager created the
    /// file, which permits preallocating it.
    pub(crate) fn new(file: std::fs::File, owns_file: bool) -> Self {
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
fn preallocation_failure_is_fatal(error: &std::io::Error) -> bool {
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

impl SinkWrite for FileSink {
    fn write_all_at(&self, buf: &mut DiskWriteCursor<'_>, pos: u64) -> std::io::Result<()> {
        crate::io::fs::write_all_at(&self.file, buf, pos)
    }

    fn prepare(&self, expected_download_len: u64) -> std::io::Result<()> {
        if self.owns_file {
            if let Err(e) = crate::io::fs::preallocate(&self.file, expected_download_len) {
                if preallocation_failure_is_fatal(&e) {
                    return Err(e);
                }
                tracing::warn!(error = %e, "failed to preallocate file space");
            }
        }
        Ok(())
    }

    fn finalize(&self, expected_download_len: u64) -> std::io::Result<()> {
        self.file.set_len(expected_download_len)
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
    fn create(&self, file: std::fs::File, owns_file: bool) -> std::io::Result<Box<dyn SinkWrite>>;
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
    fn create(&self, file: std::fs::File, owns_file: bool) -> std::io::Result<Box<dyn SinkWrite>> {
        if crate::io::fs::is_append_only(&file)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
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
