/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

/// Classifies a [`WalkError`] for consumer-side branching.
///
/// Separates fatal failures (walk cannot continue) from per-entry
/// failures (individual entry failed, walk continues). State machines
/// use [`WalkError::is_fatal`] to decide whether to abort the
/// directory transfer or to apply the `FailedTransferPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkErrorKind {
    /// Generic I/O failure reading a directory or entry.
    Io,
    /// Permission denied on a directory or entry.
    PermissionDenied,
    /// Path expected to be a directory but isn't (e.g. source is a file).
    NotADirectory,
    /// Symlink encountered with no valid target.
    BrokenSymlink,
    /// Symlink whose target is a directory already on the current descent
    /// path (a cycle). Non-cyclic duplicate symlinks (two different symlinks
    /// to the same target) are NOT reported under this kind — they are
    /// traversed normally.
    SymlinkCycle,
    /// S3 service error from `ListObjectsV2` or related call.
    Service,
    /// Other error that doesn't fit the categories above.
    Other,
}

/// Error reported by the walker for either fatal traversal failures or
/// per-entry failures encountered during a directory read.
///
/// A `WalkError` wraps an optional path (the entry or directory the error
/// pertains to), a kind classifier, a fatal flag, and a boxed source
/// error. Users can:
/// - Access the path via [`WalkError::path`]
/// - Get the kind via [`WalkError::kind`]
/// - Check if the walk will continue via [`WalkError::is_fatal`]
/// - Access the underlying error via the [`std::error::Error`] trait's
///   `source()` method
#[derive(Debug)]
pub struct WalkError {
    path: Option<PathBuf>,
    kind: WalkErrorKind,
    fatal: bool,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl WalkError {
    /// The path associated with this error, if any.
    ///
    /// For per-entry errors, this is the entry path. For fatal errors
    /// (e.g. the root directory is unreadable), this is the directory that
    /// could not be read. May be `None` for errors that don't correspond
    /// to a single path (e.g. an S3 service error).
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The classification of this error.
    pub fn kind(&self) -> WalkErrorKind {
        self.kind
    }

    /// Whether this error terminates the walk.
    ///
    /// When `true`, no further entries will be produced by the walk.
    /// When `false`, the walk continues and may produce more entries.
    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    pub(crate) fn new(
        path: Option<PathBuf>,
        kind: WalkErrorKind,
        fatal: bool,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self {
            path,
            kind,
            fatal,
            source,
        }
    }

    /// Classify an `io::Error` into a [`WalkErrorKind`].
    pub(crate) fn classify_io(err: &std::io::Error) -> WalkErrorKind {
        match err.kind() {
            std::io::ErrorKind::PermissionDenied => WalkErrorKind::PermissionDenied,
            std::io::ErrorKind::NotADirectory => WalkErrorKind::NotADirectory,
            _ => WalkErrorKind::Io,
        }
    }
}

impl std::fmt::Display for WalkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.path {
            Some(p) => write!(f, "walk error at {}: {}", p.display(), self.source),
            None => write!(f, "walk error: {}", self.source),
        }
    }
}

impl std::error::Error for WalkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_io_permission_denied() {
        let err = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            WalkError::classify_io(&err),
            WalkErrorKind::PermissionDenied
        );
    }

    #[test]
    fn test_classify_io_not_a_directory() {
        let err = std::io::Error::from(std::io::ErrorKind::NotADirectory);
        assert_eq!(WalkError::classify_io(&err), WalkErrorKind::NotADirectory);
    }

    #[test]
    fn test_classify_io_not_found_maps_to_io() {
        let err = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert_eq!(WalkError::classify_io(&err), WalkErrorKind::Io);
    }

    #[test]
    fn test_classify_io_other_maps_to_io() {
        let err = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(WalkError::classify_io(&err), WalkErrorKind::Io);
    }
}
