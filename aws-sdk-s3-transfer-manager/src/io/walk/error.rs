/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

/// How a [`WalkError`] bears on the walk and on whoever consumes it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkErrorSeverity {
    /// The walk cannot proceed and produces no further entries.
    Fatal,
    /// One entry could not be read. Whether this ends the caller's work is the
    /// caller's decision; the walk itself continues.
    EntryFailure,
    /// Something occupies this path that a walk can never yield — a socket, a
    /// device, a directory reached by a link that loops back on itself. Nothing
    /// failed, and no retry or setting would produce an entry here. Reported so a
    /// consumer knows the path is occupied even though no entry describes it.
    EntryWarning,
}

/// Classifies a [`WalkError`].
///
/// Each kind has a fixed severity; see [`WalkErrorKind::severity`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkErrorKind {
    /// Source root cannot be opened for reading (I/O or permission error
    /// on the initial path). Fatal: the walk never gets off the ground.
    SourceUnreadable,
    /// Source root exists but is not a directory. Fatal.
    NotADirectory,
    /// S3 service error from `ListObjectsV2` or related call. Fatal.
    Service,
    /// I/O error reading a subdirectory or entry during the walk.
    /// The affected entry is skipped and the walk continues.
    Io,
    /// Permission denied on a subdirectory or entry during the walk.
    PermissionDenied,
    /// A directory could not be read: the `read_dir` itself failed, or opening it
    /// for cycle detection did. The walk continues with the rest of the tree, but
    /// that subtree was never enumerated, so a consumer that infers absence from
    /// the stream must decide for itself whether to keep going. Distinct from an
    /// entry-level failure so that decision is possible. The underlying
    /// `io::Error` remains available via [`std::error::Error::source`].
    DirectoryUnreadable,
    /// Symlink encountered with no valid target.
    BrokenSymlink,
    /// Symlink whose target is a directory already on the current descent
    /// path (a cycle). Non-cyclic duplicate symlinks (two different symlinks
    /// to the same target) are not reported as cycles and are traversed
    /// normally.
    SymlinkCycle,
    /// A socket, FIFO, or block or character device. A walk yields regular files,
    /// so there is no entry this path could produce.
    SpecialFile,
    /// A symlink found while `follow_symlinks` is disabled. No entry is yielded for
    /// it. Without this report the name would look unused.
    SymlinkNotFollowed,
}

impl WalkErrorKind {
    /// How an error of this kind bears on the walk.
    pub fn severity(&self) -> WalkErrorSeverity {
        match self {
            WalkErrorKind::SourceUnreadable
            | WalkErrorKind::NotADirectory
            | WalkErrorKind::Service => WalkErrorSeverity::Fatal,
            WalkErrorKind::Io
            | WalkErrorKind::PermissionDenied
            | WalkErrorKind::DirectoryUnreadable
            | WalkErrorKind::BrokenSymlink => WalkErrorSeverity::EntryFailure,
            WalkErrorKind::SymlinkCycle
            | WalkErrorKind::SpecialFile
            | WalkErrorKind::SymlinkNotFollowed => WalkErrorSeverity::EntryWarning,
        }
    }

    /// Whether an error of this kind terminates the walk.
    pub fn is_fatal(&self) -> bool {
        self.severity() == WalkErrorSeverity::Fatal
    }
}

/// An error encountered during a directory walk.
///
/// Wraps an optional path, a [`WalkErrorKind`] classifier, and a source
/// error. Fatality is determined by [`kind`](Self::kind); see
/// [`is_fatal`](Self::is_fatal).
#[derive(Debug)]
pub struct WalkError {
    path: Option<PathBuf>,
    kind: WalkErrorKind,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl WalkError {
    /// The path associated with this error, if any.
    ///
    /// For non-fatal errors this is typically the entry that failed (a file
    /// or symlink). For fatal errors it may be the source root that could
    /// not be opened. `None` for errors not tied to a specific path (e.g.
    /// S3 service errors).
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The classification of this error.
    pub fn kind(&self) -> WalkErrorKind {
        self.kind
    }

    /// How this error bears on the walk. Equivalent to `self.kind().severity()`.
    pub fn severity(&self) -> WalkErrorSeverity {
        self.kind.severity()
    }

    /// Whether this error terminates the walk.
    ///
    /// When `true`, no further entries will be produced by the walk.
    /// When `false`, the walk continues and may produce more entries.
    /// Equivalent to `self.kind().is_fatal()`.
    pub fn is_fatal(&self) -> bool {
        self.kind.is_fatal()
    }

    pub(crate) fn new(
        path: Option<PathBuf>,
        kind: WalkErrorKind,
        source: Box<dyn std::error::Error + Send + Sync>,
    ) -> Self {
        Self { path, kind, source }
    }

    /// Consumes the error, returning its boxed source. Used to recover a concrete
    /// service error by downcast when converting to the crate error type.
    pub(crate) fn into_source(self) -> Box<dyn std::error::Error + Send + Sync> {
        self.source
    }

    /// Classify an `io::Error` into a non-root [`WalkErrorKind`].
    ///
    /// For root-level I/O failures construct
    /// [`WalkErrorKind::SourceUnreadable`] directly instead; this helper
    /// is only appropriate for errors encountered on subdirectories or
    /// entries reached after the walk has started.
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

    #[test]
    fn test_is_fatal_by_kind() {
        assert!(WalkErrorKind::SourceUnreadable.is_fatal());
        assert!(WalkErrorKind::NotADirectory.is_fatal());
        assert!(WalkErrorKind::Service.is_fatal());
        assert!(!WalkErrorKind::Io.is_fatal());
        assert!(!WalkErrorKind::PermissionDenied.is_fatal());
        assert!(!WalkErrorKind::BrokenSymlink.is_fatal());
        assert!(!WalkErrorKind::SymlinkCycle.is_fatal());
    }

    #[test]
    fn test_severity_by_kind() {
        use WalkErrorSeverity::*;
        let cases = [
            (WalkErrorKind::SourceUnreadable, Fatal),
            (WalkErrorKind::NotADirectory, Fatal),
            (WalkErrorKind::Service, Fatal),
            (WalkErrorKind::Io, EntryFailure),
            (WalkErrorKind::PermissionDenied, EntryFailure),
            (WalkErrorKind::DirectoryUnreadable, EntryFailure),
            // A link with no target should have been readable and was not.
            (WalkErrorKind::BrokenSymlink, EntryFailure),
            // A loop and a socket are paths a walk can never yield.
            (WalkErrorKind::SymlinkCycle, EntryWarning),
            (WalkErrorKind::SpecialFile, EntryWarning),
            (WalkErrorKind::SymlinkNotFollowed, EntryWarning),
        ];
        for (kind, expected) in cases {
            assert_eq!(kind.severity(), expected, "kind={kind:?}");
            assert_eq!(kind.is_fatal(), expected == Fatal, "kind={kind:?}");
        }
    }
}
