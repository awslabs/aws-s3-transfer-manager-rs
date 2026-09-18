/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

/// Classifies a [`WalkError`].
///
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkErrorKind {
    /// Source root cannot be opened for reading (I/O or permission error
    /// on the initial path). Ends the run: the walk never gets off the ground.
    SourceUnreadable,
    /// Source root exists but is not a directory. Ends the run.
    NotADirectory,
    /// S3 service error from `ListObjectsV2` or related call. Ends the run.
    Service,
    /// I/O error reading a subdirectory or entry during the walk.
    /// The affected entry is skipped and the walk continues.
    Io,
    /// Permission denied on a subdirectory or entry during the walk.
    PermissionDenied,
    /// A directory below the root could not be read: the `read_dir` itself failed,
    /// or opening it for cycle detection did. The same failure at the root is
    /// [`SourceUnreadable`](Self::SourceUnreadable), which leaves nothing to walk;
    /// here the walk continues with the rest of the tree, but that subtree was never
    /// enumerated.
    DirectoryUnreadable,
    /// Symlink encountered with no valid target.
    BrokenSymlink,
    /// Symlink whose target is a directory already on the current descent
    /// path (a cycle). Non-cyclic duplicate symlinks (two different symlinks
    /// to the same target) are not reported as cycles and are traversed
    /// normally.
    SymlinkCycle,
}

impl WalkErrorKind {
    /// Whether an error of this kind terminates the walk.
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            WalkErrorKind::SourceUnreadable | WalkErrorKind::NotADirectory | WalkErrorKind::Service
        )
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

    // Every kind, so a new one has to be placed deliberately. Only a failure that leaves nothing
    // to carry on with ends the walk; the rest cost one entry, and a cycle costs the subtree it
    // stopped at, which the run's failure policy decides about.
    #[test]
    fn only_a_failure_with_nothing_left_ends_the_walk() {
        let cases = [
            (WalkErrorKind::SourceUnreadable, true),
            (WalkErrorKind::NotADirectory, true),
            (WalkErrorKind::Service, true),
            (WalkErrorKind::Io, false),
            (WalkErrorKind::PermissionDenied, false),
            (WalkErrorKind::DirectoryUnreadable, false),
            (WalkErrorKind::BrokenSymlink, false),
            (WalkErrorKind::SymlinkCycle, false),
        ];
        for (kind, ends_the_walk) in cases {
            assert_eq!(kind.is_fatal(), ends_the_walk, "kind={kind:?}");
        }
    }
}
