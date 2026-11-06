/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

/// Classifies a [`WalkError`].
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
    /// I/O error reading one entry during the walk. That entry is skipped and the walk
    /// continues. A directory that could not be read reports `DirectoryUnreadable` instead,
    /// since the cost there is every key beneath it rather than one.
    Io,
    /// Permission denied on one entry during the walk. As with `Io`, a directory reports
    /// `DirectoryUnreadable`.
    PermissionDenied,
    /// A directory below the root could not be read: the `read_dir` itself failed,
    /// or opening it for cycle detection did. The same failure at the root is
    /// [`SourceUnreadable`](Self::SourceUnreadable), which leaves nothing to walk;
    /// here the walk continues with the rest of the tree, but that subtree was never
    /// enumerated.
    DirectoryUnreadable,
    /// An entry was named by its directory and was not there when it was read. Nothing is broken:
    /// a file deleted between those two moments leaves nothing to transfer, and a destination
    /// without that key already matches a source that no longer has it.
    Vanished,
    /// Symlink encountered with no valid target.
    BrokenSymlink,
    /// Symlink whose target is a directory already on the current descent
    /// path (a cycle). Non-cyclic duplicate symlinks (two different symlinks
    /// to the same target) are not reported as cycles and are traversed
    /// normally.
    SymlinkCycle,
}

impl WalkErrorKind {
    // Whether an error of this kind terminates the walk. The kind carries the answer because the
    // position was folded in when it was chosen: the same underlying failure becomes
    // `SourceUnreadable` at the walk root and `DirectoryUnreadable` a level down.
    //
    // Crate-private because a caller cannot hold this predicate safely. `WalkErrorKind` is public
    // and `#[non_exhaustive]`, so a hand-written version needs a wildcard arm, and a kind added
    // later would read as non-fatal there — turning a walk that stopped early into one that looks
    // finished. `FsWalk::is_done` answers the question directly instead.
    pub(crate) fn is_fatal(&self) -> bool {
        matches!(
            self,
            WalkErrorKind::SourceUnreadable | WalkErrorKind::NotADirectory | WalkErrorKind::Service
        )
    }

    // Return the error category without consuming the failure. Event integration and outcome
    // reporting read it before the failure moves into a run record.
    //
    // The match names every variant. A new walk error needs its own category.
    pub(crate) fn category(&self) -> crate::error::ErrorKind {
        use crate::error::ErrorKind;
        match self {
            WalkErrorKind::Service => ErrorKind::ServiceError,
            WalkErrorKind::SourceUnreadable | WalkErrorKind::NotADirectory => {
                ErrorKind::InputInvalid
            }
            WalkErrorKind::Io
            | WalkErrorKind::Vanished
            | WalkErrorKind::PermissionDenied
            | WalkErrorKind::DirectoryUnreadable
            | WalkErrorKind::SymlinkCycle
            | WalkErrorKind::BrokenSymlink => ErrorKind::IOError,
        }
    }

    // Return true when no sync setting can transfer the item. A symlink cycle has no terminal target.
    // A path that vanished after listing has no work left to transfer.
    pub(crate) fn is_warning(&self) -> bool {
        match self {
            WalkErrorKind::SymlinkCycle | WalkErrorKind::Vanished => true,
            WalkErrorKind::SourceUnreadable
            | WalkErrorKind::NotADirectory
            | WalkErrorKind::Service
            | WalkErrorKind::Io
            | WalkErrorKind::PermissionDenied
            | WalkErrorKind::DirectoryUnreadable
            | WalkErrorKind::BrokenSymlink => false,
        }
    }
}

/// An error encountered during a directory walk.
///
/// Wraps an optional path, a [`WalkErrorKind`] classifier, and a source
/// error. Whether the walk stopped is answered by
/// [`FsWalk::is_done`](crate::io::walk::FsWalk::is_done), not by reading the
/// kind.
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
    // Whether this error terminates the walk. Equivalent to `self.kind().is_fatal()`, and
    // crate-private for the same reason.
    pub(crate) fn is_fatal(&self) -> bool {
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
            // The name came from the directory holding it, so it existed. Not finding it now means
            // it went away in between.
            std::io::ErrorKind::NotFound => WalkErrorKind::Vanished,
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
    fn each_kind_says_whether_it_ends_the_walk_and_whether_it_was_ever_transferable() {
        for kind in [
            WalkErrorKind::SourceUnreadable,
            WalkErrorKind::NotADirectory,
            WalkErrorKind::Service,
        ] {
            assert!(kind.is_fatal(), "{kind:?} must end the walk");
            assert!(!kind.is_warning(), "{kind:?} is a failure, not a warning");
        }
        for kind in [
            WalkErrorKind::Io,
            WalkErrorKind::PermissionDenied,
            WalkErrorKind::DirectoryUnreadable,
            WalkErrorKind::BrokenSymlink,
        ] {
            assert!(!kind.is_fatal(), "{kind:?} must not end the walk");
            assert!(
                !kind.is_warning(),
                "{kind:?} should have worked, so it is a failure"
            );
        }
        assert!(!WalkErrorKind::SymlinkCycle.is_fatal());
        assert!(WalkErrorKind::SymlinkCycle.is_warning());
    }

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
    fn test_classify_io_other_errors_map_to_io() {
        let err = std::io::Error::other("some other error");
        assert_eq!(WalkError::classify_io(&err), WalkErrorKind::Io);
    }

    // A file deleted between being named by its directory and being read is routine — an editor
    // saving atomically, a log being rotated, a build directory being cleaned. Its key is not
    // missing from this side's account: it is genuinely not there, so absence stays readable either
    // side of it and nothing needs holding open.
    #[test]
    fn a_file_gone_before_it_could_be_read_is_named_as_such() {
        let gone = std::io::Error::from(std::io::ErrorKind::NotFound);
        assert_eq!(WalkError::classify_io(&gone), WalkErrorKind::Vanished);
        assert!(!WalkErrorKind::Vanished.is_fatal());
        // A file that is there and cannot be read is the opposite case and still costs one key.
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        assert_eq!(
            WalkError::classify_io(&denied),
            WalkErrorKind::PermissionDenied
        );
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
