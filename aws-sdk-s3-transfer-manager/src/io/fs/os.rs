/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The operating system's filesystem.
//!
//! Every operation calls `std::fs` on the calling thread, except where noted,
//! so an asynchronous method completes on its first poll.

use std::io;
use std::path::{Path, PathBuf};
use std::task::{Context, Poll};

use crate::io::PartBuffer;

/// An open file.
pub(crate) type OpenFile = std::fs::File;

/// Metadata of a filesystem entry.
pub(crate) type Metadata = std::fs::Metadata;

/// The entries of one directory.
pub(crate) type ReadDir = std::fs::ReadDir;

/// Whether every filesystem operation completes when it is called.
pub(crate) const COMPLETES_INLINE: bool = true;

/// Returns the metadata of `path`, following symbolic links.
pub(crate) fn metadata_blocking(path: &Path) -> io::Result<Metadata> {
    std::fs::metadata(path)
}

/// Opens the existing file `path` for reading.
pub(crate) fn open_read_blocking(path: &Path) -> io::Result<OpenFile> {
    OpenFile::open(path)
}

/// The operating system's filesystem.
#[derive(Clone, Debug, Default)]
pub(crate) struct FileSystem;

impl FileSystem {
    /// Returns the operating system's filesystem.
    pub(crate) const fn os() -> Self {
        Self
    }

    /// Returns whether every operation completes when it is called.
    pub(crate) fn completes_inline(&self) -> bool {
        COMPLETES_INLINE
    }

    /// Returns the metadata of `path`, following symbolic links.
    pub(crate) async fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        std::fs::metadata(path)
    }

    /// Returns the metadata of `path` without following a symbolic link.
    pub(crate) async fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        std::fs::symlink_metadata(path)
    }

    /// Returns the canonical form of `path`.
    pub(crate) async fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        std::fs::canonicalize(path)
    }

    /// Returns the entries of the directory `path`.
    pub(crate) async fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        std::fs::read_dir(path)
    }

    /// Returns the identity of the directory `path`, following symbolic links.
    pub(crate) async fn dir_handle(&self, path: &Path) -> io::Result<super::Handle> {
        super::Handle::from_path(path)
    }

    /// Opens the existing file `path` for reading.
    pub(crate) async fn open_read(&self, path: &Path) -> io::Result<OpenFile> {
        OpenFile::open(path)
    }

    /// Returns the metadata of the open `file`.
    pub(crate) async fn file_metadata(&self, file: &OpenFile) -> io::Result<Metadata> {
        file.metadata()
    }

    /// Creates the directory `path` and its missing parents.
    pub(crate) async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    /// Creates the first candidate path that does not exist, on tokio's
    /// blocking pool, or inline on wasm targets.
    pub(crate) async fn create_first_new(
        &self,
        candidates: Vec<PathBuf>,
    ) -> io::Result<(OpenFile, PathBuf)> {
        super::create_first_new_async(candidates).await
    }

    /// Renames `from` to `to` through `tokio::fs`, or inline on wasm targets.
    pub(crate) async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        super::rename_async(from, to).await
    }

    /// Removes the file `path` through `tokio::fs`, or inline on wasm
    /// targets.
    pub(crate) async fn remove_file(&self, path: &Path) -> io::Result<()> {
        super::remove_file_async(path).await
    }

    /// Removes the file `path` on the calling thread, ignoring failure.
    pub(crate) fn remove_file_detached(&self, path: &Path) {
        let _ = std::fs::remove_file(path);
    }

    /// Creates the directory `path` and its missing parents on the calling
    /// thread.
    pub(crate) fn create_dir_all_blocking(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    /// Creates the first candidate path that does not exist, on the calling
    /// thread.
    pub(crate) fn create_first_new_blocking(
        &self,
        candidates: Vec<PathBuf>,
    ) -> io::Result<(OpenFile, PathBuf)> {
        super::create_first_new(candidates)
    }

    /// Removes the file `path` on the calling thread.
    pub(crate) fn remove_file_blocking(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// One positioned read into pooled storage.
///
/// The read runs on the calling thread when it is polled.
#[derive(Debug, Default)]
pub(crate) struct ReadAt;

impl ReadAt {
    /// Returns a read that has not started.
    pub(crate) fn new() -> Self {
        Self
    }

    /// Fills `dst` from `file` starting at `offset`, as
    /// [`read_exact_at`](super::read_exact_at) does. Always ready.
    pub(crate) fn poll_read(
        &mut self,
        _cx: &mut Context<'_>,
        file: &OpenFile,
        dst: &mut PartBuffer,
        offset: u64,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(super::read_exact_at(file, dst, offset))
    }
}
