/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The host application's filesystem on `wasm32-unknown-unknown`.
//!
//! Every operation awaits the client's
//! [`WasmFileSystem`](crate::wasm::WasmFileSystem). A client without one
//! fails every operation with [`io::ErrorKind::Unsupported`]. Operations on
//! the calling thread, which the operating system's filesystem provides, are
//! unavailable: callers check [`FileSystem::completes_inline`] first.

use std::future::Future;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes};

use crate::io::PartBuffer;
use crate::wasm::{
    FileIdentity, FileMetadata, FileSystemFuture, FileType, OpenMode, SharedWasmFile,
    SharedWasmFileSystem, WasmFile, WasmFileSystem,
};

/// An open file.
pub(crate) type OpenFile = SharedWasmFile;

/// Metadata of a filesystem entry.
pub(crate) type Metadata = FileMetadata;

/// Most bytes one host read returns.
///
/// Host read responses are staged outside the buffer pool until they are
/// copied into pooled storage, so this bounds that staging per read.
const READ_CHUNK: usize = 1024 * 1024;

/// Whether every filesystem operation completes when it is called.
pub(crate) const COMPLETES_INLINE: bool = false;

/// Unavailable: fails with [`io::ErrorKind::Unsupported`].
pub(crate) fn metadata_blocking(_path: &Path) -> io::Result<Metadata> {
    Err(not_inline())
}

/// Unavailable: fails with [`io::ErrorKind::Unsupported`].
pub(crate) fn open_read_blocking(_path: &Path) -> io::Result<OpenFile> {
    Err(not_inline())
}

/// The error for an operation on a client without a filesystem.
fn not_configured() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "no filesystem is configured for this client",
    )
}

/// The error for an operation that must complete on the calling thread.
fn not_inline() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "the host filesystem completes operations asynchronously",
    )
}

/// The host application's filesystem for one client.
#[derive(Clone, Debug, Default)]
pub(crate) struct FileSystem {
    inner: Option<SharedWasmFileSystem>,
}

impl FileSystem {
    /// Returns the filesystem backed by `inner`, or one whose operations all
    /// fail when `inner` is `None`.
    pub(crate) fn host(inner: Option<SharedWasmFileSystem>) -> Self {
        Self { inner }
    }

    fn get(&self) -> io::Result<&SharedWasmFileSystem> {
        self.inner.as_ref().ok_or_else(not_configured)
    }

    /// Returns false: operations complete asynchronously.
    pub(crate) fn completes_inline(&self) -> bool {
        COMPLETES_INLINE
    }

    /// Returns the metadata of `path`, following symbolic links.
    pub(crate) async fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        self.get()?.metadata(path.to_path_buf(), true).await
    }

    /// Returns the metadata of `path` without following a symbolic link.
    pub(crate) async fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        self.get()?.metadata(path.to_path_buf(), false).await
    }

    /// Returns the canonical form of `path`.
    pub(crate) async fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.get()?.canonicalize(path.to_path_buf()).await
    }

    /// Returns the entries of the directory `path`.
    pub(crate) async fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        let entries = self.get()?.read_dir(path.to_path_buf()).await?;
        Ok(ReadDir {
            dir: path.to_path_buf(),
            entries: entries.into_iter(),
        })
    }

    /// Returns the identity of the directory `path`, following symbolic links.
    pub(crate) async fn dir_handle(&self, path: &Path) -> io::Result<Handle> {
        let file = self
            .get()?
            .open(path.to_path_buf(), OpenMode::Directory)
            .await?;
        let identity = file.identity().await?;
        Ok(Handle {
            _file: file,
            identity,
        })
    }

    /// Opens the existing file `path` for reading.
    pub(crate) async fn open_read(&self, path: &Path) -> io::Result<OpenFile> {
        self.get()?.open(path.to_path_buf(), OpenMode::Read).await
    }

    /// Returns the metadata of the open `file`.
    pub(crate) async fn file_metadata(&self, file: &OpenFile) -> io::Result<Metadata> {
        file.metadata().await
    }

    /// Creates the directory `path` and its missing parents.
    pub(crate) async fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.get()?.create_dir_all(path.to_path_buf()).await
    }

    /// Creates the first of `candidates` that does not exist yet, as
    /// [`create_first_new`](crate::io::fs) does on other targets.
    ///
    /// Each candidate is created atomically with [`OpenMode::CreateNew`]. An
    /// existing entry is never opened; the next candidate is tried instead.
    /// Returns [`io::ErrorKind::AlreadyExists`] when every candidate existed,
    /// or there were none. Any other error is returned at once.
    pub(crate) async fn create_first_new(
        &self,
        candidates: Vec<PathBuf>,
    ) -> io::Result<(OpenFile, PathBuf)> {
        let filesystem = self.get()?;
        let tried = candidates.len();
        for path in candidates {
            match filesystem.open(path.clone(), OpenMode::CreateNew).await {
                Ok(file) => return Ok((file, path)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("every candidate path already exists ({tried} tried)"),
        ))
    }

    /// Atomically replaces `to` with the file `from`.
    pub(crate) async fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.get()?
            .rename(from.to_path_buf(), to.to_path_buf())
            .await
    }

    /// Removes the file `path`.
    pub(crate) async fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.get()?.remove_file(path.to_path_buf()).await
    }

    /// Starts removing the file `path` without waiting for the result.
    pub(crate) fn remove_file_detached(&self, path: &Path) {
        if let Some(filesystem) = &self.inner {
            filesystem.remove_file_detached(path.to_path_buf());
        }
    }

    /// Unavailable: fails with [`io::ErrorKind::Unsupported`].
    pub(crate) fn create_dir_all_blocking(&self, _path: &Path) -> io::Result<()> {
        Err(not_inline())
    }

    /// Unavailable: fails with [`io::ErrorKind::Unsupported`].
    pub(crate) fn create_first_new_blocking(
        &self,
        _candidates: Vec<PathBuf>,
    ) -> io::Result<(OpenFile, PathBuf)> {
        Err(not_inline())
    }

    /// Unavailable: fails with [`io::ErrorKind::Unsupported`].
    pub(crate) fn remove_file_blocking(&self, _path: &Path) -> io::Result<()> {
        Err(not_inline())
    }
}

/// The identity of an open directory.
///
/// The directory stays open for the handle's lifetime, so its identity cannot
/// be issued to another object while the handle is compared.
#[derive(Debug)]
pub(crate) struct Handle {
    _file: SharedWasmFile,
    identity: FileIdentity,
}

impl PartialEq for Handle {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
    }
}

impl Eq for Handle {}

/// The entries of one directory, joined to the directory's path.
///
/// An entry whose name is not a single path component is yielded as an
/// [`io::ErrorKind::InvalidData`] error.
#[derive(Debug)]
pub(crate) struct ReadDir {
    dir: PathBuf,
    entries: std::vec::IntoIter<crate::wasm::DirectoryEntry>,
}

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.entries.next()?;
        let name = Path::new(entry.name());
        let mut components = name.components();
        let single = matches!(
            (components.next(), components.next()),
            (Some(Component::Normal(component)), None) if component == entry.name()
        );
        if !single {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "directory entry name {:?} in {} is not a single path component",
                    entry.name(),
                    self.dir.display()
                ),
            )));
        }
        Some(Ok(DirEntry {
            path: self.dir.join(name),
            file_type: entry.file_type(),
        }))
    }
}

/// One entry of a [`ReadDir`].
#[derive(Debug)]
pub(crate) struct DirEntry {
    path: PathBuf,
    file_type: FileType,
}

impl DirEntry {
    /// Returns the entry's path: its directory joined with its name.
    pub(crate) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Returns the entry's own kind, without following a symbolic link.
    pub(crate) fn file_type(&self) -> io::Result<FileType> {
        Ok(self.file_type)
    }
}

/// One positioned read into pooled storage.
///
/// The read is issued to the host in requests of at most 1 MiB, retained
/// across `Poll::Pending`, and cancelled when the value is dropped. Received
/// bytes are copied into the destination as each request completes.
#[derive(Default)]
pub(crate) struct ReadAt {
    /// The outstanding host request and the length it asked for.
    pending: Option<(FileSystemFuture<Bytes>, usize)>,
    /// Bytes of the current read already copied into the destination.
    filled: usize,
}

impl std::fmt::Debug for ReadAt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadAt")
            .field("pending", &self.pending.as_ref().map(|(_, len)| *len))
            .field("filled", &self.filled)
            .finish()
    }
}

impl ReadAt {
    /// Returns a read that has not started.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Fills every remaining writable byte of `dst` from `file`, starting at
    /// `offset`.
    ///
    /// Pass the same `file`, `dst`, and `offset` to every poll of one read.
    /// After it completes, the value may start another read. Fails with
    /// [`io::ErrorKind::InvalidData`] if the host returns a different number
    /// of bytes than requested, and with the host's error otherwise. After a
    /// failure, `dst` holds a prefix of the range and must be discarded.
    pub(crate) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        file: &OpenFile,
        dst: &mut PartBuffer,
        offset: u64,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.pending.is_none() {
                let remaining = dst.remaining_mut();
                if remaining == 0 {
                    self.filled = 0;
                    return Poll::Ready(Ok(()));
                }
                let len = remaining.min(READ_CHUNK);
                let Some(at) = offset.checked_add(self.filled as u64) else {
                    self.filled = 0;
                    return Poll::Ready(Err(io::Error::other("file read offset overflowed")));
                };
                self.pending = Some((file.read_exact_at(at, len), len));
            }
            let (future, len) = self.pending.as_mut().expect("read request started above");
            let result = match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            let len = *len;
            self.pending = None;
            let bytes = match result {
                Ok(bytes) if bytes.len() == len => bytes,
                Ok(bytes) => {
                    self.filled = 0;
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("host read returned {} bytes, expected {len}", bytes.len()),
                    )));
                }
                Err(error) => {
                    self.filled = 0;
                    return Poll::Ready(Err(error));
                }
            };
            dst.put_slice(&bytes);
            self.filled += len;
        }
    }
}
