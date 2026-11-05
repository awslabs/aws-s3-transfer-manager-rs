/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Filesystem access supplied by the host application.
//!
//! A [`WasmFileSystem`] in a client's configuration serves that client's path
//! operations: uploads from a path, downloads to a path, and both directory
//! operations. Paths name entries inside the filesystem the implementation
//! exposes. They are not ambient host paths, and the transfer manager cannot
//! widen the access an implementation grants.
//!
//! Every operation returns a [`FileSystemFuture`]. A future follows the same
//! polling contract as the HTTP client's futures: it returns promptly, stores
//! the waker of its latest poll before returning `Poll::Pending`, and cancels
//! or detaches its host request when it is dropped. The transfer manager never
//! polls a filesystem future while it holds a lock that another transfer
//! operation needs, and it may poll independent operations concurrently.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;

use crate::memory::SegmentedBytes;

/// The result of one filesystem operation.
pub type FileSystemFuture<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'static>>;

/// The kind of a filesystem entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FileType {
    /// A regular file.
    File,
    /// A directory.
    Directory,
    /// A symbolic link.
    Symlink,
    /// Any other kind of entry, such as a socket or a device.
    Other,
}

impl FileType {
    /// Returns whether this is a regular file.
    pub fn is_file(&self) -> bool {
        *self == Self::File
    }

    /// Returns whether this is a directory.
    pub fn is_dir(&self) -> bool {
        *self == Self::Directory
    }

    /// Returns whether this is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        *self == Self::Symlink
    }
}

/// The kind and length of a filesystem entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileMetadata {
    file_type: FileType,
    len: u64,
}

impl FileMetadata {
    /// Returns metadata for an entry of `file_type` that holds `len` bytes.
    pub fn new(file_type: FileType, len: u64) -> Self {
        Self { file_type, len }
    }

    /// Returns the kind of the entry.
    pub fn file_type(&self) -> FileType {
        self.file_type
    }

    /// Returns the entry's length in bytes.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns whether the entry is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type.is_file()
    }

    /// Returns whether the entry is a directory.
    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    /// Returns whether the entry is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        self.file_type.is_symlink()
    }
}

/// One entry of a directory listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    name: OsString,
    file_type: FileType,
}

impl DirectoryEntry {
    /// Returns an entry named `name` whose own kind, without following a
    /// symbolic link, is `file_type`.
    ///
    /// `name` is the entry's name within its directory, not a path.
    pub fn new(name: impl Into<OsString>, file_type: FileType) -> Self {
        Self {
            name: name.into(),
            file_type,
        }
    }

    /// Returns the entry's name within its directory.
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    /// Returns the entry's own kind. A symbolic link is reported as
    /// [`FileType::Symlink`], not as the kind of its target.
    pub fn file_type(&self) -> FileType {
        self.file_type
    }
}

/// How [`WasmFileSystem::open`] opens a path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OpenMode {
    /// Opens an existing regular file for positioned reads, following
    /// symbolic links. Fails if the path names any other kind of entry.
    Read,
    /// Creates a new, empty regular file for positioned writes. Fails with
    /// [`io::ErrorKind::AlreadyExists`] if any entry exists at the path,
    /// including a symbolic link whose target does not exist. Creation is
    /// atomic: two concurrent calls for one path cannot both succeed.
    CreateNew,
    /// Opens an existing directory to read its identity, following symbolic
    /// links. Fails if the path names any other kind of entry.
    Directory,
}

/// An identifier for one filesystem object.
///
/// Identities are comparable only within one [`WasmFileSystem`]. Two open
/// files have equal identities exactly when they refer to the same object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileIdentity(u128);

impl FileIdentity {
    /// Returns the identity `value`.
    pub fn new(value: u128) -> Self {
        Self(value)
    }
}

/// Filesystem access supplied by the host application.
///
/// Paths are interpreted by the implementation. A path that would leave the
/// directory tree the implementation exposes, through `..` or a symbolic
/// link, fails with [`io::ErrorKind::PermissionDenied`]. Error messages must
/// not reveal host paths outside that tree.
///
/// An operation the implementation cannot perform fails with
/// [`io::ErrorKind::Unsupported`]. The transfer manager does not fall back to
/// a weaker operation: a path download, for example, never writes its
/// destination in place when atomic creation or rename is unsupported.
pub trait WasmFileSystem: Send + Sync + fmt::Debug + 'static {
    /// Returns the metadata of the entry at `path`.
    ///
    /// When `follow_symlinks` is true and `path` names a symbolic link, the
    /// metadata describes the link's target. Otherwise it describes the
    /// entry itself.
    fn metadata(&self, path: PathBuf, follow_symlinks: bool) -> FileSystemFuture<FileMetadata>;

    /// Returns the absolute form of `path`, with every symbolic link and
    /// `.` or `..` component resolved.
    fn canonicalize(&self, path: PathBuf) -> FileSystemFuture<PathBuf>;

    /// Returns the entries of the directory at `path`, in any order.
    ///
    /// The listing excludes `.` and `..`. Each name is a single path
    /// component: it is nonempty, is neither `.` nor `..`, and contains no
    /// path separator.
    fn read_dir(&self, path: PathBuf) -> FileSystemFuture<Vec<DirectoryEntry>>;

    /// Opens the entry at `path` as `mode` describes.
    fn open(&self, path: PathBuf, mode: OpenMode) -> FileSystemFuture<SharedWasmFile>;

    /// Creates the directory at `path` and every missing parent.
    ///
    /// Succeeds if the directory already exists.
    fn create_dir_all(&self, path: PathBuf) -> FileSystemFuture<()>;

    /// Atomically replaces the entry at `to` with the file at `from`.
    ///
    /// After success, `from` no longer exists and `to` names the renamed
    /// file. Readers observe either the previous entry at `to` or the renamed
    /// file, never a partial file.
    fn rename(&self, from: PathBuf, to: PathBuf) -> FileSystemFuture<()>;

    /// Removes the file at `path`.
    fn remove_file(&self, path: PathBuf) -> FileSystemFuture<()>;

    /// Starts removing the file at `path` and returns without waiting.
    ///
    /// The transfer manager calls this from `Drop`, where it cannot await a
    /// future or report a failure. The method must not block, and the
    /// removal must proceed without being polled.
    fn remove_file_detached(&self, path: PathBuf);
}

/// An open file supplied by a [`WasmFileSystem`].
///
/// Reads and writes name explicit offsets and do not use a shared cursor.
/// Operations on disjoint ranges may run concurrently and complete in any
/// order. The implementation closes its host handle when the last
/// [`SharedWasmFile`] that refers to it, and every future started from it,
/// has been dropped.
pub trait WasmFile: Send + Sync + fmt::Debug + 'static {
    /// Returns the metadata of the open file.
    fn metadata(&self) -> FileSystemFuture<FileMetadata>;

    /// Returns the identity of the open file.
    ///
    /// The identity remains reserved for this object while the file is open,
    /// so no other object reports it in the meantime.
    fn identity(&self) -> FileSystemFuture<FileIdentity>;

    /// Reads exactly `len` bytes starting at `offset`.
    ///
    /// Fails with [`io::ErrorKind::UnexpectedEof`] if the file ends before
    /// `offset + len`.
    fn read_exact_at(&self, offset: u64, len: usize) -> FileSystemFuture<Bytes>;

    /// Writes all of `data` starting at `offset`, extending the file if
    /// needed.
    ///
    /// Completes only after the file has accepted every byte. The future
    /// retains `data` until it completes or is dropped.
    fn write_all_at(&self, offset: u64, data: SegmentedBytes) -> FileSystemFuture<()>;

    /// Sets the file's length to `len`, truncating or extending it.
    fn set_len(&self, len: u64) -> FileSystemFuture<()>;

    /// Prepares the file for an expected length of `len` bytes.
    ///
    /// The default implementation does nothing. The transfer manager sets the
    /// final length with [`set_len`](Self::set_len) after the last write.
    fn preallocate(&self, len: u64) -> FileSystemFuture<()> {
        let _ = len;
        Box::pin(async { Ok(()) })
    }
}

/// A cloneable [`WasmFileSystem`] that clients and walks can share.
///
/// Clones refer to the same filesystem.
#[derive(Clone, Debug)]
pub struct SharedWasmFileSystem(Arc<dyn WasmFileSystem>);

impl SharedWasmFileSystem {
    /// Wraps `filesystem` for sharing.
    pub fn new(filesystem: impl WasmFileSystem) -> Self {
        Self(Arc::new(filesystem))
    }
}

impl WasmFileSystem for SharedWasmFileSystem {
    fn metadata(&self, path: PathBuf, follow_symlinks: bool) -> FileSystemFuture<FileMetadata> {
        self.0.metadata(path, follow_symlinks)
    }

    fn canonicalize(&self, path: PathBuf) -> FileSystemFuture<PathBuf> {
        self.0.canonicalize(path)
    }

    fn read_dir(&self, path: PathBuf) -> FileSystemFuture<Vec<DirectoryEntry>> {
        self.0.read_dir(path)
    }

    fn open(&self, path: PathBuf, mode: OpenMode) -> FileSystemFuture<SharedWasmFile> {
        self.0.open(path, mode)
    }

    fn create_dir_all(&self, path: PathBuf) -> FileSystemFuture<()> {
        self.0.create_dir_all(path)
    }

    fn rename(&self, from: PathBuf, to: PathBuf) -> FileSystemFuture<()> {
        self.0.rename(from, to)
    }

    fn remove_file(&self, path: PathBuf) -> FileSystemFuture<()> {
        self.0.remove_file(path)
    }

    fn remove_file_detached(&self, path: PathBuf) {
        self.0.remove_file_detached(path);
    }
}

/// A cloneable [`WasmFile`].
///
/// Clones refer to the same open file.
#[derive(Clone, Debug)]
pub struct SharedWasmFile(Arc<dyn WasmFile>);

impl SharedWasmFile {
    /// Wraps `file` for sharing.
    pub fn new(file: impl WasmFile) -> Self {
        Self(Arc::new(file))
    }
}

impl WasmFile for SharedWasmFile {
    fn metadata(&self) -> FileSystemFuture<FileMetadata> {
        self.0.metadata()
    }

    fn identity(&self) -> FileSystemFuture<FileIdentity> {
        self.0.identity()
    }

    fn read_exact_at(&self, offset: u64, len: usize) -> FileSystemFuture<Bytes> {
        self.0.read_exact_at(offset, len)
    }

    fn write_all_at(&self, offset: u64, data: SegmentedBytes) -> FileSystemFuture<()> {
        self.0.write_all_at(offset, data)
    }

    fn set_len(&self, len: u64) -> FileSystemFuture<()> {
        self.0.set_len(len)
    }

    fn preallocate(&self, len: u64) -> FileSystemFuture<()> {
        self.0.preallocate(len)
    }
}
