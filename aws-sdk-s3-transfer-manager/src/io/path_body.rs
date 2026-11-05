/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::io::error::{Error, ErrorKind};
use crate::io::fs::{self, FileSystem, Metadata, OpenFile};
use crate::io::stream::{InputStream, RawInputStream};

/// Input stream designed to wrap file based input.
#[derive(Debug)]
pub(super) struct PathBody {
    // The path to the file
    pub(super) path: PathBuf,
    // The total number of bytes to read
    pub(super) length: u64,
    // The byte-offset to start reading from
    pub(super) offset: u64,
    // How the file is opened
    pub(super) source: PathSource,
}

/// How a [`PathBody`] obtains its open file.
#[derive(Debug)]
pub(super) enum PathSource {
    /// The reader opens the path on the calling thread when it is created.
    Path,
    /// The uploading transfer opens the path through its client's filesystem
    /// before choosing how to send it. `length` is meaningful only when
    /// `length_known` is true; otherwise it is read from the open file.
    Unresolved { length_known: bool },
    /// The file the uploading transfer opened. Every read and retry uses it.
    Open(Arc<OpenFile>),
}

impl PathBody {
    /// Returns whether the uploading transfer must open the file before
    /// choosing how to send it.
    pub(super) fn needs_resolution(&self) -> bool {
        matches!(self.source, PathSource::Unresolved { .. })
    }

    /// Returns whether `length` is the number of bytes to read.
    pub(super) fn length_known(&self) -> bool {
        !matches!(
            self.source,
            PathSource::Unresolved {
                length_known: false
            }
        )
    }

    /// Opens the path through `filesystem`, and reads the remaining length
    /// from the open file when it is not known.
    ///
    /// Returns the body with an open file and a known length. Fails with
    /// [`ErrorKind::OffsetGreaterThanFileSize`] when the offset is past the end
    /// of the file, and with the filesystem's error when the path cannot be
    /// opened or inspected. A body that does not need resolution is returned
    /// unchanged.
    pub(super) async fn resolve(mut self, filesystem: &FileSystem) -> Result<Self, Error> {
        let PathSource::Unresolved { length_known } = self.source else {
            return Ok(self);
        };
        let file = filesystem
            .open_read(&self.path)
            .await
            .map_err(|e| open_error(&self.path, e))?;
        if !length_known {
            let metadata = filesystem.file_metadata(&file).await?;
            self.length = remaining_length(&metadata, self.offset)?;
        }
        self.source = PathSource::Open(Arc::new(file));
        Ok(self)
    }

    /// Returns the open file, opening the path on the calling thread when the
    /// body has not been resolved.
    pub(super) fn open(&self) -> io::Result<Arc<OpenFile>> {
        match &self.source {
            PathSource::Open(file) => Ok(Arc::clone(file)),
            PathSource::Path | PathSource::Unresolved { .. } => fs::open_read_blocking(&self.path)
                .map(Arc::new)
                .map_err(|e| open_error(&self.path, e)),
        }
    }
}

/// Adds the path to an error from opening it.
fn open_error(path: &std::path::Path, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("failed to open {}: {error}", path.display()),
    )
}

/// Returns the bytes from `offset` to the end of the file `metadata`
/// describes, or [`ErrorKind::OffsetGreaterThanFileSize`].
fn remaining_length(metadata: &Metadata, offset: u64) -> Result<u64, Error> {
    let file_size = metadata.len();
    if offset > file_size {
        return Err(ErrorKind::OffsetGreaterThanFileSize.into());
    }
    Ok(file_size - offset)
}

/// Builder for creating [`InputStream`] from a file/path.
///
/// ```no_run
/// # {
/// use aws_sdk_s3_transfer_manager::io::InputStream;
/// use std::path::Path;
///
/// async fn input_stream_from_file() -> InputStream {
///     let stream = InputStream::read_from()
///         .path("docs/some-large-file.csv")
///         // Specify the length of the file used (skips an additional call to retrieve the size)
///         .length(123_456)
///         .build()
///         .expect("valid path");
///     stream
/// }
/// # }
/// ```
///
/// On `wasm32-unknown-unknown`, [`build`](Self::build) does not access the
/// filesystem. The client that uploads the stream opens the path, and reads
/// its length when none was set, before sending a request.
#[derive(Debug, Default)]
pub struct PathBodyBuilder {
    path: Option<PathBuf>,
    length: Option<u64>,
    offset: Option<u64>,
    metadata: Option<Metadata>,
}

impl PathBodyBuilder {
    /// Create a new [`PathBodyBuilder`].
    ///
    /// You must call [`path`](PathBodyBuilder::path) to specify what to read from.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the path to read from.
    pub fn path(mut self, path: impl AsRef<std::path::Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Specify the offset to start reading from (in bytes)
    ///
    /// When used in conjunction with [`length`](PathBodyBuilder::length), allows for reading a single "chunk" of a file.
    pub fn offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Specify the length to read (in bytes).
    ///
    /// By pre-specifying the length, this API skips an additional call to retrieve the size from file-system metadata.
    ///
    /// When used in conjunction with [`offset`](PathBodyBuilder::offset), allows for reading a single "chunk" of a file.
    ///
    /// <div class="warning">
    /// Setting the length manually will trigger no validation related to any offset provided or the actual size of
    /// the file. This is an advanced setting mainly used to avoid an additional syscall if you know the
    /// size of the file already.
    /// </div>
    pub fn length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    /// Provide `metadata` for `self.path`, potentially reducing system calls during `build` if pre-set.
    pub(crate) fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Returns a [`InputStream`] from this builder.
    pub fn build(self) -> Result<InputStream, Error> {
        let path = self.path.expect("path set");
        let offset = self.offset.unwrap_or_default();

        let (length, length_known) = match (self.length, self.metadata) {
            (Some(explicit), _) => (explicit, true),
            (None, Some(metadata)) => (remaining_length(&metadata, offset)?, true),
            // A host filesystem reads the length when the upload opens the file.
            (None, None) if !fs::COMPLETES_INLINE => (0, false),
            // TODO(aws-sdk-rust#1159, design) - evaluate if we want build() to be async and to use tokio for stat() call (bytestream FsBuilder::build() is async)
            (None, None) => (
                remaining_length(&fs::metadata_blocking(&path)?, offset)?,
                true,
            ),
        };
        let source = if fs::COMPLETES_INLINE {
            PathSource::Path
        } else {
            PathSource::Unresolved { length_known }
        };

        let body = PathBody {
            path,
            length,
            offset,
            source,
        };

        let stream = InputStream {
            inner: RawInputStream::Fs(body),
        };

        Ok(stream)
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use tempfile::NamedTempFile;

    use crate::io::{path_body::PathBodyBuilder, InputStream};

    use super::PathBody;

    fn path_body(stream: &InputStream) -> &PathBody {
        match &stream.inner {
            crate::io::stream::RawInputStream::Buf(_) => panic!("unexpected inner body"),
            crate::io::stream::RawInputStream::Dyn(_) => panic!("unexpected inner body"),
            crate::io::stream::RawInputStream::Fs(path_body) => path_body,
        }
    }

    #[test]
    fn test_from_path() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();

        let stream = PathBodyBuilder::new().path(tmp.path()).build().unwrap();
        let body = path_body(&stream);
        assert_eq!(0, body.offset);
        assert_eq!(content.len() as u64, body.length);
    }

    #[test]
    fn test_explicit_content_length() {
        let tmp = NamedTempFile::new().unwrap();

        let stream = PathBodyBuilder::new()
            .path(tmp.path())
            .length(64)
            .build()
            .unwrap();

        let body = path_body(&stream);
        assert_eq!(0, body.offset);
        // we don't validate this
        assert_eq!(64, body.length);
    }

    #[test]
    fn test_length_with_offset() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();
        let offset = 5;

        let stream = PathBodyBuilder::new()
            .path(tmp.path())
            .offset(offset)
            .build()
            .unwrap();

        let body = path_body(&stream);
        assert_eq!(offset, body.offset);
        assert_eq!(content.len() as u64 - offset, body.length);
    }

    #[test]
    fn test_explicit_content_length_and_offset() {
        let tmp = NamedTempFile::new().unwrap();

        let stream = PathBodyBuilder::new()
            .path(tmp.path())
            .length(64)
            .offset(12)
            .build()
            .unwrap();

        let body = path_body(&stream);
        assert_eq!(12, body.offset);
        assert_eq!(64, body.length);
    }

    #[should_panic]
    #[test]
    fn test_invalid_offset() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();

        let _stream = PathBodyBuilder::new()
            .path(tmp.path())
            .offset(22)
            .build()
            .unwrap();
    }
}
