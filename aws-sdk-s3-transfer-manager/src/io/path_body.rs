/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error::{self, Error};
use crate::io::stream::{InputStream, RawInputStream};
use std::fs;
use std::path::PathBuf;

/// Input stream designed to wrap file based input.
#[derive(Debug)]
pub(super) struct PathBody {
    // The path to the file
    pub(super) path: PathBuf,
    // The total number of bytes to read
    pub(super) length: u64,
    // The byte-offset to start reading from
    pub(super) offset: u64,
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
#[derive(Debug, Default)]
pub struct PathBodyBuilder {
    path: Option<PathBuf>,
    length: Option<u64>,
    offset: Option<u64>,
    metadata: Option<fs::Metadata>,
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
    pub(crate) fn metadata(mut self, metadata: fs::Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Returns an [`InputStream`] that reads the configured byte range of the file.
    ///
    /// Without an explicit [`length`](PathBodyBuilder::length), the file's size
    /// is read from filesystem metadata and the stream covers the bytes from
    /// the offset to the end of the file.
    ///
    /// # Errors
    ///
    /// - [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) when
    ///   no [`path`](PathBodyBuilder::path) was set, or when no length was set
    ///   and the offset is greater than the file's size.
    /// - [`ErrorKind::IOError`](crate::error::ErrorKind::IOError) when no length
    ///   was set and the file's metadata cannot be read, for example because the
    ///   file does not exist, or the path does not name a regular file.
    pub fn build(self) -> Result<InputStream, Error> {
        let path = self
            .path
            .ok_or_else(|| error::invalid_input("a path is required to read a file"))?;
        let offset = self.offset.unwrap_or_default();

        let length = match self.length {
            None => {
                // TODO(aws-sdk-rust#1159, design) - evaluate if we want build() to be async and to use tokio for stat() call (bytestream FsBuilder::build() is async)
                let metadata = match self.metadata {
                    Some(metadata) => metadata,
                    None => fs::metadata(&path)
                        .map_err(|e| error::io_at("reading metadata of", &path, e))?,
                };
                if !metadata.is_file() {
                    return Err(crate::io::error::Error::not_regular_file(path).into());
                }
                let file_size = metadata.len();

                if offset > file_size {
                    return Err(error::invalid_input(format!(
                        "offset {offset} is greater than the size of {} ({file_size} bytes)",
                        path.display()
                    )));
                }

                file_size - offset
            }
            Some(explicit) => explicit,
        };

        let body = PathBody {
            path,
            length,
            offset,
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

    use crate::error::ErrorKind;
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

    /// Asserts that building from `path` fails with an I/O error naming a
    /// non-regular file.
    #[cfg(unix)]
    fn assert_build_rejects_non_regular_file(path: &std::path::Path) {
        use aws_smithy_types::error::display::DisplayErrorContext;

        let error = PathBodyBuilder::new().path(path).build().unwrap_err();
        assert_eq!(error.kind(), &crate::error::ErrorKind::IOError);
        let message = DisplayErrorContext(&error).to_string();
        assert!(
            message.contains("is not a regular file"),
            "{}: {message}",
            path.display()
        );
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    #[cfg(unix)]
    fn test_rejects_non_regular_files() {
        let directory = tempfile::tempdir().unwrap();
        for path in [std::path::Path::new("/dev/zero"), directory.path()] {
            assert_build_rejects_non_regular_file(path);
        }
    }

    /// Android's app sandbox denies creating the FIFO.
    #[cfg_attr(miri, ignore)]
    #[test]
    #[cfg(all(unix, not(target_os = "android")))]
    fn test_rejects_fifo() {
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRWXU).unwrap();
        assert_build_rejects_non_regular_file(&fifo);
    }

    #[test]
    fn test_invalid_offset() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();

        // One byte past the end of the file is the first rejected offset.
        let err = PathBodyBuilder::new()
            .path(tmp.path())
            .offset(content.len() as u64 + 1)
            .build()
            .unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::InputInvalid);
    }

    #[test]
    fn test_offset_at_end_of_file_is_empty() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();

        let stream = PathBodyBuilder::new()
            .path(tmp.path())
            .offset(content.len() as u64)
            .build()
            .unwrap();
        assert_eq!(0, path_body(&stream).length);
    }

    #[test]
    fn test_missing_path_is_invalid_input() {
        let err = PathBodyBuilder::new().build().unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::InputInvalid);
    }

    #[test]
    fn test_missing_file_is_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = PathBodyBuilder::new()
            .path(dir.path().join("missing"))
            .build()
            .unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::IOError);
        let source = std::error::Error::source(&err)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("the filesystem error is the source");
        assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_supplied_metadata_skips_stat() {
        let mut tmp = NamedTempFile::new().unwrap();
        let content = "hello path body";
        tmp.write_all(content.as_bytes()).unwrap();
        let metadata = std::fs::metadata(tmp.path()).unwrap();
        let dir = tempfile::tempdir().unwrap();

        // Statting the missing path would fail, so success shows the supplied
        // metadata was used in its place.
        let stream = PathBodyBuilder::new()
            .path(dir.path().join("missing"))
            .metadata(metadata)
            .build()
            .unwrap();
        let body = path_body(&stream);
        assert_eq!(0, body.offset);
        assert_eq!(content.len() as u64, body.length);
    }
}
