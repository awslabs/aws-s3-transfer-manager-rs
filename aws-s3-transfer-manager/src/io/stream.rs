/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::default::Default;
use std::fmt;
use std::future::poll_fn;
use std::path::Path;
use std::pin::Pin;

use aws_sdk_s3::primitives::ByteStream;
use bytes::{Buf, Bytes};

use crate::error;
use crate::io::path_body::PathBody;
use crate::io::path_body::PathBodyBuilder;
use crate::io::size_hint::SizeHint;

use super::part_reader::PartData;

/// Source of binary data.
///
/// `InputStream` wraps a stream of data for ease of use.
#[derive(Debug)]
pub struct InputStream {
    pub(super) inner: RawInputStream,
}

impl InputStream {
    /// Create a new `InputStream` from a static byte slice
    pub fn from_static(bytes: &'static [u8]) -> Self {
        let inner = RawInputStream::Buf(bytes.into());
        Self { inner }
    }

    /// Return the bounds on the remaining length of the `InputStream`
    pub fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    /// Returns a [`PathBodyBuilder`], allowing you to build a `InputStream` with
    /// full control over how the file is read (eg. specifying the length of
    /// the file or the starting offset to read from).
    ///
    /// ```no_run
    /// # {
    /// use aws_s3_transfer_manager::io::InputStream;
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
    pub fn read_from() -> PathBodyBuilder {
        PathBodyBuilder::new()
    }

    /// Create a new `InputStream` that reads data from a given `path`.
    ///
    /// ## Warning
    /// The contents of the file MUST not change. The length & checksum of the file
    /// will be cached. If the contents of the file change, the operation will almost certainly fail.
    ///
    /// Furthermore, a partial write MAY seek in the file and resume from the previous location.
    ///
    /// # Examples
    /// ```no_run
    /// use aws_s3_transfer_manager::io::InputStream;
    /// use std::path::Path;
    ///  async fn make_stream() -> InputStream {
    ///     InputStream::from_path("docs/rows.csv").expect("file should be readable")
    /// }
    /// ```
    pub fn from_path(path: impl AsRef<Path>) -> Result<InputStream, crate::io::error::Error> {
        Self::read_from().path(path).build()
    }

    /// Converts `InputStream` to ByteStream that can be used in PutObject.
    pub(crate) async fn into_byte_stream(self) -> Result<ByteStream, error::Error> {
        match self.inner {
            RawInputStream::Fs(path_body) => ByteStream::from_path(path_body.path)
                .await
                .map_err(error::from_kind(error::ErrorKind::IOError)),
            RawInputStream::Buf(bytes) => Ok(ByteStream::from(bytes)),
            RawInputStream::Dyn(_) => todo!(),
        }
    }

    /// Create a new `InputStream` that reads data from the given [`PartStream`] implementation.
    pub fn from_part_stream<T: PartStream + Send + Sync + 'static>(stream: T) -> Self {
        let inner = RawInputStream::Dyn(BoxStream::new(stream));
        Self { inner }
    }
}

#[derive(Debug)]
pub(super) enum RawInputStream {
    /// In-memory buffer to read from
    Buf(Bytes),
    /// File based input
    Fs(PathBody),
    /// User provided custom stream
    Dyn(BoxStream),
}

/// Trait representing a stream of object parts (streaming body).
///
/// Individual parts are streamed via the `poll_part` function, which asynchronously yields
/// instances of `PartData`. When `Poll::Ready(None)` is returned the stream is assumed to have
/// reached EOF and is finished.
///
/// The `size_hint` function provides insight into the total number of bytes that will be streamed.
pub trait PartStream {
    /// Attempt to pull the next part from the stream
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::io::Result<PartData>>>;

    /// Returns the bounds on the total size of the stream
    fn size_hint(&self) -> crate::io::SizeHint;
}

pub(crate) struct BoxStream {
    inner: Pin<Box<dyn PartStream + Send + Sync + 'static>>,
}

impl BoxStream {
    fn new<T: PartStream + Send + Sync + 'static>(inner: T) -> Self {
        BoxStream {
            inner: Box::pin(inner),
        }
    }

    pub(crate) async fn next(&mut self) -> Option<std::io::Result<PartData>> {
        poll_fn(|cx| self.inner.as_mut().poll_part(cx)).await
    }
}

impl fmt::Debug for BoxStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxStream(dyn PartStream)").finish()
    }
}

impl RawInputStream {
    pub(super) fn size_hint(&self) -> SizeHint {
        match self {
            RawInputStream::Buf(bytes) => SizeHint::exact(bytes.remaining() as u64),
            RawInputStream::Fs(path_body) => SizeHint::exact(path_body.length),
            RawInputStream::Dyn(box_body) => box_body.inner.size_hint(),
        }
    }
}

impl Default for InputStream {
    fn default() -> Self {
        Self {
            inner: RawInputStream::Buf(Bytes::default()),
        }
    }
}

impl From<Bytes> for InputStream {
    fn from(value: Bytes) -> Self {
        Self {
            inner: RawInputStream::Buf(value),
        }
    }
}

impl From<Vec<u8>> for InputStream {
    fn from(value: Vec<u8>) -> Self {
        Self::from(Bytes::from(value))
    }
}

impl From<&'static [u8]> for InputStream {
    fn from(slice: &'static [u8]) -> InputStream {
        Self::from(Bytes::from_static(slice))
    }
}

impl From<&'static str> for InputStream {
    fn from(slice: &'static str) -> InputStream {
        Self::from(Bytes::from_static(slice.as_bytes()))
    }
}
