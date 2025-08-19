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
use crate::io::Buffer;

/// Source of binary data.
///
/// `InputStream` wraps a stream of data for ease of use.
///
/// To create an `InputStream`:
///
/// * From an in-memory source: use [`from_static`] or one of the provided `From` implementations.
/// * From a file path: use [`from_path`] or [`read_from`]
/// * From a custom implementation: use [`from_part_stream`]
///
/// [`from_static`]: InputStream::from_static
/// [`from_path`]: InputStream::from_path
/// [`read_from`]: InputStream::read_from
/// [`from_part_stream`]: InputStream::from_part_stream
///
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
    /// use aws_sdk_s3_transfer_manager::io::InputStream;
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
    /// use aws_sdk_s3_transfer_manager::io::InputStream;
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
            RawInputStream::Dyn(_) => {
                unreachable!("dyn InputStream should not have into_byte_stream called on it!")
            }
        }
    }

    /// Test if this InputStream can only be uploaded via MPU (e.g. a custom `PartStream`
    /// implementation from a user can only be a MPU due to the ability to provide custom
    /// metadata like checksums).
    pub(crate) fn is_mpu_only(&self) -> bool {
        // TODO - for our own wrappers we can probably be smarter
        matches!(self.inner, RawInputStream::Dyn(_))
    }

    /// Create a new `InputStream` that reads data from the given [`PartStream`] implementation
    /// for a [multipart upload].
    ///
    /// NOTE: Implementing `PartStream` directly is a more advanced use case. You should reach for
    /// one of the provided implementations or adapters first if possible.
    ///
    /// [multipart upload]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
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

/// The context of an input stream.
#[derive(Debug)]
pub struct StreamContext {
    part_size: usize,
}

impl StreamContext {
    pub(super) fn new(part_size: usize) -> Self {
        Self { part_size }
    }

    /// The part size to use when yielding parts.
    /// NOTE: this _may_ differ from the configured part size (e.g. if the target part size would
    /// result in exceeding the maximum number of parts allowed).
    pub fn part_size(&self) -> usize {
        self.part_size
    }

    // TODO - eventually make the ability to allocate a buffer public after carefully review of the `Buffer` API.
    /// Request a new buffer to fill
    pub(crate) fn new_buffer(&self, capacity: usize) -> Buffer {
        // TODO - replace allocation with memory pool
        Buffer::new(capacity)
    }
}

/// Contents and (optional) metadata for a single part of a [multipart upload].
///
/// [multipart upload]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartData {
    // 1-indexed
    pub(crate) part_number: u64,
    pub(crate) data: Bytes,
    pub(crate) checksum: Option<String>,
    pub(crate) is_last: Option<bool>,
}

impl PartData {
    /// Check if this is the last part
    ///
    /// It is `Option` because it's not always possible to determine
    /// whether the just-yielded part is the last one, e.g., streaming cases.
    pub fn is_last(&self) -> Option<bool> {
        self.is_last
    }

    /// Create a new part
    pub fn new(part_number: u64, data: impl Into<Bytes>) -> Self {
        debug_assert!(
            part_number > 0,
            "part numbers are 1-indexed and must be greater than zero"
        );
        Self {
            part_number,
            data: data.into(),
            checksum: None,
            is_last: None,
        }
    }

    /// Set this part's checksum, if you've calculated it yourself
    /// (base64 encoding of the big-endian checksum value for this part's data
    /// using the algorithm specified in the [ChecksumStrategy](crate::operation::upload::ChecksumStrategy)).
    ///
    /// If you don't set this, the Transfer Manager will calculate one
    /// automatically, unless you've explicitly disabled checksum calculation
    /// (see [ChecksumStrategy](crate::operation::upload::ChecksumStrategy)).
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Mark this part as the last part
    pub fn mark_last(mut self, is_last: bool) -> Self {
        self.is_last = Some(is_last);
        self
    }
}

/// Trait representing a stream of object parts (streaming body).
///
/// Individual parts are streamed via the `poll_part` function, which asynchronously yields
/// instances of `PartData`. When `Poll::Ready(None)` is returned the stream is assumed to have
/// reached EOF and is finished.
///
/// The `size_hint` function provides insight into the total number of bytes that will be streamed.
pub trait PartStream {
    /// Attempt to pull the next part from the stream.
    ///
    /// The `stream_cx` will have the part size that should be utilized. Implementations should be
    /// careful to only yield full parts for every part except the last one, which _may_ be less
    /// than the full part size.
    fn poll_part(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        stream_cx: &StreamContext,
    ) -> std::task::Poll<Option<std::io::Result<PartData>>>;

    /// Returns the bounds on the total size of the stream
    fn size_hint(&self) -> crate::io::SizeHint;

    /// If you calculated the full object checksum while streaming, return it.
    /// This will be sent to S3 for validation against the checksum it calculated server side.
    ///
    /// If None is returned (the default implementation), S3 will not do this additional validation.
    ///
    /// This function is called once, after [`PartStream::poll_part()`] yields the final part,
    /// if and only if you used a [ChecksumStrategy](crate::operation::upload::ChecksumStrategy) with
    /// [ChecksumType::FullObject](aws_sdk_s3::types::ChecksumType) and didn't set its
    /// [full_object_checksum](crate::operation::upload::ChecksumStrategy::full_object_checksum)
    /// value up front.
    ///
    /// Return the base64 encoding of the big-endian checksum value of the full object's data,
    /// using the algorithm specified in the [ChecksumStrategy](crate::operation::upload::ChecksumStrategy)).
    fn full_object_checksum(&self) -> Option<String> {
        None
    }
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

    pub(crate) async fn next(
        &mut self,
        stream_cx: &StreamContext,
    ) -> Option<std::io::Result<PartData>> {
        poll_fn(|cx| self.inner.as_mut().poll_part(cx, stream_cx)).await
    }

    pub(crate) fn full_object_checksum(&self) -> Option<String> {
        self.inner.full_object_checksum()
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
