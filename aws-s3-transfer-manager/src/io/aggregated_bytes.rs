/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::IoSlice;

use aws_sdk_s3::primitives::ByteStream;
use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;

use crate::error::ErrorKind;

///
/// Non-contiguous Binary Data Storage
///
/// When data is read from the network, it is read in a sequence of chunks that are not in
/// contiguous memory. [`AggregatedBytes`](crate::byte_stream::AggregatedBytes) provides a view of
/// this data via [`impl Buf`](bytes::Buf) or it can be copied into contiguous storage with
/// [`.into_bytes()`](crate::byte_stream::AggregatedBytes::into_bytes).
#[derive(Debug, Clone)]
pub struct AggregatedBytes(pub(crate) SegmentedBuf<Bytes>);

impl AggregatedBytes {
    /// Convert this buffer into [`Bytes`].
    ///
    /// # Why does this consume `self`?
    /// Technically, [`copy_to_bytes`](bytes::Buf::copy_to_bytes) can be called without ownership of self. However, since this
    /// mutates the underlying buffer such that no data is remaining, it is more misuse resistant to
    /// prevent the caller from attempting to reread the buffer.
    ///
    /// If the caller only holds a mutable reference, they may use [`copy_to_bytes`](bytes::Buf::copy_to_bytes)
    /// directly on `AggregatedBytes`.
    pub fn into_bytes(mut self) -> Bytes {
        self.0.copy_to_bytes(self.0.remaining())
    }

    /// Convert this buffer into an [`Iterator`] of underlying non-contiguous segments of [`Bytes`]
    pub fn into_segments(self) -> impl Iterator<Item = Bytes> {
        self.0.into_inner().into_iter()
    }

    /// Convert this buffer into a `Vec<u8>`
    pub fn to_vec(self) -> Vec<u8> {
        self.0.into_inner().into_iter().flatten().collect()
    }

    /// Make this buffer from a ByteStream
    pub(crate) async fn from_byte_stream(value: ByteStream) -> Result<Self, crate::error::Error> {
        let mut value = value;
        let mut output = SegmentedBuf::new();
        while let Some(buf) = value.next().await {
            match buf {
                Ok(buf) => output.push(buf),
                Err(err) => return Err(crate::error::from_kind(ErrorKind::ChunkFailed)(err)),
            };
        }
        Ok(AggregatedBytes(output))
    }
}

impl Buf for AggregatedBytes {
    // Forward all methods that SegmentedBuf has custom implementations of.
    fn remaining(&self) -> usize {
        self.0.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.0.chunk()
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        self.0.chunks_vectored(dst)
    }

    fn advance(&mut self, cnt: usize) {
        self.0.advance(cnt)
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        self.0.copy_to_bytes(len)
    }
}

