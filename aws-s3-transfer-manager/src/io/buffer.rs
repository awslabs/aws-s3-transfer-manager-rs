/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::ops::{Deref, DerefMut};

use bytes::{BufMut, Bytes, BytesMut};

// Akin to tokio::ReadBuf or the proposed [RFC](https://github.com/rust-lang/rfcs/blob/master/text/2930-read-buf.md#summary)
// but a type we own and can control allocations for.

// TODO - provide a way to create a Buffer from our own allocation/memory pool. The
// `Bytes::from_owner` implementation (see https://github.com/tokio-rs/bytes/pull/742) will provide
// our path to pooling buffers (for uploads at least).

/// A mutable buffer for writing sequential bytes to.
#[derive(Debug)]
pub(crate) struct Buffer {
    inner: BytesMut,
}

impl Buffer {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(capacity),
        }
    }

    /// Returns the number of bytes this buffer can hold without reallocating.
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Sets the length of the buffer. This will explicitly set the size of the buffer without
    /// actually modifying the data. It is up to the caller to ensure the data has been
    /// initialized.
    pub(crate) unsafe fn set_len(&mut self, len: usize) {
        self.inner.set_len(len)
    }
}

impl AsRef<[u8]> for Buffer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl Deref for Buffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.inner.deref()
    }
}

impl DerefMut for Buffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.inner.deref_mut()
    }
}

unsafe impl BufMut for Buffer {
    fn remaining_mut(&self) -> usize {
        self.inner.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.inner.advance_mut(cnt)
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        self.inner.chunk_mut()
    }
}

// we purposefully don't implement `From<Bytes>` here as we want to control the allocation
#[allow(clippy::from_over_into)]
impl Into<Bytes> for Buffer {
    fn into(self) -> Bytes {
        self.inner.into()
    }
}
