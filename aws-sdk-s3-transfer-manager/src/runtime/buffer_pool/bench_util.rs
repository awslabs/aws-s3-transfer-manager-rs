/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Allocation-API access for out-of-crate benchmarks.
//!
//! Each type wraps the crate-private type of the same name and delegates every
//! call to it, so a benchmark measures the pool's own reservation, acquisition,
//! growth, publication, and return paths.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::buf::UninitSlice;
use bytes::{BufMut, Bytes};

use super::{BufferPool, SegmentedBytes};

/// Reservation and acquisition methods of [`BufferPool`].
pub trait BufferPoolExt {
    /// Returns the fixed writable allocation unit used by this pool.
    fn carrier_size(&self) -> usize;

    /// Attempts one immediate reservation grant.
    fn try_reserve(&self, bytes: usize) -> Result<Option<Reservation>, ReserveError>;

    /// Creates a lazy, cancellation-safe reservation request.
    fn reserve(&self, bytes: usize) -> ReserveFuture;

    /// Acquires at least `min_bytes` under one reservation.
    fn acquire(
        &self,
        reservation: &Reservation,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError>;

    /// Acquires at least `min_bytes` without reservation-local authority.
    fn acquire_unreserved(&self, min_bytes: usize) -> Result<PooledBufMut, AcquireError>;
}

impl BufferPoolExt for BufferPool {
    #[inline]
    fn carrier_size(&self) -> usize {
        BufferPool::carrier_size(self)
    }

    #[inline]
    fn try_reserve(&self, bytes: usize) -> Result<Option<Reservation>, ReserveError> {
        BufferPool::try_reserve(self, bytes)
            .map(|grant| grant.map(Reservation))
            .map_err(ReserveError)
    }

    #[inline]
    fn reserve(&self, bytes: usize) -> ReserveFuture {
        ReserveFuture(BufferPool::reserve(self, bytes))
    }

    #[inline]
    fn acquire(
        &self,
        reservation: &Reservation,
        min_bytes: usize,
    ) -> Result<PooledBufMut, AcquireError> {
        BufferPool::acquire(self, &reservation.0, min_bytes)
            .map(PooledBufMut)
            .map_err(AcquireError)
    }

    #[inline]
    fn acquire_unreserved(&self, min_bytes: usize) -> Result<PooledBufMut, AcquireError> {
        BufferPool::acquire_unreserved(self, min_bytes)
            .map(PooledBufMut)
            .map_err(AcquireError)
    }
}

/// Acquisition authority for one admitted memory envelope.
#[derive(Debug)]
pub struct Reservation(super::Reservation);

impl Reservation {
    /// Closes acquisition through this reservation.
    #[inline]
    pub fn close_acquisition(self) {
        self.0.close_acquisition();
    }
}

/// A cancellation-safe request for one reservation.
#[derive(Debug)]
#[must_use = "a reservation request does nothing unless polled or awaited"]
pub struct ReserveFuture(super::ReserveFuture);

impl Future for ReserveFuture {
    type Output = Result<Reservation, ReserveError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.map(Reservation).map_err(ReserveError))
    }
}

/// Failure to create a reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReserveError(super::ReserveError);

/// Failure to acquire pooled storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcquireError(super::AcquireError);

/// One growable, exclusively writable stream of pooled bytes.
#[derive(Debug)]
pub struct PooledBufMut(super::PooledBufMut);

impl PooledBufMut {
    /// Returns initialized and writable bytes retained by this buffer.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns initialized bytes that have not been published.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no initialized unpublished bytes remain.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the contiguous initialized prefix awaiting publication.
    #[inline]
    pub fn initialized_chunk(&self) -> &[u8] {
        self.0.initialized_chunk()
    }

    /// Ensures at least `min_writable` uninitialized writable bytes remain.
    #[inline]
    pub fn reserve(&mut self, min_writable: usize) -> Result<(), AcquireError> {
        self.0.reserve(min_writable).map_err(AcquireError)
    }

    /// Publishes the first `count` initialized bytes as one immutable view.
    #[inline]
    pub fn publish_prefix(&mut self, count: usize) -> Bytes {
        self.0.publish_prefix(count)
    }

    /// Publishes every initialized byte and releases unused writable capacity.
    #[inline]
    pub fn freeze(self) -> SegmentedBytes {
        self.0.freeze()
    }
}

// SAFETY: every method delegates to the wrapped buffer's `BufMut` implementation,
// whose contract this wrapper therefore satisfies unchanged.
unsafe impl BufMut for PooledBufMut {
    #[inline]
    fn remaining_mut(&self) -> usize {
        self.0.remaining_mut()
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.0.chunk_mut()
    }

    #[inline]
    unsafe fn advance_mut(&mut self, count: usize) {
        // SAFETY: the caller upholds `advance_mut`'s contract for the chunk most recently
        // returned by `chunk_mut`, which is the wrapped buffer's chunk.
        unsafe { self.0.advance_mut(count) }
    }
}
