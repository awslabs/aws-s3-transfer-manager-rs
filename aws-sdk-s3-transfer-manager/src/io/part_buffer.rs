/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Pooled mutable storage for custom and adapted upload streams.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::ptr::NonNull;
use std::task::{Context, Poll};

use bytes::buf::UninitSlice;
use bytes::BufMut;

use crate::memory::{BufferPool, PooledBufMut, Reservation, ReserveFuture, SegmentedBytes};

/// Mutable pooled storage for one upload part.
///
/// A `PartBuffer` is created by [`StreamContext::part_buffer`](super::StreamContext::part_buffer).
/// Creating it does not reserve memory. Call [`PartBuffer::poll_acquire`] before writing, retain
/// the buffer across [`Poll::Pending`], and call [`PartBuffer::freeze`] when the part is complete.
///
/// Each acquisition request defines one logical memory envelope. The buffer may retain carrier
/// padding internally, but [`BufMut::remaining_mut`] exposes only bytes inside that envelope. When
/// an envelope is full, the next call to `poll_acquire` publishes it internally without copying
/// and begins the requested next envelope. This permits incremental producers to use several
/// smaller reservations without discarding partially used carrier tails.
///
/// The common path requests [`StreamContext::part_size`](super::StreamContext::part_size) once and
/// fills the complete part through repeated polls. Smaller envelopes are useful for externally
/// paced producers, but completed envelopes remain charged until the returned
/// [`SegmentedBytes`] is released.
#[derive(Debug)]
pub struct PartBuffer {
    pool: BufferPool,
    max_len: usize,
    completed: SegmentedBytes,
    state: PartBufferState,
}

// SAFETY: pooled mutable authority is reachable only through methods requiring `&mut self`.
// Shared methods inspect lengths and immutable completed bytes; they neither expose nor mutate the
// exclusive ranges held by `PooledBufMut`.
unsafe impl Sync for PartBuffer {}

/// Admission and mutable ownership for the current logical envelope.
#[derive(Debug)]
enum PartBufferState {
    /// No reservation request or mutable buffer is active.
    Empty,
    /// The first poll latched `envelope` and entered pool admission.
    Reserving {
        envelope_remaining: usize,
        future: ReserveFuture,
    },
    /// One granted envelope owns mutable pooled storage.
    Writable {
        envelope_remaining: usize,
        reservation: Option<Reservation>,
        buffer: PooledBufMut,
    },
}

impl PartBuffer {
    /// Creates an empty buffer bound to one pool and part-size limit.
    pub(crate) fn new(pool: BufferPool, max_len: usize) -> Self {
        assert!(max_len != 0, "part buffer length must be nonzero");
        Self {
            pool,
            max_len,
            completed: SegmentedBytes::from(bytes::Bytes::new()),
            state: PartBufferState::Empty,
        }
    }

    /// Polls until the buffer has writable capacity under a pooled reservation.
    ///
    /// `envelope` is the maximum number of logical bytes admitted by a newly started request. The
    /// first value is retained while admission is pending. Calls made while the current envelope
    /// still has writable capacity return immediately; their `envelope` argument applies only
    /// after the current envelope becomes full.
    ///
    /// A full current envelope is published internally before the next reservation begins. The
    /// requested envelope must be nonzero and no larger than [`Self::remaining`].
    ///
    /// Reservation and acquisition failures are returned as [`io::Error`] values that retain the
    /// underlying pool error as their source.
    pub fn poll_acquire(&mut self, cx: &mut Context<'_>, envelope: usize) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                PartBufferState::Writable {
                    envelope_remaining,
                    buffer,
                    ..
                } if *envelope_remaining != 0 && buffer.remaining_mut() != 0 => {
                    return Poll::Ready(Ok(()));
                }
                PartBufferState::Writable {
                    envelope_remaining, ..
                } => {
                    let continuation = *envelope_remaining;
                    let tail = self.publish_current();
                    if continuation == 0 {
                        let remaining = self.remaining();
                        if envelope == 0 || envelope > remaining {
                            return Poll::Ready(Err(invalid_envelope(envelope, remaining)));
                        }
                        if let Some(buffer) = tail {
                            self.state = PartBufferState::Writable {
                                envelope_remaining: envelope,
                                reservation: None,
                                buffer,
                            };
                            return Poll::Ready(Ok(()));
                        }
                        self.start_reservation(envelope);
                    } else {
                        if tail.is_some() {
                            panic!("exhausted part-buffer storage retained writable capacity");
                        }
                        self.start_reservation(continuation);
                    }
                }
                PartBufferState::Empty => {
                    let remaining = self.remaining();
                    if envelope == 0 || envelope > remaining {
                        return Poll::Ready(Err(invalid_envelope(envelope, remaining)));
                    }
                    self.start_reservation(envelope);
                }
                PartBufferState::Reserving {
                    envelope_remaining,
                    future,
                } => {
                    let reservation = match Pin::new(future).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(reservation)) => reservation,
                        Poll::Ready(Err(error)) => {
                            self.state = PartBufferState::Empty;
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                    };
                    let envelope_remaining = *envelope_remaining;
                    match self.pool.acquire(&reservation, envelope_remaining) {
                        Ok(buffer) => {
                            self.state = PartBufferState::Writable {
                                envelope_remaining,
                                reservation: Some(reservation),
                                buffer,
                            };
                            return Poll::Ready(Ok(()));
                        }
                        Err(error) => {
                            self.state = PartBufferState::Empty;
                            return Poll::Ready(Err(io::Error::other(error)));
                        }
                    }
                }
            }
        }
    }

    /// Returns initialized bytes accumulated for this part.
    pub fn len(&self) -> usize {
        self.completed
            .len()
            .checked_add(self.current_len())
            .expect("part buffer length overflowed")
    }

    /// Returns whether no bytes have been written.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns logical bytes remaining before this part reaches its limit.
    ///
    /// This differs from [`BufMut::remaining_mut`], which reports capacity available under the
    /// current reservation only.
    pub fn remaining(&self) -> usize {
        self.max_len
            .checked_sub(self.len())
            .expect("part buffer length exceeded its limit")
    }

    /// Publishes all initialized bytes and ends pending or active admission.
    ///
    /// A pending reservation is cancelled. An active partial envelope transfers its initialized
    /// bytes without copying and releases wholly unused pooled carriers.
    pub fn freeze(mut self) -> SegmentedBytes {
        if matches!(self.state, PartBufferState::Writable { .. }) {
            self.freeze_current();
        }
        self.completed
    }

    /// Returns initialized bytes in the active mutable buffer.
    fn current_len(&self) -> usize {
        match &self.state {
            PartBufferState::Writable { buffer, .. } => buffer.len(),
            PartBufferState::Empty | PartBufferState::Reserving { .. } => 0,
        }
    }

    /// Starts admission for storage not already covered by a retained tail.
    fn start_reservation(&mut self, envelope_remaining: usize) {
        self.state = PartBufferState::Reserving {
            envelope_remaining,
            future: self.pool.reserve(envelope_remaining),
        };
    }

    /// Publishes initialized prefixes while retaining any writable carrier tail.
    fn publish_current(&mut self) -> Option<PooledBufMut> {
        let state = std::mem::replace(&mut self.state, PartBufferState::Empty);
        let PartBufferState::Writable {
            reservation,
            mut buffer,
            ..
        } = state
        else {
            panic!("part buffer has no writable envelope to publish");
        };
        while !buffer.is_empty() {
            let count = buffer.initialized_chunk().len();
            let published = buffer.publish_prefix(count);
            self.completed.append_pool_view(&self.pool, published);
        }
        if let Some(reservation) = reservation {
            reservation.close_acquisition();
        }
        (buffer.remaining_mut() != 0).then_some(buffer)
    }

    /// Freezes the active mutable buffer when no later write needs its tail.
    fn freeze_current(&mut self) {
        let state = std::mem::replace(&mut self.state, PartBufferState::Empty);
        let PartBufferState::Writable {
            reservation,
            buffer,
            ..
        } = state
        else {
            panic!("part buffer has no writable envelope to freeze");
        };
        if let Some(reservation) = reservation {
            reservation.close_acquisition();
        }
        self.completed.append(buffer.freeze());
    }
}

// SAFETY: writable bytes are delegated to one `PooledBufMut` under exclusive ownership. The
// logical envelope only narrows the range that buffer already permits the caller to initialize.
unsafe impl BufMut for PartBuffer {
    fn remaining_mut(&self) -> usize {
        match &self.state {
            PartBufferState::Writable {
                envelope_remaining,
                buffer,
                ..
            } => (*envelope_remaining).min(buffer.remaining_mut()),
            PartBufferState::Empty | PartBufferState::Reserving { .. } => 0,
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let PartBufferState::Writable {
            envelope_remaining,
            buffer,
            ..
        } = &mut self.state
        else {
            // SAFETY: a dangling pointer is valid for a zero-length slice, and the returned borrow
            // remains tied to `self`.
            return unsafe {
                UninitSlice::from_raw_parts_mut(NonNull::<u8>::dangling().as_ptr(), 0)
            };
        };
        let chunk = buffer.chunk_mut();
        let exposed = chunk.len().min(*envelope_remaining);
        &mut chunk[..exposed]
    }

    unsafe fn advance_mut(&mut self, count: usize) {
        assert!(
            count <= self.remaining_mut(),
            "advanced beyond the current part-buffer envelope"
        );
        let PartBufferState::Writable {
            envelope_remaining,
            buffer,
            ..
        } = &mut self.state
        else {
            assert_eq!(count, 0, "advanced a part buffer without writable storage");
            return;
        };
        // SAFETY: the caller initialized `count` bytes from the range returned by `chunk_mut`, and
        // the assertion above keeps the update inside both the pooled range and logical envelope.
        unsafe {
            buffer.advance_mut(count);
        }
        *envelope_remaining = envelope_remaining
            .checked_sub(count)
            .expect("advanced beyond the current part-buffer envelope");
    }
}

/// Constructs an invalid-input error for an envelope outside the remaining part.
fn invalid_envelope(envelope: usize, remaining: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("part buffer envelope must be between 1 and {remaining} bytes, got {envelope}"),
    )
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use bytes::{Buf, BufMut};

    use super::*;
    use crate::types::MemoryBudgetConfig;

    const TEST_POOL_CAPACITY: usize = 1024 * 1024;

    fn test_pool() -> BufferPool {
        BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(TEST_POOL_CAPACITY))
            .build()
            .unwrap()
    }

    fn poll_acquire(buffer: &mut PartBuffer, envelope: usize) -> Poll<io::Result<()>> {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        buffer.poll_acquire(&mut cx, envelope)
    }

    #[test]
    fn complete_envelope_freezes_without_copying() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let mut buffer = PartBuffer::new(pool.clone(), carrier_size);

        assert!(poll_acquire(&mut buffer, carrier_size).is_ready());
        let ptr = buffer.chunk_mut().as_mut_ptr();
        buffer.put_slice(b"payload");
        assert_eq!(buffer.len(), 7);
        assert_eq!(buffer.remaining(), carrier_size - 7);

        let frozen = buffer.freeze();
        assert_eq!(frozen.chunk().as_ptr(), ptr.cast());
        assert_eq!(frozen.chunk(), b"payload");
        assert_eq!(pool.metrics().charged_capacity_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn full_envelopes_roll_over_without_losing_carrier_tails() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(pool.clone(), 7);

        assert!(poll_acquire(&mut buffer, 3).is_ready());
        buffer.put_slice(b"abc");
        assert_eq!(buffer.remaining_mut(), 0);

        assert!(poll_acquire(&mut buffer, 4).is_ready());
        buffer.put_slice(b"defg");
        assert_eq!(buffer.len(), 7);
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            pool.carrier_size() as u64
        );

        let frozen = buffer.freeze();
        let segments = frozen.into_segments();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], b"abcdefg"[..]);
        drop(segments);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn envelope_continues_after_exhausting_a_reused_tail() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let mut buffer = PartBuffer::new(pool.clone(), carrier_size + 3);
        let mut expected = Vec::with_capacity(carrier_size + 3);

        assert!(poll_acquire(&mut buffer, 3).is_ready());
        buffer.put_slice(b"abc");
        expected.extend_from_slice(b"abc");

        assert!(poll_acquire(&mut buffer, carrier_size).is_ready());
        let tail_len = buffer.remaining_mut();
        let tail = vec![b'x'; tail_len];
        buffer.put_slice(&tail);
        expected.extend_from_slice(&tail);
        assert_eq!(buffer.remaining_mut(), 0);

        assert!(poll_acquire(&mut buffer, 1).is_ready());
        assert_eq!(buffer.remaining_mut(), 3);
        buffer.put_slice(b"end");
        expected.extend_from_slice(b"end");

        assert_eq!(buffer.freeze().into_contiguous(), expected);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn partial_envelope_remains_active_across_acquire_polls() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(pool, 8);

        assert!(poll_acquire(&mut buffer, 8).is_ready());
        buffer.put_slice(b"abc");
        assert!(poll_acquire(&mut buffer, 2).is_ready());
        assert_eq!(buffer.remaining_mut(), 5);
        buffer.put_slice(b"defgh");

        assert_eq!(buffer.freeze().into_contiguous(), b"abcdefgh"[..]);
    }

    #[test]
    fn pending_reservation_is_retained_and_cancelled_on_drop() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let holder = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let held = pool.acquire(&holder, TEST_POOL_CAPACITY).unwrap();
        let mut buffer = PartBuffer::new(pool.clone(), carrier_size);

        assert!(poll_acquire(&mut buffer, carrier_size).is_pending());
        assert_eq!(pool.metrics().queued_reservations(), 1);
        drop(buffer);
        assert_eq!(pool.metrics().queued_reservations(), 0);

        drop(held);
        holder.close_acquisition();
    }

    #[test]
    fn pending_reservation_latches_its_first_envelope() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let holder = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let held = pool.acquire(&holder, TEST_POOL_CAPACITY).unwrap();
        let mut buffer = PartBuffer::new(pool, carrier_size);

        assert!(poll_acquire(&mut buffer, 3).is_pending());
        assert!(poll_acquire(&mut buffer, 7).is_pending());

        drop(held);
        holder.close_acquisition();

        assert!(poll_acquire(&mut buffer, 7).is_ready());
        assert_eq!(buffer.remaining_mut(), 3);
    }

    #[test]
    fn invalid_envelope_does_not_enter_admission() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(pool.clone(), 8);

        let Poll::Ready(result) = poll_acquire(&mut buffer, 9) else {
            panic!("invalid envelope entered admission");
        };
        let error = result.unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(pool.metrics().reservation_enqueues_total(), 0);
    }

    #[test]
    fn part_buffer_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PartBuffer>();
    }
}
