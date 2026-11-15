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

use crate::memory::{BufferPool, SegmentedBytes};
use crate::runtime::buffer_pool::{PooledBufMut, Reservation, ReserveFuture};

/// Mutable pooled storage for one upload part.
///
/// [`PartBuffer::new`] creates an empty buffer charged to a [`BufferPool`]. Creating it admits no
/// memory. Call [`PartBuffer::poll_acquire`] before writing, retain the buffer across
/// [`Poll::Pending`], copy bytes in with [`PartBuffer::fill_from_slice`], and call
/// [`PartBuffer::freeze`] when the part is complete.
///
/// Each acquisition request admits one memory envelope. [`PartBuffer::writable`] exposes only bytes
/// inside that envelope. When an envelope is full, the next call to `poll_acquire` keeps its bytes
/// without copying and admits the requested next envelope.
///
/// The common path requests the part length once and fills the complete part through repeated
/// polls. Smaller envelopes suit externally paced producers, but completed envelopes remain charged
/// to the pool until the returned [`SegmentedBytes`] is released.
///
/// # Examples
///
/// A [`PartStream`](super::PartStream) that copies an in-memory source into parts charged to a
/// pool shared with the client:
///
/// ```
/// use std::pin::Pin;
/// use std::task::{ready, Context, Poll};
///
/// use aws_sdk_s3_transfer_manager::io::{PartBuffer, PartData, PartStream, SizeHint, StreamContext};
/// use aws_sdk_s3_transfer_manager::memory::BufferPool;
///
/// struct PooledSource {
///     pool: BufferPool,
///     source: &'static [u8],
///     offset: usize,
///     next_part: u64,
///     current: Option<PartBuffer>,
/// }
///
/// impl PartStream for PooledSource {
///     fn poll_part(
///         mut self: Pin<&mut Self>,
///         cx: &mut Context<'_>,
///         stream_cx: &StreamContext,
///     ) -> Poll<Option<std::io::Result<PartData>>> {
///         let this = &mut *self;
///         if this.offset == this.source.len() {
///             return Poll::Ready(None);
///         }
///         let part_len = stream_cx.part_size().min(this.source.len() - this.offset);
///         let part = this
///             .current
///             .get_or_insert_with(|| PartBuffer::new(&this.pool, part_len));
///         while part.remaining() != 0 {
///             ready!(part.poll_acquire(cx, part.remaining()))?;
///             this.offset += part.fill_from_slice(&this.source[this.offset..]);
///         }
///         let data = this.current.take().expect("part in progress").freeze();
///         let part_number = this.next_part;
///         this.next_part += 1;
///         Poll::Ready(Some(Ok(PartData::from_segmented(part_number, data))))
///     }
///
///     fn size_hint(&self) -> SizeHint {
///         SizeHint::exact(self.source.len() as u64)
///     }
/// }
/// ```
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
    /// Creates an empty buffer for one part of at most `max_len` bytes, charged to `pool`.
    ///
    /// Creating a buffer admits no memory; [`Self::poll_acquire`] does. The buffer is not tied to
    /// a client or a transfer. Share `pool` with a client through
    /// [`MemoryConfig::Explicit`](crate::memory::MemoryConfig::Explicit) to bound their memory
    /// use together.
    ///
    /// # Panics
    ///
    /// Panics if `max_len` is zero.
    pub fn new(pool: &BufferPool, max_len: usize) -> Self {
        assert!(max_len != 0, "part buffer length must be nonzero");
        Self {
            pool: pool.clone(),
            max_len,
            completed: SegmentedBytes::from(bytes::Bytes::new()),
            state: PartBufferState::Empty,
        }
    }

    /// Polls until [`Self::writable`] is nonzero, admitting up to `envelope` bytes when a new
    /// envelope is needed.
    ///
    /// The first `envelope` value is retained while admission is pending. Calls made while the
    /// current envelope still has writable bytes return immediately; their `envelope` argument
    /// applies only after the current envelope becomes full. Bytes written into a full envelope
    /// are kept before the next envelope is admitted. [`Self::writable`] may be smaller than the
    /// envelope's unwritten bytes; poll again once it reaches zero.
    ///
    /// Returns `Poll::Pending` until the pool admits the envelope; the waker of `cx` is notified
    /// when admission may progress. Dropping the buffer while admission is pending cancels the
    /// request.
    ///
    /// # Errors
    ///
    /// - [`io::ErrorKind::InvalidInput`] when a new envelope is needed and `envelope` is outside
    ///   `1..=`[`Self::remaining`]. The request does not enter admission.
    /// - [`io::ErrorKind::Other`] when the pool cannot admit or provide the envelope, including an
    ///   envelope larger than the pool's capacity. The error's source describes the failure; its
    ///   type is not public.
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

    /// Returns bytes that can still be written before this part reaches its maximum length.
    ///
    /// This differs from [`Self::writable`], which counts only bytes inside the currently admitted
    /// envelope.
    pub fn remaining(&self) -> usize {
        self.max_len
            .checked_sub(self.len())
            .expect("part buffer length exceeded its limit")
    }

    /// Returns bytes that can be written into the admitted envelope without another
    /// [`Self::poll_acquire`].
    ///
    /// Zero before the first acquisition, while admission is pending, and once the envelope is
    /// full. Never exceeds [`Self::remaining`].
    pub fn writable(&self) -> usize {
        match &self.state {
            PartBufferState::Writable {
                envelope_remaining,
                buffer,
                ..
            } => (*envelope_remaining).min(buffer.remaining_mut()),
            PartBufferState::Empty | PartBufferState::Reserving { .. } => 0,
        }
    }

    /// Copies the first `min(src.len(), self.writable())` bytes of `src` and returns that count.
    ///
    /// Returns zero when [`Self::writable`] is zero; poll [`Self::poll_acquire`] to admit more.
    pub fn fill_from_slice(&mut self, src: &[u8]) -> usize {
        let mut copied = 0;
        while copied < src.len() {
            let chunk = self.chunk_mut();
            let count = chunk.len().min(src.len() - copied);
            if count == 0 {
                break;
            }
            chunk[..count].copy_from_slice(&src[copied..copied + count]);
            // SAFETY: the first `count` bytes of the chunk were initialized by the copy above.
            unsafe {
                self.advance_mut(count);
            }
            copied += count;
        }
        copied
    }

    /// Publishes all initialized bytes and ends pending or active admission.
    ///
    /// Pending admission is cancelled. The initialized bytes of a partially filled envelope are
    /// kept without copying, and pooled memory that holds none of them is released.
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

impl PartBuffer {
    /// Returns the next uninitialized run inside the admitted envelope.
    ///
    /// The run is empty when [`Self::writable`] is zero. A nonempty run may be shorter than
    /// `writable()` when the envelope spans several pooled runs.
    pub(crate) fn chunk_mut(&mut self) -> &mut UninitSlice {
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

    /// Marks the first `count` bytes of [`Self::chunk_mut`] as written.
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds [`Self::writable`].
    ///
    /// # Safety
    ///
    /// The caller must have initialized the first `count` bytes of the run most recently returned
    /// by [`Self::chunk_mut`], with no intervening mutation of `self`.
    pub(crate) unsafe fn advance_mut(&mut self, count: usize) {
        assert!(
            count <= self.writable(),
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
    use crate::types::ByteSize;
    use std::task::Waker;

    use bytes::Buf;

    use super::*;
    use crate::config::MemoryBudgetConfig;

    const TEST_POOL_CAPACITY: usize = 1024 * 1024;

    fn test_pool() -> BufferPool {
        BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(ByteSize::new(
                TEST_POOL_CAPACITY as u64,
            )))
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
        let mut buffer = PartBuffer::new(&pool, carrier_size);

        assert!(poll_acquire(&mut buffer, carrier_size).is_ready());
        let ptr = buffer.chunk_mut().as_mut_ptr();
        assert_eq!(buffer.fill_from_slice(b"payload"), b"payload".len());
        assert_eq!(buffer.len(), 7);
        assert_eq!(buffer.remaining(), carrier_size - 7);

        let frozen = buffer.freeze();
        assert_eq!(frozen.chunk().as_ptr(), ptr.cast());
        assert_eq!(frozen.chunk(), b"payload");
        assert_eq!(pool.metrics().in_use_bytes(), carrier_size as u64);
        drop(frozen);
        assert_eq!(pool.metrics().in_use_bytes(), 0);
    }

    #[test]
    fn full_envelopes_roll_over_without_losing_carrier_tails() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(&pool, 7);

        assert!(poll_acquire(&mut buffer, 3).is_ready());
        assert_eq!(buffer.fill_from_slice(b"abc"), b"abc".len());
        assert_eq!(buffer.writable(), 0);

        assert!(poll_acquire(&mut buffer, 4).is_ready());
        assert_eq!(buffer.fill_from_slice(b"defg"), b"defg".len());
        assert_eq!(buffer.len(), 7);
        assert_eq!(pool.metrics().in_use_bytes(), pool.carrier_size() as u64);

        let frozen = buffer.freeze();
        let segments = frozen.into_segments();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], b"abcdefg"[..]);
        drop(segments);
        assert_eq!(pool.metrics().in_use_bytes(), 0);
    }

    #[test]
    fn envelope_continues_after_exhausting_a_reused_tail() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let mut buffer = PartBuffer::new(&pool, carrier_size + 3);
        let mut expected = Vec::with_capacity(carrier_size + 3);

        assert!(poll_acquire(&mut buffer, 3).is_ready());
        assert_eq!(buffer.fill_from_slice(b"abc"), b"abc".len());
        expected.extend_from_slice(b"abc");

        assert!(poll_acquire(&mut buffer, carrier_size).is_ready());
        let tail_len = buffer.writable();
        let tail = vec![b'x'; tail_len];
        assert_eq!(buffer.fill_from_slice(&tail), tail.len());
        expected.extend_from_slice(&tail);
        assert_eq!(buffer.writable(), 0);

        assert!(poll_acquire(&mut buffer, 1).is_ready());
        assert_eq!(buffer.writable(), 3);
        assert_eq!(buffer.fill_from_slice(b"end"), b"end".len());
        expected.extend_from_slice(b"end");

        assert_eq!(buffer.freeze().into_contiguous(), expected);
        assert_eq!(pool.metrics().in_use_bytes(), 0);
    }

    #[test]
    fn partial_envelope_remains_active_across_acquire_polls() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(&pool, 8);

        assert!(poll_acquire(&mut buffer, 8).is_ready());
        assert_eq!(buffer.fill_from_slice(b"abc"), b"abc".len());
        assert!(poll_acquire(&mut buffer, 2).is_ready());
        assert_eq!(buffer.writable(), 5);
        assert_eq!(buffer.fill_from_slice(b"defgh"), b"defgh".len());

        assert_eq!(buffer.freeze().into_contiguous(), b"abcdefgh"[..]);
    }

    #[test]
    fn pending_reservation_is_retained_and_cancelled_on_drop() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let holder = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let held = pool.acquire(&holder, TEST_POOL_CAPACITY).unwrap();
        let mut buffer = PartBuffer::new(&pool, carrier_size);

        assert!(poll_acquire(&mut buffer, carrier_size).is_pending());
        assert_eq!(pool.metrics().waiting_requests(), 1);
        drop(buffer);
        assert_eq!(pool.metrics().waiting_requests(), 0);

        drop(held);
        holder.close_acquisition();
    }

    #[test]
    fn pending_reservation_latches_its_first_envelope() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let holder = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let held = pool.acquire(&holder, TEST_POOL_CAPACITY).unwrap();
        let mut buffer = PartBuffer::new(&pool, carrier_size);

        assert!(poll_acquire(&mut buffer, 3).is_pending());
        assert!(poll_acquire(&mut buffer, 7).is_pending());

        drop(held);
        holder.close_acquisition();

        assert!(poll_acquire(&mut buffer, 7).is_ready());
        assert_eq!(buffer.writable(), 3);
    }

    #[test]
    fn invalid_envelope_does_not_enter_admission() {
        let pool = test_pool();
        let mut buffer = PartBuffer::new(&pool, 8);

        let Poll::Ready(result) = poll_acquire(&mut buffer, 9) else {
            panic!("invalid envelope entered admission");
        };
        let error = result.unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(pool.metrics().waits_total(), 0);
    }

    /// A source longer than the admitted envelope is copied only up to the envelope, and the
    /// count reports the copied prefix. The first envelope spans three carriers, so one call must
    /// copy across several writable runs; the second is served from the third carrier's tail.
    #[test]
    fn fill_from_slice_copies_up_to_the_envelope_and_returns_the_count() {
        let pool = test_pool();
        let carrier_size = pool.carrier_size();
        let first = 2 * carrier_size + 1;
        let max_len = first + 3;
        let mut buffer = PartBuffer::new(&pool, max_len);
        let source: Vec<u8> = (0..max_len + 5).map(|i| i as u8).collect();

        assert!(poll_acquire(&mut buffer, first).is_ready());
        assert_eq!(buffer.writable(), first);
        assert_eq!(buffer.fill_from_slice(&source), first);
        assert_eq!(buffer.len(), first);
        assert_eq!(buffer.writable(), 0);
        assert_eq!(buffer.fill_from_slice(&source[first..]), 0);
        assert_eq!(buffer.len(), first);

        assert!(poll_acquire(&mut buffer, 3).is_ready());
        assert_eq!(buffer.writable(), 3);
        assert_eq!(buffer.fill_from_slice(&source[first..]), 3);
        assert_eq!(buffer.remaining(), 0);
        assert_eq!(buffer.writable(), 0);

        assert_eq!(buffer.freeze().into_contiguous(), source[..max_len]);
        assert_eq!(pool.metrics().in_use_bytes(), 0);
    }

    /// `writable` counts only admitted bytes: none before the first acquisition or while it is
    /// pending, and none once the envelope is full, even when the part has room left.
    #[test]
    fn writable_is_zero_before_acquire_and_when_full() {
        let pool = test_pool();
        let holder = pool.try_reserve(TEST_POOL_CAPACITY).unwrap().unwrap();
        let mut buffer = PartBuffer::new(&pool, 8);

        assert_eq!(buffer.writable(), 0);
        assert_eq!(buffer.fill_from_slice(b"abc"), 0);
        assert!(buffer.is_empty());

        assert!(poll_acquire(&mut buffer, 4).is_pending());
        assert_eq!(buffer.writable(), 0);
        assert_eq!(buffer.fill_from_slice(b"abc"), 0);

        holder.close_acquisition();
        assert!(poll_acquire(&mut buffer, 4).is_ready());
        assert_eq!(buffer.writable(), 4);
        assert_eq!(buffer.fill_from_slice(b"abcd"), 4);
        assert_eq!(buffer.writable(), 0);
        assert_eq!(buffer.remaining(), 4);
        assert_eq!(buffer.fill_from_slice(b"e"), 0);

        assert_eq!(buffer.freeze().into_contiguous(), b"abcd"[..]);
    }

    #[test]
    fn part_buffer_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PartBuffer>();
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod prop_tests;
