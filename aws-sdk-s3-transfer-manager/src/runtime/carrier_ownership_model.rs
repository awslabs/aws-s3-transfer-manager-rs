/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Executable ownership model for pooled carrier storage.
//!
//! This is intentionally test-only. It isolates the unsafe ownership core
//! needed to publish immutable `Bytes` windows while retaining a disjoint
//! writable suffix.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;

/// Stable storage shared by disjoint range capabilities.
///
/// This type deliberately does not implement `AsRef<[u8]>`: immutable access
/// must be limited to an initialized `PooledWindow`.
struct CarrierBacking {
    data: Box<[UnsafeCell<MaybeUninit<u8>>]>,
    _return_token: ReturnToken,
}

/// Returns one physical charge when the final backing owner disappears.
struct ReturnToken {
    returned_units: Arc<AtomicUsize>,
    units: usize,
}

impl Drop for ReturnToken {
    fn drop(&mut self) {
        self.returned_units.fetch_add(self.units, Ordering::Relaxed);
    }
}

// Access is safe only through `ExclusiveRange` and `PooledWindow`.
// `ExclusiveRange` is linear and can split only into disjoint ranges.
unsafe impl Send for CarrierBacking {}
unsafe impl Sync for CarrierBacking {}

impl CarrierBacking {
    fn heap(capacity: usize, returned_units: Arc<AtomicUsize>) -> Arc<Self> {
        assert!(capacity > 0, "a carrier must have nonzero capacity");
        let data = std::iter::repeat_with(|| UnsafeCell::new(MaybeUninit::uninit()))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Arc::new(Self {
            data,
            _return_token: ReturnToken {
                returned_units,
                units: 1,
            },
        })
    }
}

/// Linear mutable authority over exactly one range of a carrier.
///
/// This type is not cloneable. Its private constructors and consuming
/// operations are the safety boundary for `CarrierBacking`.
struct ExclusiveRange {
    backing: Arc<CarrierBacking>,
    range: Range<usize>,
}

impl ExclusiveRange {
    fn whole(backing: Arc<CarrierBacking>) -> Self {
        let capacity = backing.data.len();
        Self {
            backing,
            range: 0..capacity,
        }
    }

    fn len(&self) -> usize {
        self.range.len()
    }

    fn as_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let ptr = unsafe {
            self.backing
                .data
                .as_ptr()
                .add(self.range.start)
                .cast::<MaybeUninit<u8>>()
                .cast_mut()
        };
        // SAFETY: this linear capability is the only mutable authority for its
        // range. Splitting consumes it before creating disjoint children.
        unsafe { std::slice::from_raw_parts_mut(ptr, self.range.len()) }
    }

    fn split_at(self, offset: usize) -> (Self, Self) {
        assert!(offset <= self.len(), "split exceeds exclusive range");
        let middle = self.range.start + offset;
        let left = Self {
            backing: Arc::clone(&self.backing),
            range: self.range.start..middle,
        };
        let right = Self {
            backing: self.backing,
            range: middle..self.range.end,
        };
        (left, right)
    }

    /// Convert exclusive initialized storage into an immutable window.
    ///
    /// # Safety
    ///
    /// Every byte in this range must have been initialized.
    unsafe fn freeze_initialized(self) -> PooledWindow {
        PooledWindow {
            backing: self.backing,
            range: self.range,
        }
    }
}

/// Range-limited immutable owner passed to `Bytes::from_owner`.
struct PooledWindow {
    backing: Arc<CarrierBacking>,
    range: Range<usize>,
}

impl AsRef<[u8]> for PooledWindow {
    fn as_ref(&self) -> &[u8] {
        let ptr = unsafe {
            self.backing
                .data
                .as_ptr()
                .add(self.range.start)
                .cast::<MaybeUninit<u8>>()
                .cast::<u8>()
        };
        // SAFETY: construction consumes exclusive authority for this range and
        // requires every byte to be initialized. Mutable ranges are disjoint.
        unsafe { std::slice::from_raw_parts(ptr, self.range.len()) }
    }
}

/// Writable carrier suffix with an initialized, unpublished prefix.
///
/// The represented state is:
///
/// ```text
/// [ initialized and publishable | writable spare capacity ]
/// ```
///
/// Publishing consumes the initialized prefix's mutable authority and advances
/// this tail to the remaining initialized bytes and spare capacity.
struct WritableTail {
    exclusive: Option<ExclusiveRange>,
    initialized_len: usize,
}

impl WritableTail {
    fn heap(capacity: usize, returns: Arc<AtomicUsize>) -> Self {
        Self {
            exclusive: Some(ExclusiveRange::whole(CarrierBacking::heap(
                capacity, returns,
            ))),
            initialized_len: 0,
        }
    }

    fn remaining_capacity(&self) -> usize {
        self.exclusive
            .as_ref()
            .expect("writable tail retains its exclusive range")
            .len()
    }

    fn initialized_len(&self) -> usize {
        self.initialized_len
    }

    fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let initialized_len = self.initialized_len;
        &mut self
            .exclusive
            .as_mut()
            .expect("writable tail retains its exclusive range")
            .as_uninit_mut()[initialized_len..]
    }

    /// Mark newly initialized bytes at the start of `spare_capacity_mut`.
    ///
    /// # Safety
    ///
    /// The caller must have initialized the first `count` bytes of the spare
    /// capacity returned immediately before this call.
    unsafe fn advance_initialized(&mut self, count: usize) {
        assert!(
            count <= self.remaining_capacity() - self.initialized_len,
            "initialized length exceeds writable capacity"
        );
        self.initialized_len += count;
    }

    fn publish_prefix(&mut self, count: usize) -> Bytes {
        assert!(count > 0, "cannot publish an empty window");
        assert!(
            count <= self.initialized_len,
            "cannot publish uninitialized bytes"
        );

        let exclusive = self
            .exclusive
            .take()
            .expect("writable tail retains its exclusive range");
        let (published, remainder) = exclusive.split_at(count);
        self.exclusive = Some(remainder);
        self.initialized_len -= count;

        // SAFETY: `count <= initialized_len` and initialization advances only
        // from the start of the exclusive range.
        Bytes::from_owner(unsafe { published.freeze_initialized() })
    }

    /// Publish every initialized byte and abandon unused spare capacity.
    fn seal_initialized(mut self) -> Option<Bytes> {
        if self.initialized_len == 0 {
            return None;
        }

        let exclusive = self
            .exclusive
            .take()
            .expect("writable tail retains its exclusive range");
        let (initialized, unused) = exclusive.split_at(self.initialized_len);
        drop(unused);

        // SAFETY: the split covers exactly the initialized prefix.
        Some(Bytes::from_owner(unsafe {
            initialized.freeze_initialized()
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    fn write(tail: &mut WritableTail, bytes: &[u8]) {
        let spare = tail.spare_capacity_mut();
        assert!(bytes.len() <= spare.len());
        for (slot, value) in spare.iter_mut().zip(bytes) {
            slot.write(*value);
        }
        // SAFETY: the loop initialized exactly `bytes.len()` leading slots.
        unsafe { tail.advance_initialized(bytes.len()) };
    }

    #[test]
    fn repeated_publication_preserves_initialized_remainder_and_writable_tail() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(8, Arc::clone(&returns));

        write(&mut tail, b"abcde");
        let first = tail.publish_prefix(3);
        assert_eq!(tail.initialized_len(), 2);
        assert_eq!(tail.remaining_capacity(), 5);

        write(&mut tail, b"fgh");
        let second = tail.publish_prefix(2);
        let third = tail.publish_prefix(3);

        assert_eq!(&first[..], b"abc");
        assert_eq!(&second[..], b"de");
        assert_eq!(&third[..], b"fgh");
        assert_eq!(tail.initialized_len(), 0);
        assert_eq!(tail.remaining_capacity(), 0);

        drop(tail);
        drop(first);
        drop(second);
        assert_eq!(returns.load(Ordering::Relaxed), 0);
        drop(third);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn short_windows_and_writable_tail_share_one_physical_return() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(64, Arc::clone(&returns));

        write(&mut tail, &[1; 5]);
        let first = tail.publish_prefix(5);
        write(&mut tail, &[2; 7]);
        let second = tail.publish_prefix(7);
        write(&mut tail, &[3; 9]);
        let third = tail.publish_prefix(9);

        drop(second);
        drop(tail);
        drop(first);
        assert_eq!(returns.load(Ordering::Relaxed), 0);
        drop(third);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn short_reads_append_without_consuming_additional_carriers() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(8, Arc::clone(&returns));

        write(&mut tail, b"a");
        write(&mut tail, b"bc");
        write(&mut tail, b"defgh");

        let body = tail.seal_initialized().unwrap();
        assert_eq!(&body[..], b"abcdefgh");
        drop(body);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn immutable_window_and_writable_suffix_can_cross_threads() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(8, Arc::clone(&returns));
        write(&mut tail, b"abc");
        let prefix = tail.publish_prefix(3);

        let suffix = std::thread::spawn(move || {
            write(&mut tail, b"defgh");
            tail.seal_initialized().unwrap()
        });

        assert_eq!(&prefix[..], b"abc");
        let suffix = suffix.join().unwrap();
        assert_eq!(&suffix[..], b"defgh");

        drop(prefix);
        assert_eq!(returns.load(Ordering::Relaxed), 0);
        drop(suffix);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bytes_clones_and_slices_delay_exactly_one_backing_return() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(6, Arc::clone(&returns));
        write(&mut tail, b"abcdef");

        let whole = tail.publish_prefix(6);
        let clone = whole.clone();
        let slice = whole.slice(1..5);
        drop(tail);
        drop(whole);
        drop(clone);
        assert_eq!(returns.load(Ordering::Relaxed), 0);
        assert_eq!(&slice[..], b"bcde");
        drop(slice);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dropping_unpublished_tail_returns_backing() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(8, Arc::clone(&returns));
        write(&mut tail, b"abc");

        drop(tail);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn dropping_empty_tail_models_transport_pending_without_pinning_storage() {
        let returns = Arc::new(AtomicUsize::new(0));
        let tail = WritableTail::heap(8, Arc::clone(&returns));

        drop(tail);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sealing_drops_unused_tail_but_published_bytes_keep_backing_live() {
        let returns = Arc::new(AtomicUsize::new(0));
        let mut tail = WritableTail::heap(8, Arc::clone(&returns));
        write(&mut tail, b"abc");

        let body = tail.seal_initialized().unwrap();
        assert_eq!(returns.load(Ordering::Relaxed), 0);
        assert_eq!(&body[..], b"abc");
        drop(body);
        assert_eq!(returns.load(Ordering::Relaxed), 1);
    }
}
