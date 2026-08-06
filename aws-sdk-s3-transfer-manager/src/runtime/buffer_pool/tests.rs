/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::{Arc, Barrier};
use std::thread;

use bytes::Buf;

use super::*;

fn write_all(buffer: &mut PooledBufMut, mut bytes: &[u8]) {
    while !bytes.is_empty() {
        let writable = buffer.chunk_mut();
        let count = writable.len().min(bytes.len());
        for (slot, value) in writable.iter_mut().zip(&bytes[..count]) {
            slot.write(*value);
        }
        // SAFETY: the loop initialized exactly `count` leading bytes.
        unsafe { buffer.advance_mut(count) };
        bytes = &bytes[count..];
    }
}

#[test]
fn pooled_buffer_reports_fixed_capacity_and_initialized_length() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(5)).unwrap();

    assert_eq!(buffer.capacity(), 8);
    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.remaining_mut(), 8);

    write_all(&mut buffer, b"abcde");
    assert_eq!(buffer.capacity(), 8);
    assert_eq!(buffer.len(), 5);
    assert_eq!(buffer.remaining_mut(), 3);
}

#[test]
fn reservation_close_replaces_envelope_with_exact_outstanding_charges() {
    let pool = BufferPool::new(4, 4);
    let reservation = pool.try_reserve(ReservationPlan::new(4, 4)).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(16)).unwrap();
    write_all(&mut buffer, b"abcdefghijklmnop");
    let mut output = buffer.freeze();

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().active_planned_demand, 0);
    assert_eq!(pool.snapshot().retiring_direct_live, 4);

    output.advance(4);
    assert_eq!(pool.snapshot().retiring_direct_live, 3);
    output.advance(8);
    assert_eq!(pool.snapshot().retiring_direct_live, 1);
    drop(output);
    assert_eq!(pool.snapshot().retiring_direct_live, 0);
}

#[test]
fn cloned_segment_keeps_only_its_carrier_charged() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");
    let mut output = buffer.freeze();
    let first = output.first_bytes().unwrap();
    reservation.close_acquisition();

    output.advance(8);
    assert_eq!(pool.snapshot().retiring_direct_live, 1);
    drop(first);
    assert_eq!(pool.snapshot().retiring_direct_live, 0);
}

#[test]
fn close_racing_final_return_retires_the_charge_once() {
    for _ in 0..32 {
        let pool = BufferPool::new(4, 1);
        let reservation = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
        let mut buffer = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
        write_all(&mut buffer, b"data");
        let output = buffer.freeze();
        let barrier = Arc::new(Barrier::new(2));
        let return_barrier = Arc::clone(&barrier);

        let returning = thread::spawn(move || {
            return_barrier.wait();
            drop(output);
        });
        barrier.wait();
        reservation.close_acquisition();
        returning.join().unwrap();

        assert_eq!(pool.snapshot().admission_used, 0);
        assert_eq!(pool.snapshot().physical_live, 0);
    }
}

#[test]
fn direct_return_before_close_restores_authority_without_changing_admission() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    let buffer = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    drop(buffer);

    assert_eq!(pool.snapshot().active_planned_demand, 2);
    assert_eq!(pool.snapshot().retiring_direct_live, 0);

    let retry = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    reservation.close_acquisition();
    assert_eq!(pool.snapshot().retiring_direct_live, 2);
    drop(retry);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn partial_physical_failure_rolls_back_direct_debits_and_carriers() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    pool.fail_after_successes(1);

    assert!(matches!(
        pool.acquire(&reservation, AcquireRequest::new(8)),
        Err(AllocError::PhysicalAllocationFailed)
    ));
    assert_eq!(pool.snapshot().physical_live, 0);
    assert_eq!(pool.snapshot().active_planned_demand, 2);

    let retry = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    reservation.close_acquisition();
    drop(retry);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn partial_physical_failure_rolls_back_unreserved_charges_and_carriers() {
    let pool = BufferPool::new(4, 2);
    pool.fail_after_successes(1);

    assert!(matches!(
        pool.acquire_unreserved(AcquireRequest::new(8)),
        Err(AllocError::PhysicalAllocationFailed)
    ));
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.unreserved_live, 0);
    assert_eq!(snapshot.unreserved_debt, 0);
    assert_eq!(snapshot.physical_live, 0);

    let retry = pool.acquire_unreserved(AcquireRequest::new(8)).unwrap();
    drop(retry);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn reservation_cannot_acquire_from_another_pool() {
    let first = BufferPool::new(4, 1);
    let second = BufferPool::new(4, 1);
    let reservation = first.try_reserve(ReservationPlan::new(1, 1)).unwrap();

    assert!(matches!(
        second.acquire(&reservation, AcquireRequest::new(4)),
        Err(AllocError::ForeignReservation)
    ));
    assert_eq!(first.snapshot().physical_live, 0);
    assert_eq!(second.snapshot().physical_live, 0);

    reservation.close_acquisition();
    assert_eq!(first.snapshot().admission_used, 0);
}

#[test]
fn reserved_acquisition_grows_when_unreserved_ownership_uses_retained_capacity() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().retained, 1);

    let reserved = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().overflow, 1);
    assert_eq!(pool.snapshot().physical_live, 2);

    reservation.close_acquisition();
    drop(reserved);
    drop(unreserved);
    assert_eq!(pool.snapshot().admission_used, 0);
    assert_eq!(pool.snapshot().overflow, 1);
    assert_eq!(pool.trim_excess(), 1);
    assert_eq!(pool.snapshot().overflow, 0);
}

#[test]
fn closing_coverage_converts_live_unreserved_ownership_to_debt() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 0)).unwrap();
    let first = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let second = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 0);

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().unreserved_coverage, 0);
    assert_eq!(pool.snapshot().unreserved_debt, 2);

    drop(first);
    assert_eq!(pool.snapshot().unreserved_debt, 1);
    drop(second);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn new_reservation_does_not_absorb_existing_unreserved_debt() {
    let pool = BufferPool::new(4, 2);
    let old = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);

    let reservation = pool.try_reserve(ReservationPlan::new(1, 0)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);
    let future = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);

    reservation.close_acquisition();
    drop(old);
    drop(future);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn published_windows_and_writable_suffix_share_one_carrier_charge() {
    let pool = BufferPool::new(8, 1);
    let mut buffer = pool.acquire_unreserved(AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");
    let first = buffer.publish_prefix(3);
    let second = buffer.publish_prefix(2);
    let third = buffer.publish_prefix(3);

    drop(buffer);
    drop(second);
    drop(first);
    assert_eq!(pool.snapshot().unreserved_live, 1);
    drop(third);
    assert_eq!(pool.snapshot().unreserved_live, 0);
    assert_eq!(pool.snapshot().free_retained, 1);
}

#[test]
fn publication_cursor_follows_initialized_bytes_across_carriers() {
    let pool = BufferPool::new(4, 2);
    let mut buffer = pool.acquire_unreserved(AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");

    let first = buffer.publish_prefix(4);
    let second = buffer.publish_prefix(4);

    assert_eq!(first, b"abcd".as_slice());
    assert_eq!(second, b"efgh".as_slice());
    assert_eq!(buffer.len(), 0);
}

#[test]
fn dropping_reservation_closes_unused_acquisition_authority() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    assert_eq!(pool.snapshot().active_planned_demand, 2);
    drop(reservation);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn segmented_bytes_clone_has_an_independent_cursor() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");
    let mut first = buffer.freeze();
    let second = first.clone();
    reservation.close_acquisition();

    first.advance(4);
    assert_eq!(first.chunk(), b"efgh");
    assert_eq!(second.chunk(), b"abcd");
    drop(first);
    assert_eq!(pool.snapshot().retiring_direct_live, 2);
    drop(second);
    assert_eq!(pool.snapshot().retiring_direct_live, 0);
}

#[test]
fn snapshots_distinguish_free_retained_memory_from_admission_usage() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();
    let buffer = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    drop(buffer);

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.configured, 1);
    assert_eq!(snapshot.prepared, 1);
    assert_eq!(snapshot.active_planned_demand, 1);
    assert_eq!(snapshot.retained, 1);
    assert_eq!(snapshot.free_retained, 1);
    assert_eq!(snapshot.physical_live, 0);

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().admission_used, 0);
}
