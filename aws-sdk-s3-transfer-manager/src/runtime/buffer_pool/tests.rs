/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::Poll;
use std::thread;

use bytes::Buf;

use super::admission::WaitTicket;
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

fn notify_counter() -> (NotifyFn, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let notified = Arc::clone(&count);
    (
        Arc::new(move || {
            notified.fetch_add(1, Ordering::Release);
        }),
        count,
    )
}

fn pending_reservation(pool: &BufferPool, envelope: usize, notify: NotifyFn) -> WaitTicket {
    match pool.reserve(envelope, notify).unwrap() {
        Reserve::Ready(_) => panic!("reservation unexpectedly granted"),
        Reserve::Pending(ticket) => ticket,
    }
}

fn take_ready(ticket: &mut WaitTicket) -> Result<Reservation, ReserveError> {
    match ticket.take() {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("reservation is still pending"),
    }
}

#[test]
fn pooled_buffer_reports_fixed_capacity_and_initialized_length() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(2).unwrap();
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
fn reservation_close_converts_outstanding_ownership_to_debt() {
    let pool = BufferPool::new(4, 4);
    let reservation = pool.try_reserve(4).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(16)).unwrap();
    write_all(&mut buffer, b"abcdefghijklmnop");
    let mut output = buffer.freeze();

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().active_planned_demand, 0);
    assert_eq!(pool.snapshot().charged_live, 4);
    assert_eq!(pool.snapshot().unreserved_debt, 4);

    output.advance(4);
    assert_eq!(pool.snapshot().charged_live, 3);
    output.advance(8);
    assert_eq!(pool.snapshot().charged_live, 1);
    drop(output);
    assert_eq!(pool.snapshot().charged_live, 0);
}

#[test]
fn cloned_segment_keeps_only_its_carrier_charged() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(2).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");
    let mut output = buffer.freeze();
    let first = output.first_bytes().unwrap();
    reservation.close_acquisition();

    output.advance(8);
    assert_eq!(pool.snapshot().charged_live, 1);
    drop(first);
    assert_eq!(pool.snapshot().charged_live, 0);
}

#[test]
fn close_racing_final_return_retires_the_charge_once() {
    for _ in 0..32 {
        let pool = BufferPool::new(4, 1);
        let reservation = pool.try_reserve(1).unwrap();
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
    let reservation = pool.try_reserve(2).unwrap();
    let buffer = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    drop(buffer);

    assert_eq!(pool.snapshot().active_planned_demand, 2);
    assert_eq!(pool.snapshot().available_coverage, 2);
    assert_eq!(pool.snapshot().charged_live, 0);

    let retry = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    reservation.close_acquisition();
    assert_eq!(pool.snapshot().charged_live, 2);
    assert_eq!(pool.snapshot().unreserved_debt, 2);
    drop(retry);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn partial_physical_failure_rolls_back_direct_debits_and_carriers() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(2).unwrap();
    pool.fail_after_successes(1);

    assert!(matches!(
        pool.acquire(&reservation, AcquireRequest::new(8)),
        Err(AllocError::PhysicalAllocationFailed)
    ));
    assert_eq!(pool.snapshot().physical_live, 0);
    assert_eq!(pool.snapshot().active_planned_demand, 2);
    assert_eq!(pool.snapshot().available_coverage, 2);
    assert_eq!(pool.snapshot().charged_live, 0);

    let retry = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    reservation.close_acquisition();
    drop(retry);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn direct_preparation_failure_rolls_back_local_and_aggregate_debits() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(1).unwrap();
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    pool.fail_next_preparation();

    assert!(matches!(
        pool.acquire(&reservation, AcquireRequest::new(4)),
        Err(AllocError::PhysicalAllocationFailed)
    ));
    let failed = pool.snapshot();
    assert_eq!(failed.active_planned_demand, 1);
    assert_eq!(failed.available_coverage, 0);
    assert_eq!(failed.unreserved_debt, 0);
    assert_eq!(failed.charged_live, 1);

    let direct = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);

    reservation.close_acquisition();
    drop(direct);
    drop(unreserved);
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
    assert_eq!(snapshot.charged_live, 0);
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
    let reservation = first.try_reserve(1).unwrap();

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
fn unreserved_debt_prepares_replacement_before_consuming_retained_capacity() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(1).unwrap();
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().prepared, 1);
    assert_eq!(pool.snapshot().retained, 1);
    assert_eq!(pool.snapshot().overflow, 0);

    let reserved = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().overflow, 1);
    assert_eq!(pool.snapshot().unreserved_debt, 1);
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
    let reservation = pool.try_reserve(2).unwrap();
    let first = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let second = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 0);

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().available_coverage, 0);
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

    let reservation = pool.try_reserve(1).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);
    assert_eq!(pool.snapshot().available_coverage, 1);
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
    assert_eq!(pool.snapshot().charged_live, 1);
    drop(third);
    assert_eq!(pool.snapshot().charged_live, 0);
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
    let reservation = pool.try_reserve(2).unwrap();
    assert_eq!(pool.snapshot().active_planned_demand, 2);
    drop(reservation);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn segmented_bytes_clone_has_an_independent_cursor() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(2).unwrap();
    let mut buffer = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    write_all(&mut buffer, b"abcdefgh");
    let mut first = buffer.freeze();
    let second = first.clone();
    reservation.close_acquisition();

    first.advance(4);
    assert_eq!(first.chunk(), b"efgh");
    assert_eq!(second.chunk(), b"abcd");
    drop(first);
    assert_eq!(pool.snapshot().charged_live, 2);
    drop(second);
    assert_eq!(pool.snapshot().charged_live, 0);
}

#[test]
fn snapshots_distinguish_free_retained_memory_from_admission_usage() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(1).unwrap();
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

#[test]
fn fifo_transfers_grants_in_arrival_order() {
    let pool = BufferPool::new(4, 2);
    let holder = pool.try_reserve(2).unwrap();
    let (notify_first, first_count) = notify_counter();
    let (notify_second, second_count) = notify_counter();
    let mut first = pending_reservation(&pool, 2, notify_first);
    let mut second = pending_reservation(&pool, 1, notify_second);

    drop(holder);
    assert_eq!(first_count.load(Ordering::Acquire), 1);
    assert_eq!(second_count.load(Ordering::Acquire), 0);
    assert_eq!(pool.snapshot().active_planned_demand, 2);
    assert_eq!(pool.snapshot().waiters, 1);

    let first_reservation = take_ready(&mut first).unwrap();
    drop(first_reservation);
    assert_eq!(second_count.load(Ordering::Acquire), 1);
    assert_eq!(pool.snapshot().active_planned_demand, 1);
    assert_eq!(pool.snapshot().waiters, 0);

    drop(take_ready(&mut second).unwrap());
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn reserve_returns_an_immediate_prepared_grant() {
    let pool = BufferPool::new(4, 1);
    let (notify, notify_count) = notify_counter();

    let reservation = match pool.reserve(1, notify).unwrap() {
        Reserve::Ready(reservation) => reservation,
        Reserve::Pending(_) => panic!("reservation unexpectedly parked"),
    };

    assert_eq!(notify_count.load(Ordering::Acquire), 0);
    assert_eq!(pool.snapshot().prepared, 1);
    assert_eq!(pool.snapshot().active_planned_demand, 1);
    drop(reservation);
}

#[test]
fn fresh_reservation_does_not_bypass_fifo_head() {
    let pool = BufferPool::new(4, 2);
    let holder = pool.try_reserve(1).unwrap();
    let (notify, _) = notify_counter();
    let _front = pending_reservation(&pool, 2, notify);

    assert!(matches!(pool.try_reserve(1), Err(ReserveError::AtCapacity)));

    drop(holder);
}

#[test]
fn cancelling_fifo_head_grants_next_waiter_that_fits() {
    let pool = BufferPool::new(4, 4);
    let holder = pool.try_reserve(3).unwrap();
    let (notify_front, front_count) = notify_counter();
    let (notify_next, next_count) = notify_counter();
    let front = pending_reservation(&pool, 2, notify_front);
    let mut next = pending_reservation(&pool, 1, notify_next);

    drop(front);
    assert_eq!(front_count.load(Ordering::Acquire), 0);
    assert_eq!(next_count.load(Ordering::Acquire), 1);
    assert_eq!(pool.snapshot().waiters, 0);

    drop(take_ready(&mut next).unwrap());
    drop(holder);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn cancelling_middle_waiter_preserves_neighbor_order() {
    let pool = BufferPool::new(4, 1);
    let holder = pool.try_reserve(1).unwrap();
    let (notify_first, first_count) = notify_counter();
    let (notify_middle, middle_count) = notify_counter();
    let (notify_last, last_count) = notify_counter();
    let mut first = pending_reservation(&pool, 1, notify_first);
    let middle = pending_reservation(&pool, 1, notify_middle);
    let mut last = pending_reservation(&pool, 1, notify_last);

    drop(middle);
    drop(holder);
    assert_eq!(first_count.load(Ordering::Acquire), 1);
    assert_eq!(middle_count.load(Ordering::Acquire), 0);
    assert_eq!(last_count.load(Ordering::Acquire), 0);

    drop(take_ready(&mut first).unwrap());
    assert_eq!(last_count.load(Ordering::Acquire), 1);
    drop(take_ready(&mut last).unwrap());
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn oversized_fifo_head_waits_for_idle_then_precedes_smaller_waiter() {
    let pool = BufferPool::new(4, 1);
    let holder = pool.try_reserve(1).unwrap();
    let (notify_large, large_count) = notify_counter();
    let (notify_small, small_count) = notify_counter();
    let mut large = pending_reservation(&pool, 2, notify_large);
    let mut small = pending_reservation(&pool, 1, notify_small);

    assert_eq!(large_count.load(Ordering::Acquire), 0);
    drop(holder);

    assert_eq!(large_count.load(Ordering::Acquire), 1);
    assert_eq!(small_count.load(Ordering::Acquire), 0);
    assert_eq!(pool.snapshot().active_planned_demand, 2);
    assert_eq!(pool.snapshot().prepared, 2);

    drop(take_ready(&mut large).unwrap());
    assert_eq!(small_count.load(Ordering::Acquire), 1);
    drop(take_ready(&mut small).unwrap());
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn dropping_untaken_grant_returns_it_to_fifo() {
    let pool = BufferPool::new(4, 1);
    let holder = pool.try_reserve(1).unwrap();
    let (notify_first, first_count) = notify_counter();
    let (notify_second, second_count) = notify_counter();
    let first = pending_reservation(&pool, 1, notify_first);
    let mut second = pending_reservation(&pool, 1, notify_second);

    drop(holder);
    assert_eq!(first_count.load(Ordering::Acquire), 1);
    drop(first);
    assert_eq!(second_count.load(Ordering::Acquire), 1);

    drop(take_ready(&mut second).unwrap());
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn queued_preparation_failure_fails_head_and_grants_next() {
    let pool = BufferPool::new(4, 1);
    let holder = pool.try_reserve(1).unwrap();
    let (notify_front, front_count) = notify_counter();
    let (notify_next, next_count) = notify_counter();
    let mut front = pending_reservation(&pool, 2, notify_front);
    let mut next = pending_reservation(&pool, 1, notify_next);

    pool.fail_next_preparation();
    drop(holder);

    assert_eq!(front_count.load(Ordering::Acquire), 1);
    assert_eq!(next_count.load(Ordering::Acquire), 1);
    assert!(matches!(
        take_ready(&mut front),
        Err(ReserveError::PhysicalPreparationFailed)
    ));
    assert_eq!(pool.snapshot().active_planned_demand, 1);
    assert_eq!(pool.snapshot().waiters, 0);

    drop(take_ready(&mut next).unwrap());
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn immediate_preparation_failure_does_not_charge_admission() {
    let pool = BufferPool::new(4, 1);
    pool.fail_next_preparation();

    assert!(matches!(
        pool.try_reserve(1),
        Err(ReserveError::PhysicalPreparationFailed)
    ));
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.prepared, 0);
    assert_eq!(snapshot.admission_used, 0);
}

#[test]
fn admission_shutdown_fails_waiters_but_preserves_active_acquisition() {
    let pool = BufferPool::new(4, 1);
    let holder = pool.try_reserve(1).unwrap();
    let (notify, notify_count) = notify_counter();
    let mut waiter = pending_reservation(&pool, 1, notify);

    pool.close_admission();

    assert_eq!(notify_count.load(Ordering::Acquire), 1);
    assert!(matches!(take_ready(&mut waiter), Err(ReserveError::Closed)));
    assert!(matches!(pool.try_reserve(1), Err(ReserveError::Closed)));
    assert!(pool.snapshot().admission_closed);

    let buffer = pool.acquire(&holder, AcquireRequest::new(4)).unwrap();
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    drop(buffer);
    drop(unreserved);
    drop(holder);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn idle_escape_ignores_unreserved_debt_and_allows_one_plan() {
    let pool = BufferPool::new(4, 1);
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let reservation = pool.try_reserve(1).unwrap();

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.configured, 1);
    assert_eq!(snapshot.admission_used, 2);
    assert_eq!(snapshot.prepared, 2);
    assert_eq!(snapshot.unreserved_debt, 1);
    assert!(matches!(pool.try_reserve(1), Err(ReserveError::AtCapacity)));

    drop(reservation);
    drop(unreserved);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn closed_direct_owner_becomes_debt_and_allows_one_idle_escape() {
    let pool = BufferPool::new(4, 1);
    let reservation = pool.try_reserve(1).unwrap();
    let buffer = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();
    let (notify, notify_count) = notify_counter();
    let mut waiter = pending_reservation(&pool, 1, notify);

    reservation.close_acquisition();
    assert_eq!(pool.snapshot().unreserved_debt, 1);
    assert_eq!(notify_count.load(Ordering::Acquire), 1);
    let next = take_ready(&mut waiter).unwrap();
    assert_eq!(pool.snapshot().active_planned_demand, 1);

    drop(buffer);
    assert_eq!(pool.snapshot().unreserved_debt, 0);
    drop(next);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn any_carrier_return_repaying_debt_drains_fifo() {
    let pool = BufferPool::new(4, 2);
    let holder = pool.try_reserve(1).unwrap();
    let first = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let second = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    assert_eq!(pool.snapshot().unreserved_debt, 1);

    let (notify, notify_count) = notify_counter();
    let mut waiter = pending_reservation(&pool, 1, notify);
    drop(first);
    assert_eq!(notify_count.load(Ordering::Acquire), 1);
    assert_eq!(pool.snapshot().unreserved_debt, 0);
    let next = take_ready(&mut waiter).unwrap();

    drop(second);
    drop(next);
    drop(holder);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn one_envelope_is_fungible_across_both_acquisition_paths() {
    let pool = BufferPool::new(4, 4);
    let reservation = pool.try_reserve(4).unwrap();

    let first_direct = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();
    let unreserved = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let second_direct = pool.acquire(&reservation, AcquireRequest::new(8)).unwrap();

    let acquired = pool.snapshot();
    assert_eq!(acquired.active_planned_demand, 4);
    assert_eq!(acquired.available_coverage, 0);
    assert_eq!(acquired.unreserved_debt, 1);
    assert_eq!(acquired.charged_live, 5);

    reservation.close_acquisition();
    let closed = pool.snapshot();
    assert_eq!(closed.active_planned_demand, 0);
    assert_eq!(closed.unreserved_debt, 5);
    assert_eq!(closed.charged_live, 5);

    drop(unreserved);
    drop(first_direct);
    drop(second_direct);
    assert_eq!(pool.snapshot().admission_used, 0);
}

#[test]
fn direct_return_repays_debt_created_after_unreserved_displacement() {
    let pool = BufferPool::new(4, 2);
    let reservation = pool.try_reserve(2).unwrap();
    let first = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let second = pool.acquire_unreserved(AcquireRequest::new(4)).unwrap();
    let direct = pool.acquire(&reservation, AcquireRequest::new(4)).unwrap();

    assert_eq!(pool.snapshot().unreserved_debt, 1);
    drop(direct);
    assert_eq!(pool.snapshot().unreserved_debt, 0);
    assert_eq!(pool.snapshot().available_coverage, 0);

    reservation.close_acquisition();
    drop(first);
    drop(second);
    assert_eq!(pool.snapshot().admission_used, 0);
}
