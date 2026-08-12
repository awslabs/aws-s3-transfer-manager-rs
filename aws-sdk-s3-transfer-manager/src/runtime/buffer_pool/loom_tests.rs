/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Composed concurrency models for physical return and admission.
//!
//! These tests drive the pool API so carrier bitmap return, reservation close,
//! FIFO grant, and trim execute through their shared production-shaped path.

use std::sync::Arc as StdArc;
use std::task::Poll;

use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};
use crate::runtime::sync::thread;

use super::admission::WaitTicket;
use super::buffer::SegmentedBytes;
use super::*;

fn pending(pool: &BufferPool, plan: ReservationPlan, notify: NotifyFn) -> WaitTicket {
    match pool.reserve(plan, notify).unwrap() {
        Reserve::Ready(reservation) => {
            drop(reservation);
            panic!("reservation unexpectedly granted");
        }
        Reserve::Pending(ticket) => ticket,
    }
}

fn take_ready(ticket: &mut WaitTicket) -> Reservation {
    match ticket.take() {
        Poll::Ready(Ok(reservation)) => reservation,
        Poll::Ready(Err(error)) => panic!("reservation failed: {error:?}"),
        Poll::Pending => panic!("reservation remained pending"),
    }
}

fn freeze_one(mut buffer: PooledBufMut) -> SegmentedBytes {
    buffer.chunk_mut()[0].write(1);
    // SAFETY: the byte above was initialized before advancing the cursor.
    unsafe { buffer.advance_mut(1) };
    buffer.freeze()
}

#[test]
fn physical_return_precedes_fifo_reuse_and_trim() {
    loom::model(|| {
        let pool = BufferPool::new(1, 1);
        let holder = pool.try_reserve(ReservationPlan::new(1, 1)).unwrap();

        // Debt prepares a second carrier, then consumes the retained carrier.
        // The direct acquisition is therefore forced onto overflow.
        let unreserved = pool.acquire_unreserved(AcquireRequest::new(1)).unwrap();
        let direct = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());
        let initial = pool.snapshot();
        assert_eq!(initial.prepared, 2);
        assert_eq!(initial.retained, 1);
        assert_eq!(initial.overflow, 1);
        assert_eq!(initial.physical_live, 2);

        let ticket_slot: Arc<Mutex<Option<WaitTicket>>> = Arc::new(Mutex::new(None));
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed_prepared = Arc::new(AtomicUsize::new(usize::MAX));

        let callback_pool = pool.clone();
        let callback_ticket = Arc::clone(&ticket_slot);
        let callback_notifications = Arc::clone(&notifications);
        let callback_prepared = Arc::clone(&observed_prepared);
        let notify: NotifyFn = StdArc::new(move || {
            callback_notifications.fetch_add(1, Ordering::AcqRel);
            let mut ticket = callback_ticket
                .lock()
                .take()
                .expect("ticket is installed before notification");
            let reservation = take_ready(&mut ticket);

            // The retained carrier remains occupied. Correct final-return
            // ordering makes the old overflow carrier reusable before this
            // grant can acquire; accounting-first ordering grows a third one.
            let buffer = callback_pool
                .acquire(&reservation, AcquireRequest::new(1))
                .unwrap();
            let prepared = callback_pool.inner.admission.lock().ledger.prepared;
            callback_prepared.store(prepared, Ordering::Release);

            reservation.close_acquisition();
            drop(buffer);
        });
        let ticket = pending(&pool, ReservationPlan::new(1, 1), notify);
        *ticket_slot.lock() = Some(ticket);

        let closing = thread::spawn(move || holder.close_acquisition());
        let returning = thread::spawn(move || drop(direct));
        let trim_pool = pool.clone();
        let trimming = thread::spawn(move || trim_pool.trim_excess());

        closing.join().unwrap();
        returning.join().unwrap();
        let raced_trim = trimming.join().unwrap();

        assert_eq!(notifications.load(Ordering::Acquire), 1);
        assert_eq!(observed_prepared.load(Ordering::Acquire), 2);

        let after_grant = pool.snapshot();
        assert_eq!(after_grant.active_planned_demand, 0);
        assert_eq!(after_grant.retiring_direct_live, 0);
        assert_eq!(after_grant.unreserved_live, 1);
        assert_eq!(after_grant.unreserved_debt, 1);
        assert_eq!(after_grant.admission_used, 1);
        assert_eq!(after_grant.physical_live, 1);
        assert_eq!(after_grant.waiters, 0);

        let final_trim = pool.trim_excess();
        assert_eq!(raced_trim + final_trim, 1);
        let after_trim = pool.snapshot();
        assert_eq!(after_trim.prepared, 1);
        assert_eq!(after_trim.overflow, 0);

        drop(unreserved);
        let final_state = pool.snapshot();
        assert_eq!(final_state.admission_used, 0);
        assert_eq!(final_state.physical_live, 0);
    });
}

#[test]
fn close_racing_two_final_returns_grants_once() {
    loom::model(|| {
        let pool = BufferPool::new(1, 2);
        let holder = pool.try_reserve(ReservationPlan::new(2, 2)).unwrap();
        let first = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());
        let second = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());

        let notifications = Arc::new(AtomicUsize::new(0));
        let callback_notifications = Arc::clone(&notifications);
        let notify: NotifyFn = StdArc::new(move || {
            callback_notifications.fetch_add(1, Ordering::AcqRel);
        });
        let mut ticket = pending(&pool, ReservationPlan::new(2, 2), notify);

        let closing = thread::spawn(move || holder.close_acquisition());
        let returning_first = thread::spawn(move || drop(first));
        let returning_second = thread::spawn(move || drop(second));
        closing.join().unwrap();
        returning_first.join().unwrap();
        returning_second.join().unwrap();

        assert_eq!(notifications.load(Ordering::Acquire), 1);
        let next = take_ready(&mut ticket);
        let granted = pool.snapshot();
        assert_eq!(granted.active_planned_demand, 2);
        assert_eq!(granted.retiring_direct_live, 0);
        assert_eq!(granted.physical_live, 0);
        assert_eq!(granted.waiters, 0);

        drop(next);
        assert_eq!(pool.snapshot().admission_used, 0);
    });
}
