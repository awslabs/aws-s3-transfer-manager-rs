/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Composed concurrency models for physical return and admission.
//!
//! These tests drive the pool API so carrier bitmap return, reservation close,
//! and FIFO grant execute through their shared production-shaped path.

use std::sync::Arc as StdArc;
use std::task::Poll;

use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
use crate::runtime::sync::sync::{Arc, Mutex};
use crate::runtime::sync::thread;

use super::admission::WaitTicket;
use super::buffer::SegmentedBytes;
use super::*;

fn pending(pool: &BufferPool, envelope: usize, notify: NotifyFn) -> WaitTicket {
    match pool.reserve(envelope, notify).unwrap() {
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
fn physical_return_precedes_fifo_reuse() {
    loom::model(|| {
        let pool = BufferPool::new(1, 2);
        let anchor = pool.try_reserve(1).unwrap();
        let holder = pool.try_reserve(1).unwrap();

        // Both prepared carriers are occupied. Closing holder converts its
        // ownership to debt, so its return is what makes the waiter eligible.
        let unreserved = pool.acquire_unreserved(AcquireRequest::new(1)).unwrap();
        let direct = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());
        let initial = pool.snapshot();
        assert_eq!(initial.prepared, 2);
        assert_eq!(initial.retained, 2);
        assert_eq!(initial.overflow, 0);
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

            // Correct final-return ordering makes the returned carrier reusable
            // before this grant can acquire. Accounting-first ordering grows a
            // third carrier.
            let buffer = callback_pool
                .acquire(&reservation, AcquireRequest::new(1))
                .unwrap();
            let prepared = callback_pool.inner.admission.lock().ledger.prepared;
            callback_prepared.store(prepared, Ordering::Release);

            reservation.close_acquisition();
            drop(buffer);
        });
        let ticket = pending(&pool, 1, notify);
        *ticket_slot.lock() = Some(ticket);

        let closing = thread::spawn(move || holder.close_acquisition());
        let returning = thread::spawn(move || drop(direct));

        closing.join().unwrap();
        returning.join().unwrap();

        assert_eq!(notifications.load(Ordering::Acquire), 1);
        assert_eq!(observed_prepared.load(Ordering::Acquire), 2);

        let after_grant = pool.snapshot();
        assert_eq!(after_grant.active_planned_demand, 1);
        assert_eq!(after_grant.charged_live, 1);
        assert_eq!(after_grant.unreserved_debt, 0);
        assert_eq!(after_grant.admission_used, 1);
        assert_eq!(after_grant.physical_live, 1);
        assert_eq!(after_grant.waiters, 0);

        drop(anchor);
        assert_eq!(pool.snapshot().unreserved_debt, 1);
        drop(unreserved);
        let final_state = pool.snapshot();
        assert_eq!(final_state.admission_used, 0);
        assert_eq!(final_state.physical_live, 0);
    });
}

#[test]
fn close_racing_two_final_returns_grants_once() {
    loom::model(|| {
        let pool = BufferPool::new(1, 3);
        let anchor = pool.try_reserve(1).unwrap();
        let holder = pool.try_reserve(2).unwrap();
        let unreserved = pool.acquire_unreserved(AcquireRequest::new(1)).unwrap();
        let first = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());
        let second = freeze_one(pool.acquire(&holder, AcquireRequest::new(1)).unwrap());

        let notifications = Arc::new(AtomicUsize::new(0));
        let callback_notifications = Arc::clone(&notifications);
        let notify: NotifyFn = StdArc::new(move || {
            callback_notifications.fetch_add(1, Ordering::AcqRel);
        });
        let mut ticket = pending(&pool, 2, notify);

        let closing = thread::spawn(move || holder.close_acquisition());
        let returning_first = thread::spawn(move || drop(first));
        let returning_second = thread::spawn(move || drop(second));
        closing.join().unwrap();
        returning_first.join().unwrap();
        returning_second.join().unwrap();

        assert_eq!(notifications.load(Ordering::Acquire), 1);
        let next = take_ready(&mut ticket);
        let granted = pool.snapshot();
        assert_eq!(granted.active_planned_demand, 3);
        assert_eq!(granted.charged_live, 1);
        assert_eq!(granted.physical_live, 1);
        assert_eq!(granted.waiters, 0);

        drop(next);
        drop(anchor);
        drop(unreserved);
        assert_eq!(pool.snapshot().admission_used, 0);
    });
}
