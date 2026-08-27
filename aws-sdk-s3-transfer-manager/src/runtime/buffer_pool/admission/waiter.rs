/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reservation future and FIFO result-slot ownership.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::runtime::sync::sync::{Arc, Mutex};

use super::super::{BufferPool, CarrierCount, PoolInner};
use super::{wake_all, Reservation, ReserveError};

/// One reservation request retained in FIFO order.
pub(in crate::runtime::buffer_pool) struct Waiter {
    /// Complete requested envelope.
    pub(in crate::runtime::buffer_pool) envelope: CarrierCount,
    /// Grant-versus-cancellation linearization point.
    pub(in crate::runtime::buffer_pool) slot: Arc<WaitSlot>,
}

/// Result slot shared by admission and one reservation future.
pub(in crate::runtime::buffer_pool) struct WaitSlot {
    /// Serializes grant, cancellation, and result consumption.
    pub(in crate::runtime::buffer_pool) state: Mutex<WaitState>,
}

/// Lifecycle of one queued reservation result.
pub(in crate::runtime::buffer_pool) enum WaitState {
    /// Admission owns the queue link and may transfer a terminal result.
    Queued { waker: Waker },
    /// Admission transferred one prepared reservation.
    Granted(Reservation),
    /// Admission transferred one terminal failure.
    Failed(ReserveError),
    /// The future cancelled or consumed the terminal result.
    Taken,
}

/// Result of the first reservation poll under admission serialization.
pub(in crate::runtime::buffer_pool) enum ReservationPoll {
    /// The request was granted without entering the FIFO.
    Ready(Reservation),
    /// The request was linked behind existing or ineligible work.
    Queued(Arc<WaitSlot>),
}

/// State owned by one reservation future.
enum ReserveFutureState {
    /// The request has not entered admission.
    New { pool: BufferPool, bytes: usize },
    /// The request owns one FIFO result slot.
    Queued {
        pool: BufferPool,
        slot: Arc<WaitSlot>,
    },
    /// The result was returned or the future was cancelled.
    Complete,
}

/// A cancellation-safe request for one reservation.
///
/// The first poll either returns an immediate result or enters the pool-wide
/// FIFO. Dropping a queued future cancels it. Dropping a granted but unobserved
/// future retires the transferred reservation. Invalid requests and physical
/// preparation failures resolve to [`ReserveError`].
#[must_use = "a reservation request does nothing unless polled or awaited"]
pub(crate) struct ReserveFuture {
    state: ReserveFutureState,
}

impl WaitSlot {
    /// Creates one queued result slot with its initial task waker.
    pub(in crate::runtime::buffer_pool) fn new(waker: Waker) -> Self {
        Self {
            state: Mutex::new(WaitState::Queued { waker }),
        }
    }
}

impl ReserveFuture {
    /// Creates a lazy reservation request.
    pub(in crate::runtime::buffer_pool) fn new(pool: BufferPool, bytes: usize) -> Self {
        Self {
            state: ReserveFutureState::New { pool, bytes },
        }
    }
}

impl Future for ReserveFuture {
    type Output = Result<Reservation, ReserveError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = std::mem::replace(&mut this.state, ReserveFutureState::Complete);

        match state {
            ReserveFutureState::New { pool, bytes } => {
                let envelope = match pool.reservation_envelope(bytes) {
                    Ok(envelope) => envelope,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                match PoolInner::reserve_or_enqueue(&pool.inner, envelope, context.waker().clone())
                {
                    Ok(ReservationPoll::Ready(reservation)) => Poll::Ready(Ok(reservation)),
                    Ok(ReservationPoll::Queued(slot)) => {
                        this.state = ReserveFutureState::Queued { pool, slot };
                        Poll::Pending
                    }
                    Err(error) => Poll::Ready(Err(error)),
                }
            }
            ReserveFutureState::Queued { pool, slot } => {
                let result = poll_slot(&slot, context.waker());
                if result.is_pending() {
                    this.state = ReserveFutureState::Queued { pool, slot };
                }
                result
            }
            ReserveFutureState::Complete => panic!("reservation future polled after completion"),
        }
    }
}

impl Drop for ReserveFuture {
    fn drop(&mut self) {
        let ReserveFutureState::Queued { pool, slot } =
            std::mem::replace(&mut self.state, ReserveFutureState::Complete)
        else {
            return;
        };

        let state = {
            let mut state = slot.state.lock();
            std::mem::replace(&mut *state, WaitState::Taken)
        };
        match state {
            WaitState::Queued { .. } => {
                let wakers = PoolInner::cancel_waiter(&pool.inner, &slot);
                wake_all(wakers);
            }
            WaitState::Granted(reservation) => drop(reservation),
            WaitState::Failed(_) | WaitState::Taken => {}
        }
    }
}

/// Polls one linked result slot and refreshes its registered waker.
fn poll_slot(
    slot: &Arc<WaitSlot>,
    current_waker: &Waker,
) -> Poll<Result<Reservation, ReserveError>> {
    let mut state = slot.state.lock();
    let previous = std::mem::replace(&mut *state, WaitState::Taken);
    match previous {
        WaitState::Queued { waker } => {
            let waker = if waker.will_wake(current_waker) {
                waker
            } else {
                current_waker.clone()
            };
            *state = WaitState::Queued { waker };
            Poll::Pending
        }
        WaitState::Granted(reservation) => Poll::Ready(Ok(reservation)),
        WaitState::Failed(error) => Poll::Ready(Err(error)),
        WaitState::Taken => panic!("reservation result was already consumed"),
    }
}

#[cfg(all(test, not(s3_tm_loom)))]
mod tests {
    use super::super::super::test_util::{counting_waker, poll_reserve, test_pool, wake_count};
    use super::super::super::virtual_memory::VirtualMemoryOperation;
    use super::*;

    fn assert_pending(future: &mut ReserveFuture, waker: &Waker) {
        assert!(poll_reserve(future, waker).is_pending());
    }

    fn take_ready(future: &mut ReserveFuture, waker: &Waker) -> Reservation {
        match poll_reserve(future, waker) {
            Poll::Ready(Ok(reservation)) => reservation,
            Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
            Poll::Pending => panic!("reservation remained pending"),
        }
    }

    #[test]
    fn test_reserve_future_is_lazy_until_first_poll() {
        let (pool, carrier_size) = test_pool(1, 1);

        let future = pool.reserve(carrier_size);

        let admission = pool.inner.admission.lock();
        assert_eq!(admission.ledger.prepared_capacity, CarrierCount::ZERO);
        assert_eq!(admission.ledger.active_planned_demand, CarrierCount::ZERO);
        assert!(admission.waiters.is_empty());
        assert_eq!(admission.parked_reservations_total, 0);
        drop(admission);
        drop(future);
    }

    #[test]
    fn test_first_poll_returns_an_immediate_prepared_grant() {
        let (pool, carrier_size) = test_pool(1, 1);
        let (waker, wake_state) = counting_waker();
        let mut future = pool.reserve(carrier_size);

        let reservation = take_ready(&mut future, &waker);

        let admission = pool.inner.admission.lock();
        assert_eq!(admission.ledger.prepared_capacity, CarrierCount::new(1));
        assert_eq!(admission.ledger.active_planned_demand, CarrierCount::new(1));
        assert!(admission.waiters.is_empty());
        assert_eq!(admission.parked_reservations_total, 0);
        assert_eq!(wake_count(&wake_state), 0);
        drop(admission);
        drop(reservation);
    }

    #[test]
    fn test_fifo_transfers_grants_in_arrival_order() {
        let (pool, carrier_size) = test_pool(2, 2);
        let holder = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("initial reservation");
        let (first_waker, first_wakes) = counting_waker();
        let (second_waker, second_wakes) = counting_waker();
        let mut first = pool.reserve(carrier_size * 2);
        let mut second = pool.reserve(carrier_size);
        assert_pending(&mut first, &first_waker);
        assert_pending(&mut second, &second_waker);

        assert!(pool.try_reserve(carrier_size).unwrap().is_none());
        assert_eq!(pool.inner.admission.lock().waiters.len(), 2);
        drop(holder);

        assert_eq!(wake_count(&first_wakes), 1);
        assert_eq!(wake_count(&second_wakes), 0);
        let first_reservation = take_ready(&mut first, &first_waker);
        assert_eq!(pool.inner.admission.lock().waiters.len(), 1);

        drop(first_reservation);
        assert_eq!(wake_count(&second_wakes), 1);
        let second_reservation = take_ready(&mut second, &second_waker);
        let admission = pool.inner.admission.lock();
        assert!(admission.waiters.is_empty());
        assert_eq!(admission.parked_reservations_total, 2);
        drop(admission);
        drop(second_reservation);
    }

    #[test]
    fn test_oversized_fifo_head_waits_for_idle_and_blocks_later_work() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        let (large_waker, large_wakes) = counting_waker();
        let (small_waker, small_wakes) = counting_waker();
        let mut large = pool.reserve(carrier_size * 2);
        let mut small = pool.reserve(carrier_size);
        assert_pending(&mut large, &large_waker);
        assert_pending(&mut small, &small_waker);

        drop(holder);

        assert_eq!(wake_count(&large_wakes), 1);
        assert_eq!(wake_count(&small_wakes), 0);
        let large_reservation = take_ready(&mut large, &large_waker);
        assert_eq!(pool.inner.admission.lock().waiters.len(), 1);

        drop(large_reservation);
        assert_eq!(wake_count(&small_wakes), 1);
        drop(take_ready(&mut small, &small_waker));
    }

    #[test]
    fn test_cancelling_fifo_head_grants_next_eligible_waiter() {
        let (pool, carrier_size) = test_pool(4, 4);
        let holder = pool
            .try_reserve(carrier_size * 3)
            .unwrap()
            .expect("initial reservation");
        let (front_waker, front_wakes) = counting_waker();
        let (next_waker, next_wakes) = counting_waker();
        let mut front = pool.reserve(carrier_size * 2);
        let mut next = pool.reserve(carrier_size);
        assert_pending(&mut front, &front_waker);
        assert_pending(&mut next, &next_waker);

        drop(front);

        assert_eq!(wake_count(&front_wakes), 0);
        assert_eq!(wake_count(&next_wakes), 1);
        assert!(pool.inner.admission.lock().waiters.is_empty());
        drop(take_ready(&mut next, &next_waker));
        drop(holder);
    }

    #[test]
    fn test_cancelling_middle_waiter_preserves_neighbor_order() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        let (first_waker, first_wakes) = counting_waker();
        let (middle_waker, middle_wakes) = counting_waker();
        let (last_waker, last_wakes) = counting_waker();
        let mut first = pool.reserve(carrier_size);
        let mut middle = pool.reserve(carrier_size);
        let mut last = pool.reserve(carrier_size);
        assert_pending(&mut first, &first_waker);
        assert_pending(&mut middle, &middle_waker);
        assert_pending(&mut last, &last_waker);

        drop(middle);
        drop(holder);

        assert_eq!(wake_count(&first_wakes), 1);
        assert_eq!(wake_count(&middle_wakes), 0);
        assert_eq!(wake_count(&last_wakes), 0);
        let first_reservation = take_ready(&mut first, &first_waker);
        drop(first_reservation);
        assert_eq!(wake_count(&last_wakes), 1);
        drop(take_ready(&mut last, &last_waker));
    }

    #[test]
    fn test_repoll_replaces_waker_without_recounting_parked_request() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        let (first_waker, first_wakes) = counting_waker();
        let (replacement_waker, replacement_wakes) = counting_waker();
        let mut future = pool.reserve(carrier_size);

        assert_pending(&mut future, &first_waker);
        assert_pending(&mut future, &replacement_waker);
        assert_eq!(pool.inner.admission.lock().parked_reservations_total, 1);
        drop(holder);

        assert_eq!(wake_count(&first_wakes), 0);
        assert_eq!(wake_count(&replacement_wakes), 1);
        drop(take_ready(&mut future, &replacement_waker));
    }

    #[test]
    fn test_dropping_untaken_grant_reconsiders_fifo() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        let (first_waker, first_wakes) = counting_waker();
        let (second_waker, second_wakes) = counting_waker();
        let mut first = pool.reserve(carrier_size);
        let mut second = pool.reserve(carrier_size);
        assert_pending(&mut first, &first_waker);
        assert_pending(&mut second, &second_waker);
        drop(holder);
        assert_eq!(wake_count(&first_wakes), 1);

        drop(first);

        assert_eq!(wake_count(&second_wakes), 1);
        drop(take_ready(&mut second, &second_waker));
    }

    #[test]
    fn test_queued_preparation_failure_fails_head_and_grants_next() {
        let (pool, carrier_size) = test_pool(2, 2);
        let holder = pool
            .try_reserve(carrier_size * 2)
            .unwrap()
            .expect("initial reservation");
        let failed_slot = pool.inner.arena.reserve_slot().unwrap();
        failed_slot.inject_failure_once(VirtualMemoryOperation::Prepare);
        let (front_waker, front_wakes) = counting_waker();
        let (next_waker, next_wakes) = counting_waker();
        let mut front = pool.reserve(carrier_size * 3);
        let mut next = pool.reserve(carrier_size);
        assert_pending(&mut front, &front_waker);
        assert_pending(&mut next, &next_waker);

        drop(holder);

        assert_eq!(wake_count(&front_wakes), 1);
        assert_eq!(wake_count(&next_wakes), 1);
        assert!(matches!(
            poll_reserve(&mut front, &front_waker),
            Poll::Ready(Err(ReserveError::PhysicalPreparationFailed))
        ));
        let next_reservation = take_ready(&mut next, &next_waker);
        assert_eq!(
            pool.inner.admission.lock().ledger.active_planned_demand,
            CarrierCount::new(1)
        );
        drop(next_reservation);
    }

    #[test]
    fn test_invalid_request_fails_on_first_poll_without_parking() {
        let (pool, _) = test_pool(1, 1);
        let (waker, wake_state) = counting_waker();
        let mut future = pool.reserve(0);

        assert!(matches!(
            poll_reserve(&mut future, &waker),
            Poll::Ready(Err(ReserveError::InvalidSize))
        ));
        let admission = pool.inner.admission.lock();
        assert!(admission.waiters.is_empty());
        assert_eq!(admission.parked_reservations_total, 0);
        assert_eq!(wake_count(&wake_state), 0);
    }

    #[test]
    fn test_parked_counter_saturates_and_counts_one_fifo_entry_once() {
        let (pool, carrier_size) = test_pool(1, 1);
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        pool.inner.admission.lock().parked_reservations_total = u64::MAX;
        let (waker, _) = counting_waker();
        let mut future = pool.reserve(carrier_size);

        assert_pending(&mut future, &waker);
        assert_pending(&mut future, &waker);

        let admission = pool.inner.admission.lock();
        assert_eq!(admission.parked_reservations_total, u64::MAX);
        assert_eq!(admission.waiters.len(), 1);
        drop(admission);
        drop(future);
        drop(holder);
    }

    #[test]
    fn test_dropping_manager_owned_waiter_preserves_shared_pool() {
        let (pool, carrier_size) = test_pool(1, 1);
        let manager_pool = pool.clone();
        let holder = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("initial reservation");
        let (waker, _) = counting_waker();
        let mut future = manager_pool.reserve(carrier_size);
        assert_pending(&mut future, &waker);

        drop(manager_pool);
        drop(future);
        drop(holder);

        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("shared pool remains operational");
        drop(reservation);
    }

    #[test]
    fn test_reserve_future_is_send() {
        fn assert_send<T: Send>() {}

        assert_send::<ReserveFuture>();
    }
}

#[cfg(all(test, s3_tm_loom))]
mod loom_tests {
    use std::sync::Arc as StdArc;
    use std::task::Wake;

    use super::super::super::test_util::{
        counting_waker, poll_reserve, test_single_carrier_pool as test_pool,
    };
    use crate::runtime::sync::sync::atomic::{AtomicUsize, Ordering};
    use crate::runtime::sync::thread;

    use super::*;

    struct PollingWake {
        future: Arc<Mutex<Option<ReserveFuture>>>,
        count: Arc<AtomicUsize>,
    }

    impl PollingWake {
        fn poll_terminal(self: &StdArc<Self>) {
            let mut future = self
                .future
                .lock()
                .take()
                .expect("future installed before wake");
            let waker = Waker::from(StdArc::clone(self));
            match poll_reserve(&mut future, &waker) {
                Poll::Ready(Ok(reservation)) => drop(reservation),
                Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                Poll::Pending => panic!("waker ran before terminal result publication"),
            }
            self.count.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl Wake for PollingWake {
        fn wake(self: StdArc<Self>) {
            self.poll_terminal();
        }

        fn wake_by_ref(self: &StdArc<Self>) {
            self.poll_terminal();
        }
    }

    #[test]
    fn test_grant_racing_cancellation_retires_admission_once() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("initial reservation");
            let (waker, _) = counting_waker();
            let mut future = pool.reserve(carrier_size * 2);
            assert!(poll_reserve(&mut future, &waker).is_pending());

            let granting = thread::spawn(move || drop(holder));
            let cancelling = thread::spawn(move || drop(future));
            granting.join().unwrap();
            cancelling.join().unwrap();

            let admission = pool.inner.admission.lock();
            assert_eq!(admission.ledger.active_planned_demand, CarrierCount::ZERO);
            assert!(admission.waiters.is_empty());
        });
    }

    #[test]
    fn test_grant_racing_poll_has_one_terminal_result() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("initial reservation");
            let (waker, wake_count) = counting_waker();
            let mut future = pool.reserve(carrier_size);
            assert!(poll_reserve(&mut future, &waker).is_pending());

            let granting = thread::spawn(move || drop(holder));
            let first_poll = poll_reserve(&mut future, &waker);
            granting.join().unwrap();
            let reservation = match first_poll {
                Poll::Ready(Ok(reservation)) => reservation,
                Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                Poll::Pending => match poll_reserve(&mut future, &waker) {
                    Poll::Ready(Ok(reservation)) => reservation,
                    Poll::Ready(Err(error)) => panic!("reservation failed: {error}"),
                    Poll::Pending => panic!("reservation remained pending after close"),
                },
            };

            assert_eq!(wake_count.load(Ordering::Acquire), 1);
            drop(reservation);
            assert_eq!(
                pool.inner.admission.lock().ledger.active_planned_demand,
                CarrierCount::ZERO
            );
        });
    }

    #[test]
    fn test_terminal_waker_reenters_after_admission_unlock() {
        loom::model(|| {
            let (pool, carrier_size) = test_pool(1);
            let holder = pool
                .try_reserve(carrier_size)
                .unwrap()
                .expect("initial reservation");
            let wake_count = Arc::new(AtomicUsize::new(0));
            let future_slot = Arc::new(Mutex::new(None));
            let wake_state = StdArc::new(PollingWake {
                future: Arc::clone(&future_slot),
                count: Arc::clone(&wake_count),
            });
            let waker = Waker::from(StdArc::clone(&wake_state));
            let mut future = pool.reserve(carrier_size);
            assert!(poll_reserve(&mut future, &waker).is_pending());
            *future_slot.lock() = Some(future);

            drop(holder);

            assert_eq!(wake_count.load(Ordering::Acquire), 1);
            assert!(future_slot.lock().is_none());
            assert_eq!(
                pool.inner.admission.lock().ledger.active_planned_demand,
                CarrierCount::ZERO
            );
        });
    }
}
