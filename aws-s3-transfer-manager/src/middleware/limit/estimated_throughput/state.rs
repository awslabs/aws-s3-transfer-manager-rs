/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::metrics::{self, Throughput};

pub(super) type SharedState = Arc<Mutex<State>>;

// TODO - high water mark?

#[derive(Debug)]
pub(super) struct State {
    estimated_in_flight_bps: f64,
    target_throughput: metrics::Throughput,
    in_flight: usize,
    waiters: VecDeque<std::task::Waker>,
}

impl State {
    pub(super) fn new(target_throughput: metrics::Throughput) -> Self {
        Self {
            estimated_in_flight_bps: 0.0,
            target_throughput,
            in_flight: 0,
            waiters: VecDeque::new(),
        }
    }

    pub(super) fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
        request_throughput: Throughput,
    ) -> std::task::Poll<()> {
        tracing::trace!(
            "poll_ready: request throughput estimate: {}",
            request_throughput
        );
        let in_flight_estimate = Throughput::new_bytes_per_sec(
            (request_throughput.as_bytes_per_sec() + self.estimated_in_flight_bps).round() as u64,
        );

        if in_flight_estimate <= self.target_throughput || self.in_flight == 0 {
            tracing::trace!(
                "estimated in-flight throughput {} less than target ({}), Ready! ({} current in-flight requests)",
                in_flight_estimate,
                self.target_throughput,
                self.in_flight,
            );
            Poll::Ready(())
        } else {
            tracing::trace!(
                "estimated in-flight throughput {} exceeds target ({}), throttling ({} current in-flight requests)",
                in_flight_estimate,
                self.target_throughput,
                self.in_flight,
            );
            self.waiters.push_back(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Get an estimated throughput currently in-flight
    pub(super) fn estimated_in_flight(&self) -> metrics::Throughput {
        let estimated = self.estimated_in_flight_bps.round() as u64;
        Throughput::new_bytes_per_sec(estimated)
    }

    /// Attempt to wake up any pending requests if we have capacity
    pub(super) fn try_notify_one(&mut self) {
        let in_flight_estimate = self.estimated_in_flight();
        if in_flight_estimate < self.target_throughput {
            if let Some(waker) = self.waiters.pop_front() {
                tracing::trace!(
                    "estimated in-flight throughput {} below target, waking pending request ({} current in-flight requests)",
                    in_flight_estimate,
                    self.in_flight,
                );
                waker.wake();
            }
        }
    }

    pub(super) fn start_request(&mut self, throughput: metrics::Throughput) {
        self.estimated_in_flight_bps += throughput.as_bytes_per_sec();
        self.in_flight += 1;
        // If we still have capacity, keep trying to make progress.
        // This prevents lots of small pending requests from making progress when a large
        // request (throughput) is done.
        self.try_notify_one();
    }

    pub(super) fn end_request(&mut self, throughput: metrics::Throughput) {
        debug_assert!(self.in_flight > 0, "unbalanced call to start/end request");

        self.estimated_in_flight_bps =
            (self.estimated_in_flight_bps - throughput.as_bytes_per_sec()).max(0.0);
        self.in_flight -= 1;
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::{assert_pending, assert_ready};

    use crate::metrics::{self, Throughput};

    use super::State;

    const fn megabytes(x: u64) -> u64 {
        metrics::unit::Bytes::Megabyte.as_bytes_u64() * x
    }

    const fn kilobytes(x: u64) -> u64 {
        metrics::unit::Bytes::Kilobyte.as_bytes_u64() * x
    }

    #[tokio::test]
    async fn test_state() {
        let target_throughput = Throughput::new_bytes_per_sec(megabytes(2));
        let mut state = State::new(target_throughput);

        // 1MB/sec in-flight
        let r1 = Throughput::new_bytes_per_sec(megabytes(1));
        state.start_request(r1);
        assert_eq!(state.estimated_in_flight(), r1);

        // 1.5MB/sec in-flight
        let r2 = Throughput::new_bytes_per_sec(megabytes(1) / 2);
        state.start_request(r2);

        // still have capacity...
        let mut cx = futures_test::task::panic_context();
        assert_ready!(state.poll_ready(&mut cx, r2));

        // reached capacity, should start throttling
        state.start_request(r2);
        let (waker, wake_count) = futures_test::task::new_count_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert_pending!(state.poll_ready(&mut cx, r2));
        assert_eq!(0, wake_count.get());

        // remove estimated in-flight to bring us under target
        state.end_request(r2);

        // should wake our pending task
        state.try_notify_one();
        assert_eq!(1, wake_count.get());
    }

    // test what happens when we have N small requests in a pending state but less than that
    // in-flight (ensure that we end up waking tasks even when we have unbalanced calls to
    // notify_one() due to different throughputs being started/stopped)
    #[tokio::test]
    async fn test_uneven_loads() {
        let target_throughput = Throughput::new_bytes_per_sec(megabytes(1));
        let mut state = State::new(target_throughput);

        // consume all our capacity
        let large = Throughput::new_bytes_per_sec(megabytes(1));
        state.start_request(large);

        let small = Throughput::new_bytes_per_sec(kilobytes(256));

        let (waker, wake_count) = futures_test::task::new_count_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        // "queue"  up 4 small requests
        assert_pending!(state.poll_ready(&mut cx, small));
        assert_pending!(state.poll_ready(&mut cx, small));
        assert_pending!(state.poll_ready(&mut cx, small));
        assert_pending!(state.poll_ready(&mut cx, small));

        // free up capacity consumed by a large request
        state.end_request(large);

        // wake first small request, assume it gets started
        state.try_notify_one();
        assert_eq!(1, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx, small));
        state.start_request(small);

        // at this point we started a small request, which should attempt to wake additional tasks
        // if there was capacity still, this happens every time there is still capacity when a
        // request starts...
        assert_eq!(2, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx, small));
        state.start_request(small);

        assert_eq!(3, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx, small));
        state.start_request(small);

        // last bit of capacity taken
        assert_eq!(4, wake_count.get());
        state.start_request(small);

        // should now be pending
        assert_pending!(state.poll_ready(&mut cx, small));
    }

    // FIXME - test request throughput exceeds capacity/target throughput
    // FIXME - allow at least one through
    // FIXME - probably want min # requests allowed in flight?
}
