/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    cmp,
    collections::VecDeque,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

use crate::metrics::{self, Throughput};
use futures_util::ready;
use tower::{Layer, Service};

/// Enforces a limit on the number of concurrent requests the underlying service
/// can handle based on a configured target throughput objective and estimated in-flight
/// throughput using heuristics.
#[derive(Debug)]
pub(crate) struct EstimatedThroughputConcurrencyLimit<T> {
    /// estimated p50 request latency for the underlying service
    estimated_p50_latency_ms: usize,
    /// estimated per/request max throughput of the service
    estimated_max_request_throughput: metrics::Throughput,
    state: Arc<Mutex<State>>,
    inner: T,
}

/// Estimated P50 latency for S3
const S3_P50_REQUEST_LATENCY_MS: usize = 30;

/// Estimated per/request max throughput S3 is capable of
const S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC: u64 = 90;

impl<T> EstimatedThroughputConcurrencyLimit<T> {
    pub(crate) fn new(
        inner: T,
        estimated_p50_latency_ms: usize,
        estimated_max_request_throughput: metrics::Throughput,
        target_throughput: metrics::Throughput,
    ) -> Self {
        Self {
            estimated_p50_latency_ms,
            estimated_max_request_throughput,
            state: Arc::new(Mutex::new(State::new(target_throughput))),
            inner,
        }
    }

    /// Create a new estimated throughput limiter using S3 service defaults for
    /// estimated p50 latency and estimated max request throughput
    pub(crate) fn s3_defaults(inner: T, target_throughput: metrics::Throughput) -> Self {
        Self::new(
            inner,
            S3_P50_REQUEST_LATENCY_MS,
            Throughput::new_bytes_per_sec(S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC),
            target_throughput,
        )
    }
}

impl<T> Clone for EstimatedThroughputConcurrencyLimit<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            estimated_p50_latency_ms: self.estimated_p50_latency_ms.clone(),
            estimated_max_request_throughput: self.estimated_max_request_throughput.clone(),
            state: self.state.clone(),
            inner: self.inner.clone(),
        }
    }
}

// TODO - high water mark?

#[derive(Debug)]
struct State {
    estimated_in_flight_bps: f64,
    target_throughput: metrics::Throughput,
    in_flight: usize,
    waiters: VecDeque<std::task::Waker>,
}

impl State {
    fn new(target_throughput: metrics::Throughput) -> Self {
        Self {
            estimated_in_flight_bps: 0.0,
            target_throughput,
            in_flight: 0,
            waiters: VecDeque::new(),
        }
    }

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let in_flight_estimate = self.estimated_in_flight();
        if in_flight_estimate < self.target_throughput {
            Poll::Ready(())
        } else {
            tracing::trace!(
                "estimated in-flight throughput {} exceeds target, throttling ({} current in-flight requests)",
                in_flight_estimate,
                self.in_flight,
            );
            self.waiters.push_back(cx.waker().clone());
            Poll::Pending
        }
    }

    /// Get an estimated throughput currently in-flight
    fn estimated_in_flight(&self) -> metrics::Throughput {
        let estimated = self.estimated_in_flight_bps.round() as u64;
        Throughput::new_bytes_per_sec(estimated)
    }

    /// Attempt to wake up any pending requests if we have capacity
    fn try_notify_one(&mut self) {
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

    fn start_request(&mut self, throughput: metrics::Throughput) {
        self.estimated_in_flight_bps += throughput.as_bytes_per_sec();
        self.in_flight += 1;
        // If we still have capacity, keep trying to make progress.
        // This prevents lots of small pending requests from making progress when a large
        // request (throughput) is done.
        self.try_notify_one();
    }

    fn end_request(&mut self, throughput: metrics::Throughput) {
        debug_assert!(self.in_flight > 0, "unbalanced call to start/end request");

        self.estimated_in_flight_bps =
            (self.estimated_in_flight_bps - throughput.as_bytes_per_sec()).max(0.0);
        self.in_flight -= 1;
    }
}

/// Provide the request/response payload size
pub(crate) trait ProvidePayloadSize {
    /// Payload size in bytes
    fn payload_size(&self) -> u64;
}

impl<S, Request> Service<Request> for EstimatedThroughputConcurrencyLimit<S>
where
    S: Service<Request>,
    Request: ProvidePayloadSize,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // have we reached our target in-flight estimate?
        //   * Yes -> Throttle
        //   * No -> Allow the request through
        //       * TODO - cap on small requests we allow through?
        ready!(self.state.lock().unwrap().poll_ready(cx));
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // estimate how much we are about to add to our in-flight throughput
        let delta = estimated_throughput(
            req.payload_size(),
            self.estimated_p50_latency_ms,
            self.estimated_max_request_throughput,
        );

        self.state.lock().unwrap().start_request(delta);

        let result = self.inner.call(req);

        {
            let mut state = self.state.lock().unwrap();
            state.end_request(delta);
            state.try_notify_one();
        }

        result
    }
}

fn estimated_throughput(
    payload_size: u64,
    estimated_p50_latency_ms: usize,
    estimated_max_request_throughput: Throughput,
) -> metrics::Throughput {
    let req_estimate = Throughput::new(
        payload_size,
        Duration::from_millis(estimated_p50_latency_ms as u64),
    );

    // take lower of the maximum per request estimate service is capable of or the estimate based on the payload
    cmp::min_by(estimated_max_request_throughput, req_estimate, |x, y| {
        x.partial_cmp(y).expect("valid order")
    })
}

/// Enforces a limit on the number of concurrent requests the underlying service
/// can handle based on a configured target throughput objective and estimated in-flight
/// throughput using heuristics.
#[derive(Debug, Clone)]
pub(crate) struct EstimatedThroughputConcurrencyLimitLayer {
    estimated_p50_latency_ms: usize,
    estimated_max_request_throughput: metrics::Throughput,
    target_throughput: metrics::Throughput,
}

impl EstimatedThroughputConcurrencyLimitLayer {
    /// Create a new estimated throughput limit layer using S3 service defaults for
    /// estimated p50 latency and estimated max request throughput
    pub(crate) fn s3_defaults(target_throughput: metrics::Throughput) -> Self {
        Self {
            estimated_p50_latency_ms: S3_P50_REQUEST_LATENCY_MS,
            estimated_max_request_throughput: Throughput::new_bytes_per_sec(
                S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC,
            ),
            target_throughput,
        }
    }
}

impl<S> Layer<S> for EstimatedThroughputConcurrencyLimitLayer {
    type Service = EstimatedThroughputConcurrencyLimit<S>;
    fn layer(&self, service: S) -> Self::Service {
        EstimatedThroughputConcurrencyLimit::new(
            service,
            self.estimated_p50_latency_ms,
            self.estimated_max_request_throughput,
            self.target_throughput,
        )
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::{assert_pending, assert_ready};

    use crate::metrics::{self, Throughput};

    use super::{estimated_throughput, State};

    const fn megabytes(x: u64) -> u64 {
        metrics::unit::Bytes::Megabyte.as_bytes_u64() * x
    }

    const fn kilobytes(x: u64) -> u64 {
        metrics::unit::Bytes::Kilobyte.as_bytes_u64() * x
    }

    #[test]
    fn test_estimated_throughput() {
        let estimated_latency_ms = 1000;
        let estimated_max_request_throughput = Throughput::new_bytes_per_sec(megabytes(2));
        let expected = Throughput::new_bytes_per_sec(megabytes(1));
        assert_eq!(
            expected,
            estimated_throughput(
                megabytes(1),
                estimated_latency_ms,
                estimated_max_request_throughput
            )
        );
        assert_eq!(
            estimated_max_request_throughput,
            estimated_throughput(
                megabytes(3),
                estimated_latency_ms,
                estimated_max_request_throughput
            )
        );
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
        assert_ready!(state.poll_ready(&mut cx));

        // reached capacity, should start throttling
        state.start_request(r2);
        let (waker, wake_count) = futures_test::task::new_count_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert_pending!(state.poll_ready(&mut cx));
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

        // "queue"  up 4 small requests, in practice we don't know the size of the request at this
        // point but we will when `call()` invokes `start_request()`
        assert_pending!(state.poll_ready(&mut cx));
        assert_pending!(state.poll_ready(&mut cx));
        assert_pending!(state.poll_ready(&mut cx));
        assert_pending!(state.poll_ready(&mut cx));

        // free up capacity consumed by a large request
        state.end_request(large);

        // wake first small request, assume it gets started
        state.try_notify_one();
        assert_eq!(1, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx));
        state.start_request(small);

        // at this point we started a small request, which should attempt to wake additional tasks
        // if there was capacity still, this happens every time there is still capacity when a
        // request starts...
        assert_eq!(2, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx));
        state.start_request(small);

        assert_eq!(3, wake_count.get());
        assert_ready!(state.poll_ready(&mut cx));
        state.start_request(small);

        // last bit of capacity taken
        assert_eq!(4, wake_count.get());
        state.start_request(small);

        // should now be pending
        assert_pending!(state.poll_ready(&mut cx));
    }
}
