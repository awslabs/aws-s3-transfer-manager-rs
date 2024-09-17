/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::Throughput;
use std::{
    cmp,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};
use tower::Service;

use super::{
    future::ResponseFuture,
    state::{SharedState, State},
};

/// Enforces a limit on the number of concurrent requests the underlying service
/// can handle based on a configured target throughput objective and estimated in-flight
/// throughput using heuristics.
#[derive(Debug)]
pub(crate) struct EstimatedThroughputConcurrencyLimit<T> {
    /// estimated p50 request latency for the underlying service
    estimated_p50_latency_ms: usize,
    /// estimated per/request max throughput of the service
    estimated_max_request_throughput: Throughput,
    state: SharedState,
    inner: T,
}

impl<T> EstimatedThroughputConcurrencyLimit<T> {
    pub(crate) fn new(
        inner: T,
        estimated_p50_latency_ms: usize,
        estimated_max_request_throughput: Throughput,
        target_throughput: Throughput,
    ) -> Self {
        Self {
            estimated_p50_latency_ms,
            estimated_max_request_throughput,
            state: Arc::new(Mutex::new(State::new(target_throughput))),
            inner,
        }
    }
}

impl<T> Clone for EstimatedThroughputConcurrencyLimit<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            estimated_p50_latency_ms: self.estimated_p50_latency_ms,
            estimated_max_request_throughput: self.estimated_max_request_throughput,
            state: self.state.clone(),
            inner: self.inner.clone(),
        }
    }
}

/// Provide the request/response payload size
pub(crate) trait ProvidePayloadSize {
    /// Payload size in bytes
    fn payload_size(&self) -> u64;
}

impl<S, Request> Service<Request> for EstimatedThroughputConcurrencyLimit<S>
where
    S: Service<Request> + Clone,
    Request: ProvidePayloadSize,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S, Request>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // we can't estimate throughput without knowing the request size so
        // we move the polling of the service internally to the response future to enable
        // estimating the throughput once the request is known
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // estimate how much we are about to add to our in-flight throughput
        let estimated_req_throughput = estimated_throughput(
            req.payload_size(),
            self.estimated_p50_latency_ms,
            self.estimated_max_request_throughput,
        );
        ResponseFuture::new(
            self.inner.clone(),
            req,
            estimated_req_throughput,
            self.state.clone(),
        )
    }
}

fn estimated_throughput(
    payload_size: u64,
    estimated_p50_latency_ms: usize,
    estimated_max_request_throughput: Throughput,
) -> Throughput {
    let req_estimate = Throughput::new(
        payload_size,
        Duration::from_millis(estimated_p50_latency_ms as u64),
    );

    // take lower of the maximum per request estimate service is capable of or the estimate based on the payload
    cmp::min_by(estimated_max_request_throughput, req_estimate, |x, y| {
        x.partial_cmp(y).expect("valid order")
    })
}

#[cfg(test)]
mod tests {

    use super::estimated_throughput;
    use crate::metrics::{self, Throughput};

    const fn megabytes(x: u64) -> u64 {
        metrics::unit::Bytes::Megabyte.as_bytes_u64() * x
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
}
