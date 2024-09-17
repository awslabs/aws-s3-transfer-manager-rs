/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::state::SharedState;
use crate::metrics::Throughput;
use futures_util::ready;
use pin_project_lite::pin_project;
use std::future::Future;
use std::task::Poll;
use tower::Service;

pin_project! {
    /// Future for [`EstimatedThroughputConcurrencyLimit`] service.
    #[derive(Debug)]
    pub(crate) struct ResponseFuture<S, R>
        where S: Service<R>
    {
        inner: S,
        request: Option<R>,
        estimated_throughput: Throughput,
        state: SharedState,
        #[pin]
        future_state: FutState<S::Future>
    }
}

pin_project! {
    /// The state of the response future
    #[project = FutStateProj]
    #[derive(Debug)]
    enum FutState<F> {
        /// Polling the future from [`Service::call`]
        Called {
            #[pin]
            future: F
        },

        /// Waiting for capacity (estimated throughput to drop below target)
        Waiting,
    }
}

impl<S, R> ResponseFuture<S, R>
where
    S: Service<R>,
{
    pub(super) fn new(
        service: S,
        request: R,
        estimated_throughput: Throughput,
        state: SharedState,
    ) -> Self {
        Self {
            inner: service,
            request: Some(request),
            estimated_throughput,
            state,
            future_state: FutState::Waiting,
        }
    }
}

impl<S, Request> Future for ResponseFuture<S, Request>
where
    S: Service<Request>,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();

        loop {
            match this.future_state.as_mut().project() {
                // test if we can make progress
                FutStateProj::Waiting => {
                    // have we reached our target in-flight estimate?
                    //   * Yes -> Throttle
                    //   * No -> Allow the request through
                    //       * TODO - cap on small requests we allow through?
                    ready!(this
                        .state
                        .lock()
                        .unwrap()
                        .poll_ready(cx, *this.estimated_throughput));

                    // make sure the inner service is ready
                    ready!(this.inner.poll_ready(cx)?);

                    this.state
                        .lock()
                        .unwrap()
                        .start_request(*this.estimated_throughput);
                    let request = this.request.take().expect("request set");
                    let response_fut = this.inner.call(request);
                    this.future_state.set(FutState::Called {
                        future: response_fut,
                    });
                }
                FutStateProj::Called { future } => match future.poll(cx) {
                    Poll::Ready(result) => {
                        {
                            let mut state = this.state.lock().unwrap();
                            state.end_request(*this.estimated_throughput);
                            state.try_notify_one();
                        }

                        return Poll::Ready(result);
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio_test::{assert_pending, assert_ready_ok, task};
    use tower_test::{assert_request_eq, mock};

    use super::ResponseFuture;
    use crate::metrics::{self, Throughput};
    use crate::middleware::limit::estimated_throughput::state::State;

    // FIXME - commonize these and/or refactor metrics::unit to make this easier
    const fn megabytes(x: u64) -> u64 {
        metrics::unit::Bytes::Megabyte.as_bytes_u64() * x
    }

    type Req = &'static str;
    type Resp = &'static str;

    #[tokio::test(flavor = "current_thread")]
    async fn test_ready() {
        let target_throughput = Throughput::new_bytes_per_sec(megabytes(2));
        let state = Arc::new(Mutex::new(State::new(target_throughput)));

        let estimated_req_throughput = Throughput::new_bytes_per_sec(megabytes(1));
        let (service, mut handle) = mock::spawn::<Req, Resp>();

        let fut = ResponseFuture::new(
            service.into_inner(),
            "hello",
            estimated_req_throughput,
            state.clone(),
        );

        let mut fut = task::spawn(fut);

        // pending until we send a response from the service
        assert_pending!(fut.poll());
        assert_request_eq!(handle, "hello").send_response("world");

        let result = assert_ready_ok!(fut.poll());
        assert_eq!("world", result);
    }
}
