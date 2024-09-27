/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use futures_util::ready;
use std::future::Future;
use std::mem;
use std::pin::pin;
use tower::Service;

use super::future::ResponseFuture;
use crate::runtime::scheduler::{AcquirePermitFuture, OwnedWorkPermit, Scheduler};

// TODO - thread scheduler through client and replace existing limit middleware(s)
// TODO - benchmark for regressions
// TODO - benchmark middleware compared to simple semaphore/tower concurrency limit

/// Enforces a limit on the concurrent requests an underlying service receives
/// using the given [`Scheduler`].
#[derive(Debug)]
pub(crate) struct ConcurrencyLimit<T> {
    inner: T,
    scheduler: Scheduler,
    state: State,
}

impl<T> ConcurrencyLimit<T> {
    /// Create a new concurrency limiter
    pub(crate) fn new(inner: T, scheduler: Scheduler) -> Self {
        ConcurrencyLimit {
            inner,
            scheduler,
            state: State::pending(None),
        }
    }
}

#[derive(Debug)]
enum State {
    /// Permit acquired and ready
    Ready(OwnedWorkPermit),
    /// Waiting on a permit (or haven't attempted to acquire one)
    Pending(Option<AcquirePermitFuture>),
}

impl State {
    fn pending(future: Option<AcquirePermitFuture>) -> Self {
        Self::Pending(future)
    }
}

impl<S, Request> Service<Request> for ConcurrencyLimit<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        loop {
            match &mut self.state {
                State::Ready(_) => break,
                State::Pending(Some(permit_fut)) => {
                    let permit_fut = pin!(permit_fut);
                    let permit =
                        ready!(permit_fut.poll(cx)).expect("permit acquisition never fails");
                    self.state = State::Ready(permit);
                }
                State::Pending(None) => {
                    // we loop to ensure we poll this at least once to ensure we are woken up when ready
                    let permit_fut = self.scheduler.acquire_permit();
                    self.state = State::pending(Some(permit_fut));
                }
            }
        }
        // Once we have a permit we still need to make sure the inner service is ready
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // extract our permit and reset the state
        let permit = match mem::replace(&mut self.state, State::pending(None)) {
            State::Ready(permit) => permit,
            _ => panic!("poll_ready must be called first!"),
        };

        let fut = self.inner.call(req);

        ResponseFuture::new(fut, permit)
    }
}

impl<T: Clone> Clone for ConcurrencyLimit<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            scheduler: self.scheduler.clone(),
            state: State::pending(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::middleware::limit::concurrency::ConcurrencyLimitLayer;
    use crate::runtime::scheduler::Scheduler;
    use tokio_test::{assert_pending, assert_ready_ok};
    use tower_test::{assert_request_eq, mock};

    #[tokio::test(flavor = "current_thread")]
    async fn test_service_limit() {
        let scheduler = Scheduler::new(2);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut service, mut handle) = mock::spawn_layer(limit);

        assert_ready_ok!(service.poll_ready());
        let r1 = service.call("req 1");

        assert_ready_ok!(service.poll_ready());
        let r2 = service.call("req 2");

        assert_pending!(service.poll_ready());

        assert!(!service.is_woken());

        // pass requests through
        assert_request_eq!(handle, "req 1").send_response("foo");
        assert_request_eq!(handle, "req 2").send_response("bar");

        // no more requests
        assert_pending!(handle.poll_request());
        assert_eq!(r1.await.unwrap(), "foo");

        assert!(service.is_woken());

        // more requests can make it through
        assert_ready_ok!(service.poll_ready());
        let r3 = service.call("req 3");

        assert_pending!(service.poll_ready());

        assert_eq!(r2.await.unwrap(), "bar");

        assert_request_eq!(handle, "req 3").send_response("baz");
        assert_eq!(r3.await.unwrap(), "baz");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clone() {
        let scheduler = Scheduler::new(1);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut handle) = mock::spawn_layer(limit);

        assert_ready_ok!(s1.poll_ready());

        // s2 should share underlying scheduler
        let mut s2 = s1.clone();
        assert_pending!(s2.poll_ready());

        let r1 = s1.call("req 1");
        assert_request_eq!(handle, "req 1").send_response("foo");

        // s2 can't get capacity until the future is dropped/consumed
        assert_pending!(s2.poll_ready());
        r1.await.unwrap();
        assert_ready_ok!(s2.poll_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_service_drop_frees_capacity() {
        let scheduler = Scheduler::new(1);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut _handle) = mock::spawn_layer::<(), (), _>(limit);

        assert_ready_ok!(s1.poll_ready());

        // s2 should share underlying scheduler
        let mut s2 = s1.clone();
        assert_pending!(s2.poll_ready());

        drop(s1);
        assert!(s2.is_woken());
        assert_ready_ok!(s2.poll_ready());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn test_drop_resp_future_frees_capacity() {
        let scheduler = Scheduler::new(1);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut _handle) = mock::spawn_layer::<_, (), _>(limit);
        let mut s2 = s1.clone();

        assert_ready_ok!(s1.poll_ready());
        let r1 = s1.call("req 1");

        assert_pending!(s2.poll_ready());
        drop(r1);
        assert_ready_ok!(s2.poll_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_service_error_frees_capacity() {
        let scheduler = Scheduler::new(1);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut handle) = mock::spawn_layer::<_, (), _>(limit);
        let mut s2 = s1.clone();

        // reserve capacity on s1
        assert_ready_ok!(s1.poll_ready());
        assert_pending!(s2.poll_ready());

        let r1 = s1.call("req 1");

        assert_request_eq!(handle, "req 1").send_error("blerg");
        r1.await.unwrap_err();

        assert_ready_ok!(s2.poll_ready());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_multiple_waiting() {
        let scheduler = Scheduler::new(1);
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut _handle) = mock::spawn_layer::<(), (), _>(limit);
        let mut s2 = s1.clone();
        let mut s3 = s1.clone();

        // reserve capacity on s1
        assert_ready_ok!(s1.poll_ready());
        assert_pending!(s2.poll_ready());
        assert_pending!(s3.poll_ready());

        drop(s1);

        assert!(s2.is_woken());
        assert!(!s3.is_woken());

        drop(s2);
        assert!(s3.is_woken());
    }
}
