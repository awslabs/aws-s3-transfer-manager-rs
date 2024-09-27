/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use futures_util::ready;
use pin_project_lite::pin_project;
use std::future::Future;
use std::mem;
use std::pin::pin;
use tower::Service;

use super::future::ResponseFuture;
use crate::runtime::scheduler::{AcquirePermitFuture, OwnedWorkPermit, Scheduler};

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

pin_project! {
    #[project = StateProj]
    #[derive(Debug)]
    enum State {
        /// Permit acquired and ready
        Ready{permit: OwnedWorkPermit },
        /// Waiting on a permit (or haven't attempted to acquire one)
        Pending {
            #[pin]
            future: Option<AcquirePermitFuture>
        }
    }
}

impl State {
    fn pending(future: Option<AcquirePermitFuture>) -> Self {
        Self::Pending { future }
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
                State::Ready { .. } => break,
                State::Pending {
                    future: Some(permit_fut),
                } => {
                    let permit_fut = pin!(permit_fut);
                    let permit =
                        ready!(permit_fut.poll(cx)).expect("permit acquisition never fails");
                    self.state = State::Ready { permit };
                }
                State::Pending { future: None } => {
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
        let state = mem::replace(&mut self.state, State::pending(None));
        let permit = match state {
            State::Ready { permit } => permit,
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
