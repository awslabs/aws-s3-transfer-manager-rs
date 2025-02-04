/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error;
use crate::runtime::scheduler::{AcquirePermitFuture, OwnedWorkPermit};

use futures_util::ready;
use pin_project_lite::pin_project;
use std::sync::atomic::{self, AtomicUsize};
use std::sync::Arc;
use std::{future::Future, task::Poll};
use tower::util::Oneshot;
use tower::Service;

pin_project! {
    #[derive(Debug)]
    pub(crate) struct ResponseFuture<S, Request>
        where S: Service<Request>
    {
        request: Option<Request>,
        svc: Option<S>,
        inflight: Arc<AtomicUsize>,
        #[pin]
        state: State<Oneshot<S, Request>>
    }
}

pin_project! {
    #[project = StateProj]
    #[derive(Debug)]
    enum State<F> {
        // Polling the future from [`Service::call`]
        Called {
            #[pin]
            fut: F,
            // retain until dropped when future completes
            _permit: OwnedWorkPermit,
        },
        // Polling the future from [`Scheduler::acquire_permit`]
        AcquiringPermit {
            #[pin]
            permit_fut: AcquirePermitFuture
        }
    }
}

impl<S, Request> ResponseFuture<S, Request>
where
    S: Service<Request>,
{
    pub(crate) fn new(
        inner: S,
        req: Request,
        permit_fut: AcquirePermitFuture,
        inflight: Arc<AtomicUsize>,
    ) -> ResponseFuture<S, Request> {
        ResponseFuture {
            request: Some(req),
            svc: Some(inner),
            inflight,
            state: State::AcquiringPermit { permit_fut },
        }
    }
}

impl<S, Request> Future for ResponseFuture<S, Request>
where
    S: Service<Request>,
    S::Error: From<error::Error>,
{
    type Output = Result<S::Response, S::Error>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            match this.state.as_mut().project() {
                StateProj::AcquiringPermit { permit_fut } => {
                    let res = ready!(permit_fut.poll(cx));
                    match res {
                        Ok(_permit) => {
                            let req = this.request.take().expect("request set");
                            let inflight = this.inflight.fetch_add(1, atomic::Ordering::SeqCst) + 1;
                            tracing::trace!("in-flight requests: {inflight}");
                            let svc = this.svc.take().expect("service set");
                            // NOTE: because the service was (1) never polled for readiness
                            // originally and (2) also cloned, we need to ensure it's ready now before calling it.
                            let fut = Oneshot::new(svc, req);
                            this.state.set(State::Called { fut, _permit });
                        }
                        Err(err) => return Poll::Ready(Err(err.into())),
                    }
                }
                StateProj::Called { fut, .. } => {
                    let result = ready!(fut.poll(cx));
                    let inflight = this.inflight.fetch_sub(1, atomic::Ordering::SeqCst);
                    tracing::trace!("in-flight requests: {inflight}");
                    return Poll::Ready(result);
                }
            }
        }
    }
}
