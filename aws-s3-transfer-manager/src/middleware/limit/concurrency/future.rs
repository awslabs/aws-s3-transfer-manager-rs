/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error;
use crate::runtime::scheduler::{AcquirePermitFuture, OwnedWorkPermit};

use futures_util::ready;
use pin_project_lite::pin_project;
use std::{future::Future, task::Poll};
use tower::Service;

pin_project! {
    #[derive(Debug)]
    pub(crate) struct ResponseFuture<S, Request>
        where S: Service<Request>
    {
        request: Option<Request>,
        inner: S,
        #[pin]
        state: State<S::Future>
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
    ) -> ResponseFuture<S, Request> {
        ResponseFuture {
            request: Some(req),
            inner,
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
                            let fut = this.inner.call(req);
                            this.state.set(State::Called { fut, _permit });
                        }
                        Err(err) => return Poll::Ready(Err(err.into())),
                    }
                }
                StateProj::Called { fut, .. } => {
                    let result = ready!(fut.poll(cx));
                    return Poll::Ready(result);
                }
            }
        }
    }
}
