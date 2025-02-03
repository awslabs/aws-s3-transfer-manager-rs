/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use tower::Service;

use super::future::ResponseFuture;
use crate::error;
use crate::runtime::scheduler::{PermitType, Scheduler};

/// Enforces a limit on the concurrent requests an underlying service receives
/// using the given [`Scheduler`].
#[derive(Debug)]
pub(crate) struct ConcurrencyLimit<T> {
    inner: T,
    scheduler: Scheduler,
    inflight: Arc<AtomicUsize>,
}

impl<T> ConcurrencyLimit<T> {
    /// Create a new concurrency limiter
    pub(crate) fn new(inner: T, scheduler: Scheduler) -> Self {
        ConcurrencyLimit {
            inner,
            scheduler,
            inflight: Arc::new(AtomicUsize::default()),
        }
    }
}

/// Provide the request/response payload size estimate
pub(crate) trait ProvidePayloadSize {
    /// Payload size in bytes
    fn payload_size(&self) -> u64;
}

impl<S, Request> Service<Request> for ConcurrencyLimit<S>
where
    S: Service<Request> + Clone,
    S::Error: From<error::Error>,
    Request: ProvidePayloadSize,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S, Request>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // Once we have a permit we still need to make sure the inner service is ready
        // We can't estimate payload size without the request so we
        // move scheduling/concurrency limiting to `call()`
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // NOTE: We assume this is a dataplane request as that is the only place
        // we make use of tower is for upload/download. If this changes this logic needs updated.
        let ptype = PermitType::DataPlane(req.payload_size());
        let permit_fut = self.scheduler.acquire_permit(ptype);
        ResponseFuture::new(self.inner.clone(), req, permit_fut, self.inflight.clone())
    }
}

impl<T: Clone> Clone for ConcurrencyLimit<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            scheduler: self.scheduler.clone(),
            inflight: self.inflight.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::runtime::scheduler::Scheduler;
    use crate::{middleware::limit::concurrency::ConcurrencyLimitLayer, types::ConcurrencyMode};
    use tokio_test::{assert_pending, assert_ready_ok, task};
    use tower_test::{assert_request_eq, mock};

    use super::ProvidePayloadSize;

    #[derive(Debug)]
    struct TestInput(&'static str);

    impl ProvidePayloadSize for TestInput {
        fn payload_size(&self) -> u64 {
            self.0.len() as u64
        }
    }

    impl PartialEq<&'static str> for TestInput {
        fn eq(&self, other: &&'static str) -> bool {
            self.0 == *other
        }
    }

    // #[tokio::test(flavor = "current_thread")]
    // async fn test_service_limit() {
    //     let scheduler = Scheduler::new(ConcurrencyMode::Explicit(2));
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut service, mut handle) = mock::spawn_layer(limit);
    //
    //     // println!("first call");
    //     // assert_ready_ok!(service.poll_ready());
    //     // let r1 = service.call(TestInput("req 1"));
    //     //
    //     // println!("second call");
    //     // assert_ready_ok!(service.poll_ready());
    //     // let r2 = service.call(TestInput("req 2"));
    //     //
    //     // println!("third call");
    //     // assert_ready_ok!(service.poll_ready());
    //     // let r3 = service.call(TestInput("req 3"));
    //
    //     // assert_pending!(service.poll_ready());
    //     // assert!(!service.is_woken());
    //     let mut t1 = task::spawn(async {
    //         // assert_ready_ok!(service.poll_ready());
    //         let c = service.call(TestInput("req 1"));
    //         c.await.unwrap();
    //     });
    //
    //
    //     // let mut t1 = task::spawn(r1);
    //     // let mut t2 = task::spawn(r2);
    //     // let mut t3 = task::spawn(r3);
    //     println!("poll t1");
    //     assert_pending!(t1.poll());
    //
    //
    //     println!("pass requests through");
    //     //
    //     // // pass requests through
    //     assert_request_eq!(handle, "req 1").send_response("foo");
    //     // assert_request_eq!(handle, "req 2").send_response("bar");
    //     //
    //     println!("no more requests");
    //     // // no more requests
    //     // assert_pending!(handle.poll_request());
    //     // assert_eq!(r1.await.unwrap(), "foo");
    //     //
    //     // assert!(service.is_woken());
    //     //
    //     // println!("check no more requests through");
    //     // // more requests can make it through
    //     // assert_ready_ok!(service.poll_ready());
    //     // let r3 = service.call(TestInput("req 3"));
    //     //
    //     // // assert_pending!(service.poll_ready());
    //     //
    //     // println!("check r2");
    //     // assert_eq!(r2.await.unwrap(), "bar");
    //     //
    //     // println!("check r3");
    //     // assert_request_eq!(handle, "req 3").send_response("baz");
    //     // assert_eq!(r3.await.unwrap(), "baz");
    // }
    //
    // #[tokio::test(flavor = "current_thread")]
    // async fn test_clone() {
    //     let scheduler = Scheduler::new(1);
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut s1, mut handle) = mock::spawn_layer(limit);
    //
    //     assert_ready_ok!(s1.poll_ready());
    //
    //     // s2 should share underlying scheduler
    //     let mut s2 = s1.clone();
    //     assert_pending!(s2.poll_ready());
    //
    //     let r1 = s1.call("req 1");
    //     assert_request_eq!(handle, "req 1").send_response("foo");
    //
    //     // s2 can't get capacity until the future is dropped/consumed
    //     assert_pending!(s2.poll_ready());
    //     r1.await.unwrap();
    //     assert_ready_ok!(s2.poll_ready());
    // }
    //
    // #[tokio::test(flavor = "current_thread")]
    // async fn test_service_drop_frees_capacity() {
    //     let scheduler = Scheduler::new(1);
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut s1, mut _handle) = mock::spawn_layer::<(), (), _>(limit);
    //
    //     assert_ready_ok!(s1.poll_ready());
    //
    //     // s2 should share underlying scheduler
    //     let mut s2 = s1.clone();
    //     assert_pending!(s2.poll_ready());
    //
    //     drop(s1);
    //     assert!(s2.is_woken());
    //     assert_ready_ok!(s2.poll_ready());
    // }
    // #[tokio::test(flavor = "current_thread")]
    // async fn test_drop_resp_future_frees_capacity() {
    //     let scheduler = Scheduler::new(1);
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut s1, mut _handle) = mock::spawn_layer::<_, (), _>(limit);
    //     let mut s2 = s1.clone();
    //
    //     assert_ready_ok!(s1.poll_ready());
    //     let r1 = s1.call("req 1");
    //
    //     assert_pending!(s2.poll_ready());
    //     drop(r1);
    //     assert_ready_ok!(s2.poll_ready());
    // }
    //
    // #[tokio::test(flavor = "current_thread")]
    // async fn test_service_error_frees_capacity() {
    //     let scheduler = Scheduler::new(1);
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut s1, mut handle) = mock::spawn_layer::<_, (), _>(limit);
    //     let mut s2 = s1.clone();
    //
    //     // reserve capacity on s1
    //     assert_ready_ok!(s1.poll_ready());
    //     assert_pending!(s2.poll_ready());
    //
    //     let r1 = s1.call("req 1");
    //
    //     assert_request_eq!(handle, "req 1").send_error("blerg");
    //     r1.await.unwrap_err();
    //
    //     assert_ready_ok!(s2.poll_ready());
    // }
    //
    // #[tokio::test(flavor = "current_thread")]
    // async fn test_multiple_waiting() {
    //     let scheduler = Scheduler::new(1);
    //     let limit = ConcurrencyLimitLayer::new(scheduler);
    //     let (mut s1, mut _handle) = mock::spawn_layer::<(), (), _>(limit);
    //     let mut s2 = s1.clone();
    //     let mut s3 = s1.clone();
    //
    //     // reserve capacity on s1
    //     assert_ready_ok!(s1.poll_ready());
    //     assert_pending!(s2.poll_ready());
    //     assert_pending!(s3.poll_ready());
    //
    //     drop(s1);
    //
    //     assert!(s2.is_woken());
    //     assert!(!s3.is_woken());
    //
    //     drop(s2);
    //     assert!(s3.is_woken());
    // }
}
