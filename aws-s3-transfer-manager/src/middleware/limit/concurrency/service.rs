/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::task::Poll;

use tower::Service;

use super::future::ResponseFuture;
use crate::error;
use crate::runtime::scheduler::{NetworkPermitContext, PermitType, Scheduler};

/// Enforces a limit on the concurrent requests an underlying service receives
/// using the given [`Scheduler`].
#[derive(Debug)]
pub(crate) struct ConcurrencyLimit<T> {
    inner: T,
    scheduler: Scheduler,
}

impl<T> ConcurrencyLimit<T> {
    /// Create a new concurrency limiter
    pub(crate) fn new(inner: T, scheduler: Scheduler) -> Self {
        ConcurrencyLimit { inner, scheduler }
    }
}

/// Provide the request/response payload size estimate
pub(crate) trait ProvideNetworkPermitContext {
    /// Payload size estimate in bytes
    fn network_permit_context(&self) -> NetworkPermitContext;
}

impl<S, Request> Service<Request> for ConcurrencyLimit<S>
where
    S: Service<Request> + Clone,
    S::Error: From<error::Error>,
    Request: ProvideNetworkPermitContext,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S, Request>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // We can't estimate payload size without the request so we
        // move scheduling/concurrency limiting to `call()`.
        // Also calling inner.poll_ready() would reserve a slot well in advance of us
        // actually consuming it potentially as well as invalidating it later due to cloning it.
        // Instead, signal readiness here and treat the service as a oneshot later.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // NOTE: We assume this is a dataplane request as that is the only place
        // we make use of tower is for upload/download. If this changes this logic needs updated.
        let ptype = PermitType::Network(req.network_permit_context());
        let permit_fut = self.scheduler.acquire_permit(ptype);
        ResponseFuture::new(self.inner.clone(), req, permit_fut, self.scheduler.clone())
    }
}

impl<T: Clone> Clone for ConcurrencyLimit<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            scheduler: self.scheduler.clone(),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::metrics::unit::ByteUnit;
    use crate::runtime::scheduler::{Scheduler, TransferDirection};
    use crate::types::BucketType;
    use crate::types::TargetThroughput;
    use crate::{middleware::limit::concurrency::ConcurrencyLimitLayer, types::ConcurrencyMode};
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use tokio_test::{assert_pending, assert_ready_ok, task};
    use tower_test::{assert_request_eq, mock};

    use super::ProvideNetworkPermitContext;

    #[derive(Debug)]
    struct TestInput(&'static str);

    impl ProvideNetworkPermitContext for TestInput {
        fn network_permit_context(&self) -> super::NetworkPermitContext {
            super::NetworkPermitContext {
                payload_size_estimate: self.0.len() as u64,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            }
        }
    }

    impl PartialEq<&'static str> for TestInput {
        fn eq(&self, other: &&'static str) -> bool {
            self.0 == *other
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_service_limit() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(2));
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut service, mut handle) = mock::spawn_layer(limit);

        assert_ready_ok!(service.poll_ready());
        let r1 = service.call(TestInput("req 1"));
        assert_ready_ok!(service.poll_ready());
        let r2 = service.call(TestInput("req 2"));

        assert_ready_ok!(service.poll_ready());
        let r3 = service.call(TestInput("req 3"));

        let mut t1 = task::spawn(r1);
        let mut t2 = task::spawn(r2);
        let mut t3 = task::spawn(r3);
        assert_pending!(t1.poll());
        assert_pending!(t2.poll());
        assert_pending!(t3.poll());

        // pass requests through
        assert_request_eq!(handle, "req 1").send_response("foo");
        assert_request_eq!(handle, "req 2").send_response("bar");

        assert_pending!(handle.poll_request());
        assert_eq!(t1.await.unwrap(), "foo");
        assert_eq!(t2.await.unwrap(), "bar");

        // NOTE: because poll_ready() is implemented in poll() for this middleware we have to
        // manually poll it here for it to pickup the permit and actually call the service
        // otherwise we'd block on send_response(). This is mismatch between tower-test and
        // how they expect a service to behave.
        assert_pending!(t3.poll());
        assert_request_eq!(handle, "req 3").send_response("baz");
        assert_eq!(t3.await.unwrap(), "baz");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_clone() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(1));
        let limit = ConcurrencyLimitLayer::new(scheduler);
        let (mut s1, mut handle) = mock::spawn_layer(limit);

        assert_ready_ok!(s1.poll_ready());

        // s2 should share underlying scheduler
        let mut s2 = s1.clone();
        assert_ready_ok!(s2.poll_ready());

        let r1 = s1.call(TestInput("req 1"));
        let r2 = s2.call(TestInput("req 2"));
        let mut t1 = task::spawn(r1);
        let mut t2 = task::spawn(r2);
        assert_pending!(t1.poll());
        assert_request_eq!(handle, "req 1").send_response("foo");

        // s2 can't get capacity until the future is dropped/consumed
        t1.await.unwrap();
        assert_pending!(t2.poll());
        assert_request_eq!(handle, "req 2").send_response("bar");
        assert_eq!(t2.await.unwrap(), "bar");
    }

    #[derive(Debug)]
    struct MockPayload(u64);

    impl ProvideNetworkPermitContext for MockPayload {
        fn network_permit_context(&self) -> super::NetworkPermitContext {
            super::NetworkPermitContext {
                payload_size_estimate: self.0,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_throughput_mode() {
        let _logs = show_test_logs();
        // sanity test throughput mode limits
        let scheduler = Scheduler::new(ConcurrencyMode::TargetThroughput(
            TargetThroughput::new_gigabits_per_sec(100),
        ));

        let limit = ConcurrencyLimitLayer::new(scheduler.clone());
        let (mut service, _handle) = mock::spawn_layer::<_, (), _>(limit);

        let part_size = 5 * ByteUnit::Mebibyte.as_bytes_u64();

        let mut tasks = Vec::new();

        for _ in 0..256 {
            let r = service.call(MockPayload(part_size));
            let mut t = task::spawn(r);
            assert_pending!(t.poll());
            tasks.push(t);
        }

        // NOTE: this is heavily dependent on the token bucket implementation and constants used.
        // This is a sanity test that we don't arbitrarily break expected concurrency controls in throughput
        // mode using an expected result for a given workload.
        let expected_inflight = 138;
        assert_eq!(expected_inflight, scheduler.metrics.inflight());

        for fut in tasks.into_iter() {
            let mut fut = fut;
            assert_pending!(fut.poll());
            drop(fut);
        }

        assert_eq!(0, scheduler.metrics.inflight());
    }
}
