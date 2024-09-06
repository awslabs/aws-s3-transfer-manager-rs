/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use futures_util::future;
use tower::retry::budget::{Budget, TpsBudget};

/// A `tower::retry::Policy` implementation for retrying requests
#[derive(Debug, Clone)]
pub(crate) struct RetryPolicy {
    budget: Arc<TpsBudget>,
    remaining_attempts: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            budget: Arc::new(TpsBudget::default()),
            remaining_attempts: 2,
        }
    }
}

impl<Req, Res, E> tower::retry::Policy<Req, Res, E> for RetryPolicy
where
    Req: Clone,
{
    type Future = future::Ready<()>;

    fn retry(&mut self, _req: &mut Req, result: &mut Result<Res, E>) -> Option<Self::Future> {
        match result {
            Ok(_) => {
                self.budget.deposit();
                None
            }
            Err(_) => {
                if self.remaining_attempts == 0 || !self.budget.withdraw() {
                    return None;
                }
                self.remaining_attempts -= 1;
                Some(future::ready(()))
            }
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(req.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use tokio_test::{assert_pending, assert_ready_err, assert_ready_ok, task};
    use tower_test::{assert_request_eq, mock};

    type Req = &'static str;
    type Resp = &'static str;
    type Mock = mock::Mock<Req, Resp>;
    type Handle = mock::Handle<Req, Resp>;

    fn new_service() -> (mock::Spawn<tower::retry::Retry<RetryPolicy, Mock>>, Handle) {
        let retry = tower::retry::RetryLayer::new(RetryPolicy::default());
        mock::spawn_layer(retry)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_retry_max_attempts() {
        let (mut service, mut handle) = new_service();
        assert_ready_ok!(service.poll_ready());
        let mut fut = task::spawn(service.call("hello"));

        assert_request_eq!(handle, "hello").send_error("retry 1");
        assert_pending!(fut.poll());

        assert_request_eq!(handle, "hello").send_error("retry 2");
        assert_pending!(fut.poll());

        assert_request_eq!(handle, "hello").send_error("retry 3");
        let err = assert_ready_err!(fut.poll());
        assert_eq!(err.to_string(), "retry 3");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_retry_state_independent() {
        let (mut service, mut handle) = new_service();
        assert_ready_ok!(service.poll_ready());
        let mut fut = task::spawn(service.call("hello"));

        assert_request_eq!(handle, "hello").send_error("retry 1");
        assert_pending!(fut.poll());

        assert_request_eq!(handle, "hello").send_response("response 1");
        assert_eq!(assert_ready_ok!(fut.poll()), "response 1");

        // second request should have independent max attempts
        assert_ready_ok!(service.poll_ready());
        let mut fut = task::spawn(service.call("hello 2"));
        assert_request_eq!(handle, "hello 2").send_error("retry 1");
        assert_pending!(fut.poll());

        assert_request_eq!(handle, "hello 2").send_error("retry 2");
        assert_pending!(fut.poll());

        assert_request_eq!(handle, "hello 2").send_error("retry 3");
        let err = assert_ready_err!(fut.poll());
        assert_eq!(err.to_string(), "retry 3");
    }
}
