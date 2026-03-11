/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Mock transfer implementations for testing scheduler behavior.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::transfer::{
    IoKind, IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome,
};

/// Trait for mock state machines that drive transfer behavior.
pub(crate) trait MockStateMachine: Send + Sync + std::fmt::Debug {
    fn poll_work(&self, id: TransferId) -> PollWork;
    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;
}

/// Mock transfer that wraps any [`MockStateMachine`].
#[derive(Clone)]
pub(crate) struct MockTransfer {
    id: TransferId,
    ctx: TransferContext,
    state_machine: Arc<dyn MockStateMachine>,
}

impl std::fmt::Debug for MockTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockTransfer")
            .field("id", &self.id)
            .field("state_machine", &self.state_machine)
            .finish()
    }
}

impl MockTransfer {
    pub(crate) fn new<S: MockStateMachine + 'static>(
        id: TransferId,
        state_machine: Arc<S>,
    ) -> Self {
        use crate::DEFAULT_CONCURRENCY;
        use std::sync::Arc;

        // Create a minimal handle for testing
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().client(s3_client).build();
        let handle = Arc::new(crate::client::Handle::with_config_and_scheduler(
            config,
            crate::scheduler::Scheduler::new(DEFAULT_CONCURRENCY),
        ));

        let (ctx, _completion_rx) = TransferContext::with_id(id, handle);

        Self {
            id,
            ctx,
            state_machine,
        }
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        self.state_machine.poll_work(self.id)
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let outcome = self.state_machine.execute(work).await;
        // Mirror real transfer behavior: Failed means the transfer transitions
        // itself to terminal state before returning.
        if matches!(outcome, WorkOutcome::Failed { .. }) {
            self.ctx.set_failed(crate::error::from_kind(
                crate::error::ErrorKind::RuntimeError,
            )("mock transfer failed"));
            self.ctx.signal_terminal();
        }
        outcome
    }
}

impl Transfer for MockTransfer {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        MockTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(MockTransfer::execute(self, work))
    }
}

/// Simple state machine that generates N Network work items.
#[derive(Debug)]
pub(crate) struct FixedWorkCount {
    total: u64,
    generated: AtomicU64,
    completed: AtomicU64,
}

impl FixedWorkCount {
    pub(crate) fn new(count: u64) -> Self {
        Self {
            total: count,
            generated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.completed.load(Ordering::SeqCst) >= self.total
    }

    pub(crate) fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }
}

impl MockStateMachine for FixedWorkCount {
    fn poll_work(&self, _id: TransferId) -> PollWork {
        let gen = self.generated.fetch_add(1, Ordering::SeqCst);
        if gen >= self.total {
            return PollWork::Done;
        }

        PollWork::Ready(IoRequest {
            kind: IoKind::Network,
            data: None,
        })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.completed.fetch_add(1, Ordering::SeqCst);
            WorkOutcome::Success {
                schedule_next: None,
                data: None,
                metrics: None,
            }
        })
    }
}

/// Wraps a state machine to add delay before each execution.
#[derive(Debug)]
pub(crate) struct WithDelay<S> {
    inner: S,
    delay: Duration,
}

impl<S: MockStateMachine> WithDelay<S> {
    pub(crate) fn new(inner: S, delay: Duration) -> Self {
        Self { inner, delay }
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: MockStateMachine> MockStateMachine for WithDelay<S> {
    fn poll_work(&self, id: TransferId) -> PollWork {
        self.inner.poll_work(id)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.execute(work).await
        })
    }
}

/// Wraps a state machine to override execute behavior with a custom function.
pub(crate) struct WithExecute<S> {
    inner: S,
    execute_fn: fn(&mut IoRequest) -> WorkOutcome,
}

impl<S: std::fmt::Debug> std::fmt::Debug for WithExecute<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithExecute")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<S> WithExecute<S> {
    pub(crate) fn new(inner: S, execute_fn: fn(&mut IoRequest) -> WorkOutcome) -> Self {
        Self { inner, execute_fn }
    }
}

impl<S> MockStateMachine for WithExecute<S>
where
    S: MockStateMachine,
{
    fn poll_work(&self, id: TransferId) -> PollWork {
        self.inner.poll_work(id)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        let outcome = (self.execute_fn)(work);
        Box::pin(async move { outcome })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id() -> TransferId {
        TransferId {
            id: 1,
            parent: None,
        }
    }

    #[tokio::test]
    async fn test_fixed_work_count() {
        let sm = FixedWorkCount::new(3);
        let id = test_id();

        for _ in 0..3 {
            assert!(matches!(sm.poll_work(id), PollWork::Ready(_)));
        }
        assert!(matches!(sm.poll_work(id), PollWork::Done));
    }

    #[tokio::test]
    async fn test_with_delay() {
        let sm = WithDelay::new(FixedWorkCount::new(1), Duration::from_millis(50));
        let id = test_id();

        let mut work = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };

        let start = std::time::Instant::now();
        sm.execute(&mut work).await;
        assert!(start.elapsed() >= Duration::from_millis(50));
    }
}
