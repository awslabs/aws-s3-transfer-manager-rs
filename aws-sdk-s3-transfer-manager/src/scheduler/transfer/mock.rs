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

use crate::scheduler::context::TransferContext;
use crate::scheduler::{PollWork, Transfer, TransferId, WorkItem, WorkKind, WorkOutcome};

/// Create a TransferContext for testing.
pub(crate) fn test_context(id: TransferId) -> TransferContext {
    use crate::DEFAULT_CONCURRENCY;

    let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
    let config = crate::Config::builder().client(s3_client).build();
    let handle = Arc::new(crate::client::Handle {
        config,
        scheduler: crate::scheduler::Scheduler::with_controller(Arc::new(
            crate::scheduler::FixedConcurrency::new(DEFAULT_CONCURRENCY),
        )),
        legacy_scheduler: crate::runtime::scheduler::Scheduler::new(
            crate::types::ConcurrencyMode::Explicit(DEFAULT_CONCURRENCY),
        ),
    });

    let (ctx, _completion_rx) = TransferContext::with_id(id, handle);
    ctx
}

/// Blanket impl so `Arc<T>` can be used as `Box<dyn Transfer>` in tests
/// while retaining shared access to the inner mock.
impl<T: Transfer> Transfer for Arc<T> {
    fn ctx(&self) -> &TransferContext {
        (**self).ctx()
    }

    fn poll_work(&self) -> PollWork {
        (**self).poll_work()
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        (**self).execute(work)
    }
}

/// Simple transfer that generates N Network work items.
#[derive(Debug)]
pub(crate) struct FixedWorkCount {
    ctx: TransferContext,
    total: u64,
    generated: AtomicU64,
    completed: AtomicU64,
}

impl FixedWorkCount {
    pub(crate) fn new(id: TransferId, count: u64) -> Self {
        Self {
            ctx: test_context(id),
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

impl Transfer for FixedWorkCount {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        let gen = self.generated.fetch_add(1, Ordering::SeqCst);
        if gen >= self.total {
            return PollWork::Done;
        }

        PollWork::Ready(WorkItem {
            kind: WorkKind::Network,
            data: None,
        })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.completed.fetch_add(1, Ordering::SeqCst);
            WorkOutcome::Success(None)
        })
    }
}

/// Wraps a transfer to add delay before each execution.
#[derive(Debug)]
pub(crate) struct WithDelay<S> {
    inner: S,
    delay: Duration,
}

impl<S: Transfer> WithDelay<S> {
    pub(crate) fn new(inner: S, delay: Duration) -> Self {
        Self { inner, delay }
    }

    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: Transfer> Transfer for WithDelay<S> {
    fn ctx(&self) -> &TransferContext {
        self.inner.ctx()
    }

    fn poll_work(&self) -> PollWork {
        self.inner.poll_work()
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.execute(work).await
        })
    }
}

/// Wraps a transfer to override execute behavior with a custom function.
///
/// If the execute_fn returns `WorkOutcome::Failed`, the inner transfer's context
/// is transitioned to terminal state (mirroring real transfer behavior).
pub(crate) struct WithExecute<S> {
    inner: S,
    execute_fn: fn(&mut WorkItem) -> WorkOutcome,
}

impl<S: std::fmt::Debug> std::fmt::Debug for WithExecute<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithExecute")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<S: Transfer> WithExecute<S> {
    pub(crate) fn new(inner: S, execute_fn: fn(&mut WorkItem) -> WorkOutcome) -> Self {
        Self { inner, execute_fn }
    }
}

impl<S: Transfer> Transfer for WithExecute<S> {
    fn ctx(&self) -> &TransferContext {
        self.inner.ctx()
    }

    fn poll_work(&self) -> PollWork {
        self.inner.poll_work()
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        let outcome = (self.execute_fn)(work);
        Box::pin(async move {
            if matches!(outcome, WorkOutcome::Failed) {
                self.inner.ctx().set_failed(crate::error::from_kind(
                    crate::error::ErrorKind::RuntimeError,
                )("mock transfer failed"));
                self.inner.ctx().signal_terminal();
            }
            outcome
        })
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
        let sm = FixedWorkCount::new(test_id(), 3);

        for _ in 0..3 {
            assert!(matches!(sm.poll_work(), PollWork::Ready(_)));
        }
        assert!(matches!(sm.poll_work(), PollWork::Done));
    }

    #[tokio::test]
    async fn test_with_delay() {
        let sm = WithDelay::new(FixedWorkCount::new(test_id(), 1), Duration::from_millis(50));

        let mut work = match sm.poll_work() {
            PollWork::Ready(w) => w,
            _ => panic!("expected Ready"),
        };

        let start = std::time::Instant::now();
        sm.execute(&mut work).await;
        assert!(start.elapsed() >= Duration::from_millis(50));
    }
}
