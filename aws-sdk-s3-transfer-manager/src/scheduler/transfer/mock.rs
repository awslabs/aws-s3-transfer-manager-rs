/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Mock transfer implementations for testing scheduler behavior.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::transfer::{
    IoRequest, PollWork, StateMachineTerminalReceiver, Transfer, TransferContext, TransferId,
    WorkOutcome,
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
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().client(s3_client).build();
        let handle = crate::client::Handle::new_for_test(config, 1);

        let (ctx, _completion_rx) = TransferContext::with_id(id, handle);

        Self {
            id,
            ctx,
            state_machine,
        }
    }

    /// Create a mock transfer that shares the given handle's scheduler.
    ///
    /// Use this when tests need multiple transfers routed through the same
    /// scheduler instance (e.g., composite parent + children).
    pub(crate) fn new_with_handle<S: MockStateMachine + 'static>(
        id: TransferId,
        state_machine: Arc<S>,
        handle: Arc<crate::client::Handle>,
    ) -> Self {
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

        PollWork::Ready(IoRequest { data: None })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.completed.fetch_add(1, Ordering::SeqCst);
            WorkOutcome::Success { data: None }
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

/// A work counter shared across children of a composite transfer.
///
/// Each child's `execute` increments this counter, allowing tests to observe
/// how many dispatches a particular tree has received.
#[derive(Debug, Clone)]
pub(crate) struct DispatchCounter(Arc<AtomicU64>);

impl DispatchCounter {
    /// Create a new zero-valued counter.
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// Current count of dispatches observed.
    pub(crate) fn count(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Mock state machine that counts work items and increments a shared dispatch counter.
///
/// Used as the child state machine in composite transfer tests. Each `execute`
/// call increments both the internal completion counter and the shared
/// `DispatchCounter`, letting tests assert per-tree dispatch ratios.
#[derive(Debug)]
pub(crate) struct CountedWork {
    total: u64,
    generated: AtomicU64,
    completed: AtomicU64,
    counter: DispatchCounter,
}

impl CountedWork {
    /// Create a counted work mock with `count` work items that increments `counter` on each execute.
    pub(crate) fn new(count: u64, counter: DispatchCounter) -> Self {
        Self {
            total: count,
            generated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            counter,
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.completed.load(Ordering::SeqCst) >= self.total
    }

    #[allow(dead_code)]
    pub(crate) fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }
}

impl MockStateMachine for CountedWork {
    fn poll_work(&self, _id: TransferId) -> PollWork {
        let gen = self.generated.fetch_add(1, Ordering::SeqCst);
        if gen >= self.total {
            return PollWork::Done;
        }
        PollWork::Ready(IoRequest { data: None })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.counter.0.fetch_add(1, Ordering::SeqCst);
            self.completed.fetch_add(1, Ordering::SeqCst);
            WorkOutcome::Success { data: None }
        })
    }
}

/// Mock state machine whose execute blocks until a `tokio::sync::Notify` is signaled.
///
/// Used to test memory-cap behavior where children must remain in-flight
/// (not completing) so the composite can observe backpressure.
#[derive(Debug)]
pub(crate) struct BlockingWork {
    generated: AtomicU64,
    total: u64,
    completed: AtomicU64,
    notify: Arc<tokio::sync::Notify>,
    counter: DispatchCounter,
}

impl BlockingWork {
    /// Create a blocking work mock. Each `execute` waits on `notify` before completing.
    #[allow(dead_code)]
    pub(crate) fn new(
        count: u64,
        notify: Arc<tokio::sync::Notify>,
        counter: DispatchCounter,
    ) -> Self {
        Self {
            generated: AtomicU64::new(0),
            total: count,
            completed: AtomicU64::new(0),
            notify,
            counter,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }
}

impl MockStateMachine for BlockingWork {
    fn poll_work(&self, _id: TransferId) -> PollWork {
        let gen = self.generated.fetch_add(1, Ordering::SeqCst);
        if gen >= self.total {
            return PollWork::Done;
        }
        PollWork::Ready(IoRequest { data: None })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.notify.notified().await;
            self.counter.0.fetch_add(1, Ordering::SeqCst);
            self.completed.fetch_add(1, Ordering::SeqCst);
            WorkOutcome::Success { data: None }
        })
    }
}

use std::sync::Mutex;

/// A child transfer that signals terminal when all work completes.
///
/// Unlike `MockTransfer` (which never calls `signal_terminal` on success),
/// this transfer properly signals its parent when done, enabling composite
/// transfers to observe child completion and resume spawning.
pub(crate) struct ChildMockTransfer {
    ctx: TransferContext,
    total: u64,
    generated: AtomicU64,
    /// Protected by `state_lock` for wake protocol correctness.
    completed: AtomicU64,
    counter: DispatchCounter,
    /// If set, execute blocks on this notify before completing.
    notify: Option<Arc<tokio::sync::Notify>>,
    /// Lock shared between poll_work and execute for wake protocol.
    state_lock: Mutex<()>,
    /// If set, incremented when this transfer's `poll_work` returns `Done`.
    /// Composite parents track child terminations through this so they can
    /// gate their own `Done` on all children having actually finished
    /// (not merely on `execute` having run, which fires before the
    /// child's terminating poll).
    terminated_counter: Option<Arc<AtomicU64>>,
}

impl std::fmt::Debug for ChildMockTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChildMockTransfer")
            .field("id", &self.ctx.id)
            .finish_non_exhaustive()
    }
}

impl ChildMockTransfer {
    /// Create a child transfer that increments `counter` on each dispatch and
    /// signals terminal when all work completes.
    pub(crate) fn new(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
        work_count: u64,
        counter: DispatchCounter,
    ) -> Self {
        let (ctx, _rx) = TransferContext::with_id(id, handle);
        Self {
            ctx,
            total: work_count,
            generated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            counter,
            notify: None,
            state_lock: Mutex::new(()),
            terminated_counter: None,
        }
    }

    /// Create a child transfer whose execute blocks until `notify` is signaled.
    pub(crate) fn new_blocking(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
        counter: DispatchCounter,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        let (ctx, _rx) = TransferContext::with_id(id, handle);
        Self {
            ctx,
            total: 1,
            generated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            counter,
            notify: Some(notify),
            state_lock: Mutex::new(()),
            terminated_counter: None,
        }
    }

    /// Set the parent's terminated counter. Incremented by 1 when this
    /// transfer's `poll_work` returns `Done`. Used by composite parents to
    /// track child terminations so they can wait for all children to finish
    /// before returning `Done` themselves.
    pub(crate) fn with_terminated_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.terminated_counter = Some(counter);
        self
    }
}

impl Transfer for ChildMockTransfer {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        // Only generate work if we haven't generated all items yet
        let gen = self.generated.load(Ordering::SeqCst);
        if gen >= self.total {
            // All work generated. Follow the wake protocol:
            // lock → set_pending → check condition → unlock
            let _guard = self.state_lock.lock().unwrap();
            self.ctx.set_pending();
            if self.completed.load(Ordering::SeqCst) >= self.total {
                // All completed
                drop(_guard);
                self.ctx.set_completed();
                // Increment terminated_counter BEFORE signal_terminal so the
                // parent's wake observes the up-to-date count. signal_terminal
                // calls scheduler.wake(parent), which can synchronously drive
                // the parent's poll_work via generate_work; the parent must
                // see this child counted as terminated to make the right
                // Done/Pending decision.
                if let Some(ref tc) = self.terminated_counter {
                    tc.fetch_add(1, Ordering::SeqCst);
                }
                self.ctx.signal_terminal();
                return PollWork::Done;
            }
            return PollWork::Pending;
        }
        self.generated.fetch_add(1, Ordering::SeqCst);
        PollWork::Ready(IoRequest { data: None })
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            if let Some(ref notify) = self.notify {
                // Poll for either the notify signal or cancellation
                loop {
                    if self.ctx.is_cancelled() {
                        return WorkOutcome::Cancelled;
                    }
                    match tokio::time::timeout(Duration::from_millis(10), notify.notified()).await {
                        Ok(()) => break,
                        Err(_) => continue,
                    }
                }
            }
            if self.ctx.is_cancelled() {
                return WorkOutcome::Cancelled;
            }
            self.counter.0.fetch_add(1, Ordering::SeqCst);
            // Mutator pattern: lock → mutate → unlock → try_wake
            {
                let _guard = self.state_lock.lock().unwrap();
                self.completed.fetch_add(1, Ordering::SeqCst);
            }
            self.ctx.try_wake();
            WorkOutcome::Success { data: None }
        })
    }
}

/// Internal state for the composite mock transfer.
struct CompositeMockState {
    total_children: u64,
    spawned: u64,
}

/// A composite transfer mock that spawns child transfers into the scheduler.
///
/// Implements `Transfer` directly (not `MockStateMachine`) because composites
/// need access to the parent `TransferContext` for lifecycle management and
/// the shared `Handle` for enqueueing children.
///
/// Each `poll_work` call spawns children (up to a memory cap) and returns
/// `Pending` while waiting for children to complete, or `Done` when all
/// children have terminated. Done is gated on child *termination* (each
/// child's `poll_work` returning `Done` after its work is complete), not on
/// the work counter alone. The work counter is incremented inside `execute`,
/// which fires strictly before the child's terminating poll. Returning
/// `Done` based only on the work counter is a contract violation: the
/// parent would dismiss its group while children are still in the middle
/// of their wake-then-Done sequence, leaving them orphaned in the scheduler.
pub(crate) struct CompositeMock {
    ctx: TransferContext,
    handle: Arc<crate::client::Handle>,
    state: Mutex<CompositeMockState>,
    work_per_child: u64,
    memory_cap: u64,
    counter: DispatchCounter,
    /// Notify used for blocking children (if provided).
    child_notify: Option<Arc<tokio::sync::Notify>>,
    /// Next child id counter (global atomic to avoid collisions).
    next_child_id: AtomicU64,
    /// Shared with all children. Incremented when a child's `poll_work`
    /// returns `Done`. Used to determine when this composite can return
    /// `Done` without leaking unreaped children.
    terminated_counter: Arc<AtomicU64>,
}

impl std::fmt::Debug for CompositeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeMock")
            .field("id", &self.ctx.id)
            .finish_non_exhaustive()
    }
}

impl CompositeMock {
    /// Create a new composite mock transfer.
    ///
    /// # Arguments
    /// - `id`: The transfer's identity (must have `parent: None` for a top-level composite).
    /// - `handle`: Shared handle whose scheduler receives child transfers.
    /// - `total_children`: How many child transfers to spawn over the composite's lifetime.
    /// - `work_per_child`: Work items each child produces.
    /// - `memory_cap`: Maximum children spawned before waiting for completions.
    /// - `counter`: Shared dispatch counter incremented by each child's execute.
    pub(crate) fn new(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
        total_children: u64,
        work_per_child: u64,
        memory_cap: u64,
        counter: DispatchCounter,
    ) -> Self {
        let (ctx, _rx) = TransferContext::with_id(id, handle.clone());
        Self {
            ctx,
            handle,
            state: Mutex::new(CompositeMockState {
                total_children,
                spawned: 0,
            }),
            work_per_child,
            memory_cap,
            counter,
            child_notify: None,
            next_child_id: AtomicU64::new(id.id * 1_000_000 + 1),
            terminated_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a composite whose children block in execute until `notify` is signaled.
    ///
    /// Used to test memory-cap backpressure where children must remain in-flight.
    pub(crate) fn new_blocking(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
        total_children: u64,
        memory_cap: u64,
        counter: DispatchCounter,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        let (ctx, _rx) = TransferContext::with_id(id, handle.clone());
        Self {
            ctx,
            handle,
            state: Mutex::new(CompositeMockState {
                total_children,
                spawned: 0,
            }),
            work_per_child: 1,
            memory_cap,
            counter,
            child_notify: Some(notify),
            next_child_id: AtomicU64::new(id.id * 1_000_000 + 1),
            terminated_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Total children spawned so far.
    #[allow(dead_code)]
    pub(crate) fn total_spawned(&self) -> u64 {
        self.state.lock().unwrap().spawned
    }

    /// Total children that have terminated (their `poll_work` returned `Done`).
    #[allow(dead_code)]
    pub(crate) fn total_terminated(&self) -> u64 {
        self.terminated_counter.load(Ordering::SeqCst)
    }

    /// Whether all children have been spawned and terminated.
    #[allow(dead_code)]
    pub(crate) fn is_complete(&self) -> bool {
        let s = self.state.lock().unwrap();
        let terminated = self.terminated_counter.load(Ordering::SeqCst);
        terminated >= s.total_children && s.spawned >= s.total_children
    }

    /// The dispatch counter shared with all children.
    #[allow(dead_code)]
    pub(crate) fn dispatch_counter(&self) -> &DispatchCounter {
        &self.counter
    }

    fn spawn_children(&self, state: &mut CompositeMockState) {
        let terminated = self.terminated_counter.load(Ordering::SeqCst);
        let in_flight = state.spawned.saturating_sub(terminated);
        let available = self.memory_cap.saturating_sub(in_flight);
        let remaining = state.total_children - state.spawned;
        let to_spawn = available.min(remaining).min(32); // SPAWN_BATCH_SIZE cap

        for _ in 0..to_spawn {
            let child_id_num = self.next_child_id.fetch_add(1, Ordering::Relaxed);
            let child_id = TransferId {
                id: child_id_num,
                parent: Some(self.ctx.id.id),
            };

            let child: Box<dyn Transfer> = if let Some(ref notify) = self.child_notify {
                Box::new(
                    ChildMockTransfer::new_blocking(
                        child_id,
                        self.handle.clone(),
                        self.counter.clone(),
                        notify.clone(),
                    )
                    .with_terminated_counter(self.terminated_counter.clone()),
                )
            } else {
                Box::new(
                    ChildMockTransfer::new(
                        child_id,
                        self.handle.clone(),
                        self.work_per_child,
                        self.counter.clone(),
                    )
                    .with_terminated_counter(self.terminated_counter.clone()),
                )
            };

            self.handle.scheduler.enqueue_transfer(child);
            state.spawned += 1;
        }
    }
}

impl Transfer for CompositeMock {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        let mut state = self.state.lock().unwrap();

        // Done condition: all children spawned AND all children terminated
        // (their poll_work returned Done). The work counter is NOT used for
        // termination because it is incremented in execute, which fires
        // before the child's terminating poll - using it would race against
        // children mid-wake and orphan them.
        let terminated = self.terminated_counter.load(Ordering::SeqCst);
        if terminated >= state.total_children && state.spawned >= state.total_children {
            drop(state);
            self.ctx.set_completed();
            self.ctx.signal_terminal();
            return PollWork::Done;
        }

        // Spawn more children if under cap
        if state.spawned < state.total_children {
            self.spawn_children(&mut state);
        }

        // Still waiting for children to complete
        drop(state);
        self.ctx.set_pending();
        PollWork::Pending
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        unreachable!("CompositeMock never returns PollWork::Ready")
    }
}

/// A composite mock that panics on its first `poll_work` invocation.
///
/// Used to test the scheduler's panic recovery: the scheduler catches the
/// panic, force-terminates the transfer, and continues processing peers.
pub(crate) struct PanickingCompositeMock {
    ctx: TransferContext,
}

impl std::fmt::Debug for PanickingCompositeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PanickingCompositeMock").finish()
    }
}

impl PanickingCompositeMock {
    /// Create a panicking composite mock with the given identity.
    pub(crate) fn new(id: TransferId, handle: Arc<crate::client::Handle>) -> Self {
        let (ctx, _rx) = TransferContext::with_id(id, handle);
        Self { ctx }
    }
}

impl Transfer for PanickingCompositeMock {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        panic!("PanickingCompositeMock: intentional panic in poll_work")
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        unreachable!("PanickingCompositeMock never returns PollWork::Ready")
    }
}

/// A composite that violates the Transfer contract by returning `Done`
/// while children are still alive in the scheduler. Used to test the
/// scheduler's defensive cleanup of orphaned children in
/// `generate_work`'s Done branch.
///
/// On first `poll_work`, spawns `num_children` no-op children whose
/// `poll_work` returns `Pending` indefinitely. Then signals terminal
/// and returns `Done`. Without defensive cleanup, the children would
/// remain in the transfers map forever (their handles never resolving).
pub(crate) struct BuggyDoneMock {
    ctx: TransferContext,
    handle: Arc<crate::client::Handle>,
    num_children: u64,
    children_spawned: AtomicBool,
    /// Senders we hand off to the children via construction. Filled on
    /// first `poll_work`. The caller holds a clone of this Arc so it
    /// can take the receivers after enqueue ownership-transfers the
    /// mock to the scheduler.
    child_terminals: Arc<std::sync::Mutex<Option<Vec<StateMachineTerminalReceiver>>>>,
}

/// Handle returned by `BuggyDoneMock::new` so the test can observe each
/// child's termination after the mock has been moved into the scheduler.
pub(crate) type BuggyDoneChildTerminals =
    Arc<std::sync::Mutex<Option<Vec<StateMachineTerminalReceiver>>>>;

impl std::fmt::Debug for BuggyDoneMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuggyDoneMock").finish()
    }
}

impl BuggyDoneMock {
    pub(crate) fn new(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
        num_children: u64,
    ) -> (Self, BuggyDoneChildTerminals) {
        let (ctx, _rx) = TransferContext::with_id(id, handle.clone());
        let terminals: BuggyDoneChildTerminals = Arc::new(std::sync::Mutex::new(Some(
            Vec::with_capacity(num_children as usize),
        )));
        let mock = Self {
            ctx,
            handle,
            num_children,
            children_spawned: AtomicBool::new(false),
            child_terminals: terminals.clone(),
        };
        (mock, terminals)
    }
}

impl Transfer for BuggyDoneMock {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        if !self
            .children_spawned
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            // Spawn children with NoopChild::poll_work returning Pending,
            // so they sit in the transfers map indefinitely until cleaned
            // up by the Done branch's defensive code.
            let mut terminals = Vec::with_capacity(self.num_children as usize);
            for i in 0..self.num_children {
                let child_id = TransferId {
                    id: 100_000 + i,
                    parent: Some(self.ctx.id.id),
                };
                let (child, term_rx) = NoopChild::new(child_id, self.handle.clone());
                terminals.push(term_rx);
                self.handle.scheduler.enqueue_transfer(Box::new(child));
            }
            *self.child_terminals.lock().unwrap() = Some(terminals);
        }
        // Contract violation: return Done while children are still alive.
        self.ctx.set_completed();
        self.ctx.signal_terminal();
        PollWork::Done
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        unreachable!("BuggyDoneMock never returns PollWork::Ready")
    }
}

/// A child transfer that always returns `Pending` from `poll_work` and
/// never produces work. Used by `BuggyDoneMock` so children remain in
/// the transfers map until cancelled.
pub(crate) struct NoopChild {
    ctx: TransferContext,
}

impl std::fmt::Debug for NoopChild {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoopChild").finish()
    }
}

impl NoopChild {
    pub(crate) fn new(
        id: TransferId,
        handle: Arc<crate::client::Handle>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (ctx, rx) = TransferContext::with_id(id, handle);
        (Self { ctx }, rx)
    }
}

impl Transfer for NoopChild {
    fn ctx(&self) -> &TransferContext {
        &self.ctx
    }

    fn poll_work(&self) -> PollWork {
        self.ctx.set_pending();
        PollWork::Pending
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        unreachable!("NoopChild never returns PollWork::Ready")
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fixed_work_count() {
        let sm = FixedWorkCount::new(3);
        let id = test_id();

        for _ in 0..3 {
            assert!(matches!(sm.poll_work(id), PollWork::Ready(_)));
        }
        assert!(matches!(sm.poll_work(id), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
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
