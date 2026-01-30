/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Mock transfer implementations for testing scheduler behavior.
//!
//! # Design
//!
//! Mock transfers are built from composable state machines that model
//! different transfer patterns and behaviors.
//!
//! ## Core Trait
//!
//! [`MockStateMachine`] defines the interface - `poll_work()` and `execute()`.
//!
//! ## Base State Machines
//!
//! - [`FixedWorkCount`] - Generates N work items (for basic scheduler tests)
//! - [`MpuUpload`] - Models CreateMPU -> N x UploadPart -> CompleteMPU
//! - [`PutObject`] - Models single PutObject call
//!
//! ## Wrappers (Composable Behaviors)
//!
//! - [`WithDelay`] - Adds execution delay to any state machine
//! - [`FailAt`] - Fails at a specific execution count
//!
//! ## Examples
//!
//! ```ignore
//! // Simple 10-part upload
//! MockTransfer::new(id, MpuUpload::new(10))
//!
//! // Slow upload for cancellation testing
//! MockTransfer::new(id, WithDelay::new(
//!     MpuUpload::new(10),
//!     Duration::from_millis(100),
//! ))
//!
//! // Upload that fails on part 5
//! MockTransfer::new(id, FailAt::new(
//!     MpuUpload::new(10),
//!     5,
//!     ErrorKind::IOError,
//!     "simulated failure",
//! ))
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::{Error, ErrorKind};
use crate::scheduler::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};

// =============================================================================
// Core Trait
// =============================================================================

/// Trait for mock state machines that drive transfer behavior.
pub(crate) trait MockStateMachine: Send + Sync + std::fmt::Debug {
    /// Poll for next work item.
    fn poll_work(&self, id: TransferId) -> PollWork;

    /// Execute a work item. Called by worker after pulling work.
    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;
}

// =============================================================================
// MockTransfer - Wrapper for any MockStateMachine
// =============================================================================

/// Mock transfer that wraps any [`MockStateMachine`].
#[derive(Clone)]
pub(crate) struct MockTransfer {
    id: TransferId,
    state_machine: Arc<dyn MockStateMachine>,
    cancellation_token: CancellationToken,
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
        Self {
            id,
            state_machine,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub(crate) fn id(&self) -> TransferId {
        self.id
    }

    pub(crate) fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        self.state_machine.poll_work(self.id)
    }

    pub(crate) async fn execute(&self, work: &mut WorkItem) -> WorkOutcome {
        self.state_machine.execute(work).await
    }
}

// =============================================================================
// Base State Machines
// =============================================================================

/// Models an MPU upload: CreateMPU -> N x UploadPart (DataIO->Network) -> CompleteMPU
#[derive(Debug)]
pub(crate) struct MpuUpload {
    part_count: u64,
    state: AtomicUsize, // 0=init, 1=uploading, 2=completing, 3=done
    next_part: AtomicU64,
    parts_completed: AtomicU64,
}

impl MpuUpload {
    pub(crate) fn new(part_count: u64) -> Self {
        Self {
            part_count,
            state: AtomicUsize::new(0),
            next_part: AtomicU64::new(1),
            parts_completed: AtomicU64::new(0),
        }
    }
}

impl MockStateMachine for MpuUpload {
    fn poll_work(&self, id: TransferId) -> PollWork {
        match self.state.load(Ordering::SeqCst) {
            0 => {
                // Init -> CreateMPU
                PollWork::Ready(WorkItem {
                    transfer_id: id,
                    kind: WorkKind::Network,
                    data: WorkData::CreateMPU,
                })
            }
            1 => {
                // Uploading parts
                let part = self.next_part.fetch_add(1, Ordering::SeqCst);
                if part > self.part_count {
                    self.next_part.store(self.part_count + 1, Ordering::SeqCst);
                    PollWork::Pending
                } else {
                    PollWork::Ready(WorkItem {
                        transfer_id: id,
                        kind: WorkKind::DataIO,
                        data: WorkData::UploadPart {
                            part_number: part,
                            part_data: None,
                        },
                    })
                }
            }
            2 => {
                // Completing
                PollWork::Ready(WorkItem {
                    transfer_id: id,
                    kind: WorkKind::Network,
                    data: WorkData::CompleteMPU,
                })
            }
            _ => PollWork::Done,
        }
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            match &work.data {
                WorkData::CreateMPU => {
                    self.state.store(1, Ordering::SeqCst);
                    WorkOutcome::Success {
                        schedule_next: None,
                        data: WorkData::CreateMPU,
                    }
                }
                WorkData::UploadPart { part_number, .. } => {
                    if work.kind == WorkKind::DataIO {
                        // DataIO phase -> schedule Network
                        WorkOutcome::Success {
                            schedule_next: Some(WorkKind::Network),
                            data: WorkData::UploadPart {
                                part_number: *part_number,
                                part_data: None,
                            },
                        }
                    } else {
                        // Network phase complete
                        let completed = self.parts_completed.fetch_add(1, Ordering::SeqCst) + 1;
                        if completed >= self.part_count {
                            self.state.store(2, Ordering::SeqCst);
                        }
                        WorkOutcome::Success {
                            schedule_next: None,
                            data: WorkData::UploadPart {
                                part_number: *part_number,
                                part_data: None,
                            },
                        }
                    }
                }
                WorkData::CompleteMPU => {
                    self.state.store(3, Ordering::SeqCst);
                    WorkOutcome::Success {
                        schedule_next: None,
                        data: WorkData::CompleteMPU,
                    }
                }
                _ => unreachable!("MpuUpload received unexpected work data"),
            }
        })
    }
}

/// Models a single PutObject upload.
#[derive(Debug)]
pub(crate) struct PutObject {
    done: std::sync::atomic::AtomicBool,
}

impl PutObject {
    pub(crate) fn new() -> Self {
        Self {
            done: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl MockStateMachine for PutObject {
    fn poll_work(&self, id: TransferId) -> PollWork {
        if self.done.load(Ordering::SeqCst) {
            PollWork::Done
        } else {
            PollWork::Ready(WorkItem {
                transfer_id: id,
                kind: WorkKind::Network,
                data: WorkData::PutObject { stream: None },
            })
        }
    }

    fn execute<'a>(
        &'a self,
        _work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            self.done.store(true, Ordering::SeqCst);
            WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::PutObject { stream: None },
            }
        })
    }
}

// =============================================================================
// Wrappers (Composable Behaviors)
// =============================================================================

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

    /// Access the wrapped state machine (for test assertions).
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
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.execute(work).await
        })
    }
}

/// Wraps a state machine to fail at a specific execution count.
#[derive(Debug)]
pub(crate) struct FailAt<S> {
    inner: S,
    fail_at: u64,
    execution_count: AtomicU64,
    error_kind: ErrorKind,
    error_msg: &'static str,
}

impl<S: MockStateMachine> FailAt<S> {
    pub(crate) fn new(
        inner: S,
        fail_at: u64,
        error_kind: ErrorKind,
        error_msg: &'static str,
    ) -> Self {
        Self {
            inner,
            fail_at,
            execution_count: AtomicU64::new(0),
            error_kind,
            error_msg,
        }
    }
}

impl<S: MockStateMachine> MockStateMachine for FailAt<S> {
    fn poll_work(&self, id: TransferId) -> PollWork {
        self.inner.poll_work(id)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            let count = self.execution_count.fetch_add(1, Ordering::SeqCst) + 1;
            if count == self.fail_at {
                WorkOutcome::Failed
            } else {
                self.inner.execute(work).await
            }
        })
    }
}

// =============================================================================
// Simple state machine for basic tests (backwards compat)
// =============================================================================

/// Simple state machine that generates N Network work items.
///
/// Used for basic scheduler tests where we just need a fixed amount of work
/// to flow through the system. All work items are `WorkKind::Network` with
/// `WorkData::UploadPart` (the specific data doesn't matter for scheduler tests).
///
/// For tests that need realistic upload behavior, use [`MpuUpload`] or [`PutObject`].
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

    /// Number of work items generated via poll_work()
    pub(crate) fn generated_count(&self) -> u64 {
        self.generated.load(Ordering::SeqCst).min(self.total)
    }

    /// Number of work items that completed execution
    pub(crate) fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::SeqCst)
    }

    /// Total work items this state machine will generate
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Whether all work has been completed
    pub(crate) fn is_complete(&self) -> bool {
        self.completed.load(Ordering::SeqCst) >= self.total
    }
}

impl MockStateMachine for FixedWorkCount {
    fn poll_work(&self, id: TransferId) -> PollWork {
        let completed = self.completed.load(Ordering::SeqCst);
        if completed >= self.total {
            return PollWork::Done;
        }

        let gen = self.generated.fetch_add(1, Ordering::SeqCst);
        if gen >= self.total {
            self.generated.store(self.total, Ordering::SeqCst);
            return PollWork::Pending;
        }

        PollWork::Ready(WorkItem {
            transfer_id: id,
            kind: WorkKind::Network,
            data: WorkData::UploadPart {
                part_number: gen + 1,
                part_data: None,
            },
        })
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            let _completed = self.completed.fetch_add(1, Ordering::SeqCst) + 1;
            WorkOutcome::Success {
                schedule_next: None,
                data: WorkData::UploadPart {
                    part_number: match &work.data {
                        WorkData::UploadPart { part_number, .. } => *part_number,
                        _ => 0,
                    },
                    part_data: None,
                },
            }
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

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

        // Should generate 3 work items
        for _i in 1..=3 {
            match sm.poll_work(id) {
                PollWork::Ready(work) => {
                    assert_eq!(work.transfer_id, id);
                }
                other => panic!("expected Ready, got {:?}", other),
            }
        }

        // Should be pending (waiting for completions)
        assert!(matches!(sm.poll_work(id), PollWork::Pending));
    }

    #[tokio::test]
    async fn test_mpu_upload_state_transitions() {
        let sm = MpuUpload::new(2);
        let id = test_id();

        // First poll -> CreateMPU
        let mut work = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            other => panic!("expected Ready(CreateMPU), got {:?}", other),
        };
        assert!(matches!(work.data, WorkData::CreateMPU));

        // Execute CreateMPU
        sm.execute(&mut work).await;

        // Next polls -> UploadPart (DataIO)
        for _part in 1..=2 {
            let mut work = match sm.poll_work(id) {
                PollWork::Ready(w) => w,
                other => panic!("expected Ready(UploadPart), got {:?}", other),
            };
            assert!(matches!(work.data, WorkData::UploadPart { .. }));
            assert_eq!(work.kind, WorkKind::DataIO);

            // Execute DataIO -> should schedule Network
            let outcome = sm.execute(&mut work).await;
            assert!(matches!(
                outcome,
                WorkOutcome::Success {
                    schedule_next: Some(WorkKind::Network),
                    ..
                }
            ));

            // Simulate network phase
            work.kind = WorkKind::Network;
            sm.execute(&mut work).await;
        }

        // Should be pending until woken, then CompleteMPU
        // (In real scheduler, wake would be called after parts complete)
    }

    #[tokio::test]
    async fn test_with_delay() {
        let sm = WithDelay::new(FixedWorkCount::new(1), Duration::from_millis(50));
        let id = test_id();

        let mut work = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            other => panic!("expected Ready, got {:?}", other),
        };

        let start = std::time::Instant::now();
        sm.execute(&mut work).await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_fail_at() {
        let sm = FailAt::new(
            FixedWorkCount::new(3),
            2,
            ErrorKind::IOError,
            "simulated failure",
        );
        let id = test_id();

        // First execution succeeds
        let mut work1 = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            _ => panic!(),
        };
        assert!(matches!(
            sm.execute(&mut work1).await,
            WorkOutcome::Success { .. }
        ));

        // Second execution fails
        let mut work2 = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            _ => panic!(),
        };
        assert!(matches!(
            sm.execute(&mut work2).await,
            WorkOutcome::Failed { .. }
        ));

        // Third execution succeeds
        let mut work3 = match sm.poll_work(id) {
            PollWork::Ready(w) => w,
            _ => panic!(),
        };
        assert!(matches!(
            sm.execute(&mut work3).await,
            WorkOutcome::Success { .. }
        ));
    }
}
