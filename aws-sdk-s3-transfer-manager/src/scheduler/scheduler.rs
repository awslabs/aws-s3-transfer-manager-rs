/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Event-driven scheduler for coordinating transfer operations.
//!
//! The scheduler has a global view of all transfers and their associated work.
//! It controls concurrency for specific phases (disk I/O vs network) allowing
//! them to be tuned independently.
//!
//! # Overview
//!
//! ```text
//!   ┌─────────────┐     enqueue      ┌─────────────┐
//!   │   Upload    │ ───────────────► │  Scheduler  │
//!   │  Transfer   │                  │             │
//!   └─────────────┘                  └──────┬──────┘
//!                                           │
//!                          ┌────────────────┼────────────────┐
//!                          ▼                ▼                ▼
//!                    ┌──────────┐     ┌──────────┐     ┌──────────┐
//!                    │ Transfer │     │ Transfer │     │ Transfer │
//!                    │    A     │     │    B     │     │    C     │
//!                    └──────────┘     └──────────┘     └──────────┘
//! ```
//!
//! # Worker Pool Model
//!
//! Workers pull work via `pool.next_work()` rather than work being spawned
//! per-item. This enables:
//! - Priority control (changes affect next work selection)
//! - Bounded in-flight work (worker count = max concurrency)
//! - Clean shutdown (workers exit when pool signals shutdown)
//!
//! Workers are spawned lazily on first `enqueue_transfer()` call, avoiding
//! the need for an async context during scheduler construction.
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────┐
//!   │                        Scheduler                            │
//!   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
//!   │  │  Transfers  │  │ DataIO Pool │  │    Network Pool     │ │
//!   │  └─────────────┘  └──────┬──────┘  └──────────┬──────────┘ │
//!   └──────────────────────────┼────────────────────┼────────────┘
//!                              │                    │
//!                    Workers pull work      Workers pull work
//!                              │                    │
//!                         ┌────┴────┐          ┌────┴────┐
//!                         │ Workers │          │ Workers │
//!                         └─────────┘          └─────────┘
//! ```
//!
//! # Work Flow
//!
//! Work progresses through phases. For uploads: DataIO (read) → Network (send).
//! For downloads: Network (receive) → DataIO (write).
//!
//! # Transfer States
//!
//! Transfers are either "ready" (can be polled for work) or "pending" (blocked
//! waiting for in-flight work to complete). The scheduler only polls ready
//! transfers, avoiding wasted work.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{PollWork, Transfer, TransferId, WorkItem, WorkKind, WorkOutcome, WorkerPool};

/// State for a tracked transfer
struct TransferState {
    transfer: Transfer,
    pending: bool,
}

/// Inner scheduler state protected by mutex
struct SchedulerInner {
    /// All transfers by ID
    transfers: HashMap<TransferId, TransferState>,
    /// IDs of transfers ready to be polled for work
    ready_queue: VecDeque<TransferId>,
}

/// Event-driven scheduler for coordinating transfer work.
///
/// Workers pull work via `next_work()` rather than work being spawned per-item.
/// This enables priority control and bounded in-flight work.
///
/// Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    data_io_pool: Arc<WorkerPool>,
    network_pool: Arc<WorkerPool>,
}

impl std::fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scheduler").finish_non_exhaustive()
    }
}

impl Scheduler {
    pub(crate) fn new(data_io_concurrency: usize, network_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                transfers: HashMap::new(),
                ready_queue: VecDeque::new(),
            })),
            data_io_pool: Arc::new(WorkerPool::new(data_io_concurrency)),
            network_pool: Arc::new(WorkerPool::new(network_concurrency)),
        }
    }

    /// Ensure workers are spawned for a pool. Called lazily on first enqueue.
    fn ensure_workers_started(&self, pool: &Arc<WorkerPool>) {
        if pool.mark_started() {
            // We're the first - spawn workers
            for _ in 0..pool.concurrency() {
                let pool = Arc::clone(pool);
                let scheduler = self.clone();
                tokio::spawn(async move {
                    worker_loop(pool, scheduler).await;
                });
            }
        }
    }

    /// Add a transfer and start generating work.
    pub(crate) fn enqueue_transfer(&self, transfer: Transfer) {
        // Ensure workers are running (lazy spawn)
        self.ensure_workers_started(&self.data_io_pool);
        self.ensure_workers_started(&self.network_pool);

        let mut inner = self.inner.lock().unwrap();
        let id = transfer.id();
        inner.transfers.insert(
            id,
            TransferState {
                transfer,
                pending: false,
            },
        );
        inner.ready_queue.push_back(id);
        self.generate_work(&mut inner);
    }

    /// Get a transfer by ID. Used by workers to execute work.
    pub(crate) fn get_transfer(&self, id: TransferId) -> Option<Transfer> {
        let inner = self.inner.lock().unwrap();
        inner.transfers.get(&id).map(|s| s.transfer.clone())
    }

    /// Wake a transfer, moving it from pending to ready.
    ///
    /// Called when work completes or on external events (abort/cancel).
    pub(crate) fn wake(&self, id: TransferId) {
        let mut inner = self.inner.lock().unwrap();
        inner.wake(id);
        self.generate_work(&mut inner);
    }

    /// Cancel a transfer, removing it from the scheduler.
    ///
    /// In-flight work may still complete, but:
    /// - No new work will be generated for this transfer
    /// - Follow-on work won't be scheduled
    /// - Workers will skip queued work when they find transfer missing
    ///
    /// TODO(redux): When upload_objects/download_objects use the new scheduler,
    /// this needs to cancel child transfers too (where tid.parent == Some(id.id)).
    pub(crate) fn cancel_transfer(&self, id: TransferId) {
        let mut inner = self.inner.lock().unwrap();
        inner.transfers.remove(&id);
    }

    /// Called by workers when work completes.
    ///
    /// Note: Workers call pool.complete() directly - this only handles
    /// transfer state and follow-on work generation.
    pub(crate) fn on_completion(&self, work: WorkItem, outcome: WorkOutcome) {
        let mut inner = self.inner.lock().unwrap();

        // Only process if transfer still exists (not cancelled)
        let transfer_exists = inner.transfers.contains_key(&work.transfer_id);
        if transfer_exists {
            // Wake the transfer so it can be polled again
            inner.wake(work.transfer_id);
        }

        match outcome {
            WorkOutcome::Success {
                schedule_next,
                data,
            } => {
                // Only enqueue follow-on work if transfer still exists
                if transfer_exists {
                    if let Some(kind) = schedule_next {
                        let next_item = WorkItem {
                            transfer_id: work.transfer_id,
                            kind,
                            data,
                        };
                        self.enqueue_to_pool(next_item);
                    }
                }
            }
            WorkOutcome::Failed { .. } | WorkOutcome::Cancelled => {
                // Work failed or was cancelled - no follow-on work
            }
        }

        self.generate_work(&mut inner);
    }

    /// Generate work from ready transfers and push to pools.
    fn generate_work(&self, inner: &mut MutexGuard<'_, SchedulerInner>) {
        let target = self.total_capacity();
        let current = self.total_pending() + self.total_in_flight();

        if current >= target {
            return;
        }

        let to_generate = target - current;
        let mut generated = 0;

        while generated < to_generate {
            let Some(id) = inner.ready_queue.pop_front() else {
                break;
            };

            let Some(state) = inner.transfers.get_mut(&id) else {
                continue;
            };

            match state.transfer.poll_work() {
                PollWork::Ready(work) => {
                    self.enqueue_to_pool(work);
                    generated += 1;
                    // Re-queue - might have more work
                    inner.ready_queue.push_back(id);
                }
                PollWork::Pending => {
                    state.pending = true;
                    // Don't re-queue - will be woken when work completes
                }
                PollWork::Done => {
                    inner.transfers.remove(&id);
                }
            }
        }
    }

    fn enqueue_to_pool(&self, item: WorkItem) {
        match item.kind {
            WorkKind::DataIO => self.data_io_pool.push(item),
            WorkKind::Network => self.network_pool.push(item),
        }
    }

    fn total_capacity(&self) -> usize {
        self.data_io_pool.concurrency() + self.network_pool.concurrency()
    }

    fn total_pending(&self) -> usize {
        self.data_io_pool.pending_count() + self.network_pool.pending_count()
    }

    fn total_in_flight(&self) -> usize {
        self.data_io_pool.in_flight_count() + self.network_pool.in_flight_count()
    }

    /// Check if scheduler is idle (no transfers, no pending work, no in-flight work).
    pub(crate) fn is_idle(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        self.data_io_pool.pending_count() == 0
            && self.data_io_pool.in_flight_count() == 0
            && self.network_pool.pending_count() == 0
            && self.network_pool.in_flight_count() == 0
            && inner.transfers.is_empty()
    }

    /// Shutdown the scheduler. Workers will exit after completing current work.
    pub(crate) fn shutdown(&self) {
        self.data_io_pool.shutdown();
        self.network_pool.shutdown();
    }
}

impl SchedulerInner {
    /// Wake a transfer, moving it from pending to ready.
    fn wake(&mut self, id: TransferId) {
        if let Some(state) = self.transfers.get_mut(&id) {
            if state.pending {
                state.pending = false;
                self.ready_queue.push_back(id);
            }
        }
    }
}

/// Worker loop - pulls work from pool and executes it.
async fn worker_loop(pool: Arc<WorkerPool>, scheduler: Scheduler) {
    loop {
        // Pull work (blocks until available + has capacity, or shutdown)
        let Some(mut work) = pool.next_work().await else {
            break; // Shutdown signaled
        };

        // Get transfer - may be gone if aborted
        let Some(transfer) = scheduler.get_transfer(work.transfer_id) else {
            pool.complete(); // Free capacity
            continue;
        };

        // Execute work with cancellation support
        let token = transfer.cancellation_token().clone();
        let outcome = tokio::select! {
            biased;
            _ = token.cancelled() => WorkOutcome::Cancelled,
            outcome = transfer.execute(&mut work) => outcome,
        };

        // Free capacity and notify scheduler
        pool.complete();
        scheduler.on_completion(work, outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::transfer::mock::{FixedWorkCount, WithDelay};
    use crate::scheduler::MockTransfer;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_single_transfer_completes() {
        let scheduler = Scheduler::new(4, 4);
        let id = TransferId {
            id: 1,
            parent: None,
        };

        let sm = Arc::new(FixedWorkCount::new(5));
        let transfer = Transfer::Mock(MockTransfer::new(id, sm.clone()));
        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        assert!(sm.is_complete());
        assert_eq!(sm.completed_count(), 5);
        scheduler.shutdown();
    }

    #[tokio::test]
    async fn test_multiple_transfers_complete() {
        let scheduler = Scheduler::new(4, 4);
        let mut state_machines = Vec::new();

        for i in 0..3u64 {
            let id = TransferId {
                id: i,
                parent: None,
            };
            let sm = Arc::new(FixedWorkCount::new(4));
            state_machines.push(sm.clone());
            scheduler.enqueue_transfer(Transfer::Mock(MockTransfer::new(id, sm)));
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle");

        for (i, sm) in state_machines.iter().enumerate() {
            assert!(sm.is_complete(), "transfer {} should be complete", i);
            assert_eq!(sm.completed_count(), 4);
        }
        scheduler.shutdown();
    }

    #[test]
    fn test_new_does_not_require_runtime() {
        let _scheduler = Scheduler::new(4, 4);
    }

    #[tokio::test]
    async fn test_cancel_transfer_stops_work() {
        let scheduler = Scheduler::new(2, 2);
        let id = TransferId {
            id: 1,
            parent: None,
        };

        // Use WithDelay to make work slow enough to cancel mid-flight
        let inner = FixedWorkCount::new(20);
        let sm = Arc::new(WithDelay::new(inner, Duration::from_millis(50)));
        let transfer = Transfer::Mock(MockTransfer::new(id, sm.clone()));
        scheduler.enqueue_transfer(transfer);

        // Let some work start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the transfer
        scheduler.cancel_transfer(id);
        assert!(scheduler.get_transfer(id).is_none());

        // Wait for scheduler to become idle
        tokio::time::timeout(Duration::from_secs(2), async {
            while !scheduler.is_idle() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("scheduler should become idle after cancel");

        // Should have completed fewer than total (cancelled early)
        let completed = sm.inner().completed_count();
        assert!(
            completed < 20,
            "expected fewer than 20 completions, got {}",
            completed
        );

        scheduler.shutdown();
    }
}
