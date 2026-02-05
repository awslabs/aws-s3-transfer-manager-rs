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

use super::{
    PollWork, ScheduledWork, Transfer, TransferId, WorkItem, WorkKind, WorkOutcome, WorkerPool,
};
use crate::scheduler::descriptor::TransferDescriptor;
use crate::scheduler::ready_set::ReadySet;
use std::collections::HashMap;
use std::sync::Arc;

/// Event-driven scheduler for coordinating transfer work.
///
/// Workers pull work via `next_work()` rather than work being spawned per-item.
/// This enables priority control and bounded in-flight work.
///
use std::sync::RwLock;

/// Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Scheduler(Arc<SchedulerInner>);

struct SchedulerInner {
    transfers: RwLock<HashMap<TransferId, TransferDescriptor>>,
    ready_set: ReadySet,
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
        Self(Arc::new(SchedulerInner {
            transfers: RwLock::new(HashMap::new()),
            ready_set: ReadySet::new(),
            data_io_pool: Arc::new(WorkerPool::new(data_io_concurrency)),
            network_pool: Arc::new(WorkerPool::new(network_concurrency)),
        }))
    }

    /// Ensure workers are spawned for a pool. Called lazily on first enqueue.
    fn ensure_workers_started(&self, pool: &Arc<WorkerPool>) {
        if pool.mark_started() {
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
        self.ensure_workers_started(&self.0.data_io_pool);
        self.ensure_workers_started(&self.0.network_pool);

        // start with lowest current vruntime to avoid new transfer
        // playing aggressive catchup over already in-flight transfers
        let desc = TransferDescriptor::new_with_vruntime(transfer, self.0.ready_set.min_vruntime());

        {
            let mut transfers = self.0.transfers.write().unwrap();
            transfers.insert(desc.id(), desc.clone());
            self.0.ready_set.insert(desc);
        }

        self.generate_work();
    }

    /// Wake a transfer, moving it from pending to ready.
    ///
    /// Called by state machines when they transition from blocked to ready.
    /// Only generates work if transfer exists and was inserted to ready set.
    pub(crate) fn wake(&self, id: TransferId) {
        let desc = {
            let transfers = self.0.transfers.read().unwrap();
            transfers.get(&id).cloned()
        };
        if let Some(desc) = desc {
            self.0.ready_set.insert(desc);
            self.generate_work();
        }
    }

    /// Cancel a transfer, removing it from the scheduler.
    ///
    /// - Removes from transfers map
    /// - Purges queued work for this transfer from pools
    /// - Outstanding work being executed will complete naturally
    /// - Use `wait_for_idle(id)` to wait for all outstanding work to finish
    ///
    /// TODO(redux): When upload_objects/download_objects use the new scheduler,
    /// this needs to cancel child transfers too (where tid.parent == Some(id.id)).
    pub(crate) fn cancel_transfer(&self, id: TransferId) -> Option<Transfer> {
        let desc = self.0.transfers.write().unwrap().remove(&id)?;

        // Purge queued work
        let purged = self.0.data_io_pool.remove_for_transfer(id)
            + self.0.network_pool.remove_for_transfer(id);
        desc.work_purged(purged);

        Some(desc.transfer().clone())
    }

    /// Called by workers when work completes.
    pub(crate) fn on_completion(&self, work: ScheduledWork, outcome: WorkOutcome) {
        let desc = &work.descriptor;
        let is_idle = desc.work_finished();
        if is_idle {
            desc.notify_idle();
        }

        // if terminal, no further work can be queued
        if desc.is_terminal() {
            return;
        }

        // handle any follow-on work
        if let WorkOutcome::Success {
            schedule_next: Some(kind),
            data,
        } = outcome
        {
            let next = ScheduledWork {
                item: WorkItem { kind, data },
                descriptor: desc.clone(),
            };
            self.enqueue_to_pool(next);
        }

        // capacity has freed try to queue up more work
        self.generate_work();
    }

    fn has_capacity(&self) -> bool {
        self.0.data_io_pool.has_capacity() || self.0.network_pool.has_capacity()
    }

    /// Generate work from ready transfers and push to pools.
    fn generate_work(&self) {
        while self.has_capacity() {
            let Some(desc) = self.0.ready_set.pop() else {
                break;
            };

            match desc.transfer().poll_work() {
                PollWork::Ready(item) => {
                    desc.work_generated();
                    self.0.ready_set.insert(desc.clone());
                    self.enqueue_to_pool(ScheduledWork {
                        item,
                        descriptor: desc,
                    });
                }
                PollWork::Pending => {
                    // re-added on wake as state machine progresses
                }
                PollWork::Done => {
                    // done generating work, remove from transfers
                    self.0.transfers.write().unwrap().remove(&desc.id());
                }
            }
        }
    }

    fn enqueue_to_pool(&self, work: ScheduledWork) {
        work.descriptor.work_queued();
        match work.item.kind {
            WorkKind::DataIO => self.0.data_io_pool.push(work),
            WorkKind::Network => self.0.network_pool.push(work),
        }
    }

    /// Check if scheduler is idle (no transfers, no pending work, no in-flight work).
    pub(crate) fn is_idle(&self) -> bool {
        self.0.transfers.read().unwrap().is_empty()
            && self.0.data_io_pool.pending_count() == 0
            && self.0.data_io_pool.in_flight_count() == 0
            && self.0.network_pool.pending_count() == 0
            && self.0.network_pool.in_flight_count() == 0
    }

    /// Wait for a specific transfer to have no outstanding work.
    pub(crate) async fn wait_for_idle(&self, id: TransferId) {
        let desc = {
            let transfers = self.0.transfers.read().unwrap();
            transfers.get(&id).cloned()
        };
        if let Some(desc) = desc {
            desc.wait_for_idle().await;
        }
    }

    /// Shutdown the scheduler. Workers will exit after completing current work.
    pub(crate) fn shutdown(&self) {
        self.0.data_io_pool.shutdown();
        self.0.network_pool.shutdown();
    }
}

/// Worker loop - pulls work from pool and executes it.
async fn worker_loop(pool: Arc<WorkerPool>, scheduler: Scheduler) {
    static WORKER_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let wid = WORKER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    loop {
        let Some(mut work) = pool.next_work().await else {
            tracing::debug!(wid, "shutdown");
            break;
        };

        let tid = work.descriptor.id();
        work.descriptor.work_started();

        // Skip execution if transfer already terminal (failed/cancelled by another work item)
        if work.descriptor.is_terminal() {
            tracing::debug!(wid, %tid, work = %work.item.data.debug_label(), "skipped (terminal)");
            pool.complete();
            scheduler.on_completion(work, WorkOutcome::Cancelled);
            continue;
        }

        tracing::debug!(wid, %tid, work = %work.item.data.debug_label(), "executing");
        let transfer = work.descriptor.transfer();

        let token = transfer.cancellation_token().clone();
        let outcome = tokio::select! {
            biased;
            _ = token.cancelled() => WorkOutcome::Cancelled,
            outcome = transfer.execute(&mut work.item) => outcome,
        };

        tracing::debug!(wid, %tid, work = %work.item.data.debug_label(), ?outcome, "completed");

        pool.complete();
        scheduler.on_completion(work, outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::transfer::mock::{FixedWorkCount, WithDelay};
    use crate::scheduler::MockTransfer;
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_single_transfer_completes() {
        let _logs = show_test_logs();
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
    #[ignore = "requires mock with wake support to test cancel mid-flight"]
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

        // Cancel the transfer - returns Some on first call
        assert!(scheduler.cancel_transfer(id).is_some());
        assert!(scheduler.cancel_transfer(id).is_none());

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
