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
//! # Work Flow
//!
//! Work progresses through phases. For uploads: DataIO (read) → Network (send).
//! For downloads: Network (receive) → DataIO (write).
//!
//! ```text
//!   Transfer.next_work()
//!          │
//!          ▼
//!   ┌──────────────┐                  ┌─────────────┐
//!   │   DataIO     │    on_complete   │   Network   │
//!   │    Queue     │ ───────────────► │    Queue    │
//!   └──────────────┘                  └──────┬──────┘
//!                                            │ on_complete
//!                                            ▼
//!                                     Transfer complete
//! ```
//!
//! # Task Lifecycle
//!
//! 1. Transfer enqueued → scheduler generates work items
//! 2. Work items placed in appropriate phase queue (DataIO or Network)
//! 3. Tasks spawned up to concurrency limit for each queue
//! 4. Task completes → triggers phase transition or transfer completion
//! 5. More work generated and spawned as capacity allows
//!
//! No background loop - completions drive the next scheduling cycle.
//!
//! # Completion Processing
//!
//! Completions are queued and processed by whichever task finishes and
//! acquires drainer status. An atomic flag ensures exactly one task drains
//! at a time while others continue without blocking.
//!
//! ```text
//!   Task A completes    Task B completes    Task C completes
//!         │                   │                   │
//!         ▼                   ▼                   ▼
//!   ┌─────────────────────────────────────────────────┐
//!   │              Completion Queue                   │
//!   └─────────────────────────────────────────────────┘
//!                          │
//!                          ▼
//!              CAS(draining: false → true)
//!                          │
//!              ┌───────────┴───────────┐
//!              ▼                       ▼
//!         CAS succeeded            CAS failed
//!              │                       │
//!              ▼                       ▼
//!       drain, spawn,           (drainer will
//!       release, re-check        process ours)
//! ```
//!
//! # Priority
//!
//! TODO(Phase 5): Priority-based scheduling for prefetch vs active requests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_queue::SegQueue;

use super::{Transfer, WorkItem, WorkKind, WorkOutcome, WorkQueue};

/// Completion message sent when work finishes
struct WorkComplete {
    work: WorkItem,
    outcome: WorkOutcome,
}

/// Inner scheduler state protected by mutex
struct SchedulerInner {
    data_io_queue: WorkQueue,
    network_queue: WorkQueue,
    active_transfers: Vec<Transfer>,
}

/// Event-driven scheduler for coordinating transfer work.
///
/// Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    completion_queue: Arc<SegQueue<WorkComplete>>,
    draining: Arc<AtomicBool>,
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
                data_io_queue: WorkQueue::new(data_io_concurrency),
                network_queue: WorkQueue::new(network_concurrency),
                active_transfers: Vec::new(),
            })),
            completion_queue: Arc::new(SegQueue::new()),
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Add a transfer and start executing work.
    pub(crate) fn enqueue_transfer(&self, transfer: Transfer) {
        let mut inner = self.inner.lock().unwrap();
        inner.active_transfers.push(transfer);
        inner.generate_work();
        self.spawn_ready_work(&mut inner);
    }

    /// Called by spawned tasks when work completes.
    ///
    /// Queues the completion and attempts to become the drainer. If another
    /// task is already draining, this returns immediately - the drainer will
    /// process our completion.
    pub(crate) fn complete_work(&self, work: WorkItem, outcome: WorkOutcome) {
        self.completion_queue.push(WorkComplete { work, outcome });

        // Try to become the drainer
        if self
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // We're the drainer
            let mut inner = self.inner.lock().unwrap();
            loop {
                self.drain_completions(&mut inner);
                self.draining.store(false, Ordering::Release);

                // Re-check queue - items may have arrived while draining
                if self.completion_queue.is_empty() {
                    break;
                }
                // Try to re-acquire drainer status
                if self
                    .draining
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    break; // Someone else took over
                }
            }
        }
        // If CAS failed, another task is draining and will process our completion
    }

    /// Drain all pending completions and process them.
    fn drain_completions(&self, inner: &mut MutexGuard<'_, SchedulerInner>) {
        // Drain until empty
        // Note: new completions may arrive while we drain, that's fine -
        // we'll get them in this pass or the next drainer will
        while let Some(c) = self.completion_queue.pop() {
            inner.on_work_complete(c.work, c.outcome);
        }
        self.spawn_ready_work(inner);
    }

    /// Spawn tasks for ready work.
    ///
    /// Called while holding the lock. Spawning itself is cheap (just queues to runtime).
    fn spawn_ready_work(&self, inner: &mut MutexGuard<'_, SchedulerInner>) {
        // Collect work to spawn - need to do this in two passes to avoid borrow issues
        let mut to_spawn = Vec::new();

        // Pass 1: collect from DataIO queue
        while inner.data_io_queue.has_capacity() {
            let Some(work) = inner.data_io_queue.pop() else {
                break;
            };
            inner.data_io_queue.mark_in_flight();

            let transfer = inner
                .active_transfers
                .iter()
                .find(|t| t.id() == work.transfer_id)
                .cloned();

            if let Some(transfer) = transfer {
                to_spawn.push((work, transfer));
            } else {
                inner.data_io_queue.mark_complete();
            }
        }

        // Pass 2: collect from Network queue
        while inner.network_queue.has_capacity() {
            let Some(work) = inner.network_queue.pop() else {
                break;
            };
            inner.network_queue.mark_in_flight();

            let transfer = inner
                .active_transfers
                .iter()
                .find(|t| t.id() == work.transfer_id)
                .cloned();

            if let Some(transfer) = transfer {
                to_spawn.push((work, transfer));
            } else {
                inner.network_queue.mark_complete();
            }
        }

        // Spawn tasks
        for (mut work, transfer) in to_spawn {
            let scheduler = self.clone();
            tokio::spawn(async move {
                let outcome = transfer.execute(&mut work).await;
                scheduler.complete_work(work, outcome);
            });
        }
    }

    /// Check if all work is done.
    pub(crate) fn is_done(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.data_io_queue.pending_count() == 0
            && inner.data_io_queue.in_flight_count() == 0
            && inner.network_queue.pending_count() == 0
            && inner.network_queue.in_flight_count() == 0
            && inner.active_transfers.iter().all(|t| t.is_done())
    }
}

impl SchedulerInner {
    fn on_work_complete(&mut self, work: WorkItem, outcome: WorkOutcome) {
        self.mark_complete(work.kind);
        match outcome {
            WorkOutcome::Success {
                schedule_next,
                data,
            } => {
                if let Some(kind) = schedule_next {
                    let next_item = WorkItem {
                        transfer_id: work.transfer_id,
                        kind,
                        data,
                    };
                    self.enqueue_to_kind(next_item);
                }
            }
            WorkOutcome::Failed { .. } | WorkOutcome::Cancelled => {
                // TODO(Phase 5): handle failure/cancellation
            }
        }
        self.generate_work();
    }

    fn generate_work(&mut self) {
        let target = self.total_capacity();
        let current = self.total_pending() + self.total_in_flight();

        if current >= target {
            return;
        }

        let to_generate = target - current;
        let mut generated = 0;
        let mut new_work = Vec::new();

        while generated < to_generate {
            let mut made_progress = false;
            for transfer in &self.active_transfers {
                if let Some(work) = transfer.next_work() {
                    new_work.push(work);
                    generated += 1;
                    made_progress = true;
                    if generated >= to_generate {
                        break;
                    }
                }
            }
            if !made_progress {
                break;
            }
        }

        for item in new_work {
            self.enqueue_to_kind(item);
        }
    }

    fn enqueue_to_kind(&mut self, item: WorkItem) {
        match item.kind {
            WorkKind::DataIO => self.data_io_queue.push(item),
            WorkKind::Network => self.network_queue.push(item),
        }
    }

    fn total_capacity(&self) -> usize {
        self.data_io_queue.concurrency() + self.network_queue.concurrency()
    }

    fn total_pending(&self) -> usize {
        self.data_io_queue.pending_count() + self.network_queue.pending_count()
    }

    fn total_in_flight(&self) -> usize {
        self.data_io_queue.in_flight_count() + self.network_queue.in_flight_count()
    }

    fn mark_complete(&mut self, kind: WorkKind) {
        match kind {
            WorkKind::DataIO => self.data_io_queue.mark_complete(),
            WorkKind::Network => self.network_queue.mark_complete(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{MockTransfer, TransferId};
    use std::time::Duration;

    #[tokio::test]
    async fn test_single_transfer_completes() {
        let scheduler = Scheduler::new(4, 4);

        let transfer = Transfer::Mock(MockTransfer::new(
            TransferId {
                id: 1,
                parent: None,
            },
            2,
        ));

        scheduler.enqueue_transfer(transfer);

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transfer should complete");

        assert!(scheduler.is_done());
    }

    #[tokio::test]
    async fn test_multiple_transfers_complete() {
        let scheduler = Scheduler::new(4, 4);

        for i in 0..5 {
            let transfer = Transfer::Mock(MockTransfer::new(
                TransferId {
                    id: i,
                    parent: None,
                },
                3,
            ));
            scheduler.enqueue_transfer(transfer);
        }

        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all transfers should complete");

        assert!(scheduler.is_done());
    }
}
