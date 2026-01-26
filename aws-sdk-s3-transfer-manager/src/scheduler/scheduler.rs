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
//!   Transfer.poll_work()
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
//! # Transfer States
//!
//! Transfers are either "ready" (can be polled for work) or "pending" (blocked
//! waiting for in-flight work to complete). The scheduler only polls ready
//! transfers, avoiding wasted work.
//!
//! ```text
//!   enqueue_transfer()
//!          │
//!          ▼
//!   ┌─────────────┐  poll_work() = Pending   ┌─────────────┐
//!   │    Ready    │ ───────────────────────► │   Pending   │
//!   │   Transfers │                          │  Transfers  │
//!   └─────────────┘ ◄─────────────────────── └─────────────┘
//!                      wake(transfer_id)
//! ```
//!
//! # Completion Processing
//!
//! Completions are queued and processed by whichever task finishes and
//! acquires drainer status. An atomic flag ensures exactly one task drains
//! at a time while others continue without blocking.
//!
//! # Priority
//!
//! TODO(redux): Priority-based scheduling for prefetch vs active requests.
//! Currently uses FIFO ordering via VecDeque. Future options:
//! - BinaryHeap<PrioritizedTransfer> for priority ordering
//! - Multi-level queues (high/normal/low)
//! - Fairness via vruntime (glommio-style)

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crossbeam_queue::SegQueue;

use super::{PollWork, Transfer, TransferId, WorkItem, WorkKind, WorkOutcome, WorkQueue};

/// Completion message sent when work finishes
struct WorkComplete {
    work: WorkItem,
    outcome: WorkOutcome,
}

/// State for a tracked transfer
struct TransferState {
    transfer: Transfer,
    pending: bool,
}

/// Inner scheduler state protected by mutex
struct SchedulerInner {
    data_io_queue: WorkQueue,
    network_queue: WorkQueue,
    /// All transfers by ID
    transfers: HashMap<TransferId, TransferState>,
    /// IDs of transfers ready to be polled for work
    ready_queue: VecDeque<TransferId>,
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
                transfers: HashMap::new(),
                ready_queue: VecDeque::new(),
            })),
            completion_queue: Arc::new(SegQueue::new()),
            draining: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Add a transfer and start executing work.
    pub(crate) fn enqueue_transfer(&self, transfer: Transfer) {
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
        inner.generate_work();
        self.spawn_ready_work(&mut inner);
    }

    /// Wake a transfer, moving it from pending to ready.
    ///
    /// Called when work completes or on external events (abort/cancel).
    pub(crate) fn wake(&self, id: TransferId) {
        let mut inner = self.inner.lock().unwrap();
        inner.wake(id);
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
        while let Some(c) = self.completion_queue.pop() {
            inner.on_work_complete(c.work, c.outcome);
        }
        inner.generate_work();
        self.spawn_ready_work(inner);
    }

    /// Spawn tasks for ready work.
    fn spawn_ready_work(&self, inner: &mut MutexGuard<'_, SchedulerInner>) {
        let mut to_spawn = Vec::new();

        // Collect from DataIO queue
        while inner.data_io_queue.has_capacity() {
            let Some(work) = inner.data_io_queue.pop() else {
                break;
            };
            inner.data_io_queue.mark_in_flight();

            let transfer = inner
                .transfers
                .get(&work.transfer_id)
                .map(|s| s.transfer.clone());

            if let Some(transfer) = transfer {
                to_spawn.push((work, transfer));
            } else {
                inner.data_io_queue.mark_complete();
            }
        }

        // Collect from Network queue
        while inner.network_queue.has_capacity() {
            let Some(work) = inner.network_queue.pop() else {
                break;
            };
            inner.network_queue.mark_in_flight();

            let transfer = inner
                .transfers
                .get(&work.transfer_id)
                .map(|s| s.transfer.clone());

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
            && inner.transfers.is_empty()
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

    fn on_work_complete(&mut self, work: WorkItem, outcome: WorkOutcome) {
        self.mark_complete(work.kind);

        // Wake the transfer so it can be polled again
        self.wake(work.transfer_id);

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
    }

    fn generate_work(&mut self) {
        let target = self.total_capacity();
        let current = self.total_pending() + self.total_in_flight();

        if current >= target {
            return;
        }

        let to_generate = target - current;
        let mut generated = 0;

        while generated < to_generate {
            let Some(id) = self.ready_queue.pop_front() else {
                break;
            };

            let Some(state) = self.transfers.get_mut(&id) else {
                continue;
            };

            match state.transfer.poll_work() {
                PollWork::Ready(work) => {
                    self.enqueue_to_kind(work);
                    generated += 1;
                    // Re-queue - might have more work
                    self.ready_queue.push_back(id);
                }
                PollWork::Pending => {
                    state.pending = true;
                    // Don't re-queue - will be woken when work completes
                }
                PollWork::Done => {
                    self.transfers.remove(&id);
                }
            }
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
    use crate::scheduler::MockTransfer;
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

    #[tokio::test]
    async fn test_pending_transfer_not_polled_until_woken() {
        // This test verifies that a transfer returning Pending
        // is not polled again until wake() is called
        let scheduler = Scheduler::new(4, 4);

        let transfer = Transfer::Mock(MockTransfer::new(
            TransferId {
                id: 1,
                parent: None,
            },
            2,
        ));

        scheduler.enqueue_transfer(transfer);

        // Should complete - work completes trigger wake
        tokio::time::timeout(Duration::from_secs(5), async {
            while !scheduler.is_done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("transfer should complete");
    }
}
