/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Worker pool for executing work items.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::runtime::ScheduledWork;
use crate::transfer::TransferId;
use tokio::sync::Notify;

/// A queue of work items with in-flight tracking.
#[derive(Debug)]
struct WorkQueue {
    pending: VecDeque<ScheduledWork>,
    in_flight: usize,
}

impl WorkQueue {
    fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            in_flight: 0,
        }
    }

    fn push(&mut self, item: ScheduledWork) {
        self.pending.push_back(item);
    }

    fn pop(&mut self) -> Option<ScheduledWork> {
        self.pending.pop_front()
    }

    fn mark_in_flight(&mut self) {
        self.in_flight += 1;
    }

    fn mark_complete(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    #[allow(dead_code)] // TODO(phase3): runtime observability
    fn pending_count(&self) -> usize {
        self.pending.len()
    }

    #[allow(dead_code)] // TODO(phase3): runtime observability
    fn in_flight_count(&self) -> usize {
        self.in_flight
    }

    /// Remove all pending work for a transfer. Returns count removed.
    fn remove_for_transfer(&mut self, id: TransferId) -> usize {
        let before = self.pending.len();
        self.pending.retain(|work| work.descriptor.id() != id);
        before - self.pending.len()
    }
}

/// A pool of workers that pull work from a queue.
#[derive(Debug)]
pub(super) struct WorkerPool {
    queue: Mutex<WorkQueue>,
    work_available: Notify,
    shutdown: AtomicBool,
    started: AtomicBool,
}

impl WorkerPool {
    pub(super) fn new() -> Self {
        Self {
            queue: Mutex::new(WorkQueue::new()),
            work_available: Notify::new(),
            shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
        }
    }

    /// Push work to this pool's queue.
    pub(super) fn push(&self, work: ScheduledWork) {
        self.queue.lock().unwrap().push(work);
        self.work_available.notify_one();
    }

    /// Pull next work item. Returns None on shutdown.
    pub(super) async fn next_work(&self) -> Option<ScheduledWork> {
        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            {
                let mut queue = self.queue.lock().unwrap();
                if let Some(work) = queue.pop() {
                    queue.mark_in_flight();
                    return Some(work);
                }
            }
            self.work_available.notified().await;
        }
    }

    /// Mark work complete, freeing capacity.
    pub(super) fn complete(&self) {
        self.queue.lock().unwrap().mark_complete();
        // Notify in case a worker is waiting for capacity
        self.work_available.notify_one();
    }

    /// Signal shutdown. Workers will exit after current work.
    #[allow(dead_code)] // TODO(phase3): scheduler observability + lifecycle
    pub(super) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake all waiting workers so they can exit
        self.work_available.notify_waiters();
    }

    /// Mark workers as started. Returns true if this call started them (was first).
    pub(super) fn mark_started(&self) -> bool {
        self.started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Number of pending work items.
    #[allow(dead_code)] // TODO(phase3): runtime observability
    pub(super) fn pending_count(&self) -> usize {
        self.queue.lock().unwrap().pending_count()
    }

    /// Number of in-flight work items.
    #[allow(dead_code)] // TODO(phase3): runtime observability
    pub(super) fn in_flight_count(&self) -> usize {
        self.queue.lock().unwrap().in_flight_count()
    }

    /// Remove all pending work for a transfer. Returns count removed.
    pub(super) fn remove_for_transfer(&self, id: TransferId) -> usize {
        self.queue.lock().unwrap().remove_for_transfer(id)
    }
}
