/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Worker pool for executing work items.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use super::{ScheduledWork, WorkQueue};
use crate::scheduler::TransferId;
use tokio::sync::Notify;

/// A pool of workers that pull work from a queue.
#[derive(Debug)]
pub(crate) struct WorkerPool {
    queue: Mutex<WorkQueue>,
    work_available: Notify,
    shutdown: AtomicBool,
    started: AtomicBool,
}

impl WorkerPool {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(WorkQueue::new()),
            work_available: Notify::new(),
            shutdown: AtomicBool::new(false),
            started: AtomicBool::new(false),
        }
    }

    /// Push work to this pool's queue.
    pub(in crate::scheduler) fn push(&self, work: ScheduledWork) {
        self.queue.lock().unwrap().push(work);
        self.work_available.notify_one();
    }

    /// Pull next work item. Returns None on shutdown.
    pub(in crate::scheduler) async fn next_work(&self) -> Option<ScheduledWork> {
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
    pub(crate) fn complete(&self) {
        self.queue.lock().unwrap().mark_complete();
        // Notify in case a worker is waiting for capacity
        self.work_available.notify_one();
    }

    /// Signal shutdown. Workers will exit after current work.
    #[allow(dead_code)] // TODO(phase3): scheduler observability + lifecycle
    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake all waiting workers so they can exit
        self.work_available.notify_waiters();
    }

    /// Check if workers have been started.
    #[allow(dead_code)] // TODO(phase3): scheduler observability + lifecycle
    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// Mark workers as started. Returns true if this call started them (was first).
    pub(crate) fn mark_started(&self) -> bool {
        self.started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Number of pending work items.
    #[allow(dead_code)] // TODO(phase3): scheduler observability + lifecycle
    pub(crate) fn pending_count(&self) -> usize {
        self.queue.lock().unwrap().pending_count()
    }

    /// Number of in-flight work items.
    #[allow(dead_code)] // TODO(phase3): scheduler observability + lifecycle
    pub(crate) fn in_flight_count(&self) -> usize {
        self.queue.lock().unwrap().in_flight_count()
    }

    /// Remove all pending work for a transfer. Returns count removed.
    pub(crate) fn remove_for_transfer(&self, id: TransferId) -> usize {
        self.queue.lock().unwrap().remove_for_transfer(id)
    }
}
