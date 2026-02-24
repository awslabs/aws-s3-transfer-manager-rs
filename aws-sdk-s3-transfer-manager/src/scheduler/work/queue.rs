/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;

use super::{ScheduledWork, TransferId};

/// A queue of work items with concurrency control
#[derive(Debug)]
pub(super) struct WorkQueue {
    pending: VecDeque<ScheduledWork>,
    in_flight: usize,
    concurrency: usize,
}

impl WorkQueue {
    pub(super) fn new(concurrency: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            in_flight: 0,
            concurrency,
        }
    }

    pub(super) fn has_capacity(&self) -> bool {
        self.pending.len() + self.in_flight < self.concurrency
    }

    pub(super) fn push(&mut self, item: ScheduledWork) {
        self.pending.push_back(item);
    }

    pub(super) fn pop(&mut self) -> Option<ScheduledWork> {
        self.pending.pop_front()
    }

    pub(super) fn mark_in_flight(&mut self) {
        self.in_flight += 1;
    }

    pub(super) fn mark_complete(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    #[allow(dead_code)] // TODO(phase3): backing for WorkerPool
    pub(super) fn concurrency(&self) -> usize {
        self.concurrency
    }

    #[allow(dead_code)] // TODO(phase3): backing for WorkerPool
    pub(super) fn pending_count(&self) -> usize {
        self.pending.len()
    }

    #[allow(dead_code)] // TODO(phase3): backing for WorkerPool
    pub(super) fn in_flight_count(&self) -> usize {
        self.in_flight
    }

    /// Remove all pending work for a transfer. Returns count removed.
    pub(super) fn remove_for_transfer(&mut self, id: TransferId) -> usize {
        let before = self.pending.len();
        self.pending.retain(|work| work.descriptor.id() != id);
        before - self.pending.len()
    }
}
