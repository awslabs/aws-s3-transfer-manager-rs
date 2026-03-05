/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;

use super::{ScheduledWork, TransferId};

/// A queue of work items with in-flight tracking
#[derive(Debug)]
pub(super) struct WorkQueue {
    pending: VecDeque<ScheduledWork>,
    in_flight: usize,
}

impl WorkQueue {
    pub(super) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            in_flight: 0,
        }
    }

    pub(super) fn push(&mut self, item: ScheduledWork) {
        self.pending.push_back(item);
    }

    pub(super) fn pop(&mut self) -> Option<ScheduledWork> {
        self.pending.pop_front()
    }

    /// Record that a dequeued item is now executing. Separate from `pop` because
    /// work may be staged (batched, routed) between dequeue and execution.
    pub(super) fn mark_in_flight(&mut self) {
        self.in_flight += 1;
    }

    pub(super) fn mark_complete(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    pub(super) fn pending_count(&self) -> usize {
        self.pending.len()
    }

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
