/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;

use super::{TransferId, WorkItem};

/// A queue of work items with concurrency control
#[derive(Debug)]
pub(crate) struct WorkQueue {
    pending: VecDeque<WorkItem>,
    in_flight: usize,
    // TODO(Phase 5): adaptive concurrency will require additional context
    // (e.g. NetworkPermitContext, latency/throughput samples) and a ConcurrencyController trait
    concurrency: usize,
}

impl WorkQueue {
    pub(crate) fn new(concurrency: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            in_flight: 0,
            concurrency,
        }
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.in_flight < self.concurrency
    }

    pub(crate) fn push(&mut self, item: WorkItem) {
        self.pending.push_back(item);
    }

    pub(crate) fn pop(&mut self) -> Option<WorkItem> {
        self.pending.pop_front()
    }

    pub(crate) fn mark_in_flight(&mut self) {
        self.in_flight += 1;
    }

    pub(crate) fn mark_complete(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    pub(crate) fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn in_flight_count(&self) -> usize {
        self.in_flight
    }

    /// Remove all pending work for a transfer. Returns count removed.
    pub(crate) fn remove_for_transfer(&mut self, id: TransferId) -> usize {
        let before = self.pending.len();
        self.pending.retain(|item| item.transfer_id != id);
        before - self.pending.len()
    }
}
