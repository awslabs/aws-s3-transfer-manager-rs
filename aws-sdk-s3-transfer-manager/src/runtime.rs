/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Execution runtime for running IO requests dispatched by the scheduler.

pub(crate) mod scheduler;
pub(crate) mod token_bucket;

mod tokio_mt;
pub(crate) use tokio_mt::TokioMultiThreadRuntime;

use crate::scheduler::descriptor::TransferDescriptor;
use crate::transfer::{IoRequest, TransferId};

/// Work item with scheduler tracking attached.
///
/// Wraps an `IoRequest` with a `TransferDescriptor` that the runtime uses to
/// report execution lifecycle events back to the scheduler. The runtime calls
/// `descriptor.work_started()` when execution begins and the scheduler observes
/// completion in `on_completion`. This lets the scheduler track outstanding work
/// without prescribing when or how the runtime executes it.
#[derive(Debug)]
pub(crate) struct ScheduledWork {
    pub(crate) item: IoRequest,
    pub(crate) descriptor: TransferDescriptor,
}

/// The execution layer that runs IO requests dispatched by the scheduler.
pub(crate) trait ExecutionRuntime: Send + Sync + std::fmt::Debug {
    /// Dispatch an IO request for execution.
    fn dispatch(&self, work: ScheduledWork);

    /// Shut down the runtime, draining in-flight work.
    fn shutdown(&self);

    /// Remove all pending work for a transfer. Returns count removed.
    fn remove_pending_for_transfer(&self, id: TransferId) -> usize;
}
