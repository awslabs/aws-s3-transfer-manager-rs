/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Execution runtime for running IO requests dispatched by the scheduler.
//!
//! The scheduler decides WHAT to run and WHEN. The runtime decides WHERE and HOW.

mod tokio_mt;
pub(crate) use tokio_mt::TokioMultiThreadRuntime;

use crate::scheduler::descriptor::TransferDescriptor;
use crate::scheduler::work::IoRequest;
use crate::scheduler::TransferId;

/// Work item with scheduler tracking attached.
///
/// Wraps an `IoRequest` (what transfers produce) with the `TransferDescriptor`
/// (scheduler's tracking context). This keeps scheduling concerns out of
/// transfer state machines.
#[derive(Debug)]
pub(crate) struct ScheduledWork {
    pub(crate) item: IoRequest,
    pub(crate) descriptor: TransferDescriptor,
}

/// The execution layer that runs IO requests dispatched by the scheduler.
///
/// The scheduler decides WHAT to run and WHEN. The runtime decides WHERE and HOW.
pub(crate) trait ExecutionRuntime: Send + Sync + std::fmt::Debug {
    /// Dispatch an IO request for execution.
    fn dispatch(&self, work: ScheduledWork);

    /// Shut down the runtime, draining in-flight work.
    fn shutdown(&self);

    /// Remove all pending work for a transfer. Returns count removed.
    fn remove_pending_for_transfer(&self, id: TransferId) -> usize;
}
