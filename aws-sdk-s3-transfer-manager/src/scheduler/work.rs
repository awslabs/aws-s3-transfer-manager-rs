/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Work abstraction for the scheduler.

mod item;
mod pool;
mod queue;

pub(crate) use item::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};
pub(crate) use pool::WorkerPool;
use queue::WorkQueue;

use super::descriptor::TransferDescriptor;

/// Work item with scheduler tracking attached.
///
/// Wraps a `WorkItem` (what transfers produce) with the `TransferDescriptor`
/// (scheduler's tracking context). This keeps scheduling concerns out of
/// transfer state machines.
#[derive(Debug)]
pub(crate) struct ScheduledWork {
    pub(crate) item: WorkItem,
    pub(crate) descriptor: TransferDescriptor,
}
