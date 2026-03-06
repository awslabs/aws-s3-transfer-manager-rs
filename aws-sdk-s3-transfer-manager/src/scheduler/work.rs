/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Work abstraction for the scheduler.

mod item;
mod pool;
mod queue;

pub(crate) use item::{IoKind, IoRequest, PollWork, TransferId, WorkOutcome};
pub(crate) use pool::WorkerPool;
use queue::WorkQueue;

use super::descriptor::TransferDescriptor;

/// Work item with scheduler tracking attached.
///
/// Wraps an `IoRequest` (what transfers produce) with the `TransferDescriptor`
/// (scheduler's tracking context). This keeps scheduling concerns out of
/// transfer state machines.
#[derive(Debug)]
pub(super) struct ScheduledWork {
    pub(super) item: IoRequest,
    pub(super) descriptor: TransferDescriptor,
}
