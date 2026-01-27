/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Work abstraction for the scheduler.
//!
//! This module contains:
//! - `WorkItem`, `WorkData`, `WorkKind` - the work unit abstraction
//! - `WorkQueue` - a queue with concurrency control
//! - `WorkerPool` - a pool of workers that pull work from a queue

mod item;
mod pool;
mod queue;

pub(crate) use item::{PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome};
pub(crate) use pool::WorkerPool;
pub(crate) use queue::WorkQueue;
