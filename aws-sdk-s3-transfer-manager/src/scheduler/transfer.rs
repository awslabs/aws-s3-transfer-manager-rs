/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types for scheduler integration.

use std::future::Future;
use std::pin::Pin;

use super::context::TransferContext;
use super::{PollWork, WorkOutcome};
use crate::scheduler::WorkItem;

/// A transfer operation that generates and executes work.
///
/// The scheduler's uniform interface to transfer operations (upload, download).
/// Each transfer is a state machine that the scheduler polls for work. The scheduler
/// is agnostic to what a transfer does -- it only knows about priority, capacity,
/// and the ready/pending/done lifecycle.
///
/// Upload, download, and multi-object variants all implement this trait.
pub(crate) trait Transfer: Send + Sync + std::fmt::Debug {
    /// Returns the transfer's context.
    ///
    /// Provides access to the handle (config, scheduler), status tracking, and cancellation.
    fn ctx(&self) -> &TransferContext;

    /// Called by the scheduler when capacity is available and the transfer is in the ready set.
    ///
    /// Returns `Ready(work)` with a work item to execute, `Pending` if the transfer is blocked
    /// (e.g. waiting for a completion or resource), or `Done` when the transfer has no more work.
    /// The scheduler never calls this on a transfer that returned `Pending` until it is explicitly woken.
    fn poll_work(&self) -> PollWork;

    /// Executes a work item produced by `poll_work()`.
    ///
    /// Returns `Success`, `Failed`, or `Cancelled`. Called by worker threads,
    /// not the scheduler's generate_work loop.
    fn execute<'a>(
        &'a self,
        work: &'a mut WorkItem,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;
}

pub(crate) type BoxTransfer = Box<dyn Transfer>;

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(crate) mod test_util;
