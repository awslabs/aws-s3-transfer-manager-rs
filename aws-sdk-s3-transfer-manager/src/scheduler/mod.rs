/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod scheduler;
mod transfer;
mod work;

pub(crate) use scheduler::Scheduler;
pub(crate) use transfer::{BoxTransfer, Transfer};
pub(crate) use work::{
    PollWork, TransferId, WorkData, WorkItem, WorkKind, WorkOutcome, WorkerPool,
};

mod descriptor;
mod ready_set;

#[cfg(test)]
pub(crate) use transfer::MockTransfer;

#[cfg(test)]
pub(crate) mod test_util {
    pub(crate) use super::transfer::test_util::*;
}
