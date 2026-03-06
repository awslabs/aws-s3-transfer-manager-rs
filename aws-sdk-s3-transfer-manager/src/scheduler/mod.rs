/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod concurrency;
mod scheduler;
mod transfer;
pub(crate) mod work;

pub(crate) use concurrency::{
    classify_error, AdaptiveConcurrencyController, AdaptiveConfig, CompletionSample,
    ConcurrencyController, FixedConcurrency,
};
pub(crate) use scheduler::{Scheduler, SchedulerBuilder};
pub(crate) use transfer::{BoxTransfer, Transfer};
pub(crate) use work::{IoKind, IoRequest, PollWork, TransferId, WorkOutcome};

pub(crate) mod descriptor;
mod ready_set;

#[cfg(test)]
pub(crate) use transfer::MockTransfer;

#[cfg(test)]
pub(crate) mod test_util {
    pub(crate) use super::transfer::test_util::*;
}
