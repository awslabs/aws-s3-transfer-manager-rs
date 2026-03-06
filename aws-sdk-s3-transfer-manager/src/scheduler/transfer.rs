/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transfer types for scheduler integration.

use std::future::Future;
use std::pin::Pin;

use super::{PollWork, WorkOutcome};
use crate::operation::TransferContext;
use crate::scheduler::IoRequest;

/// A transfer operation that generates and executes work.
pub(crate) trait Transfer: Send + Sync + std::fmt::Debug {
    fn ctx(&self) -> &TransferContext;
    fn poll_work(&self) -> PollWork;
    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>>;
}

pub(crate) type BoxTransfer = Box<dyn Transfer>;

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(crate) mod test_util;

#[cfg(test)]
pub(crate) use mock::MockTransfer;
