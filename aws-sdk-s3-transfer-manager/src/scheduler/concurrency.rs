/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

/// Controls how many work items can be in-flight concurrently.
///
/// The scheduler calls `target()` before generating work. Work is only
/// generated when total in-flight + pending is below the target.
///
/// `FixedConcurrency` provides a constant target. Future implementations
/// (e.g., `AdaptiveConcurrency`) can adjust the target dynamically based
/// on observed throughput.
pub(crate) trait ConcurrencyController: Send + Sync + fmt::Debug {
    /// Current concurrency target. May change between calls.
    fn target(&self) -> usize;
}

/// Fixed concurrency target that never changes.
#[derive(Debug)]
pub(crate) struct FixedConcurrency(usize);

impl FixedConcurrency {
    pub(crate) fn new(target: usize) -> Self {
        assert!(target > 0, "concurrency target must be at least 1");
        Self(target)
    }
}

impl ConcurrencyController for FixedConcurrency {
    fn target(&self) -> usize {
        self.0
    }
}
