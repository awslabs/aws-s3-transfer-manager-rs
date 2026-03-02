/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::time::Duration;

use super::WorkKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    /// S3 throttling (503, 429, SlowDown). Triggers immediate backoff.
    Throttle,
    /// Transient errors (timeout, connection reset). Noise for the controller.
    Transient,
    /// Permanent errors (404, 403). Transfer-level concern, not controller.
    Permanent,
}

/// I/O throughput metrics reported by a completed work item.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IoMetrics {
    /// Bytes read from disk (upload source)
    pub bytes_read: u64,
    /// Bytes written to disk (download sink)
    pub bytes_written: u64,
    /// Bytes sent over network (upload)
    pub bytes_sent: u64,
    /// Bytes received from network (download)
    pub bytes_received: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CompletionSample {
    pub metrics: IoMetrics,
    pub duration: Duration,
    pub error: Option<ErrorKind>,
    pub kind: WorkKind,
}

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
    fn on_completion(&self, _sample: &CompletionSample) {}
}

/// Fixed concurrency target that never changes.
#[derive(Debug)]
pub(crate) struct FixedConcurrency(usize);

impl FixedConcurrency {
    pub(crate) fn new(target: usize) -> Self {
        Self(target)
    }
}

impl ConcurrencyController for FixedConcurrency {
    fn target(&self) -> usize {
        self.0
    }
}
