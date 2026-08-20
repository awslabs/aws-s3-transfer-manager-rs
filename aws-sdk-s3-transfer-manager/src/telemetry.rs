/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tracing target constants and observability surface for transfer operations.
//!
//! These targets allow filtering logs by concern rather than module path:
//!
//! ```text
//! RUST_LOG=aws_sdk_s3_transfer_manager::concurrency=debug   # adaptive algorithm decisions
//! RUST_LOG=aws_sdk_s3_transfer_manager::scheduling=debug    # scheduler + memory-budget admission
//! RUST_LOG=aws_sdk_s3_transfer_manager::execution=trace     # per-work-item execute/complete
//! RUST_LOG=aws_sdk_s3_transfer_manager::transfer=debug      # transfer lifecycle events
//! RUST_LOG=aws_sdk_s3_transfer_manager::memory=debug        # buffer-pool admission + storage
//! ```

use crate::metrics::latency::LatencyTracker;
use crate::metrics::IOCounters;
use std::sync::Arc;
use std::time::Duration;

/// Adaptive concurrency controller: phase transitions, target changes, probe results.
pub(crate) const TARGET_CONCURRENCY: &str = "aws_sdk_s3_transfer_manager::concurrency";

/// Scheduler capacity decisions, worker pool growth, and memory-budget admission.
pub(crate) const TARGET_SCHEDULING: &str = "aws_sdk_s3_transfer_manager::scheduling";

/// Buffer-pool admission, physical storage, reclamation, and fatal errors.
pub(crate) const TARGET_MEMORY: &str = "aws_sdk_s3_transfer_manager::memory";

/// Per-work-item execution: dispatch, complete, skip, panic.
pub(crate) const TARGET_EXECUTION: &str = "aws_sdk_s3_transfer_manager::execution";

/// Transfer lifecycle: enqueue, complete, cancel, fail, state transitions.
pub(crate) const TARGET_TRANSFER: &str = "aws_sdk_s3_transfer_manager::transfer";

/// Observability surface for transfer operations.
///
/// Groups latency tracking and throughput counters. Transfers record
/// directly; the adaptive concurrency controller reads aggregated views.
pub(crate) struct Telemetry {
    /// Latency tracking for inbound data plane (download range receives); drives
    /// the download time-to-first-byte deadline. There is no send-side tracker:
    /// the upload path carries no adaptive latency deadline.
    pub(crate) recv_latencies: LatencyTracker,
    /// Throughput counters (network tx/rx, disk read/write).
    pub(crate) io_counters: Arc<IOCounters>,
}

impl Telemetry {
    /// Create telemetry with the given window duration for throughput counters.
    pub(crate) fn new(counter_window: Duration) -> Self {
        Self {
            recv_latencies: LatencyTracker::new(),
            io_counters: Arc::new(IOCounters::new(counter_window)),
        }
    }
}

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telemetry")
            .field("recv_latencies", &self.recv_latencies)
            .finish_non_exhaustive()
    }
}
