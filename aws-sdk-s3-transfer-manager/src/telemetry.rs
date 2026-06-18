/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tracing target constants and observability surface for transfer operations.
//!
//! These targets allow filtering logs by concern rather than module path:
//!
//! ```text
//! RUST_LOG=aws_s3_transfer_manager::concurrency=debug   # adaptive algorithm decisions
//! RUST_LOG=aws_s3_transfer_manager::scheduling=debug     # scheduler capacity, worker pool
//! RUST_LOG=aws_s3_transfer_manager::runtime=debug        # runtime: thread/NIC placement, pool state
//! RUST_LOG=aws_s3_transfer_manager::runtime=trace        # + per-work-item dispatch, per-connection
//! RUST_LOG=aws_s3_transfer_manager::transfer=debug       # transfer lifecycle events
//! ```

use crate::metrics::latency::LatencyTracker;
use crate::metrics::IOCounters;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

/// Adaptive concurrency controller: phase transitions, target changes, probe results.
pub(crate) const TARGET_CONCURRENCY: &str = "aws_sdk_s3_transfer_manager::concurrency";

/// Scheduler capacity decisions, worker pool growth.
pub(crate) const TARGET_SCHEDULING: &str = "aws_sdk_s3_transfer_manager::scheduling";

/// Execution runtime layer. DEBUG: thread/core/NIC placement at construction,
/// periodic per-NIC connection-pool snapshot. TRACE: per-work-item dispatch,
/// complete, skip, panic; per-connection lifecycle; per-partition pool detail.
pub(crate) const TARGET_RUNTIME: &str = "aws_sdk_s3_transfer_manager::runtime";

/// Transfer lifecycle: enqueue, complete, cancel, fail, state transitions.
pub(crate) const TARGET_TRANSFER: &str = "aws_sdk_s3_transfer_manager::transfer";

/// Observability surface for transfer operations.
///
/// Groups latency tracking and throughput counters. Transfers record
/// directly; the adaptive concurrency controller reads aggregated views.
pub(crate) struct Telemetry {
    /// Latency tracking for outbound data plane (upload part sends).
    pub(crate) send_latencies: LatencyTracker,
    /// Latency tracking for inbound data plane (download range receives).
    pub(crate) recv_latencies: LatencyTracker,
    /// Throughput counters (network tx/rx, disk read/write).
    pub(crate) io_counters: Arc<IOCounters>,
    /// Download transfers currently unable to fetch ahead because their prefetch
    /// window is full (claimed-but-unconsumed parts at the window limit, blocked
    /// on in-order consumption of a slow head part). A gauge: high values with
    /// low in-flight indicate the pipeline is prefetch-window/head-of-line bound
    /// rather than connection- or work-generation bound.
    pub(crate) window_blocked_downloads: AtomicUsize,
}

impl Telemetry {
    /// Create telemetry with the given window duration for throughput counters.
    pub(crate) fn new(counter_window: Duration) -> Self {
        Self {
            send_latencies: LatencyTracker::new(),
            recv_latencies: LatencyTracker::new(),
            io_counters: Arc::new(IOCounters::new(counter_window)),
            window_blocked_downloads: AtomicUsize::new(0),
        }
    }
}

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telemetry")
            .field("send_latencies", &self.send_latencies)
            .field("recv_latencies", &self.recv_latencies)
            .finish_non_exhaustive()
    }
}
