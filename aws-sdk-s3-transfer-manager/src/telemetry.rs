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
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
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
    /// Live counts of download transfers currently parked, by cause. See
    /// [`DownloadBackpressure`].
    pub(crate) download_backpressure: DownloadBackpressure,
}

impl Telemetry {
    /// Create telemetry with the given window duration for throughput counters.
    pub(crate) fn new(counter_window: Duration) -> Self {
        Self {
            send_latencies: LatencyTracker::new(),
            recv_latencies: LatencyTracker::new(),
            io_counters: Arc::new(IOCounters::new(counter_window)),
            download_backpressure: DownloadBackpressure::default(),
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

/// Why a download transfer is currently parked. Mutually exclusive: the
/// claim-first gate claims the slot (the prefetch-window gate) and only then
/// reserves memory, so a transfer is parked on one or the other, never both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackpressureSource {
    /// Prefetch window full: claimed-but-unconsumed parts at the limit, waiting
    /// on in-order consumption of a slow head part (head-of-line).
    PrefetchWindow,
    /// Global memory budget exhausted: a slot was claimed but no chunk could be
    /// reserved.
    MemoryBudget,
}

impl BackpressureSource {
    // Encoding for BackpressureState's atomic; 0 means not parked.
    fn as_u8(self) -> u8 {
        match self {
            BackpressureSource::PrefetchWindow => 1,
            BackpressureSource::MemoryBudget => 2,
        }
    }

    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(BackpressureSource::PrefetchWindow),
            2 => Some(BackpressureSource::MemoryBudget),
            _ => None,
        }
    }
}

/// Edge-tracked backpressure state for one download transfer: at most one
/// [`BackpressureSource`], or none, encoded in a `u8` (0 = running).
#[derive(Debug, Default)]
pub(crate) struct BackpressureState(AtomicU8);

impl BackpressureState {
    /// Swap to `next`, returning the previous state.
    pub(crate) fn swap(&self, next: Option<BackpressureSource>) -> Option<BackpressureSource> {
        let encoded = next.map_or(0, BackpressureSource::as_u8);
        BackpressureSource::from_u8(self.0.swap(encoded, Ordering::Relaxed))
    }

    /// The current state.
    pub(crate) fn load(&self) -> Option<BackpressureSource> {
        BackpressureSource::from_u8(self.0.load(Ordering::Relaxed))
    }
}

/// Live counts of download transfers parked on each cause; per transfer the two
/// are mutually exclusive. High `window` with low in-flight means the pipeline
/// is prefetch-window / head-of-line bound (raise the window, or the consumer is
/// slow); high `budget` means it is global-memory-budget bound (raise the
/// budget, or the machine is RAM-bound).
#[derive(Debug, Default)]
pub(crate) struct DownloadBackpressure {
    /// Parked because the prefetch window is full.
    pub(crate) window: AtomicUsize,
    /// Parked because the global memory budget is exhausted.
    pub(crate) budget: AtomicUsize,
}

impl DownloadBackpressure {
    /// Record one more transfer parked on `source`; returns the new count.
    pub(crate) fn increment(&self, source: BackpressureSource) -> usize {
        self.count(source).fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Record one fewer transfer parked on `source`.
    pub(crate) fn decrement(&self, source: BackpressureSource) {
        self.count(source).fetch_sub(1, Ordering::Relaxed);
    }

    fn count(&self, source: BackpressureSource) -> &AtomicUsize {
        match source {
            BackpressureSource::PrefetchWindow => &self.window,
            BackpressureSource::MemoryBudget => &self.budget,
        }
    }
}
