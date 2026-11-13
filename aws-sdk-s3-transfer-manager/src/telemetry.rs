/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tracing target constants and observability surface for transfer operations.
//!
//! These targets allow filtering logs by concern rather than module path:
//!
//! ```text
//! RUST_LOG=aws_sdk_s3_transfer_manager::concurrency=debug   # concurrency resolution and control
//! RUST_LOG=aws_sdk_s3_transfer_manager::scheduling=debug    # scheduler + memory-budget admission
//! RUST_LOG=aws_sdk_s3_transfer_manager::execution=trace     # per-work-item execute/complete
//! RUST_LOG=aws_sdk_s3_transfer_manager::transfer=debug      # transfer lifecycle events
//! RUST_LOG=aws_sdk_s3_transfer_manager::memory=debug        # buffer-pool admission + storage
//! ```
//!
//! ## Transfer targets
//!
//! Each transfer operation logs under its own target below `transfer`:
//!
//! ```text
//! aws_sdk_s3_transfer_manager::transfer                    # shared lifecycle: pending, wake, retry
//! aws_sdk_s3_transfer_manager::transfer::upload            # upload
//! aws_sdk_s3_transfer_manager::transfer::download          # download
//! aws_sdk_s3_transfer_manager::transfer::upload_objects    # upload_objects parent
//! aws_sdk_s3_transfer_manager::transfer::download_objects  # download_objects parent
//! ```
//!
//! A directive matches its target and every target that starts with it, so
//! `transfer=debug` enables all transfer records. `transfer::upload` also
//! matches `transfer::upload_objects`, and `transfer::download` also matches
//! `transfer::download_objects`. The longest matching directive wins, so a
//! second directive sets the composite parent to a different level:
//!
//! ```text
//! RUST_LOG=aws_sdk_s3_transfer_manager::transfer::upload=debug,aws_sdk_s3_transfer_manager::transfer::upload_objects=warn
//! ```
//!
//! Each child of a composite transfer is an ordinary upload or download and
//! logs under `transfer::upload` or `transfer::download`, not under its
//! parent's target.

use crate::metrics::latency::LatencyTracker;
use crate::metrics::IOCounters;
use std::sync::Arc;
use std::time::Duration;

/// Concurrency resolution and control: selected targets, transitions, and probe results.
pub(crate) const TARGET_CONCURRENCY: &str = "aws_sdk_s3_transfer_manager::concurrency";

/// Scheduler capacity decisions, worker pool growth, and memory-budget admission.
pub(crate) const TARGET_SCHEDULING: &str = "aws_sdk_s3_transfer_manager::scheduling";

/// Buffer-pool admission, physical storage, reclamation, and fatal errors.
pub(crate) const TARGET_MEMORY: &str = "aws_sdk_s3_transfer_manager::memory";

/// Per-work-item execution: dispatch, complete, skip, panic.
pub(crate) const TARGET_EXECUTION: &str = "aws_sdk_s3_transfer_manager::execution";

/// Transfer lifecycle: enqueue, complete, cancel, fail, state transitions.
pub(crate) const TARGET_TRANSFER: &str = "aws_sdk_s3_transfer_manager::transfer";

/// Upload state machine: events, request records, and terminal summary.
pub(crate) const TARGET_TRANSFER_UPLOAD: &str = "aws_sdk_s3_transfer_manager::transfer::upload";

/// Download state machine: events, request records, and terminal summary.
pub(crate) const TARGET_TRANSFER_DOWNLOAD: &str = "aws_sdk_s3_transfer_manager::transfer::download";

/// `upload_objects` parent: events, walker records, and terminal summary.
pub(crate) const TARGET_TRANSFER_UPLOAD_OBJECTS: &str =
    "aws_sdk_s3_transfer_manager::transfer::upload_objects";

/// `download_objects` parent: events, walker records, and terminal summary.
pub(crate) const TARGET_TRANSFER_DOWNLOAD_OBJECTS: &str =
    "aws_sdk_s3_transfer_manager::transfer::download_objects";

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

#[cfg(test)]
mod tests {
    use super::{
        TARGET_TRANSFER, TARGET_TRANSFER_DOWNLOAD, TARGET_TRANSFER_DOWNLOAD_OBJECTS,
        TARGET_TRANSFER_UPLOAD, TARGET_TRANSFER_UPLOAD_OBJECTS,
    };
    use tracing::Level;
    use tracing_subscriber::EnvFilter;

    // Runs `check` with a subscriber filtered by `directives`.
    fn with_filter(directives: &str, check: impl FnOnce()) {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new(directives))
            .with_writer(std::io::sink)
            .finish();
        tracing::subscriber::with_default(subscriber, check);
    }

    #[test]
    fn transfer_directive_enables_every_operation() {
        with_filter("aws_sdk_s3_transfer_manager::transfer=debug", || {
            assert!(tracing::enabled!(target: TARGET_TRANSFER, Level::DEBUG));
            assert!(tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::DEBUG));
            assert!(tracing::enabled!(target: TARGET_TRANSFER_DOWNLOAD, Level::DEBUG));
            assert!(tracing::enabled!(
                target: TARGET_TRANSFER_UPLOAD_OBJECTS,
                Level::DEBUG
            ));
            assert!(tracing::enabled!(
                target: TARGET_TRANSFER_DOWNLOAD_OBJECTS,
                Level::DEBUG
            ));
            assert!(!tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::TRACE));
        });
    }

    #[test]
    fn operation_directive_selects_only_its_operation() {
        with_filter(
            "aws_sdk_s3_transfer_manager::transfer::download=debug",
            || {
                assert!(tracing::enabled!(target: TARGET_TRANSFER_DOWNLOAD, Level::DEBUG));
                assert!(!tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::DEBUG));
                assert!(!tracing::enabled!(
                    target: TARGET_TRANSFER_UPLOAD_OBJECTS,
                    Level::DEBUG
                ));
                assert!(!tracing::enabled!(target: TARGET_TRANSFER, Level::DEBUG));
                // `transfer::download` is a prefix of `transfer::download_objects`.
                assert!(tracing::enabled!(
                    target: TARGET_TRANSFER_DOWNLOAD_OBJECTS,
                    Level::DEBUG
                ));
            },
        );
    }

    #[test]
    fn child_directive_overrides_parent_directive() {
        with_filter(
            "aws_sdk_s3_transfer_manager::transfer=warn,aws_sdk_s3_transfer_manager::transfer::upload=debug",
            || {
                assert!(tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::DEBUG));
                assert!(!tracing::enabled!(target: TARGET_TRANSFER_DOWNLOAD, Level::DEBUG));
                assert!(tracing::enabled!(target: TARGET_TRANSFER_DOWNLOAD, Level::WARN));
            },
        );
    }

    #[test]
    fn one_operation_at_a_different_level() {
        with_filter(
            "aws_sdk_s3_transfer_manager::transfer=debug,aws_sdk_s3_transfer_manager::transfer::upload_objects=trace",
            || {
                assert!(tracing::enabled!(
                    target: TARGET_TRANSFER_UPLOAD_OBJECTS,
                    Level::TRACE
                ));
                assert!(!tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::TRACE));
                assert!(tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::DEBUG));
            },
        );
    }

    #[test]
    fn longer_directive_sets_composite_parent_level() {
        with_filter(
            "aws_sdk_s3_transfer_manager::transfer::upload=debug,aws_sdk_s3_transfer_manager::transfer::upload_objects=warn",
            || {
                assert!(tracing::enabled!(target: TARGET_TRANSFER_UPLOAD, Level::DEBUG));
                assert!(!tracing::enabled!(
                    target: TARGET_TRANSFER_UPLOAD_OBJECTS,
                    Level::DEBUG
                ));
                assert!(tracing::enabled!(
                    target: TARGET_TRANSFER_UPLOAD_OBJECTS,
                    Level::WARN
                ));
            },
        );
    }
}
