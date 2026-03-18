/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Tracing target constants for structured log filtering.
//!
//! These targets allow filtering logs by concern rather than module path:
//!
//! ```text
//! RUST_LOG=aws_s3_transfer_manager::concurrency=debug    # concurrency decisions
//! RUST_LOG=aws_s3_transfer_manager::scheduling=debug     # scheduling decisions
//! RUST_LOG=aws_s3_transfer_manager::execution=trace      # per-work-item execute/complete
//! RUST_LOG=aws_s3_transfer_manager::transfer=debug       # transfer lifecycle events
//! ```

/// Adaptive concurrency controller: phase transitions, target changes, probe results.
pub(crate) const TARGET_CONCURRENCY: &str = "aws_s3_transfer_manager::concurrency";

/// Scheduler capacity decisions, worker pool growth.
pub(crate) const TARGET_SCHEDULING: &str = "aws_s3_transfer_manager::scheduling";

/// Per-work-item execution: dispatch, complete, skip, panic.
pub(crate) const TARGET_EXECUTION: &str = "aws_s3_transfer_manager::execution";

/// Transfer lifecycle: enqueue, complete, cancel, fail, state transitions.
pub(crate) const TARGET_TRANSFER: &str = "aws_s3_transfer_manager::transfer";
