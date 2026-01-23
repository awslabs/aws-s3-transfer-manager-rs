/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Phase of work execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkPhase {
    /// Disk I/O (read for uploads, write for downloads)
    DataIO,
    /// HTTP request (uploads and downloads)
    Network,
}

/// Unique identifier for a transfer, with optional parent for hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TransferId {
    pub(crate) id: u64,
    pub(crate) parent: Option<u64>,
}

/// A unit of work to be scheduled
#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) transfer_id: TransferId,
    pub(crate) phase: WorkPhase,
}

/// Result of executing a work item
#[derive(Debug)]
pub(crate) enum WorkOutcome {
    Success { next_phase: Option<WorkPhase> },
    Failed { error: crate::error::Error },
    Cancelled,
}
