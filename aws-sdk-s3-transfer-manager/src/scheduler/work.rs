/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::PartData;

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
#[derive(Debug)]
pub(crate) struct WorkItem {
    pub(crate) transfer_id: TransferId,
    pub(crate) phase: WorkPhase,
    pub(crate) data: WorkData,
}

/// Data associated with a work item.
///
/// Flows between phases - e.g., UploadPart reads data in DataIO phase,
/// then sends it in Network phase.
#[derive(Debug)]
pub(crate) enum WorkData {
    /// Create multipart upload (Network phase only)
    CreateMPU,
    /// Upload a single part
    UploadPart {
        part_number: u64,
        /// Part data - None before DataIO, Some after
        part_data: Option<PartData>,
    },
    /// Complete multipart upload (Network phase only)
    CompleteMPU,
    // TODO: PutObject (single part upload)
    // TODO: GetObjectRange (download)
}

/// Result of executing a work item
#[derive(Debug)]
pub(crate) enum WorkOutcome {
    Success {
        next_phase: Option<WorkPhase>,
        data: WorkData,
    },
    Failed {
        error: crate::error::Error,
    },
    Cancelled,
}
