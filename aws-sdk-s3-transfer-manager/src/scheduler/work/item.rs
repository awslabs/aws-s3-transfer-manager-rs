/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::PartData;

/// The kind of work to be executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkKind {
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

/// A unit of work to be scheduled.
#[derive(Debug, Clone)]
pub(crate) struct WorkItem {
    pub(crate) transfer_id: TransferId,
    pub(crate) kind: WorkKind,
    pub(crate) data: WorkData,
}

/// Data associated with a work item.
#[derive(Debug, Clone)]
pub(crate) enum WorkData {
    /// Create multipart upload (Network only)
    CreateMPU,
    /// Upload a single part
    UploadPart {
        part_number: u64,
        /// Part data - None before DataIO, Some after
        part_data: Option<PartData>,
    },
    /// Complete multipart upload (Network only)
    CompleteMPU,
    // /// Single PutObject upload (for small files below MPU threshold)
    // PutObject {
    //     /// Body data - None before DataIO, Some after
    //     body: Option<PartData>,
    // },
    // TODO: GetObjectRange (download)
}

/// Result of polling a transfer for work.
#[derive(Debug)]
pub(crate) enum PollWork {
    /// Work is available to execute.
    Ready(WorkItem),
    /// Transfer is blocked waiting for in-flight work to complete.
    /// Scheduler should not poll again until `wake(transfer_id)` is called.
    Pending,
    /// Transfer has completed all work.
    Done,
}

/// Result of executing a work item.
#[derive(Debug)]
pub(crate) enum WorkOutcome {
    Success {
        /// Schedule follow-on work of this kind, or None if complete.
        schedule_next: Option<WorkKind>,
        data: WorkData,
    },
    Failed {
        error: crate::error::Error,
    },
    Cancelled,
}
