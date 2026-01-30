/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::io::{InputStream, PartData};
use crate::operation::download::ChunkMetadata;
use crate::operation::ChunkSender;

use aws_sdk_s3::primitives::ByteStream;

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
#[derive(Debug)]
pub(crate) struct WorkItem {
    pub(crate) transfer_id: TransferId,
    pub(crate) kind: WorkKind,
    pub(crate) data: WorkData,
}

/// Data associated with a work item.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum WorkData {
    // ==================== Upload ====================
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
    /// Single PutObject upload (for small files below MPU threshold)
    ///
    /// TODO(redux): Currently PutObject is Network-only - the actual disk I/O happens lazily
    /// when the SDK consumes the ByteStream during HTTP send. For true scheduler control over
    /// disk I/O (important for large numbers of small files), we should:
    /// 1. Have DataIO phase read file into memory (or into a buffer we control)
    /// 2. Network phase sends from that buffer
    /// This requires tighter integration between InputStream internals and our scheduler.
    PutObject {
        /// The input stream to upload - converted to ByteStream at send time
        stream: Option<InputStream>,
    },

    // ==================== Download ====================
    /// Discover object metadata (HeadObject or first ranged GET)
    Discovery {
        /// Channel to send chunks to Body
        chunk_tx: ChunkSender,
    },
    /// Read the body from discovery's initial chunk
    ReadDiscoveryBody {
        /// The body stream from discovery
        stream: ByteStream,
        /// Sequence number (always 0 for initial chunk)
        seq: u64,
        /// Metadata from the discovery response
        chunk_meta: ChunkMetadata,
        /// Channel to send chunks to Body
        chunk_tx: ChunkSender,
    },
    /// Download a range of an object (Network only for now)
    GetObjectRange {
        /// Byte range to download (inclusive)
        range: RangeInclusive<u64>,
        /// Sequence number for ordering chunks in the output Body
        seq: u64,
        /// ETag for if_match consistency (from discovery, shared across all ranges)
        etag: Option<Arc<str>>,
        /// Channel to send chunks to Body
        chunk_tx: ChunkSender,
    },
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
    /// Work failed - error is stored in transfer context, not here
    Failed,
    Cancelled,
}
