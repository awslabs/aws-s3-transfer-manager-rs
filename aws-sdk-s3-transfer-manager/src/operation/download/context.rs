/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::DownloadInput;
use crate::operation::{ChunkSender, TransferContext};
use crate::types::BucketType;

pub(crate) type DownloadContext = TransferContext<DownloadState>;

impl DownloadContext {
    pub(crate) fn new(
        id: crate::scheduler::TransferId,
        handle: Arc<crate::client::Handle>,
        bucket_type: BucketType,
        input: DownloadInput,
        chunk_tx: ChunkSender,
    ) -> (Self, crate::operation::StateMachineCompleteReceiver) {
        let state = Arc::new(DownloadState {
            request: Arc::new(input),
            bucket_type,
            current_seq: AtomicU64::new(0),
            object_meta: std::sync::OnceLock::new(),
            discovery_notify: tokio::sync::Notify::new(),
            work: Mutex::new(DownloadWorkState::new(chunk_tx)),
        });
        TransferContext::from_state(id, handle, state)
    }

    /// The target part size to use for this download
    pub(crate) fn target_part_size_bytes(&self) -> u64 {
        self.handle.download_part_size_bytes()
    }

    /// Returns the type of bucket targeted by this operation
    pub(crate) fn bucket_type(&self) -> BucketType {
        self.state.bucket_type
    }

    /// Returns the next seq and increments
    pub(crate) fn next_seq(&self) -> u64 {
        self.state.current_seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Current seq without incrementing
    pub(crate) fn current_seq(&self) -> u64 {
        self.state.current_seq.load(Ordering::SeqCst)
    }
}

/// Download operation specific state
#[derive(Debug)]
pub(crate) struct DownloadState {
    /// The original request
    pub(crate) request: Arc<DownloadInput>,

    /// Type of S3 bucket targeted by this operation
    pub(crate) bucket_type: BucketType,

    /// Sequence counter for chunks
    pub(crate) current_seq: AtomicU64,

    /// Object metadata from discovery (set once discovery completes)
    pub(crate) object_meta: std::sync::OnceLock<ObjectMetadata>,

    /// Notified when discovery completes (success or failure)
    pub(crate) discovery_notify: tokio::sync::Notify,

    /// Mutable work state (protected by mutex)
    pub(crate) work: Mutex<DownloadWorkState>,
}

impl DownloadState {
    /// The original request
    pub(crate) fn request(&self) -> &DownloadInput {
        &self.request
    }

    /// Type of S3 bucket targeted by this operation
    pub(crate) fn bucket_type(&self) -> BucketType {
        self.bucket_type
    }
}

/// Mutable state for tracking download work progress
#[derive(Debug)]
pub(crate) enum DownloadWorkState {
    /// Waiting to start discovery
    PendingDiscovery {
        /// Channel to send chunks to Body (passed to Transferring state)
        chunk_tx: ChunkSender,
    },

    /// Discovery request in flight
    DiscoveryInFlight {
        /// Channel to send chunks to Body
        chunk_tx: ChunkSender,
    },

    /// Data transfer in progress (downloading ranges)
    Transferring {
        /// Remaining byte range to fetch (None if all ranges generated)
        remaining: Option<std::ops::RangeInclusive<u64>>,
        /// Number of ranges currently in flight
        ranges_in_flight: usize,
        /// ETag for consistency (shared across all range requests)
        etag: Option<Arc<str>>,
        /// Object metadata from discovery
        object_meta: ObjectMetadata,
        /// Channel to send chunks to Body
        chunk_tx: ChunkSender,
    },

    /// Done
    Done,
}

impl DownloadWorkState {
    pub(crate) fn new(chunk_tx: ChunkSender) -> Self {
        DownloadWorkState::PendingDiscovery { chunk_tx }
    }
}
