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

/// Default maximum gap between claimed and consumed sequences.
/// With 8MB parts, this is 128MB of buffered data.
const DEFAULT_SEQ_WINDOW_MAX_GAP: u64 = 16;

/// Controls how far ahead of consumer work can be generated.
///
/// Invariant: `claimed < consumed + max_gap`
#[derive(Debug)]
pub(crate) struct SeqWindow {
    /// Next seq to be consumed by Body.next()
    consumed: AtomicU64,
    /// Next seq to be claimed by poll_work()
    claimed: AtomicU64,
    /// Maximum allowed gap
    max_gap: AtomicU64,
}

impl SeqWindow {
    pub(crate) fn new(max_gap: u64) -> Self {
        Self {
            consumed: AtomicU64::new(0),
            claimed: AtomicU64::new(0),
            max_gap: AtomicU64::new(max_gap),
        }
    }

    /// Try to claim next seq for work generation. Returns None if window exhausted.
    pub(crate) fn try_claim(&self) -> Option<u64> {
        loop {
            let consumed = self.consumed.load(Ordering::Acquire);
            let claimed = self.claimed.load(Ordering::Acquire);
            let max_gap = self.max_gap.load(Ordering::Acquire);

            if claimed >= consumed + max_gap {
                return None;
            }

            if self
                .claimed
                .compare_exchange_weak(claimed, claimed + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(claimed);
            }
        }
    }

    /// Mark seq as consumed. Returns true if window was exhausted (should wake).
    pub(crate) fn consume(&self, seq: u64) -> bool {
        let was_exhausted = self.is_exhausted();
        self.consumed.fetch_max(seq + 1, Ordering::AcqRel);
        was_exhausted && !self.is_exhausted()
    }

    fn is_exhausted(&self) -> bool {
        let consumed = self.consumed.load(Ordering::Acquire);
        let claimed = self.claimed.load(Ordering::Acquire);
        let max_gap = self.max_gap.load(Ordering::Acquire);
        claimed >= consumed + max_gap
    }

    pub(crate) fn set_max_gap(&self, gap: u64) {
        self.max_gap.store(gap, Ordering::Release);
    }

    pub(crate) fn max_gap(&self) -> u64 {
        self.max_gap.load(Ordering::Acquire)
    }
}

impl Default for SeqWindow {
    fn default() -> Self {
        Self::new(DEFAULT_SEQ_WINDOW_MAX_GAP)
    }
}

pub(crate) type DownloadContext = TransferContext<DownloadState>;

impl DownloadContext {
    pub(crate) fn new(
        id: crate::scheduler::TransferId,
        handle: Arc<crate::client::Handle>,
        bucket_type: BucketType,
        input: DownloadInput,
        chunk_tx: ChunkSender,
    ) -> (Self, crate::operation::StateMachineTerminalReceiver) {
        let state = Arc::new(DownloadState {
            request: Arc::new(input),
            bucket_type,
            seq_window: SeqWindow::default(),
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
}

/// Download operation specific state
#[derive(Debug)]
pub(crate) struct DownloadState {
    /// The original request
    pub(crate) request: Arc<DownloadInput>,

    /// Type of S3 bucket targeted by this operation
    pub(crate) bucket_type: BucketType,

    /// Sequence window for backpressure control
    pub(crate) seq_window: SeqWindow,

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

    /// Terminal state - transfer ended (success, failure, or cancelled)
    /// TransferContext status holds final result
    Terminal,
}

impl DownloadWorkState {
    pub(crate) fn new(chunk_tx: ChunkSender) -> Self {
        DownloadWorkState::PendingDiscovery { chunk_tx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seq_window_try_claim_within_gap() {
        let window = SeqWindow::new(4);

        // Should be able to claim 0, 1, 2, 3
        assert_eq!(window.try_claim(), Some(0));
        assert_eq!(window.try_claim(), Some(1));
        assert_eq!(window.try_claim(), Some(2));
        assert_eq!(window.try_claim(), Some(3));

        // Gap exhausted (claimed=4, consumed=0, gap=4)
        assert_eq!(window.try_claim(), None);
    }

    #[test]
    fn test_seq_window_consume_enables_claim() {
        let window = SeqWindow::new(2);

        // Claim up to gap
        assert_eq!(window.try_claim(), Some(0));
        assert_eq!(window.try_claim(), Some(1));
        assert_eq!(window.try_claim(), None);

        // Consume seq 0 - should enable one more claim
        let was_exhausted = window.consume(0);
        assert!(was_exhausted, "window was exhausted before consume");

        assert_eq!(window.try_claim(), Some(2));
        assert_eq!(window.try_claim(), None);
    }

    #[test]
    fn test_seq_window_consume_returns_false_when_not_exhausted() {
        let window = SeqWindow::new(4);

        assert_eq!(window.try_claim(), Some(0));
        // Window not exhausted (claimed=1, consumed=0, gap=4)

        let was_exhausted = window.consume(0);
        assert!(!was_exhausted, "window was not exhausted");
    }

    #[test]
    fn test_seq_window_out_of_order_consume() {
        let window = SeqWindow::new(2);

        assert_eq!(window.try_claim(), Some(0));
        assert_eq!(window.try_claim(), Some(1));
        assert_eq!(window.try_claim(), None);

        // Consume seq 1 first (out of order) - consumed advances to 2
        window.consume(1);

        // Now can claim seq 2 and 3
        assert_eq!(window.try_claim(), Some(2));
        assert_eq!(window.try_claim(), Some(3));
        assert_eq!(window.try_claim(), None);
    }

    #[test]
    fn test_seq_window_set_max_gap() {
        let window = SeqWindow::new(2);

        assert_eq!(window.try_claim(), Some(0));
        assert_eq!(window.try_claim(), Some(1));
        assert_eq!(window.try_claim(), None);

        // Increase gap
        window.set_max_gap(4);

        assert_eq!(window.try_claim(), Some(2));
        assert_eq!(window.try_claim(), Some(3));
        assert_eq!(window.try_claim(), None);
    }
}
