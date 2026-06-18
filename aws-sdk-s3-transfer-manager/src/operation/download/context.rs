/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::download::body::BodySlot;
use crate::runtime::memory::WaitTicket;

/// A claimed slot whose backing memory is not yet reserved. Held while a transfer
/// is budget-blocked: `try_claim` already granted the slot (the prefetch window
/// had room — the consumer is keeping up), but the slot cannot be filled until the
/// budget grants `ticket`. Dropping it (on terminal) releases the slot and cancels
/// the budget wait.
#[derive(Debug)]
pub(crate) struct PendingClaim {
    pub(crate) slot: BodySlot,
    pub(crate) ticket: WaitTicket,
}

/// Mutable state for tracking download work progress
#[derive(Debug)]
pub(crate) enum DownloadState {
    /// Waiting to start discovery
    PendingDiscovery,

    /// Discovery request in flight
    DiscoveryInFlight,

    /// Data transfer in progress (downloading ranges)
    Transferring {
        /// Remaining byte range to fetch (None if all ranges generated)
        remaining: Option<std::ops::RangeInclusive<u64>>,
        /// Number of ranges currently in flight
        ranges_in_flight: usize,
        /// ETag for consistency (shared across all range requests)
        etag: Option<std::sync::Arc<str>>,
        /// Per-chunk size used to slice `remaining`. Normally the configured
        /// download part size; for a multipart object being validated it is the
        /// object's stored part size so each range aligns to a stored part
        /// boundary (S3 returns a per-part checksum only for an aligned range).
        part_size: u64,
        /// A claimed-but-unfilled slot whose memory reservation is still pending,
        /// held only while budget-blocked. `Some` after the window granted a slot
        /// but the budget queued the reservation; taken once the budget grants.
        /// Dropping the state on terminal releases the slot and cancels the wait.
        pending: Option<PendingClaim>,
    },

    /// Terminal state - transfer ended (success, failure, or cancelled)
    /// TransferContext status holds final result
    Terminal,
}

impl DownloadState {
    pub(crate) fn new() -> Self {
        DownloadState::PendingDiscovery
    }
}
