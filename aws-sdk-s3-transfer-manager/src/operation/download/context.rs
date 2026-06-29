/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

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
        /// Number of slots claimed (the W-gate numerator). Tracks the issuance
        /// cursor for the read-ahead bound: `issued - consumed < W`.
        issued: u64,
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
