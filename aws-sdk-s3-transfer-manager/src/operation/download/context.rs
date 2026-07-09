/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::download::body::BodySlot;
use crate::runtime::memory::WaitTicket;

/// A claimed slot whose backing memory is not yet reserved. Held while a transfer
/// is budget-blocked: the read-ahead gate admitted the slot (`gate.try_issue`
/// already counted it as issued) and it was claimed from the ring, but it cannot be
/// filled until the budget grants `ticket`. Dropping it (on terminal) releases the
/// slot and cancels the budget wait.
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
        /// Read-ahead occupancy accounting. Bounds resident memory by gating
        /// issuance on `issued - released < window`.
        gate: OccupancyGate,
        /// A claimed-but-unfilled slot whose memory reservation is still pending,
        /// held only while budget-blocked. `Some` after the gate admitted a slot
        /// (already counted in `gate.issued`) but the budget queued the reservation;
        /// taken once the budget grants. Dropping the state on terminal releases the
        /// slot and cancels the wait.
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

    /// The sanctioned transition to `Terminal`. Extracts a budget-parked claim, if any, so the
    /// caller can drop it AFTER releasing the state lock. `WaitTicket::drop` takes the budget
    /// lock, which must never nest under the state lock this state is guarded by; returning the
    /// claim here makes releasing the lock first the only way to use this method. Assign
    /// `Terminal` through this method rather than directly.
    #[must_use = "drop the returned PendingClaim only after releasing the state lock"]
    pub(crate) fn enter_terminal(&mut self) -> Option<PendingClaim> {
        let pending = match self {
            DownloadState::Transferring { pending, .. } => pending.take(),
            _ => None,
        };
        *self = DownloadState::Terminal;
        pending
    }
}

/// Read-ahead occupancy accounting: the numerator of the issuance gate.
///
/// Bounds resident memory by holding `issued - released < window`, where
/// `issued` counts parts claimed for issuance and `released` counts parts whose
/// payload memory has been freed by either delivery surface (stream `poll_next`
/// or disk drain). `window` is *not* held here — it is the dynamically settable
/// [`ReadAhead`](super::read_ahead::ReadAhead) knob, read as an input on each
/// check — so this type owns exactly the two counters that must move together.
///
/// # Why this is plain (non-atomic) state
///
/// This struct lives inside [`DownloadState`], guarded by the transfer's `state`
/// mutex, and is only ever reached through a `MutexGuard`. That is the entire
/// lost-wake safety argument: `released` cannot be advanced, nor the gate read to
/// arm the wake, except while holding the lock the gate is checked under. The
/// issuer's park (read `issued - released`, then `set_pending`) and the
/// consumer's release (advance `released`, then `try_wake`) are therefore always
/// ordered by that lock — the mutator discipline `lock → mutate → unlock →
/// try_wake` (see [`TransferContext::set_pending`](crate::transfer::TransferContext::set_pending)).
/// There is no lock-free path to `released`, so the store-buffer interleaving
/// that would lose a wake is unrepresentable rather than merely avoided.
#[derive(Debug, Default)]
pub(crate) struct OccupancyGate {
    /// Parts claimed for issuance. Monotonic.
    issued: u64,
    /// Parts whose payload memory has been freed by a delivery surface.
    released: u64,
}

impl OccupancyGate {
    /// Create a gate that already counts `issued` parts as claimed (with none
    /// released yet). Used at the discovery→transfer transition, where a
    /// discovery chunk is one part already in flight.
    pub(crate) fn with_issued(issued: u64) -> Self {
        Self {
            issued,
            released: 0,
        }
    }

    /// Issuer side: if the gate is open at `window`, count one part issued and
    /// return `true`. Returns `false` (gate closed) without mutating, so the
    /// caller parks. `window` is supplied by the read-ahead controller.
    pub(crate) fn try_issue(&mut self, window: u64) -> bool {
        if self.issued - self.released >= window {
            false
        } else {
            self.issued += 1;
            true
        }
    }

    /// Consumer/drain side: record `n` parts freed, lowering resident occupancy.
    ///
    /// The caller wakes the issuer unconditionally after this returns (a wake is a
    /// no-op unless the issuer parked), so this does not report whether the gate
    /// reopened — it is a plain accumulator.
    pub(crate) fn release(&mut self, n: u64) {
        self.released += n;
    }

    /// Resident occupancy in parts: `issued - released`.
    pub(crate) fn resident(&self) -> u64 {
        self.issued - self.released
    }

    /// Parts issued so far (for tracing/tests).
    pub(crate) fn issued(&self) -> u64 {
        self.issued
    }

    /// Parts released so far (for tracing/tests).
    pub(crate) fn released(&self) -> u64 {
        self.released
    }
}

#[cfg(test)]
mod tests {
    use super::OccupancyGate;

    #[test]
    fn gate_closes_at_window() {
        let mut g = OccupancyGate::default();
        // Window 2: two issues succeed, the third is gated.
        assert!(g.try_issue(2));
        assert!(g.try_issue(2));
        assert!(!g.try_issue(2), "gate must close once resident == window");
        assert_eq!(g.resident(), 2);
        assert_eq!(g.issued(), 2, "a gated try_issue must not bump issued");
    }

    #[test]
    fn release_lowers_resident_and_reopens_the_gate() {
        let mut g = OccupancyGate::default();
        g.try_issue(2);
        g.try_issue(2); // resident == window == 2, gate closed
        assert!(!g.try_issue(2), "closed at the window");
        g.release(1); // frees one part
        assert_eq!(g.resident(), 1, "release lowers resident occupancy");
        assert!(
            g.try_issue(2),
            "the freed part reopened the gate for one more"
        );
    }

    #[test]
    fn window_one_admits_exactly_one_resident_part() {
        // Window 1 is the demand-paging regime (`Parts(0)` resolves to it): exactly
        // one part in flight, the one the consumer is waiting on.
        let mut g = OccupancyGate::default();
        assert!(g.try_issue(1));
        assert!(!g.try_issue(1), "window 1 admits exactly one resident part");
        g.release(1);
        assert!(g.try_issue(1), "gate reopened, next part may issue");
    }
}
