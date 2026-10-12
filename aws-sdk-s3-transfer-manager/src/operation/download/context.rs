/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::download::body::BodySlot;
use crate::runtime::buffer_pool::ReserveFuture;
use crate::transfer::{PendingCategory, PendingCause};

/// Why the download state machine cannot produce another work item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DownloadPendingReason {
    /// Object discovery is still in flight.
    Discovery,
    /// Stream delivery is waiting for the consumer to release read-ahead capacity.
    ReadAhead,
    /// A claimed body slot is waiting for shared buffer-pool admission.
    MemoryAdmission,
    /// Every range was issued and in-flight range work must retire.
    RangeCompletion,
    /// Every range retired, and in-flight drains (memory-relief writes) must
    /// finish before the destination can be finalized.
    DrainCompletion,
}

impl From<DownloadPendingReason> for PendingCause {
    fn from(reason: DownloadPendingReason) -> Self {
        match reason {
            DownloadPendingReason::Discovery => Self::in_flight_work("discovery"),
            DownloadPendingReason::ReadAhead => Self::new(PendingCategory::Consumer, "read_ahead"),
            DownloadPendingReason::MemoryAdmission => {
                Self::new(PendingCategory::Memory, "memory_admission")
            }
            DownloadPendingReason::RangeCompletion => Self::in_flight_work("range_completion"),
            DownloadPendingReason::DrainCompletion => Self::in_flight_work("drain_completion"),
        }
    }
}

/// A claimed slot waiting for shared memory admission.
///
/// The read-ahead gate already counted the slot as issued. Dropping this value
/// releases the slot and cancellation-safely removes its reservation future
/// from the memory-admission FIFO.
pub(crate) struct PendingClaim {
    pub(crate) slot: BodySlot,
    pub(crate) reservation: ReserveFuture,
}

impl std::fmt::Debug for PendingClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingClaim")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// Mutable state for tracking download work progress
///
/// # Completion
///
/// Every work item `poll_work` issues is counted when it is issued and
/// uncounted when it retires: a range in `ranges_in_flight`, a memory-relief
/// item in `drains_in_flight`, and discovery by the `DiscoveryInFlight` state,
/// its body, if any, then counted in `ranges_in_flight` like a range. The item
/// that uncounts the last one with nothing left to issue (discovery itself,
/// for an object with no ranges) claims completion with
/// [`try_claim_completion`](Self::try_claim_completion) under the state lock
/// and finalizes the destination after releasing it. `poll_work` issues work
/// and parks; it never completes a transfer. Failure can be claimed from any
/// path, through the transfer's `fail`.
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
        /// Memory-relief items in flight: counted when `poll_work` issues one
        /// and uncounted when it retires, after writing every run it claimed.
        /// A claimed run is invisible to the terminal drain, so the transfer
        /// completes only when this is zero. At most one is in flight, since
        /// one item drains every drainable run.
        drains_in_flight: u32,
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
        /// A claimed-but-unfilled slot whose memory reservation is still pending.
        /// `Some` after the gate admitted a slot (already counted in
        /// `gate.issued`) but memory admission queued the reservation; taken once
        /// capacity is granted. Dropping terminal state releases the slot and
        /// cancellation-safely removes the future from the FIFO.
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

    /// Transitions this state to `Terminal`.
    ///
    /// Returns any pending claim so its reservation future can be cancelled
    /// after the caller releases the state lock. Cancellation enters memory
    /// admission, which must not be nested under this lock. Assign `Terminal`
    /// through this method rather than directly.
    #[must_use = "drop the returned PendingClaim only after releasing the state lock"]
    pub(crate) fn enter_terminal(&mut self) -> Option<PendingClaim> {
        let pending = match self {
            DownloadState::Transferring { pending, .. } => pending.take(),
            _ => None,
        };
        *self = DownloadState::Terminal;
        pending
    }

    /// Claims successful completion when nothing is left to issue and no work is in
    /// flight, and enters `Terminal`.
    ///
    /// Returns `Some` at most once per transfer: from `Transferring` with no range
    /// remaining, no range or memory-relief item in flight, and no pending claim,
    /// after which the state is `Terminal`. Any other state returns `None` and is
    /// left unchanged. The caller holds the state lock across the check and the
    /// transition, and finalizes the destination only after releasing it.
    ///
    /// `remaining: None` implies `pending: None`: a claim waits in `pending` only
    /// while its range is still in `remaining`, because `poll_work` commits a range
    /// only once its slot is ready. The pattern matches `pending: None` to state the
    /// full condition, so entering `Terminal` here never returns a claim to cancel.
    pub(crate) fn try_claim_completion(&mut self) -> Option<CompletionClaim> {
        let DownloadState::Transferring {
            remaining: None,
            ranges_in_flight: 0,
            drains_in_flight: 0,
            pending: None,
            ..
        } = self
        else {
            return None;
        };
        let _no_claim = self.enter_terminal();
        Some(CompletionClaim(()))
    }
}

/// Proof that this caller claimed successful completion under the state lock.
///
/// Only [`DownloadState::try_claim_completion`] constructs it; `finalize_completion`
/// consumes it. The private field keeps code outside this module from
/// constructing one.
///
/// A claim taken while the transfer is active must be finalized. A claim taken
/// after the transfer stopped being active is dropped instead, as
/// `bail_if_terminal!` does: that can happen only between a cancellation's status
/// change and its `on_terminal`, and the cancellation's terminal path already
/// owns the destination. `fail` cannot open that window, because it changes the
/// status and the state together under the state lock.
#[must_use = "a completion claimed while active must be finalized after releasing the state lock"]
#[derive(Debug)]
pub(crate) struct CompletionClaim(());

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
    use super::{DownloadPendingReason, DownloadState, OccupancyGate};
    use crate::transfer::PendingCategory;

    /// A `Transferring` state with the given work outstanding and no pending claim.
    fn transferring(
        remaining: Option<std::ops::RangeInclusive<u64>>,
        ranges_in_flight: usize,
        drains_in_flight: u32,
    ) -> DownloadState {
        DownloadState::Transferring {
            remaining,
            ranges_in_flight,
            drains_in_flight,
            etag: None,
            part_size: 8,
            gate: OccupancyGate::default(),
            pending: None,
        }
    }

    /// Completion is not claimed while a range remains to issue, a range or a
    /// drain is in flight, or the transfer is not transferring, and the state is
    /// left as it was.
    #[test]
    fn completion_is_not_claimed_with_work_outstanding() {
        let outstanding = [
            ("a range remaining", transferring(Some(0..=7), 0, 0)),
            ("a range in flight", transferring(None, 1, 0)),
            ("a drain in flight", transferring(None, 0, 1)),
        ];
        for (what, mut state) in outstanding {
            assert!(
                state.try_claim_completion().is_none(),
                "completion claimed with {what}"
            );
            assert!(
                matches!(state, DownloadState::Transferring { .. }),
                "state changed with {what}: {state:?}"
            );
        }

        for mut state in [
            DownloadState::PendingDiscovery,
            DownloadState::DiscoveryInFlight,
            DownloadState::Terminal,
        ] {
            let before = format!("{state:?}");
            assert!(
                state.try_claim_completion().is_none(),
                "claimed from {before}"
            );
            assert_eq!(format!("{state:?}"), before);
        }
    }

    /// With nothing remaining and nothing in flight, completion is claimed once,
    /// and the claim leaves the state `Terminal`.
    #[test]
    fn completion_is_claimed_exactly_once() {
        let mut state = transferring(None, 0, 0);
        assert!(state.try_claim_completion().is_some());
        assert!(matches!(state, DownloadState::Terminal));
        assert!(state.try_claim_completion().is_none(), "claimed twice");
    }

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

    #[test]
    fn pending_reasons_map_to_common_transfer_categories() {
        let cases = [
            (
                DownloadPendingReason::Discovery,
                PendingCategory::InFlightWork,
                "discovery",
            ),
            (
                DownloadPendingReason::ReadAhead,
                PendingCategory::Consumer,
                "read_ahead",
            ),
            (
                DownloadPendingReason::MemoryAdmission,
                PendingCategory::Memory,
                "memory_admission",
            ),
            (
                DownloadPendingReason::RangeCompletion,
                PendingCategory::InFlightWork,
                "range_completion",
            ),
            (
                DownloadPendingReason::DrainCompletion,
                PendingCategory::InFlightWork,
                "drain_completion",
            ),
        ];

        for (reason, category, detail) in cases {
            let cause = crate::transfer::PendingCause::from(reason);
            assert_eq!(cause.category, category);
            assert_eq!(cause.reason, detail);
        }
    }
}
