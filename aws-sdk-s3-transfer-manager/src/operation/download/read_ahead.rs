/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-transfer read-ahead window — the **receive window** (rwnd) of the download.
//!
//! # The bound it owns
//!
//! Issuance of a speculative part requires `issued - consumed < window()`. The
//! quantity `issued - consumed` is the transfer's **resident occupancy in parts**:
//! parts claimed (issued) but not yet delivered in order to the consumer. So the
//! gate is a pure occupancy bound — "do not run more than `window` parts ahead of
//! the consumer" — and nothing more. It is one of three independent upper bounds
//! on issuance (the others — total in-flight concurrency, and the aggregate memory
//! budget — live elsewhere); issuance takes their min. This type owns only the
//! read-ahead bound.
//!
//! # rwnd, not cwnd — and occupancy, not rate
//!
//! The window maps onto TCP's *receive window* (rwnd), which is advertised from
//! the receiver's **free buffer space** — `capacity - unread`. It is not estimated
//! and it does not decay: it is an exact subtraction recomputed as the buffer fills
//! and drains. This is categorically different from the *congestion window* (cwnd),
//! which probes toward the bandwidth-delay product `R̂·L̂`. The cwnd job — ramping
//! to the throughput knee — belongs to the **concurrency controller** (global,
//! separate), never here.
//!
//! An earlier version of this controller derived the operating window from a
//! measured drain rate (`W* = R̂·L̂/part_size`, smoothed by an EWMA, decayed on
//! idle). That is cwnd math on an rwnd, and it self-destructs: any gap in delivery
//! — a consumer between bursts, or one blocked behind a not-yet-filled part —
//! decays the rate estimate toward zero, collapsing the window to its floor and
//! throttling issuance to a single outstanding part (a wedge). The window is
//! occupancy-bounded; no rate is estimated.
//!
//! # The window today: a fixed ceiling
//!
//! The window is a **fixed constant** ([`DEFAULT_WINDOW_PARTS`]) — a per-transfer
//! resident cap, so memory bounds the transfer rather than a control loop. The
//! pacing behaviors fall out of the occupancy gate directly, with no adaptation:
//!
//! - **Fast consumer:** `consumed` keeps pace → `issued - consumed` stays small →
//!   the gate never binds → the transfer runs at full speed, bounded by concurrency
//!   and (later) the global budget, not by us.
//! - **Slow consumer:** `consumed` lags → `issued - consumed` fills to the window →
//!   the gate closes → issuance pauses until the consumer drains. Resident memory is
//!   bounded at exactly `window` parts; we never pile up parts the consumer will not
//!   read soon. This is the backpressure, and it is automatic.
//! - **Blocked consumer (a stalled/missing part):** `consumed` cannot advance past
//!   the hole → occupancy pins at the window → issuance stops at `window`, leaving a
//!   full window of slack to make progress around the hole. It does not collapse to
//!   one part.
//!
//! # What is deferred
//!
//! Two layers build on this and are **not** implemented here yet (see
//! `flow-control-architecture.md` §7b, the backpressure ladder):
//!
//! 1. **Budget-pressure clamp (ladder rung 1).** When aggregate resident across all
//!    transfers approaches the global budget, the budget layer pulls each transfer's
//!    effective window down toward its demand. The fixed constant here becomes
//!    `memory_budget / part_size`, arbitrated globally. This is what stops one slow
//!    transfer from holding buffer space other transfers need.
//! 2. **`Auto`-mode resident-size adaptation.** A fill-side signal — resident set
//!    size ballooning relative to the window — clamps the window down ahead of the
//!    buffer filling completely. Observed where parts are filled (not by polling the
//!    consumer, which is the thing we are waiting on). Layered under the public
//!    `ReadAhead::Auto` mode once the fixed model is proven.
//!
//! The window is stored in an [`AtomicU64`] (not a plain constant) precisely so
//! those layers can lower it with a lock-free store; today nothing writes it after
//! construction except the test helper.

use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed read-ahead window, in parts. A per-transfer cap on resident occupancy:
/// with an 8 MiB part this holds at most 8 GiB of buffer for one transfer, while
/// staying well above the bandwidth-delay product of a 100 Gbps link (so the gate
/// does not bind a consumer that keeps pace).
///
/// Provisional: when a global memory budget exists the window is sized from it
/// (`memory_budget / part_size`) and arbitrated across transfers rather than fixed
/// per transfer (see `flow-control-architecture.md` §7b, ladder rung 1).
///
/// The default for the public [`ReadAhead::Auto`](crate::types::ReadAhead) mode;
/// `Handle::download_read_ahead_window` resolves the knob to a window in parts.
pub(crate) const DEFAULT_WINDOW_PARTS: u64 = 1024;

/// Map the public [`ReadAhead`](crate::types::ReadAhead) knob to a window in parts.
/// `Auto` is the fixed per-transfer cap (a future memory budget will size it);
/// `Parts(n)` is `n + 1` — `n` parts of speculation beyond the part the consumer is
/// waiting on, which is always admitted, so `Parts(0)` is demand paging. The single
/// definition of the knob's meaning, used at transfer construction and by the
/// dynamic control surface.
pub(crate) fn window_parts_for(mode: &crate::types::ReadAhead) -> u64 {
    match mode {
        crate::types::ReadAhead::Auto => DEFAULT_WINDOW_PARTS,
        crate::types::ReadAhead::Parts(n) => (*n as u64).saturating_add(1),
    }
}

/// Per-transfer read-ahead window — the receive-window (rwnd) bound on speculative
/// issuance.
///
/// `window()` is the hot read (the issuance gate, per claim) and is lock-free.
/// Today the value is fixed at construction; the [`AtomicU64`] exists so the
/// deferred budget-clamp and `Auto` adaptation layers can lower it without a lock.
/// Shared `&self` across the issuer and consumer tasks.
pub(crate) struct ReadAhead {
    /// Current window in parts. Read by the gate on every claim.
    window: AtomicU64,
}

impl ReadAhead {
    /// Create a controller with the default fixed window ([`DEFAULT_WINDOW_PARTS`]).
    pub(crate) fn new() -> Self {
        Self::with_window(DEFAULT_WINDOW_PARTS)
    }

    /// Create a controller with an explicit window, in parts. The resolved value of
    /// the public [`ReadAhead`](crate::types::ReadAhead) knob enters here.
    pub(crate) fn with_window(parts: u64) -> Self {
        Self {
            window: AtomicU64::new(parts),
        }
    }

    /// The current read-ahead window, in parts. Lock-free; the issuance gate reads
    /// this per claim (`issued - consumed < window()`).
    pub(crate) fn window(&self) -> u64 {
        self.window.load(Ordering::Relaxed)
    }

    /// Set the window, in parts. Lock-free. Used to apply a dynamic read-ahead
    /// change to a running transfer; the next gate read observes the new value.
    pub(crate) fn set_window(&self, parts: u64) {
        self.window.store(parts, Ordering::Relaxed);
    }

    /// Test-only: force the window to a fixed value, so transfer-level tests can
    /// drive the issuance gate deterministically (e.g. exercise the closed-gate
    /// path without claiming a thousand parts).
    #[cfg(test)]
    pub(crate) fn force_window(&self, w: u64) {
        self.set_window(w);
    }
}

impl Default for ReadAhead {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ReadAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadAhead")
            .field("window", &self.window.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_opens_at_the_fixed_default() {
        // rwnd: the window is the fixed per-transfer ceiling from construction. No
        // warm-up, no slow-start, no estimate to wait on.
        let ra = ReadAhead::new();
        assert_eq!(ra.window(), DEFAULT_WINDOW_PARTS);
    }

    #[test]
    fn window_does_not_drift_on_its_own() {
        // The controller never changes the window itself; only an explicit
        // set_window (the budget clamp or the dynamic knob) moves it. Repeated reads
        // with no writer are stable.
        let ra = ReadAhead::new();
        let w = ra.window();
        for _ in 0..1000 {
            assert_eq!(ra.window(), w, "window must not drift");
        }
    }

    #[test]
    fn set_window_updates_the_value() {
        let ra = ReadAhead::new();
        ra.set_window(7);
        assert_eq!(ra.window(), 7);
        ra.set_window(1);
        assert_eq!(ra.window(), 1);
    }

    #[test]
    fn force_window_overrides_for_gate_tests() {
        let ra = ReadAhead::new();
        ra.force_window(3);
        assert_eq!(ra.window(), 3);
    }

    #[test]
    fn window_parts_for_maps_the_knob() {
        use crate::types::ReadAhead as Knob;
        // Auto -> the fixed default.
        assert_eq!(window_parts_for(&Knob::Auto), DEFAULT_WINDOW_PARTS);
        // Parts(n) -> n + 1: n speculative parts plus the always-admitted demand part.
        assert_eq!(window_parts_for(&Knob::Parts(0)), 1, "Parts(0) is demand paging");
        assert_eq!(window_parts_for(&Knob::Parts(1)), 2);
        assert_eq!(window_parts_for(&Knob::Parts(255)), 256);
        // The + 1 saturates rather than overflowing at the top of the range.
        assert_eq!(
            window_parts_for(&Knob::Parts(usize::MAX)),
            (usize::MAX as u64).saturating_add(1)
        );
    }
}
