/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-transfer read-ahead window: the occupancy bound on speculative issuance.
//!
//! Issuance of a speculative part requires `issued - released < window()`, where
//! `issued` counts parts claimed for issuance and `released` counts parts freed by
//! either delivery surface (stream pull or disk drain; see `recv_buffer`). The
//! quantity `issued - released` is the transfer's resident occupancy in parts, so
//! the gate bounds how far issuance runs ahead of consumption and nothing more.
//! It is one of three independent upper bounds on issuance; the others — total
//! in-flight concurrency and the aggregate memory budget — live elsewhere, and
//! issuance takes their min.
//!
//! The window is free buffer space, `window - occupancy`, an exact subtraction
//! recomputed as the buffer fills and drains. It is not estimated from a rate and
//! it does not decay, so a gap in delivery does not shrink it: a consumer paused
//! between bursts, or blocked behind a not-yet-filled part, still leaves a full
//! window of slack to make progress.
//!
//! # Pacing
//!
//! The window is a fixed per-transfer cap ([`DEFAULT_WINDOW_PARTS`]). The pacing
//! behaviors follow from the occupancy gate directly:
//!
//! - Fast consumer: `released` keeps pace, `issued - released` stays small, the
//!   gate does not bind, and the transfer runs at the concurrency limit.
//! - Slow consumer: `released` lags, `issued - released` fills to the window, the
//!   gate closes, and issuance pauses until the consumer drains. Resident memory is
//!   bounded at `window` parts.
//! - Blocked consumer (a stalled or missing part): `released` cannot advance past
//!   the hole, occupancy pins at the window, and issuance stops at `window`, leaving
//!   a full window of slack to make progress around the hole rather than collapsing
//!   to one part.
//!
//! The window is stored in an [`AtomicU64`] so it can be lowered with a lock-free
//! store while a transfer runs (see [`ReadAhead::set_window`]).

use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed read-ahead window, in parts. A per-transfer cap on resident occupancy:
/// with an 8 MiB part this holds at most 8 GiB of buffer for one transfer, while
/// staying well above the bandwidth-delay product of a 100 Gbps link (so the gate
/// does not bind a consumer that keeps pace).
///
/// The default for the public [`ReadAhead::Auto`](crate::types::ReadAhead) mode;
/// [`window_parts_for`] resolves the knob to a window in parts.
pub(crate) const DEFAULT_WINDOW_PARTS: u64 = 1024;

/// Resolve the effective read-ahead window in parts for a download, in precedence
/// order: the per-request [`ReadAhead`](crate::types::ReadAhead) override if set,
/// else the client default. See [`window_parts_for`] for the per-mode mapping.
pub(crate) fn resolve_window(
    request: Option<&crate::types::ReadAhead>,
    client_default: &crate::types::ReadAhead,
) -> u64 {
    window_parts_for(request.unwrap_or(client_default))
}

/// Map the public [`ReadAhead`](crate::types::ReadAhead) knob to a window in parts.
/// `Auto` is the fixed per-transfer cap; `Parts(n)` is `n + 1` — `n` parts of
/// speculation beyond the part the consumer is waiting on, which is always admitted,
/// so `Parts(0)` is demand paging. The single definition of the knob's meaning, used
/// at transfer construction and by the dynamic control surface.
pub(crate) fn window_parts_for(mode: &crate::types::ReadAhead) -> u64 {
    match mode {
        crate::types::ReadAhead::Auto => DEFAULT_WINDOW_PARTS,
        crate::types::ReadAhead::Parts(n) => (*n as u64).saturating_add(1),
    }
}

/// Per-transfer read-ahead window: the occupancy bound on speculative issuance.
///
/// `window()` is the hot read (the issuance gate, per claim) and is lock-free. The
/// value is fixed at construction; the [`AtomicU64`] allows it to be lowered with a
/// lock-free store while the transfer runs. Shared `&self` across the issuer and
/// consumer tasks.
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
    /// this per claim (`issued - released < window()`).
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
        assert_eq!(
            window_parts_for(&Knob::Parts(0)),
            1,
            "Parts(0) is demand paging"
        );
        assert_eq!(window_parts_for(&Knob::Parts(1)), 2);
        assert_eq!(window_parts_for(&Knob::Parts(255)), 256);
        // The + 1 saturates rather than overflowing at the top of the range.
        assert_eq!(
            window_parts_for(&Knob::Parts(usize::MAX)),
            (usize::MAX as u64).saturating_add(1)
        );
    }
}
