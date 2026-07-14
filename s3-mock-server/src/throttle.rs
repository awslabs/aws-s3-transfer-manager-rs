/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Server-wide, request-counted throttling that recovers.
//!
//! Models an S3 prefix throttling a burst of load and then relenting — the
//! shape a high-fan-out transfer provokes. Unlike the per-`(bucket, key)`
//! [`FaultRegistry`](crate::faults::FaultRegistry), a [`ThrottleSchedule`] is
//! scoped to the whole server and advances by a global request ordinal, so it
//! models "the service is throttling *you*" across every object in a fan-out.
//!
//! Deterministic and repeatable: the verdict for the Nth qualifying request is a
//! pure function of the ordinal ([`ThrottleSchedule::verdict`]), never of timing
//! or randomness. Request *order* across concurrent handlers is not fixed, but
//! the *count* of throttled requests is — tests assert transfer-level outcome,
//! not which object was throttled. A permanent storm is the
//! [`Always`](crate::faults::Occurrence::Always) fault's job; a `ThrottleSchedule`
//! always recovers once its phases are consumed.

use std::sync::atomic::{AtomicU64, Ordering};

/// One phase of a [`ThrottleSchedule`]: for its `count` requests, throttle
/// `fraction` of them (0.0 = none, 1.0 = all).
#[derive(Debug, Clone, Copy)]
struct Phase {
    count: u64,
    fraction: f64,
}

/// A server-wide throttle that advances by request count through a sequence of
/// phases, then passes all further requests (recovers).
///
/// Build with [`builder`](Self::builder). The schedule holds no state; the
/// running ordinal lives in [`ThrottleState`].
#[derive(Debug, Clone, Default)]
pub struct ThrottleSchedule {
    phases: Vec<Phase>,
}

impl ThrottleSchedule {
    /// Start building a schedule.
    pub fn builder() -> ThrottleScheduleBuilder {
        ThrottleScheduleBuilder { phases: Vec::new() }
    }

    /// Whether the request at zero-based `ordinal` is throttled.
    ///
    /// Pure and deterministic. Walks the phases in order: `ordinal` past all
    /// phases returns `false` (recovered). Within a phase, `fraction` is applied
    /// by position — throttle iff `floor(k · f) != floor((k − 1) · f)` for `k`
    /// the 1-based position within the phase — which throttles exactly `fraction`
    /// of the phase's requests, evenly spread, with no randomness.
    pub(crate) fn verdict(&self, ordinal: u64) -> bool {
        let mut start = 0u64;
        for phase in &self.phases {
            let end = start.saturating_add(phase.count);
            if ordinal < end {
                let k = ordinal - start + 1; // 1-based position within the phase
                let f = phase.fraction;
                return (k as f64 * f).floor() as i64 != ((k - 1) as f64 * f).floor() as i64;
            }
            start = end;
        }
        false
    }
}

/// Builder for a [`ThrottleSchedule`]. Phases apply in the order added; once all
/// are consumed the schedule recovers (further requests pass).
#[derive(Debug)]
pub struct ThrottleScheduleBuilder {
    phases: Vec<Phase>,
}

impl ThrottleScheduleBuilder {
    /// The next `count` requests pass untouched. Sugar for `throttled(count, 0.0)`.
    pub fn healthy(self, count: u64) -> Self {
        self.throttled(count, 0.0)
    }

    /// The next `count` requests throttle `fraction` of themselves (0.0 = none,
    /// 1.0 = all), applied deterministically by position.
    pub fn throttled(mut self, count: u64, fraction: f64) -> Self {
        self.phases.push(Phase {
            count,
            fraction: fraction.clamp(0.0, 1.0),
        });
        self
    }

    /// Finish the schedule.
    pub fn build(self) -> ThrottleSchedule {
        ThrottleSchedule {
            phases: self.phases,
        }
    }
}

/// A [`ThrottleSchedule`] plus the running request ordinal, shared across the
/// server's request handlers.
#[derive(Debug, Default)]
pub(crate) struct ThrottleState {
    schedule: ThrottleSchedule,
    next_ordinal: AtomicU64,
}

impl ThrottleState {
    pub(crate) fn new(schedule: ThrottleSchedule) -> Self {
        Self {
            schedule,
            next_ordinal: AtomicU64::new(0),
        }
    }

    /// Claim the next request ordinal and return whether it is throttled. The
    /// ordinal is monotonic across concurrent handlers, so the count throttled is
    /// deterministic even when request order is not.
    pub(crate) fn throttle_next(&self) -> bool {
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::Relaxed);
        self.schedule.verdict(ordinal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_schedule_never_throttles() {
        let s = ThrottleSchedule::builder().build();
        for ordinal in [0, 1, 100, 10_000] {
            assert!(!s.verdict(ordinal));
        }
    }

    #[test]
    fn healthy_then_storm_then_recovered_boundaries() {
        // 50 healthy, 100 storm, then recovered — pin the off-by-one at each edge.
        let s = ThrottleSchedule::builder()
            .healthy(50)
            .throttled(100, 1.0)
            .build();

        assert!(!s.verdict(0), "first request is healthy");
        assert!(!s.verdict(49), "last healthy request");
        assert!(s.verdict(50), "first storm request");
        assert!(s.verdict(149), "last storm request");
        assert!(!s.verdict(150), "recovered immediately after the storm");
        assert!(!s.verdict(10_000), "recovered stays recovered");
    }

    #[test]
    fn fraction_is_deterministic_and_evenly_spread() {
        // fraction 0.5 over a phase throttles exactly every other request, and the
        // same set on every call (no randomness).
        let s = ThrottleSchedule::builder().throttled(10, 0.5).build();
        let throttled: Vec<u64> = (0..10).filter(|&o| s.verdict(o)).collect();
        assert_eq!(throttled.len(), 5, "exactly half of 10 throttled");
        // Repeatable: identical set on a second pass.
        let again: Vec<u64> = (0..10).filter(|&o| s.verdict(o)).collect();
        assert_eq!(throttled, again);
    }

    #[test]
    fn fraction_edges_none_and_all() {
        let none = ThrottleSchedule::builder().throttled(20, 0.0).build();
        assert!((0..20).all(|o| !none.verdict(o)));

        let all = ThrottleSchedule::builder().throttled(20, 1.0).build();
        assert!((0..20).all(|o| all.verdict(o)));
    }

    #[test]
    fn fraction_count_is_exact_across_rates() {
        // For a range of fractions, the number throttled in a 1000-request phase
        // is floor(1000 * fraction) — exact, not approximate.
        for (f, expected) in [(0.1, 100), (0.25, 250), (0.333, 333), (0.9, 900)] {
            let s = ThrottleSchedule::builder().throttled(1000, f).build();
            let n = (0..1000).filter(|&o| s.verdict(o)).count();
            assert_eq!(n, expected, "fraction {f} over 1000 requests");
        }
    }

    #[test]
    fn tiered_recovery_phases_apply_in_order() {
        // storm (all) -> partial (half) -> recovered.
        let s = ThrottleSchedule::builder()
            .throttled(100, 1.0)
            .throttled(200, 0.5)
            .build();

        assert!(s.verdict(0) && s.verdict(99), "storm phase all throttled");
        let partial = (100..300).filter(|&o| s.verdict(o)).count();
        assert_eq!(partial, 100, "partial phase throttles half of 200");
        assert!(!s.verdict(300), "recovered after both phases");
    }

    #[test]
    fn state_ordinal_is_monotonic_and_maps_to_verdict() {
        // ThrottleState claims ordinals in order and applies the schedule.
        let state = ThrottleState::new(
            ThrottleSchedule::builder()
                .healthy(2)
                .throttled(2, 1.0)
                .build(),
        );
        assert!(!state.throttle_next(), "ordinal 0 healthy");
        assert!(!state.throttle_next(), "ordinal 1 healthy");
        assert!(state.throttle_next(), "ordinal 2 storm");
        assert!(state.throttle_next(), "ordinal 3 storm");
        assert!(!state.throttle_next(), "ordinal 4 recovered");
    }
}
