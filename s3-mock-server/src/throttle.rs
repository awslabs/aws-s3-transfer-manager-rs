/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Server-wide, load-driven throttling that recovers.
//!
//! Models an S3 prefix shedding a burst of load and then relenting — the shape a
//! high-fan-out transfer provokes. Unlike the per-`(bucket, key)`
//! [`FaultRegistry`](crate::faults::FaultRegistry), a [`RateThrottle`] is scoped
//! to the whole server, so it models "the service is throttling *you*" across
//! every object in a fan-out. A persistent storm (one that never relents) is the
//! [`Always`](crate::faults::Occurrence::Always) service-error fault's job; a
//! [`RateThrottle`] always recovers once the client's arrival rate drops.

/// A load-driven throttle: a token bucket that admits requests up to a sustained
/// `rate` per second (with a `burst` allowance) and sheds the rest, relenting as
/// the client's arrival rate drops back under `rate`. [`try_take`](Self::try_take)
/// returns the admit/shed verdict; mapping a shed to a 503 `SlowDown` response is
/// the caller's job (see the server's request handlers).
///
/// Recovery is contingent on client behavior: a transfer that never backs off
/// never lets the rate drop. Rate (not in-flight concurrency) is what accumulates
/// under a burst of near-instant requests, so it produces real backpressure the
/// mock's fast in-memory serving otherwise would not.
///
/// Which request is shed is not fixed — it depends on arrival timing. The
/// invariant is behavioral: an arrival rate above `rate` throttles, and dropping
/// under it recovers. The non-determinism is deliberate — across runs it exercises
/// the client's retry/backoff path against varied interleavings rather than one
/// fixed schedule.
#[derive(Debug)]
pub struct RateThrottle {
    /// `(available_tokens, last_refill)` under one lock. Tokens are fractional so
    /// a sub-second gap refills proportionally.
    state: std::sync::Mutex<(f64, std::time::Instant)>,
    /// Sustained admits per second (token refill rate).
    rate: f64,
    /// Maximum token accumulation, bounding the instantaneous burst admitted.
    burst: f64,
}

impl RateThrottle {
    /// A token bucket refilling at `rate` tokens/sec, capped at `burst` tokens,
    /// starting full.
    pub(crate) fn new(rate: f64, burst: f64) -> Self {
        Self {
            state: std::sync::Mutex::new((burst, std::time::Instant::now())),
            rate,
            burst,
        }
    }

    /// Refill by elapsed time, then take one token. `true` admits the request;
    /// `false` sheds it (bucket empty). Time is a parameter so callers pass the
    /// wall clock in production and a fixed instant in tests.
    pub(crate) fn try_take(&self, now: std::time::Instant) -> bool {
        let mut guard = self.state.lock().unwrap();
        let (tokens, last) = &mut *guard;
        let elapsed = now.saturating_duration_since(*last).as_secs_f64();
        *tokens = (*tokens + elapsed * self.rate).min(self.burst);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn rate_admits_up_to_burst_then_sheds() {
        // Starts full at `burst`; with no time advance only `burst` requests are
        // admitted, then the bucket is empty and the rest are shed.
        let rt = RateThrottle::new(10.0, 5.0);
        let t0 = Instant::now();
        let admitted = (0..20).filter(|_| rt.try_take(t0)).count();
        assert_eq!(admitted, 5, "exactly `burst` admitted with no refill");
    }

    #[test]
    fn rate_refills_over_time() {
        // Drain the burst, then a one-second gap refills `rate` tokens. Burst is
        // set to `rate` so the refill is not clipped by the cap (see the cap test).
        let rt = RateThrottle::new(10.0, 10.0);
        let t0 = Instant::now();
        for _ in 0..10 {
            assert!(rt.try_take(t0));
        }
        assert!(!rt.try_take(t0), "bucket drained");
        let t1 = t0 + Duration::from_secs(1);
        let admitted = (0..30).filter(|_| rt.try_take(t1)).count();
        assert_eq!(admitted, 10, "one second refills exactly `rate` tokens");
    }

    #[test]
    fn rate_refill_is_capped_at_burst() {
        // A long idle gap cannot accumulate more than `burst` tokens.
        let rt = RateThrottle::new(10.0, 5.0);
        let t0 = Instant::now();
        for _ in 0..5 {
            assert!(rt.try_take(t0));
        }
        let t1 = t0 + Duration::from_secs(100); // would refill 1000 uncapped
        let admitted = (0..20).filter(|_| rt.try_take(t1)).count();
        assert_eq!(
            admitted, 5,
            "accumulation is capped at `burst`, not rate*elapsed"
        );
    }

    #[test]
    fn rate_sub_second_gap_refills_proportionally() {
        // Fractional tokens: at 10/sec, 100ms refills exactly one token.
        let rt = RateThrottle::new(10.0, 1.0);
        let t0 = Instant::now();
        assert!(rt.try_take(t0), "first (burst) token");
        assert!(!rt.try_take(t0), "drained");
        assert!(
            !rt.try_take(t0 + Duration::from_millis(50)),
            "50ms = 0.5 token, still below 1"
        );
        assert!(
            rt.try_take(t0 + Duration::from_millis(100)),
            "100ms accumulates a whole token"
        );
    }
}
