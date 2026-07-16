/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Adaptive per-request deadline for the retryable data plane.
//!
//! The deadline is a control variable, not a percentile recomputed per call:
//! successful requests relax it toward observed latency, clustered timeouts
//! widen it in tiers, and the widening decays back out as successes resume. The
//! design is adapted from CRT's upload-part timeout controller
//! (`aws_s3_client_update_upload_part_timeout`, aws-c-s3 `s3_client.c`).

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Successful samples required before a deadline is applied. Below this the
/// controller only observes latency and buffers samples for the seed;
/// [`LatencyTracker::guarded`] runs untimed.
const WARM_THRESHOLD: u64 = 10;

/// Floor for the initial deadline seed. The seed is `max(p90, SEED_FLOOR_US)`,
/// so a link whose warmup is uniformly fast still starts with a full second of
/// tolerance rather than a sub-`offset` deadline. CRT's `1s` init floor.
const SEED_FLOOR_US: f64 = 1_000_000.0;

/// Aging factor for the observed-latency mean: `mean ← (1−α)·mean + α·sample`.
///
/// CRT uses a lifetime cumulative mean that never ages; this ages it so a
/// lasting shift in network conditions moves the deadline target instead of
/// being diluted by all-time history. α = 1/64 gives an effective memory of
/// ~64 recent requests.
const MEAN_EWMA_ALPHA: f64 = 1.0 / 64.0;

/// Damping factor for the deadline as it tracks its target: `deadline ←
/// (1−α)·deadline + α·(mean + offset)`.
///
/// CRT's value (`0.99·current + 0.01·expected`). Small on purpose: a tier bump
/// applied on a timeout decays back out over ~100 subsequent successes rather
/// than being erased by the next one, so a burst of timeouts leaves a lasting
/// widening.
const VALUE_EWMA_ALPHA: f64 = 0.01;

/// Safety margin added to the observed mean to form the deadline target.
///
/// CRT's `g_expect_timeout_offset_ms` = 700ms, field-tuned for upload
/// response-to-first-byte; reused unchanged as the download time-to-first-byte
/// margin, which is not separately tuned.
const EXPECTED_OFFSET_US: f64 = 700_000.0;

/// Escape hatch: once the deadline target or a widened deadline reaches this,
/// the deadline is disabled. Past it, re-issuing costs more than waiting for the
/// slow response. CRT's 5s `s_upload_timeout_threshold_ns`.
const ESCAPE_US: f64 = 5_000_000.0;

/// Deadline increment at the moderate timeout-rate tier (> 0.1% of completions).
const MODERATE_BUMP_US: f64 = 100_000.0;

/// Deadline increment at the high timeout-rate tier (> 1% of completions).
const HIGH_BUMP_US: f64 = 1_000_000.0;

/// Completion count at which the rate window halves both counters.
///
/// The tier denominator (`rate_completed`) would otherwise grow with a
/// long-lived client's lifetime volume, so a later burst — measured against
/// millions of past completions — could never reach `completed/100`, defeating
/// the widening on exactly the clients it targets. Halving at this cap keeps the
/// denominator recent. CRT resets only on a high-tier crossing, which suffices
/// for its short-lived per-session stat block but not for a client handle shared
/// across many transfers.
const RATE_WINDOW: u64 = 10_000;

/// Outcome of a single [`LatencyTracker::guarded`] attempt that did not succeed.
pub(crate) enum GuardError<E> {
    /// The future did not complete within the adaptive deadline (the carried
    /// [`Duration`]). The in-flight future was dropped, releasing its HTTP
    /// connection, so a retry runs on a fresh one. Carries no inner error.
    DeadlineExceeded(Duration),
    /// The guarded future produced an error before the deadline.
    Inner(E),
}

/// The `q`-quantile of `samples` by nearest-rank, sorting in place.
///
/// `q` is clamped to `[0, 1]`. Panics if `samples` is empty; the seed path calls
/// it only at [`WARM_THRESHOLD`] samples. The rank is `ceil(q · n)` clamped to a
/// valid index, so `p90` of 10 samples is the 9th smallest — the min of the
/// slowest tenth, matching CRT's p90 min-heap.
fn percentile(samples: &mut [f64], q: f64) -> f64 {
    assert!(!samples.is_empty(), "percentile of no samples");
    samples.sort_by(|a, b| a.total_cmp(b));
    let n = samples.len();
    let rank = (q.clamp(0.0, 1.0) * n as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(n - 1);
    samples[idx]
}

/// One exponentially-weighted moving average. `None` until the first sample, so
/// the first `observe` seeds the average rather than blending against a
/// synthetic zero.
struct Ewma {
    alpha: f64,
    value: Option<f64>,
}

impl Ewma {
    const fn new(alpha: f64) -> Self {
        Self { alpha, value: None }
    }

    /// Fold `sample` in and return the updated average.
    fn observe(&mut self, sample: f64) -> f64 {
        let next = match self.value {
            Some(v) => (1.0 - self.alpha) * v + self.alpha * sample,
            None => sample,
        };
        self.value = Some(next);
        next
    }
}

/// Adaptive per-request deadline held as a control variable.
///
/// The first [`WARM_THRESHOLD`] successes are buffered, not timed. At the
/// threshold the deadline is seeded once at `max(p90, `[`SEED_FLOOR_US`]`)` over
/// the buffered samples — a tail percentile, not the mean, so a high-variance
/// link whose fast requests dominate the mean still starts with a deadline that
/// clears its slow tail. If the arithmetic mean of the buffered samples reaches
/// [`ESCAPE_US`] the link is uniformly too slow to time and no deadline is ever
/// armed (`stop_timeout`). This gate uses the arithmetic warmup mean, distinct
/// from `mean_ttfb`, the aging EWMA that drives the steady-state target below.
///
/// Thereafter:
///   - **success** relaxes the deadline toward `mean + `[`EXPECTED_OFFSET_US`] by
///     [`VALUE_EWMA_ALPHA`], and ages the mean by [`MEAN_EWMA_ALPHA`];
///   - **timeout** widens the deadline in tiers keyed on the recent timeout
///     rate (`failed`/`completed` over a bounded window, see [`RATE_WINDOW`]):
///     `+`[`MODERATE_BUMP_US`] above 0.1%, `+`[`HIGH_BUMP_US`] above 1%.
///
/// A timed-out request never updates the mean, so a bad regime cannot ratchet
/// the deadline tighter — the failure mode of a percentile computed only over
/// successes. When the value that would apply reaches [`ESCAPE_US`] the deadline
/// is disabled: a link that genuinely takes this long is not worth re-issuing.
/// The escape is not sticky — the mean keeps updating on the untimed path, so a
/// later healthy run re-arms the deadline, which matters for a client handle
/// shared across a long-lived client's transfers.
struct DeadlineController {
    /// Aging mean of observed successful-request latency, microseconds.
    mean_ttfb: Ewma,
    /// Successful samples, gating warmup.
    successes: u64,
    /// Warmup latencies (µs), buffered until [`WARM_THRESHOLD`] and consumed once
    /// to seed the deadline at their p90, then cleared. Empty outside warmup.
    warmup_samples: Vec<f64>,
    /// Set at the seed point when the arithmetic warmup mean reaches [`ESCAPE_US`]:
    /// the link is uniformly slower than re-issuing helps, so no deadline is ever
    /// armed. Distinct from the escape, which re-arms; this startup decision does
    /// not.
    stop_timeout: bool,
    /// The control variable, microseconds. `None` while cold or escaped.
    deadline_us: Option<f64>,
    /// Completions in the current rate window (the tier denominator).
    rate_completed: u64,
    /// Timeouts in the current rate window (the tier numerator).
    rate_failed: u64,
    /// Lifetime timeout count; not read by the control loop (Debug + tests).
    lifetime_timeouts: u64,
}

impl DeadlineController {
    fn new() -> Self {
        Self {
            mean_ttfb: Ewma::new(MEAN_EWMA_ALPHA),
            successes: 0,
            warmup_samples: Vec::new(),
            stop_timeout: false,
            deadline_us: None,
            rate_completed: 0,
            rate_failed: 0,
            lifetime_timeouts: 0,
        }
    }

    /// The current deadline, or `None` when cold or escaped.
    fn deadline(&self) -> Option<Duration> {
        self.deadline_us.map(|us| Duration::from_micros(us as u64))
    }

    /// Fold a successful request's latency in: age the mean, advance the rate
    /// window, then seed the deadline once at warm and thereafter relax it toward
    /// `mean + offset`, decaying out prior tier bumps.
    fn record_success(&mut self, latency: Duration) {
        let sample_us = latency.as_micros() as f64;
        let mean = self.mean_ttfb.observe(sample_us);
        self.successes += 1;
        self.record_completion();

        // Warmup: buffer the sample and arm nothing.
        if self.successes < WARM_THRESHOLD {
            self.warmup_samples.push(sample_us);
            return;
        }
        // Seed once, at the warm threshold.
        if self.successes == WARM_THRESHOLD {
            self.warmup_samples.push(sample_us);
            // A link whose warmup is UNIFORMLY slower than the escape is slower
            // than re-issuing helps: never arm a deadline for it. Gated on the
            // arithmetic mean of the buffered samples, not `mean_ttfb` (the aging
            // EWMA, which weights the first sample ~0.87 over ten samples) — so a
            // single cold-start request cannot latch a healthy link off.
            let warmup_mean =
                self.warmup_samples.iter().sum::<f64>() / self.warmup_samples.len() as f64;
            if warmup_mean >= ESCAPE_US {
                self.stop_timeout = true;
                self.warmup_samples = Vec::new();
                return;
            }
            // Seed at the tail (p90), floored: the deadline must clear the slow
            // tail of a high-variance link, which a mean-based seed would sit
            // under and false-cancel. A p90 past the escape (tail-slow but not
            // uniformly slow) arms nothing yet stays re-armable, unlike the
            // uniformly-slow latch above.
            let seed = percentile(&mut self.warmup_samples, 0.90).max(SEED_FLOOR_US);
            self.warmup_samples = Vec::new();
            self.deadline_us = (seed < ESCAPE_US).then_some(seed);
            return;
        }
        // A link classified too slow to time at seed stays untimed for life.
        if self.stop_timeout {
            return;
        }
        let target = mean + EXPECTED_OFFSET_US;
        // Damp toward the target from the active deadline; re-seed at the target
        // when re-arming after an escape (the warmup buffer is long gone).
        let next = match self.deadline_us {
            Some(current) => (1.0 - VALUE_EWMA_ALPHA) * current + VALUE_EWMA_ALPHA * target,
            None => target,
        };
        // Escape is not sticky: disable the deadline while the value that would
        // apply reaches the threshold, and re-arm automatically once observed
        // latency (which keeps updating on the untimed path) recovers. Checked
        // on the damped `next`, not the raw target, so a transient spike does
        // not latch the deadline off.
        let re_arming = self.deadline_us.is_none();
        self.deadline_us = (next < ESCAPE_US).then_some(next);
        // Clear the rate window when re-arming from an escape that accumulated
        // failures: while escaped, in-flight stragglers keep incrementing
        // `rate_failed` (record_timeout increments before its no-deadline early
        // return) with no tier reset, so a re-armed deadline would otherwise
        // inherit an outage-era failure count and the first post-recovery timeout
        // would over-bump or immediately re-escape. Gated on `rate_failed > 0` so
        // the initial cold→warm arming (an all-success window, nothing to clear)
        // keeps its accumulated denominator.
        if re_arming && self.deadline_us.is_some() && self.rate_failed > 0 {
            self.rate_completed = 0;
            self.rate_failed = 0;
        }
    }

    /// Fold a timed-out request in and widen the deadline per the recent rate.
    ///
    /// `issued` is the deadline the timed-out attempt was launched with. It
    /// gates the bump so that concurrent stragglers issued at the same value do
    /// not each stack a bump a peer already applied (CRT's stale-timeout guard):
    /// a bump lands only if `issued + bump` would exceed the current deadline.
    fn record_timeout(&mut self, issued: Duration) {
        self.rate_failed = self.rate_failed.saturating_add(1);
        self.record_completion();
        self.lifetime_timeouts += 1;

        let Some(current) = self.deadline_us else {
            return; // cold or escaped: nothing to widen
        };
        let issued_us = issued.as_micros() as f64;

        // `ceil` denominators so an isolated timeout cannot trip a tier on a
        // small sample: high needs failed > completed/100, moderate > /1000. The
        // `issued + bump > current` gate suppresses a straggler issued under an
        // older, smaller deadline from stacking a bump a peer already applied.
        let failed = self.rate_failed as f64;
        let high = failed > (self.rate_completed as f64 / 100.0).ceil();
        let moderate = failed > (self.rate_completed as f64 / 1000.0).ceil();
        let (widened, high_applied) = if high && issued_us + HIGH_BUMP_US > current {
            (Some(current + HIGH_BUMP_US), true)
        } else if moderate && issued_us + MODERATE_BUMP_US > current {
            (Some(current + MODERATE_BUMP_US), false)
        } else {
            (None, false)
        };

        // Reset the window only when a HIGH bump actually lands, so the next rate
        // is measured against the widened deadline. Resetting on a gate-suppressed
        // crossing would discard legitimate failure evidence and delay the next
        // real escalation.
        if high_applied {
            self.rate_completed = 0;
            self.rate_failed = 0;
        }

        // Escape is not sticky (see `record_success`): a bump past the threshold
        // disables the deadline; a later healthy run re-arms it.
        if let Some(w) = widened {
            self.deadline_us = (w < ESCAPE_US).then_some(w);
        }
    }

    /// Count one completion (success or timeout) into the rate window, halving
    /// both counters when the window fills. Halving preserves the failure ratio
    /// while bounding the denominator, so the tier thresholds stay sensitive to
    /// a recent burst instead of being diluted by a long-lived client's lifetime
    /// volume — without the sensitivity discontinuity a reset-to-zero would add
    /// at the window boundary.
    fn record_completion(&mut self) {
        self.rate_completed = self.rate_completed.saturating_add(1);
        if self.rate_completed >= RATE_WINDOW {
            self.rate_completed /= 2;
            self.rate_failed /= 2;
        }
    }
}

/// Tracks per-operation latency and computes an adaptive deadline.
///
/// Shared across a client's concurrent requests, so the controller is guarded
/// by a mutex. Each operation is O(1) and the lock is never held across an
/// `await`.
pub(crate) struct LatencyTracker {
    controller: Mutex<DeadlineController>,
}

impl LatencyTracker {
    /// Create a new tracker with no samples.
    pub(crate) fn new() -> Self {
        Self {
            controller: Mutex::new(DeadlineController::new()),
        }
    }

    /// Record a completed request's latency.
    pub(crate) fn record(&self, latency: Duration) {
        self.controller.lock().unwrap().record_success(latency);
    }

    /// Record a timed-out request. `issued` is the deadline the attempt ran
    /// under, used to gate tier bumps against concurrent stragglers.
    pub(crate) fn record_timeout(&self, issued: Duration) {
        self.controller.lock().unwrap().record_timeout(issued);
    }

    /// Number of recorded timeouts over the tracker's lifetime. Test-only.
    #[cfg(test)]
    pub(crate) fn timeout_count(&self) -> u64 {
        self.controller.lock().unwrap().lifetime_timeouts
    }

    /// The current adaptive deadline, or `None` when cold or escaped.
    fn deadline(&self) -> Option<Duration> {
        self.controller.lock().unwrap().deadline()
    }

    /// Run `fut` once under the adaptive deadline.
    ///
    /// On success records the latency (warming the controller) and returns the
    /// value. When cold (no deadline) runs without a timeout, so the only
    /// failure is the inner error. When warm, races `fut` against the deadline:
    /// if it elapses, the in-flight future is dropped, a timeout is recorded
    /// against the issued deadline, and [`GuardError::DeadlineExceeded`] carries
    /// the deadline that was exceeded.
    ///
    /// A single attempt: the inner error is returned verbatim as
    /// [`GuardError::Inner`]. Retry and classification live in [`crate::retry`].
    pub(crate) async fn guarded<T, E, Fut>(&self, fut: Fut) -> Result<T, GuardError<E>>
    where
        Fut: Future<Output = Result<T, E>>,
    {
        let start = Instant::now();
        match self.deadline() {
            None => match fut.await {
                Ok(val) => {
                    self.record(start.elapsed());
                    Ok(val)
                }
                Err(e) => Err(GuardError::Inner(e)),
            },
            Some(dl) => match tokio::time::timeout(dl, fut).await {
                Ok(Ok(val)) => {
                    self.record(start.elapsed());
                    Ok(val)
                }
                Ok(Err(e)) => Err(GuardError::Inner(e)),
                Err(_timeout) => {
                    self.record_timeout(dl);
                    tracing::debug!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        deadline_ms = dl.as_millis() as u64,
                        "request exceeded latency deadline"
                    );
                    Err(GuardError::DeadlineExceeded(dl))
                }
            },
        }
    }
}

impl std::fmt::Debug for LatencyTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = self.controller.lock().unwrap();
        f.debug_struct("LatencyTracker")
            .field("successes", &c.successes)
            .field("timeouts", &c.lifetime_timeouts)
            .field("deadline", &c.deadline())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Microseconds in the current deadline, or panic — for arithmetic asserts.
    fn dl_us(c: &DeadlineController) -> f64 {
        c.deadline_us.expect("expected a warm deadline")
    }

    /// Drive `n` successes at a fixed latency.
    fn warm(c: &mut DeadlineController, latency: Duration, n: u64) {
        for _ in 0..n {
            c.record_success(latency);
        }
    }

    // --- warmup + seeding -----------------------------------------------------

    #[test]
    fn cold_until_warm_threshold() {
        let mut c = DeadlineController::new();
        // One shy of warm: still no deadline.
        warm(&mut c, Duration::from_millis(50), WARM_THRESHOLD - 1);
        assert_eq!(c.deadline(), None, "must stay cold below WARM_THRESHOLD");

        // The warm-th sample seeds the deadline at max(p90, floor). Uniform 50ms
        // samples give p90 = 50ms, so the SEED_FLOOR_US (1s) floor applies.
        c.record_success(Duration::from_millis(50));
        let seeded = dl_us(&c);
        assert!(
            (seeded - SEED_FLOOR_US).abs() < 1.0,
            "a fast warmup must seed at the floor ≈ {SEED_FLOOR_US}, got {seeded}"
        );
    }

    #[test]
    fn seed_tracks_the_warmup_tail_not_the_mean() {
        // A high-variance warmup: fast mass with a slow tail. The mean sits near
        // the fast floor, but the seed must clear the tail (p90), so a genuinely
        // slow-but-healthy request is not immediately cancelled. Eight samples at
        // 100ms and two at 3s: nearest-rank p90 of 10 is the 9th smallest = 3s
        // (the slowest tenth), well above both the mean (~680ms) and the 1s floor.
        let mut c = DeadlineController::new();
        for _ in 0..WARM_THRESHOLD - 2 {
            c.record_success(Duration::from_millis(100));
        }
        c.record_success(Duration::from_millis(3000));
        c.record_success(Duration::from_millis(3000));
        let seeded = dl_us(&c);
        assert!(
            (seeded - 3_000_000.0).abs() < 1.0,
            "seed must be the p90 tail (3s), got {seeded}"
        );
        // A mean-based seed would have landed near 680ms+700ms ≈ 1.38s, below the
        // tail — the exact clip this change prevents.
        assert!(
            seeded > 1_380_000.0,
            "seed must exceed the mean-based value it replaces"
        );
    }

    #[test]
    fn slow_warmup_never_arms_a_deadline() {
        // CRT's stop_timeout: if the warmup mean already reaches the escape, the
        // link is slower than re-issuing helps, so no deadline is ever armed and
        // later timeouts cannot arm one either.
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(5500), WARM_THRESHOLD);
        assert_eq!(
            c.deadline(),
            None,
            "a warmup mean past the escape must never arm a deadline"
        );
        assert!(c.stop_timeout, "the slow-link gate must latch");

        // Even a healthy run afterward stays untimed: the decision is permanent
        // (unlike the escape, which re-arms).
        warm(&mut c, Duration::from_millis(50), 500);
        assert_eq!(
            c.deadline(),
            None,
            "stop_timeout is not re-armable, unlike the escape"
        );
    }

    #[test]
    fn cold_first_sample_does_not_latch_a_healthy_link() {
        // The stop_timeout gate uses the arithmetic warmup mean, not the aging
        // EWMA. One cold first request (8s TLS/DNS setup) then nine fast ones:
        // arithmetic mean = (8000 + 9*50)/10 = 845ms, well under the escape, so
        // the link is timed normally. The EWMA weights that first sample ~0.87,
        // reporting ~7s and would falsely latch stop_timeout — the skew this gate
        // must not have.
        let mut c = DeadlineController::new();
        c.record_success(Duration::from_millis(8000));
        for _ in 0..WARM_THRESHOLD - 1 {
            c.record_success(Duration::from_millis(50));
        }
        assert!(
            !c.stop_timeout,
            "a single cold-start sample must not latch a healthy link off"
        );
        assert!(
            c.deadline().is_some(),
            "a link whose arithmetic warmup mean is under the escape must be timed"
        );
    }

    #[test]
    fn ewma_seeds_on_first_sample_not_against_zero() {
        // A raw EWMA blended against 0 would report alpha*sample on the first
        // observe; seeding must return the sample itself.
        let mut e = Ewma::new(0.01);
        assert_eq!(e.observe(1234.0), 1234.0);
    }

    // --- success relaxation ---------------------------------------------------

    #[test]
    fn success_relaxes_deadline_toward_target_slowly() {
        let mut c = DeadlineController::new();
        // Seed high off a slow tail (p90 = 3s) while the target (mean+offset) is
        // far lower, so a healthy run relaxes the deadline DOWN toward the target,
        // fractionally per sample (VALUE_EWMA_ALPHA = 0.01). Eight fast + two slow
        // warmup samples: p90 = 3s seed, mean ≈ 680ms → target ≈ 1.38s.
        for _ in 0..WARM_THRESHOLD - 2 {
            c.record_success(Duration::from_millis(100));
        }
        c.record_success(Duration::from_millis(3000));
        c.record_success(Duration::from_millis(3000));
        let seeded = dl_us(&c); // ≈ 3s
        assert!(
            (seeded - 3_000_000.0).abs() < 1.0,
            "precondition: p90 seed = 3s"
        );

        c.record_success(Duration::from_millis(100));
        let after_one = dl_us(&c);
        assert!(
            after_one < seeded,
            "a target below the seed must relax the deadline down"
        );
        // One step moves at most ~1% of the gap: nowhere near the new target.
        let moved_fraction = (seeded - after_one) / seeded;
        assert!(
            moved_fraction < 0.02,
            "one success should move the deadline <2%, moved {moved_fraction}"
        );

        // Over many fast samples it converges toward mean(→100ms) + 700ms.
        warm(&mut c, Duration::from_millis(100), 2000);
        let converged = dl_us(&c);
        assert!(
            (converged - (100_000.0 + EXPECTED_OFFSET_US)).abs() < 50_000.0,
            "should converge near 100ms+700ms, got {converged}"
        );
    }

    // --- timeout rate tiers ---------------------------------------------------

    #[test]
    fn isolated_timeout_after_long_healthy_run_does_not_widen() {
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), 5000);
        let before = dl_us(&c);

        // A single timeout against 5000 completions is 1/5001 ≈ 0.02% — below
        // both tiers (ceil(5001/1000) = 6, failed = 1). No widening.
        c.record_timeout(c.deadline().unwrap());
        assert_eq!(
            dl_us(&c),
            before,
            "one timeout in thousands must not widen the deadline"
        );
    }

    #[test]
    fn moderate_tier_widens_by_100ms_without_tripping_high() {
        let mut c = DeadlineController::new();
        // ~1000 completions separates the tiers: high needs failed >
        // ceil(1000/100)=11, moderate needs failed > ceil(1000/1000)=2. All
        // samples are 100ms, so after the p90 seed (1s) relaxes over ~990
        // successes the deadline converges to the target (100ms+700ms=800ms);
        // only tier bumps move it thereafter.
        warm(&mut c, Duration::from_millis(100), 1000);
        let base = dl_us(&c);
        assert!(
            (base - (100_000.0 + EXPECTED_OFFSET_US)).abs() < 100.0,
            "base must converge to the target ≈800ms, got {base}"
        );
        let issued = c.deadline().unwrap();

        // failed=1 (>ceil(1001/1000)=2? no) and failed=2 (>ceil(1002/1000)=2? no)
        // stay below MODERATE.
        c.record_timeout(issued);
        c.record_timeout(issued);
        assert_eq!(dl_us(&c), base, "failed ≤ 0.1% must not widen");

        // failed=3 > ceil(1003/1000)=2 trips MODERATE; well below high's 11.
        c.record_timeout(issued);
        let widened = dl_us(&c);
        assert!(
            (widened - (base + MODERATE_BUMP_US)).abs() < 1.0,
            "crossing 0.1% must add exactly MODERATE (+100ms): base {base}, got {widened}"
        );
    }

    #[test]
    fn high_tier_bump_resets_the_rate_window() {
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), WARM_THRESHOLD);
        let issued = c.deadline().unwrap();

        // With completed ≈ 10, high threshold = ceil(10/100) = 1. The 2nd
        // timeout (failed=2 > 1) trips HIGH and resets the counters.
        c.record_timeout(issued);
        c.record_timeout(issued);
        assert_eq!(c.rate_completed, 0, "high tier must reset completions");
        assert_eq!(c.rate_failed, 0, "high tier must reset failures");
        assert!(
            dl_us(&c) >= 100_000.0 + EXPECTED_OFFSET_US + HIGH_BUMP_US - 1.0,
            "HIGH must add ~1s to the deadline"
        );
    }

    /// The `issued + bump > current` stale-timeout gate: a straggler that was
    /// launched under an OLD (smaller) deadline must not stack a bump the
    /// deadline has already grown past, while a straggler at the CURRENT deadline
    /// still widens. Driven through the MODERATE tier, which does not reset the
    /// rate counters, so the gate — not a counter reset — is what suppresses.
    #[test]
    fn stale_concurrent_timeout_is_gated_but_current_still_widens() {
        let mut c = DeadlineController::new();
        // ~1000 completions: MODERATE trips at failed > 2, HIGH at failed > 11,
        // so the burst below stays in MODERATE (no counter reset) throughout.
        warm(&mut c, Duration::from_millis(100), 1000);
        let stale = c.deadline().unwrap(); // 800ms — the "old" issued value

        // Raise the deadline via MODERATE bumps, each timeout carrying the
        // then-current deadline (a fresh straggler), until current is well above
        // `stale + MODERATE_BUMP`.
        for _ in 0..5 {
            let fresh = c.deadline().unwrap();
            c.record_timeout(fresh);
        }
        let raised = dl_us(&c);
        assert!(
            raised > stale.as_micros() as f64 + MODERATE_BUMP_US,
            "precondition: deadline {raised} must exceed stale+bump so the gate can bite"
        );

        // A straggler issued under the STALE deadline trips the tier but the gate
        // suppresses the bump: stale + MODERATE_BUMP <= current, so no change.
        c.record_timeout(stale);
        assert_eq!(
            dl_us(&c),
            raised,
            "a timeout from the pre-bump deadline must not re-widen (gate)"
        );

        // A straggler issued at the CURRENT deadline still widens: the gate only
        // suppresses stale ones. This is the teeth — deleting the gate makes the
        // assert above fail, deleting the tier makes this one fail.
        let fresh = c.deadline().unwrap();
        c.record_timeout(fresh);
        assert!(
            dl_us(&c) > raised,
            "a timeout at the current deadline must still widen"
        );
    }

    // --- escape hatch ---------------------------------------------------------

    /// A steady-state escape (widened past 5s by tier bumps) is NOT sticky: a
    /// healthy run must re-arm the deadline. Distinct from `stop_timeout`, which a
    /// slow *warmup* latches permanently (see `slow_warmup_never_arms_a_deadline`).
    /// Critical for a client handle shared across a long-lived client's transfers.
    #[test]
    fn escape_re_arms_after_latency_recovers() {
        let mut c = DeadlineController::new();
        // Arm at a fast seed, then stack HIGH bumps until the deadline widens past
        // the 5s escape and disables. Bump-escape does not set stop_timeout, so it
        // stays re-armable — unlike a slow-warmup escape.
        warm(&mut c, Duration::from_millis(100), WARM_THRESHOLD);
        while c.deadline().is_some() {
            c.record_timeout(c.deadline().unwrap());
            c.record_timeout(c.deadline().unwrap());
        }
        assert_eq!(c.deadline(), None, "precondition: escaped via bumps");

        // The mean is still ~100ms (timeouts never feed it), so the next success
        // re-seeds the deadline at the low target and it re-arms immediately.
        c.record_success(Duration::from_millis(50));
        let rearmed = c
            .deadline()
            .expect("a healthy success must re-arm after a bump escape");
        assert!(
            rearmed.as_micros() as f64 <= 1_500_000.0,
            "re-armed deadline should recover well below the 5s escape, got {rearmed:?}"
        );
    }

    #[test]
    fn escape_via_stacked_bumps_disables_deadline() {
        let mut c = DeadlineController::new();
        // Seed just under the escape so a few HIGH bumps push over it.
        warm(&mut c, Duration::from_millis(3500), WARM_THRESHOLD); // target ≈ 4.2s
        for _ in 0..10 {
            let Some(issued) = c.deadline() else { break };
            // Each pair of timeouts trips HIGH (+1s) and resets the window.
            c.record_timeout(issued);
            c.record_timeout(issued);
        }
        assert_eq!(
            c.deadline(),
            None,
            "bumps past 5s must disable the deadline"
        );
    }

    // --- rate window (bounded denominator) -----------------------------------

    #[test]
    fn rate_window_bounds_the_denominator() {
        // The tier denominator must not grow with lifetime volume, or a burst on
        // a long-lived client could never reach `completed/100`.
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), 50_000);
        assert!(
            c.rate_completed <= RATE_WINDOW,
            "denominator must stay bounded by the window, got {}",
            c.rate_completed
        );
    }

    #[test]
    fn burst_after_long_healthy_run_still_escalates() {
        // The bug the window fixes: a lifetime denominator would make a burst
        // after a long healthy run unable to move the deadline. 200_000 healthy
        // completions land the window exactly on a halving boundary, so the
        // denominator is RATE_WINDOW/2 = 5_000 at burst start: MODERATE trips at
        // failed > ceil(5000/1000)=5, HIGH at failed > ceil(5000/100)=50 (rising
        // to 8/75 as completions refill toward RATE_WINDOW). The burst escalates
        // through HIGH and stacks past the 5s escape. With the OLD unbounded
        // denominator (200_000) MODERATE alone needs failed > 200, so this
        // 150-timeout burst would leave the deadline UNCHANGED.
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), 200_000);
        let seed = dl_us(&c); // ~800ms

        for _ in 0..150 {
            // Once HIGH bumps stack past 5s the deadline escapes (None) — the
            // strongest possible evidence the burst reached the HIGH tier.
            let Some(issued) = c.deadline() else { return };
            c.record_timeout(issued);
        }
        // If it did not escape, it must at least have climbed a full HIGH bump —
        // either outcome proves the burst escalated, which the old scheme could
        // not do at this denominator.
        assert!(
            dl_us(&c) >= seed + HIGH_BUMP_US * 0.9,
            "a burst after a long healthy run must escalate (HIGH or escape); \
             seed {seed}, got {}",
            dl_us(&c)
        );
    }

    #[test]
    fn halving_preserves_the_failure_ratio() {
        // The window's whole point: halving must scale BOTH counters so the
        // ratio (not just the denominator) is preserved. A mutation that halved
        // only `rate_completed` would inflate the rate and this must catch it.
        let mut c = DeadlineController::new();
        // Drive to one completion short of the window with a ~1% failure salted
        // in, then trip the boundary and check the ratio survives.
        for i in 0..RATE_WINDOW - 1 {
            if i % 100 == 0 {
                c.rate_failed += 1; // ~1% synthetic failures, directly
            }
            c.record_completion();
        }
        let (c_before, f_before) = (c.rate_completed, c.rate_failed);
        assert_eq!(
            c_before,
            RATE_WINDOW - 1,
            "precondition: one short of window"
        );
        // The completion that crosses RATE_WINDOW halves both.
        c.record_completion();
        assert_eq!(c.rate_completed, RATE_WINDOW / 2, "denominator must halve");
        assert_eq!(
            c.rate_failed,
            f_before / 2,
            "numerator must halve in lockstep"
        );
        // Ratio preserved within integer-truncation of one unit.
        let before = f_before as f64 / c_before as f64;
        let after = c.rate_failed as f64 / c.rate_completed as f64;
        assert!(
            (before - after).abs() < 0.0005,
            "halving must preserve the failure ratio: {before} vs {after}"
        );
    }

    #[test]
    fn re_arm_after_escape_clears_the_poisoned_rate_window() {
        // Regression for the escape→re-arm thrash: while escaped, in-flight
        // stragglers keep incrementing `rate_failed` with no tier reset, so a
        // naive re-arm would inherit that count and the first post-recovery
        // timeout would over-bump or instantly re-escape. The re-arm must clear
        // the window.
        let mut c = DeadlineController::new();
        // Arm near the escape, then trip MODERATE repeatedly to escape (MODERATE
        // does not reset the window).
        warm(&mut c, Duration::from_millis(4250), 1000);
        while c.deadline().is_some() {
            c.record_timeout(c.deadline().unwrap());
        }
        // Escape-period stragglers accumulate failures with the deadline None.
        for _ in 0..50 {
            c.record_timeout(Duration::from_millis(4950));
        }
        assert!(
            c.rate_failed > 0,
            "precondition: escaped window is poisoned"
        );

        // Recover: a healthy run pulls the mean down and re-arms the deadline.
        warm(&mut c, Duration::from_millis(50), 500);
        assert!(c.deadline().is_some(), "precondition: re-armed");
        assert_eq!(
            c.rate_failed, 0,
            "re-arm must clear the poisoned failure count"
        );
        let rearmed = dl_us(&c);

        // The first timeout after recovery must NOT over-bump off a stale count:
        // with the window cleared it takes a real burst to trip a tier again, so
        // one isolated timeout leaves the re-armed deadline unchanged.
        c.record_timeout(c.deadline().unwrap());
        assert_eq!(
            dl_us(&c),
            rearmed,
            "one timeout after re-arm must not widen a freshly recovered deadline"
        );
    }

    #[test]
    fn gate_suppressed_high_crossing_does_not_reset_the_window() {
        // The window must reset only when a HIGH bump actually lands: a HIGH
        // crossing whose bump the `issued` gate suppresses must NOT discard the
        // accumulated failure evidence.
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), 1000);
        // Raise the deadline well above a stale `issued` via fresh HIGH bumps.
        let stale = c.deadline().unwrap();
        // Trip HIGH once with a fresh straggler to grow the deadline and reset.
        // (2 timeouts: failed 2 > ceil(~1002/100)=11? no — need a real burst.)
        for _ in 0..12 {
            let fresh = c.deadline().unwrap();
            c.record_timeout(fresh);
        }
        // Now the deadline is ≥1s above `stale`. Feed timeouts carrying the stale
        // (small) issued value: they cross HIGH on the rate but the gate
        // suppresses the bump. The window must keep accumulating, not reset.
        let completed_before = c.rate_completed;
        let failed_before = c.rate_failed;
        c.record_timeout(stale);
        assert!(
            c.rate_failed > failed_before && c.rate_completed > completed_before,
            "a gate-suppressed crossing must not zero the window: \
             failed {failed_before}->{}, completed {completed_before}->{}",
            c.rate_failed,
            c.rate_completed
        );
    }

    // --- concurrency ----------------------------------------------------------

    #[cfg_attr(miri, ignore)]
    #[test]
    fn shared_tracker_survives_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        // The tracker is shared across all concurrent requests on a client
        // handle. Hammer it from many threads mixing successes and timeouts;
        // assert no deadlock/poison and that the lifetime timeout count is exact.
        let tracker = Arc::new(LatencyTracker::new());
        let threads = 8;
        let timeouts_per_thread = 500;
        let successes_per_thread = 500;

        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let t = tracker.clone();
                thread::spawn(move || {
                    for _ in 0..successes_per_thread {
                        t.record(Duration::from_millis(100));
                    }
                    for _ in 0..timeouts_per_thread {
                        t.record_timeout(Duration::from_millis(800));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker thread must not panic");
        }

        assert_eq!(
            tracker.timeout_count(),
            threads * timeouts_per_thread,
            "every timeout must be counted exactly once under contention"
        );
    }

    #[test]
    fn timeout_never_moves_the_mean() {
        // A timed-out request must not enter the latency mean (the censored-P99
        // trap): the mean stays put across timeouts.
        let mut c = DeadlineController::new();
        warm(&mut c, Duration::from_millis(100), WARM_THRESHOLD);
        let mean_before = c.mean_ttfb.value.unwrap();
        c.record_timeout(c.deadline().unwrap());
        assert_eq!(
            c.mean_ttfb.value.unwrap(),
            mean_before,
            "a timeout must not feed the latency mean"
        );
    }

    // --- guarded() integration ------------------------------------------------

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn guarded_cold_applies_no_timeout() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.deadline(), None);

        // 100ms would exceed a warm deadline, but cold applies none.
        let result = tracker
            .guarded(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, crate::error::Error>(42)
            })
            .await;
        assert!(matches!(result, Ok(42)));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn guarded_success_records_and_returns() {
        let tracker = LatencyTracker::new();
        // Warm to just below the threshold so the guarded success is the sample
        // that crosses it: a warm deadline afterward proves the success path
        // recorded (dropping `record()` would leave the tracker cold → None).
        for _ in 0..WARM_THRESHOLD - 1 {
            tracker.record(Duration::from_millis(100));
        }
        assert_eq!(tracker.deadline(), None, "precondition: one sample short");

        let result = tracker
            .guarded(async { Ok::<_, crate::error::Error>(42) })
            .await;
        assert!(matches!(result, Ok(42)));
        assert!(
            tracker.deadline().is_some(),
            "a guarded success must record its sample (warming the tracker)"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn guarded_deadline_exceeded_records_timeout() {
        let tracker = LatencyTracker::new();
        // Warm at 100ms → deadline ≈ 800ms; a 30s future trips it.
        for _ in 0..WARM_THRESHOLD {
            tracker.record(Duration::from_millis(100));
        }
        let result: Result<(), _> = tracker
            .guarded(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<_, crate::error::Error>(())
            })
            .await;
        assert!(matches!(result, Err(GuardError::DeadlineExceeded(_))));
        assert_eq!(tracker.timeout_count(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn guarded_inner_error_passed_through() {
        let tracker = LatencyTracker::new();
        let result: Result<(), _> = tracker
            .guarded(async {
                Err::<(), _>(crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "simulated SDK error",
                ))
            })
            .await;
        assert!(matches!(result, Err(GuardError::Inner(_))));
    }

    /// A WARM tracker's inner-error arm: an SDK error that resolves before the
    /// deadline must surface as `Inner` and must NOT be counted as a timeout.
    /// The cold test above exercises the `None` arm; this covers the `Some(dl)`
    /// `Ok(Err(_))` arm, which decides a real error under an active deadline is
    /// not a deadline exceedance.
    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn guarded_warm_inner_error_is_not_a_timeout() {
        let tracker = LatencyTracker::new();
        for _ in 0..WARM_THRESHOLD {
            tracker.record(Duration::from_millis(100));
        }
        assert!(tracker.deadline().is_some(), "precondition: warm");

        // Resolves immediately with an error — well within the ~800ms deadline.
        let result: Result<(), _> = tracker
            .guarded(async {
                Err::<(), _>(crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "simulated SDK error",
                ))
            })
            .await;
        assert!(matches!(result, Err(GuardError::Inner(_))));
        assert_eq!(
            tracker.timeout_count(),
            0,
            "a warm inner error must not be recorded as a timeout"
        );
    }
}
