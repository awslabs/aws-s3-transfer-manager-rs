/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Client-level latency tracking with adaptive deadlines and timeout+retry.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

/// Multiplier applied to observed P99 latency for the warm deadline.
///
/// Observed on m6idn.16xlarge (100 Gbps NIC, 64 vCPU) with 8 MB parts:
///   - P99: ~250ms, P999: ~400-700ms, stragglers: 5000ms+
///   - At 2.0x P99 (~500ms), all normal requests pass while
///     5s+ stragglers are caught and retried.
const WARM_DEADLINE_MULTIPLIER: f64 = 2.0;

/// Number of samples required before switching to the adaptive P99-based
/// deadline. Needs enough data for a meaningful P99 estimate.
const WARM_THRESHOLD: usize = 10;

/// If the observed average request duration exceeds this threshold,
/// disable adaptive timeouts (return None). For very large parts,
/// re-uploading/re-downloading is more expensive than waiting for a
/// slow response.
///
/// Matches CRT's escape hatch: `s_upload_timeout_threshold_ns` = 5s.
const RETRY_COST_THRESHOLD: Duration = Duration::from_secs(5);

/// Minimum recordable latency in microseconds.
const HISTOGRAM_MIN_US: u64 = 1;

/// Maximum recordable latency in microseconds (60 seconds).
const HISTOGRAM_MAX_US: u64 = 60_000_000;

/// Number of significant value digits for the histogram.
const HISTOGRAM_PRECISION: u8 = 3;

/// Timeout rate threshold for moderate backoff (+100ms).
/// Matches CRT's 0.1% threshold.
const TIMEOUT_RATE_MODERATE: f64 = 0.001;

/// Timeout rate threshold for aggressive backoff (+1s).
/// Matches CRT's 1% threshold.
const TIMEOUT_RATE_HIGH: f64 = 0.01;

/// Backoff added to deadline at moderate timeout rate.
const TIMEOUT_BACKOFF_MODERATE: Duration = Duration::from_millis(100);

/// Backoff added to deadline at high timeout rate.
const TIMEOUT_BACKOFF_HIGH: Duration = Duration::from_secs(1);

/// Outcome of a single [`LatencyTracker::guarded`] attempt that did not succeed.
pub(crate) enum GuardError<E> {
    /// The future did not complete within the adaptive deadline (the carried
    /// [`Duration`]). The in-flight future was dropped, releasing its HTTP
    /// connection, so a retry runs on a fresh one. Carries no inner error.
    DeadlineExceeded(Duration),
    /// The guarded future produced an error before the deadline.
    Inner(E),
}

/// Tracks per-operation request latencies and computes adaptive deadlines.
///
/// Uses an HdrHistogram to derive a P99-based deadline once warmed up.
/// Before warmup, returns `None` (no timeout applied).
///
// TODO: consider periodic histogram reset for aging out old samples
// if transfers with changing network conditions need faster adaptation.
pub(crate) struct LatencyTracker {
    hist: Mutex<Histogram<u64>>,
    sample_count: AtomicUsize,
    timeout_count: AtomicUsize,
}

impl LatencyTracker {
    /// Create a new tracker with no samples.
    pub(crate) fn new() -> Self {
        Self {
            hist: Mutex::new(
                Histogram::new_with_bounds(HISTOGRAM_MIN_US, HISTOGRAM_MAX_US, HISTOGRAM_PRECISION)
                    .expect("valid histogram bounds"),
            ),
            sample_count: AtomicUsize::new(0),
            timeout_count: AtomicUsize::new(0),
        }
    }

    /// Record a completed request duration.
    pub(crate) fn record(&self, duration: Duration) {
        let mut hist = self.hist.lock().unwrap();
        let micros = (duration.as_micros() as u64).min(HISTOGRAM_MAX_US);
        let _ = hist.record(micros);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a timed-out request.
    pub(crate) fn record_timeout(&self) {
        self.timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of recorded timeouts. Test-only accessor for retry-loop tests.
    #[cfg(test)]
    pub(crate) fn timeout_count(&self) -> usize {
        self.timeout_count.load(Ordering::Relaxed)
    }

    /// Compute the adaptive deadline for the next request.
    ///
    /// Returns `None` when cold (fewer than [`WARM_THRESHOLD`] samples) or
    /// when the average latency exceeds [`RETRY_COST_THRESHOLD`] (escape hatch
    /// for large parts). Otherwise returns P99 × [`WARM_DEADLINE_MULTIPLIER`],
    /// widened by rate-based backoff when timeouts are frequent.
    pub(crate) fn deadline(&self) -> Option<Duration> {
        let samples = self.sample_count.load(Ordering::Relaxed);
        if samples < WARM_THRESHOLD {
            // TODO(vnext): Cold-start timeout strategy. Currently no timeout until
            // WARM_THRESHOLD samples collected. This means stuck connections during
            // warmup have no protection beyond the SDK's connect timeout (3.1s).
            // Options to explore:
            // - Conservative initial deadline (e.g. 10s) generous enough to never
            //   false-positive but catches truly stuck connections
            // - First-byte timeout separate from total request timeout
            // - Timeout derived from part_size / expected_throughput
            return None;
        }

        let hist = self.hist.lock().unwrap();
        let avg = Duration::from_micros(hist.mean() as u64);
        if avg > RETRY_COST_THRESHOLD {
            tracing::debug!(
                avg_ms = avg.as_millis() as u64,
                "adaptive timeout disabled: average latency exceeds retry cost threshold"
            );
            return None;
        }

        let p99 = Duration::from_micros(hist.value_at_percentile(99.0));
        let mut deadline = p99.mul_f64(WARM_DEADLINE_MULTIPLIER);

        // Rate-based backoff: widen deadline when timeouts are frequent.
        // Prevents cascading cancellations that destroy connections faster
        // than the pool can replace them.
        let timeouts = self.timeout_count.load(Ordering::Relaxed);
        let total = samples + timeouts;
        if total > 0 {
            let timeout_rate = timeouts as f64 / total as f64;
            if timeout_rate > TIMEOUT_RATE_HIGH {
                deadline += TIMEOUT_BACKOFF_HIGH;
            } else if timeout_rate > TIMEOUT_RATE_MODERATE {
                deadline += TIMEOUT_BACKOFF_MODERATE;
            }
        }

        Some(deadline)
    }

    /// Run `fut` once under the adaptive deadline.
    ///
    /// On success records the duration (warming the tracker) and returns the
    /// value. When cold (no deadline) runs without a timeout, so the only
    /// failure is the inner error. When warm, races `fut` against the deadline:
    /// if it elapses, the in-flight future is dropped, a timeout is recorded,
    /// and [`GuardError::DeadlineExceeded`] carries the deadline that was
    /// exceeded.
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
                    self.record_timeout();
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
        f.debug_struct("LatencyTracker")
            .field("samples", &self.sample_count.load(Ordering::Relaxed))
            .field("timeouts", &self.timeout_count.load(Ordering::Relaxed))
            .field("deadline", &self.deadline())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Pre-warm a tracker with uniform samples so deadline() returns Some.
    fn warm_tracker(tracker: &LatencyTracker, duration: Duration) {
        for _ in 0..WARM_THRESHOLD {
            tracker.record(duration);
        }
    }

    #[test]
    fn test_cold_deadline() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.deadline(), None);
    }

    #[test]
    fn test_warm_transition() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker, Duration::from_millis(100));
        // All samples at 100ms → P99 ≈ 100ms, deadline ≈ 200ms
        // HdrHistogram quantizes values to 3 significant digits, so allow ±1ms.
        let deadline = tracker.deadline().expect("should be warm");
        assert!(
            (deadline.as_millis() as i64 - 200).unsigned_abs() <= 1,
            "expected ~200ms, got {deadline:?}"
        );
    }

    #[test]
    fn test_p99_calculation() {
        let tracker = LatencyTracker::new();
        // 98 samples at 100ms, 2 at 500ms
        for _ in 0..98 {
            tracker.record(Duration::from_millis(100));
        }
        tracker.record(Duration::from_millis(500));
        tracker.record(Duration::from_millis(500));

        // P99 of this distribution should be ~500ms (top 1% = 500ms values)
        // deadline ≈ 500ms * 2.0 = 1000ms. Allow ±2ms for quantization.
        let deadline = tracker.deadline().expect("should be warm");
        assert!(
            (deadline.as_millis() as i64 - 1000).unsigned_abs() <= 2,
            "expected ~1000ms, got {deadline:?}"
        );
    }

    #[test]
    fn test_escape_hatch() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker, Duration::from_secs(6));
        // Average 6s > RETRY_COST_THRESHOLD (5s), should return None
        assert_eq!(tracker.deadline(), None);
    }

    #[test]
    fn test_many_samples() {
        let tracker = LatencyTracker::new();
        // Record many samples — histogram handles arbitrary counts
        for _ in 0..1000 {
            tracker.record(Duration::from_millis(100));
        }
        // All uniform at 100ms → deadline ≈ 200ms. Allow ±1ms for quantization.
        let deadline = tracker.deadline().expect("should be warm");
        assert!(
            (deadline.as_millis() as i64 - 200).unsigned_abs() <= 1,
            "expected ~200ms, got {deadline:?}"
        );
    }

    #[test]
    fn test_rate_backoff_moderate() {
        let tracker = LatencyTracker::new();
        // 1000 samples at 100ms → P99 ≈ 100ms, base deadline ≈ 200ms
        for _ in 0..1000 {
            tracker.record(Duration::from_millis(100));
        }
        // 2 timeouts out of 1002 total ≈ 0.2% > TIMEOUT_RATE_MODERATE (0.1%)
        tracker.record_timeout();
        tracker.record_timeout();

        let deadline = tracker.deadline().expect("should be warm");
        // Base ~200ms + 100ms backoff = ~300ms
        assert!(
            deadline.as_millis() >= 290,
            "expected >= 290ms with moderate backoff, got {deadline:?}"
        );
    }

    #[test]
    fn test_rate_backoff_high() {
        let tracker = LatencyTracker::new();
        // 100 samples at 100ms → P99 ≈ 100ms, base deadline ≈ 200ms
        for _ in 0..100 {
            tracker.record(Duration::from_millis(100));
        }
        // 2 timeouts out of 102 total ≈ 1.96% > TIMEOUT_RATE_HIGH (1%)
        tracker.record_timeout();
        tracker.record_timeout();

        let deadline = tracker.deadline().expect("should be warm");
        // Base ~200ms + 1s backoff = ~1200ms
        assert!(
            deadline.as_millis() >= 1190,
            "expected >= 1190ms with high backoff, got {deadline:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn test_cold_no_timeout() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.deadline(), None);

        // Even though 100ms would exceed a warm P99, cold tracker applies no timeout
        let result = tracker
            .guarded(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, crate::error::Error>(42)
            })
            .await;
        assert!(matches!(result, Ok(42)));
        assert_eq!(tracker.sample_count.load(Ordering::Relaxed), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn test_guarded_success() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker, Duration::from_millis(100));

        let result = tracker
            .guarded(async { Ok::<_, crate::error::Error>(42) })
            .await;
        assert!(matches!(result, Ok(42)));
        assert_eq!(
            tracker.sample_count.load(Ordering::Relaxed),
            WARM_THRESHOLD + 1
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn test_guarded_deadline_exceeded() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker, Duration::from_millis(100));
        // Warm deadline ≈ 200ms; a 30s future trips it.

        let result: Result<(), _> = tracker
            .guarded(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<_, crate::error::Error>(())
            })
            .await;
        assert!(matches!(result, Err(GuardError::DeadlineExceeded(_))));
        // The deadline drop tears down the connection and records a timeout.
        assert_eq!(tracker.timeout_count.load(Ordering::Relaxed), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn test_guarded_inner_error_passed_through() {
        let tracker = LatencyTracker::new();
        let result: Result<(), _> = tracker
            .guarded(async {
                Err::<(), _>(crate::error::Error::new(
                    crate::error::ErrorKind::RuntimeError,
                    "simulated SDK error",
                ))
            })
            .await;
        // The inner error is returned verbatim for the caller to classify.
        assert!(matches!(result, Err(GuardError::Inner(_))));
    }
}
