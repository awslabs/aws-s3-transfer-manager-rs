/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-transfer latency tracking with adaptive deadlines and timeout+retry.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

/// Initial deadline before enough samples are collected.
///
/// Must exceed the SDK's default connect timeout (3.1s) to avoid
/// false timeouts on cold connections that haven't completed TLS
/// handshake yet.
///
/// Observed on m6idn.16xlarge (100 Gbps NIC, 64 vCPU):
///   - First-part latency: 90-130ms for 8 MB parts (warm connections)
///   - P99 at steady state: ~250ms for 8 MB parts
///   - Stragglers: 5+ seconds (stuck connections)
///
/// 5s accommodates connect timeout + first transfer on cold connections.
/// Warm deadline (P99 * multiplier) takes over after WARM_THRESHOLD samples.
const INITIAL_DEADLINE: Duration = Duration::from_secs(5);

/// Multiplier applied to observed P99 latency for the warm deadline.
///
/// Observed on m6idn.16xlarge (100 Gbps NIC, 64 vCPU) with 8 MB parts:
///   - P99: ~250ms, P999: ~400-700ms, stragglers: 5000ms+
///   - At 2.0x P99 (~500ms), all normal requests pass while
///     5s+ stragglers are caught and retried.
const WARM_DEADLINE_MULTIPLIER: f64 = 2.0;

/// Number of samples required before switching from INITIAL_DEADLINE
/// to the adaptive P99-based deadline. Needs enough data for a
/// meaningful P99 estimate.
const WARM_THRESHOLD: usize = 10;

/// Maximum attempts before failing the request. Each attempt after
/// the first cancels the in-flight request (dropping the future
/// releases the HTTP connection) and retries on a fresh connection.
const MAX_TIMEOUT_ATTEMPTS: usize = 3;

/// If the observed average request duration exceeds this threshold,
/// disable adaptive timeouts (return INITIAL_DEADLINE). For very
/// large parts, re-uploading/re-downloading is more expensive than
/// waiting for a slow response.
///
/// Matches CRT's escape hatch: `s_upload_timeout_threshold_ns` = 5s.
const RETRY_COST_THRESHOLD: Duration = Duration::from_secs(5);

/// Minimum recordable latency in microseconds.
const HISTOGRAM_MIN_US: u64 = 1;

/// Maximum recordable latency in microseconds (60 seconds).
const HISTOGRAM_MAX_US: u64 = 60_000_000;

/// Number of significant value digits for the histogram.
const HISTOGRAM_PRECISION: u8 = 3;

/// Tracks per-transfer request latencies and computes adaptive deadlines.
///
/// Uses an HdrHistogram to derive a P99-based deadline once warmed up.
/// Before warmup, falls back to a conservative initial deadline that
/// accommodates cold TLS connections.
///
// TODO: consider periodic histogram reset for aging out old samples
// if transfers with changing network conditions need faster adaptation.
pub(crate) struct LatencyTracker {
    hist: Mutex<Histogram<u64>>,
    sample_count: AtomicUsize,
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
        }
    }

    /// Record a completed request duration.
    pub(crate) fn record(&self, duration: Duration) {
        let mut hist = self.hist.lock().unwrap();
        // Clamp to histogram range; values outside are recorded at the boundary.
        let _ = hist.record(duration.as_micros() as u64);
        self.sample_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Compute the adaptive deadline for the next request.
    ///
    /// Returns [`INITIAL_DEADLINE`] when cold (fewer than [`WARM_THRESHOLD`]
    /// samples) or when the average latency exceeds [`RETRY_COST_THRESHOLD`]
    /// (escape hatch for large parts). Otherwise returns P99 × [`WARM_DEADLINE_MULTIPLIER`].
    pub(crate) fn deadline(&self) -> Duration {
        if self.sample_count.load(Ordering::Relaxed) < WARM_THRESHOLD {
            return INITIAL_DEADLINE;
        }

        let hist = self.hist.lock().unwrap();
        let mean = Duration::from_micros(hist.mean() as u64);
        if mean > RETRY_COST_THRESHOLD {
            return INITIAL_DEADLINE;
        }

        let p99 = Duration::from_micros(hist.value_at_percentile(99.0));
        p99.mul_f64(WARM_DEADLINE_MULTIPLIER)
    }

    /// Execute `build` with an adaptive timeout, retrying up to
    /// [`MAX_TIMEOUT_ATTEMPTS`] times on timeout.
    ///
    /// On timeout the in-flight future is dropped, which tears down the
    /// HTTP connection (hyper's `Pooled::Drop` sees the incomplete request
    /// and discards the connection). The next `build()` call gets a fresh
    /// connection from the pool.
    ///
    /// SDK errors (the inner `Err`) are returned immediately without retry —
    /// they represent server-side rejections, not straggler latency.
    pub(crate) async fn guarded<T, E, F, Fut>(&self, mut build: F) -> Result<T, crate::error::Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: Into<crate::error::Error>,
    {
        for attempt in 1..=MAX_TIMEOUT_ATTEMPTS {
            let deadline = self.deadline();
            let start = Instant::now();
            match tokio::time::timeout(deadline, build()).await {
                Ok(Ok(val)) => {
                    self.record(start.elapsed());
                    return Ok(val);
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_timeout) => {
                    tracing::debug!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        attempt,
                        deadline_ms = deadline.as_millis() as u64,
                        "request timed out, retrying"
                    );
                }
            }
        }

        Err(crate::error::Error::new(
            crate::error::ErrorKind::IOError,
            format!("request timed out after {MAX_TIMEOUT_ATTEMPTS} attempts"),
        ))
    }
}

impl std::fmt::Debug for LatencyTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyTracker")
            .field("sample_count", &self.sample_count.load(Ordering::Relaxed))
            .field("deadline", &self.deadline())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_cold_deadline() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.deadline(), INITIAL_DEADLINE);
    }

    #[test]
    fn test_warm_transition() {
        let tracker = LatencyTracker::new();
        for _ in 0..WARM_THRESHOLD {
            tracker.record(Duration::from_millis(100));
        }
        // All samples at 100ms → P99 ≈ 100ms, deadline ≈ 200ms
        // HdrHistogram quantizes values to 3 significant digits, so allow ±1ms.
        let deadline = tracker.deadline();
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
        let deadline = tracker.deadline();
        assert!(
            (deadline.as_millis() as i64 - 1000).unsigned_abs() <= 2,
            "expected ~1000ms, got {deadline:?}"
        );
    }

    #[test]
    fn test_escape_hatch() {
        let tracker = LatencyTracker::new();
        for _ in 0..WARM_THRESHOLD {
            tracker.record(Duration::from_secs(6));
        }
        // Average 6s > RETRY_COST_THRESHOLD (5s), should fall back
        assert_eq!(tracker.deadline(), INITIAL_DEADLINE);
    }

    #[test]
    fn test_many_samples() {
        let tracker = LatencyTracker::new();
        // Record many samples — histogram handles arbitrary counts
        for _ in 0..1000 {
            tracker.record(Duration::from_millis(100));
        }
        // All uniform at 100ms → deadline ≈ 200ms. Allow ±1ms for quantization.
        let deadline = tracker.deadline();
        assert!(
            (deadline.as_millis() as i64 - 200).unsigned_abs() <= 1,
            "expected ~200ms, got {deadline:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_guarded_success() {
        let tracker = LatencyTracker::new();
        let result = tracker
            .guarded(|| async { Ok::<_, crate::error::Error>(42) })
            .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(tracker.sample_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn test_guarded_timeout_then_success() {
        let tracker = LatencyTracker::new();
        let attempts = AtomicUsize::new(0);
        let result = tracker
            .guarded(|| {
                let n = attempts.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 0 {
                        // Exceeds INITIAL_DEADLINE (5s)
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Ok::<_, crate::error::Error>(42)
                }
            })
            .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_guarded_all_attempts_exhausted() {
        let tracker = LatencyTracker::new();
        let attempts = AtomicUsize::new(0);
        let result = tracker
            .guarded(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok::<_, crate::error::Error>(())
                }
            })
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), &crate::error::ErrorKind::IOError);
        assert_eq!(attempts.load(Ordering::Relaxed), MAX_TIMEOUT_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn test_guarded_sdk_error_no_retry() {
        let tracker = LatencyTracker::new();
        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = tracker
            .guarded(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async {
                    Err::<(), _>(crate::error::Error::new(
                        crate::error::ErrorKind::ChildOperationFailed,
                        "simulated SDK error",
                    ))
                }
            })
            .await;
        assert!(result.is_err());
        // Should not retry on SDK errors
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }
}
