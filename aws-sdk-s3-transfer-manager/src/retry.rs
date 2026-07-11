/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Retry loop layered over the latency deadline guard.
//!
//! [`LatencyTracker::guarded`](crate::metrics::latency::LatencyTracker::guarded)
//! runs one attempt under a deadline and reports its outcome. This module loops
//! it: a caller-supplied classifier decides, per failed attempt, whether to
//! retry. The guard holds no retry policy; the classifier holds the domain
//! knowledge of which errors are transient.

use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryPartition;

use crate::error::{Error, ErrorKind};
use crate::metrics::latency::{GuardError, LatencyTracker};

/// Maximum attempts per operation before the last failure is returned. Counts
/// every attempt, not just timeouts: a retryable failure of any kind re-issues
/// until this budget is spent.
const MAX_ATTEMPTS: u32 = 3;

/// Base delay for transient-transport backoff (both the upload part-send and the
/// download body-read paths use it). The first retry waits a full-jittered value
/// in `[0, INITIAL_BACKOFF]`, doubling each attempt. Sized above the SDK's own
/// 50ms transient base because this loop is an *outer* retry that fires only after
/// the SDK's inner retry already exhausted — a re-issue should span the window in
/// which in-flight work completes and returns retry-quota tokens, not collide with
/// the drained bucket again. See `networking/retry-ownership-and-token-bucket.md`.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Ceiling for a single backoff delay (applied before jitter). The SDK SEP uses
/// 20s; the TM caps lower because a multi-second stall on one part inside a
/// larger transfer is pathological.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Full-jitter truncated exponential backoff schedule, matching the AWS retry
/// SEP form `t = b · min(initial · 2^i, max)` with `b` uniform in `[0, 1)`.
///
/// Full jitter (the whole capped value scaled by a uniform random, not a tight
/// band around the base) is what de-correlates a burst of simultaneous failures:
/// it spreads re-issues across the entire `[0, cap]` window instead of
/// clustering them near the base.
///
/// Pure and `Copy`: holds only parameters and owns no RNG. The random draw is
/// supplied by the caller (production: `fastrand::f64()`; tests: a fixed value),
/// so the schedule is a pure function of `(retry_index, rand_unit)` and the
/// geometric progression is directly assertable. See
/// `networking/retry-ownership-and-token-bucket.md`.
#[derive(Clone, Copy)]
pub(crate) struct Backoff {
    initial: Duration,
    max: Duration,
}

impl Backoff {
    /// Backoff tuned for transient-transport retries (upload part-send and
    /// download body-read paths).
    pub(crate) const fn transient() -> Self {
        Self {
            initial: INITIAL_BACKOFF,
            max: MAX_BACKOFF,
        }
    }

    /// Un-jittered ceiling for a 0-based retry index: `min(initial · 2^i, max)`.
    fn ceiling(&self, retry_index: u32) -> Duration {
        let factor = 2u32.saturating_pow(retry_index);
        self.initial.saturating_mul(factor).min(self.max)
    }

    /// Full-jittered delay for a 0-based retry index (`0` = first retry).
    /// `rand_unit` must be in `[0, 1)`; the result lands in `[0, ceiling(i)]`.
    pub(crate) fn delay(&self, retry_index: u32, rand_unit: f64) -> Duration {
        self.ceiling(retry_index).mul_f64(rand_unit)
    }
}

/// A classifier's verdict for a failed attempt.
#[derive(Debug)]
pub(crate) enum RetryDecision {
    /// Re-issue the operation, optionally after a delay.
    Retry {
        /// `None` retries immediately; `Some` sleeps first.
        delay: Option<Duration>,
    },
    /// Give up and return the last error.
    NoRetry,
}

/// Run `build` under the latency deadline, retrying per `classify` up to
/// [`MAX_ATTEMPTS`] total attempts.
///
/// Each iteration builds a fresh future via `build` and runs it through
/// [`LatencyTracker::guarded`]. On a [`GuardError`], `classify` decides whether
/// to retry; a retryable failure re-issues (after any backoff) until the budget
/// is spent, then the last error is returned. `build` must produce an identical
/// request each call (same range, etag, version) so a retry reads the same data.
///
/// The entire `build` future is under the deadline. Use this when the whole
/// operation is the unit being timed (e.g. an upload part-send). For a download,
/// where the deadline should cover only time-to-first-byte and NOT the body-read
/// (whose duration scales with part size), use [`retry_guarded_response`].
///
/// `classify` sees the [`GuardError`] directly, so a deadline timeout is a
/// distinct arm from inner errors. [`into_error`] renders a terminal
/// `DeadlineExceeded` into the returned `Error` only at the return path.
pub(crate) async fn retry_guarded<T, F, Fut>(
    tracker: &LatencyTracker,
    classify: impl Fn(&GuardError<Error>, u32) -> RetryDecision,
    mut build: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    // The whole operation is the timed unit: guard it, with an identity tail.
    retry_guarded_response(tracker, classify, || {
        (build(), |v| std::future::ready(Ok(v)))
    })
    .await
}

/// Retry loop where the latency deadline covers only the first phase (the
/// response), not the untimed tail that consumes it.
///
/// Each attempt calls `build` to get `(send, consume)`, both fresh so `consume`
/// can capture per-attempt state:
///   - `send` is run under the deadline — for a download GET this resolves when
///     response headers arrive, i.e. time-to-first-byte. A deadline timeout here
///     is a `DeadlineExceeded` straggler (slow to respond / bad connection),
///     re-issued on a fresh connection.
///   - `consume` runs on `send`'s success value WITHOUT a deadline — for a
///     download this is the body-read, whose duration scales with part size and
///     link speed and must not be cancelled as if it were a straggler. A dead
///     mid-body stream is caught by stalled-stream protection, not this deadline.
///
/// An error from either phase is classified and retried identically (a
/// body-read `IOError` re-issues the whole GET). `build` must produce an
/// identical request each call so a retry reads the same data.
pub(crate) async fn retry_guarded_response<T, U, F, C, SendFut, ConsumeFut>(
    tracker: &LatencyTracker,
    classify: impl Fn(&GuardError<Error>, u32) -> RetryDecision,
    mut build: F,
) -> Result<U, Error>
where
    F: FnMut() -> (SendFut, C),
    C: FnOnce(T) -> ConsumeFut,
    SendFut: Future<Output = Result<T, Error>>,
    ConsumeFut: Future<Output = Result<U, Error>>,
{
    let mut attempt = 1u32;
    loop {
        let (send, consume) = build();
        // Time only the response (TTFB); on success, consume the body untimed.
        let outcome = match tracker.guarded(send).await {
            Ok(resp) => consume(resp).await.map_err(GuardError::Inner),
            Err(ge) => Err(ge),
        };
        match outcome {
            Ok(val) => return Ok(val),
            // `attempt - 1` is the 0-based retry index for backoff: 0 on the
            // first failure, so the classifier's first computed delay uses 2^0.
            Err(ge) => match classify(&ge, attempt - 1) {
                RetryDecision::NoRetry => return Err(into_error(ge)),
                RetryDecision::Retry { .. } if attempt >= MAX_ATTEMPTS => {
                    return Err(into_error(ge));
                }
                RetryDecision::Retry { delay } => {
                    if let Some(delay) = delay {
                        // TODO: thread the runtime's `AsyncSleep` impl (via
                        // RuntimeComponents, as the SDK does) instead of binding
                        // directly to tokio, so the backoff sleep is runtime-agnostic.
                        tokio::time::sleep(delay).await;
                    }
                    attempt += 1;
                }
            },
        }
    }
}

/// Convert a terminal [`GuardError`] into the error returned to the caller.
///
/// [`GuardError::DeadlineExceeded`] carries no inner error, so it becomes an
/// [`ErrorKind::IOError`](crate::error::ErrorKind::IOError) naming the deadline
/// the final attempt exceeded. An inner error is returned verbatim.
fn into_error(ge: GuardError<Error>) -> Error {
    match ge {
        GuardError::DeadlineExceeded(deadline) => Error::new(
            ErrorKind::IOError,
            format!(
                "request exceeded the {}ms latency deadline",
                deadline.as_millis()
            ),
        ),
        GuardError::Inner(e) => e,
    }
}

/// Classifier for the upload part-send path.
///
/// Retries two cases, with different timing:
///   - a latency-deadline timeout (a straggler) → retry **immediately** on a
///     fresh connection. A straggler is not shared-quota contention, so there is
///     nothing to de-correlate.
///   - a **transient transport** inner error (connection IO error or client-side
///     timeout) → retry with **full-jittered exponential backoff**. The SDK
///     normally retries these over the rewindable in-memory part body, but its
///     retry can be defeated by token-bucket exhaustion when many parts fail
///     concurrently (an ENOBUFS-style burst); the jittered re-issue lands after
///     in-flight parts complete and refill the quota, instead of re-colliding.
///
/// Everything else is terminal: a modeled service error (including throttling /
/// 503 `SlowDown`, which the SDK's shared token bucket exists to handle — a TM
/// re-issue would amplify the storm) and any non-transport inner error.
///
/// `retry_index` is 0-based; the jitter is drawn from the `fastrand` global.
pub(crate) fn classify_upload_part_retry(
    ge: &GuardError<Error>,
    retry_index: u32,
    backoff: &Backoff,
) -> RetryDecision {
    match ge {
        GuardError::DeadlineExceeded(_) => RetryDecision::Retry { delay: None },
        GuardError::Inner(e) if e.is_transient_transport() => RetryDecision::Retry {
            delay: Some(backoff.delay(retry_index, fastrand::f64())),
        },
        GuardError::Inner(_) => RetryDecision::NoRetry,
    }
}

/// Classifier for download body reads.
///
/// A chunk's body is consumed after the SDK's GetObject orchestration completes,
/// so the SDK's own retry never covers a mid-stream failure. This loop recovers
/// exactly that, re-issuing the ranged GET, and it is the SOLE retrier for the
/// body path.
///
/// Retries three cases, all with full-jittered backoff except the straggler:
///   - a **latency-deadline timeout** (a straggler on one connection) → retry
///     **immediately** on a fresh connection; nothing shared to de-correlate.
///   - a **transient transport** failure on the re-issue's send (connection IO
///     error or client-side timeout) → **backoff**. These correlate under load —
///     a system-wide resource exhaustion (e.g. macOS mbuf/ENOBUFS) fails many
///     connections at once, and immediate re-issue re-collides on the same empty
///     pool. Full jitter spreads the retries, same as the upload-part path.
///   - a **body-stream** failure ([`ErrorKind::IOError`]: reset, truncated, short
///     body) → **backoff** too. Under a saturated link these also cluster, so a
///     jittered re-issue avoids marching the whole batch back into the congestion.
///
/// A checksum mismatch ([`ErrorKind::IntegrityError`], classified at the body-read
/// boundary) is terminal: a corrupt body must never be re-fetched and masked.
/// Everything else is terminal.
pub(crate) fn classify_body_retry(
    ge: &GuardError<Error>,
    retry_index: u32,
    backoff: &Backoff,
) -> RetryDecision {
    let err = match ge {
        GuardError::DeadlineExceeded(_) => return RetryDecision::Retry { delay: None },
        GuardError::Inner(e) => e,
    };

    match err.kind() {
        // A checksum mismatch must never be re-fetched and masked.
        ErrorKind::IntegrityError(_) => RetryDecision::NoRetry,
        // Transient transport on the send (surfaced as ServiceError with the
        // transient-transport flag): correlated under resource exhaustion, so
        // back off to de-correlate the burst.
        ErrorKind::ServiceError if err.is_transient_transport() => RetryDecision::Retry {
            delay: Some(backoff.delay(retry_index, fastrand::f64())),
        },
        // Mid-stream body-read failure: back off too (clusters under load).
        ErrorKind::IOError => RetryDecision::Retry {
            delay: Some(backoff.delay(retry_index, fastrand::f64())),
        },
        _ => RetryDecision::NoRetry,
    }
}

/// Retry partition for an S3 bucket.
///
/// The SDK shares one retry token bucket per [`RetryPartition`]; the default
/// is region-wide (`s3-{region}`), so a throttle storm on one bucket drains the
/// retry budget for every other bucket on the client. Keying the partition by
/// bucket isolates them: each bucket gets its own token bucket, matching CRT,
/// which partitions per S3 host. Applied per operation via `config_override`.
pub(crate) fn bucket_retry_partition(bucket: &str) -> RetryPartition {
    RetryPartition::new(format!("s3-tm-{bucket}"))
}

/// Per-operation config override that keys the retry token bucket by bucket.
///
/// Returns an empty override when no bucket is set (a no-op merge), so call
/// sites can attach it unconditionally via `.config_override(...)`.
pub(crate) fn bucket_partition_override(bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
    let builder = aws_sdk_s3::config::Builder::default();
    match bucket {
        Some(b) => builder.retry_partition(bucket_retry_partition(b)),
        None => builder,
    }
}

/// Stalled-stream grace period for download GET bodies.
///
/// Stalled-stream protection cancels a response body that delivers zero bytes
/// for this long, so a dead mid-body connection is dropped and the GET re-issued
/// on a fresh one — the head-of-line-blocking case the latency deadline no longer
/// covers (the deadline now guards only time-to-first-byte, not the body read).
/// The SDK default is 5s; downloads tighten it because a multi-second dead gap on
/// one part stalls the whole transfer, and the check triggers only on *zero*
/// throughput — a slow-but-progressing stream is never affected, so tightening
/// cannot false-cancel a healthy slow link.
///
// TODO(vnext): ground this value in measured dead-gap data (the parked dead-gap
// detector work) rather than a chosen constant; 2s is a conservative first cut.
const DOWNLOAD_STALL_GRACE: Duration = Duration::from_secs(2);

/// Per-operation config override for a download GET: the bucket retry partition
/// plus a tightened stalled-stream grace period on the response body.
///
/// Attach unconditionally via `.config_override(...)`; with no bucket the retry
/// partition is left at the client default and only the grace period is set.
pub(crate) fn download_get_override(bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
    bucket_partition_override(bucket).stalled_stream_protection(
        aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
            .grace_period(DOWNLOAD_STALL_GRACE)
            .build(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Pre-warm a tracker so its deadline is `Some` (mirrors latency.rs helper).
    fn warm_tracker(tracker: &LatencyTracker) {
        for _ in 0..10 {
            tracker.record(Duration::from_millis(100));
        }
    }

    /// Test-only classifier matching the pre-split "deadline only" behavior:
    /// retry a deadline timeout immediately, treat any inner error as terminal.
    fn deadline_only(ge: &GuardError<Error>, _retry_index: u32) -> RetryDecision {
        match ge {
            GuardError::DeadlineExceeded(_) => RetryDecision::Retry { delay: None },
            GuardError::Inner(_) => RetryDecision::NoRetry,
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn retries_timeout_then_succeeds() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker); // deadline ≈ 200ms

        let attempts = AtomicUsize::new(0);
        let result = retry_guarded(&tracker, deadline_only, || {
            let n = attempts.fetch_add(1, Ordering::Relaxed);
            async move {
                if n == 0 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                Ok::<_, Error>(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(tracker.timeout_count(), 1);
    }

    // --- split-timing (retry_guarded_response) ----------------------------
    //
    // The deadline must cover only the `send` phase (TTFB), never the `consume`
    // tail (body-read). These pin that boundary: a slow consume must NOT trip the
    // deadline (the large-part false-cancel bug), while a slow send must.

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn slow_consume_does_not_trip_deadline() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker); // deadline ≈ 200ms

        // send resolves immediately; consume (the "body read") takes 30s — far
        // past the deadline. It must still succeed: the body read is untimed.
        let attempts = AtomicUsize::new(0);
        let result = retry_guarded_response(&tracker, deadline_only, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            (std::future::ready(Ok::<_, Error>(())), |()| async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<_, Error>(42)
            })
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            1,
            "no retry: consume is untimed"
        );
        assert_eq!(
            tracker.timeout_count(),
            0,
            "a slow body read is not a timeout"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn slow_send_trips_deadline_then_retries() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker); // deadline ≈ 200ms

        // First attempt's send stalls 30s (slow TTFB) → deadline fires → retry;
        // second attempt's send is immediate and consume succeeds.
        let attempts = AtomicUsize::new(0);
        let result = retry_guarded_response(&tracker, deadline_only, || {
            let n = attempts.fetch_add(1, Ordering::Relaxed);
            let send = async move {
                if n == 0 {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                Ok::<_, Error>(())
            };
            (send, |()| std::future::ready(Ok::<_, Error>(7)))
        })
        .await;

        assert_eq!(result.unwrap(), 7);
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            2,
            "slow send trips deadline → one retry"
        );
        assert_eq!(tracker.timeout_count(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn consume_error_is_classified_and_retried() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker);

        // A body-read (consume) IOError must re-enter the retry loop exactly like
        // a send error would — proving the untimed tail is still retryable.
        let attempts = AtomicUsize::new(0);
        let classify = |ge: &GuardError<Error>, _i: u32| match ge {
            GuardError::Inner(e) if *e.kind() == ErrorKind::IOError => {
                RetryDecision::Retry { delay: None }
            }
            _ => RetryDecision::NoRetry,
        };
        let result = retry_guarded_response(&tracker, classify, || {
            let n = attempts.fetch_add(1, Ordering::Relaxed);
            (
                std::future::ready(Ok::<_, Error>(())),
                move |()| async move {
                    if n == 0 {
                        return Err(Error::new(ErrorKind::IOError, "body read reset"));
                    }
                    Ok::<_, Error>(99)
                },
            )
        })
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            2,
            "consume error re-issues the GET"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn exhausts_attempts_on_persistent_timeout() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker);

        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry_guarded(&tracker, deadline_only, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<_, Error>(())
            }
        })
        .await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::IOError);
        assert_eq!(attempts.load(Ordering::Relaxed), MAX_ATTEMPTS as usize);
        assert_eq!(tracker.timeout_count(), MAX_ATTEMPTS as usize);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn does_not_retry_terminal_inner_error() {
        let tracker = LatencyTracker::new();
        let attempts = AtomicUsize::new(0);

        let result: Result<(), _> = retry_guarded(&tracker, deadline_only, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(Error::new(ErrorKind::RuntimeError, "simulated SDK error")) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    // --- Backoff schedule (pure; rand supplied) -----------------------------

    #[test]
    fn backoff_ceiling_progression_doubles_then_caps() {
        // rand_unit = 1.0 yields the full ceiling, so the geometric progression
        // for INITIAL_BACKOFF=100ms, factor 2, MAX_BACKOFF=5s is directly checked.
        let b = Backoff::transient();
        assert_eq!(b.delay(0, 1.0), Duration::from_millis(100));
        assert_eq!(b.delay(1, 1.0), Duration::from_millis(200));
        assert_eq!(b.delay(2, 1.0), Duration::from_millis(400));
        assert_eq!(b.delay(3, 1.0), Duration::from_millis(800));
        assert_eq!(b.delay(4, 1.0), Duration::from_millis(1600));
        assert_eq!(b.delay(5, 1.0), Duration::from_millis(3200));
        // 100ms * 2^6 = 6400ms, capped at MAX_BACKOFF = 5s.
        assert_eq!(b.delay(6, 1.0), Duration::from_secs(5));
        assert_eq!(b.delay(7, 1.0), Duration::from_secs(5));
    }

    #[test]
    fn backoff_large_index_saturates_without_overflow() {
        // 2^u32::MAX must not panic; saturating_pow/mul keep it at the cap.
        let b = Backoff::transient();
        assert_eq!(b.delay(u32::MAX, 1.0), Duration::from_secs(5));
    }

    #[test]
    fn backoff_full_jitter_scales_linearly_within_ceiling() {
        let b = Backoff::transient();
        // index 2 -> 400ms ceiling. Full jitter spans [0, ceiling].
        assert_eq!(b.delay(2, 0.0), Duration::ZERO);
        assert_eq!(b.delay(2, 0.5), Duration::from_millis(200));
        assert_eq!(b.delay(2, 0.25), Duration::from_millis(100));
    }

    #[test]
    fn backoff_real_draws_stay_within_ceiling() {
        let b = Backoff::transient();
        for _ in 0..10_000 {
            let d = b.delay(2, fastrand::f64());
            assert!(
                d <= Duration::from_millis(400),
                "delay {d:?} exceeded ceiling"
            );
        }
    }

    // --- classify_upload_part_retry -----------------------------------------

    fn upload_decision(ge: &GuardError<Error>, retry_index: u32) -> RetryDecision {
        classify_upload_part_retry(ge, retry_index, &Backoff::transient())
    }

    #[test]
    fn upload_deadline_retries_immediately_no_backoff() {
        // A straggler is not contention: retry now, no delay.
        let d = upload_decision(&GuardError::DeadlineExceeded(Duration::from_secs(1)), 0);
        assert!(matches!(d, RetryDecision::Retry { delay: None }));
    }

    #[test]
    fn upload_transient_transport_retries_with_backoff() {
        // A transient-transport ServiceError (the ENOBUFS shape) retries with a
        // jittered delay within the index-0 ceiling [0, 100ms].
        let err = Error::test_transient_transport();
        let d = upload_decision(&GuardError::Inner(err), 0);
        match d {
            RetryDecision::Retry { delay: Some(delay) } => {
                assert!(delay <= Duration::from_millis(100));
            }
            other => panic!("expected backoff retry, got {other:?}"),
        }
    }

    #[test]
    fn upload_non_transport_service_error_is_terminal() {
        // A plain ServiceError (e.g. throttling/permanent) must NOT retry here —
        // throttling is the SDK token bucket's job.
        let err = Error::new(ErrorKind::ServiceError, "503 SlowDown");
        let d = upload_decision(&GuardError::Inner(err), 0);
        assert!(matches!(d, RetryDecision::NoRetry));
    }

    #[test]
    fn upload_other_inner_error_is_terminal() {
        let err = Error::new(ErrorKind::RuntimeError, "not a transport error");
        let d = upload_decision(&GuardError::Inner(err), 0);
        assert!(matches!(d, RetryDecision::NoRetry));
    }

    // --- classify_body_retry ------------------------------------------------

    fn retry_delay(decision: RetryDecision) -> Option<Option<Duration>> {
        match decision {
            RetryDecision::Retry { delay } => Some(delay),
            RetryDecision::NoRetry => None,
        }
    }

    #[test]
    fn body_retry_deadline_exceeded_retries_immediately() {
        let b = Backoff::transient();
        let d = classify_body_retry(&GuardError::DeadlineExceeded(Duration::from_secs(1)), 0, &b);
        // A straggler retries immediately — no backoff.
        assert_eq!(retry_delay(d), Some(None));
    }

    #[test]
    fn body_retry_cancellation_does_not_retry() {
        let b = Backoff::transient();
        let err = Error::new(ErrorKind::OperationCancelled, "cancelled mid body");
        let d = classify_body_retry(&GuardError::Inner(err), 0, &b);
        assert!(matches!(d, RetryDecision::NoRetry));
    }

    #[test]
    fn body_retry_io_error_backs_off() {
        // A body-stream failure retries with backoff (clusters under load).
        let b = Backoff::transient();
        let err = Error::new(ErrorKind::IOError, "connection reset by peer");
        let d = classify_body_retry(&GuardError::Inner(err), 0, &b);
        assert!(
            matches!(retry_delay(d), Some(Some(_))),
            "IOError should retry with a backoff delay"
        );
    }

    #[test]
    fn body_retry_transient_transport_backs_off() {
        // A transient-transport send failure (ENOBUFS/connect/timeout class)
        // retries with backoff, de-correlating a correlated burst.
        let b = Backoff::transient();
        let err = Error::test_transient_transport();
        let d = classify_body_retry(&GuardError::Inner(err), 0, &b);
        assert!(
            matches!(retry_delay(d), Some(Some(_))),
            "transient-transport ServiceError should retry with a backoff delay"
        );
    }
}
