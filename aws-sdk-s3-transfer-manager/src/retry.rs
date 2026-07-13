/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Retry loop for transfer operations and its classifiers.
//!
//! [`retry`] re-issues an operation up to [`MAX_ATTEMPTS`] times, deciding per
//! failure via a caller-supplied classifier and backing off per [`Backoff`].
//! The loop is deadline-agnostic: any latency deadline is composed by the caller
//! inside the built future (see [`LatencyTracker::guarded`]), surfacing a timeout
//! as [`GuardError::DeadlineExceeded`].

use std::future::Future;
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryPartition;

use crate::error::{Error, ErrorKind};
pub(crate) use crate::metrics::latency::GuardError;

/// Maximum attempts per operation before the last error is returned.
const MAX_ATTEMPTS: u32 = 3;

/// Base of the full-jitter backoff: the first retry waits a random duration in
/// `[0, INITIAL_BACKOFF]`, doubling the ceiling each attempt.
///
/// Larger than the SDK's own transient base because this is an *outer* retry
/// that fires only after the SDK's inner retry exhausted; a re-issue should span
/// the window in which in-flight work completes and refills the shared retry
/// quota, rather than collide with the still-drained bucket.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Ceiling for a single backoff delay, before jitter. Capped below the SDK SEP's
/// 20s because a multi-second stall on one part of a larger transfer is
/// pathological.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Full-jitter truncated-exponential backoff schedule: `delay = b · min(initial
/// · 2^i, max)`, `b` uniform in `[0, 1)`.
///
/// Full jitter (the whole capped value scaled, not a band around the base)
/// de-correlates a burst of simultaneous failures by spreading re-issues across
/// the entire `[0, cap]` window.
///
/// Owns no RNG: the caller supplies the `[0, 1)` draw, so [`delay`](Self::delay)
/// is a pure function of `(retry_index, rand_unit)`.
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

/// A classifier's verdict for a failed attempt: whether to retry, never how
/// long to wait. [`retry`] owns the schedule, so classifiers are pure functions
/// of the error.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Re-issue the operation after a full-jittered backoff.
    Retry,
    /// Give up and return the last error.
    NoRetry,
}

/// Retry an operation up to [`MAX_ATTEMPTS`] times, deciding each failure via
/// `classify` and backing off per `backoff`.
///
/// Each attempt calls `build()` for a fresh future; `build` must produce an
/// identical request each call so a retry re-sends/re-reads the same data. Any
/// latency deadline is composed inside `build` (via [`LatencyTracker::guarded`])
/// and surfaces as [`GuardError::DeadlineExceeded`]; other errors surface as
/// [`GuardError::Inner`]. On [`RetryDecision::Retry`] the loop sleeps a
/// full-jittered [`Backoff`] delay keyed on the 0-based retry index; on
/// [`RetryDecision::NoRetry`] or after the last attempt it returns the error via
/// [`into_error`].
pub(crate) async fn retry<T, F, Fut>(
    classify: impl Fn(&GuardError<Error>) -> RetryDecision,
    backoff: &Backoff,
    mut build: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, GuardError<Error>>>,
{
    let mut attempt = 1u32;
    loop {
        match build().await {
            Ok(val) => return Ok(val),
            Err(ge) => match classify(&ge) {
                RetryDecision::NoRetry => return Err(into_error(ge)),
                RetryDecision::Retry if attempt >= MAX_ATTEMPTS => return Err(into_error(ge)),
                RetryDecision::Retry => {
                    // `attempt - 1` is the 0-based retry index: 0 on the first
                    // failure, so the first backoff uses 2^0.
                    // TODO: thread the runtime's `AsyncSleep` impl (via
                    // RuntimeComponents, as the SDK does) instead of binding
                    // directly to tokio, so the backoff sleep is runtime-agnostic.
                    tokio::time::sleep(backoff.delay(attempt - 1, fastrand::f64())).await;
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
/// Retries a **transient transport** inner error (connection IO error or
/// client-side timeout). The SDK normally retries these over the rewindable
/// in-memory part body, but its retry can be defeated by token-bucket exhaustion
/// when many parts fail concurrently (an ENOBUFS-style burst); the outer
/// re-issue (with the loop's jittered backoff) lands after in-flight parts
/// complete and refill the quota, instead of re-colliding.
///
/// Everything else is terminal: a modeled service error (including throttling /
/// 503 `SlowDown`, which the SDK's shared token bucket exists to handle — a TM
/// re-issue would amplify the storm) and any non-transport inner error. Upload
/// has no latency deadline, so [`GuardError::DeadlineExceeded`] never arises here.
pub(crate) fn classify_upload_part_retry(ge: &GuardError<Error>) -> RetryDecision {
    match ge {
        GuardError::DeadlineExceeded(_) => RetryDecision::Retry,
        GuardError::Inner(e) if e.is_transient_transport() => RetryDecision::Retry,
        GuardError::Inner(_) => RetryDecision::NoRetry,
    }
}

/// Classifier for download body reads.
///
/// A chunk's body is consumed after the SDK's GetObject orchestration completes,
/// so the SDK's own retry never covers a mid-stream failure. This loop recovers
/// exactly that, re-issuing the ranged GET, and it is the SOLE retrier for the
/// body path. Retries (all with the loop's full-jittered backoff):
///   - a **latency-deadline timeout** (a slow-TTFB straggler) → re-issue on a
///     fresh connection.
///   - a **transient transport** failure on the re-issue's send (connection IO
///     error or client-side timeout) → these correlate under load (e.g. macOS
///     mbuf/ENOBUFS fails many connections at once), so the jittered backoff
///     spreads the retries instead of re-colliding on the same empty pool.
///   - a **body-stream** failure ([`ErrorKind::IOError`]: reset, truncated, short
///     body, SSP stall) → also clusters under a saturated link, same backoff.
///
/// A checksum mismatch ([`ErrorKind::IntegrityError`], classified at the body-read
/// boundary) is terminal: a corrupt body must never be re-fetched and masked.
/// Everything else is terminal.
pub(crate) fn classify_body_retry(ge: &GuardError<Error>) -> RetryDecision {
    let err = match ge {
        GuardError::DeadlineExceeded(_) => return RetryDecision::Retry,
        GuardError::Inner(e) => e,
    };

    match err.kind() {
        // A checksum mismatch must never be re-fetched and masked.
        ErrorKind::IntegrityError(_) => RetryDecision::NoRetry,
        // Transient transport on the send (surfaced as ServiceError with the
        // transient-transport flag).
        ErrorKind::ServiceError if err.is_transient_transport() => RetryDecision::Retry,
        // Mid-stream body-read failure (reset, truncation, SSP stall).
        ErrorKind::IOError => RetryDecision::Retry,
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

/// Stalled-stream grace period applied to transfer bodies.
///
/// Stalled-stream protection aborts a body whose *throughput* stays at zero for
/// this long, re-issuing the operation on a fresh connection. It fires only on
/// zero byte-progress, so a slow-but-progressing stream is never affected;
/// tightening below the SDK's 5s default only shortens how long a genuinely dead
/// gap wedges the transfer. Bounds a mid-download-body dead connection and a
/// mid-upload-body stall (the transferring peer stops making progress); it does
/// not bound a response that never arrives after the request body is fully sent.
const STALL_GRACE: Duration = Duration::from_secs(2);

/// Stalled-stream protection enabled with the tightened [`STALL_GRACE`].
fn tightened_ssp() -> aws_sdk_s3::config::StalledStreamProtectionConfig {
    aws_sdk_s3::config::StalledStreamProtectionConfig::enabled()
        .grace_period(STALL_GRACE)
        .build()
}

/// Per-operation config override for a download GET: bucket retry partition plus
/// the tightened stalled-stream grace on the response body.
///
/// Attach unconditionally via `.config_override(...)`; with no bucket the retry
/// partition is left at the client default and only the grace period is set.
pub(crate) fn download_get_override(bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
    bucket_partition_override(bucket).stalled_stream_protection(tightened_ssp())
}

/// Per-operation config override for an upload (UploadPart / PutObject): bucket
/// retry partition plus the tightened stalled-stream grace on the request body.
pub(crate) fn upload_override(bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
    bucket_partition_override(bucket).stalled_stream_protection(tightened_ssp())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Classifier for the loop tests: retry a `DeadlineExceeded` or an `IOError`
    /// inner; everything else terminal. Exercises both `GuardError` arms.
    fn classify_test(ge: &GuardError<Error>) -> RetryDecision {
        match ge {
            GuardError::DeadlineExceeded(_) => RetryDecision::Retry,
            GuardError::Inner(e) if *e.kind() == ErrorKind::IOError => RetryDecision::Retry,
            GuardError::Inner(_) => RetryDecision::NoRetry,
        }
    }

    // --- retry loop (pure; no deadline — timing is composed by callers) -----

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn retries_then_succeeds() {
        // First attempt fails retryably (IOError), second succeeds.
        let attempts = AtomicUsize::new(0);
        let result = retry(classify_test, &Backoff::transient(), || {
            let n = attempts.fetch_add(1, Ordering::Relaxed);
            async move {
                if n == 0 {
                    return Err(GuardError::Inner(Error::new(ErrorKind::IOError, "reset")));
                }
                Ok::<_, GuardError<Error>>(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn deadline_exceeded_is_retried() {
        // A `DeadlineExceeded` (what a composed `guarded` produces on timeout) is
        // a retryable arm; the terminal one renders to an IOError via `into_error`.
        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry(classify_test, &Backoff::transient(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(GuardError::DeadlineExceeded(Duration::from_millis(200))) }
        })
        .await;

        let err = result.unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::IOError);
        assert_eq!(attempts.load(Ordering::Relaxed), MAX_ATTEMPTS as usize);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn exhausts_attempts_on_persistent_retryable_error() {
        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry(classify_test, &Backoff::transient(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(GuardError::Inner(Error::new(ErrorKind::IOError, "reset"))) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), MAX_ATTEMPTS as usize);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn does_not_retry_terminal_inner_error() {
        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry(classify_test, &Backoff::transient(), || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async {
                Err::<(), _>(GuardError::Inner(Error::new(
                    ErrorKind::RuntimeError,
                    "simulated SDK error",
                )))
            }
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

    #[test]
    fn upload_transient_transport_retries() {
        // A transient-transport ServiceError (the ENOBUFS shape) is retried.
        let d = classify_upload_part_retry(&GuardError::Inner(Error::test_transient_transport()));
        assert_eq!(d, RetryDecision::Retry);
    }

    #[test]
    fn upload_non_transport_service_error_is_terminal() {
        // A plain ServiceError (e.g. throttling/permanent) must NOT retry here —
        // throttling is the SDK token bucket's job.
        let err = Error::new(ErrorKind::ServiceError, "503 SlowDown");
        assert_eq!(
            classify_upload_part_retry(&GuardError::Inner(err)),
            RetryDecision::NoRetry
        );
    }

    #[test]
    fn upload_other_inner_error_is_terminal() {
        let err = Error::new(ErrorKind::RuntimeError, "not a transport error");
        assert_eq!(
            classify_upload_part_retry(&GuardError::Inner(err)),
            RetryDecision::NoRetry
        );
    }

    // --- classify_body_retry ------------------------------------------------

    #[test]
    fn body_retry_deadline_exceeded_retries() {
        let d = classify_body_retry(&GuardError::DeadlineExceeded(Duration::from_secs(1)));
        assert_eq!(d, RetryDecision::Retry);
    }

    #[test]
    fn body_retry_cancellation_does_not_retry() {
        let err = Error::new(ErrorKind::OperationCancelled, "cancelled mid body");
        assert_eq!(
            classify_body_retry(&GuardError::Inner(err)),
            RetryDecision::NoRetry
        );
    }

    #[test]
    fn body_retry_io_error_retries() {
        // A body-stream failure (reset, truncation, SSP stall) is retried.
        let err = Error::new(ErrorKind::IOError, "connection reset by peer");
        assert_eq!(
            classify_body_retry(&GuardError::Inner(err)),
            RetryDecision::Retry
        );
    }

    #[test]
    fn body_retry_transient_transport_retries() {
        // A transient-transport send failure (ENOBUFS/connect/timeout class).
        let d = classify_body_retry(&GuardError::Inner(Error::test_transient_transport()));
        assert_eq!(d, RetryDecision::Retry);
    }

    // The integrity-error-is-terminal contract is covered end-to-end by the
    // `corrupt_body_once_is_not_retried` integration test (a real checksum
    // mismatch through the full stack), which is stronger than a unit assertion
    // over a hand-built error.
}
