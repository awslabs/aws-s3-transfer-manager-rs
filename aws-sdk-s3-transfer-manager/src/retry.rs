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

use crate::error::{Error, ErrorKind};
use crate::metrics::latency::{GuardError, LatencyTracker};

/// Maximum attempts per operation before the last failure is returned. Counts
/// every attempt, not just timeouts: a retryable failure of any kind re-issues
/// until this budget is spent. Each retry runs on a fresh connection.
const MAX_ATTEMPTS: u32 = 3;

/// A classifier's verdict for a failed attempt.
pub(crate) enum RetryDecision {
    /// Re-issue the operation, optionally after a backoff delay.
    Retry {
        /// `None` retries immediately; `Some` sleeps first (used for throttling).
        after: Option<Duration>,
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
/// `classify` sees the [`GuardError`] directly, so a deadline timeout is a
/// distinct arm from inner errors. [`into_error`] renders a terminal
/// `DeadlineExceeded` into the returned `Error` only at the return path.
pub(crate) async fn retry_guarded<T, F, Fut>(
    tracker: &LatencyTracker,
    classify: impl Fn(&GuardError<Error>) -> RetryDecision,
    mut build: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut attempt = 1;
    loop {
        match tracker.guarded(build()).await {
            Ok(val) => return Ok(val),
            Err(ge) => match classify(&ge) {
                RetryDecision::NoRetry => return Err(into_error(ge)),
                RetryDecision::Retry { .. } if attempt >= MAX_ATTEMPTS => {
                    return Err(into_error(ge));
                }
                RetryDecision::Retry { after } => {
                    if let Some(delay) = after {
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

/// Classifier that retries only on a deadline timeout and never on an inner
/// error.
///
/// Reproduces the pre-split behavior: stragglers retry on a fresh connection,
/// a returned inner error is terminal. Used by the upload part-send path, where
/// the SDK already retries the dispatch over a rewindable body.
pub(crate) fn retry_deadline_only(ge: &GuardError<Error>) -> RetryDecision {
    match ge {
        GuardError::DeadlineExceeded(_) => RetryDecision::Retry { after: None },
        GuardError::Inner(_) => RetryDecision::NoRetry,
    }
}

/// Classifier for download body reads.
///
/// A chunk's body is consumed after the SDK's GetObject orchestration
/// completes, so the SDK's own retry never covers a mid-stream failure. This
/// loop exists to recover exactly that: a body-read failure (connection reset,
/// truncated or short body) re-issues the ranged GET.
///
/// Within the body-read closure the error kind already separates the cases: a
/// body-stream failure is an [`ErrorKind::IOError`], while a send-time failure
/// arrives as an `SdkError`-derived kind that the SDK already retried before
/// surfacing — re-issuing those is redundant, so they are terminal. A checksum
/// mismatch arrives as [`ErrorKind::IntegrityError`] (classified at the body-read
/// boundary) and is terminal: a corrupt body must never be re-fetched and masked.
pub(crate) fn classify_body_retry(ge: &GuardError<Error>) -> RetryDecision {
    let err = match ge {
        GuardError::DeadlineExceeded(_) => return RetryDecision::Retry { after: None },
        GuardError::Inner(e) => e,
    };

    match err.kind() {
        // A checksum mismatch must never be re-fetched and masked.
        ErrorKind::IntegrityError(_) => RetryDecision::NoRetry,
        ErrorKind::IOError => RetryDecision::Retry { after: None },
        _ => RetryDecision::NoRetry,
    }
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn deadline_only_retries_timeout_then_succeeds() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker); // deadline ≈ 200ms

        let attempts = AtomicUsize::new(0);
        let result = retry_guarded(&tracker, retry_deadline_only, || {
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

    #[cfg_attr(miri, ignore)]
    #[tokio::test(start_paused = true)]
    async fn deadline_only_exhausts_attempts_on_persistent_timeout() {
        let tracker = LatencyTracker::new();
        warm_tracker(&tracker);

        let attempts = AtomicUsize::new(0);
        let result: Result<(), _> = retry_guarded(&tracker, retry_deadline_only, || {
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
    async fn deadline_only_does_not_retry_inner_error() {
        let tracker = LatencyTracker::new();
        let attempts = AtomicUsize::new(0);

        let result: Result<(), _> = retry_guarded(&tracker, retry_deadline_only, || {
            attempts.fetch_add(1, Ordering::Relaxed);
            async { Err::<(), _>(Error::new(ErrorKind::RuntimeError, "simulated SDK error")) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    // classify_body_retry — arms that can be constructed directly. The SdkError
    // (throttle/transient/permanent) and checksum-mismatch arms use real wire
    // error shapes; those are covered by the mock-server fault integration tests
    // (the non_exhaustive checksum error cannot be built outside its crate).

    fn retry_after(decision: RetryDecision) -> Option<Option<Duration>> {
        match decision {
            RetryDecision::Retry { after } => Some(after),
            RetryDecision::NoRetry => None,
        }
    }

    #[test]
    fn body_retry_deadline_exceeded_retries_immediately() {
        let decision = classify_body_retry(&GuardError::DeadlineExceeded(Duration::from_secs(1)));
        assert_eq!(retry_after(decision), Some(None));
    }

    #[test]
    fn body_retry_cancellation_does_not_retry() {
        let err = Error::new(ErrorKind::OperationCancelled, "cancelled mid body");
        let decision = classify_body_retry(&GuardError::Inner(err));
        assert!(matches!(decision, RetryDecision::NoRetry));
    }

    #[test]
    fn body_retry_unrecognized_io_error_retries() {
        // A transport/byte-stream error that isn't an SdkError or checksum
        // mismatch falls through to retry-by-default.
        let err = Error::new(ErrorKind::IOError, "connection reset by peer");
        let decision = classify_body_retry(&GuardError::Inner(err));
        assert_eq!(retry_after(decision), Some(None));
    }
}
