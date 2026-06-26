/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod adaptive;

pub(crate) use adaptive::{AdaptiveConcurrencyController, AdaptiveConfig};

use std::fmt;

use aws_sdk_s3::error::SdkError;
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ErrorKind {
    /// S3 throttling (503, 429, SlowDown). Triggers immediate backoff.
    Throttle,
    /// Transient errors (timeout, connection reset). Noise for the controller.
    Transient,
    /// Permanent errors (404, 403). Transfer-level concern, not controller.
    Permanent,
}

/// Per-work-item completion data reported to the concurrency controller.
///
/// Built by the scheduler from work outcomes. The controller uses aggregate
/// goodput (bytes over time) to adjust the concurrency target.
#[derive(Debug, Clone)]
pub(crate) struct CompletionSample {
    /// Error classification, if the work item failed.
    pub error: Option<ErrorKind>,
}

/// Controls how many work items can be in-flight concurrently.
///
/// The scheduler calls `target()` before generating work. Work is only
/// generated when total in-flight + pending is below the target.
///
/// Implementations: [`FixedConcurrency`] (constant target),
/// [`AdaptiveConcurrencyController`] (adjusts based on observed throughput).
pub(crate) trait ConcurrencyController: Send + Sync + fmt::Debug {
    /// Current concurrency target. May change between calls.
    ///
    /// MUST be `>= 1`. The scheduler's work-generation retire path relies on
    /// this: when at capacity (`dispatched >= target`) it retires the runner
    /// rather than spinning, trusting that an in-flight item will complete and
    /// re-drive generation. A `target` of 0 would let "at capacity" hold with
    /// nothing in flight, stranding queued work with no completion to wake it.
    fn target(&self) -> usize;
    /// Called when a work item is dispatched to a worker.
    fn on_dispatch(&self) {}
    fn on_completion(&self, _sample: &CompletionSample) {}
}

/// Fixed concurrency target that never changes.
#[derive(Debug)]
pub(crate) struct FixedConcurrency(usize);

impl FixedConcurrency {
    pub(crate) fn new(target: usize) -> Self {
        assert!(target > 0, "concurrency target must be > 0");
        Self(target)
    }
}

impl ConcurrencyController for FixedConcurrency {
    fn target(&self) -> usize {
        self.0
    }
}

// S3 error codes that indicate throttling. Subset of the SDK's full list,
// filtered to codes S3 actually returns.
const THROTTLING_CODES: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "SlowDown",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
];

/// Classify an SDK error for adaptive concurrency decisions.
///
/// Throttle errors trigger immediate concurrency reduction. Transient errors
/// are noise (don't affect the algorithm). Permanent errors are transfer-level
/// concerns handled elsewhere.
pub(crate) fn classify_sdk_error<E>(err: &SdkError<E, Response>) -> ErrorKind
where
    E: ProvideErrorMetadata,
{
    match err {
        SdkError::ServiceError(ctx) => {
            // Check error code first (most specific signal)
            if let Some(code) = ctx.err().code() {
                if THROTTLING_CODES.contains(&code) {
                    return ErrorKind::Throttle;
                }
            }
            // Fall back to HTTP status code
            match ctx.raw().status().as_u16() {
                429 | 503 => ErrorKind::Throttle,
                500 | 502 | 504 => ErrorKind::Transient,
                _ => ErrorKind::Permanent,
            }
        }
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => ErrorKind::Transient,
        _ => ErrorKind::Transient,
    }
}

/// Classify an error for adaptive concurrency by walking the source chain.
///
/// Attempts to downcast to each SDK error type we use. Returns `None` for
/// non-SDK errors (I/O errors, panics, etc.).
pub(crate) fn classify_error(err: &crate::error::Error) -> Option<ErrorKind> {
    use aws_sdk_s3::error::SdkError;
    use aws_sdk_s3::operation::{
        complete_multipart_upload::CompleteMultipartUploadError,
        create_multipart_upload::CreateMultipartUploadError, get_object::GetObjectError,
        put_object::PutObjectError, upload_part::UploadPartError,
    };
    use std::error::Error as _;

    let mut source: Option<&(dyn std::error::Error + 'static)> = err.source();
    while let Some(e) = source {
        if let Some(e) = e.downcast_ref::<SdkError<GetObjectError>>() {
            return Some(classify_sdk_error(e));
        }
        if let Some(e) = e.downcast_ref::<SdkError<CreateMultipartUploadError>>() {
            return Some(classify_sdk_error(e));
        }
        if let Some(e) = e.downcast_ref::<SdkError<UploadPartError>>() {
            return Some(classify_sdk_error(e));
        }
        if let Some(e) = e.downcast_ref::<SdkError<CompleteMultipartUploadError>>() {
            return Some(classify_sdk_error(e));
        }
        if let Some(e) = e.downcast_ref::<SdkError<PutObjectError>>() {
            return Some(classify_sdk_error(e));
        }
        if let Some(e) = e.downcast_ref::<SdkError<aws_sdk_s3::Error>>() {
            return Some(classify_sdk_error(e));
        }
        source = e.source();
    }
    None
}
