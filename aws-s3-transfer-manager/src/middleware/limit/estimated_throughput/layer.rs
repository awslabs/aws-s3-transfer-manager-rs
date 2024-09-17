/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::service::EstimatedThroughputConcurrencyLimit;
use crate::metrics::{self, Throughput};
use tower::Layer;

/// Estimated P50 latency for S3
const S3_P50_REQUEST_LATENCY_MS: usize = 30;

/// Estimated per/request max throughput S3 is capable of
const S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC: u64 = 90;

/// Enforces a limit on the number of concurrent requests the underlying service
/// can handle based on a configured target throughput objective and estimated in-flight
/// throughput using heuristics.
#[derive(Debug, Clone)]
pub(crate) struct EstimatedThroughputConcurrencyLimitLayer {
    estimated_p50_latency_ms: usize,
    estimated_max_request_throughput: Throughput,
    target_throughput: Throughput,
}

impl EstimatedThroughputConcurrencyLimitLayer {
    /// Create a new estimated throughput limit layer using S3 service defaults for
    /// estimated p50 latency and estimated max request throughput
    pub(crate) fn s3_defaults(target_throughput: Throughput) -> Self {
        Self {
            estimated_p50_latency_ms: S3_P50_REQUEST_LATENCY_MS,
            estimated_max_request_throughput: Throughput::new_bytes_per_sec(
                S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC
                    * metrics::unit::Bytes::Megabyte.as_bytes_u64(),
            ),
            target_throughput,
        }
    }
}

impl<S> Layer<S> for EstimatedThroughputConcurrencyLimitLayer {
    type Service = EstimatedThroughputConcurrencyLimit<S>;
    fn layer(&self, service: S) -> Self::Service {
        EstimatedThroughputConcurrencyLimit::new(
            service,
            self.estimated_p50_latency_ms,
            self.estimated_max_request_throughput,
            self.target_throughput,
        )
    }
}
