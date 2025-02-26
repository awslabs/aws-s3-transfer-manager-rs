/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use pin_project_lite::pin_project;
use std::future::Future;
use std::task::Poll;
use std::{cmp, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio_util::sync::PollSemaphore;

use crate::error;
use crate::metrics::{unit::ByteUnit, Throughput};
use crate::runtime::scheduler::PermitType;
use crate::types::BucketType;
use crate::types::ConcurrencyMode;

use super::scheduler::{NetworkPermitContext, TransferDirection};

/// Default throughput target for auto mode (10 Gbps)
///
/// Source: CRT default for target throughput: https://github.com/awslabs/aws-c-s3/blob/6eb8be530b100fed5c6d24ca48a57ee2e6098fbf/source/s3_client.c#L79
/// Applies to: ConcurrencyMode::TargetThroughput
const AUTO_TARGET_THROUGHPUT: Throughput =
    Throughput::new_bytes_per_sec(10 * ByteUnit::Gigabit.as_bytes_u64());

/// Estimated P50 latency for S3
///
/// Source: S3 team
/// This is internal implementation detail and subject to change in future.
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_P50_REQUEST_LATENCY: Duration = Duration::from_millis(30);

/// Estimated P50 latency for S3Express
///
/// Source: S3 team
/// This is internal implementation detail and subject to change in future.
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_EXPRESS_P50_REQUEST_LATENCY: Duration = Duration::from_millis(4);

/// Estimated per/request max download throughput S3 is capable of
///
/// Source: S3 team and S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html#optimizing-performance-parallelization
/// > Make one concurrent request for each 85-90 MB/s of desired network throughput
///
/// This is internal implementation detail and subject to change in future.
///
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_MAX_PER_REQUEST_DOWNLOAD_THROUGHPUT: Throughput =
    Throughput::new_bytes_per_sec(90 * 1000 * 1000);

/// Estimated per/request max upload throughput S3 is capable of
///
/// Source: S3 team.
/// This is internal implementation detail and subject to change in future.
///
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_MAX_PER_REQUEST_UPLOAD_THROUGHPUT: Throughput =
    Throughput::new_bytes_per_sec(20 * 1000 * 1000);

/// Estimated per/request max download throughput S3Express is capable of
///
/// Source: S3Express team.
/// This is internal implementation detail and subject to change in future.
///
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_EXPRESS_MAX_PER_REQUEST_DOWNLOAD_THROUGHPUT: Throughput =
    Throughput::new_bytes_per_sec(150 * 1000 * 1000);

/// Estimated per/request max upload throughput S3Express is capable of
///
/// Source: S3Express team. The actual upload per connection speed is 125Mbps but our testing
/// showed that having more connections has better throughput.
/// This is internal implementation detail and subject to change in future.
///
/// Applies to: ConcurrencyMode::TargetThroughput
const S3_EXPRESS_MAX_PER_REQUEST_UPLOAD_THROUGHPUT: Throughput =
    Throughput::new_bytes_per_sec(110 * 1000 * 1000);

/// Minimum concurrent requests at full throughput we want to support
///
/// Source: None, reasonable default
/// Applies to: ConcurrencyMode::TargetThroughput
const MIN_CONCURRENT_REQUESTS: u64 = 8;

/// Min tokens for a bucket.
///
/// NOTE: In target throughput mode 1 token = 1 Mbit of estimated throughput.
/// To ensure min concurrent requests capacity is available we use the estimated
/// max S3 throughput for a single connection to figure out the number of tokens
/// we'd need to achieve that concurrency.
///
/// Source: None, reasonable default
/// Applies to: ConcurrencyMode::TargetThroughput
const MIN_BUCKET_TOKENS: u64 = (S3_EXPRESS_MAX_PER_REQUEST_DOWNLOAD_THROUGHPUT.bytes_transferred()
    / 1_000_000)
    * 8
    * MIN_CONCURRENT_REQUESTS;

/// Minimum token cost regardless of payload size
///
/// Source: None, reasonable default
/// Applies to: ConcurrencyMode::TargetThroughput
const MIN_PAYLOAD_COST_TOKENS: u64 = 5;

impl PermitType {
    /// The token cost for the permit type in Mbps
    fn token_cost_megabit_per_sec(&self) -> u32 {
        let cost = match self {
            PermitType::Network(network_ctx) => tokens_for_network_context(network_ctx),
        };
        cost.try_into().unwrap()
    }
}

impl NetworkPermitContext {
    fn max_per_request_throughput(&self) -> Throughput {
        match (&self.bucket_type, &self.direction) {
            (BucketType::Standard, TransferDirection::Download) => {
                S3_MAX_PER_REQUEST_DOWNLOAD_THROUGHPUT
            }
            (BucketType::Standard, TransferDirection::Upload) => {
                S3_MAX_PER_REQUEST_UPLOAD_THROUGHPUT
            }
            (BucketType::Express, TransferDirection::Download) => {
                S3_EXPRESS_MAX_PER_REQUEST_DOWNLOAD_THROUGHPUT
            }
            (BucketType::Express, TransferDirection::Upload) => {
                S3_EXPRESS_MAX_PER_REQUEST_UPLOAD_THROUGHPUT
            }
        }
    }

    fn p50_request_latency(&self) -> Duration {
        match &self.bucket_type {
            BucketType::Standard => S3_P50_REQUEST_LATENCY,
            BucketType::Express => S3_EXPRESS_P50_REQUEST_LATENCY,
        }
    }
}

/// Token bucket used for controlling target throughput
///
/// Tokens are weighted based on the permit type being acquired (e.g. based on payload size).
#[derive(Debug, Clone)]
pub(crate) struct TokenBucket {
    // NOTE: tokio semaphore is fair, permits are given out in the order requested
    semaphore: Arc<Semaphore>,
    mode: ConcurrencyMode,
}

impl TokenBucket {
    /// Create a new token bucket using the given target throughput to set the maximum number of tokens
    pub(crate) fn new(mode: ConcurrencyMode) -> Self {
        // Permits/tokens are dependent on the concurrency mode:
        //
        // ConcurrencyMode::TargetThroughput -> 1 token = 1 Mbit of throughput
        // ConcurrencyMode::Explicit -> 1 token = 1 request
        let max_tokens = match &mode {
            ConcurrencyMode::Auto => token_bucket_size(AUTO_TARGET_THROUGHPUT),
            ConcurrencyMode::TargetThroughput(target_throughput) => {
                // TODO - we don't (yet) publicly allow configuring upload/download independently so we
                // just pick one for now as they must be the same at the moment.
                let thrpt = target_throughput.download();
                token_bucket_size(*thrpt)
            }
            ConcurrencyMode::Explicit(concurrency) => *concurrency as u64,
        };

        TokenBucket {
            semaphore: Arc::new(Semaphore::new(max_tokens.try_into().unwrap())),
            mode,
        }
    }

    /// Calculate the token cost for the given permit type (and current mode)
    fn cost(&self, ptype: PermitType) -> u32 {
        match self.mode {
            // in explicit mode each acquire is weighted the same regardless of permit type
            ConcurrencyMode::Explicit(_) => 1,
            _ => ptype.token_cost_megabit_per_sec(),
        }
    }

    /// Acquire a token for the given permit type. Tokens are returned to the bucket when the
    /// [OwnedToken] is dropped.
    pub(crate) fn acquire(&self, ptype: PermitType) -> AcquireTokenFuture {
        AcquireTokenFuture::new(PollSemaphore::new(self.semaphore.clone()), self.cost(ptype))
    }

    pub(crate) fn try_acquire(
        &self,
        ptype: PermitType,
    ) -> Result<Option<OwnedToken>, error::Error> {
        let cost = self.cost(ptype);
        match self.semaphore.clone().try_acquire_many_owned(cost) {
            Ok(permit) => Ok(Some(OwnedToken::new(permit))),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(err @ TryAcquireError::Closed) => {
                Err(error::Error::new(error::ErrorKind::RuntimeError, err))
            }
        }
    }
}

pin_project! {
    #[derive(Debug, Clone)]
    pub(crate) struct AcquireTokenFuture {
        sem: PollSemaphore,
        tokens: u32
    }
}

impl AcquireTokenFuture {
    fn new(sem: PollSemaphore, tokens: u32) -> Self {
        Self { sem, tokens }
    }
}

impl Future for AcquireTokenFuture {
    type Output = Result<OwnedToken, error::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this.sem.poll_acquire_many(cx, *this.tokens) {
            Poll::Ready(Some(permit)) => Poll::Ready(Ok(OwnedToken::new(permit))),
            Poll::Ready(None) => Poll::Ready(Err(error::Error::new(
                error::ErrorKind::RuntimeError,
                "semaphore closed",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An owned permit from the scheduler to perform some unit of work.
#[must_use]
#[clippy::has_significant_drop]
#[derive(Debug)]
pub(crate) struct OwnedToken {
    _inner: OwnedSemaphorePermit,
}

impl OwnedToken {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        OwnedToken { _inner: permit }
    }
}

/// Get the token bucket size to use for a given target throughput
fn token_bucket_size(throughput: Throughput) -> u64 {
    let megabit_per_sec = throughput.as_unit_per_sec(ByteUnit::Megabit).max(1.0) as u64;
    cmp::max(MIN_BUCKET_TOKENS, megabit_per_sec)
}

/// Tokens for network context
fn tokens_for_network_context(network_context: &NetworkPermitContext) -> u64 {
    let estimated_mbps = estimated_throughput(
        network_context.payload_size_estimate,
        network_context.p50_request_latency(),
        network_context.max_per_request_throughput(),
    )
    .as_unit_per_sec(ByteUnit::Megabit)
    .round()
    .max(1.0) as u64;

    cmp::max(estimated_mbps, MIN_PAYLOAD_COST_TOKENS)
}

/// Estimate the throughput of a given request payload based on S3 latencies
/// and max per/connection estimates.
fn estimated_throughput(
    payload_size_bytes: u64,
    estimated_p50_latency: Duration,
    estimated_max_request_throughput: Throughput,
) -> Throughput {
    let req_estimate = Throughput::new(payload_size_bytes, estimated_p50_latency);

    // take lower of the maximum per request estimate service is capable of or the estimate based on the payload
    cmp::min_by(estimated_max_request_throughput, req_estimate, |x, y| {
        x.partial_cmp(y).expect("valid order")
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::metrics::unit::ByteUnit;
    use crate::runtime::scheduler::{NetworkPermitContext, TransferDirection};
    use crate::runtime::token_bucket::{estimated_throughput, tokens_for_network_context};
    use crate::types::BucketType;
    use crate::{
        metrics::Throughput,
        runtime::token_bucket::{token_bucket_size, MIN_BUCKET_TOKENS},
    };

    const MEGABYTE: u64 = 1000 * 1000;

    #[test]
    fn test_estimated_throughput() {
        let estimated_latency = Duration::from_secs(1);
        let estimated_max_request_throughput = Throughput::new_bytes_per_sec(2 * MEGABYTE);
        assert_eq!(
            Throughput::new_bytes_per_sec(MEGABYTE),
            estimated_throughput(
                MEGABYTE,
                estimated_latency,
                estimated_max_request_throughput
            )
        );
        assert_eq!(
            estimated_max_request_throughput,
            estimated_throughput(
                3 * MEGABYTE,
                estimated_latency,
                estimated_max_request_throughput
            )
        );
    }

    #[test]
    fn test_token_bucket_size() {
        assert_eq!(
            MIN_BUCKET_TOKENS,
            token_bucket_size(Throughput::new_bytes_per_sec(1000))
        );
        assert_eq!(
            10_000,
            token_bucket_size(Throughput::new_bytes_per_sec(
                ByteUnit::Gigabit.as_bytes_u64() * 10
            ))
        );
    }

    #[test]
    fn test_tokens_for_payload() {
        assert_eq!(
            5,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 1024,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            27,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 100 * 1024,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            267,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: MEGABYTE,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            720,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 5 * MEGABYTE,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            720,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 8 * MEGABYTE,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            720,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 1000 * MEGABYTE,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            27,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 100 * 1024,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Upload,
            })
        );
        assert_eq!(
            160,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: MEGABYTE,
                bucket_type: BucketType::Standard,
                direction: TransferDirection::Upload,
            })
        );
        assert_eq!(
            205,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 100 * 1024,
                bucket_type: BucketType::Express,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            1200,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: MEGABYTE,
                bucket_type: BucketType::Express,
                direction: TransferDirection::Download,
            })
        );
        assert_eq!(
            205,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: 100 * 1024,
                bucket_type: BucketType::Express,
                direction: TransferDirection::Upload,
            })
        );
        assert_eq!(
            880,
            tokens_for_network_context(&NetworkPermitContext {
                payload_size_estimate: MEGABYTE,
                bucket_type: BucketType::Express,
                direction: TransferDirection::Upload,
            })
        );
    }
}
