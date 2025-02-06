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
use crate::types::ConcurrencyMode;

/// Default throughput target for auto mode
const AUTO_TARGET_THROUGHPUT_GIGABYTES_PER_SEC: u64 = ByteUnit::Gigabit.as_bytes_u64() * 10;

/// Estimated P50 latency for S3
const S3_P50_REQUEST_LATENCY_MS: usize = 30;

/// Estimated per/request max throughput S3 is capable of
const S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC: u64 = 90;

/// Minimum concurrent requests at full throughput we want to support
const MIN_CONCURRENT_REQUESTS: usize = 8;

/// Min bucket size tokens
const MIN_TOKENS: usize =
    (S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC as usize) * 8 * MIN_CONCURRENT_REQUESTS;

/// Minimum token cost regardless of payload size
const MIN_PAYLOAD_COST_TOKENS: usize = 5;

impl PermitType {
    fn token_cost(&self) -> u32 {
        let cost = match *self {
            PermitType::DataPlane(payload_size) => tokens_for_payload(payload_size),
            _ => MIN_PAYLOAD_COST_TOKENS,
        };
        cost.try_into().unwrap()
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
        let max_permits = match &mode {
            ConcurrencyMode::Auto => token_bucket_size(Throughput::new_bytes_per_sec(
                AUTO_TARGET_THROUGHPUT_GIGABYTES_PER_SEC,
            )),
            ConcurrencyMode::TargetThroughput(target_throughput) => {
                // TODO - we don't publicly allow configuring upload/download independently so we
                // just pick one for now as they must be the same at the moment.
                let thrpt = target_throughput.download();
                token_bucket_size(*thrpt)
            }
            ConcurrencyMode::Explicit(concurrency) => *concurrency,
        };

        TokenBucket {
            semaphore: Arc::new(Semaphore::new(max_permits)),
            mode,
        }
    }

    /// Calculate the token cost for the given permit type (and current mode)
    fn cost(&self, ptype: PermitType) -> u32 {
        match self.mode {
            // in explicit mode each acquire is weighted the same regardless of permit type
            ConcurrencyMode::Explicit(_) => 1,
            _ => ptype.token_cost(),
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
fn token_bucket_size(throughput: Throughput) -> usize {
    let megabit_per_sec = throughput.as_unit_per_sec(ByteUnit::Megabit).max(1.0) as usize;
    cmp::max(MIN_TOKENS, megabit_per_sec)
}

/// Tokens for payload size
fn tokens_for_payload(payload_size: u64) -> usize {
    let estimated_mbps = estimated_throughput(
        payload_size,
        S3_P50_REQUEST_LATENCY_MS,
        Throughput::new_bytes_per_sec(S3_MAX_PER_REQUEST_THROUGHPUT_MB_PER_SEC * 1000 * 1000),
    )
    .as_unit_per_sec(ByteUnit::Megabit)
    .round()
    .max(1.0) as usize;

    cmp::max(estimated_mbps, MIN_PAYLOAD_COST_TOKENS)
}

/// Estimate the throughput of a given request payload based on S3 latencies
/// and max per/connection estimates.
fn estimated_throughput(
    payload_size: u64,
    estimated_p50_latency_ms: usize,
    estimated_max_request_throughput: Throughput,
) -> Throughput {
    let req_estimate = Throughput::new(
        payload_size,
        Duration::from_millis(estimated_p50_latency_ms as u64),
    );

    // take lower of the maximum per request estimate service is capable of or the estimate based on the payload
    cmp::min_by(estimated_max_request_throughput, req_estimate, |x, y| {
        x.partial_cmp(y).expect("valid order")
    })
}

#[cfg(test)]
mod tests {
    use crate::metrics::unit::ByteUnit;
    use crate::runtime::token_bucket::{estimated_throughput, tokens_for_payload};
    use crate::{
        metrics::Throughput,
        runtime::token_bucket::{token_bucket_size, MIN_TOKENS},
    };

    const MEGABYTE: u64 = 1000 * 1000;

    #[test]
    fn test_estimated_throughput() {
        let estimated_latency_ms = 1000;
        let estimated_max_request_throughput = Throughput::new_bytes_per_sec(2 * MEGABYTE);
        assert_eq!(
            Throughput::new_bytes_per_sec(MEGABYTE),
            estimated_throughput(
                MEGABYTE,
                estimated_latency_ms,
                estimated_max_request_throughput
            )
        );
        assert_eq!(
            estimated_max_request_throughput,
            estimated_throughput(
                3 * MEGABYTE,
                estimated_latency_ms,
                estimated_max_request_throughput
            )
        );
    }

    #[test]
    fn test_token_bucket_size() {
        assert_eq!(
            MIN_TOKENS,
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
        assert_eq!(5, tokens_for_payload(1024));
        assert_eq!(27, tokens_for_payload(100 * 1024));
        assert_eq!(267, tokens_for_payload(MEGABYTE));
        assert_eq!(720, tokens_for_payload(5 * MEGABYTE));
        assert_eq!(720, tokens_for_payload(8 * MEGABYTE));
        assert_eq!(720, tokens_for_payload(1000 * MEGABYTE));
    }
}
