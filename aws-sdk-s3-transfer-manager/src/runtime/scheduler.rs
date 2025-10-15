/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use futures_util::TryFutureExt;
use pin_project_lite::pin_project;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use crate::error;
use crate::metrics::aggregators::SchedulerMetrics;
use crate::runtime::token_bucket::{OwnedToken, TokenBucket};
use crate::types::BucketType;
use crate::types::ConcurrencyMode;

// TODO - measure actual throughput and track high water mark

/// Manages scheduling networking and I/O work
///
/// Scheduler is internally reference-counted and can be freely cloned.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    token_bucket: TokenBucket,
    pub(crate) metrics: Arc<SchedulerMetrics>,
}

impl Scheduler {
    /// Create a new scheduler with the initial number of work permits.
    pub(crate) fn new(mode: ConcurrencyMode) -> Self {
        let metrics = Arc::new(SchedulerMetrics::new());
        let token_bucket = TokenBucket::new(mode, metrics.clone());

        Self {
            token_bucket,
            metrics,
        }
    }

    /// Get the maximum number of tokens this scheduler can hold
    pub(crate) fn max_tokens(&self) -> u64 {
        self.token_bucket.max_tokens()
    }

    /// Acquire a permit to perform some unit of work
    pub(crate) fn acquire_permit(&self, ptype: PermitType) -> AcquirePermitFuture {
        let start_time = Instant::now();

        match self.try_acquire_permit(ptype.clone()) {
            Ok(Some(permit)) => {
                // Record immediate acquisition
                self.metrics.record_permit_wait_time(0.0);
                AcquirePermitFuture::ready(Ok(permit))
            }
            Ok(None) => {
                let metrics = self.metrics.clone();
                let inner = self.token_bucket.acquire(ptype).map_ok(move |token| {
                    // Record wait time when permit is acquired
                    metrics.record_permit_wait_time(start_time.elapsed().as_secs_f64());
                    OwnedWorkPermit::from(token)
                });
                AcquirePermitFuture::new(inner)
            }
            Err(err) => {
                self.metrics.record_permit_acquisition_failure();
                AcquirePermitFuture::ready(Err(err))
            }
        }
    }

    fn try_acquire_permit(
        &self,
        ptype: PermitType,
    ) -> Result<Option<OwnedWorkPermit>, error::Error> {
        self.token_bucket
            .try_acquire(ptype)
            .map(|token| token.map(OwnedWorkPermit::from))
    }
}

/// Direction of the transfer
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TransferDirection {
    Upload,
    Download,
}

/// Context needed to determine number of tokens needed for Network Permit.
#[derive(Debug, Clone)]
pub(crate) struct NetworkPermitContext {
    pub(crate) payload_size_estimate: u64,
    pub(crate) bucket_type: BucketType,
    pub(crate) direction: TransferDirection,
}

// TODO - when we support configuring throughput indepently we'll need to distinguish the permit
// type and track it separately in the token bucket(s).

/// The type of work to be done
#[derive(Debug, Clone)]
pub(crate) enum PermitType {
    /// A network request to transmit or receive data from an API.
    Network(NetworkPermitContext),
}

/// An owned permit from the scheduler to perform some unit of work.
#[must_use]
#[clippy::has_significant_drop]
#[derive(Debug)]
pub(crate) struct OwnedWorkPermit {
    _inner: OwnedToken,
}

impl From<OwnedToken> for OwnedWorkPermit {
    fn from(value: OwnedToken) -> Self {
        Self { _inner: value }
    }
}

pin_project! {
    #[derive(Debug)]
    pub(crate) struct AcquirePermitFuture {
        #[pin]
        inner: aws_smithy_async::future::now_or_later::NowOrLater<
            Result<OwnedWorkPermit, error::Error>,
            aws_smithy_async::future::BoxFuture<'static, OwnedWorkPermit, error::Error>
        >,
    }
}

impl AcquirePermitFuture {
    // TODO - with the addition of a concrete token future type we can probably get rid of the
    // boxing of aws_smithy_async::NowOrLater here...
    fn new<F>(future: F) -> Self
    where
        F: Future<Output = Result<OwnedWorkPermit, error::Error>> + Send + 'static,
    {
        Self {
            inner: aws_smithy_async::future::now_or_later::NowOrLater::new(Box::pin(future)),
        }
    }

    fn ready(result: Result<OwnedWorkPermit, error::Error>) -> Self {
        Self {
            inner: aws_smithy_async::future::now_or_later::NowOrLater::ready(result),
        }
    }
}

impl Future for AcquirePermitFuture {
    type Output = Result<OwnedWorkPermit, error::Error>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{PermitType, Scheduler};
    use crate::{
        runtime::scheduler::{NetworkPermitContext, TransferDirection},
        types::BucketType,
        types::ConcurrencyMode,
    };

    #[tokio::test]
    async fn test_acquire_mode_explicit() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(1));
        let network_permit_context = NetworkPermitContext {
            payload_size_estimate: 0,
            bucket_type: BucketType::Standard,
            direction: TransferDirection::Download,
        };
        let p1 = scheduler
            .acquire_permit(PermitType::Network(network_permit_context.clone()))
            .await
            .unwrap();
        let scheduler2 = scheduler.clone();
        let jh = tokio::spawn(async move {
            let _p2 = scheduler2
                .acquire_permit(PermitType::Network(network_permit_context))
                .await;
        });
        assert!(!jh.is_finished());
        drop(p1);
        jh.await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_metrics() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(2));
        let permit_context = NetworkPermitContext {
            payload_size_estimate: 1024,
            bucket_type: BucketType::Standard,
            direction: TransferDirection::Download,
        };

        let _permit1 = scheduler
            .acquire_permit(PermitType::Network(permit_context.clone()))
            .await
            .unwrap();

        assert!(scheduler.metrics.permit_wait_time().count() > 0);

        // We never actually fire off a request so no failures or inflight
        assert_eq!(scheduler.metrics.permit_acquisition_failures().value(), 0);
        assert_eq!(scheduler.metrics.max_inflight().value(), 0);
    }

    #[tokio::test]
    async fn test_token_bucket_metrics() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(10));
        let permit_context = NetworkPermitContext {
            payload_size_estimate: 1024,
            bucket_type: BucketType::Standard,
            direction: TransferDirection::Download,
        };

        // Initial token metrics
        assert_eq!(
            scheduler.token_bucket.metrics().available_tokens().value(),
            10
        );
        assert_eq!(scheduler.max_tokens(), 10);

        let permit = scheduler
            .acquire_permit(PermitType::Network(permit_context))
            .await
            .unwrap();

        // Token acquisition should be recorded
        assert_eq!(
            scheduler.token_bucket.metrics().available_tokens().value(),
            9
        );

        drop(permit);

        // Token drop should be recorded
        assert_eq!(
            scheduler.token_bucket.metrics().available_tokens().value(),
            10
        );
    }
}
