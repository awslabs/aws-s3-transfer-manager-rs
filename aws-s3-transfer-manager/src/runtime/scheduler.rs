/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use futures_util::TryFutureExt;
use pin_project_lite::pin_project;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::error;
use crate::operation::BucketType;
use crate::runtime::token_bucket::{OwnedToken, TokenBucket};
use crate::types::ConcurrencyMode;

// TODO - measure actual throughput and track high water mark

/// Manages scheduling networking and I/O work
///
/// Scheduler is internally reference-counted and can be freely cloned.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    token_bucket: TokenBucket,
    pub(crate) metrics: SchedulerMetrics,
}

impl Scheduler {
    /// Create a new scheduler with the initial number of work permits.
    pub(crate) fn new(mode: ConcurrencyMode) -> Self {
        Self {
            token_bucket: TokenBucket::new(mode),
            metrics: SchedulerMetrics::default(),
        }
    }

    /// Acquire a permit to perform some unit of work
    pub(crate) fn acquire_permit(&self, ptype: PermitType) -> AcquirePermitFuture {
        match self.try_acquire_permit(ptype.clone()) {
            Ok(Some(permit)) => AcquirePermitFuture::ready(Ok(permit)),
            Ok(None) => {
                let inner = self
                    .token_bucket
                    .acquire(ptype)
                    .map_ok(OwnedWorkPermit::from);
                AcquirePermitFuture::new(inner)
            }
            Err(err) => AcquirePermitFuture::ready(Err(err)),
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TransferDirection {
    Upload,
    Download,
}

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
    /// A network request to transmit or receive to or from an API with the given payload size estimate
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

#[derive(Debug, Clone, Default)]
pub(crate) struct SchedulerMetrics {
    // current number of in-flight requests
    inflight: Arc<AtomicUsize>,
}

impl SchedulerMetrics {
    /// Increment the number of in-flight requests and returns the number currently in-flight after
    /// incrementing.
    pub(crate) fn increment_inflight(&self) -> usize {
        self.inflight.fetch_add(1, Ordering::SeqCst) + 1
    }
    /// Decrement the number of in-flight requests and returns the number currently in-flight after
    /// decrementing.
    pub(crate) fn decrement_inflight(&self) -> usize {
        self.inflight.fetch_sub(1, Ordering::SeqCst) - 1
    }

    /// Get the current number of in-flight requests
    #[cfg(test)]
    pub(crate) fn inflight(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::{PermitType, Scheduler};
    use crate::{
        operation::BucketType,
        runtime::scheduler::{NetworkPermitContext, TransferDirection},
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
}
