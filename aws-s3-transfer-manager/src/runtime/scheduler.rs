/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use futures_util::TryFutureExt;
use pin_project_lite::pin_project;
use std::future::Future;

use crate::error;
use crate::runtime::token_bucket::{OwnedToken, TokenBucket};
use crate::types::ConcurrencyMode;

// TODO - track high water mark
// TODO - add statistics/telemetry

/// Manages scheduling networking and I/O work
///
/// Scheduler is internally reference-counted and can be freely cloned.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    token_bucket: TokenBucket,
}

impl Scheduler {
    /// Create a new scheduler with the initial number of work permits.
    pub(crate) fn new(mode: ConcurrencyMode) -> Self {
        Self {
            token_bucket: TokenBucket::new(mode),
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

pub(crate) type PayloadEstimate = u64;

// TODO - when we support configuring throughput indepently we'll need to distinguish the permit
// type and track it separately in the token bucket(s).

/// The type of work to be done
#[derive(Debug, Clone)]
pub(crate) enum PermitType {
    /// A network request to transmit or receive to or from an API with the given payload size estimate
    Network(PayloadEstimate),
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
    use crate::types::ConcurrencyMode;

    #[tokio::test]
    async fn test_acquire_mode_explicit() {
        let scheduler = Scheduler::new(ConcurrencyMode::Explicit(1));
        let p1 = scheduler
            .acquire_permit(PermitType::Network(0))
            .await
            .unwrap();
        let scheduler2 = scheduler.clone();
        let jh = tokio::spawn(async move {
            let _p2 = scheduler2.acquire_permit(PermitType::Network(0)).await;
        });
        assert!(!jh.is_finished());
        drop(p1);
        jh.await.unwrap();
    }
}
