/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::error;

use futures_util::TryFutureExt;
use pin_project_lite::pin_project;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// Manages scheduling networking and I/O work
///
/// Scheduler is internally reference-counted and can be freely cloned.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    sem: Arc<Semaphore>,
}

impl Scheduler {
    /// Create a new scheduler with the initial number of work permits.
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            // NOTE: tokio semahpore is fair, permits are given out in the order requested
            sem: Arc::new(Semaphore::new(permits)),
        }
    }

    // TODO - add some notion of "work type" and/or "work estimate" to permit acquisition to allow
    // for scheduler to make choices on what work gets prioritized

    /// Acquire a permit to perform some unit of work
    pub(crate) fn acquire_permit(&self) -> AcquirePermitFuture {
        match self.try_acquire_permit() {
            Ok(Some(permit)) => AcquirePermitFuture::ready(Ok(permit)),
            Ok(None) => {
                let inner = self
                    .sem
                    .clone()
                    .acquire_owned()
                    .map_ok(OwnedWorkPermit::from)
                    .map_err(error::from_kind(error::ErrorKind::RuntimeError));

                AcquirePermitFuture::new(inner)
            }
            Err(err) => AcquirePermitFuture::ready(Err(err)),
        }
    }

    /// Try to acquire a permit for some unit of work.
    ///
    /// If there are no permits left, this returns `Ok(None)`. Otherwise, this returns
    /// `Ok(Some(OwnedWorkPermit))`
    pub(crate) fn try_acquire_permit(&self) -> Result<Option<OwnedWorkPermit>, error::Error> {
        match self.sem.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(permit.into())),
            Err(err) => match err {
                TryAcquireError::Closed => {
                    Err(error::Error::new(error::ErrorKind::RuntimeError, err))
                }
                TryAcquireError::NoPermits => Ok(None),
            },
        }
    }
}

/// An owned permit from the scheduler to perform some unit of work.
#[must_use]
#[clippy::has_significant_drop]
#[derive(Debug)]
pub(crate) struct OwnedWorkPermit {
    _inner: OwnedSemaphorePermit,
}

impl From<OwnedSemaphorePermit> for OwnedWorkPermit {
    fn from(value: OwnedSemaphorePermit) -> Self {
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
    use super::Scheduler;

    #[test]
    fn try_acquire() {
        let scheduler = Scheduler::new(1);
        {
            let p1 = scheduler.try_acquire_permit().unwrap();
            assert!(p1.is_some());
            let p2 = scheduler.try_acquire_permit().unwrap();
            assert!(p2.is_none());
        }
        // p1 dropped
        let p3 = scheduler.try_acquire_permit().unwrap();
        assert!(p3.is_some());
    }

    #[tokio::test]
    async fn test_acquire() {
        let scheduler = Scheduler::new(1);
        let p1 = scheduler.acquire_permit().await.unwrap();
        let scheduler2 = scheduler.clone();
        let jh = tokio::spawn(async move {
            let _p2 = scheduler2.acquire_permit().await;
        });
        assert!(!jh.is_finished());
        drop(p1);
        jh.await.unwrap();
    }
}
