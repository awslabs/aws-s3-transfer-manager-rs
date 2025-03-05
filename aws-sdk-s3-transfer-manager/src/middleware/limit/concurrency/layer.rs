/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::runtime::scheduler::Scheduler;
use tower::Layer;

use super::service::ConcurrencyLimit;

/// Enforces a limit on the concurrent number of requests the underlying
/// service can handle.
#[derive(Debug, Clone)]
pub(crate) struct ConcurrencyLimitLayer {
    scheduler: Scheduler,
}

impl ConcurrencyLimitLayer {
    /// Create a new concurrency limit layer.
    pub(crate) const fn new(scheduler: Scheduler) -> Self {
        ConcurrencyLimitLayer { scheduler }
    }
}

impl<S> Layer<S> for ConcurrencyLimitLayer {
    type Service = ConcurrencyLimit<S>;

    fn layer(&self, service: S) -> Self::Service {
        ConcurrencyLimit::new(service, self.scheduler.clone())
    }
}
