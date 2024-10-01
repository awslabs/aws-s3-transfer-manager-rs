/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::time::Duration;
use tower::{
    hedge::{Hedge, Policy},
    layer::layer_fn,
    BoxError, Layer, Service,
};

pub(crate) struct HedgeBuilder<P> {
    policy: P,
    latency_percentile: f32,
    min_data_points: u64,
    period: Duration,
}

impl<P> HedgeBuilder<P> {
    pub(crate) fn new(policy: P) -> Self {
        Self {
            /*
             * During uploads, S3 recommends retrying the slowest 5% of requests for latency-sensitive applications,
             * as some requests can experience high time to first byte. If a slow part is hit near the end of the request,
             * the application may spend the last few seconds waiting for those final parts to complete, which can reduce overall
             * throughput. This layer is used to retry the slowest 5% of requests to improve performance.
             * Based on our experiments, this makes a significant difference for multipart upload use-cases and
             * does not have a noticeable impact for the Download.
             * not a noticeable impact for Download.
             */
            policy,
            latency_percentile: 95.0,
            min_data_points: 20,
            period: Duration::new(2, 0),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_percentile(mut self, percentile: f32) -> Self {
        self.latency_percentile = percentile;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_min_data_points(mut self, min_data_points: u64) -> Self {
        self.min_data_points = min_data_points;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_duration(mut self, duration: Duration) -> Self {
        self.period = duration;
        self
    }

    /// Converts the `Hedge` into a `Layer` that can be used in a service stack.
    pub(crate) fn into_layer<Request, S>(self) -> impl Layer<S, Service = Hedge<S, P>> + Clone
    where
        P: Policy<Request> + Clone,
        S: Service<Request> + Clone,
        S::Error: Into<BoxError>,
    {
        let policy = self.policy;
        let min_data_points = self.min_data_points;
        let latency_percentile = self.latency_percentile;
        let period = self.period;

        layer_fn(move |service: S| {
            Hedge::new(
                service,
                policy.clone(),
                min_data_points,
                latency_percentile,
                period,
            )
        })
    }
}
