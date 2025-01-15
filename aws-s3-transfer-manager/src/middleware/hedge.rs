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

/// S3 recommends retrying the slowest 5% of the requests.
const LATENCY_PERCENTILE: f32 = 95.0;
/// This value was chosen randomly, but seems to work.
const MIN_DATA_POINTS: u64 = 20;
/// The Hedge layer maintains two rotating histograms, i.e., ReadHistogram and WriteHistogram. They
/// are switched every period. This value was chosen randomly but with consideration given to the fact that most 8-16 MB part
/// requests take on average 0.2 seconds, and we should retry it if it takes more than a second.
const PERIOD: Duration = Duration::from_secs(2);

/*
 * During uploads, S3 recommends retrying the slowest 5% of requests for latency-sensitive applications,
 * as some requests can experience high time to first byte. If a slow part is hit near the end of the request,
 * the application may spend the last few seconds waiting for those final parts to complete, which can reduce overall
 * throughput. This layer is used to retry the slowest 5% of requests to improve performance.
 * Based on our experiments, this makes a significant difference for multipart upload use-cases and
 * does not have a noticeable impact for the Download.
 */
pub(crate) struct Builder<P> {
    policy: P,
    latency_percentile: f32,
    min_data_points: u64,
    period: Duration,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DefaultPolicy;

impl Default for Builder<DefaultPolicy> {
    fn default() -> Self {
        Self {
            policy: DefaultPolicy,
            latency_percentile: LATENCY_PERCENTILE,
            min_data_points: MIN_DATA_POINTS,
            period: PERIOD,
        }
    }
}

impl<P> Builder<P> {
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
