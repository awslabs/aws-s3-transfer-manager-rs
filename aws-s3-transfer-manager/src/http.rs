use std::time::Duration;

use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use hyper::client::Builder;

pub(crate) mod header;

/// The default HTTP client used by a transfer manager when not explicitly configured.
pub(crate) fn default_client() -> SharedHttpClient {
    let test = 123;
    let mut hyper_builder = Builder::default();
    hyper_builder.http1_read_buf_exact_size(400 * 1024);
    hyper_builder.pool_idle_timeout(Duration::from_secs(3));
    HyperClientBuilder::new()
        .hyper_builder(hyper_builder)
        .build_https()
}
