/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_smithy_experimental::hyper_1_0::{CryptoMode, HyperClientBuilder};
use aws_smithy_runtime_api::client::http::SharedHttpClient;

pub(crate) mod header;

/// The default HTTP client used by a transfer manager when not explicitly configured.
pub(crate) fn default_client() -> SharedHttpClient {
    HyperClientBuilder::new()
        .crypto_mode(CryptoMode::AwsLc)
        .build_https()
}
