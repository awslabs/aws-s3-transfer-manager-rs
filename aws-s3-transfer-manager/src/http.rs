use std::time::Duration;

use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use hyper::client::{Builder, HttpConnector};
use hyper_rustls::{ConfigBuilderExt, HttpsConnector};
use once_cell::sync::Lazy;

pub(crate) mod header;

static HTTPS_NATIVE_ROOTS: Lazy<HttpsConnector<HttpConnector>> = Lazy::new(|| {
    let mut http = HttpConnector::new();
    // HttpConnector won't enforce scheme, but HttpsConnector will
    http.enforce_http(false);
    // Set SO_NODELAY, which we have found significantly improves Lambda invocation latency
    http.set_nodelay(true);
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(
            rustls::ClientConfig::builder()
                .with_cipher_suites(&[
                    // TLS1.3 suites
                    rustls::cipher_suite::TLS13_AES_256_GCM_SHA384,
                    rustls::cipher_suite::TLS13_AES_128_GCM_SHA256,
                    // TLS1.2 suites
                    rustls::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
                    rustls::cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
                    rustls::cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                    rustls::cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                    rustls::cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
                ])
                .with_safe_default_kx_groups()
                .with_safe_default_protocol_versions()
                .expect("Error with the TLS configuration. Please file a bug report under https://github.com/restatedev/restate/issues.")
                .with_native_roots()
                .with_no_client_auth()
        )
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .wrap_connector(http)
});
/// The default HTTP client used by a transfer manager when not explicitly configured.
pub(crate) fn default_client() -> SharedHttpClient {
    let test_nodelay = 123;
    let mut hyper_builder = Builder::default();
    //hyper_builder.http1_read_buf_exact_size(400 * 1024);
    //hyper_builder.pool_idle_timeout(Duration::from_secs(3));
    HyperClientBuilder::new()
        .hyper_builder(hyper_builder)
        .build(HTTPS_NATIVE_ROOTS.clone())
}
