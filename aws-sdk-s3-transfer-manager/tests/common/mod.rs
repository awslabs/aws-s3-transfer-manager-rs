/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3_transfer_manager::config::S3ClientConfig;
use aws_smithy_runtime_api::client::http::HttpClient;

/// Shared configuration with fixed test credentials/region and the supplied
/// HTTP client, without environment-based configuration loading.
pub fn shared_config(http_client: impl HttpClient + 'static) -> aws_types::SdkConfig {
    aws_types::SdkConfig::builder()
        .behavior_version(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::from_static("us-west-2"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::for_tests()))
        .http_client(http_client)
        .build()
}

/// Use the supplied test HTTP client without replacement by managed-runtime
/// transport, regardless of the runtime selected by a test.
pub fn s3_config(http_client: impl HttpClient + 'static) -> S3ClientConfig {
    S3ClientConfig::new(&shared_config(http_client)).enable_runtime_http(false)
}
