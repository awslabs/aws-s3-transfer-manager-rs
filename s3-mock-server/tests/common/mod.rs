// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Helpers shared by the integration tests that drive the server through the SDK.

// Each test binary uses a different subset of the helpers.
#![allow(dead_code)]

use std::fmt::Debug;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use s3_mock_server::{S3MockServer, S3MockServerBuilder, ServerHandle};
use uuid::Uuid;

pub type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A running in-memory server with one bucket, and an SDK client that makes a single attempt
/// per request, so request counts and fault schedules are exact.
pub struct Fixture {
    pub server: S3MockServer,
    pub handle: ServerHandle,
    pub s3: Client,
    pub bucket: String,
}

impl Fixture {
    pub async fn new() -> Self {
        Self::with(S3MockServer::builder()).await
    }

    pub async fn with(builder: S3MockServerBuilder) -> Self {
        let server = builder
            .with_in_memory_store()
            .build()
            .expect("build server");
        let handle = server.start().await.expect("start server");
        let s3 = no_retry(&handle.client().await);
        let bucket = format!("test-bucket-{}", Uuid::new_v4());
        server.create_bucket(&bucket).await.expect("create bucket");
        Self {
            server,
            handle,
            s3,
            bucket,
        }
    }

    /// Put `content` at `key` and return the quoted ETag.
    pub async fn put(&self, key: &str, content: &'static str) -> String {
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .expect("put object")
            .e_tag
            .expect("etag")
    }

    /// The stored content of `key`, read directly from storage.
    pub async fn content(&self, key: &str) -> Vec<u8> {
        self.server
            .get_object(&self.bucket, key)
            .await
            .expect("get object")
            .unwrap_or_else(|| panic!("{key} should exist"))
            .body
            .to_vec()
    }

    pub async fn exists(&self, key: &str) -> bool {
        self.server
            .object_exists(&self.bucket, key)
            .await
            .expect("object exists")
    }

    pub async fn shutdown(self) {
        self.handle.shutdown().await.expect("shutdown");
    }
}

/// A copy of `client` that does not retry.
pub fn no_retry(client: &Client) -> Client {
    Client::from_conf(
        client
            .config()
            .to_builder()
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .build(),
    )
}

/// HTTP status, S3 error code and message of a failed SDK call.
#[derive(Debug)]
pub struct Failure {
    pub status: u16,
    pub code: Option<String>,
    pub message: Option<String>,
}

/// The failure of `result`. Panics if the call succeeded or failed without a response.
#[track_caller]
pub fn failure<T: Debug, E: ProvideErrorMetadata + Debug>(
    result: std::result::Result<T, SdkError<E, HttpResponse>>,
) -> Failure {
    let err = result.expect_err("request should fail");
    let status = err
        .raw_response()
        .map(|r| r.status().as_u16())
        .unwrap_or_else(|| panic!("no response: {err:?}"));
    Failure {
        status,
        code: err.code().map(str::to_owned),
        message: err.message().map(str::to_owned),
    }
}

#[track_caller]
pub fn assert_failure(failure: &Failure, status: u16, code: &str) {
    assert_eq!(failure.status, status, "{failure:?}");
    assert_eq!(failure.code.as_deref(), Some(code), "{failure:?}");
}
