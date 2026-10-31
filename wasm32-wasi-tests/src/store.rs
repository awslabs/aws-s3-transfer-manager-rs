/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The S3 backends that scenarios run against.

use std::fmt;
use std::net::SocketAddr;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3_transfer_manager::config::Builder as ConfigBuilder;
use aws_sdk_s3_transfer_manager::types::PartSize;
use aws_sdk_s3_transfer_manager::{Client, Config};
use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_runtime_api::client::http::{http_client_fn, SharedHttpConnector};
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_types::sdk_config::{RetryConfig, SharedAsyncSleep, SharedCredentialsProvider};
use aws_types::SdkConfig;
use bytes::Bytes;
use in_memory_s3::MemoryS3;

use crate::http::TcpHttp;

/// The part size of every client.
const PART_SIZE: u64 = 8 * 1024 * 1024;

/// The credentials that `s3-mock-server` accepts.
const SERVER_ACCESS_KEY: &str = "mock-akid";
const SERVER_SECRET_KEY: &str = "mock-secret";

/// The transports that scenarios send requests over.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Transport {
    /// The in-memory S3 connector.
    Memory,
    /// HTTP to the mock S3 server.
    Http,
}

/// Prints `in-memory` or `HTTP`.
impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Transport::Memory => "in-memory",
            Transport::Http => "HTTP",
        })
    }
}

enum Backend {
    Memory(MemoryS3),
    Http(TcpHttp),
}

/// A scenario's S3 backend, and the configuration of clients that use it.
///
/// A store for [`Transport::Memory`] is a new, empty in-memory connector that
/// records the requests it serves. A store for [`Transport::Http`] reaches the
/// mock S3 server over a new connection pool; the server keeps objects across
/// stores. Objects live in the bucket `bucket`.
pub(crate) struct Store {
    backend: Backend,
    sdk: SdkConfig,
}

impl Store {
    /// Returns a store for `transport`. `server` is the mock S3 server's
    /// address.
    pub(crate) fn new(transport: Transport, server: SocketAddr) -> Self {
        let (backend, connector, endpoint, credentials) = match transport {
            Transport::Memory => {
                let store = MemoryS3::default();
                let connector = SharedHttpConnector::new(store.clone());
                let credentials = Credentials::new("akid", "secret", None, None, "harness");
                let endpoint = "http://s3.harness.invalid".to_owned();
                (Backend::Memory(store), connector, endpoint, credentials)
            }
            Transport::Http => {
                let http = TcpHttp::new(server);
                let connector = SharedHttpConnector::new(http.clone());
                let credentials =
                    Credentials::new(SERVER_ACCESS_KEY, SERVER_SECRET_KEY, None, None, "harness");
                // S3 addresses buckets path-style on an IP endpoint.
                let endpoint = format!("http://{server}");
                (Backend::Http(http), connector, endpoint, credentials)
            }
        };
        let sdk = SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .endpoint_url(endpoint)
            .http_client(http_client_fn(move |_, _| connector.clone()))
            .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
            // The SDK makes one attempt, so every retry observed here is the
            // transfer manager's.
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .build();
        Self { backend, sdk }
    }

    /// Returns a transfer-manager client with 8 MiB target parts, after
    /// `configure` adjusts its configuration.
    pub(crate) fn client(&self, configure: impl FnOnce(ConfigBuilder) -> ConfigBuilder) -> Client {
        let config = Config::builder()
            .s3_config(&self.sdk)
            .part_size(PartSize::Target(PART_SIZE));
        Client::new(configure(config).build())
    }

    /// Returns the in-memory connector, or `None` for an HTTP store.
    pub(crate) fn memory(&self) -> Option<&MemoryS3> {
        match &self.backend {
            Backend::Memory(store) => Some(store),
            Backend::Http(_) => None,
        }
    }

    /// Stores `data` under `key`. An HTTP store sends a `PutObject` request.
    pub(crate) async fn insert(&self, key: &str, data: Bytes) -> Result<(), String> {
        match &self.backend {
            Backend::Memory(store) => store.insert(key, data),
            Backend::Http(_) => {
                self.s3()
                    .put_object()
                    .bucket("bucket")
                    .key(key)
                    .body(ByteStream::from(data))
                    .send()
                    .await
                    .map_err(|e| format!("PutObject {key}: {}", DisplayErrorContext(e)))?;
            }
        }
        Ok(())
    }

    /// Returns the object stored under `key`. An HTTP store sends a
    /// `GetObject` request.
    pub(crate) async fn object(&self, key: &str) -> Result<Bytes, String> {
        match &self.backend {
            Backend::Memory(store) => store.object(key).ok_or_else(|| format!("no object {key}")),
            Backend::Http(_) => {
                let output = self
                    .s3()
                    .get_object()
                    .bucket("bucket")
                    .key(key)
                    .send()
                    .await
                    .map_err(|e| format!("GetObject {key}: {}", DisplayErrorContext(e)))?;
                let body = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| format!("GetObject {key} body: {}", DisplayErrorContext(e)))?;
                Ok(body.into_bytes())
            }
        }
    }

    fn s3(&self) -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::new(&self.sdk)
    }
}

/// Lists the requests that clients of the store sent, such as `3 UploadPart, 2
/// multipart control` for an in-memory store or `7 HTTP requests over 2
/// connections` for an HTTP store.
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let plural = |count: usize| if count == 1 { "" } else { "s" };
        match &self.backend {
            Backend::Memory(store) => write!(f, "{}", store.requests()),
            Backend::Http(http) => {
                let (requests, connections) = (http.requests(), http.connections());
                write!(
                    f,
                    "{requests} HTTP request{} over {connections} connection{}",
                    plural(requests),
                    plural(connections)
                )
            }
        }
    }
}
