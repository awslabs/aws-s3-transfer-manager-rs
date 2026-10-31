/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A scenario's connection to the mock S3 server, and a record of the
//! requests its transfer-manager clients send.

use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3_transfer_manager::config::Builder as ConfigBuilder;
use aws_sdk_s3_transfer_manager::types::PartSize;
use aws_sdk_s3_transfer_manager::{Client, Config};
use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_runtime_api::client::http::{
    http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_types::sdk_config::{RetryConfig, SharedAsyncSleep, SharedCredentialsProvider};
use aws_types::SdkConfig;
use bytes::Bytes;

use crate::http::TcpHttp;

/// The part size of every client.
const PART_SIZE: u64 = 8 * 1024 * 1024;

/// The credentials that `s3-mock-server` accepts.
const SERVER_ACCESS_KEY: &str = "mock-akid";
const SERVER_SECRET_KEY: &str = "mock-secret";

/// Request counters, by the S3 operation each request's method and query
/// select.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Requests {
    /// `PutObject` requests.
    pub(crate) put_object: usize,
    /// `UploadPart` requests.
    pub(crate) upload_part: usize,
    /// `CreateMultipartUpload`, `CompleteMultipartUpload`, and
    /// `AbortMultipartUpload` requests.
    pub(crate) multipart_control: usize,
    /// `GetObject` requests.
    pub(crate) get_object: usize,
    /// `ListObjectsV2` requests.
    pub(crate) list_objects: usize,
    /// Requests for any other operation.
    pub(crate) other: usize,
    /// Requests of any operation answered with `503`.
    pub(crate) throttled: usize,
}

/// Lists the nonzero counters, such as `3 UploadPart, 2 multipart control`,
/// or `no requests`.
impl fmt::Display for Requests {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let counters = [
            (self.put_object, "PutObject"),
            (self.upload_part, "UploadPart"),
            (self.multipart_control, "multipart control"),
            (self.get_object, "GetObject"),
            (self.list_objects, "ListObjectsV2"),
            (self.other, "other"),
            (self.throttled, "throttled"),
        ];
        let mut nonzero = counters.iter().filter(|(count, _)| *count != 0).peekable();
        if nonzero.peek().is_none() {
            return f.write_str("no requests");
        }
        for (index, (count, name)) in nonzero.enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{count} {name}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Log {
    requests: Requests,
    /// `GetObject` range starts in request order.
    get_ranges: Vec<Option<u64>>,
}

/// Records each request, then sends it with `inner`. Clones share the record.
#[derive(Clone, Debug)]
struct Recorder {
    inner: TcpHttp,
    log: Arc<Mutex<Log>>,
}

impl Recorder {
    fn record(&self, request: &HttpRequest) {
        let query = request.uri().split_once('?').map_or("", |(_, query)| query);
        let param = |name: &str| {
            query
                .split('&')
                .map(|pair| pair.split_once('=').map_or(pair, |(key, _)| key))
                .any(|key| key == name)
        };
        let mut log = self.log.lock().unwrap();
        let log = &mut *log;
        let requests = &mut log.requests;
        match request.method() {
            "PUT" if param("partNumber") => requests.upload_part += 1,
            "PUT" => requests.put_object += 1,
            "POST" | "DELETE" if param("uploads") || param("uploadId") => {
                requests.multipart_control += 1
            }
            "GET" if query.split('&').any(|pair| pair == "list-type=2") => {
                requests.list_objects += 1
            }
            "GET" => {
                requests.get_object += 1;
                let start = request
                    .headers()
                    .get("range")
                    .and_then(|range| range.strip_prefix("bytes="))
                    .and_then(|range| range.split_once('-'))
                    .and_then(|(start, _)| start.parse().ok());
                log.get_ranges.push(start);
            }
            _ => requests.other += 1,
        }
    }
}

impl HttpConnector for Recorder {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        self.record(&request);
        let log = Arc::clone(&self.log);
        let response = self.inner.call(request);
        HttpConnectorFuture::new(async move {
            let response = response.await?;
            if response.status().as_u16() == 503 {
                log.lock().unwrap().requests.throttled += 1;
            }
            Ok(response)
        })
    }
}

/// A scenario's connection to the mock S3 server, over a new connection pool.
///
/// The store records the requests that its transfer-manager clients send, but
/// not those of [`insert`](Self::insert) and [`object`](Self::object). The
/// server keeps objects across stores. Objects live in the bucket `bucket`.
pub(crate) struct Store {
    recorder: Recorder,
    /// Configuration for transfer-manager clients, which send requests through
    /// the recorder.
    sdk: SdkConfig,
    /// A client that bypasses the recorder.
    s3: aws_sdk_s3::Client,
}

impl Store {
    /// Returns a store with an empty record for the mock S3 server at
    /// `server`.
    pub(crate) fn new(server: SocketAddr) -> Self {
        let http = TcpHttp::new(server);
        let recorder = Recorder {
            inner: http.clone(),
            log: Arc::default(),
        };
        let sdk = sdk_config(server, SharedHttpConnector::new(recorder.clone()));
        let s3 = aws_sdk_s3::Client::new(&sdk_config(server, SharedHttpConnector::new(http)));
        Self { recorder, sdk, s3 }
    }

    /// Returns a transfer-manager client with 8 MiB target parts, after
    /// `configure` adjusts its configuration.
    pub(crate) fn client(&self, configure: impl FnOnce(ConfigBuilder) -> ConfigBuilder) -> Client {
        let config = Config::builder()
            .s3_config(&self.sdk)
            .part_size(PartSize::Target(PART_SIZE));
        Client::new(configure(config).build())
    }

    /// Returns the request counters.
    pub(crate) fn requests(&self) -> Requests {
        self.recorder.log.lock().unwrap().requests
    }

    /// Returns the range start of every `GetObject` request in order.
    pub(crate) fn get_ranges(&self) -> Vec<Option<u64>> {
        self.recorder.log.lock().unwrap().get_ranges.clone()
    }

    /// Stores `data` under `key` with a `PutObject` request.
    pub(crate) async fn insert(&self, key: &str, data: Bytes) -> Result<(), String> {
        self.s3
            .put_object()
            .bucket("bucket")
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| format!("PutObject {key}: {}", DisplayErrorContext(e)))?;
        Ok(())
    }

    /// Returns the object stored under `key` with a `GetObject` request.
    pub(crate) async fn object(&self, key: &str) -> Result<Bytes, String> {
        let output = self
            .s3
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

/// Lists the recorded requests and the connections the store opened, such as
/// `3 UploadPart, 2 multipart control over 3 connections`, or `no requests`.
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let connections = self.recorder.inner.connections();
        if connections == 0 {
            return write!(f, "{}", self.requests());
        }
        let plural = if connections == 1 { "" } else { "s" };
        write!(
            f,
            "{} over {connections} connection{plural}",
            self.requests()
        )
    }
}

fn sdk_config(server: SocketAddr, connector: SharedHttpConnector) -> SdkConfig {
    let credentials = Credentials::new(SERVER_ACCESS_KEY, SERVER_SECRET_KEY, None, None, "harness");
    SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        // S3 addresses buckets path-style on an IP endpoint.
        .endpoint_url(format!("http://{server}"))
        .http_client(http_client_fn(move |_, _| connector.clone()))
        .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
        // The SDK makes one attempt, so every retry observed here is the
        // transfer manager's.
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build()
}
