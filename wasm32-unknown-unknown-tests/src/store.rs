/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A scenario's connection to the mock S3 server through the host, and a
//! record of the requests its transfer-manager clients send.

use std::fmt;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3_transfer_manager::config::Builder as ConfigBuilder;
use aws_sdk_s3_transfer_manager::{Client, Config, RuntimeConfig};
use aws_smithy_runtime_api::client::http::{
    http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_types::sdk_config::{RetryConfig, SharedCredentialsProvider};
use aws_types::SdkConfig;
use bytes::Bytes;

use crate::host::{HostRuntime, HostSleep, HostWallClock};
use crate::host_http::HostHttp;

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

/// A request that [`Store::hold`] keeps pending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HoldRule {
    /// HTTP method, such as `PUT`.
    pub(crate) method: &'static str,
    /// First byte of the requested range, or `None` to match any request with
    /// this method.
    pub(crate) range_start: Option<u64>,
}

#[derive(Debug, Default)]
struct Log {
    requests: Requests,
    /// `GetObject` range starts in request order.
    get_ranges: Vec<Option<u64>>,
    /// Requests whose futures are live.
    in_flight: usize,
    max_in_flight: usize,
    hold: Option<HoldRule>,
    held: usize,
    held_dropped: usize,
}

/// Records each request, then sends it with [`HostHttp`] unless it matches
/// the hold rule. Clones share the record.
#[derive(Clone, Debug, Default)]
struct Recorder {
    log: Arc<Mutex<Log>>,
}

impl Recorder {
    /// Records `request`. Returns whether it matches the hold rule, which it
    /// then clears.
    fn record(&self, request: &HttpRequest) -> bool {
        let query = request.uri().split_once('?').map_or("", |(_, query)| query);
        let param = |name: &str| {
            query
                .split('&')
                .map(|pair| pair.split_once('=').map_or(pair, |(key, _)| key))
                .any(|key| key == name)
        };
        let range_start = request
            .headers()
            .get("range")
            .and_then(|range| range.strip_prefix("bytes="))
            .and_then(|range| range.split_once('-'))
            .and_then(|(start, _)| start.parse().ok());
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
                log.get_ranges.push(range_start);
            }
            _ => requests.other += 1,
        }
        log.in_flight += 1;
        log.max_in_flight = log.max_in_flight.max(log.in_flight);
        let held = log.hold.as_ref().is_some_and(|rule| {
            rule.method == request.method()
                && (rule.range_start.is_none() || rule.range_start == range_start)
        });
        if held {
            log.hold = None;
            log.held += 1;
        }
        held
    }
}

/// Counts a request's future as live until it drops.
struct InFlight(Arc<Mutex<Log>>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.lock().unwrap().in_flight -= 1;
    }
}

/// Counts the drop of a held request's future.
struct Held(Arc<Mutex<Log>>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.lock().unwrap().held_dropped += 1;
    }
}

impl HttpConnector for Recorder {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let held = self.record(&request);
        let in_flight = InFlight(Arc::clone(&self.log));
        if held {
            let held = Held(Arc::clone(&self.log));
            return HttpConnectorFuture::new(async move {
                let _in_flight = in_flight;
                let _held = held;
                std::future::pending::<Result<HttpResponse, ConnectorError>>().await
            });
        }
        let log = Arc::clone(&self.log);
        let response = HostHttp.call(request);
        HttpConnectorFuture::new(async move {
            let _in_flight = in_flight;
            let response = response.await?;
            if response.status().as_u16() == 503 {
                log.lock().unwrap().requests.throttled += 1;
            }
            Ok(response)
        })
    }
}

/// A scenario's connection to the mock S3 server at an endpoint, through the
/// host.
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
    /// `endpoint`, such as `http://127.0.0.1:49152`.
    pub(crate) fn new(endpoint: &str) -> Self {
        let recorder = Recorder::default();
        let sdk = sdk_config(endpoint, SharedHttpConnector::new(recorder.clone()), true);
        let s3 = aws_sdk_s3::Client::new(&sdk_config(
            endpoint,
            SharedHttpConnector::new(HostHttp),
            true,
        ));
        Self { recorder, sdk, s3 }
    }

    /// Returns a transfer-manager client on [`HostRuntime`], after `configure`
    /// adjusts its configuration.
    pub(crate) fn client(&self, configure: impl FnOnce(ConfigBuilder) -> ConfigBuilder) -> Client {
        let config = Config::builder()
            .s3_config(&self.sdk)
            .runtime_config(RuntimeConfig::wasm(HostRuntime));
        Client::new(configure(config).build())
    }

    /// Returns the request counters.
    pub(crate) fn requests(&self) -> Requests {
        self.log().requests
    }

    /// Returns the range start of every `GetObject` request in order.
    pub(crate) fn get_ranges(&self) -> Vec<Option<u64>> {
        self.log().get_ranges.clone()
    }

    /// Returns the most requests whose futures were live at once.
    pub(crate) fn max_in_flight(&self) -> usize {
        self.log().max_in_flight
    }

    /// Holds the first later request matching `rule` until its future is
    /// dropped. The request never reaches the host.
    pub(crate) fn hold(&self, rule: HoldRule) {
        self.log().hold = Some(rule);
    }

    /// Returns the number of requests held.
    pub(crate) fn held(&self) -> usize {
        self.log().held
    }

    /// Returns the number of held requests whose futures have been dropped.
    pub(crate) fn held_dropped(&self) -> usize {
        self.log().held_dropped
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

    /// Returns the object stored under `key` with a `GetObject` request, or
    /// `None` if the server answers `NoSuchKey`.
    pub(crate) async fn object(&self, key: &str) -> Result<Option<Bytes>, String> {
        let output = match self.s3.get_object().bucket("bucket").key(key).send().await {
            Ok(output) => output,
            Err(e) if e.as_service_error().is_some_and(|e| e.is_no_such_key()) => return Ok(None),
            Err(e) => return Err(format!("GetObject {key}: {}", DisplayErrorContext(e))),
        };
        let body = output
            .body
            .collect()
            .await
            .map_err(|e| format!("GetObject {key} body: {}", DisplayErrorContext(e)))?;
        Ok(Some(body.into_bytes()))
    }

    fn log(&self) -> std::sync::MutexGuard<'_, Log> {
        self.recorder.log.lock().unwrap()
    }
}

/// Lists the recorded requests, such as `3 UploadPart, 2 multipart control`,
/// or `no requests`.
impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.requests())
    }
}

/// Builds shared configuration for the server at `endpoint` over `connector`,
/// with every capability the target requires, omitting the sleep
/// implementation when `with_sleep` is false.
pub(crate) fn sdk_config(
    endpoint: &str,
    connector: SharedHttpConnector,
    with_sleep: bool,
) -> SdkConfig {
    let credentials = Credentials::new(SERVER_ACCESS_KEY, SERVER_SECRET_KEY, None, None, "harness");
    let builder = SdkConfig::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        // S3 addresses buckets path-style on an IP endpoint.
        .endpoint_url(endpoint)
        .http_client(http_client_fn(move |_, _| connector.clone()))
        .time_source(HostWallClock)
        // The SDK makes one attempt, so every retry observed here is the
        // transfer manager's.
        .retry_config(RetryConfig::standard().with_max_attempts(1));
    let builder = if with_sleep {
        builder.sleep_impl(HostSleep)
    } else {
        builder
    };
    builder.build()
}
