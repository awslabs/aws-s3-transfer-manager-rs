/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

mod common;
#[cfg(any(unix, windows))]
#[path = "download_test/test_util.rs"]
mod test_util;

#[cfg(unix)]
use test_util::await_shutdown;
#[cfg(any(unix, windows))]
use test_util::{
    assert_download_releases_destination, drop_and_await_shutdown, Destination, Ending,
    OpenFileProbe,
};

use aws_sdk_s3_transfer_manager::RuntimeConfig;
use aws_sdk_s3_transfer_manager::{
    error::{BoxError, ErrorKind},
    metrics::unit::ByteUnit,
    types::{
        ChecksumAlgorithm, ChecksumMode, ChecksumValidation, ConcurrencyMode, NotValidatedReason,
        PartSize, TransferStatus,
    },
};
use aws_smithy_runtime_api::client::http::{
    http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use pin_project_lite::pin_project;
use std::{
    cmp,
    iter::{self, repeat_with},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
    time::Duration,
};
use tokio::sync::Semaphore;

use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;
use test_common::drain;

// NOTE: these tests are somewhat brittle as they assume particular paths through the codebase.
// As an example we generally assume object discovery goes through `GetObject` with a ranged get
// for the first part.

fn rand_data(size: usize) -> Bytes {
    iter::repeat_with(fastrand::alphanumeric)
        .take(size)
        .map(|x| x as u8)
        .collect::<Vec<_>>()
        .into()
}

/// create a dummy placeholder request for StaticReplayClient. This is used when we don't
/// want to use `assert_requests()` and make our own assertions about the actually captured
/// requests. Useful when you don't want to mock up the entire http request that is expected.
fn dummy_expected_request() -> http::Request<SdkBody> {
    http::Request::builder()
        .uri("https://not-used")
        .body(SdkBody::from(&b""[..]))
        .unwrap()
}

/// Create a static replay client (http connector) for an object of the given size.
///
/// Assumptions:
///     1. Expected requests are not created. A dummy placeholder is used. Callers need to make
///        assertions directly on the captured requests.
///     2. Object discovery goes through ranged get which will fetch the first part.
///     3. Concurrency of 1 is used since responses for a static replay client are just returned in
///        the order given.
fn simple_object_connector(data: &Bytes, part_size: usize) -> StaticReplayClient {
    let events = data
        .chunks(part_size)
        .enumerate()
        .map(|(idx, chunk)| {
            let start = idx * part_size;
            let end = std::cmp::min(start + part_size, data.len()) - 1;
            ReplayEvent::new(
                // NOTE: Rather than try to recreate all the expected requests we just put in placeholders and
                // make our own assertions against the captured requests.
                dummy_expected_request(),
                http::Response::builder()
                    .status(200)
                    .header("Content-Length", format!("{}", end - start + 1))
                    .header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", data.len()),
                    )
                    .header("ETag", "my-etag")
                    .body(SdkBody::from(chunk))
                    .unwrap(),
            )
        })
        .collect();

    StaticReplayClient::new(events)
}

fn simple_test_tm(
    data: &Bytes,
    part_size: usize,
) -> (aws_sdk_s3_transfer_manager::Client, StaticReplayClient) {
    let http_client = simple_object_connector(data, part_size);
    let tm = test_tm(http_client.clone(), part_size);
    (tm, http_client)
}

fn test_tm(
    http_client: StaticReplayClient,
    part_size: usize,
) -> aws_sdk_s3_transfer_manager::Client {
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .s3_config(common::s3_config(http_client))
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencyMode::Explicit(1))
        .build();

    aws_sdk_s3_transfer_manager::Client::new(config)
}

/// Test the object ranges are expected and we get all the data
#[tokio::test]
async fn test_download_ranges() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    let (tm, http_client) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let body = drain(&mut handle).await.unwrap();

    assert_eq!(data.len(), body.len());
    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert_eq!(3, requests.len());

    assert_eq!(requests[0].headers().get("Range"), Some("bytes=0-5242879"));
    assert_eq!(
        requests[1].headers().get("Range"),
        Some("bytes=5242880-10485759")
    );
    assert_eq!(
        requests[2].headers().get("Range"),
        Some("bytes=10485760-12582911")
    );
}

/// Test body not consumed which should not prevent the handle from being dropped
#[tokio::test]
async fn test_body_not_consumed() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    let (tm, _) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let _ = handle.body_mut().next().await;
}

/// Connector answering the discovery GET with S3's `NoSuchKey` error, so
/// discovery fails immediately.
fn no_such_key_connector() -> StaticReplayClient {
    StaticReplayClient::new(vec![ReplayEvent::new(
        dummy_expected_request(),
        http::Response::builder()
            .status(404)
            .header("Content-Type", "application/xml")
            .body(SdkBody::from(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>test-object</Key><RequestId>test-request-id</RequestId></Error>"#,
            ))
            .unwrap(),
    )])
}

/// `object_meta()` called after discovery has already failed must return
/// `ObjectNotDiscoverable` rather than wait for a discovery notification that
/// was delivered before the caller started waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_object_meta_after_discovery_failure() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let http_client = no_such_key_connector();
    let tm = test_tm(http_client.clone(), part_size);

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    // Force the ordering where discovery fails before `object_meta()` is called.
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.status() != TransferStatus::Failed {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("discovery did not fail on NoSuchKey");

    let kind = tokio::time::timeout(Duration::from_secs(1), handle.object_meta())
        .await
        .expect("object_meta() did not return after discovery failed")
        .expect_err("object_meta() must fail when discovery failed")
        .kind()
        .clone();
    assert_eq!(ErrorKind::ObjectNotDiscoverable, kind);

    tokio::time::timeout(Duration::from_secs(1), handle.abort())
        .await
        .expect("abort hung after discovery failed");

    assert_eq!(1, http_client.actual_requests().count());
}

/// Connector modelling a ranged-GET object store for an object of `total`
/// bytes.
///
/// The first GET is answered with the requested range. Every later GET stays
/// in flight until its future is dropped; each one adds a permit to
/// `parked_started` when it begins.
#[derive(Debug)]
struct ParkAfterDiscoveryConnector {
    total: usize,
    requests: Arc<AtomicUsize>,
    parked_started: Arc<Semaphore>,
}

impl ParkAfterDiscoveryConnector {
    fn discovery_response(&self, request: &HttpRequest) -> HttpResponse {
        let range = request
            .headers()
            .get("Range")
            .expect("discovery GET must be ranged");
        let (start, end) = range
            .strip_prefix("bytes=")
            .and_then(|r| r.split_once('-'))
            .map(|(s, e)| (s.parse::<usize>().unwrap(), e.parse::<usize>().unwrap()))
            .unwrap_or_else(|| panic!("unexpected Range header: {range}"));
        let end = cmp::min(end, self.total - 1);
        let len = end - start + 1;

        let mut response = HttpResponse::new(
            StatusCode::try_from(206).unwrap(),
            SdkBody::from(rand_data(len)),
        );
        let headers = response.headers_mut();
        headers.insert("Content-Length", len.to_string());
        headers.insert(
            "Content-Range",
            format!("bytes {start}-{end}/{}", self.total),
        );
        headers.insert("ETag", "my-etag");
        response
    }
}

impl HttpConnector for ParkAfterDiscoveryConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        assert_eq!("GET", request.method());
        let seq = self.requests.fetch_add(1, Ordering::SeqCst);
        if seq == 0 {
            let response = self.discovery_response(&request);
            return HttpConnectorFuture::ready(Ok(response));
        }

        let parked_started = self.parked_started.clone();
        HttpConnectorFuture::new(async move {
            parked_started.add_permits(1);
            std::future::pending::<Result<HttpResponse, ConnectorError>>().await
        })
    }
}

/// `abort()` must return once ranged GETs following discovery are executing,
/// without waiting for those requests to complete on their own.
///
/// The body is never consumed.
async fn abort_download_with_executing_gets(runtime_config: RuntimeConfig) {
    let total = 25 * ByteUnit::Mebibyte.as_bytes_usize();
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let requests = Arc::new(AtomicUsize::new(0));
    let parked_started = Arc::new(Semaphore::new(0));
    let connector = SharedHttpConnector::new(ParkAfterDiscoveryConnector {
        total,
        requests: requests.clone(),
        parked_started: parked_started.clone(),
    });
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .s3_config(common::s3_config(http_client_fn(move |_, _| {
            connector.clone()
        })))
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencyMode::Explicit(2))
        .runtime_config(runtime_config)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), parked_started.acquire())
        .await
        .expect("no GET after discovery started")
        .expect("start semaphore closed")
        .forget();

    tokio::time::timeout(Duration::from_secs(1), handle.abort())
        .await
        .expect("abort hung while a ranged GET was executing");

    let requests = requests.load(Ordering::SeqCst);
    assert!(
        requests < total / part_size,
        "expected fewer requests than parts, got {requests}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_abort_download_on_tokio_runtime() {
    abort_download_with_executing_gets(RuntimeConfig::multi_thread_tokio()).await;
}

#[tokio::test]
async fn test_abort_download_on_managed_runtime() {
    abort_download_with_executing_gets(RuntimeConfig::default()).await;
}

pin_project! {
    #[derive(Debug)]
    struct FailingBody {
        data: Bytes,
        fail_after_byte: usize,
        frame_size: usize,
        idx: usize,
    }
}

impl FailingBody {
    fn new(data: Bytes, fail_after: usize, frame_size: usize) -> Self {
        Self {
            data,
            fail_after_byte: fail_after,
            frame_size,
            idx: 0,
        }
    }
}

impl http_body_1x::Body for FailingBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body_1x::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let result = if this.idx >= this.fail_after_byte {
            // fail forevermore
            Err(BoxError::from("simulated body read failure"))
        } else {
            let end = cmp::min(*this.fail_after_byte, *this.idx + *this.frame_size - 1);
            let data = this.data.slice(*this.idx..end);
            *this.idx = end + 1;
            let frame = http_body_1x::Frame::data(data);
            Ok(frame)
        };

        Poll::Ready(Some(result))
    }
}

/// Test chunk/part failure is retried
#[tokio::test]
async fn test_retry_failed_chunk() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 8 * ByteUnit::Mebibyte.as_bytes_usize();
    let frame_size = 16 * ByteUnit::Kibibyte.as_bytes_usize();
    let fail_after_byte = frame_size * 4;

    let http_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", part_size))
                .header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", part_size - 1, data.len()),
                )
                .body(SdkBody::from(data.slice(0..part_size)))
                .unwrap(),
        ),
        // fail the second chunk after reading some of it
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", data.len() - part_size))
                .header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", part_size, data.len() - 1, data.len()),
                )
                .body(SdkBody::from_body_1_x(FailingBody::new(
                    data.slice(part_size..),
                    fail_after_byte,
                    frame_size,
                )))
                .unwrap(),
        ),
        // request for second chunk should be retried
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", data.len() - part_size))
                .header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", part_size, data.len() - 1, data.len()),
                )
                .body(SdkBody::from(data.slice(part_size..)))
                .unwrap(),
        ),
    ]);

    let tm = test_tm(http_client.clone(), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let body = drain(&mut handle).await.unwrap();

    assert_eq!(data.len(), body.len());
    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert_eq!(3, requests.len());
}

const ERROR_RESPONSE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>ExpiredToken</Code>
            <Message>The provided token has expired</Message>
            <RequestId>K2H6N7ZGQT6WHCEG</RequestId>
            <HostId>WWoZlnK4pTjKCYn6eNV7GgOurabfqLkjbSyqTvDMGBaI9uwzyNhSaDhOCPs8paFGye7S6b/AB3A=</HostId>
        </Error>
"#;

/// Test non retryable SdkError
#[tokio::test]
async fn test_non_retryable_error() {
    let data = rand_data(20 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 8 * ByteUnit::Mebibyte.as_bytes_usize();

    let http_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", part_size))
                .header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", part_size - 1, data.len()),
                )
                .body(SdkBody::from(data.slice(0..part_size)))
                .unwrap(),
        ),
        // fail chunk with non-retryable error
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(400)
                .body(SdkBody::from(ERROR_RESPONSE))
                .unwrap(),
        ),
    ]);

    let tm = test_tm(http_client.clone(), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let _ = drain(&mut handle).await.unwrap_err();

    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert_eq!(2, requests.len());
}

/// Test max attempts exhausted reading a stream
#[tokio::test]
async fn test_retry_max_attempts() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 8 * ByteUnit::Mebibyte.as_bytes_usize();
    let frame_size = 16 * 1024;
    let fail_after_byte = frame_size * 4;

    let mut failures = repeat_with(|| {
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", part_size))
                .header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", part_size, data.len() - 1, data.len()),
                )
                .body(SdkBody::from_body_1_x(FailingBody::new(
                    data.slice(part_size..),
                    fail_after_byte,
                    frame_size,
                )))
                .unwrap(),
        )
    })
    .take(3)
    .collect::<Vec<_>>();

    let mut events = vec![ReplayEvent::new(
        dummy_expected_request(),
        http::Response::builder()
            .status(200)
            .header("Content-Length", format!("{}", part_size))
            .header(
                "Content-Range",
                format!("bytes 0-{}/{}", part_size - 1, data.len()),
            )
            .body(SdkBody::from(data.slice(0..part_size)))
            .unwrap(),
    )];

    events.append(&mut failures);

    let http_client = StaticReplayClient::new(events);
    let tm = test_tm(http_client.clone(), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let _ = drain(&mut handle).await.unwrap_err();
    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert_eq!(4, requests.len());
}

/// Test the if_match header was added correctly based on the response from server.
#[tokio::test]
async fn test_download_if_match() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    let (tm, http_client) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    let _ = drain(&mut handle).await.unwrap();

    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert_eq!(3, requests.len());

    // The first request is to discover the object meta data and should not have any if-match
    assert_eq!(requests[0].headers().get("If-Match"), None);
    // All the following requests should have the if-match header
    assert_eq!(requests[1].headers().get("If-Match"), Some("my-etag"));
    assert_eq!(requests[2].headers().get("If-Match"), Some("my-etag"));
}

const OBJECT_MODIFIED_RESPONSE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
    <Error>
        <Code>PreconditionFailed</Code>
        <Message>At least one of the pre-conditions you specified did not hold</Message>
        <Condition>If-Match</Condition>
    </Error>
"#;

/// Test that if the object modified during download.
#[tokio::test]
async fn test_download_object_modified() {
    let _logs = show_test_logs();
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    // Create a static replay client (http connector) to mock the S3 response when object modified during download.
    //
    // Assumptions:
    //     1. First request for discovery, succeed with etag
    //     2. Followed requests fail to mock the object changed during download.
    let events = data
        .chunks(part_size)
        .enumerate()
        .map(|(idx, chunk)| {
            let start = idx * part_size;
            let end = std::cmp::min(start + part_size, data.len()) - 1;
            let mut response = http::Response::builder()
                .status(206)
                .header("Content-Length", format!("{}", end - start + 1))
                .header(
                    "Content-Range",
                    format!("bytes {start}-{end}/{}", data.len()),
                )
                .header("ETag", "my-etag")
                .body(SdkBody::from(chunk))
                .unwrap();
            if idx > 0 {
                response = http::Response::builder()
                    .status(412)
                    .header("Date", "Thu, 12 Jan 2023 00:04:21 GMT")
                    .body(SdkBody::from(OBJECT_MODIFIED_RESPONSE))
                    .unwrap();
            }
            ReplayEvent::new(
                // NOTE: Rather than try to recreate all the expected requests we just put in placeholders and
                // make our own assertions against the captured requests.
                dummy_expected_request(),
                response,
            )
        })
        .collect();

    let http_client = StaticReplayClient::new(events);
    let tm = test_tm(http_client.clone(), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();

    // drain() sees the transfer failed — body returns an error when channel closes
    let _drain_err = drain(&mut handle).await.unwrap_err();
    // drain again to exhaust the body
    let _ = drain(&mut handle).await;
    // join() returns the actual SDK error with full context
    let error = handle.join().await.unwrap_err();
    assert!(
        format!("{:?}", error).contains("PreconditionFailed"),
        "expected PreconditionFailed, got: {:?}",
        error
    );
}

/// Test download via `write_to_path` writes all data and cleans up temp files.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path() {
    let data = rand_data(10 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tm, _http_client) = simple_test_tm(&data, part_size);

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.join().await.unwrap();

    let written = std::fs::read(&dest_path).unwrap();
    assert_eq!(data.as_ref(), written.as_slice());

    // No .s3tmp files should remain
    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().to_string_lossy().contains(".s3tmp"))
        .collect();
    assert!(tmp_files.is_empty(), "leftover temp files: {:?}", tmp_files);
}

macro_rules! download_releases_destination_tests {
    ($($(#[$attr:meta])* $name:ident: $destination:ident, $ending:ident, $runtime:expr;)*) => {$(
        $(#[$attr])*
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            assert_download_releases_destination(
                Destination::$destination,
                Ending::$ending,
                $runtime,
            )
            .await;
        }
    )*};
}

// ref: awslabs/aws-s3-transfer-manager-rs#228, V2429410200
// Windows probes by path, so `write_to_path` endings that remove the
// temporary file are covered on Unix only.
download_releases_destination_tests! {
    #[cfg(any(unix, windows))]
    write_to_path_success_releases_destination: Path, Success, RuntimeConfig::default();
    #[cfg(any(unix, windows))]
    write_to_path_success_on_tokio_runtime_releases_destination: Path, Success, RuntimeConfig::multi_thread_tokio();
    #[cfg(unix)]
    write_to_path_body_failure_releases_temporary_file: Path, BodyFailure, RuntimeConfig::default();
    #[cfg(unix)]
    write_to_path_abort_releases_temporary_file: Path, Abort, RuntimeConfig::default();
    #[cfg(unix)]
    write_to_path_dropped_handle_releases_temporary_file: Path, DropHandle, RuntimeConfig::default();
    #[cfg(any(unix, windows))]
    write_to_file_success_releases_caller_file: File, Success, RuntimeConfig::default();
    #[cfg(any(unix, windows))]
    write_to_file_body_failure_releases_caller_file: File, BodyFailure, RuntimeConfig::default();
    #[cfg(any(unix, windows))]
    write_to_file_abort_releases_caller_file: File, Abort, RuntimeConfig::default();
    #[cfg(any(unix, windows))]
    write_to_file_dropped_handle_releases_caller_file: File, DropHandle, RuntimeConfig::default();
}

// ref: V2429410200
/// Completed `download_objects` children release their destination files
/// through ordinary retirement, with the client still alive.
#[cfg(any(unix, windows))]
#[tokio::test(flavor = "multi_thread")]
async fn download_objects_releases_destination_files() {
    const LIST_ONE_KEY: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>test-bucket</Name>
    <KeyCount>1</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>key1</Key>
        <Size>8</Size>
    </Contents>
</ListBucketResult>"#;
    let data = rand_data(8);
    let list = ReplayEvent::new(
        dummy_expected_request(),
        http::Response::builder()
            .status(200)
            .body(SdkBody::from(LIST_ONE_KEY))
            .unwrap(),
    );
    let get = ReplayEvent::new(
        dummy_expected_request(),
        http::Response::builder()
            .status(200)
            .header("Content-Length", "8")
            .header("Content-Range", "bytes 0-7/8")
            .header("ETag", "my-etag")
            .body(SdkBody::from(data.clone()))
            .unwrap(),
    );
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .s3_config(common::s3_config(StaticReplayClient::new(vec![list, get])))
        .concurrency(ConcurrencyMode::Explicit(1))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let dir = tempfile::tempdir().unwrap();
    let output = tm
        .download_objects()
        .bucket("test-bucket")
        .destination(dir.path())
        .initiate()
        .unwrap()
        .join()
        .await
        .unwrap();
    assert_eq!(output.objects_downloaded(), 1);
    let path = dir.path().join("key1");
    assert_eq!(std::fs::read(&path).unwrap(), data.as_ref());
    OpenFileProbe::new(&path).assert_released().await;
    drop_and_await_shutdown(tm).await;
}

// ref: V2429410200
/// A completed download releases its destination after both clients are
/// dropped and the destination is unlinked.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn write_to_path_drop_clients_then_unlink_releases_destination() {
    let data = Bytes::from_static(b"snapshot");
    let response = ReplayEvent::new(
        dummy_expected_request(),
        http::Response::builder()
            .status(200)
            .header("Content-Length", data.len().to_string())
            .body(SdkBody::from(data.clone()))
            .unwrap(),
    );
    let shared = common::shared_config(StaticReplayClient::new(vec![response]));
    // Reproduce the reported drop sequence with both clients sharing SDK configuration.
    let s3 = aws_sdk_s3::Client::new(&shared);
    let tm = aws_sdk_s3_transfer_manager::Client::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .s3_config(&shared)
            .build(),
    );
    let completion = tm.shutdown_completion();
    let dir = tempfile::tempdir().unwrap();
    let destination = dir.path().join("snapshot.bin");
    tm.download()
        .bucket("bucket")
        .key("key")
        .write_to_path(&destination)
        .await
        .unwrap()
        .join()
        .await
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), data.as_ref());
    let probe = OpenFileProbe::new(&destination);
    drop(tm);
    drop(s3);
    drop(shared);
    std::fs::remove_file(&destination).unwrap();
    assert!(!destination.exists());
    probe.assert_released().await;
    await_shutdown(completion).await;
}

/// A file already at the temporary name `write_to_path` draws is left
/// untouched, and the download writes through another name. Seeding
/// `fastrand` on the polling thread, which draws the name, makes the first
/// name predictable.
#[cfg(any(unix, windows))]
#[tokio::test(flavor = "current_thread")]
async fn test_download_write_to_path_leaves_existing_temp_file() {
    const SEED: u64 = 0x5eed_0042;
    let data = rand_data(10 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tm, _http_client) = simple_test_tm(&data, part_size);

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");
    let first_suffix = fastrand::Rng::with_seed(SEED).u32(..);
    let sentinel_path = dir
        .path()
        .join(format!("output.dat.s3tmp.{first_suffix:08x}"));
    std::fs::write(&sentinel_path, b"SENTINEL").unwrap();

    let download = tm.download().bucket("test-bucket").key("test-object");
    fastrand::seed(SEED);
    let handle = download.write_to_path(&dest_path).await.unwrap();
    handle.join().await.unwrap();

    assert_eq!(std::fs::read(&sentinel_path).unwrap(), b"SENTINEL");
    assert_eq!(std::fs::read(&dest_path).unwrap(), data.as_ref());
    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|path| path.to_string_lossy().contains(".s3tmp"))
        .collect();
    assert_eq!(tmp_files, vec![sentinel_path]);
}

/// Test that aborting a download-to-file cleans up both temp and dest files.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_abort_cleans_up() {
    let data = rand_data(10 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tm, _http_client) = simple_test_tm(&data, part_size);

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("output.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    handle.abort().await;

    assert!(
        !dest_path.exists(),
        "dest file should not exist after abort"
    );

    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().to_string_lossy().contains(".s3tmp"))
        .collect();
    assert!(tmp_files.is_empty(), "leftover temp files: {:?}", tmp_files);
}

/// Test that a mid-transfer error cleans up temp and destination files.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_path_error_cleans_up() {
    let _logs = show_test_logs();
    let data = rand_data(20 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();

    // Discovery succeeds, second request fails with 412 PreconditionFailed.
    let http_client = StaticReplayClient::new(vec![
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", part_size))
                .header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", part_size - 1, data.len()),
                )
                .header("ETag", "my-etag")
                .body(SdkBody::from(data.slice(0..part_size)))
                .unwrap(),
        ),
        ReplayEvent::new(
            dummy_expected_request(),
            http::Response::builder()
                .status(412)
                .body(SdkBody::from(
                    r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>PreconditionFailed</Code></Error>"#,
                ))
                .unwrap(),
        ),
    ]);

    let tm = test_tm(http_client, part_size);

    let dir = tempfile::tempdir().unwrap();
    let dest_path = dir.path().join("should_not_exist.dat");

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .write_to_path(&dest_path)
        .await
        .unwrap();

    let result = handle.join().await;
    assert!(result.is_err(), "expected error from failed transfer");

    assert!(
        !dest_path.exists(),
        "destination file should not exist after error"
    );

    let tmp_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().to_string_lossy().contains(".s3tmp"))
        .collect();
    assert!(tmp_files.is_empty(), "leftover temp files: {:?}", tmp_files);
}

/// `write_to_file` rejects a destination opened in append mode before sending
/// any request, and leaves the file's contents as they were.
#[cfg(any(unix, windows))]
#[tokio::test]
async fn test_download_write_to_file_rejects_append_mode() {
    let data = rand_data(10 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tm, http_client) = simple_test_tm(&data, part_size);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("output.dat");
    let header = rand_data(4096);
    std::fs::write(&path, &header).unwrap();
    let file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();

    let error = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .write_to_file(file)
        .expect_err("an append-mode destination must be rejected");

    assert_eq!(error.kind(), &ErrorKind::InputInvalid);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), header.len() as u64);
    assert_eq!(std::fs::read(&path).unwrap(), header.as_ref());
    assert_eq!(http_client.actual_requests().count(), 0);
}

/// Download requests carry `x-amz-checksum-mode: ENABLED` by default. The SDK's
/// GetObject operation auto-enables it when the client resolves
/// `ResponseChecksumValidation` to `WHEN_SUPPORTED` (the default), so the TM gets
/// response validation without setting the mode itself. This guards against a
/// regression (SDK change or client config) that would silently stop requesting
/// the stored checksum and therefore stop validating downloads.
#[tokio::test]
async fn test_checksum_mode_enabled_by_default() {
    // Multipart object so the assertion covers the discovery GET and every
    // subsequent range-chunk GET.
    let data = rand_data(20 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 8 * ByteUnit::Mebibyte.as_bytes_usize();
    let (tm, http_client) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();

    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert!(requests.len() > 1, "expected multiple GETs (multipart)");
    for (i, req) in requests.iter().enumerate() {
        assert_eq!(
            req.headers().get("x-amz-checksum-mode"),
            Some("ENABLED"),
            "GET #{i} must carry x-amz-checksum-mode=ENABLED by default"
        );
    }
}

fn single_part_connector(data: &Bytes, crc32: Option<&str>) -> StaticReplayClient {
    single_part_connector_with_checksum(data, crc32.map(|value| ("x-amz-checksum-crc32", value)))
}

/// Like `single_part_connector`, but `checksum` sets any checksum header, as
/// `(header, value)`.
fn single_part_connector_with_checksum(
    data: &Bytes,
    checksum: Option<(&str, &str)>,
) -> StaticReplayClient {
    let mut resp = http::Response::builder()
        .status(200)
        .header("Content-Length", format!("{}", data.len()))
        .header(
            "Content-Range",
            format!("bytes 0-{}/{}", data.len() - 1, data.len()),
        )
        .header("ETag", "my-etag");
    if let Some((header, value)) = checksum {
        resp = resp.header(header, value);
    }
    StaticReplayClient::new(vec![ReplayEvent::new(
        dummy_expected_request(),
        resp.body(SdkBody::from(data.clone())).unwrap(),
    )])
}

/// Like `simple_object_connector`, but each response carries the
/// `x-amz-checksum-crc32` value `crc32` returns for the chunk's index.
fn crc32_object_connector(
    data: &Bytes,
    part_size: usize,
    crc32: impl Fn(usize) -> String,
) -> StaticReplayClient {
    let events = data
        .chunks(part_size)
        .enumerate()
        .map(|(idx, chunk)| {
            let start = idx * part_size;
            let end = start + chunk.len() - 1;
            ReplayEvent::new(
                dummy_expected_request(),
                http::Response::builder()
                    .status(200)
                    .header("Content-Length", format!("{}", chunk.len()))
                    .header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", data.len()),
                    )
                    .header("ETag", "my-etag")
                    .header("x-amz-checksum-crc32", crc32(idx))
                    .body(SdkBody::from(chunk))
                    .unwrap(),
            )
        })
        .collect();

    StaticReplayClient::new(events)
}

// "hello world" has CRC32 `DUoRhQ==`. Using a correct value lets the SDK's
// default validation pass so we exercise the success path.
const HELLO: &[u8] = b"hello world";
const HELLO_CRC32: &str = "DUoRhQ==";

/// The base64 CRC32 of `bytes`, as S3 reports it.
fn crc32_base64(bytes: &[u8]) -> String {
    let mut crc32 = aws_smithy_checksums::ChecksumAlgorithm::Crc32.into_impl();
    crc32.update(bytes);
    aws_smithy_types::base64::encode(crc32.finalize())
}

/// Downloads `test-object` through `tm`, with `checksum_mode` if set, and returns
/// the download's checksum verdict.
async fn download_checksum_validation(
    tm: &aws_sdk_s3_transfer_manager::Client,
    checksum_mode: Option<ChecksumMode>,
) -> ChecksumValidation {
    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .set_checksum_mode(checksum_mode)
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();
    let output = handle.join().await.unwrap();
    output.integrity_checks().checksum_validation().clone()
}

fn assert_validated(validation: &ChecksumValidation, expected: ChecksumAlgorithm) {
    match validation {
        ChecksumValidation::Validated { algorithm, .. } => assert_eq!(*algorithm, expected),
        other => panic!("expected Validated{{{expected:?}}}, got {other:?}"),
    }
}

fn assert_not_validated(validation: &ChecksumValidation, expected: NotValidatedReason) {
    match validation {
        ChecksumValidation::NotValidated { reason, .. } => assert_eq!(*reason, expected),
        other => panic!("expected NotValidated{{{expected:?}}}, got {other:?}"),
    }
}

/// Default checksum_mode (unset): the SDK auto-enables validation when the client
/// resolves ResponseChecksumValidation to WhenSupported (the default), so a
/// response whose CRC32 matches reads Validated, NOT Disabled. See
/// `test_integrity_checks_disabled_when_validation_when_required` for the only
/// genuinely-disabled case.
#[tokio::test]
async fn test_integrity_checks_default_mode_validates() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, Some(HELLO_CRC32)), part_size);

    let validation = download_checksum_validation(&tm, None).await;
    assert_validated(&validation, ChecksumAlgorithm::Crc32);
}

/// Client ResponseChecksumValidation=WhenRequired with no request override: the
/// SDK does not auto-enable ChecksumMode, so validation is genuinely off and the
/// verdict is Disabled.
#[tokio::test]
async fn test_integrity_checks_disabled_when_validation_when_required() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let http_client = single_part_connector(&data, Some(HELLO_CRC32));
    let shared = common::shared_config(http_client)
        .into_builder()
        .response_checksum_validation(
            aws_smithy_types::checksum_config::ResponseChecksumValidation::WhenRequired,
        )
        .build();
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .s3_config(aws_sdk_s3_transfer_manager::config::S3ClientConfig::new(
            &shared,
        ))
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencyMode::Explicit(1))
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(config);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();
    let output = handle.join().await.unwrap();

    match output.integrity_checks().checksum_validation() {
        ChecksumValidation::NotValidated { reason, .. } => {
            assert_eq!(*reason, NotValidatedReason::Disabled);
        }
        other => panic!("expected NotValidated{{Disabled}}, got {other:?}"),
    }
}

/// checksum_mode on, and the response's CRC32 matches the delivered bytes: the
/// verdict is Validated{Crc32}.
#[tokio::test]
async fn test_integrity_checks_enabled_validates() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, Some(HELLO_CRC32)), part_size);

    let validation = download_checksum_validation(&tm, Some(ChecksumMode::Enabled)).await;
    assert_validated(&validation, ChecksumAlgorithm::Crc32);
}

/// A response that carries only a part-level checksum (`<base64>-<parts>`), which
/// the SDK cannot validate, reads NotValidated{CompositeChecksum}.
#[tokio::test]
async fn test_integrity_checks_part_level_only_reads_composite_checksum() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, Some("DUoRhQ==-3")), part_size);

    let validation = download_checksum_validation(&tm, Some(ChecksumMode::Enabled)).await;
    assert_not_validated(&validation, NotValidatedReason::CompositeChecksum);
}

/// The first chunk's response carries its CRC32 and the second's only a part-level
/// checksum: some delivered bytes were validated, so the verdict is
/// NotValidated{PartialCoverage}.
#[tokio::test]
async fn test_integrity_checks_validated_and_part_level_read_partial_coverage() {
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let data = rand_data(part_size + 1024);
    let first_crc32 = crc32_base64(&data[..part_size]);
    let http_client = crc32_object_connector(&data, part_size, |index| match index {
        0 => first_crc32.clone(),
        _ => "AAAAAA==-2".to_owned(),
    });
    let tm = test_tm(http_client, part_size);

    let validation = download_checksum_validation(&tm, Some(ChecksumMode::Enabled)).await;
    assert_not_validated(&validation, NotValidatedReason::PartialCoverage);
}

/// A response with no checksum header, with validation requested, reads
/// NotValidated{Unavailable}.
#[tokio::test]
async fn test_integrity_checks_no_checksum_reads_unavailable() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, None), part_size);

    let validation = download_checksum_validation(&tm, Some(ChecksumMode::Enabled)).await;
    assert_not_validated(&validation, NotValidatedReason::Unavailable);
}

/// A response whose only checksum is MD5, which the SDK does not compute, reads
/// `NotValidated { reason: UnsupportedAlgorithm }`.
#[tokio::test]
async fn test_integrity_checks_md5_only_reads_unsupported_algorithm() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let http_client = single_part_connector_with_checksum(
        &data,
        Some(("x-amz-checksum-md5", "XrY7u+Ae7tCTyyK7j1rNww==")),
    );
    let tm = test_tm(http_client, part_size);

    let validation = download_checksum_validation(&tm, Some(ChecksumMode::Enabled)).await;
    assert_not_validated(&validation, NotValidatedReason::UnsupportedAlgorithm);
}

/// Whole object in the discovery chunk → the response checksum IS the object's,
/// surfaced on the value members.
#[tokio::test]
async fn test_integrity_checks_surfaces_whole_object_checksum() {
    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, Some(HELLO_CRC32)), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();
    let output = handle.join().await.unwrap();

    assert_eq!(
        output.integrity_checks().checksum_crc32(),
        Some(HELLO_CRC32)
    );
}

/// Multipart object (data larger than part_size) → discovery chunk carries only
/// part 1's checksum, which MUST NOT be surfaced as the object's checksum.
#[tokio::test]
async fn test_integrity_checks_multipart_does_not_surface_part_checksum() {
    let data = rand_data(12 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // simple_object_connector splits into parts and stamps no checksum header,
    // but even if part 1 carried one, it must not be reported as the object value.
    let (tm, _c) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();
    let output = handle.join().await.unwrap();

    assert_eq!(output.integrity_checks().checksum_crc32(), None);
    assert_eq!(output.integrity_checks().checksum_type(), None);
}
