/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_config::Region;
use aws_sdk_s3_transfer_manager::{
    error::BoxError,
    metrics::unit::ByteUnit,
    types::{ConcurrencyMode, PartSize},
};
use pin_project_lite::pin_project;
use std::{
    cmp,
    iter::{self, repeat_with},
    task::Poll,
};

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
    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Config::builder()
            .http_client(http_client)
            .region(Region::from_static("us-west-2"))
            .with_test_defaults()
            .build(),
    );

    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_abort_download() {
    let data = rand_data(25 * ByteUnit::Mebibyte.as_bytes_usize());
    let part_size = ByteUnit::Mebibyte.as_bytes_usize();

    let (tm, http_client) = simple_test_tm(&data, part_size);

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .initiate()
        .unwrap();
    let _ = handle.object_meta().await;
    handle.abort().await;
    let requests = http_client.actual_requests().collect::<Vec<_>>();
    assert!(requests.len() < data.len() / part_size);
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
#[ignore = "TODO(redux): body read retry not implemented"]
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
#[ignore = "TODO(redux): body read retry not implemented"]
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

/// Single-part (whole object in the discovery chunk) response carrying an
/// optional CRC32 checksum header. `data` must match `crc32` (the SDK validates
/// any present checksum header under the default `WhenSupported` setting).
fn single_part_connector(data: &Bytes, crc32: Option<&str>) -> StaticReplayClient {
    let mut resp = http::Response::builder()
        .status(200)
        .header("Content-Length", format!("{}", data.len()))
        .header(
            "Content-Range",
            format!("bytes 0-{}/{}", data.len() - 1, data.len()),
        )
        .header("ETag", "my-etag");
    if let Some(c) = crc32 {
        resp = resp.header("x-amz-checksum-crc32", c);
    }
    StaticReplayClient::new(vec![ReplayEvent::new(
        dummy_expected_request(),
        resp.body(SdkBody::from(data.clone())).unwrap(),
    )])
}

// "hello world" has CRC32 `DUoRhQ==`. Using a correct value lets the SDK's
// default validation pass so we exercise the success path.
const HELLO: &[u8] = b"hello world";
const HELLO_CRC32: &str = "DUoRhQ==";

/// checksum_mode off → validation reported as Disabled, regardless of headers.
#[tokio::test]
async fn test_integrity_checks_disabled_when_mode_off() {
    use aws_sdk_s3_transfer_manager::types::{ChecksumValidation, NotValidatedReason};

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
        *output.integrity_checks().checksum_validation(),
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Disabled
        }
    );
}

/// checksum_mode on → reported as NotValidated (never falsely Validated) until
/// the SDK surfaces a positive validation outcome. Guards that an unconfirmed
/// chunk MUST NOT read Validated.
#[tokio::test]
async fn test_integrity_checks_enabled_not_falsely_validated() {
    use aws_sdk_s3_transfer_manager::types::ChecksumValidation;

    let data = Bytes::from_static(HELLO);
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    let tm = test_tm(single_part_connector(&data, Some(HELLO_CRC32)), part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .initiate()
        .unwrap();
    let _ = drain(&mut handle).await.unwrap();
    let output = handle.join().await.unwrap();

    assert!(
        !matches!(
            output.integrity_checks().checksum_validation(),
            ChecksumValidation::Validated { .. }
        ),
        "must not report Validated without an SDK-confirmed outcome"
    );
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
