/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_config::Region;
use aws_s3_transfer_manager::{
    error::BoxError,
    operation::download::DownloadHandle,
    types::{ConcurrencySetting, PartSize},
};
use std::iter;

use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use bytes::{BufMut, Bytes, BytesMut};

/// NOTE: these tests are somewhat brittle as they assume particular paths through the codebase.
/// As an example we generally assume object discovery goes through `GetObject` with a ranged get
/// for the first part.

const MEBIBYTE: usize = 1024 * 1024;

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
fn dummy_expected_request() -> http_02x::Request<SdkBody> {
    http_02x::Request::builder()
        .uri("https://not-used")
        .body(SdkBody::from(&b""[..]))
        .unwrap()
}

/// drain/consume the body
async fn drain(handle: &mut DownloadHandle) -> Result<Bytes, BoxError> {
    let body = handle.body_mut();
    let mut data = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?.into_bytes();
        data.put(chunk);
    }

    Ok(data.into())
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
            let end = part_size * (idx + 1) - 1;
            ReplayEvent::new(
                // NOTE: Rather than try to recreate all the expected requests we just put in placeholders and
                // make our own assertions against the captured requests.
                dummy_expected_request(),
                http_02x::Response::builder()
                    .status(200)
                    .header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", data.len()),
                    )
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
) -> (aws_s3_transfer_manager::Client, StaticReplayClient) {
    let http_client = simple_object_connector(data, part_size);

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Config::builder()
            .http_client(http_client.clone())
            .region(Region::from_static("us-west-2"))
            .with_test_defaults()
            .build(),
    );

    let config = aws_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .part_size(PartSize::Target(part_size as u64))
        .concurrency(ConcurrencySetting::Explicit(1))
        .build();

    let tm = aws_s3_transfer_manager::Client::new(config);

    (tm, http_client)
}

/// Test the object ranges are expected and we get all the data
#[tokio::test]
async fn test_download_ranges() {
    let data = rand_data(12 * MEBIBYTE);
    let part_size = 5 * MEBIBYTE;

    let (tm, http_client) = simple_test_tm(&data, part_size);

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .send()
        .await
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

    handle.join().await.unwrap();
}

/// Test body not consumed which should not prevent the handle from being joined
#[tokio::test]
async fn test_body_not_consumed() {
    let data = rand_data(12 * MEBIBYTE);
    let part_size = 5 * MEBIBYTE;

    let (tm, _) = simple_test_tm(&data, part_size);

    let handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .send()
        .await
        .unwrap();

    handle.join().await.unwrap();
}