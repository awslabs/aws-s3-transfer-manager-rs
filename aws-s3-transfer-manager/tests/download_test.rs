/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{iter, mem};

use aws_config::Region;
use aws_s3_transfer_manager::{
    error::BoxError,
    operation::download::{body::Body, DownloadHandle},
    types::{ConcurrencySetting, PartSize},
};

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

fn dummy_expected_request() -> http_02x::Request<SdkBody> {
    http_02x::Request::builder()
        .uri("https://not-used")
        .body(SdkBody::from(&b""[..]))
        .unwrap()
}

async fn drain(handle: &mut DownloadHandle) -> Result<Bytes, BoxError> {
    let mut body = mem::replace(&mut handle.body, Body::empty());

    let mut data = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk?.into_bytes();
        data.put(chunk);
    }

    Ok(data.into())
}

/// Test the object ranges are expected and we get all the data
#[tokio::test]
async fn test_download_ranges() {
    tracing_subscriber::fmt::init();

    let object_data = rand_data(12 * MEBIBYTE);
    let part_size = 5 * MEBIBYTE;

    // NOTE: Rather than try to recreate all the expected requests we just put in placeholders and
    // make our own assertions against the captured requests.
    let http_client = StaticReplayClient::new(vec![
        // discovery w/first chunk
        ReplayEvent::new(
            dummy_expected_request(),
            http_02x::Response::builder()
                .status(200)
                .header(
                    "Content-Range",
                    format!("bytes {}/{}", 5 * MEBIBYTE, object_data.len()),
                )
                .body(SdkBody::from(object_data.slice(0..=part_size - 1)))
                .unwrap(),
        ),
        // chunk seq 1
        ReplayEvent::new(
            dummy_expected_request(),
            http_02x::Response::builder()
                .status(200)
                .body(SdkBody::from(
                    object_data.slice(part_size..=part_size * 2 - 1),
                ))
                .unwrap(),
        ),
        // chunk seq 2
        ReplayEvent::new(
            dummy_expected_request(),
            http_02x::Response::builder()
                .status(200)
                .body(SdkBody::from(object_data.slice(part_size * 2..)))
                .unwrap(),
        ),
    ]);

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

    let mut handle = tm
        .download()
        .bucket("test-bucket")
        .key("test-object")
        .send()
        .await
        .unwrap();

    // FIXME - without draining the body handle.join() will wait forever (e.g. due to channel being full)
    // we need to define the semantics we want here w.r.t the response body.
    let body = drain(&mut handle).await.unwrap();

    assert_eq!(object_data.len(), body.len());
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
