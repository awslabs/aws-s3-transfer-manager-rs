/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Objects, S3 clients, handles and transfers that download tests run against.

use std::sync::Arc;

use crate::operation::download::body::{new_recv_body_with_disk_mode, RecvBodyConsumer};
use crate::operation::download::sink::SinkFactory;
use crate::operation::download::{DownloadInput, DownloadTransfer};
use crate::transfer::TransferContext;
use crate::types::BucketType;

/// Deterministic object contents of `len` bytes, distinct from the zeros an
/// unwritten or preallocated region reads as.
pub(crate) fn object_bytes(len: usize) -> Arc<[u8]> {
    (0..len).map(|i| (i % 251) as u8 + 1).collect()
}

/// An S3 client that serves `object` for any key, answering each ranged
/// `GetObject` with exactly the requested bytes.
pub(crate) fn object_client(object: Arc<[u8]>) -> aws_sdk_s3::Client {
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{mock, mock_client, MockResponse, RuleMode};

    use crate::http::header::{ByteRange, Range};

    let get = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
        let last = object.len() as u64 - 1;
        let (start, end) = match req.range().and_then(|r| r.parse::<Range>().ok()) {
            Some(Range(ByteRange::Inclusive(start, end))) => (start, end.min(last)),
            Some(Range(ByteRange::AllFrom(start))) => (start, last),
            _ => (0, last),
        };
        let body = object[start as usize..=end as usize].to_vec();
        MockResponse::Output(
            GetObjectOutput::builder()
                .content_length(body.len() as i64)
                .content_range(format!("bytes {start}-{end}/{}", object.len()))
                .e_tag("test-etag")
                .body(ByteStream::from(body))
                .build(),
        )
    });
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get])
}

/// An S3 client serving `object` by range that answers the range starting at
/// `failing_start` with a server error. SDK retries are disabled.
///
/// Panics on a request without an inclusive byte range.
pub(crate) fn object_client_failing_at(
    object: Arc<[u8]>,
    failing_start: u64,
) -> aws_sdk_s3::Client {
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{mock, mock_client, MockResponse, RuleMode};
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;

    use crate::http::header::{ByteRange, Range};

    let get = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
        let (start, end) = match req.range().and_then(|r| r.parse::<Range>().ok()) {
            Some(Range(ByteRange::Inclusive(start, end))) => {
                (start, end.min(object.len() as u64 - 1))
            }
            other => panic!("unexpected range request {other:?}"),
        };
        if start == failing_start {
            return MockResponse::Http(Response::new(
                StatusCode::try_from(500).unwrap(),
                SdkBody::from("internal error"),
            ));
        }
        let body = object[start as usize..=end as usize].to_vec();
        MockResponse::Output(
            GetObjectOutput::builder()
                .content_length(body.len() as i64)
                .content_range(format!("bytes {start}-{end}/{}", object.len()))
                .e_tag("test-etag")
                .body(ByteStream::from(body))
                .build(),
        )
    });
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get], |conf| {
        conf.retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
    })
}

/// A test handle on the ambient tokio runtime whose S3 client is `client`,
/// slicing downloads into `part_size` ranges.
pub(crate) fn disk_test_handle(
    client: aws_sdk_s3::Client,
    part_size: u64,
) -> Arc<crate::client::Handle> {
    let config = crate::Config::builder()
        .client(client)
        .part_size(crate::types::PartSize::Target(part_size))
        .build();
    crate::client::Handle::test_handle_tokio(config)
}

/// A test handle whose work runs on four managed threads under a fixed
/// `concurrency` target.
///
/// A held destination write blocks its managed thread, and work routed to that
/// thread queues behind it. A low target bounds how much work is in flight at
/// once, so a test can keep other work off the held thread.
pub(crate) fn managed_test_handle(
    config: crate::Config,
    concurrency: usize,
) -> Arc<crate::client::Handle> {
    crate::client::Handle::new_for_test_with_runtime(
        config,
        Arc::new(crate::scheduler::FixedConcurrency::new(concurrency)),
        |weak| {
            Arc::new(
                crate::runtime::ManagedThreadRuntime::builder(weak)
                    .topology(crate::runtime::Topology::uniform(4))
                    .build(),
            )
        },
    )
}

/// A disk download transfer on `handle`, writing through a sink that `sinks`
/// opens over a new file named `out` in a fresh temporary directory.
///
/// Transfers built on one handle share its memory budget. Returns the transfer,
/// its body consumer, and the directory; the test keeps both for the
/// transfer's lifetime.
pub(crate) fn disk_transfer(
    handle: Arc<crate::client::Handle>,
    sinks: &dyn SinkFactory,
) -> (DownloadTransfer, RecvBodyConsumer, tempfile::TempDir) {
    let input = DownloadInput::builder()
        .bucket("test-bucket")
        .key("test-key")
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = std::fs::File::create(dir.path().join("out")).unwrap();
    let (writer, consumer) = new_recv_body_with_disk_mode(sinks.open(file, false));
    let (ctx, _completion_rx) = TransferContext::new(handle);
    let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);
    (transfer, consumer, dir)
}
