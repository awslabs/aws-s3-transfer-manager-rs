/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Objects, S3 clients and handles that download tests run against.

use std::sync::Arc;

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
