/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Replay connectors for download tests, and the Unix and Windows retention
//! helpers in [`retention`].

#[cfg(any(unix, windows))]
pub(super) mod retention;

use crate::dummy_expected_request;
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use bytes::Bytes;

/// Create a static replay client (http connector) for an object of the given size.
///
/// Assumptions:
///     1. Expected requests are not created. A dummy placeholder is used. Callers need to make
///        assertions directly on the captured requests.
///     2. Object discovery goes through ranged get which will fetch the first part.
///     3. Concurrency of 1 is used since responses for a static replay client are just returned in
///        the order given.
pub(super) fn object_connector(data: &Bytes, part_size: usize) -> StaticReplayClient {
    object_connector_with_checksum_headers(data, part_size, |_, _| None)
}

/// Like [`object_connector`], but each response also carries the header, as
/// `(name, value)`, that `checksum_header` returns for the chunk's index and
/// bytes.
pub(super) fn object_connector_with_checksum_headers(
    data: &Bytes,
    part_size: usize,
    checksum_header: impl Fn(usize, &[u8]) -> Option<(&'static str, String)>,
) -> StaticReplayClient {
    let events = data
        .chunks(part_size)
        .enumerate()
        .map(|(idx, chunk)| {
            let start = idx * part_size;
            let end = start + chunk.len() - 1;
            let mut response = http::Response::builder()
                .status(200)
                .header("Content-Length", format!("{}", chunk.len()))
                .header(
                    "Content-Range",
                    format!("bytes {start}-{end}/{}", data.len()),
                )
                .header("ETag", "my-etag");
            if let Some((name, value)) = checksum_header(idx, chunk) {
                response = response.header(name, value);
            }
            ReplayEvent::new(
                // NOTE: Rather than try to recreate all the expected requests we just put in placeholders and
                // make our own assertions against the captured requests.
                dummy_expected_request(),
                response.body(SdkBody::from(chunk)).unwrap(),
            )
        })
        .collect();

    StaticReplayClient::new(events)
}
