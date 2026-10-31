/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! An in-memory S3 connector covering the requests that single-part and
//! multipart transfers send.
//!
//! Every request first waits for one simulated host I/O completion, so each
//! request future returns `Pending` at least once and resumes only through the
//! executor's wake path. The connector supports `PutObject`, multipart upload
//! creation, parts, and completion, and ranged `GetObject`. Tests can answer
//! requests with `503 SlowDown` and hold one matching request until its future
//! is dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use tokio::sync::Notify;

/// A request that [`MemoryS3::hold`] keeps pending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HoldRule {
    /// HTTP method, such as `PUT`.
    pub(crate) method: &'static str,
    /// First byte of the requested range, or `None` to match any request with
    /// this method.
    pub(crate) range_start: Option<u64>,
}

/// Request counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Requests {
    /// `PutObject` requests.
    pub(crate) put_object: usize,
    /// `UploadPart` requests.
    pub(crate) upload_part: usize,
    /// `CreateMultipartUpload` and `CompleteMultipartUpload` requests.
    pub(crate) multipart_control: usize,
    /// `GetObject` requests.
    pub(crate) get_object: usize,
    /// Requests answered with `503 SlowDown`.
    pub(crate) throttled: usize,
}

#[derive(Debug, Default)]
struct State {
    objects: HashMap<String, Bytes>,
    parts: HashMap<(String, u32), Bytes>,
    requests: Requests,
    /// `GetObject` range starts in request order.
    get_ranges: Vec<Option<u64>>,
    hold: Option<HoldRule>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    throttle_next: AtomicUsize,
    held_dropped: AtomicUsize,
    held: Notify,
}

/// The connector and its object store.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryS3 {
    shared: Arc<Shared>,
}

impl MemoryS3 {
    /// Stores `data` under `key` without sending a request.
    pub(crate) fn insert(&self, key: &str, data: Bytes) {
        self.shared
            .state
            .lock()
            .unwrap()
            .objects
            .insert(key.to_owned(), data);
    }

    /// Returns the object stored under `key`.
    pub(crate) fn object(&self, key: &str) -> Option<Bytes> {
        self.shared.state.lock().unwrap().objects.get(key).cloned()
    }

    /// Answers the next `count` requests with `503 SlowDown`.
    pub(crate) fn throttle_next(&self, count: usize) {
        self.shared.throttle_next.store(count, Ordering::SeqCst);
    }

    /// Holds the first later request matching `rule` until its future is dropped.
    pub(crate) fn hold(&self, rule: HoldRule) {
        self.shared.state.lock().unwrap().hold = Some(rule);
    }

    /// Completes after a request has been held.
    pub(crate) async fn wait_held(&self) {
        self.shared.held.notified().await;
    }

    /// Number of held request futures that have been dropped.
    pub(crate) fn held_dropped(&self) -> usize {
        self.shared.held_dropped.load(Ordering::SeqCst)
    }

    /// Returns the request counters.
    pub(crate) fn requests(&self) -> Requests {
        self.shared.state.lock().unwrap().requests
    }

    /// Returns the range start of every `GetObject` request in order.
    pub(crate) fn get_ranges(&self) -> Vec<Option<u64>> {
        self.shared.state.lock().unwrap().get_ranges.clone()
    }
}

/// Counts the drop of a held request future.
struct HeldGuard(Arc<Shared>);

impl Drop for HeldGuard {
    fn drop(&mut self) {
        self.0.held_dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl HttpConnector for MemoryS3 {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let shared = Arc::clone(&self.shared);
        HttpConnectorFuture::new(async move {
            crate::host::host_io().await;
            let method = request.method().to_owned();
            let (key, query) = split_uri(request.uri());
            let chunked = request
                .headers()
                .get("content-encoding")
                .is_some_and(|value| value.contains("aws-chunked"));
            let range = request
                .headers()
                .get("range")
                .map(parse_range)
                .transpose()?;
            let body = ByteStream::new(request.into_body())
                .collect()
                .await
                .map_err(|e| ConnectorError::other(e.into(), None))?
                .into_bytes();
            let body = if chunked {
                decode_aws_chunked(&body)?
            } else {
                body
            };

            let held = {
                let mut state = shared.state.lock().unwrap();
                let matches = state.hold.as_ref().is_some_and(|rule| {
                    rule.method == method
                        && (rule.range_start.is_none()
                            || rule.range_start == range.map(|(start, _)| start))
                });
                if matches {
                    state.hold = None;
                }
                matches
            };
            if held {
                let _guard = HeldGuard(Arc::clone(&shared));
                shared.held.notify_one();
                std::future::pending::<()>().await;
            }

            let throttled = shared
                .throttle_next
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            let mut state = shared.state.lock().unwrap();
            if throttled {
                state.requests.throttled += 1;
                return Ok(response(
                    503,
                    &[],
                    "<Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>",
                ));
            }
            Ok(serve(&mut state, &method, key, &query, range, body))
        })
    }
}

fn serve(
    state: &mut State,
    method: &str,
    key: String,
    query: &str,
    range: Option<(u64, Option<u64>)>,
    body: Bytes,
) -> HttpResponse {
    let param = |name: &str| {
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
    };
    let xml = ("content-type", "application/xml".to_owned());
    match method {
        "POST" if query.starts_with("uploads") => {
            state.requests.multipart_control += 1;
            response(
                200,
                &[xml],
                "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key>\
                 <UploadId>upload</UploadId></InitiateMultipartUploadResult>",
            )
        }
        "POST" => {
            state.requests.multipart_control += 1;
            let mut numbers: Vec<u32> = state
                .parts
                .keys()
                .filter(|(part_key, _)| *part_key == key)
                .map(|(_, number)| *number)
                .collect();
            numbers.sort_unstable();
            let mut object = Vec::new();
            for number in numbers {
                object.extend_from_slice(&state.parts.remove(&(key.clone(), number)).unwrap());
            }
            state.objects.insert(key, object.into());
            response(
                200,
                &[xml],
                "<CompleteMultipartUploadResult><ETag>\"object\"</ETag>\
                 </CompleteMultipartUploadResult>",
            )
        }
        "PUT" => match param("partNumber").map(str::parse::<u32>) {
            Some(Ok(number)) => {
                state.requests.upload_part += 1;
                state.parts.insert((key, number), body);
                response(200, &[("etag", format!("\"part-{number}\""))], "")
            }
            Some(Err(_)) => response(400, &[], "<Error><Code>InvalidArgument</Code></Error>"),
            None => {
                state.requests.put_object += 1;
                state.objects.insert(key, body);
                response(200, &[("etag", "\"object\"".to_owned())], "")
            }
        },
        "GET" => {
            state.requests.get_object += 1;
            state.get_ranges.push(range.map(|(start, _)| start));
            let Some(data) = state.objects.get(&key) else {
                return response(404, &[], "<Error><Code>NoSuchKey</Code></Error>");
            };
            let total = data.len() as u64;
            let (start, end) = match range {
                Some((start, end)) => (start, end.unwrap_or(total - 1).min(total - 1)),
                None => (0, total - 1),
            };
            let slice = data.slice(start as usize..=end as usize);
            let mut resp = response(
                206,
                &[
                    ("etag", "\"object\"".to_owned()),
                    ("content-range", format!("bytes {start}-{end}/{total}")),
                    ("content-length", slice.len().to_string()),
                ],
                "",
            );
            *resp.body_mut() = SdkBody::from(slice);
            resp
        }
        _ => response(501, &[], ""),
    }
}

/// Splits a request URI into its object key path and query string.
///
/// The key path starts with `/` and excludes the bucket for both virtual-hosted
/// and path-style addressing.
fn split_uri(uri: &str) -> (String, String) {
    let without_scheme = uri.split_once("://").map_or(uri, |(_, rest)| rest);
    let path = without_scheme
        .find('/')
        .map_or("/", |index| &without_scheme[index..]);
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let path = path
        .strip_prefix("/bucket")
        .filter(|rest| rest.starts_with('/'))
        .unwrap_or(path);
    (path.to_owned(), query.to_owned())
}

/// Parses `bytes=start-[end]`.
fn parse_range(value: &str) -> Result<(u64, Option<u64>), ConnectorError> {
    let invalid = || ConnectorError::other(format!("invalid range header: {value}").into(), None);
    let (start, end) = value
        .strip_prefix("bytes=")
        .and_then(|range| range.split_once('-'))
        .ok_or_else(invalid)?;
    let start = start.parse().map_err(|_| invalid())?;
    let end = match end {
        "" => None,
        end => Some(end.parse().map_err(|_| invalid())?),
    };
    Ok((start, end))
}

/// Decodes an `aws-chunked` body, discarding chunk signatures and trailers.
fn decode_aws_chunked(mut rest: &[u8]) -> Result<Bytes, ConnectorError> {
    let invalid = || ConnectorError::other("malformed aws-chunked body".into(), None);
    let mut out = Vec::new();
    loop {
        let line_end = rest
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(invalid)?;
        let line = std::str::from_utf8(&rest[..line_end]).map_err(|_| invalid())?;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or(""), 16)
            .map_err(|_| invalid())?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(out.into());
        }
        if rest.len() < size + 2 {
            return Err(invalid());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

fn response(status: u16, headers: &[(&str, String)], body: &'static str) -> HttpResponse {
    let mut response = HttpResponse::new(
        StatusCode::try_from(status).expect("valid status code"),
        SdkBody::from(body),
    );
    for (name, value) in headers {
        response
            .headers_mut()
            .insert(name.to_string(), value.clone());
    }
    response
}
