/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! An in-memory S3 connector covering the requests that object and directory
//! transfers send.
//!
//! The connector supports `PutObject`, multipart upload creation, parts, and
//! completion, ranged `GetObject`, and single-page `ListObjectsV2`. Every
//! request yields to the runtime once before it is served, so each request
//! future returns `Pending` at least once. Tests can answer requests with
//! `503 SlowDown`.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;

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
    /// `ListObjectsV2` requests.
    pub(crate) list_objects: usize,
    /// Requests answered with `503 SlowDown`.
    pub(crate) throttled: usize,
    /// Object bytes received in `PutObject` and `UploadPart` bodies.
    pub(crate) bytes_received: usize,
    /// Object bytes sent in `GetObject` bodies.
    pub(crate) bytes_sent: usize,
}

/// Lists the nonzero counters, such as `3 UploadPart, 2 multipart control,
/// 20971520 bytes received`, or `no requests`.
impl std::fmt::Display for Requests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let counters = [
            (self.put_object, "PutObject"),
            (self.upload_part, "UploadPart"),
            (self.multipart_control, "multipart control"),
            (self.get_object, "GetObject"),
            (self.list_objects, "ListObjectsV2"),
            (self.throttled, "throttled"),
            (self.bytes_received, "bytes received"),
            (self.bytes_sent, "bytes sent"),
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
struct State {
    /// Objects by key, without a leading `/`.
    objects: BTreeMap<String, Bytes>,
    /// Parts of each open multipart upload, by upload ID and part number.
    uploads: HashMap<String, BTreeMap<u32, Bytes>>,
    next_upload: usize,
    requests: Requests,
    /// `GetObject` range starts in request order.
    get_ranges: Vec<Option<u64>>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    throttle_next: AtomicUsize,
}

/// The connector and its object store.
#[derive(Clone, Debug, Default)]
pub(crate) struct MemoryS3 {
    shared: Arc<Shared>,
}

impl MemoryS3 {
    /// Stores `data` under `key` without sending a request.
    pub(crate) fn insert(&self, key: &str, data: Bytes) {
        self.state().objects.insert(key.to_owned(), data);
    }

    /// Returns the object stored under `key`.
    pub(crate) fn object(&self, key: &str) -> Option<Bytes> {
        self.state().objects.get(key).cloned()
    }

    /// Answers the next `count` requests with `503 SlowDown`.
    pub(crate) fn throttle_next(&self, count: usize) {
        self.shared.throttle_next.store(count, Ordering::SeqCst);
    }

    /// Returns the request counters.
    pub(crate) fn requests(&self) -> Requests {
        self.state().requests
    }

    /// Returns the range start of every `GetObject` request in order.
    pub(crate) fn get_ranges(&self) -> Vec<Option<u64>> {
        self.state().get_ranges.clone()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.shared.state.lock().unwrap()
    }
}

impl HttpConnector for MemoryS3 {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let shared = Arc::clone(&self.shared);
        HttpConnectorFuture::new(async move {
            tokio::task::yield_now().await;
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
                    "<Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>"
                        .into(),
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
    query: &HashMap<String, String>,
    range: Option<(u64, Option<u64>)>,
    body: Bytes,
) -> HttpResponse {
    let xml = ("content-type", "application/xml".to_owned());
    match method {
        "POST" if query.contains_key("uploads") => {
            state.requests.multipart_control += 1;
            state.next_upload += 1;
            let upload_id = format!("upload-{}", state.next_upload);
            state.uploads.insert(upload_id.clone(), BTreeMap::new());
            response(
                200,
                &[xml],
                format!(
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket>\
                     <Key>{}</Key><UploadId>{upload_id}</UploadId>\
                     </InitiateMultipartUploadResult>",
                    escape_xml(&key)
                )
                .into(),
            )
        }
        "POST" => {
            state.requests.multipart_control += 1;
            let Some(parts) = query
                .get("uploadId")
                .and_then(|id| state.uploads.remove(id))
            else {
                return response(404, &[], "<Error><Code>NoSuchUpload</Code></Error>".into());
            };
            let mut object = Vec::new();
            for part in parts.values() {
                object.extend_from_slice(part);
            }
            state.objects.insert(key, object.into());
            response(
                200,
                &[xml],
                "<CompleteMultipartUploadResult><ETag>\"object\"</ETag>\
                 </CompleteMultipartUploadResult>"
                    .into(),
            )
        }
        "PUT" => match query.get("partNumber").map(|n| n.parse::<u32>()) {
            Some(Ok(number)) => {
                state.requests.upload_part += 1;
                let Some(parts) = query
                    .get("uploadId")
                    .and_then(|id| state.uploads.get_mut(id))
                else {
                    return response(404, &[], "<Error><Code>NoSuchUpload</Code></Error>".into());
                };
                state.requests.bytes_received += body.len();
                parts.insert(number, body);
                response(200, &[("etag", format!("\"part-{number}\""))], Bytes::new())
            }
            Some(Err(_)) => response(
                400,
                &[],
                "<Error><Code>InvalidArgument</Code></Error>".into(),
            ),
            None => {
                state.requests.put_object += 1;
                state.requests.bytes_received += body.len();
                state.objects.insert(key, body);
                response(200, &[("etag", "\"object\"".to_owned())], Bytes::new())
            }
        },
        "GET" if query.get("list-type").map(String::as_str) == Some("2") => {
            state.requests.list_objects += 1;
            let prefix = query.get("prefix").map_or("", String::as_str);
            let mut contents = String::new();
            let mut count = 0;
            for (key, data) in state.objects.range(prefix.to_owned()..) {
                if !key.starts_with(prefix) {
                    break;
                }
                count += 1;
                write!(
                    contents,
                    "<Contents><Key>{}</Key><LastModified>2025-01-01T00:00:00.000Z</LastModified>\
                     <ETag>&quot;object&quot;</ETag><Size>{}</Size>\
                     <StorageClass>STANDARD</StorageClass></Contents>",
                    escape_xml(key),
                    data.len()
                )
                .expect("writing to a string cannot fail");
            }
            response(
                200,
                &[xml],
                format!(
                    "<ListBucketResult><Name>bucket</Name><Prefix>{}</Prefix>\
                     <KeyCount>{count}</KeyCount><MaxKeys>1000</MaxKeys>\
                     <IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
                    escape_xml(prefix)
                )
                .into(),
            )
        }
        "GET" => {
            state.requests.get_object += 1;
            state.get_ranges.push(range.map(|(start, _)| start));
            let Some(data) = state.objects.get(&key) else {
                return response(404, &[], "<Error><Code>NoSuchKey</Code></Error>".into());
            };
            let total = data.len() as u64;
            let Some((start, end)) = range else {
                let data = data.clone();
                state.requests.bytes_sent += data.len();
                return response(
                    200,
                    &[
                        ("etag", "\"object\"".to_owned()),
                        ("content-length", total.to_string()),
                    ],
                    data,
                );
            };
            let end = end.unwrap_or(total - 1).min(total - 1);
            let slice = data.slice(start as usize..=end as usize);
            state.requests.bytes_sent += slice.len();
            response(
                206,
                &[
                    ("etag", "\"object\"".to_owned()),
                    ("content-range", format!("bytes {start}-{end}/{total}")),
                    ("content-length", slice.len().to_string()),
                ],
                slice,
            )
        }
        _ => response(501, &[], Bytes::new()),
    }
}

/// Splits a request URI into its decoded object key, without a leading `/`,
/// and its decoded query parameters.
///
/// The key excludes the bucket for both virtual-hosted and path-style
/// addressing.
fn split_uri(uri: &str) -> (String, HashMap<String, String>) {
    let without_scheme = uri.split_once("://").map_or(uri, |(_, rest)| rest);
    let path = without_scheme
        .find('/')
        .map_or("/", |index| &without_scheme[index..]);
    let (path, query) = path.split_once('?').unwrap_or((path, ""));
    let path = path
        .strip_prefix("/bucket")
        .filter(|rest| rest.is_empty() || rest.starts_with('/'))
        .unwrap_or(path);
    let key = percent_decode(path.trim_start_matches('/'));
    let query = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_decode(name), percent_decode(value))
        })
        .collect();
    (key, query)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
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

fn response(status: u16, headers: &[(&str, String)], body: Bytes) -> HttpResponse {
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
