/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A connector whose requests the embedding host answers.
//!
//! [`HostHttp`] serializes each request into a queue and returns a future that
//! stays pending until the host submits a response. The host takes requests
//! with [`next_request`], writes each response into the buffer returned by
//! [`buffer`], and submits it with [`respond`], in any order and across any
//! number of calls into the module.
//!
//! A request is `METHOD URI\n`, one `name: value\n` line per header, a blank
//! line, and the body. A response is `STATUS\n`, header lines in the same form,
//! a blank line, and the body. Header values contain no newline.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::future::poll_fn;
use std::sync::Mutex;
use std::task::{Poll, Waker};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;

static EXCHANGE: Mutex<Exchange> = Mutex::new(Exchange::new());

struct Exchange {
    next_id: u32,
    /// Serialized requests the host has not taken.
    queued: VecDeque<(u32, Vec<u8>)>,
    /// Requests whose futures are live, by id.
    outstanding: BTreeMap<u32, Outstanding>,
    /// The request most recently returned by [`next_request`].
    taken: Vec<u8>,
    /// Bytes the host writes before [`respond`] or another consumer reads them.
    buffer: Vec<u8>,
}

#[derive(Default)]
struct Outstanding {
    response: Option<HttpResponse>,
    waker: Option<Waker>,
}

impl Exchange {
    const fn new() -> Self {
        Self {
            next_id: 1,
            queued: VecDeque::new(),
            outstanding: BTreeMap::new(),
            taken: Vec::new(),
            buffer: Vec::new(),
        }
    }
}

/// The connector. Every instance shares the module's one exchange.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HostHttp;

/// Removes a request from the exchange when its future is dropped.
struct Registration(u32);

impl Drop for Registration {
    fn drop(&mut self) {
        let mut exchange = EXCHANGE.lock().unwrap();
        exchange.queued.retain(|(id, _)| *id != self.0);
        exchange.outstanding.remove(&self.0);
    }
}

impl HttpConnector for HostHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        HttpConnectorFuture::new(async move {
            let mut head = format!("{} {}\n", request.method(), request.uri());
            for (name, value) in request.headers() {
                writeln!(head, "{name}: {value}").expect("writing to a string cannot fail");
            }
            head.push('\n');
            let body = ByteStream::new(request.into_body())
                .collect()
                .await
                .map_err(|e| ConnectorError::other(e.into(), None))?
                .into_bytes();
            let mut serialized = head.into_bytes();
            serialized.extend_from_slice(&body);

            let registration = {
                let mut exchange = EXCHANGE.lock().unwrap();
                let id = exchange.next_id;
                exchange.next_id += 1;
                exchange.queued.push_back((id, serialized));
                exchange.outstanding.insert(id, Outstanding::default());
                Registration(id)
            };
            let response = poll_fn(|cx| {
                let mut exchange = EXCHANGE.lock().unwrap();
                let outstanding = exchange
                    .outstanding
                    .get_mut(&registration.0)
                    .expect("a live request stays registered");
                match outstanding.response.take() {
                    Some(response) => Poll::Ready(response),
                    None => {
                        outstanding.waker = Some(cx.waker().clone());
                        Poll::Pending
                    }
                }
            })
            .await;
            Ok(response)
        })
    }
}

/// Whether any request future is waiting for a response.
pub(crate) fn awaiting_host() -> bool {
    !EXCHANGE.lock().unwrap().outstanding.is_empty()
}

/// Hands the next queued request to the host. Returns its nonzero id, or zero
/// when none is queued. [`taken`] holds its bytes until the next call.
pub(crate) fn next_request() -> u32 {
    let mut exchange = EXCHANGE.lock().unwrap();
    match exchange.queued.pop_front() {
        Some((id, bytes)) => {
            exchange.taken = bytes;
            id
        }
        None => {
            exchange.taken.clear();
            0
        }
    }
}

/// Address and length of the request most recently handed to the host.
pub(crate) fn taken() -> (*const u8, usize) {
    let exchange = EXCHANGE.lock().unwrap();
    (exchange.taken.as_ptr(), exchange.taken.len())
}

/// Resizes the host buffer to `len` zeroed bytes and returns its address. The
/// address is valid until the module next consumes or resizes the buffer.
pub(crate) fn buffer(len: usize) -> *mut u8 {
    let mut exchange = EXCHANGE.lock().unwrap();
    exchange.buffer.clear();
    exchange.buffer.resize(len, 0);
    exchange.buffer.as_mut_ptr()
}

/// Removes and returns the host buffer's contents.
pub(crate) fn take_buffer() -> Bytes {
    std::mem::take(&mut EXCHANGE.lock().unwrap().buffer).into()
}

/// Completes request `id` with the response serialized in the host buffer.
///
/// Returns `Ok(false)` if the request's future was dropped first, and an error
/// if the response is malformed.
pub(crate) fn respond(id: u32) -> Result<bool, String> {
    let response = parse_response(take_buffer())?;
    let waker = {
        let mut exchange = EXCHANGE.lock().unwrap();
        let Some(outstanding) = exchange.outstanding.get_mut(&id) else {
            return Ok(false);
        };
        outstanding.response = Some(response);
        outstanding.waker.take()
    };
    if let Some(waker) = waker {
        waker.wake();
    }
    Ok(true)
}

fn parse_response(bytes: Bytes) -> Result<HttpResponse, String> {
    let split = bytes
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or("response has no blank line after its head")?;
    let head = std::str::from_utf8(&bytes[..split]).map_err(|e| format!("response head: {e}"))?;
    let mut lines = head.split('\n');
    let status = lines
        .next()
        .and_then(|line| line.parse::<u16>().ok())
        .and_then(|code| StatusCode::try_from(code).ok())
        .ok_or_else(|| format!("invalid response status line in {head:?}"))?;
    let mut response = HttpResponse::new(status, SdkBody::from(bytes.slice(split + 2..)));
    for line in lines {
        let (name, value) = line
            .split_once(": ")
            .ok_or_else(|| format!("invalid response header {line:?}"))?;
        response
            .headers_mut()
            .insert(name.to_owned(), value.to_owned());
    }
    Ok(response)
}
