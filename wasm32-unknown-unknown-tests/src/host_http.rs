/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! A connector whose requests the embedding host performs.
//!
//! [`HostHttp`] serializes each request, passes it to the host with the
//! `http_send` import, and returns a future that stays pending until the host
//! writes the response into the buffer returned by [`buffer`] and submits it
//! with [`respond`]. The host answers requests in any order, between calls
//! into the module. Dropping the future before the host answers calls the
//! `http_cancel` import.
//!
//! A request is `METHOD URI\n`, one `name: value\n` line per header, a blank
//! line, and the body. A response is `STATUS\n`, header lines in the same form,
//! a blank line, and the body. Header values contain no newline.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::poll_fn;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Poll, Waker};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;

use crate::imports;

static EXCHANGE: Mutex<Exchange> = Mutex::new(Exchange::new());

static SENT: AtomicUsize = AtomicUsize::new(0);
static PENDING_POLLS: AtomicUsize = AtomicUsize::new(0);
static CANCELLED: AtomicUsize = AtomicUsize::new(0);

struct Exchange {
    next_id: u32,
    /// Requests whose futures are live, by id.
    outstanding: BTreeMap<u32, Outstanding>,
    /// Bytes the host writes before [`respond`] or another consumer reads them.
    buffer: Vec<u8>,
}

#[derive(Default)]
struct Outstanding {
    answered: bool,
    response: Option<HttpResponse>,
    waker: Option<Waker>,
}

impl Exchange {
    const fn new() -> Self {
        Self {
            next_id: 1,
            outstanding: BTreeMap::new(),
            buffer: Vec::new(),
        }
    }
}

/// Request counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
    /// Requests passed to `http_send`.
    pub(crate) sent: usize,
    /// Polls in which a sent request's future returned `Pending`.
    pub(crate) pending_polls: usize,
    /// Requests passed to `http_cancel`.
    pub(crate) cancelled: usize,
}

/// Returns the current counters.
pub(crate) fn stats() -> Stats {
    Stats {
        sent: SENT.load(Ordering::SeqCst),
        pending_polls: PENDING_POLLS.load(Ordering::SeqCst),
        cancelled: CANCELLED.load(Ordering::SeqCst),
    }
}

/// The connector. Every instance shares the module's one exchange.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HostHttp;

/// Removes a request from the exchange when its future is dropped, and
/// cancels it if the host has not answered it.
struct Registration(u32);

impl Drop for Registration {
    fn drop(&mut self) {
        let removed = EXCHANGE.lock().unwrap().outstanding.remove(&self.0);
        if removed.is_some_and(|outstanding| !outstanding.answered) {
            CANCELLED.fetch_add(1, Ordering::SeqCst);
            imports::http_cancel(self.0);
        }
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
                exchange.next_id = id.checked_add(1).expect("request ids are exhausted");
                exchange.outstanding.insert(id, Outstanding::default());
                Registration(id)
            };
            SENT.fetch_add(1, Ordering::SeqCst);
            imports::http_send(registration.0, &serialized);
            drop(serialized);
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
                        PENDING_POLLS.fetch_add(1, Ordering::SeqCst);
                        Poll::Pending
                    }
                }
            })
            .await;
            Ok(response)
        })
    }
}

/// Whether any sent request awaits the host's response.
pub(crate) fn awaiting_host() -> bool {
    EXCHANGE
        .lock()
        .unwrap()
        .outstanding
        .values()
        .any(|outstanding| !outstanding.answered)
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
/// if the response is malformed or the request was already answered.
pub(crate) fn respond(id: u32) -> Result<bool, String> {
    let response = parse_response(take_buffer())?;
    let waker = {
        let mut exchange = EXCHANGE.lock().unwrap();
        let Some(outstanding) = exchange.outstanding.get_mut(&id) else {
            return Ok(false);
        };
        if outstanding.answered {
            return Err(format!("request {id} was answered twice"));
        }
        outstanding.answered = true;
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
            .or_else(|| line.strip_suffix(':').map(|name| (name, "")))
            .ok_or_else(|| format!("invalid response header {line:?}"))?;
        response
            .headers_mut()
            .append(name.to_owned(), value.to_owned());
    }
    Ok(response)
}
