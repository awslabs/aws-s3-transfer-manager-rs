/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Connection-level fault injection.
//!
//! A [`ConnectionFault`] is created per accepted connection and shared three
//! ways: the [`AbortAfterWrite`] IO wrapper around the socket reads it on every
//! write, the [`InjectConnectionFault`] service wrapper places a clone into each
//! request's extensions, and the request handler (`s3s.rs`) arms it when the
//! fault registry fires a connection fault for the requested key. Arming sets a
//! byte budget; once that many further bytes are written to the client, the
//! socket is aborted with a TCP RST and the client observes a `ConnectionReset`.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Sentinel meaning "not armed". A real budget of zero (abort immediately) is
/// represented as 1 internally; callers pass the desired further-byte budget.
const DISARMED: u64 = u64::MAX;

/// Per-connection abort control. Armed by the request handler, read by the
/// socket wrapper.
#[derive(Debug)]
pub(crate) struct ConnectionFault {
    /// Further bytes that may be written before the connection is aborted.
    /// `DISARMED` means inactive.
    budget: AtomicU64,
}

impl ConnectionFault {
    pub(crate) fn new() -> Self {
        Self {
            budget: AtomicU64::new(DISARMED),
        }
    }

    /// Arm the fault: abort the connection after `after_bytes` further bytes are
    /// written to the client.
    pub(crate) fn arm(&self, after_bytes: u64) {
        self.budget.store(after_bytes, Ordering::Relaxed);
    }

    fn is_armed(&self) -> bool {
        self.budget.load(Ordering::Relaxed) != DISARMED
    }

    /// Account `n` written bytes against the budget; returns true once the
    /// budget is exhausted and the connection should abort.
    fn consume(&self, n: u64) -> bool {
        let mut cur = self.budget.load(Ordering::Relaxed);
        loop {
            if cur == DISARMED {
                return false;
            }
            let next = cur.saturating_sub(n);
            match self
                .budget
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next == 0,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// A `TcpStream` wrapper that aborts the connection (RST) once the shared
/// [`ConnectionFault`] budget is exhausted.
pub(crate) struct AbortAfterWrite {
    stream: TcpStream,
    fault: Arc<ConnectionFault>,
    aborted: bool,
}

impl AbortAfterWrite {
    pub(crate) fn new(stream: TcpStream, fault: Arc<ConnectionFault>) -> Self {
        Self {
            stream,
            fault,
            aborted: false,
        }
    }

    /// Set linger to zero so the drop sends a RST, then report a reset error.
    fn abort(&mut self) -> io::Error {
        self.aborted = true;
        let _ = self.stream.set_zero_linger();
        io::Error::new(io::ErrorKind::ConnectionReset, "injected connection fault")
    }
}

impl AsyncRead for AbortAfterWrite {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.aborted {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected connection fault",
            )));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for AbortAfterWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.aborted {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "injected connection fault",
            )));
        }
        if !self.fault.is_armed() {
            return Pin::new(&mut self.stream).poll_write(cx, buf);
        }
        let n = match Pin::new(&mut self.stream).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => n,
            other => return other,
        };
        if self.fault.consume(n as u64) {
            return Poll::Ready(Err(self.abort()));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

/// Hyper service wrapper that injects the per-connection [`ConnectionFault`] into
/// every request's extensions, so the request handler can arm it for the
/// connection serving that request.
#[derive(Clone)]
pub(crate) struct InjectConnectionFault<S> {
    inner: S,
    fault: Arc<ConnectionFault>,
}

impl<S> InjectConnectionFault<S> {
    pub(crate) fn new(inner: S, fault: Arc<ConnectionFault>) -> Self {
        Self { inner, fault }
    }
}

impl<S> hyper::service::Service<http::Request<hyper::body::Incoming>> for InjectConnectionFault<S>
where
    S: hyper::service::Service<http::Request<hyper::body::Incoming>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn call(&self, mut req: http::Request<hyper::body::Incoming>) -> Self::Future {
        req.extensions_mut().insert(self.fault.clone());
        self.inner.call(req)
    }
}
