/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! An HTTP/1.1 connector that sends an S3 client's requests over TCP.
//!
//! The connector pools connections with hyper-util's client and opens them
//! with `tokio::net::TcpStream`, which creates each socket non-blocking.
//! hyper-util's own TCP connector fails on WASI targets when it switches a new
//! socket to non-blocking.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use aws_smithy_runtime_api::client::http::{HttpConnector, HttpConnectorFuture};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::body::SdkBody;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::net::TcpStream;

/// Sends every request to one server over a pool of HTTP/1.1 connections,
/// whatever the request URI's authority.
///
/// Clones share the pool and the connection count. A spawned task drives each
/// connection and exits once the connection closes or every clone is dropped
/// while the connection is idle. Every request error is reported as an I/O
/// error.
#[derive(Clone, Debug)]
pub(crate) struct TcpHttp {
    client: Client<Connect, SdkBody>,
    connections: Arc<AtomicUsize>,
}

impl TcpHttp {
    /// Returns a connector with an empty pool for `server`.
    pub(crate) fn new(server: SocketAddr) -> Self {
        let connections = Arc::new(AtomicUsize::new(0));
        let connect = Connect {
            server,
            connections: Arc::clone(&connections),
        };
        let client = Client::builder(TokioExecutor::new()).build(connect);
        Self {
            client,
            connections,
        }
    }

    /// The number of connections opened.
    pub(crate) fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }
}

impl HttpConnector for TcpHttp {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(request) => request,
            Err(e) => return HttpConnectorFuture::ready(Err(ConnectorError::user(e.into()))),
        };
        let response = self.client.request(request);
        HttpConnectorFuture::new(async move {
            let response = response.await.map_err(|e| ConnectorError::io(e.into()))?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|e| ConnectorError::other(e.into(), None))
        })
    }
}

/// Opens a TCP connection to `server` for any URI.
#[derive(Clone, Debug)]
struct Connect {
    server: SocketAddr,
    connections: Arc<AtomicUsize>,
}

impl tower_service::Service<hyper::Uri> for Connect {
    type Response = TokioIo<TcpStream>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = std::io::Result<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: hyper::Uri) -> Self::Future {
        let server = self.server;
        let connections = Arc::clone(&self.connections);
        Box::pin(async move {
            let stream = TcpStream::connect(server).await?;
            stream.set_nodelay(true)?;
            connections.fetch_add(1, Ordering::Relaxed);
            Ok(TokioIo::new(stream))
        })
    }
}
