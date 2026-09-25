/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The runtime-provided HTTP transport honors proxy environment variables, as
//! the SDK's default HTTPS client does.
//!
//! Its own test binary: proxy configuration is read from the process
//! environment, which must not change underneath concurrently running tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3_transfer_manager::config::S3ClientConfig;
use aws_sdk_s3_transfer_manager::types::RuntimeMode;
use s3_mock_server::S3MockServer;
use tokio::net::{TcpListener, TcpStream};

/// A forwarding HTTP proxy: relays every accepted connection to `upstream`
/// and counts connections, so the test can observe that requests went through
/// it. Plain-HTTP proxying sends absolute-form request targets, which the mock
/// server accepts, so no request rewriting is needed.
async fn start_relay_proxy(
    upstream: std::net::SocketAddr,
) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let addr = listener.local_addr().expect("proxy address");
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = connections.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                if let Ok(mut outbound) = TcpStream::connect(upstream).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    (addr, connections)
}

#[tokio::test]
async fn test_managed_runtime_http_uses_environment_proxy() {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");
    let content = vec![7u8; 64 * 1024];
    server
        .add_object("test-bucket", "proxied", content.clone(), None)
        .await
        .expect("add object");

    let (proxy_addr, proxy_connections) = start_relay_proxy(handle.socket_addr()).await;
    // This binary contains only this test, so nothing else observes the
    // environment change.
    std::env::set_var("HTTP_PROXY", format!("http://{proxy_addr}"));
    std::env::remove_var("http_proxy");
    std::env::remove_var("NO_PROXY");
    std::env::remove_var("no_proxy");

    let s3_config = handle.client().await.config().to_builder();
    let tm = aws_sdk_s3_transfer_manager::Client::new(
        aws_sdk_s3_transfer_manager::Config::builder()
            .s3_config(S3ClientConfig::new(s3_config))
            .runtime_mode(RuntimeMode::Managed)
            .build(),
    );

    let mut download = tm
        .download()
        .bucket("test-bucket")
        .key("proxied")
        .initiate()
        .expect("initiate download");
    let body = tokio::time::timeout(Duration::from_secs(30), async {
        let mut body = Vec::new();
        while let Some(chunk) = download.body_mut().next().await {
            body.extend_from_slice(&chunk.expect("download chunk").data.into_contiguous());
        }
        body
    })
    .await
    .expect("download timed out");

    assert!(body == content, "data integrity check failed");
    assert!(
        proxy_connections.load(Ordering::Relaxed) > 0,
        "requests must be routed through the HTTP_PROXY proxy"
    );

    handle.shutdown().await.expect("shutdown");
}
