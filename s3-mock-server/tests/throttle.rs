// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Server-applies tests for the load-driven [`RateThrottle`]: its token-bucket
//! verdict is unit-tested in `src/throttle.rs`; these assert the observable HTTP
//! outcome (503 `SlowDown`) over real requests, including under concurrency.

use std::sync::Arc;

use aws_sdk_s3::Client;
use s3_mock_server::S3MockServer;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const B: &str = "throttle";
const K: &str = "obj";

/// A client to the mock with SDK retry DISABLED, so one SDK call is exactly one
/// HTTP request — and thus draws exactly one throttle token. (The default
/// `handle.client()` retries a 503 transparently, which would both hide the
/// throttle from the caller and draw extra tokens.)
async fn no_retry_client(handle: &s3_mock_server::ServerHandle) -> Client {
    let base = handle.client().await;
    let conf = base
        .config()
        .to_builder()
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
        .build();
    Client::from_conf(conf)
}

/// Start a server with a seeded object so HeadObject (a cheap, body-less probe of
/// the throttle) resolves when not throttled.
async fn setup() -> Result<(S3MockServer, s3_mock_server::ServerHandle)> {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build");
    let handle = server.start().await.expect("start");
    let s3 = handle.client().await;
    s3.create_bucket().bucket(B).send().await.ok();
    s3.put_object()
        .bucket(B)
        .key(K)
        .body(bytes::Bytes::from_static(b"x").into())
        .send()
        .await?;
    Ok((server, handle))
}

/// `true` if a HeadObject was throttled (503 `SlowDown`), `false` if it succeeded.
/// Uses a no-retry client so each call is one HTTP request / one token draw.
async fn head_is_throttled(client: &Client) -> bool {
    // A throttled request returns HTTP 503. HeadObject has no response body, so
    // the SDK cannot parse an error code (`SlowDown`) — detect by status.
    match client.head_object().bucket(B).key(K).send().await {
        Ok(_) => false,
        Err(e) => e.raw_response().map(|r| r.status().as_u16()) == Some(503),
    }
}

/// A burst beyond the token bucket's `burst` allowance is shed with 503, and once
/// the client stops (letting the bucket refill) requests are served again — the
/// load-driven relent, observed over real HTTP. Install the throttle AFTER seeding
/// so the seed's `create_bucket` + `put_object` do not draw tokens.
#[tokio::test]
async fn rate_throttle_sheds_a_burst_then_recovers() -> Result<()> {
    let (server, handle) = setup().await?;
    let client = no_retry_client(&handle).await;

    // A slow refill (1/sec) with a small burst, so a same-instant burst of 8
    // draws past the 3-token allowance and the rest are shed. 1/sec refill means
    // no meaningful refill happens within the tight burst loop.
    server.set_rate_throttle(1.0, 3.0);

    let mut throttled = 0usize;
    for _ in 0..8 {
        if head_is_throttled(&client).await {
            throttled += 1;
        }
    }
    // At most `burst` (3) are served; the remaining 5 are shed. Exact counts can
    // shift by one if a whole second elapses mid-loop (it will not on any real
    // machine), so assert the load-driven property: the burst is shed once the
    // allowance is spent, and not everything passed.
    assert!(
        throttled >= 4,
        "a burst past the token allowance must be shed, got {throttled} of 8"
    );

    // Let the bucket refill, then a single request is served again (relent).
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert!(
        !head_is_throttled(&client).await,
        "after refill the throttle must relent and serve the request"
    );

    handle.shutdown().await?;
    Ok(())
}

/// Under concurrency the token bucket is race-free: firing many requests at once
/// against a small burst allowance and slow refill sheds all but roughly the
/// allowance, and never serves more than the tokens available (no double-spend).
#[tokio::test]
async fn concurrent_burst_does_not_oversubscribe_tokens() -> Result<()> {
    let (server, handle) = setup().await?;
    const BURST: usize = 5;
    const TOTAL: usize = 100;
    let client = Arc::new(no_retry_client(&handle).await);

    // Slow refill so essentially no tokens are added during the concurrent burst;
    // the number served is bounded by the burst allowance.
    server.set_rate_throttle(1.0, BURST as f64);

    let tasks: Vec<_> = (0..TOTAL)
        .map(|_| {
            let c = client.clone();
            tokio::spawn(async move { head_is_throttled(&c).await })
        })
        .collect();

    let mut served = 0usize;
    for t in tasks {
        if !t.await? {
            served += 1;
        }
    }
    // A race (double-spent token) would serve MORE than the allowance. Allow a
    // tiny slack for at most one refill tick during the burst.
    assert!(
        (1..=BURST + 1).contains(&served),
        "served {served} must not exceed the burst allowance {BURST} (+1 refill slack)"
    );

    handle.shutdown().await?;
    Ok(())
}
