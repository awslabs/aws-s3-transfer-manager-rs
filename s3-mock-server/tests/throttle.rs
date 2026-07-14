// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! Server-applies tests for [`ThrottleSchedule`]: the schedule's verdict logic is
//! unit-tested in `src/throttle.rs`; these assert the observable HTTP outcome
//! (503 `SlowDown`) and that the global ordinal counts correctly across requests,
//! including under concurrency.

use std::sync::Arc;

use aws_sdk_s3::Client;
use s3_mock_server::{S3MockServer, ThrottleSchedule};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const B: &str = "throttle";
const K: &str = "obj";

/// A client to the mock with SDK retry DISABLED, so one SDK call is exactly one
/// HTTP request — and thus consumes exactly one throttle ordinal. (The default
/// `handle.client()` retries a 503 transparently, which would both hide the
/// throttle from the caller and burn extra ordinals.)
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
/// Uses a no-retry client so each call is one HTTP request / one ordinal.
async fn head_is_throttled(client: &Client) -> bool {
    // A throttled request returns HTTP 503. HeadObject has no response body, so
    // the SDK cannot parse an error code (`SlowDown`) — detect by status.
    match client.head_object().bucket(B).key(K).send().await {
        Ok(_) => false,
        Err(e) => e.raw_response().map(|r| r.status().as_u16()) == Some(503),
    }
}

/// A `healthy(N).throttled(M, 1.0)` schedule throttles exactly requests N..N+M and
/// passes the rest, as observed over sequential HTTP calls. Note: the seed
/// `create_bucket` + `put_object` in `setup` consume the first two ordinals, so
/// install the schedule AFTER seeding to count from zero.
#[tokio::test]
async fn schedule_throttles_the_scheduled_window() -> Result<()> {
    let (server, handle) = setup().await?;
    let client = no_retry_client(&handle).await;
    server.set_throttle_schedule(
        ThrottleSchedule::builder()
            .healthy(3)
            .throttled(4, 1.0)
            .build(),
    );

    let observed: Vec<bool> = {
        let mut v = Vec::new();
        for _ in 0..10 {
            v.push(head_is_throttled(&client).await);
        }
        v
    };
    // Ordinals 0,1,2 healthy; 3,4,5,6 throttled; 7,8,9 recovered.
    let expected = [
        false, false, false, true, true, true, true, false, false, false,
    ];
    assert_eq!(
        observed, expected,
        "throttle window did not match the schedule"
    );

    handle.shutdown().await?;
    Ok(())
}

/// Under concurrency the global ordinal is race-free: firing many requests at
/// once against a fixed-size storm throttles EXACTLY the scheduled count, even
/// though which requests are throttled is not fixed.
#[tokio::test]
async fn concurrent_requests_throttle_exactly_the_scheduled_count() -> Result<()> {
    let (server, handle) = setup().await?;
    const STORM: usize = 30;
    const TOTAL: usize = 100;
    let client = no_retry_client(&handle).await;
    server.set_throttle_schedule(
        ThrottleSchedule::builder()
            .throttled(STORM as u64, 1.0)
            .build(),
    );

    let client = Arc::new(client);
    let tasks: Vec<_> = (0..TOTAL)
        .map(|_| {
            let c = client.clone();
            tokio::spawn(async move { head_is_throttled(&c).await })
        })
        .collect();

    let mut throttled = 0usize;
    for t in tasks {
        if t.await? {
            throttled += 1;
        }
    }
    assert_eq!(
        throttled, STORM,
        "exactly the storm count must be throttled under concurrency"
    );

    handle.shutdown().await?;
    Ok(())
}
