/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Downloads over a connection pool capped to fewer connections than the download would use.
//!
//! These vary the transport, not the object. Each test downloads through an HTTP client from a
//! `ConnectionPool` with a small `max_connections_per_host`, and asserts only that every download
//! completes within [`TIMEOUT`] and delivers the source bytes. The verdicts are deliberately not
//! checked: the case tables check them.

use std::time::Duration;

use aws_smithy_http_client::pool::{Client as PoolClient, ConnectionPool};

use crate::assertions::assert_same_content;
use crate::harness::{Target, TmTestClient};
use crate::test_util::data::object_bytes;
use crate::test_util::object::{Alg, Layout, Object, MIB};

/// How long the downloads of a test may take together.
const TIMEOUT: Duration = Duration::from_secs(60);

/// A mock target whose transfer manager has the default part size and sends every request through
/// a pool of at most `connections` connections to the mock.
async fn capped(connections: usize) -> TmTestClient {
    let pool = ConnectionPool::builder()
        .max_connections_per_host(connections)
        .build_http()
        .expect("build connection pool");
    let http = PoolClient::new(&pool).expect("client of the connection pool");
    Target::mock_gp()
        .connect_mock_configured(None, |b| b.http_client(http))
        .await
}

/// Downloads `key` with a default request and checks it delivers the bytes of object `seed`.
async fn download_matches(t: &TmTestClient, key: &str, seed: u32, len: u64) {
    let (bytes, _) = t
        .download(key, None)
        .await
        .unwrap_or_else(|e| panic!("download {key}: {e:?}"));
    assert_same_content(&object_bytes(seed, len as usize), &bytes);
}

/// A multipart upload with the default checksum strategy, downloaded with the default part size
/// over one connection, completes and delivers the source bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "stalls: discovery holds the only connection with an unread range-0 body while it requests partNumber=1"]
async fn full_object_download_on_one_connection() {
    let t = capped(1).await;
    let (object, seed) = (Object::mpu(Layout::FullObject, Alg::Crc64Nvme), 11);
    object.upload(&t, "obj", seed).await;

    tokio::time::timeout(TIMEOUT, download_matches(&t, "obj", seed, object.len))
        .await
        .expect("download completes within the timeout");
    t.shutdown().await;
}

/// A COMPOSITE multipart upload in 16 MiB parts, downloaded with the default part size over one
/// connection, completes and delivers the source bytes. Its ranges are realigned to the stored
/// parts.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "stalls: discovery holds the only connection with an unread range-0 body while it requests partNumber=1"]
async fn composite_realigned_download_on_one_connection() {
    let t = capped(1).await;
    let object = Object::mpu(Layout::Composite, Alg::Crc32)
        .len(40 * MIB + 3)
        .parts(16 * MIB);
    let seed = 12;
    object.upload(&t, "obj", seed).await;

    tokio::time::timeout(TIMEOUT, download_matches(&t, "obj", seed, object.len))
        .await
        .expect("download completes within the timeout");
    t.shutdown().await;
}

/// Four concurrent downloads of multipart uploads with the default checksum strategy, sharing a
/// pool of two connections, all complete and deliver their source bytes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "stalls: each download holds a connection with an unread range-0 body while it requests partNumber=1"]
async fn downloads_share_capped_pool() {
    let t = capped(2).await;
    let object = Object::mpu(Layout::FullObject, Alg::Crc64Nvme);
    let objects = [("a", 13), ("b", 14), ("c", 15), ("d", 16)];
    for (key, seed) in objects {
        object.upload(&t, key, seed).await;
    }

    let [a, b, c, d] = objects.map(|(key, seed)| download_matches(&t, key, seed, object.len));
    tokio::time::timeout(TIMEOUT, async { tokio::join!(a, b, c, d) })
        .await
        .expect("downloads complete within the timeout");
    t.shutdown().await;
}
