/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Custom-part-stream scheduling benchmarks.
//!
//! These scenarios exercise the public upload path through `PartStream`, the
//! transfer scheduler, HTTP requests, and the local S3 mock. They are intended
//! for Criterion baseline comparisons across implementations of custom-stream
//! serialization, not as an isolated measure of network throughput.
//!
//! Eight uploads start together so fixed client concurrency creates a cohort of
//! speculative part dispatches. An immediately ready source measures direct gate
//! handoff. An alternating source returns `Pending` once before every other part
//! and wakes itself, measuring retained-future requeue without external delay. A
//! delayed source waits before every part, measuring whether blocked custom
//! sources retain execution slots or accumulate waiters behind the source gate.
//!
//! Parts are intentionally smaller than S3's multipart service minimum. The
//! local mock accepts them so each sample emphasizes source scheduling rather
//! than bulk data movement; this is not a service-protocol throughput workload.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use aws_sdk_s3_transfer_manager::types::ConcurrencyMode;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use s3_mock_server::{S3MockServer, ServerHandle};
use tokio::time::Sleep;

const UPLOADS: usize = 8;
const PARTS_PER_UPLOAD: u64 = 16;
const PART_BYTES: usize = 16 * 1024;
const SOURCE_DELAY: Duration = Duration::from_millis(1);

/// Source readiness pattern applied independently to every produced part.
#[derive(Clone, Copy, Debug)]
enum Readiness {
    /// Every poll returns a part or end-of-stream immediately.
    Ready,
    /// Every other part first returns `Pending` after scheduling its own wake.
    Alternating,
    /// Every part waits for a timer before becoming ready.
    Delayed,
}

impl Readiness {
    fn name(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Alternating => "alternating",
            Self::Delayed => "delayed",
        }
    }
}

/// Produces a fixed number of equal-sized parts with controlled readiness.
#[derive(Debug)]
struct BenchmarkPartStream {
    readiness: Readiness,
    payload: Bytes,
    remaining_parts: u64,
    next_part_number: u64,
    yielded_pending: bool,
    delay: Option<Pin<Box<Sleep>>>,
}

impl BenchmarkPartStream {
    fn new(readiness: Readiness, payload: Bytes) -> Self {
        Self {
            readiness,
            payload,
            remaining_parts: PARTS_PER_UPLOAD,
            next_part_number: 1,
            yielded_pending: false,
            delay: None,
        }
    }

    fn next_part(&mut self) -> PartData {
        let part = PartData::new(self.next_part_number, self.payload.clone());
        self.next_part_number += 1;
        self.remaining_parts -= 1;
        self.yielded_pending = false;
        part
    }
}

impl PartStream for BenchmarkPartStream {
    fn poll_part(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _stream_cx: &StreamContext,
    ) -> Poll<Option<io::Result<PartData>>> {
        if self.remaining_parts == 0 {
            return Poll::Ready(None);
        }

        match self.readiness {
            Readiness::Ready => {}
            Readiness::Alternating
                if self.next_part_number.is_multiple_of(2) && !self.yielded_pending =>
            {
                self.yielded_pending = true;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Readiness::Alternating => {}
            Readiness::Delayed => {
                let delay = self
                    .delay
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(SOURCE_DELAY)));
                if delay.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.delay = None;
            }
        }

        Poll::Ready(Some(Ok(self.next_part())))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

/// Starts one mock endpoint and one transfer-manager client for a concurrency case.
async fn setup(
    concurrency: usize,
) -> (
    S3MockServer,
    ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("mock server should build");
    let handle = server.start().await.expect("mock server should start");
    let s3_client = handle.client().await;
    let config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .concurrency(ConcurrencyMode::Explicit(concurrency))
        .build();
    let client = aws_sdk_s3_transfer_manager::Client::new(config);
    (server, handle, client)
}

/// Runs one cohort of uploads through the complete public custom-stream path.
async fn run_cohort(
    client: &aws_sdk_s3_transfer_manager::Client,
    readiness: Readiness,
    payload: &Bytes,
) {
    let mut handles = Vec::with_capacity(UPLOADS);
    for upload in 0..UPLOADS {
        let stream = BenchmarkPartStream::new(readiness, payload.clone());
        let handle = client
            .upload()
            .bucket("benchmark-bucket")
            .key(format!("part-stream-{}-{upload}", readiness.name()))
            .body(InputStream::from_part_stream(stream))
            .initiate()
            .expect("benchmark upload should initiate");
        handles.push(handle);
    }

    for handle in handles {
        std::hint::black_box(
            handle
                .join()
                .await
                .expect("benchmark upload should complete"),
        );
    }
}

/// Measures ready handoff and blocked-source behavior as scheduler concurrency grows.
fn benchmark_part_stream_gate(c: &mut Criterion) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should start");
    let payload = Bytes::from(vec![0x5a; PART_BYTES]);
    let bytes_per_cohort = (UPLOADS as u64) * PARTS_PER_UPLOAD * (PART_BYTES as u64);
    let mut group = c.benchmark_group("upload/part_stream_gate");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(bytes_per_cohort));

    for concurrency in [1usize, 2, 8, 32, 64] {
        let (server, handle, client) = runtime.block_on(setup(concurrency));

        for readiness in [Readiness::Ready, Readiness::Alternating, Readiness::Delayed] {
            group.bench_function(BenchmarkId::new(readiness.name(), concurrency), |b| {
                b.iter_custom(|iterations| {
                    runtime.block_on(async {
                        let started = Instant::now();
                        for _ in 0..iterations {
                            run_cohort(&client, readiness, &payload).await;
                        }
                        started.elapsed()
                    })
                });
            });
        }

        runtime
            .block_on(handle.shutdown())
            .expect("mock server should stop");
        drop(server);
    }

    group.finish();
}

criterion_group!(benches, benchmark_part_stream_gate);
criterion_main!(benches);
