/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3_transfer_manager::error::Error;
use aws_sdk_s3_transfer_manager::operation::download::DownloadHandle;
use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use s3_mock_server::S3MockServer;

/// drain/consume the body
pub async fn drain(handle: &mut DownloadHandle) -> Result<(), Error> {
    let body = handle.body_mut();
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(_chunk) => {}
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

fn download_throughput_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("download_throughput");

    // Test sizes relevant for multipart downloads (5MB minimum part size)
    let sizes = vec![
        ("5GB", 5 * 1024 * 1024 * 1024),
        ("10GB", 10 * 1024 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        // Benchmark S3 client directly
        group.bench_with_input(BenchmarkId::new("s3_client", name), &size, |b, &size| {
            b.iter_custom(|iters| {
                rt.block_on(async {
                    // Setup: Create server and upload object once
                    let mock_server = S3MockServer::builder()
                        .with_in_memory_store()
                        .build()
                        .unwrap();

                    let handle = mock_server.start().await.unwrap();
                    let client = handle.client().await;

                    let data = Bytes::from(vec![0u8; size]);
                    let _put_result = client
                        .put_object()
                        .bucket("test-bucket")
                        .key("test-key")
                        .body(data.into())
                        .send()
                        .await
                        .unwrap();

                    // Benchmark: Time only the downloads
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut resp = client
                            .get_object()
                            .bucket("test-bucket")
                            .key("test-key")
                            .send()
                            .await
                            .unwrap();

                        while let Some(chunk) = resp.body.next().await {
                            let _ = chunk.unwrap();
                        }
                    }
                    let elapsed = start.elapsed();

                    handle.shutdown().await.unwrap();
                    elapsed
                })
            });
        });

        // Benchmark Transfer Manager
        group.bench_with_input(
            BenchmarkId::new("transfer_manager", name),
            &size,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        // Setup: Create server and upload object once
                        let mock_server = S3MockServer::builder()
                            .with_in_memory_store()
                            .build()
                            .unwrap();

                        let handle = mock_server.start().await.unwrap();
                        let s3_client = handle.client().await;

                        let data = Bytes::from(vec![0u8; size]);
                        let _put_result = s3_client
                            .put_object()
                            .bucket("test-bucket")
                            .key("test-key")
                            .body(data.into())
                            .send()
                            .await
                            .unwrap();

                        // Create Transfer Manager
                        let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
                            .client(s3_client)
                            .build();
                        let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

                        // Benchmark: Time only the downloads
                        let start = std::time::Instant::now();
                        for _ in 0..iters {
                            let mut dl_handle = tm
                                .download()
                                .bucket("test-bucket")
                                .key("test-key")
                                .initiate()
                                .expect("successful transfer initiate");
                            drain(&mut dl_handle).await.unwrap();
                        }
                        let elapsed = start.elapsed();

                        handle.shutdown().await.unwrap();
                        elapsed
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, download_throughput_benchmark);
criterion_main!(benches);
