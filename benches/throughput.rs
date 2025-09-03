/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3_transfer_manager::error::Error;
use aws_sdk_s3_transfer_manager::operation::download::{Body, DownloadHandle};
use bytes::Bytes;
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use s3_mock_server::S3MockServer;
use tokio::fs;
use tokio::io::AsyncWriteExt;

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

/// write body to file
pub async fn write_body(
    body: &mut Body,
    mut dest: fs::File,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while let Some(chunk) = body.next().await {
        let chunk = chunk?.data;
        for segment in chunk.into_segments() {
            dest.write_all(segment.as_ref()).await?;
        }
    }
    Ok(())
}

/// Setup shared test infrastructure
async fn setup_test(
    size: usize,
) -> (
    S3MockServer,
    s3_mock_server::ServerHandle,
    aws_sdk_s3_transfer_manager::Client,
) {
    let mock_server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .unwrap();

    let data = Bytes::from(vec![0u8; size]);
    mock_server
        .add_object("test-key", data, None)
        .await
        .unwrap();

    let handle = mock_server.start().await.unwrap();
    let s3_client = handle.client().await;

    let tm_config = aws_sdk_s3_transfer_manager::Config::builder()
        .client(s3_client)
        .build();
    let tm = aws_sdk_s3_transfer_manager::Client::new(tm_config);

    (mock_server, handle, tm)
}

fn download_throughput_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("download_throughput");
    group.sample_size(10);

    // Test sizes relevant for multipart downloads (5MB minimum part size)
    let sizes = vec![
        ("5GB", 5 * 1024 * 1024 * 1024),
        ("10GB", 10 * 1024 * 1024 * 1024),
    ];

    for (name, size) in sizes {
        group.throughput(Throughput::Bytes(size as u64));

        // Benchmark Transfer Manager - drain to memory
        group.bench_with_input(
            BenchmarkId::new("transfer_manager_ram", name),
            &size,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_mock_server, handle, tm) = setup_test(size).await;

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

        // Benchmark Transfer Manager - write to file
        group.bench_with_input(
            BenchmarkId::new("transfer_manager_tmpfs", name),
            &size,
            |b, &size| {
                b.iter_custom(|iters| {
                    rt.block_on(async {
                        let (_mock_server, handle, tm) = setup_test(size).await;

                        let mut tmp_files = Vec::new();
                        let start = std::time::Instant::now();
                        for i in 0..iters {
                            let mut dl_handle = tm
                                .download()
                                .bucket("test-bucket")
                                .key("test-key")
                                .initiate()
                                .expect("successful transfer initiate");

                            let temp_file = format!("/tmp/benchmark_output_{}.dat", i);
                            tmp_files.push(temp_file.clone());
                            let file = fs::File::create(&temp_file).await.unwrap();
                            write_body(dl_handle.body_mut(), file).await.unwrap();
                        }
                        let elapsed = start.elapsed();

                        // cleanup
                        for temp_file in tmp_files {
                            let _ = fs::remove_file(&temp_file).await;
                        }
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

fn main() {
    tracing_subscriber::fmt::init();
    benches();

    Criterion::default().configure_from_args().final_summary();
}
