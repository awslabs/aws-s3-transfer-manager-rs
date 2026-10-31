/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Public composition, native routing, and runtime ownership contracts.

#![cfg(not(miri))]

use aws_sdk_s3_transfer_manager as tm;
use aws_sdk_s3_transfer_manager_ext as tm_ext;
use aws_smithy_execution::{ManagedRuntime, ManagedRuntimeHandle, topology::Topology};
use aws_smithy_http_client::test_util::wire::connection::{ConnectionTestHarness, EndpointPlan};
use aws_smithy_managed_http::client::{ManagedHttpPool, pool};
use aws_smithy_runtime_api::client::http::HttpClient;
use std::{
    future,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(5);

fn runtime() -> ManagedRuntime {
    ManagedRuntime::builder()
        .topology(Topology::detect().unwrap().with_max_placements(2))
        .pin_threads(false)
        .build()
        .unwrap()
}

async fn watch_owner(execution: &ManagedRuntimeHandle) -> tokio::task::JoinHandle<()> {
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = execution.partitions()[0].spawn(async move {
        started.send(()).unwrap();
        future::pending::<()>().await;
    });
    tokio::time::timeout(WAIT, ready).await.unwrap().unwrap();
    task
}

async fn assert_stopped(task: tokio::task::JoinHandle<()>) {
    assert!(
        tokio::time::timeout(WAIT, task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
}

#[tokio::test]
async fn unused_managed_values_and_configs_own_shutdown_despite_retained_http() {
    for use_config in [false, true] {
        let owner = runtime();
        let execution = owner.handle();
        let watch = watch_owner(&execution).await;
        let pool = ManagedHttpPool::builder()
            .pool_builder(pool::ConnectionPool::builder())
            .runtime(execution.clone())
            .build()
            .unwrap();
        let http = pool.clone();
        let managed = tm_ext::managed(owner);
        if use_config {
            let sdk = aws_types::SdkConfig::builder()
                .http_client(http.clone())
                .build();
            let config = tm::Config::builder()
                .s3_config(&sdk)
                .runtime_config(tm::RuntimeConfig::managed(managed))
                .build();
            drop(config);
        } else {
            drop(managed);
        }
        assert_stopped(watch).await;
        // Keep the HTTP view, native wrapper and execution handle alive above
        // the assertion: none may own the runtime's stop sender.
        assert!(http.connector_metadata().is_some());
        assert_eq!(pool.runtime_id(), execution.id());
    }
}

#[tokio::test]
async fn adopted_file_upload_uses_configured_native_pool_and_retains_runtime_until_completion() {
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use aws_smithy_http_client::test_util::wire::connection::{ManualGate, SocketScript};
    use std::io::Write;

    for cancel in [false, true] {
        let owner = runtime();
        let execution = owner.handle();
        let watch = watch_owner(&execution).await;
        let completion = owner.shutdown_completion();
        let gate = ManualGate::new();
        let payload = vec![0x5a; 128 * 1024];
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&payload).unwrap();
        let mut checksum = aws_smithy_checksums::ChecksumAlgorithm::Crc32.into_impl();
        checksum.update(&payload);
        let checksum = checksum.header_value().to_str().unwrap().to_owned();
        let harness = ConnectionTestHarness::builder()
            .endpoint(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                EndpointPlan::repeat_n(1, {
                    let script = SocketScript::new()
                        .read_until(b"\r\n\r\n", 64 * 1024)
                        .expect_bytes(&payload)
                        .wait(gate.waiter());
                    if cancel {
                        script.await_client_close()
                    } else {
                        script                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"test-etag\"\r\n\r\n",
                    )
.await_client_close()
                    }
                }),
            )
            .build()
            .await
            .unwrap();
        let opened = Arc::new(Mutex::new(Vec::new()));
        let observed = opened.clone();
        let driver_execution = execution.clone();
        let http = {
            let pool = ManagedHttpPool::builder()
                .pool_builder(
                    pool::ConnectionPool::builder()
                        .max_connections_per_host(1)
                        .event_listener(move |event: &pool::ConnectionEvent<'_>| {
                            if let pool::ConnectionEvent::Opened(event) = event {
                                observed
                                    .lock()
                                    .unwrap()
                                    .push(driver_execution.current_partition().unwrap().id());
                                assert_eq!(
                                    event.establishment().partition(),
                                    pool::PartitionId::from_index(
                                        driver_execution.current_partition().unwrap().id().index()
                                    ),
                                );
                            }
                        }),
                )
                .runtime(execution.clone())
                .build()
                .unwrap();
            pool.clone()
        };
        let sdk = aws_types::SdkConfig::builder()
            .http_client(http.clone())
            .region(aws_types::region::Region::new("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test", "test", None, None, "test",
            )))
            .endpoint_url(format!("http://127.0.0.1:{}", harness.port()))
            .behavior_version(
                aws_smithy_runtime_api::client::behavior_version::BehaviorVersion::latest(),
            )
            .build();
        let client = tm::Client::new(
            tm::Config::builder()
                .s3_config(&sdk)
                .runtime_config(tm::RuntimeConfig::managed(tm_ext::managed(owner)))
                .build(),
        );
        let stopped = client.shutdown_completion().unwrap();
        let upload = client
            .upload()
            .bucket("test-bucket")
            .key("file")
            .checksum_strategy(tm::operation::upload::ChecksumStrategy::with_crc32(
                &checksum,
            ))
            .body(tm::io::InputStream::from_path(file.path()).unwrap())
            .initiate()
            .unwrap();
        gate.wait_until_reached(WAIT).await.unwrap();
        drop(client);
        assert_eq!(
            execution.partitions()[0].spawn(async { 42 }).await.unwrap(),
            42,
            "an active transfer must retain the adopted owner",
        );
        if cancel {
            tokio::time::timeout(WAIT, upload.abort())
                .await
                .unwrap()
                .unwrap();
            gate.release();
        } else {
            gate.release();
            let output = tokio::time::timeout(WAIT, upload.join())
                .await
                .unwrap()
                .unwrap();
            assert!(
                output.upload_id().is_none(),
                "exercise file-backed single PUT"
            );
        }
        assert_eq!(harness.tcp_accepted_count(), 1);
        {
            let observed = opened.lock().unwrap();
            assert_eq!(
                observed.len(),
                1,
                "the configured native pool handled the upload"
            );
            assert_eq!(observed[0].runtime_id(), execution.id());
        }

        // Scheduler descriptors are retired through the existing global epoch
        // collector. Advance collection before asserting final owner destruction.
        tokio::time::timeout(WAIT, async {
            let retired = crossbeam_skiplist::SkipMap::new();
            while !watch.is_finished() {
                for index in 0..256 {
                    retired.insert(index, ());
                    retired.remove(&index);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_stopped(watch).await;
        tokio::time::timeout(WAIT, stopped.wait())
            .await
            .unwrap()
            .unwrap();
        completion.wait().await.unwrap();
        assert!(
            http.connector_metadata().is_some(),
            "the HTTP view remains alive"
        );
        harness.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn automatic_managed_completion_does_not_retain_client_clones() {
    let sdk = aws_types::SdkConfig::builder()
        .behavior_version(
            aws_smithy_runtime_api::client::behavior_version::BehaviorVersion::latest(),
        )
        .build();
    let client = tm::Client::new(tm::Config::builder().s3_config(&sdk).build());
    let completion = client
        .shutdown_completion()
        .expect("default execution is managed");
    let other = completion.clone();
    let retained = client.clone();
    drop(client);
    {
        let mut waiting = Box::pin(completion.wait());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
    }
    drop(retained);
    tokio::time::timeout(WAIT, completion.wait())
        .await
        .unwrap()
        .unwrap();
    other.wait().await.unwrap();
    completion.wait().await.unwrap();
}

#[tokio::test]
async fn caller_tokio_execution_has_no_owned_runtime_completion() {
    let sdk = aws_types::SdkConfig::builder()
        .behavior_version(
            aws_smithy_runtime_api::client::behavior_version::BehaviorVersion::latest(),
        )
        .build();
    let client = tm::Client::new(
        tm::Config::builder()
            .s3_config(&sdk)
            .runtime_config(tm::RuntimeConfig::multi_thread_tokio())
            .build(),
    );
    assert!(client.shutdown_completion().is_none());
}

#[tokio::test]
async fn adopted_completion_waits_for_blocking_work_and_thread_local_cleanup() {
    use test_util::{DropSignal, THREAD_EXIT};
    let owner = runtime();
    let completion = owner.shutdown_completion();
    let execution = owner.handle();
    let partition = execution.partitions()[0].clone();
    let (started, ready) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let (exited, exit) = std::sync::mpsc::channel();
    drop(partition.spawn(async move {
        THREAD_EXIT.with(|value| *value.borrow_mut() = Some(DropSignal(exited)));
        tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = released.recv();
        })
        .await
        .unwrap();
    }));
    ready.recv_timeout(WAIT).unwrap();
    let sdk = aws_types::SdkConfig::builder()
        .behavior_version(
            aws_smithy_runtime_api::client::behavior_version::BehaviorVersion::latest(),
        )
        .build();
    let client = tm::Client::new(
        tm::Config::builder()
            .s3_config(&sdk)
            .runtime_config(tm::RuntimeConfig::managed(tm_ext::managed(owner)))
            .build(),
    );
    let stopped = client.shutdown_completion().unwrap();
    let self_wait = stopped.clone();
    let error = partition
        .spawn(async move { self_wait.wait().await })
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), tm::ShutdownErrorKind::FromRuntime);
    drop(client);
    {
        let mut waiting = Box::pin(stopped.wait());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(waiting.as_mut(), &mut context).is_pending());
    }
    drop(release);
    tokio::time::timeout(WAIT, stopped.wait())
        .await
        .unwrap()
        .unwrap();
    completion.wait().await.unwrap();
    exit.try_recv().unwrap();
    assert!(partition.spawn(async {}).await.unwrap_err().is_cancelled());
}

mod test_util {
    use std::{cell::RefCell, sync::mpsc};

    pub(super) struct DropSignal(pub(super) mpsc::Sender<()>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    thread_local! {
        pub(super) static THREAD_EXIT: RefCell<Option<DropSignal>> = const { RefCell::new(None) };
    }
}
