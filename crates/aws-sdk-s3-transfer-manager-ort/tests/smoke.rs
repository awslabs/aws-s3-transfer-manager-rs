/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Public composition, native routing, and runtime ownership contracts.

#![cfg(not(miri))]

use aws_ort_execution::{ManagedRuntime, ManagedRuntimeHandle, topology::Topology};
use aws_ort_http::client::{ManagedHttpPool, pool};
use aws_sdk_s3_transfer_manager as tm;
use aws_sdk_s3_transfer_manager_ort as ort_tm;
use aws_smithy_http_client::test_util::wire::connection::{
    ConnectionTestHarness, EndpointPlan, Http1Response, Http1Script,
};
use aws_smithy_runtime_api::client::{
    http::{HttpClient, HttpConnector, HttpConnectorSettings},
    orchestrator::HttpRequest,
    runtime_components::RuntimeComponentsBuilder,
};
use aws_smithy_types::{body::SdkBody, config_bag::ConfigBag};
use std::{
    error::Error,
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
async fn mismatched_pool_is_rejected_before_client_construction() {
    let owner = runtime();
    let foreign = runtime();
    let execution = owner.handle();
    let pool = ManagedHttpPool::builder()
        .pool_builder(pool::ConnectionPool::builder())
        .runtime(execution.clone())
        .build()
        .unwrap();
    // Indices are equal, but the containing runtimes are distinct.
    assert_eq!(
        execution.partitions()[0].id().index(),
        foreign.handle().partitions()[0].id().index()
    );
    let error = ort_tm::http_client(foreign.handle(), &pool).unwrap_err();
    assert!(error.to_string().contains(&execution.id().to_string()));
    assert!(
        error
            .to_string()
            .contains(&foreign.handle().id().to_string())
    );
    assert!(error.source().is_none());
    owner.shutdown().await.unwrap();
    foreign.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_foreign_and_outside_requests_retain_native_configuration_and_driver_placement() {
    let owner = runtime();
    let foreign = runtime();
    let execution = owner.handle();
    let observed_execution = execution.clone();
    let opened = Arc::new(Mutex::new(Vec::new()));
    let observed = opened.clone();
    let http = {
        let pool = ManagedHttpPool::builder()
            .pool_builder(
                pool::ConnectionPool::builder()
                    .max_connections_per_host(3)
                    .event_listener(move |event: &pool::ConnectionEvent<'_>| {
                        if let pool::ConnectionEvent::Opened(event) = event {
                            observed.lock().unwrap().push((
                                event.establishment().partition(),
                                observed_execution.current_partition().unwrap().id(),
                            ));
                        }
                    }),
            )
            .runtime(execution.clone())
            .build()
            .unwrap();
        ort_tm::http_client(execution.clone(), &pool).unwrap()
        // Dropping this wrapper must preserve the native clients' shared pool.
    };
    assert!(http.connector_metadata().is_some());
    http.validate_base_client_config(&RuntimeComponentsBuilder::for_tests(), &ConfigBag::base())
        .unwrap();

    let components = RuntimeComponentsBuilder::for_tests().build().unwrap();
    let settings = HttpConnectorSettings::builder().build();
    for index in 0..execution.partitions().len() + 2 {
        // Fresh origins prevent connection reuse from masking client selection.
        let harness = ConnectionTestHarness::builder()
            .endpoint(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                EndpointPlan::repeat_n(1, Http1Script::responses([Http1Response::ok()])),
            )
            .build()
            .await
            .unwrap();
        let mut request = HttpRequest::new(SdkBody::empty());
        request
            .set_uri(format!("http://127.0.0.1:{}/", harness.port()))
            .unwrap();
        let http = http.clone();
        let settings = settings.clone();
        let components = components.clone();
        let send = async move {
            tokio::time::timeout(
                WAIT,
                http.http_connector(&settings, &components).call(request),
            )
            .await
            .unwrap()
            .unwrap()
        };
        let response = if let Some(partition) = execution.partitions().get(index) {
            partition.spawn(send).await.unwrap()
        } else if index == execution.partitions().len() {
            foreign.handle().partitions()[0].spawn(send).await.unwrap()
        } else {
            send.await
        };
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(harness.tcp_accepted_count(), 1);
        let (native, driver) = opened.lock().unwrap()[index];
        assert_eq!(driver.runtime_id(), execution.id());
        assert_eq!(native, pool::PartitionId::from_index(driver.index()));
        if index < execution.partitions().len() {
            assert_eq!(driver, execution.partitions()[index].id());
        }
        drop(response);
        harness.shutdown().await.unwrap();
    }
    owner.shutdown().await.unwrap();
    foreign.shutdown().await.unwrap();
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
        let http = ort_tm::http_client(execution.clone(), &pool).unwrap();
        let managed = ort_tm::managed(owner);
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

    let owner = runtime();
    let execution = owner.handle();
    let watch = watch_owner(&execution).await;
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
            EndpointPlan::repeat_n(
                1,
                SocketScript::new()
                    .read_until(b"\r\n\r\n", 64 * 1024)
                    .expect_bytes(&payload)
                    .wait(gate.waiter())
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"test-etag\"\r\n\r\n",
                    )
                    .await_client_close(),
            ),
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
        ort_tm::http_client(execution.clone(), &pool).unwrap()
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
            .runtime_config(tm::RuntimeConfig::managed(ort_tm::managed(owner)))
            .build(),
    );
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
    gate.release();
    let output = tokio::time::timeout(WAIT, upload.join())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.upload_id().is_none(),
        "exercise file-backed single PUT"
    );
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
    assert!(
        http.connector_metadata().is_some(),
        "the HTTP view remains alive"
    );
    harness.shutdown().await.unwrap();
}
