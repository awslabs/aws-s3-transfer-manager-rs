/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Integration contracts at the managed/native pool boundary.

#![cfg(feature = "client")]

use aws_smithy_async::rt::sleep::{SharedAsyncSleep, TokioSleep};
use aws_smithy_execution::{
    ManagedRuntime, ManagedRuntimeHandle, RuntimePartitionId, topology::Topology,
};
use aws_smithy_http_client::test_util::wire::connection::{
    ConnectionTestHarness, EndpointPlan, Http1Response, Http1Script, MockDnsResolver,
};
use aws_smithy_managed_http::client::{BuildErrorKind, Builder, ManagedHttpPool, pool};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorSettings, SharedHttpClient,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponentsBuilder;
use aws_smithy_runtime_api::shared::IntoShared;
use aws_smithy_types::{body::SdkBody, config_bag::ConfigBag};
use http_body_util::BodyExt;
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
    thread,
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

fn caller() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn missing_inputs_and_caller_partitions_are_rejected() {
    let runtime = runtime();
    let execution = runtime.handle();
    let error = ManagedHttpPool::builder()
        .pool_builder(pool::ConnectionPool::builder())
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), BuildErrorKind::MissingRuntime);
    assert_eq!(error.partition(), None);
    assert!(error.source().is_none());
    assert!(error.to_string().contains("runtime handle is required"));

    let mut builder: Builder<pool::Builder> = Builder::default();
    builder.set_runtime(Some(execution.clone()));
    let error = builder.build().unwrap_err();
    assert_eq!(error.kind(), BuildErrorKind::MissingPoolBuilder);
    assert_eq!(error.partition(), None);
    assert!(error.source().is_none());
    assert!(error.to_string().contains("pool builder is required"));

    // Both empty and nonempty declarations belong to the caller; neither may
    // disappear when managed construction supplies its own placement.
    for partitions in [
        Vec::new(),
        vec![pool::Partition::new(
            pool::PartitionId::from_index(9),
            pool::DriverSpawner::from_fn(drop),
        )],
    ] {
        let error = ManagedHttpPool::builder()
            .runtime(execution.clone())
            .pool_builder(pool::ConnectionPool::builder().partitions(partitions))
            .build()
            .unwrap_err();
        assert_eq!(error.kind(), BuildErrorKind::ExplicitPartitions);
        assert_eq!(error.partition(), None);
        assert!(error.to_string().contains("native partitions conflict"));
        assert!(error.source().is_none());
    }

    let mut transport = pool::ConnectionPool::builder().partitions([]);
    transport.set_partitions(None);
    let http = ManagedHttpPool::builder()
        .pool_builder(transport)
        .runtime(execution.clone())
        .build()
        .unwrap();
    assert!(
        http.client_for_partition(execution.partitions()[0].id())
            .is_ok()
    );
    caller().block_on(runtime.shutdown()).unwrap();
}

#[test]
fn native_validation_errors_keep_their_sources() {
    let runtime = runtime();
    let error = ManagedHttpPool::builder()
        .pool_builder(pool::ConnectionPool::builder().max_connections_per_host(0))
        .runtime(runtime.handle())
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), BuildErrorKind::Pool);
    assert_eq!(error.partition(), None);
    assert!(error.source().unwrap().is::<pool::BuildError>());
    assert!(
        error
            .source()
            .unwrap()
            .to_string()
            .contains("max_connections_per_host")
    );
    caller().block_on(runtime.shutdown()).unwrap();
}

#[test]
fn lookup_uses_runtime_scoped_identity_and_returns_native_clients() {
    let first = runtime();
    let second = runtime();
    let execution = first.handle();
    let foreign = second.handle();
    let http = ManagedHttpPool::builder()
        .pool_builder(pool::ConnectionPool::builder())
        .runtime(execution.clone())
        .build()
        .unwrap();
    assert_eq!(http.runtime_id(), execution.id());
    for partition in execution.partitions() {
        let client: pool::Client = http.client_for_partition(partition.id()).unwrap();
        let shared: SharedHttpClient = client.clone().into_shared();
        assert!(shared.connector_metadata().is_some());
    }
    let requested = foreign.partitions()[0].id();
    assert_eq!(requested.index(), execution.partitions()[0].id().index());
    let error = http.client_for_partition(requested).unwrap_err();
    assert_eq!(error.partition(), requested);
    assert_eq!(error.runtime_id(), execution.id());
    caller().block_on(first.shutdown()).unwrap();
    caller().block_on(second.shutdown()).unwrap();
}

#[test]
fn https_builder_uses_the_same_managed_construction_and_native_validation() {
    use aws_smithy_http_client::tls;
    let runtime = runtime();
    let crypto = tls::rustls_provider::CryptoMode::Ring;

    let native: pool::HttpsBuilder =
        pool::ConnectionPool::builder().tls_provider(tls::Provider::rustls(crypto.clone()));
    let http = ManagedHttpPool::builder()
        .pool_builder(native)
        .runtime(runtime.handle())
        .build()
        .unwrap();
    let id = runtime.handle().partitions()[0].id();
    assert!(http.client_for_partition(id).is_ok());
    let error = ManagedHttpPool::builder()
        .pool_builder(
            pool::ConnectionPool::builder()
                .max_connections_per_host(0)
                .tls_provider(tls::Provider::rustls(crypto)),
        )
        .runtime(runtime.handle())
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), BuildErrorKind::Pool);
    assert_eq!(error.partition(), None);
    assert!(error.source().unwrap().is::<pool::BuildError>());
    caller().block_on(runtime.shutdown()).unwrap();
}

#[derive(Debug)]
struct Resolver {
    inner: MockDnsResolver,
    execution: ManagedRuntimeHandle,
    observations: Arc<Mutex<Vec<(String, RuntimePartitionId, thread::ThreadId)>>>,
}

impl ResolveDns for Resolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move {
            let partition = self
                .execution
                .current_partition()
                .expect("DNS on managed partition")
                .id();
            self.observations.lock().unwrap().push((
                name.to_owned(),
                partition,
                thread::current().id(),
            ));
            self.inner.resolve_dns(name).await
        })
    }
}

fn build_managed_pool(
    transport: pool::Builder,
    execution: ManagedRuntimeHandle,
) -> ManagedHttpPool {
    ManagedHttpPool::builder()
        .pool_builder(transport)
        .runtime(execution)
        .build()
        .unwrap()
}

fn build_manual_pool(transport: pool::Builder, execution: ManagedRuntimeHandle) -> ManagedHttpPool {
    let partitions = execution.partitions().iter().map(|target| {
        let id = pool::PartitionId::from_index(target.id().index());
        let target = target.clone();
        let partition = pool::Partition::new(
            id,
            pool::DriverSpawner::from_fn(move |task| {
                let handle = target.spawn(task);
                drop(handle);
            }),
        );
        #[cfg(target_os = "linux")]
        let partition = partition.interface("lo");
        #[cfg(target_os = "macos")]
        let partition = partition.interface("lo0");
        partition
    });
    let native = transport.partitions(partitions).build_http().unwrap();
    ManagedHttpPool::from_pool(execution, native).unwrap()
}

fn exercise_http(
    runtime: &ManagedRuntime,
    build: fn(pool::Builder, ManagedRuntimeHandle) -> ManagedHttpPool,
) {
    let execution = runtime.handle();
    let awaiting = caller();
    let harnesses = awaiting.block_on(async {
        let mut harnesses = Vec::new();
        for _ in execution.partitions() {
            let harness = ConnectionTestHarness::builder()
                .endpoint(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    EndpointPlan::repeat_n(
                        1,
                        Http1Script::responses([
                            Http1Response::ok().body("ok"),
                            Http1Response::ok().body("ok"),
                        ]),
                    ),
                )
                .dns_all("managed.invalid")
                .build()
                .await
                .unwrap();
            harnesses.push(harness);
        }
        harnesses
    });
    let observations = Arc::new(Mutex::new(Vec::new()));
    let opened = Arc::new(Mutex::new(Vec::new()));
    let capture = opened.clone();
    let transport = pool::ConnectionPool::builder()
        .idle_timeout(None)
        .max_connections_per_host(1)
        .dns_resolver(Resolver {
            inner: harnesses[0].dns_resolver(),
            execution: execution.clone(),
            observations: observations.clone(),
        })
        .event_listener(move |event: &pool::ConnectionEvent<'_>| {
            if let pool::ConnectionEvent::Opened(event) = event {
                capture
                    .lock()
                    .unwrap()
                    .push(event.establishment().partition());
            }
        });
    let http = build(transport, execution.clone());
    let components = RuntimeComponentsBuilder::for_tests()
        .with_sleep_impl(Some(SharedAsyncSleep::new(TokioSleep::new())))
        .build()
        .unwrap();
    let settings = HttpConnectorSettings::builder()
        .connect_timeout(WAIT)
        .read_timeout(WAIT)
        .build();
    let caller_thread = thread::current().id();
    for (partition, harness) in execution.partitions().iter().zip(&harnesses) {
        let expected = partition.id();
        let first: SharedHttpClient = http.client_for_partition(expected).unwrap().into_shared();
        let second: SharedHttpClient = http.client_for_partition(expected).unwrap().into_shared();
        // A separate origin exercises every target without peer reuse masking
        // the establishment mapping. Each origin still reuses its own connection.
        let port = harness.port();
        awaiting.block_on(async {
            for client in [first, second] {
                let connector = client.http_connector(&settings, &components);
                let mut request = HttpRequest::new(SdkBody::empty());
                request
                    .set_uri(format!("http://managed.invalid:{port}/"))
                    .unwrap();
                let response = tokio::time::timeout(WAIT, connector.call(request))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(response.status().as_u16(), 200);
                let body = response.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(body.as_ref(), b"ok");
            }
        });
        assert_eq!(harness.tcp_accepted_count(), 1);
        assert_eq!(
            harness.http_requests(),
            vec![("/".to_owned(), Some(format!("managed.invalid:{port}"))); 2],
        );
    }
    let observations = observations.lock().unwrap();
    assert_eq!(
        observations.len(),
        execution.partitions().len(),
        "native clients share one reused connection per origin"
    );
    let mut threads = std::collections::HashSet::new();
    for (observation, partition) in observations.iter().zip(execution.partitions()) {
        assert_eq!(
            observation.0, "managed.invalid",
            "native DNS configuration is retained"
        );
        assert_eq!(observation.1, partition.id());
        assert_ne!(observation.2, caller_thread);
        threads.insert(observation.2);
    }
    assert_eq!(threads.len(), execution.partitions().len());
    assert_eq!(
        *opened.lock().unwrap(),
        execution
            .partitions()
            .iter()
            .map(|partition| pool::PartitionId::from_index(partition.id().index()))
            .collect::<Vec<_>>()
    );
    drop(observations);
    drop(http);
    awaiting.block_on(async {
        for harness in harnesses {
            harness.shutdown().await.unwrap();
        }
    });
}

#[test]
fn native_configuration_reuse_and_driver_placement_work_from_another_executor() {
    let runtime = runtime();
    exercise_http(&runtime, build_managed_pool);
    caller().block_on(runtime.shutdown()).unwrap();
}

#[test]
fn manual_pool_preserves_interface_configuration_reuse_and_driver_placement() {
    let runtime = runtime();
    exercise_http(&runtime, build_manual_pool);
    caller().block_on(runtime.shutdown()).unwrap();
}

#[test]
fn manual_pool_rejects_missing_execution_partitions() {
    let runtime = runtime();
    let execution = runtime.handle();
    let native = pool::ConnectionPool::builder()
        .partitions([pool::Partition::new(
            pool::PartitionId::from_index(execution.partitions().len()),
            pool::DriverSpawner::from_fn(drop),
        )])
        .build_http()
        .unwrap();
    let error = ManagedHttpPool::from_pool(execution.clone(), native).unwrap_err();
    assert_eq!(error.kind(), BuildErrorKind::Client);
    assert_eq!(error.partition(), Some(execution.partitions()[0].id()));
    let missing = error
        .source()
        .unwrap()
        .downcast_ref::<pool::ClientBuildError>()
        .unwrap();
    assert_eq!(missing.partition(), Some(pool::PartitionId::from_index(0)));
    assert!(
        error
            .to_string()
            .contains(&execution.partitions()[0].id().to_string())
    );
    caller().block_on(runtime.shutdown()).unwrap();
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
        pool.clone()
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

#[cfg(feature = "dial9")]
mod tracing {
    use super::*;
    use dial9::{
        MemoryBuffer,
        core::pipeline::{ProcessError, SegmentData, SegmentProcessor},
        format::{Decoder, types::FieldValueRef},
        recorder,
    };
    use std::{future::Future, pin::Pin};

    struct Capture(Arc<Mutex<Vec<Vec<u8>>>>);

    impl SegmentProcessor for Capture {
        fn name(&self) -> &'static str {
            "capture"
        }

        fn process(
            &mut self,
            data: SegmentData,
        ) -> Pin<Box<dyn Future<Output = Result<SegmentData, ProcessError>> + Send + '_>> {
            self.0
                .lock()
                .unwrap()
                .push(data.payload().clone().into_vec());
            Box::pin(async move { Ok(data) })
        }
    }

    #[test]
    fn pool_tasks_use_the_instrumented_partition_submission_path() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let recorder = recorder(MemoryBuffer::new(1 << 20).unwrap())
            .pipe(Capture(captured.clone()))
            .build();
        let runtime = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .pin_threads(false)
            .dial9_handle(recorder.handle().clone())
            .build()
            .unwrap();
        exercise_http(&runtime, build_managed_pool);
        exercise_http(&runtime, build_manual_pool);
        caller().block_on(runtime.shutdown()).unwrap();
        recorder.graceful_shutdown(WAIT);
        let mut managed_polls = 0;
        let mut manual_polls = 0;
        for bytes in captured.lock().unwrap().iter() {
            Decoder::new(bytes)
                .unwrap()
                .for_each_event(|event| {
                    if event.name == "PollStartEvent" {
                        for (name, field) in event.field_names().zip(event.fields) {
                            if let ("spawn_loc", FieldValueRef::PooledString(id)) = (name, field)
                                && let Some(location) = event.string_pool.get(*id)
                                && location.contains("aws-smithy-managed-http")
                            {
                                if location.contains("builder.rs") {
                                    managed_polls += 1;
                                } else if location.contains("smoke.rs") {
                                    manual_polls += 1;
                                }
                            }
                        }
                    }
                })
                .unwrap();
        }
        assert!(
            managed_polls > 0,
            "generated partition tasks were not wrapped by partition.spawn"
        );
        assert!(
            manual_polls > 0,
            "manual partition tasks were not wrapped by partition.spawn"
        );
    }
}
