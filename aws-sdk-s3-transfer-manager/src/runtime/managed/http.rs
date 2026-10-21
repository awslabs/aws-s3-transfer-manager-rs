/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! HTTP connector routing over one native pool.

use aws_smithy_execution::ManagedRuntimeHandle;
use aws_smithy_http_client::pool;
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        connector_metadata::ConnectorMetadata,
        http::{HttpClient, HttpConnectorSettings, SharedHttpConnector},
        runtime_components::{
            RuntimeComponents as SmithyRuntimeComponents, RuntimeComponentsBuilder,
        },
    },
};
use aws_smithy_types::config_bag::ConfigBag;
use std::sync::Arc;

/// HTTP client that routes each operation to the calling managed thread's
/// connection pool partition.
///
/// The SDK client holds a single [`HttpClient`], but the pool binds each
/// [`pool::Client`] to one partition whose connections and protocol drivers
/// live on one managed thread's runtime. Smithy requests an HTTP connector
/// from the task executing the operation. The execution handle identifies
/// this runtime's partition associated with the calling managed thread.
///
/// Some operations are sent from outside the managed threads, for example
/// [`UploadHandle::abort`](crate::operation::upload::UploadHandle::abort)
/// runs on the caller's runtime. Those requests use a randomly selected
/// partition: establishment and protocol drivers still run on that partition's
/// thread, and the calling task only awaits the response.
#[derive(Debug, Clone)]
pub(super) struct ManagedHttpClient {
    execution: ManagedRuntimeHandle,
    /// One client per managed thread, indexed by thread (and partition) index.
    clients: Arc<[pool::Client]>,
}

impl HttpClient for ManagedHttpClient {
    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Transport preflight builds and caches the TLS connectors (loading
        // native trust roots) so that work happens here, when the S3 client is
        // built, instead of on a managed thread's first connection. It needs no
        // runtime context. Connectors are cached by interface binding, so
        // visiting every partition covers each distinct interface; unbound
        // partitions share one connector and repeat visits are cache hits.
        self.clients
            .iter()
            .try_for_each(|client| client.validate_base_client_config(runtime_components, cfg))
    }

    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &SmithyRuntimeComponents,
    ) -> SharedHttpConnector {
        let index = match self.execution.current_partition() {
            Some(partition) => partition.id().index(),
            None => fastrand::usize(..self.clients.len()),
        };
        self.clients[index].http_connector(settings, components)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.clients.first()?.connector_metadata()
    }
}

impl ManagedHttpClient {
    pub(super) fn new(execution: ManagedRuntimeHandle, clients: Arc<[pool::Client]>) -> Self {
        assert_eq!(execution.partitions().len(), clients.len());
        Self { execution, clients }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use super::*;
    use aws_smithy_execution::{topology::Topology, ManagedRuntime};
    use aws_smithy_http_client::pool::{ConnectionPool, DriverSpawner, Partition, PartitionId};
    use aws_smithy_http_client::test_util::wire::connection::{
        ConnectionTestHarness, EndpointPlan, Http1Response, Http1Script,
    };
    use aws_smithy_runtime_api::client::http::HttpConnector;
    use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
    use aws_smithy_types::body::SdkBody;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Mutex;
    use std::time::Duration;

    #[tokio::test]
    async fn local_foreign_and_outside_requests_use_live_owned_partitions() {
        let owner = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(2))
            .pin_threads(false)
            .build()
            .unwrap();
        let foreign = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .pin_threads(false)
            .build()
            .unwrap();
        let execution = owner.handle();
        let opened = Arc::new(Mutex::new(Vec::new()));
        let observed = opened.clone();
        let pool = ConnectionPool::builder()
            .partitions(execution.partitions().iter().map(|target| {
                let id = PartitionId::from_index(target.id().index());
                let target = target.clone();
                Partition::new(
                    id,
                    DriverSpawner::from_fn(move |driver| drop(target.spawn(driver))),
                )
            }))
            .event_listener(move |event: &pool::ConnectionEvent<'_>| {
                if let pool::ConnectionEvent::Opened(event) = event {
                    observed
                        .lock()
                        .unwrap()
                        .push(event.establishment().partition());
                }
            })
            .build_http()
            .unwrap();
        let clients = execution
            .partitions()
            .iter()
            .map(|target| {
                pool::Client::from_partition(&pool, PartitionId::from_index(target.id().index()))
                    .unwrap()
            })
            .collect();
        let client = ManagedHttpClient::new(execution.clone(), clients);
        let components = RuntimeComponentsBuilder::for_tests().build().unwrap();
        let settings = HttpConnectorSettings::builder().build();

        // Each request uses a fresh origin so peer reuse cannot mask routing.
        for index in 0..execution.partitions().len() + 2 {
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
            let client = client.clone();
            let settings = settings.clone();
            let components = components.clone();
            let send = async move {
                // Connector selection runs in the request's execution context.
                tokio::time::timeout(
                    Duration::from_secs(5),
                    client.http_connector(&settings, &components).call(request),
                )
                .await
                .unwrap()
                .unwrap()
            };
            let response = if let Some(partition) = execution.partitions().get(index) {
                partition.spawn(send).await.unwrap()
            } else if index == execution.partitions().len() {
                assert!(execution.current_partition().is_none());
                foreign.handle().partitions()[0].spawn(send).await.unwrap()
            } else {
                send.await
            };
            assert_eq!(response.status().as_u16(), 200);
            assert_eq!(harness.tcp_accepted_count(), 1);
            let selected = opened.lock().unwrap()[index];
            if index < execution.partitions().len() {
                assert_eq!(selected, PartitionId::from_index(index));
            } else {
                assert!((0..execution.partitions().len())
                    .any(|index| selected == PartitionId::from_index(index)));
            }
            drop(response);
            harness.shutdown().await.unwrap();
        }
        owner.shutdown().await.unwrap();
        foreign.shutdown().await.unwrap();
    }
}
