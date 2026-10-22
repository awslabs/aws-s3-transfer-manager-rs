/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{BuildError, Builder, PartitionError};
use aws_smithy_execution::{ManagedRuntimeHandle, RuntimeId, RuntimePartitionId};
use aws_smithy_http_client::pool;
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        connector_metadata::ConnectorMetadata,
        http::{HttpClient, HttpConnectorSettings, SharedHttpConnector},
        runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
    },
};
use aws_smithy_types::config_bag::ConfigBag;
use std::sync::Arc;

/// One native connection pool with clients bound to managed execution partitions.
///
/// Construction resolves each native client once. Clones share those clients,
/// connections, and transport policy. The runtime owner remains with the caller.
///
/// This pool implements Smithy's [`HttpClient`]. Connector selection uses the
/// current execution partition when it belongs to this runtime, or a random
/// partition for callers outside this runtime. Native connection reuse may
/// borrow another partition's connection; client selection does not guarantee
/// request-body polling on the selected partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ManagedHttpPool {
    runtime: ManagedRuntimeHandle,
    // Execution guarantees that each partition index equals its slice position.
    clients: Arc<[pool::Client]>,
}

impl ManagedHttpPool {
    /// Returns an empty builder awaiting native configuration and a runtime.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Returns the identity of the runtime used for connection placement.
    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime.id()
    }

    /// Returns a native client for the explicitly chosen execution partition.
    ///
    /// The returned client is a cheap clone sharing this pool's connections.
    /// Lookup does not submit an SDK operation or select another partition.
    ///
    /// # Errors
    ///
    /// Returns [`PartitionError`] if the partition does not belong to this pool's
    /// runtime. Equal indices in different runtimes remain distinct.
    pub fn client_for_partition(
        &self,
        id: RuntimePartitionId,
    ) -> Result<pool::Client, PartitionError> {
        if id.runtime_id() != self.runtime.id() {
            return Err(PartitionError::new(id, self.runtime.id()));
        }
        self.clients
            .get(id.index())
            .cloned()
            .ok_or_else(|| PartitionError::new(id, self.runtime.id()))
    }

    /// Creates a managed HTTP view over an already configured native pool.
    ///
    /// Each execution partition must have a native partition with the same
    /// index. Configure the native driver spawners to submit work to the
    /// corresponding execution partitions. This constructor checks that the
    /// expected native partition IDs exist; it cannot inspect the runtimes
    /// behind opaque driver spawners.
    ///
    /// Native transport settings and partition configuration, including
    /// interface bindings, are preserved. Clients retain the native pool, while
    /// the execution handle does not own runtime shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] if a native client cannot be resolved for an
    /// execution partition.
    pub fn from_pool(
        runtime: ManagedRuntimeHandle,
        pool: pool::ConnectionPool,
    ) -> Result<Self, BuildError> {
        let clients = runtime
            .partitions()
            .iter()
            .map(|partition| {
                pool::Client::from_partition(
                    &pool,
                    pool::PartitionId::from_index(partition.id().index()),
                )
                .map_err(|source| BuildError::client(partition.id(), source))
            })
            .collect::<Result<Arc<[_]>, _>>()?;
        // Native clients retain the shared pool; a second stored pool is unnecessary.
        Ok(Self { runtime, clients })
    }
}

impl HttpClient for ManagedHttpPool {
    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        // Native connector preflight is cached per interface binding. Visiting
        // each client covers every distinct binding before requests are sent.
        self.clients
            .iter()
            .try_for_each(|client| client.validate_base_client_config(runtime_components, cfg))
    }

    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        let index = match self.runtime.current_partition() {
            Some(partition) => partition.id().index(),
            None => fastrand::usize(..self.clients.len()),
        };
        self.clients[index].http_connector(settings, components)
    }

    fn connector_metadata(&self) -> Option<ConnectorMetadata> {
        self.clients.first()?.connector_metadata()
    }
}
