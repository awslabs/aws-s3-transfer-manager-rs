/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::HttpClientError;
use aws_ort_execution::ManagedRuntimeHandle;
use aws_ort_http::client::{ManagedHttpPool, pool};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        connector_metadata::ConnectorMetadata,
        http::{HttpClient, HttpConnectorSettings, SharedHttpClient, SharedHttpConnector},
        runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
    },
    shared::IntoShared,
};
use aws_smithy_types::config_bag::ConfigBag;

pub(crate) fn client(
    execution: ManagedRuntimeHandle,
    pool: &ManagedHttpPool,
) -> Result<SharedHttpClient, HttpClientError> {
    if execution.id() != pool.runtime_id() {
        return Err(HttpClientError::runtime_mismatch(
            execution.id(),
            pool.runtime_id(),
        ));
    }
    let clients = execution
        .partitions()
        .iter()
        .map(|partition| pool.client_for_partition(partition.id()))
        .collect::<Result<Box<[_]>, _>>()
        .map_err(HttpClientError::partition)?;
    Ok(PartitionHttpClient { execution, clients }.into_shared())
}

#[derive(Debug)]
struct PartitionHttpClient {
    execution: ManagedRuntimeHandle,
    // Runtime partition indices match their positions in this immutable slice.
    clients: Box<[pool::Client]>,
}

impl HttpClient for PartitionHttpClient {
    fn validate_base_client_config(
        &self,
        runtime_components: &RuntimeComponentsBuilder,
        cfg: &ConfigBag,
    ) -> Result<(), BoxError> {
        self.clients
            .iter()
            .try_for_each(|client| client.validate_base_client_config(runtime_components, cfg))
    }

    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
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
