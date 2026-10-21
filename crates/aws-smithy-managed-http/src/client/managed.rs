/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{BuildError, Builder, PartitionError};
use aws_smithy_execution::{ManagedRuntimeHandle, RuntimeId, RuntimePartitionId};
use aws_smithy_http_client::pool;

/// One native connection pool with clients bound to managed execution partitions.
///
/// Each build creates a new pool and resolves each client once. Clients obtained
/// from that pool share its connections and policy. The runtime owner remains
/// with the caller.
#[non_exhaustive]
#[derive(Debug)]
pub struct ManagedHttpPool {
    runtime: ManagedRuntimeHandle,
    // Execution guarantees that each partition index equals its slice position.
    clients: Box<[pool::Client]>,
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

    pub(super) fn from_pool(
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
            .collect::<Result<Box<[_]>, _>>()?;
        // Native clients retain the shared pool; a second stored pool is unnecessary.
        Ok(Self { runtime, clients })
    }
}
