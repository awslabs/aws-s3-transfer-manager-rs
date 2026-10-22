/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::{BuildError, ManagedHttpPool, pool};
use aws_smithy_execution::ManagedRuntimeHandle;

/// Retains native transport configuration until a managed runtime is supplied.
///
/// `B` is the supplied native builder type. [`Self::pool_builder`] retains its
/// TLS state; terminal `build` methods delegate to the corresponding native
/// HTTP or HTTPS constructor.
#[non_exhaustive]
#[derive(Debug)]
pub struct Builder<B = ()> {
    pool_builder: Option<B>,
    runtime: Option<ManagedRuntimeHandle>,
}

impl<B> Default for Builder<B> {
    fn default() -> Self {
        Self {
            pool_builder: None,
            runtime: None,
        }
    }
}

impl<B> Builder<B> {
    /// Supplies the native construction value, preserving its transport state.
    ///
    /// Native TLS, proxy, DNS, limits, reuse, timers, and observation settings
    /// remain on this builder. Explicit native partition declarations conflict
    /// with managed construction and are rejected by `build`.
    pub fn pool_builder<P>(self, builder: P) -> Builder<P> {
        let mut next = Builder {
            pool_builder: None,
            runtime: self.runtime,
        };
        next.set_pool_builder(Some(builder));
        next
    }

    /// Replaces or clears the construction value without changing its type.
    pub fn set_pool_builder(&mut self, builder: Option<B>) -> &mut Self {
        self.pool_builder = builder;
        self
    }

    /// Uses this runtime's execution partitions for connection-owned work.
    pub fn runtime(mut self, runtime: ManagedRuntimeHandle) -> Self {
        self.set_runtime(Some(runtime));
        self
    }

    /// Sets or clears the runtime capability.
    pub fn set_runtime(&mut self, runtime: Option<ManagedRuntimeHandle>) -> &mut Self {
        self.runtime = runtime;
        self
    }
}

impl<Tls> Builder<pool::Builder<Tls>> {
    fn configure(self) -> Result<(pool::Builder<Tls>, ManagedRuntimeHandle), BuildError> {
        let transport = self
            .pool_builder
            .ok_or_else(BuildError::missing_pool_builder)?;
        let runtime = self.runtime.ok_or_else(BuildError::missing_runtime)?;
        if transport.has_explicit_partitions() {
            return Err(BuildError::explicit_partitions());
        }

        let partitions = runtime.partitions().iter().map(|partition| {
            // Pool identities are local to this pool; runtime-scoped IDs stay
            // at the managed boundary for checked client lookup.
            let id = pool::PartitionId::from_index(partition.id().index());
            let target = partition.clone();
            let spawner = pool::DriverSpawner::from_fn(move |task| {
                let handle = target.spawn(task);
                drop(handle);
            });
            pool::Partition::new(id, spawner)
        });
        let transport = transport.partitions(partitions);
        Ok((transport, runtime))
    }
}

impl Builder<pool::Builder> {
    /// Builds using the native cleartext HTTP constructor.
    ///
    /// # Errors
    ///
    /// Returns an error for missing construction inputs, predeclared native
    /// partitions, or an invalid native pool configuration.
    pub fn build(self) -> Result<ManagedHttpPool, BuildError> {
        let (transport, runtime) = self.configure()?;
        let pool = transport.build_http().map_err(BuildError::pool)?;
        ManagedHttpPool::from_pool(runtime, pool)
    }
}

impl Builder<pool::HttpsBuilder> {
    /// Builds using the native HTTPS constructor and selected TLS provider.
    ///
    /// # Errors
    ///
    /// Returns an error for missing construction inputs, predeclared native
    /// partitions, or an invalid native pool configuration.
    pub fn build(self) -> Result<ManagedHttpPool, BuildError> {
        let (transport, runtime) = self.configure()?;
        let pool = transport.build_https().map_err(BuildError::pool)?;
        ManagedHttpPool::from_pool(runtime, pool)
    }
}
