/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_execution::{RuntimeId, RuntimePartitionId};
use aws_smithy_http_client::pool;
use std::{error::Error, fmt};

/// Failure to compose native transport with managed execution partitions.
#[non_exhaustive]
#[derive(Debug)]
pub struct BuildError {
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    MissingPoolBuilder,
    MissingRuntime,
    ExplicitPartitions,
    Pool(pool::BuildError),
    Client(RuntimePartitionId, pool::ClientBuildError),
}

impl BuildError {
    pub(super) fn missing_pool_builder() -> Self {
        Self {
            kind: Kind::MissingPoolBuilder,
        }
    }

    pub(super) fn missing_runtime() -> Self {
        Self {
            kind: Kind::MissingRuntime,
        }
    }

    pub(super) fn explicit_partitions() -> Self {
        Self {
            kind: Kind::ExplicitPartitions,
        }
    }

    pub(super) fn pool(source: pool::BuildError) -> Self {
        Self {
            kind: Kind::Pool(source),
        }
    }

    pub(super) fn client(id: RuntimePartitionId, source: pool::ClientBuildError) -> Self {
        Self {
            kind: Kind::Client(id, source),
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            Kind::MissingPoolBuilder => f.write_str("native connection-pool builder is required"),
            Kind::MissingRuntime => f.write_str("managed runtime handle is required"),
            Kind::ExplicitPartitions => f.write_str(
                "native partitions conflict with managed runtime partition construction",
            ),
            Kind::Pool(source) => write!(f, "unable to build native connection pool: {source}"),
            Kind::Client(id, source) => {
                write!(f, "unable to resolve HTTP client for {id}: {source}")
            }
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            Kind::Pool(source) => Some(source),
            Kind::Client(_, source) => Some(source),
            _ => None,
        }
    }
}

/// An execution partition is unavailable in this pool's runtime.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionError {
    partition: RuntimePartitionId,
    runtime: RuntimeId,
}

impl PartitionError {
    pub(super) fn new(partition: RuntimePartitionId, runtime: RuntimeId) -> Self {
        Self { partition, runtime }
    }

    /// Returns the requested execution partition.
    pub fn partition(&self) -> RuntimePartitionId {
        self.partition
    }

    /// Returns the runtime associated with the HTTP pool.
    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime
    }
}

impl fmt::Display for PartitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "execution partition {} is unavailable in HTTP pool for {}",
            self.partition, self.runtime,
        )
    }
}

impl Error for PartitionError {}
