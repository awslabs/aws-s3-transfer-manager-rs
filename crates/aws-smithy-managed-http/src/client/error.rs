/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_execution::{RuntimeId, RuntimePartitionId};
use aws_smithy_http_client::pool;
use aws_smithy_runtime_api::box_error::BoxError;
use std::{error::Error, fmt};

/// Classifies a failure to compose native transport with managed execution.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildErrorKind {
    /// No native connection-pool builder was supplied.
    MissingPoolBuilder,
    /// No managed runtime handle was supplied.
    MissingRuntime,
    /// Native partitions were declared before managed partition construction.
    ExplicitPartitions,
    /// Native connection-pool construction failed.
    Pool,
    /// A native client could not be resolved for an execution partition.
    Client,
}

/// Failure to compose native transport with managed execution partitions.
#[non_exhaustive]
#[derive(Debug)]
pub struct BuildError {
    kind: BuildErrorKind,
    partition: Option<RuntimePartitionId>,
    source: Option<BoxError>,
}

impl BuildError {
    /// Returns the classification of this construction failure.
    pub fn kind(&self) -> BuildErrorKind {
        self.kind
    }

    /// Returns the execution partition whose native client could not be resolved.
    ///
    /// This is `Some` for [`BuildErrorKind::Client`] and `None` for other failures.
    pub fn partition(&self) -> Option<RuntimePartitionId> {
        self.partition
    }

    pub(super) fn missing_pool_builder() -> Self {
        Self {
            kind: BuildErrorKind::MissingPoolBuilder,
            partition: None,
            source: None,
        }
    }

    pub(super) fn missing_runtime() -> Self {
        Self {
            kind: BuildErrorKind::MissingRuntime,
            partition: None,
            source: None,
        }
    }

    pub(super) fn explicit_partitions() -> Self {
        Self {
            kind: BuildErrorKind::ExplicitPartitions,
            partition: None,
            source: None,
        }
    }

    pub(super) fn pool(source: pool::BuildError) -> Self {
        Self {
            kind: BuildErrorKind::Pool,
            partition: None,
            source: Some(Box::new(source)),
        }
    }

    pub(super) fn client(id: RuntimePartitionId, source: pool::ClientBuildError) -> Self {
        Self {
            kind: BuildErrorKind::Client,
            partition: Some(id),
            source: Some(Box::new(source)),
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            BuildErrorKind::MissingPoolBuilder => {
                f.write_str("native connection-pool builder is required")
            }
            BuildErrorKind::MissingRuntime => f.write_str("managed runtime handle is required"),
            BuildErrorKind::ExplicitPartitions => f.write_str(
                "native partitions conflict with managed runtime partition construction",
            ),
            BuildErrorKind::Pool => write!(
                f,
                "unable to build native connection pool: {}",
                self.source.as_deref().expect("pool failure has a source"),
            ),
            BuildErrorKind::Client => write!(
                f,
                "unable to resolve HTTP client for {}: {}",
                self.partition.expect("client failure has a partition"),
                self.source.as_deref().expect("client failure has a source"),
            ),
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
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
