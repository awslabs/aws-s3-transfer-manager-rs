/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::RuntimePartitionId;
use aws_smithy_topology::{LogicalProcessorId, Placement};
use std::future::Future;
use tokio::{runtime::Handle, task::JoinHandle};

/// Cloneable access to an execution partition.
///
/// Each partition runs a Tokio current-thread runtime on a dedicated managed
/// thread. Tasks submitted here run on that runtime.
/// Cloning a partition does not retain ownership of the thread's lifecycle.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RuntimePartition {
    pub(crate) id: RuntimePartitionId,
    pub(crate) placement: Placement,
    pub(crate) bound_processor: Option<LogicalProcessorId>,
    pub(crate) tokio: Handle,
    #[cfg(feature = "dial9")]
    pub(crate) traced: bool,
}

impl RuntimePartition {
    /// Returns this execution partition's runtime-scoped identity.
    pub fn id(&self) -> RuntimePartitionId {
        self.id
    }

    /// Returns the intended target supplied by topology.
    ///
    /// The target is retained when CPU pinning is disabled.
    pub fn placement(&self) -> &Placement {
        &self.placement
    }

    /// Returns the processor binding established during startup, if any.
    ///
    /// This records startup binding, not the thread's current executing CPU or
    /// subsequent affinity changes. It establishes no memory allocation policy.
    pub fn bound_processor(&self) -> Option<LogicalProcessorId> {
        self.bound_processor
    }

    /// Submits a Send future to this execution partition and returns its typed task handle.
    ///
    /// The caller can submit from another thread or async runtime. Task
    /// completion, cancellation, panic reporting, and handle drop follow Tokio
    /// semantics. Owner destruction requests shutdown without synchronously
    /// rejecting submissions; after this thread stops, submitted tasks are
    /// cancelled.
    #[track_caller]
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        #[cfg(feature = "dial9")]
        if self.traced {
            return dial9::spawn_in(&self.tokio, future);
        }
        self.tokio.spawn(future)
    }
}
