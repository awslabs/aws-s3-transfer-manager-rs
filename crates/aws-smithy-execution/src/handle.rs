/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{RuntimeId, RuntimePartition, identity};
use aws_smithy_topology::Topology;
use std::sync::Arc;

/// Cloneable access to managed runtime topology and execution partitions.
///
/// A handle does not own shutdown or keep threads running after their owner is
/// dropped. Topology and partition metadata remain available after shutdown.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ManagedRuntimeHandle {
    pub(crate) state: Arc<RuntimeState>,
}

#[derive(Debug)]
pub(crate) struct RuntimeState {
    pub(crate) id: RuntimeId,
    pub(crate) topology: Topology,
    pub(crate) partitions: Box<[RuntimePartition]>,
}

impl ManagedRuntimeHandle {
    /// Returns this runtime's process-local identity.
    pub fn id(&self) -> RuntimeId {
        self.state.id
    }

    /// Returns the discovered resources and selected execution targets.
    pub fn topology(&self) -> &Topology {
        &self.state.topology
    }

    /// Returns the nonempty execution partition collection in topology placement order.
    ///
    /// Indices are dense: the partition at position `i` has index `i`, and all
    /// partitions belong to this runtime. The collection is fixed at construction.
    pub fn partitions(&self) -> &[RuntimePartition] {
        &self.state.partitions
    }

    /// Returns the receiver's execution partition associated with the calling managed thread.
    ///
    /// Returns `None` outside this runtime's managed threads, including a
    /// thread from another runtime or a Tokio blocking-pool thread. Entering a
    /// Tokio handle on another thread does not make that thread a partition.
    pub fn current_partition(&self) -> Option<&RuntimePartition> {
        let current = identity::current_partition()?;
        if current.runtime_id() != self.id() {
            return None;
        }
        Some(
            self.state
                .partitions
                .get(current.index())
                .expect("current partition index must belong to this runtime"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ManagedRuntime, RuntimePartitionId};

    #[test]
    #[should_panic(expected = "current partition index must belong to this runtime")]
    fn same_runtime_invalid_partition_is_a_contract_violation() {
        let owner = ManagedRuntime::builder()
            .topology(Topology::detect().unwrap().with_max_placements(1))
            .pin_threads(false)
            .build()
            .unwrap();
        let execution = owner.handle();
        let _current =
            identity::CurrentPartition::enter(RuntimePartitionId::new(execution.id(), 1));
        execution.current_partition();
    }
}
