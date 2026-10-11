/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fmt, sync::Arc};

/// An opaque identifier for an OS-visible logical processor.
///
/// Identifiers come from native discovery and may be sparse. Their order is not
/// a measure of physical proximity or physical-core membership.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalProcessorId(pub(crate) u32);

impl fmt::Display for LogicalProcessorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cpu-{}", self.0)
    }
}

/// An opaque NUMA node identifier.
///
/// Identifiers name reported memory locality domains. Ordering does not
/// describe distance between nodes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NumaNodeId(pub(crate) u32);

impl fmt::Display for NumaNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "numa-{}", self.0)
    }
}

/// An identified logical processor usable in the discovery context.
///
/// A logical processor is one hardware thread that the operating system can
/// schedule work onto. Multiple logical processors can share a physical core.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalProcessor {
    pub(crate) id: LogicalProcessorId,
    pub(crate) numa_node: Option<NumaNodeId>,
}

impl LogicalProcessor {
    /// Returns the processor identifier.
    pub fn id(&self) -> LogicalProcessorId {
        self.id
    }

    /// Returns its reported NUMA association, or `None` when unavailable.
    pub fn numa_node(&self) -> Option<NumaNodeId> {
        self.numa_node
    }
}

/// A reported NUMA node containing usable identified processors.
///
/// A node is a memory locality domain with associated processors.
/// Membership covers the discovery context, rather than the complete host.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NumaNode {
    pub(crate) id: NumaNodeId,
    pub(crate) logical_processors: Arc<[LogicalProcessorId]>,
}

impl NumaNode {
    /// Returns the node identifier.
    pub fn id(&self) -> NumaNodeId {
        self.id
    }

    /// Returns usable identified processors associated with this node.
    ///
    /// Members are ordered by processor identifier and retained after placement
    /// caps. The list does not describe complete host inventory.
    pub fn logical_processors(&self) -> &[LogicalProcessorId] {
        &self.logical_processors
    }
}
