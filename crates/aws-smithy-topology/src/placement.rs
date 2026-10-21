/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{LogicalProcessorId, NumaNodeId};

/// An intended execution target, such as a worker thread's CPU.
///
/// Identifies a logical processor and its reported NUMA association when
/// available. These describe the target without changing thread affinity or
/// memory placement.
///
/// CPU binding is a separate operation through
/// [`Topology::bind_current_thread`](crate::Topology::bind_current_thread).
/// A target without a processor identifier leaves CPU selection to the
/// operating system.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    pub(crate) logical_processor: Option<LogicalProcessorId>,
    pub(crate) numa_node: Option<NumaNodeId>,
}

impl Placement {
    /// Returns the intended processor, or `None` when no identified target is available.
    pub fn logical_processor(&self) -> Option<LogicalProcessorId> {
        self.logical_processor
    }

    /// Returns the target's reported NUMA association, when available.
    pub fn numa_node(&self) -> Option<NumaNodeId> {
        self.numa_node
    }
}
