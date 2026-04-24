/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Hardware topology types for managed thread runtimes.

use std::fmt;

/// A logical CPU that a managed thread is pinned to.
///
/// Dense index assigned during topology construction, not a hardware
/// core id. Use [`Topology`] to map a `Cpu` to its hardware core and
/// NUMA node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Cpu(pub(crate) usize);

impl fmt::Display for Cpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cpu-{}", self.0)
    }
}

/// A NUMA node with its assigned cores.
#[derive(Debug, Clone)]
pub(crate) struct NumaNode {
    #[allow(dead_code)] // TODO: used by buffer pool NUMA partitioning
    pub(crate) id: usize,
    pub(crate) cores: Vec<usize>,
}

/// Hardware topology: NUMA nodes and their cores.
///
/// Thread assignment is dense and ordered: threads are assigned sequentially
/// across nodes. For a two-node topology where node 0 has cores 0,1 and
/// node 1 has cores 4,5, the thread ids are 0,1 (node 0) and 2,3 (node 1).
#[derive(Debug, Clone)]
pub(crate) struct Topology {
    #[allow(dead_code)] // TODO: used by buffer pool NUMA partitioning
    nodes: Vec<NumaNode>,
    /// Pre-computed: thread_id.0 to (node index, core id)
    thread_map: Vec<(usize, usize)>,
    /// Pre-computed: node index to thread ids
    #[allow(dead_code)] // TODO: used by buffer pool NUMA partitioning
    node_threads: Vec<Vec<Cpu>>,
}

// Most Topology methods are used only in tests today but are the planned API
// surface for buffer pool NUMA partitioning, core pinning, and dispatch routing.
#[allow(dead_code)]
impl Topology {
    /// Single NUMA node with cores `0..num_cores`. No affinity, works everywhere.
    pub(crate) fn uniform(num_cores: usize) -> Self {
        Self::from_nodes(vec![NumaNode {
            id: 0,
            cores: (0..num_cores).collect(),
        }])
    }

    /// Build from explicit node descriptions. Pre-computes lookup tables.
    pub(crate) fn from_nodes(nodes: Vec<NumaNode>) -> Self {
        let mut thread_map = Vec::new();
        let mut node_threads = vec![Vec::new(); nodes.len()];
        for (node_idx, node) in nodes.iter().enumerate() {
            for &core in &node.cores {
                let tid = Cpu(thread_map.len());
                thread_map.push((node_idx, core));
                node_threads[node_idx].push(tid);
            }
        }
        Self {
            nodes,
            thread_map,
            node_threads,
        }
    }

    /// All thread ids.
    pub(crate) fn thread_ids(&self) -> impl Iterator<Item = Cpu> {
        (0..self.thread_map.len()).map(Cpu)
    }

    /// NUMA node index for a thread.
    pub(crate) fn node_for_thread(&self, id: Cpu) -> usize {
        self.thread_map[id.0].0
    }

    /// Core id for a thread (for pinning).
    pub(crate) fn core_for_thread(&self, id: Cpu) -> usize {
        self.thread_map[id.0].1
    }

    /// All threads on a NUMA node.
    pub(crate) fn threads_on_node(&self, node: usize) -> &[Cpu] {
        &self.node_threads[node]
    }

    /// Number of NUMA nodes.
    pub(crate) fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// The NUMA nodes in this topology.
    pub(crate) fn nodes(&self) -> &[NumaNode] {
        &self.nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_single_node() {
        let topo = Topology::uniform(4);
        assert_eq!(topo.thread_ids().count(), 4);
        assert_eq!(topo.num_nodes(), 1);
        assert_eq!(topo.threads_on_node(0).len(), 4);
        for i in 0..4 {
            assert_eq!(topo.core_for_thread(Cpu(i)), i);
            assert_eq!(topo.node_for_thread(Cpu(i)), 0);
        }
    }

    #[test]
    fn multi_node() {
        let topo = Topology::from_nodes(vec![
            NumaNode {
                id: 0,
                cores: vec![0, 1, 2, 3],
            },
            NumaNode {
                id: 1,
                cores: vec![4, 5, 6, 7],
            },
        ]);
        assert_eq!(topo.thread_ids().count(), 8);
        assert_eq!(topo.num_nodes(), 2);
        assert_eq!(topo.threads_on_node(0).len(), 4);
        assert_eq!(topo.threads_on_node(1).len(), 4);
        for i in 0..4 {
            assert_eq!(topo.node_for_thread(Cpu(i)), 0);
            assert_eq!(topo.core_for_thread(Cpu(i)), i);
        }
        for i in 4..8 {
            assert_eq!(topo.node_for_thread(Cpu(i)), 1);
            assert_eq!(topo.core_for_thread(Cpu(i)), i);
        }
    }

    #[test]
    fn non_contiguous_cores() {
        let topo = Topology::from_nodes(vec![
            NumaNode {
                id: 0,
                cores: vec![0, 2, 4, 6],
            },
            NumaNode {
                id: 1,
                cores: vec![1, 3, 5, 7],
            },
        ]);
        assert_eq!(topo.thread_ids().count(), 8);
        // Thread 0 → core 0, thread 1 → core 2, thread 2 → core 4, thread 3 → core 6
        assert_eq!(topo.core_for_thread(Cpu(0)), 0);
        assert_eq!(topo.core_for_thread(Cpu(1)), 2);
        assert_eq!(topo.core_for_thread(Cpu(2)), 4);
        assert_eq!(topo.core_for_thread(Cpu(3)), 6);
        // Thread 4 → core 1, thread 5 → core 3, thread 6 → core 5, thread 7 → core 7
        assert_eq!(topo.core_for_thread(Cpu(4)), 1);
        assert_eq!(topo.core_for_thread(Cpu(5)), 3);
        assert_eq!(topo.core_for_thread(Cpu(6)), 5);
        assert_eq!(topo.core_for_thread(Cpu(7)), 7);
    }

    #[test]
    fn thread_ids_iterator() {
        let topo = Topology::uniform(3);
        let ids: Vec<_> = topo.thread_ids().collect();
        assert_eq!(ids, vec![Cpu(0), Cpu(1), Cpu(2)]);
    }
}
