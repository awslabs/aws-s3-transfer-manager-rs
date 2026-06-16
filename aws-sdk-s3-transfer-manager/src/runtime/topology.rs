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

/// A NUMA node with its assigned cores and the NICs bound to it.
#[derive(Debug, Clone)]
pub(crate) struct NumaNode {
    #[allow(dead_code)] // TODO: used by buffer pool NUMA partitioning
    pub(crate) id: usize,
    pub(crate) cores: Vec<usize>,
    /// NICs pinned to this node. Empty means no interface binding for threads
    /// on this node (partition `nic = None`).
    pub(crate) nics: Vec<String>,
}

impl NumaNode {
    /// A node with the given cores and no NIC binding.
    pub(crate) fn new(id: usize, cores: Vec<usize>) -> Self {
        Self {
            id,
            cores,
            nics: Vec::new(),
        }
    }

    /// Bind the given NICs to this node.
    pub(crate) fn with_nics(mut self, nics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.nics = nics.into_iter().map(Into::into).collect();
        self
    }
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
    /// Whether `core_for_thread` returns real hardware processor ids that may be
    /// pinned. False for [`uniform`](Topology::uniform), whose cores are
    /// synthetic indices with no hardware affinity.
    pinnable: bool,
}

// Most Topology methods are used only in tests today but are the planned API
// surface for buffer pool NUMA partitioning, core pinning, and dispatch routing.
#[allow(dead_code)]
impl Topology {
    /// Single NUMA node with cores `0..num_cores`. No affinity, works everywhere.
    pub(crate) fn uniform(num_cores: usize) -> Self {
        let mut topo = Self::from_nodes(vec![NumaNode::new(0, (0..num_cores).collect())]);
        topo.pinnable = false;
        topo
    }

    /// Detect NUMA nodes and cores from the system and attach `nics` to the
    /// node each is on. Synchronous: reads sysfs/cpuset once, intended only for
    /// the runtime construction phase, never a request path.
    ///
    /// Falls back to a single non-pinnable node when the platform reports no
    /// usable processors.
    pub(crate) fn detect(nics: &[String]) -> Self {
        let (cpus, pinnable) = detect_cpus();
        let nic_nodes = nics
            .iter()
            .map(|nic| (nic.clone(), nic_numa_node(nic)))
            .collect();
        let nodes = build_nodes(cpus, nic_nodes);
        if nodes.is_empty() {
            return Self::uniform(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1),
            );
        }
        let mut topo = Self::from_nodes(nodes);
        topo.pinnable = pinnable;
        topo
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
            pinnable: true,
        }
    }

    /// Whether threads may be pinned to their cores. False for synthetic
    /// (non-detected) topologies.
    pub(crate) fn pinnable(&self) -> bool {
        self.pinnable
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

    /// NIC this thread should bind, if its node has any. `None` means no
    /// interface binding (partition `nic = None`). When a node has multiple
    /// NICs, threads on the node are distributed across them by their ordinal
    /// within the node.
    pub(crate) fn nic_for_thread(&self, id: Cpu) -> Option<&str> {
        let node_idx = self.node_for_thread(id);
        let nics = &self.nodes[node_idx].nics;
        if nics.is_empty() {
            return None;
        }
        let ordinal = self.node_threads[node_idx]
            .iter()
            .position(|c| *c == id)
            .expect("thread belongs to its node");
        Some(nics[ordinal % nics.len()].as_str())
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

/// The NUMA node a NIC is attached to, read from sysfs.
///
/// `/sys/class/net/<nic>/device/numa_node` resolves through the net device's
/// `device` symlink to the backing (PCI) device, whose `numa_node` attribute
/// the kernel documents as the node id, or `-1` when affinity is unknown
/// (single-node systems, some virtualized NICs). See the kernel PCI sysfs ABI:
/// <https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-bus-pci>.
///
/// Returns `None` when affinity is unknown (`-1`), the attribute is absent
/// (non-PCI / virtual NIC), or the path does not exist (non-Linux).
fn nic_numa_node(nic: &str) -> Option<usize> {
    let path = format!("/sys/class/net/{nic}/device/numa_node");
    parse_numa_node(&std::fs::read_to_string(path).ok()?)
}

/// Parse a sysfs `numa_node` attribute value. A non-negative integer is the
/// node id; `-1` (unknown affinity) and any empty/unparseable value map to
/// `None`.
fn parse_numa_node(contents: &str) -> Option<usize> {
    match contents.trim().parse::<i64>().ok()? {
        n if n >= 0 => Some(n as usize),
        _ => None,
    }
}

/// `(processor id, memory region id)` for every usable processor, and whether
/// those ids are real hardware ids (pinnable).
#[cfg(target_os = "linux")]
fn detect_cpus() -> (Vec<(usize, usize)>, bool) {
    let set = many_cpus::SystemHardware::current().processors();
    let cpus = set
        .processors()
        .iter()
        .map(|p| (p.id() as usize, p.memory_region_id() as usize))
        .collect();
    (cpus, true)
}

/// Non-Linux fallback: a single memory region over all cores, synthetic ids
/// (not pinnable).
#[cfg(not(target_os = "linux"))]
fn detect_cpus() -> (Vec<(usize, usize)>, bool) {
    let n = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    ((0..n).map(|core| (core, 0)).collect(), false)
}

/// Group processors into NUMA nodes (by memory region, first-seen order) and
/// attach NICs to their nodes. A NIC with a known node binds to it; a NIC with
/// no known node (`-1`/absent) is distributed round-robin across the nodes.
fn build_nodes(
    cpus: Vec<(usize, usize)>,
    nic_nodes: Vec<(String, Option<usize>)>,
) -> Vec<NumaNode> {
    let mut regions: Vec<usize> = Vec::new();
    let mut cores: Vec<Vec<usize>> = Vec::new();
    for (proc_id, region) in cpus {
        let idx = regions
            .iter()
            .position(|r| *r == region)
            .unwrap_or_else(|| {
                regions.push(region);
                cores.push(Vec::new());
                regions.len() - 1
            });
        cores[idx].push(proc_id);
    }
    if regions.is_empty() {
        return Vec::new();
    }

    let mut nics: Vec<Vec<String>> = vec![Vec::new(); regions.len()];
    let mut next = 0usize;
    for (nic, node) in nic_nodes {
        let idx = node
            .and_then(|region| regions.iter().position(|r| *r == region))
            .unwrap_or_else(|| {
                let i = next % regions.len();
                next += 1;
                i
            });
        nics[idx].push(nic);
    }

    regions
        .into_iter()
        .enumerate()
        .map(|(i, region)| {
            NumaNode::new(region, std::mem::take(&mut cores[i]))
                .with_nics(std::mem::take(&mut nics[i]))
        })
        .collect()
}

/// Pin the calling thread to the processor with the given OS id. No-op when the
/// id is not present, or on platforms without affinity support.
#[cfg(target_os = "linux")]
pub(crate) fn pin_current_thread(core: usize) {
    if let Some(set) = many_cpus::SystemHardware::current()
        .processors()
        .filter(|p| p.id() as usize == core)
    {
        set.pin_current_thread_to();
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pin_current_thread(_core: usize) {}

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
            NumaNode::new(0, vec![0, 1, 2, 3]),
            NumaNode::new(1, vec![4, 5, 6, 7]),
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
            NumaNode::new(0, vec![0, 2, 4, 6]),
            NumaNode::new(1, vec![1, 3, 5, 7]),
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

    #[test]
    fn nic_for_thread_none_when_unbound() {
        let topo = Topology::uniform(4);
        for i in 0..4 {
            assert_eq!(topo.nic_for_thread(Cpu(i)), None);
        }
    }

    #[test]
    fn nic_for_thread_distributes_within_node() {
        // node 0 (threads 0,1): one NIC. node 1 (threads 2,3): two NICs.
        let topo = Topology::from_nodes(vec![
            NumaNode::new(0, vec![0, 1]).with_nics(["eth0"]),
            NumaNode::new(1, vec![2, 3]).with_nics(["eth1", "eth2"]),
        ]);
        assert_eq!(topo.nic_for_thread(Cpu(0)), Some("eth0"));
        assert_eq!(topo.nic_for_thread(Cpu(1)), Some("eth0"));
        // round-robin across the node's NICs by ordinal within the node
        assert_eq!(topo.nic_for_thread(Cpu(2)), Some("eth1"));
        assert_eq!(topo.nic_for_thread(Cpu(3)), Some("eth2"));
    }

    // Values per the kernel PCI sysfs ABI (numa_node): node id, or -1 unknown.
    // https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-bus-pci
    #[test]
    fn parse_numa_node_values() {
        assert_eq!(parse_numa_node("0\n"), Some(0));
        assert_eq!(parse_numa_node("1"), Some(1));
        assert_eq!(parse_numa_node("3\n"), Some(3));
        // -1 = kernel reports no NUMA affinity (single-node / virtual NIC)
        assert_eq!(parse_numa_node("-1\n"), None);
        assert_eq!(parse_numa_node(""), None);
        assert_eq!(parse_numa_node("  \n"), None);
        assert_eq!(parse_numa_node("garbage"), None);
    }

    #[test]
    fn build_nodes_groups_by_region() {
        // Two memory regions with interleaved processor ids.
        let nodes = build_nodes(vec![(0, 0), (1, 1), (2, 0), (3, 1)], vec![]);
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, 0);
        assert_eq!(nodes[0].cores, vec![0, 2]);
        assert_eq!(nodes[1].id, 1);
        assert_eq!(nodes[1].cores, vec![1, 3]);
    }

    #[test]
    fn build_nodes_attaches_nic_to_its_region() {
        let nodes = build_nodes(
            vec![(0, 0), (1, 1)],
            vec![("eth0".into(), Some(1)), ("eth1".into(), Some(0))],
        );
        assert_eq!(nodes[0].nics, vec!["eth1"]);
        assert_eq!(nodes[1].nics, vec!["eth0"]);
    }

    #[test]
    fn build_nodes_round_robins_unknown_nic_node() {
        // No known node: distribute across nodes by arrival order.
        let nodes = build_nodes(
            vec![(0, 0), (1, 1)],
            vec![("a".into(), None), ("b".into(), None), ("c".into(), None)],
        );
        assert_eq!(nodes[0].nics, vec!["a", "c"]);
        assert_eq!(nodes[1].nics, vec!["b"]);
    }

    #[test]
    fn build_nodes_empty_when_no_cpus() {
        assert!(build_nodes(vec![], vec![("eth0".into(), Some(0))]).is_empty());
    }
}
