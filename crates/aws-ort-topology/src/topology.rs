/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::platform::Discovery;
use crate::{BindError, DetectError, LogicalProcessor, LogicalProcessorId, NumaNode, Placement};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

/// Immutable discovered resources and ordered intended execution placements.
///
/// Clones share resource data. Limiting placements on a clone does not alter the
/// original value or remove processors from its discovered inventory.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Topology {
    resources: Arc<Resources>,
    placements: Arc<[Placement]>,
}

#[derive(Debug)]
struct Resources {
    processors: Box<[LogicalProcessor]>,
    nodes: Box<[NumaNode]>,
    parallelism: NonZeroUsize,
}

impl Topology {
    /// Captures usable processors and default placement targets.
    ///
    /// On Linux and Windows, native discovery restricts the usable inventory to
    /// processors available to the calling thread before applying resource quota
    /// to default placement selection. Other platforms return a count estimate
    /// without inventing processor identities.
    ///
    /// Discovery may reuse cached hardware inventory and does not change thread
    /// affinity. Separate calls need not select the same default processors.
    ///
    /// # Errors
    ///
    /// Returns an error if discovery cannot produce a nonempty usable topology.
    /// In unwind builds, native discovery panics become errors; the panic hook
    /// still runs. Aborting panics cannot be recovered.
    pub fn detect() -> Result<Self, DetectError> {
        Self::from_discovery(crate::platform::detect()?)
    }

    /// Returns identified usable processors, empty for count-only discovery.
    ///
    /// Native processors are ordered by identifier. The inventory is retained
    /// when a quota or caller cap reduces selected placements.
    pub fn logical_processors(&self) -> &[LogicalProcessor] {
        &self.resources.processors
    }

    /// Returns reported NUMA associations, empty when unavailable.
    ///
    /// Nodes are ordered by identifier. Membership covers usable identified
    /// processors in this snapshot, rather than the complete host.
    pub fn numa_nodes(&self) -> &[NumaNode] {
        &self.resources.nodes
    }

    /// Returns selected execution targets, always nonempty.
    ///
    /// Each placement identifies a processor and its NUMA association when
    /// available. Querying or selecting placements changes no thread affinity.
    /// Native placements are ordered by target processor identifier. Count-only
    /// placements have no target. A slot's position is not a processor identity.
    pub fn placements(&self) -> &[Placement] {
        &self.placements
    }

    /// Returns the positive discovered default placement count, before caller caps.
    ///
    /// Native discovery uses quota-limited default selection. Count-only
    /// discovery uses the platform's available-parallelism estimate.
    pub fn parallelism_estimate(&self) -> usize {
        self.resources.parallelism.get()
    }

    /// Caps selected slots while preserving discovery facts and their order.
    ///
    /// Keeps the prefix of the current placements. Zero is treated as one,
    /// preserving a nonempty selection. A larger maximum leaves selection
    /// unchanged; repeated calls cannot restore removed placements.
    /// This does not rebalance placements across NUMA nodes.
    pub fn with_max_placements(mut self, maximum: usize) -> Self {
        let maximum = maximum.max(1);
        if maximum < self.placements.len() {
            self.placements = Arc::from(&self.placements[..maximum]);
        }
        self
    }

    /// Binds the calling thread to one identified logical processor.
    ///
    /// The processor must belong to this topology's resource inventory and
    /// remain available to the calling thread at binding time. It need not be
    /// among the selected placements. Binding does not widen the thread's
    /// current available processor set; after binding, another processor
    /// outside that set cannot be selected through this method.
    ///
    /// On success, the platform reports only the requested processor as
    /// available to this thread. The binding persists after this topology is
    /// dropped. This changes neither the selected placements nor memory placement.
    /// Native CPU binding is supported on Linux and Windows.
    ///
    /// # Errors
    ///
    /// Returns an error for a processor outside the inventory, an unavailable
    /// target, unsupported native binding, or a native failure. An error after
    /// the binding operation may leave affinity changed; no rollback is done.
    /// In unwind builds, native panics become errors; the panic hook still runs.
    /// Aborting panics cannot be recovered.
    pub fn bind_current_thread(&self, processor: LogicalProcessorId) -> Result<(), BindError> {
        self.resources
            .processors
            .binary_search_by_key(&processor, LogicalProcessor::id)
            .map_err(|_| BindError::unknown_processor(processor))?;
        crate::platform::bind_current_thread(processor)
    }

    pub(crate) fn from_discovery(discovery: Discovery) -> Result<Self, DetectError> {
        let (processors, placements) = match discovery {
            Discovery::CountOnly(count) => (
                Vec::new(),
                vec![
                    Placement {
                        logical_processor: None,
                        numa_node: None
                    };
                    count.get()
                ],
            ),
            Discovery::Processors {
                mut processors,
                mut selected,
            } => {
                // Keep the usable inventory separate from selected targets.
                // Canonicalize identifiers and reject ambiguous resource entries.
                processors.sort_unstable_by_key(LogicalProcessor::id);
                let by_id: BTreeMap<_, _> = processors
                    .iter()
                    .map(|processor| (processor.id(), processor.numa_node()))
                    .collect();
                if by_id.len() != processors.len() {
                    return Err(DetectError::invalid("duplicate processor identifiers"));
                }
                selected.sort_unstable();
                if selected.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(DetectError::invalid("duplicate placement targets"));
                }
                // Resolve every target through the inventory so placement CPU
                // identities and their NUMA associations agree with resource views.
                let placements = selected
                    .into_iter()
                    .map(|id| {
                        let numa_node = *by_id.get(&id).ok_or_else(|| {
                            DetectError::invalid("placement targets an unknown processor")
                        })?;
                        Ok(Placement {
                            logical_processor: Some(id),
                            numa_node,
                        })
                    })
                    .collect::<Result<Vec<_>, DetectError>>()?;
                (processors, placements)
            }
        };
        let parallelism =
            NonZeroUsize::new(placements.len()).ok_or_else(DetectError::no_processors)?;
        // NUMA membership comes from the full usable inventory, including CPUs
        // omitted from placements by a quota. Caller caps preserve this inventory.
        let mut members = BTreeMap::new();
        for processor in &processors {
            if let Some(node) = processor.numa_node() {
                members
                    .entry(node)
                    .or_insert_with(Vec::new)
                    .push(processor.id());
            }
        }
        let nodes = members
            .into_iter()
            .map(|(id, processors)| NumaNode {
                id,
                logical_processors: processors.into(),
            })
            .collect();
        Ok(Self {
            resources: Arc::new(Resources {
                processors: processors.into(),
                nodes,
                parallelism,
            }),
            placements: placements.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NumaNodeId;

    fn processor(id: u32, node: Option<u32>) -> LogicalProcessor {
        LogicalProcessor {
            id: LogicalProcessorId(id),
            numa_node: node.map(NumaNodeId),
        }
    }

    fn native(processors: Vec<LogicalProcessor>, selected: &[u32]) -> Discovery {
        Discovery::Processors {
            processors,
            selected: selected.iter().copied().map(LogicalProcessorId).collect(),
        }
    }

    fn fixture() -> Topology {
        Topology::from_discovery(native(
            vec![
                processor(42, Some(8)),
                processor(3, Some(2)),
                processor(19, Some(8)),
                processor(7, Some(2)),
            ],
            &[42, 19, 3, 7],
        ))
        .unwrap()
    }

    #[test]
    fn node_membership_uses_resource_inventory_and_preserves_unknown_associations() {
        let topology = Topology::from_discovery(native(
            vec![
                processor(42, None),
                processor(19, Some(8)),
                processor(7, Some(2)),
                processor(3, Some(2)),
            ],
            &[42, 7],
        ))
        .unwrap();
        let nodes = topology.numa_nodes();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id(), NumaNodeId(2));
        assert_eq!(
            nodes[0].logical_processors(),
            [LogicalProcessorId(3), LogicalProcessorId(7)],
        );
        assert_eq!(nodes[1].id(), NumaNodeId(8));
        assert_eq!(nodes[1].logical_processors(), [LogicalProcessorId(19)]);
        assert_eq!(topology.placements()[0].numa_node(), Some(NumaNodeId(2)));
        assert_eq!(
            topology.placements()[1].logical_processor(),
            Some(LogicalProcessorId(42)),
        );
        assert_eq!(topology.placements()[1].numa_node(), None);
    }

    #[test]
    fn cap_preserves_resources_estimate_and_original_selection() {
        let original = fixture();
        let capped = original.clone().with_max_placements(2);
        assert_eq!(original.placements().len(), 4);
        assert_eq!(capped.placements().len(), 2);
        assert_eq!(capped.parallelism_estimate(), 4);
        assert_eq!(capped.logical_processors(), original.logical_processors());
        assert_eq!(capped.placements(), &original.placements()[..2]);
        assert_eq!(capped.numa_nodes()[0].logical_processors().len(), 2);
        assert_eq!(capped.numa_nodes()[1].logical_processors().len(), 2);
        // Resource data is shared even when placement selection differs.
        assert!(Arc::ptr_eq(&capped.resources, &original.resources));
    }

    #[test]
    fn zero_cap_retains_one_placement_and_discovery_facts() {
        let count_only =
            Topology::from_discovery(Discovery::CountOnly(NonZeroUsize::new(3).unwrap())).unwrap();
        for original in [fixture(), count_only] {
            let capped = original.clone().with_max_placements(0);
            assert_eq!(capped.placements(), &original.placements()[..1]);
            assert_eq!(
                capped.parallelism_estimate(),
                original.parallelism_estimate()
            );
            assert_eq!(capped.logical_processors(), original.logical_processors());
        }
    }

    #[test]
    fn larger_cap_does_not_expand_or_restore_placements() {
        let original = fixture();
        let unchanged = original.clone().with_max_placements(8);
        assert_eq!(unchanged.placements(), original.placements());
        let reduced = original.with_max_placements(2).with_max_placements(8);
        assert_eq!(reduced.placements().len(), 2);
        assert_eq!(reduced.parallelism_estimate(), 4);
    }

    #[test]
    fn count_only_has_no_invented_hardware() {
        let topology =
            Topology::from_discovery(Discovery::CountOnly(NonZeroUsize::new(3).unwrap())).unwrap();
        assert_eq!(topology.parallelism_estimate(), 3);
        assert_eq!(topology.placements().len(), 3);
        assert!(topology.logical_processors().is_empty());
        assert!(topology.numa_nodes().is_empty());
        assert!(topology.placements().iter().all(|placement| {
            placement.logical_processor().is_none() && placement.numa_node().is_none()
        }));
        let capped = topology.with_max_placements(1);
        assert_eq!(capped.placements().len(), 1);
        assert_eq!(capped.parallelism_estimate(), 3);
    }

    #[test]
    fn duplicate_processor_identifiers_are_rejected() {
        let snapshot = native(vec![processor(42, Some(8)), processor(42, Some(2))], &[42]);
        let error = Topology::from_discovery(snapshot).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate processor identifiers")
        );
    }

    #[test]
    fn duplicate_placement_targets_are_rejected() {
        let snapshot = native(vec![processor(42, Some(8))], &[42, 42]);
        let error = Topology::from_discovery(snapshot).unwrap_err();
        assert!(error.to_string().contains("duplicate placement targets"));
    }

    #[test]
    fn unknown_placement_targets_are_rejected() {
        let snapshot = native(vec![processor(42, Some(8))], &[7]);
        let error = Topology::from_discovery(snapshot).unwrap_err();
        assert!(error.to_string().contains("unknown processor"));
    }

    #[test]
    fn empty_native_selection_is_rejected() {
        let snapshot = native(vec![processor(42, Some(8))], &[]);
        assert!(Topology::from_discovery(snapshot).is_err());
    }

    #[test]
    fn binding_rejects_unknown_processor_before_platform_operation() {
        let error = fixture()
            .bind_current_thread(LogicalProcessorId(99))
            .unwrap_err();
        assert!(error.to_string().contains("cpu-99"));
        assert!(error.to_string().contains("not in this topology"));
    }

    #[test]
    fn count_only_topology_cannot_bind_an_invented_processor() {
        let topology =
            Topology::from_discovery(Discovery::CountOnly(NonZeroUsize::new(3).unwrap())).unwrap();
        let error = topology
            .bind_current_thread(LogicalProcessorId(0))
            .unwrap_err();
        assert!(error.to_string().contains("not in this topology"));
    }
}
