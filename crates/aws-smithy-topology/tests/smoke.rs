/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Exercise the public API against the actual host, without fixed CPU counts or
// IDs. Fake-hardware unit tests cover specific platform and selection behavior.
use aws_smithy_topology::Topology;

#[test]
fn host_discovery_preserves_resource_and_placement_contracts() {
    let topology = Topology::detect().unwrap();
    assert!(!topology.placements().is_empty());
    let estimate: usize = topology.parallelism_estimate();
    assert_eq!(topology.placements().len(), estimate);
    let capped = topology.clone().with_max_placements(2);
    assert!(capped.placements().len() <= 2);
    assert_eq!(
        capped.parallelism_estimate(),
        topology.parallelism_estimate()
    );
    assert_eq!(capped.logical_processors(), topology.logical_processors());
    assert_eq!(
        capped.placements(),
        &topology.placements()[..topology.placements().len().min(2)],
    );
    for placement in capped.placements() {
        if let Some(id) = placement.logical_processor() {
            let processor = topology
                .logical_processors()
                .iter()
                .find(|processor| processor.id() == id)
                .expect("selected targets refer to the resource inventory");
            assert_eq!(processor.numa_node(), placement.numa_node());
        }
    }
    if topology.logical_processors().is_empty() {
        assert!(topology.numa_nodes().is_empty());
        assert!(
            topology
                .placements()
                .iter()
                .all(|p| p.logical_processor().is_none() && p.numa_node().is_none())
        );
    }
}

// Bind a disposable child so the test caller's affinity stays unchanged.
#[cfg(all(any(target_os = "linux", target_os = "windows"), not(miri)))]
#[test]
fn native_binding_restricts_child_without_changing_caller_or_placements() {
    let topology = Topology::detect().unwrap();
    let processors = topology.logical_processors().to_vec();
    let placements = topology.placements().to_vec();
    let target = processors[0].id();
    let worker_topology = topology.clone().with_max_placements(1);

    std::thread::spawn(move || {
        worker_topology.bind_current_thread(target).unwrap();
        let observed = Topology::detect().unwrap();
        assert_eq!(observed.logical_processors().len(), 1);
        assert_eq!(observed.logical_processors()[0].id(), target);
        assert_eq!(worker_topology.logical_processors(), processors);
        assert_eq!(worker_topology.placements(), &placements[..1]);
    })
    .join()
    .unwrap();

    assert_eq!(
        Topology::detect().unwrap().logical_processors(),
        topology.logical_processors(),
    );
}
