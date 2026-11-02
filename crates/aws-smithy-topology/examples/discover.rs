/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_topology::Topology;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topology = Topology::detect()?.with_max_placements(8);
    println!(
        "Identified logical processors: {}",
        topology.logical_processors().len()
    );
    println!("Default parallelism: {}", topology.parallelism_estimate());
    println!("Selected placements: {}", topology.placements().len());
    for processor in topology.logical_processors() {
        println!(
            "Processor: {}, NUMA: {:?}",
            processor.id(),
            processor.numa_node()
        );
    }
    for node in topology.numa_nodes() {
        println!(
            "NUMA: {}, processors: {:?}",
            node.id(),
            node.logical_processors()
        );
    }
    for (ordinal, placement) in topology.placements().iter().enumerate() {
        println!(
            "Slot {ordinal}: CPU {:?}, NUMA {:?}",
            placement.logical_processor(),
            placement.numa_node()
        );
    }
    Ok(())
}
