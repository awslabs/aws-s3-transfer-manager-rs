/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_execution::{ManagedRuntime, topology::Topology};
use std::{error::Error, thread, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = ManagedRuntime::builder()
        .topology(Topology::detect()?.with_max_placements(4))
        .thread_name_prefix("example")
        .build()?;
    let execution = runtime.handle();
    let mut tasks = Vec::new();
    for partition in execution.partitions() {
        let id = partition.id();
        println!(
            "{id}: target {:?}, bound {:?}",
            partition.placement().logical_processor(),
            partition.bound_processor()
        );
        let local = execution.clone();
        tasks.push(partition.spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(local.current_partition().unwrap().id(), id);
            (id, thread::current().name().unwrap().to_owned())
        }));
    }

    // The managed threads drive the tasks and timers. This caller runtime only
    // awaits their results and shutdown completion.
    let caller = tokio::runtime::Builder::new_current_thread().build()?;
    caller.block_on(async move {
        for task in tasks {
            let (id, name) = task.await?;
            println!("{id} completed on {name}");
        }
        runtime.shutdown().await?;
        Ok::<(), Box<dyn Error>>(())
    })
}
