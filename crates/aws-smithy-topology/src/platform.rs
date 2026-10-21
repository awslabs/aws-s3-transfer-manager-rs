/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Private platform discovery, CPU binding, and conversion into owned topology data.
//!
//! All `many_cpus` references stay here. Its portable fallback synthesizes
//! processor identities and a memory region; use a count instead on platforms
//! where native identities are unavailable.

use crate::{BindError, DetectError, LogicalProcessor, LogicalProcessorId, NumaNodeId};
use many_cpus::{Processor, SystemHardware};
use std::{num::NonZeroUsize, panic::UnwindSafe, path::Path};

#[derive(Debug)]
pub(crate) enum Discovery {
    Processors {
        processors: Vec<LogicalProcessor>,
        selected: Vec<LogicalProcessorId>,
    },
    CountOnly(NonZeroUsize),
}

pub(crate) fn detect() -> Result<Discovery, DetectError> {
    discovery_result(|| {
        if cfg!(all(
            any(target_os = "linux", target_os = "windows"),
            not(miri)
        )) {
            discovery_from(SystemHardware::current(), reported_numa_node)
        } else {
            std::thread::available_parallelism()
                .map(Discovery::CountOnly)
                .map_err(DetectError::parallelism)
        }
    })
}

pub(crate) fn bind_current_thread(processor: LogicalProcessorId) -> Result<(), BindError> {
    if !cfg!(all(
        any(target_os = "linux", target_os = "windows"),
        not(miri)
    )) {
        // The portable implementation's pinning is a no-op.
        return Err(BindError::unsupported(processor));
    }
    binding_result(processor, || {
        bind_from(SystemHardware::current(), processor)
    })
}

fn binding_result(
    processor: LogicalProcessorId,
    bind: impl FnOnce() -> Result<(), BindError> + UnwindSafe,
) -> Result<(), BindError> {
    std::panic::catch_unwind(bind)
        .map_err(|payload| BindError::binding_panic(processor, payload))?
}

fn bind_from(hardware: &SystemHardware, processor: LogicalProcessorId) -> Result<(), BindError> {
    // Discovery's captured restrictions may differ from this thread's current
    // restrictions. Explicit binding ignores quota selection but never widens
    // the current thread's available processor set.
    let selected = hardware
        .all_processors()
        .to_builder()
        .where_available_for_current_thread()
        .filter(|candidate| candidate.id() == processor.0)
        .take_all()
        .ok_or_else(|| BindError::unavailable_processor(processor))?;
    selected.pin_current_thread_to();
    verify_binding(hardware, processor)
}

fn verify_binding(
    hardware: &SystemHardware,
    processor: LogicalProcessorId,
) -> Result<(), BindError> {
    // Query platform restrictions, not many_cpus's cached pin-status flag.
    let available = hardware
        .all_processors()
        .to_builder()
        .where_available_for_current_thread()
        .take_all();
    if available.is_some_and(|available| {
        available.len() == 1
            && available
                .iter()
                .all(|candidate| candidate.id() == processor.0)
    }) {
        Ok(())
    } else {
        Err(BindError::not_bound(processor))
    }
}

// Native discovery can panic on OS query failures. Own the error at this
// boundary, including the panic's message, without exposing third-party errors.
// The panic hook still runs; panic=abort cannot be recovered.
fn discovery_result(
    discover: impl FnOnce() -> Result<Discovery, DetectError> + UnwindSafe,
) -> Result<Discovery, DetectError> {
    std::panic::catch_unwind(discover).map_err(DetectError::discovery_panic)?
}

fn discovery_from(
    hardware: &SystemHardware,
    node_for: impl Fn(&Processor) -> Option<u32>,
) -> Result<Discovery, DetectError> {
    // Apply current-thread restrictions before quota selection: the cached
    // quota-limited set could omit every processor allowed to this thread.
    let usable = hardware
        .all_processors()
        .to_builder()
        .where_available_for_current_thread()
        .take_all()
        .ok_or_else(DetectError::no_processors)?;
    let defaults = usable
        .to_builder()
        .enforce_resource_quota()
        .take_all()
        .ok_or_else(DetectError::no_processors)?;

    Ok(Discovery::Processors {
        processors: usable
            .iter()
            .map(|processor| LogicalProcessor {
                id: LogicalProcessorId(processor.id()),
                numa_node: node_for(processor).map(NumaNodeId),
            })
            .collect(),
        selected: defaults
            .iter()
            .map(|processor| LogicalProcessorId(processor.id()))
            .collect(),
    })
}

fn reported_numa_node(processor: &Processor) -> Option<u32> {
    let node = processor.memory_region_id();
    if cfg!(target_os = "linux") {
        // many_cpus uses region zero when NUMA information is missing. Only
        // retain the association if sysfs confirms this particular CPU's node.
        // A real node zero passes this same check.
        linux_numa_node(Path::new("/sys/devices/system/cpu"), processor.id(), node)
    } else if cfg!(target_os = "windows") {
        // The native Windows implementation obtains this mapping from the
        // operating system's NUMA API.
        Some(node)
    } else {
        None
    }
}

fn linux_numa_node(cpu_root: &Path, processor: u32, node: u32) -> Option<u32> {
    cpu_root
        .join(format!("cpu{processor}/node{node}"))
        .is_dir()
        .then_some(node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Topology;
    use many_cpus::fake::{HardwareBuilder, ProcessorBuilder};

    fn fixture(quota: f64) -> SystemHardware {
        SystemHardware::fake(
            HardwareBuilder::new()
                .processor(ProcessorBuilder::new().id(42).memory_region(8))
                .processor(ProcessorBuilder::new().id(3).memory_region(2))
                .processor(ProcessorBuilder::new().id(19).memory_region(8))
                .processor(ProcessorBuilder::new().id(7).memory_region(2))
                .max_processor_time(quota),
        )
    }

    fn from_hardware(hardware: &SystemHardware) -> Topology {
        Topology::from_discovery(
            discovery_from(hardware, |processor| Some(processor.memory_region_id())).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn quota_limits_placements_without_discarding_usable_processors() {
        let topology = from_hardware(&fixture(2.5));
        assert_eq!(
            topology
                .logical_processors()
                .iter()
                .map(LogicalProcessor::id)
                .collect::<Vec<_>>(),
            [3, 7, 19, 42].map(LogicalProcessorId),
        );
        // many_cpus rounds fractional quotas down, with a minimum of one.
        assert_eq!(topology.parallelism_estimate(), 2);
        assert_eq!(topology.placements().len(), 2);
        assert!(
            topology
                .placements()
                .windows(2)
                .all(|pair| pair[0].logical_processor() < pair[1].logical_processor())
        );
        assert!(topology.placements().iter().all(|placement| {
            topology.logical_processors().iter().any(|processor| {
                Some(processor.id()) == placement.logical_processor()
                    && processor.numa_node() == placement.numa_node()
            })
        }));
    }

    #[test]
    fn current_thread_restriction_precedes_quota_selection() {
        let hardware = fixture(2.5);
        let quota_limited = hardware.processors();
        let excluded = hardware
            .all_processors()
            .iter()
            .find(|processor| {
                !quota_limited
                    .iter()
                    .any(|selected| selected.id() == processor.id())
            })
            .unwrap()
            .id();
        // This changes the fake hardware's state, never actual OS affinity.
        // Choose a CPU excluded by cached default selection, so filtering that
        // set instead of the complete inventory would fail deterministically.
        hardware
            .all_processors()
            .filter(|processor| processor.id() == excluded)
            .unwrap()
            .pin_current_thread_to();
        let topology = from_hardware(&hardware);
        assert_eq!(topology.logical_processors().len(), 1);
        assert_eq!(topology.parallelism_estimate(), 1);
        assert_eq!(
            topology.placements()[0].logical_processor(),
            Some(LogicalProcessorId(excluded)),
        );
    }

    #[test]
    fn fractional_quota_keeps_at_least_one_placement() {
        let topology = from_hardware(&fixture(0.5));
        assert_eq!(topology.logical_processors().len(), 4);
        assert_eq!(topology.parallelism_estimate(), 1);
        assert_eq!(topology.placements().len(), 1);
    }

    #[test]
    fn unavailable_numa_is_not_a_synthetic_node_zero() {
        let hardware = fixture(4.0);
        let topology =
            Topology::from_discovery(discovery_from(&hardware, |_| None).unwrap()).unwrap();
        assert!(topology.numa_nodes().is_empty());
        assert!(
            topology
                .logical_processors()
                .iter()
                .all(|processor| processor.numa_node().is_none())
        );
        assert!(
            topology
                .placements()
                .iter()
                .all(|placement| placement.numa_node().is_none())
        );
    }

    #[test]
    fn linux_association_requires_the_reported_cpu_node_link() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir_all(root.join("cpu42/node0")).unwrap();
        std::fs::create_dir_all(root.join("cpu7/node8")).unwrap();
        assert_eq!(linux_numa_node(root, 42, 0), Some(0));
        assert_eq!(linux_numa_node(root, 7, 8), Some(8));
        assert_eq!(linux_numa_node(root, 19, 0), None);
        assert_eq!(linux_numa_node(root, 42, 8), None);
    }

    #[cfg(unix)]
    #[test]
    fn linux_association_follows_node_links_and_rejects_dangling_links() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir(root.join("cpu42")).unwrap();
        std::fs::create_dir(root.join("node8")).unwrap();
        std::os::unix::fs::symlink("../node8", root.join("cpu42/node8")).unwrap();
        std::os::unix::fs::symlink("../missing", root.join("cpu42/node0")).unwrap();
        assert_eq!(linux_numa_node(root, 42, 8), Some(8));
        assert_eq!(linux_numa_node(root, 42, 0), None);
    }

    #[test]
    fn unwinding_discovery_failure_retains_message() {
        let error = discovery_result(|| panic!("hardware query failed")).unwrap_err();
        assert!(error.to_string().contains("hardware query failed"));
    }

    #[test]
    fn discovery_error_is_not_replaced_by_panic_handling() {
        let error = discovery_result(|| Err(DetectError::no_processors())).unwrap_err();
        assert_eq!(
            error.to_string(),
            "no processors usable in the discovery context",
        );
    }

    #[test]
    fn binding_uses_inventory_outside_quota_limited_selection() {
        let hardware = fixture(1.0);
        let defaults = hardware.processors();
        let target = hardware
            .all_processors()
            .iter()
            .find(|processor| {
                !defaults
                    .iter()
                    .any(|selected| selected.id() == processor.id())
            })
            .map(|processor| LogicalProcessorId(processor.id()))
            .unwrap();
        bind_from(&hardware, target).unwrap();
        let discovered = from_hardware(&hardware);
        assert_eq!(discovered.logical_processors().len(), 1);
        assert_eq!(discovered.logical_processors()[0].id(), target);
    }

    #[test]
    fn binding_rechecks_restrictions_without_widening_them() {
        let hardware = fixture(4.0);
        let topology = from_hardware(&hardware);
        let retained = topology.logical_processors()[0].id();
        let excluded = topology.logical_processors()[1].id();
        hardware
            .all_processors()
            .filter(|processor| processor.id() == retained.0)
            .unwrap()
            .pin_current_thread_to();

        let error = bind_from(&hardware, excluded).unwrap_err();
        assert!(error.to_string().contains(&excluded.to_string()));
        assert!(error.to_string().contains("not currently available"));
        let observed = from_hardware(&hardware);
        assert_eq!(observed.logical_processors().len(), 1);
        assert_eq!(observed.logical_processors()[0].id(), retained);
    }

    #[test]
    fn verification_rejects_an_unbound_or_differently_bound_thread() {
        let hardware = fixture(4.0);
        let target = LogicalProcessorId(42);
        let error = verify_binding(&hardware, target).unwrap_err();
        assert!(error.to_string().contains("did not establish"));
        bind_from(&hardware, LogicalProcessorId(3)).unwrap();
        assert!(verify_binding(&hardware, target).is_err());
    }

    #[test]
    fn unwinding_binding_failure_retains_target_and_message() {
        let target = LogicalProcessorId(42);
        let error = binding_result(target, || panic!("affinity call failed")).unwrap_err();
        assert!(error.to_string().contains("cpu-42"));
        assert!(error.to_string().contains("affinity call failed"));
    }

    #[test]
    fn binding_error_is_not_replaced_by_panic_handling() {
        let target = LogicalProcessorId(42);
        let error =
            binding_result(target, || Err(BindError::unavailable_processor(target))).unwrap_err();
        assert!(error.to_string().contains("not currently available"));
        assert!(!error.to_string().contains("native CPU binding failed"));
    }

    #[cfg(not(all(any(target_os = "linux", target_os = "windows"), not(miri))))]
    #[test]
    fn unsupported_platform_does_not_report_successful_binding() {
        let error = bind_current_thread(LogicalProcessorId(0)).unwrap_err();
        assert!(error.to_string().contains("unsupported on this platform"));
    }
}
