/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Construction-time resource sizing.
//!
//! [`resolve`] maps the signals available when a client is built to its
//! payload-memory ceiling, concurrency target, part size, and per-host
//! connection cap. It performs no detection; [`super::platform`] supplies the
//! machine facts.
//!
//! Resolution is a single pass in which each quantity reads only quantities
//! resolved before it. Part size is an input: the configured value, or
//! [`DEFAULT_PART_SIZE`].
//!
//! A configured value is used as given. The concurrency target is not bounded
//! by the connection cap or the ceiling: work beyond either waits for a
//! connection or for memory admission at run time.

use crate::metrics::unit::ByteUnit;

use super::buffer_pool::auto_capacity_bytes;
use super::platform::{
    resolve_auto_concurrency, seed_from_gbps, AutoConcurrencySource, ConnectionCap,
    ResolvedAutoConcurrency,
};

/// Part size for uploads and downloads when none is configured.
pub(crate) const DEFAULT_PART_SIZE: u64 = 8 * ByteUnit::Mebibyte.as_bytes_u64();

/// Lower bound on the automatic ceiling when memory is detected.
///
/// Holds three [`DEFAULT_PART_SIZE`] buffers for each request at the minimum
/// concurrency target, so a small host rarely waits on memory admission at that
/// target. Limited to `1 / FLOOR_MAX_RAM_DIVISOR` of detected memory.
const AUTO_FLOOR_BYTES: u64 = 256 * ByteUnit::Mebibyte.as_bytes_u64();

/// Divisor of detected memory that limits [`AUTO_FLOOR_BYTES`], so the
/// automatic ceiling stays below detected memory.
const FLOOR_MAX_RAM_DIVISOR: usize = 2;

/// Inputs to [`resolve`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Signals<'a> {
    /// Configured throughput target, in Gbps.
    pub(crate) throughput_gbps: Option<f64>,
    /// EC2 instance type.
    pub(crate) instance_type: Option<&'a str>,
    /// Usable vCPUs.
    pub(crate) vcpus: usize,
    /// Usable memory in bytes; `None` when undetected.
    pub(crate) ram_bytes: Option<usize>,
    /// Descriptor-derived per-host connection cap.
    pub(crate) connection_cap: ConnectionCap,
    /// Configured concurrency target; used as given.
    pub(crate) explicit_concurrency: Option<usize>,
    /// Configured part size; used as given.
    pub(crate) explicit_part_size: Option<u64>,
    /// Capacity of a caller-supplied pool; replaces the automatic ceiling.
    pub(crate) ceiling_override_bytes: Option<usize>,
}

/// Signal the concurrency target was derived from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConcurrencySource {
    /// A configured target was used as given.
    ConfiguredConcurrency,
    /// Seeded from a configured throughput target.
    ConfiguredThroughput,
    /// Seeded from the detected instance family's bandwidth estimate.
    InstanceFamily,
    /// Scaled from usable vCPUs, no family estimate available.
    VcpuFallback,
}

impl ConcurrencySource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ConfiguredConcurrency => "configured_concurrency",
            Self::ConfiguredThroughput => "configured_throughput",
            Self::InstanceFamily => "instance_family",
            Self::VcpuFallback => "vcpu_fallback",
        }
    }
}

impl From<AutoConcurrencySource> for ConcurrencySource {
    fn from(source: AutoConcurrencySource) -> Self {
        match source {
            AutoConcurrencySource::InstanceFamily => Self::InstanceFamily,
            AutoConcurrencySource::VcpuFallback => Self::VcpuFallback,
        }
    }
}

/// Resource sizes one client runs with, and how each was resolved.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ResourcePlan {
    /// Admission ceiling for the buffer pool.
    pub(crate) ceiling_bytes: usize,
    /// Concurrency target for the scheduler.
    pub(crate) concurrency: usize,
    /// Part size for uploads and downloads.
    pub(crate) part_size: u64,
    /// Per-host connection cap; the descriptor-derived cap.
    ///
    /// Independent of [`Self::concurrency`]: connections open on demand, so a
    /// cap above the target reserves nothing, and requests beyond a cap below it
    /// wait for a connection.
    pub(crate) connection_cap: usize,
    /// Throughput the target was seeded from, in Gbps; `None` for a configured
    /// target or when no estimate is available.
    pub(crate) throughput_gbps: Option<f64>,
    /// Signal [`Self::concurrency`] was derived from.
    pub(crate) concurrency_source: ConcurrencySource,
    /// Whether the ceiling was derived from detected memory.
    pub(crate) memory_detected: bool,
    /// Whether [`Self::part_size`] exceeds [`Self::ceiling_bytes`]. The pool
    /// rejects a reservation larger than its capacity, so a part this large
    /// cannot be transferred.
    pub(crate) part_exceeds_ceiling: bool,
}

/// Resolves the resource plan for `signals`.
///
/// Performs no I/O and does not panic. Arithmetic saturates; the concurrency
/// target and connection cap are at least one.
pub(crate) fn resolve(signals: Signals<'_>) -> ResourcePlan {
    let auto = resolve_auto_concurrency(signals.instance_type, signals.vcpus);
    let configured_throughput = signals.throughput_gbps;
    let throughput_gbps = configured_throughput.or(auto.estimated_gbps);
    let ceiling_bytes = ceiling(&signals);
    let part_size = signals.explicit_part_size.unwrap_or(DEFAULT_PART_SIZE);

    let concurrency = match signals.explicit_concurrency {
        Some(configured) => configured,
        None => derived_concurrency(&auto, throughput_gbps),
    }
    .max(1);

    ResourcePlan {
        ceiling_bytes,
        concurrency,
        part_size,
        connection_cap: signals.connection_cap.max_per_host.max(1),
        throughput_gbps: if signals.explicit_concurrency.is_some() {
            None
        } else {
            throughput_gbps
        },
        concurrency_source: if signals.explicit_concurrency.is_some() {
            ConcurrencySource::ConfiguredConcurrency
        } else if configured_throughput.is_some() {
            ConcurrencySource::ConfiguredThroughput
        } else {
            auto.source.into()
        },
        memory_detected: signals.ceiling_override_bytes.is_none()
            && signals.ram_bytes.is_some_and(|ram| ram > 0),
        part_exceeds_ceiling: part_size > ceiling_bytes as u64,
    }
}

/// Admission ceiling in bytes.
///
/// A caller-supplied capacity is used as given. Otherwise the buffer pool's
/// automatic capacity, raised to at least [`AUTO_FLOOR_BYTES`] limited to
/// `1 / FLOOR_MAX_RAM_DIVISOR` of detected memory.
fn ceiling(signals: &Signals<'_>) -> usize {
    if let Some(override_bytes) = signals.ceiling_override_bytes {
        return override_bytes;
    }
    let ram = signals.ram_bytes.filter(|&ram| ram > 0);
    let automatic = auto_capacity_bytes(ram);
    match ram {
        Some(ram) => {
            let floor = saturating_usize(AUTO_FLOOR_BYTES).min(ram / FLOOR_MAX_RAM_DIVISOR);
            automatic.max(floor)
        }
        None => automatic,
    }
}

/// Concurrency target from a throughput estimate, or the vCPU fallback.
fn derived_concurrency(auto: &ResolvedAutoConcurrency, throughput_gbps: Option<f64>) -> usize {
    match throughput_gbps {
        Some(gbps) => seed_from_gbps(gbps),
        None => auto.target,
    }
}

/// Narrows a byte count to `usize`, saturating on targets where it does not fit.
fn saturating_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

// The sizing tests model multi-GiB host RAM; those values exceed `usize` on
// 32-bit platforms, so the tests only compile and run on 64-bit. The sizing
// logic itself still compiles on 32-bit from the real `available_ram()`
// signal, which fits a 32-bit `usize` by construction.
#[cfg(all(test, target_pointer_width = "64"))]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    /// A connection cap of `max_per_host`, with no descriptor limit detected.
    fn cap(max_per_host: usize) -> ConnectionCap {
        ConnectionCap {
            max_per_host,
            descriptors: None,
        }
    }

    /// Signals for a host with ample descriptors and no configured overrides.
    fn signals<'a>(ram_bytes: Option<usize>, vcpus: usize) -> Signals<'a> {
        Signals {
            throughput_gbps: None,
            instance_type: None,
            vcpus,
            ram_bytes,
            connection_cap: cap(4096),
            explicit_concurrency: None,
            explicit_part_size: None,
            ceiling_override_bytes: None,
        }
    }

    #[test]
    fn configured_throughput_seeds_the_concurrency_target() {
        // A configured target and an instance-family estimate share one seed,
        // so equal Gbps resolve to equal targets.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(100.0);
        let plan = resolve(s);

        assert_eq!(plan.throughput_gbps, Some(100.0));
        assert_eq!(plan.concurrency, seed_from_gbps(100.0));
    }

    #[test]
    fn configured_throughput_takes_precedence_over_the_family_estimate() {
        // With both present, the configured value is used.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.instance_type = Some("m6idn.16xlarge");
        s.throughput_gbps = Some(10.0);

        assert_eq!(resolve(s).throughput_gbps, Some(10.0));
    }

    #[test]
    fn family_estimate_substitutes_for_an_unset_throughput() {
        // m6idn's flagship is 200 Gbps at 128 vCPU, so 64 vCPU estimates 100.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.instance_type = Some("m6idn.16xlarge");
        let plan = resolve(s);

        assert_eq!(plan.throughput_gbps, Some(100.0));
        assert_eq!(plan.concurrency, seed_from_gbps(100.0));
    }

    #[test]
    fn unknown_family_falls_back_to_vcpu_scaling() {
        let mut s = signals(Some(64 * GIB as usize), 16);
        s.instance_type = Some("gcp-n2-standard-16");
        let plan = resolve(s);

        assert_eq!(plan.throughput_gbps, None);
        assert_eq!(
            plan.concurrency,
            resolve_auto_concurrency(Some("gcp-n2-standard-16"), 16).target
        );
    }

    #[test]
    fn ceiling_is_a_share_of_detected_memory() {
        let plan = resolve(signals(Some(64 * GIB as usize), 16));
        assert_eq!(
            plan.ceiling_bytes,
            auto_capacity_bytes(Some(64 * GIB as usize))
        );
        assert!(plan.memory_detected);
    }

    #[test]
    fn ceiling_is_capped_on_a_large_host() {
        let ram = 768 * GIB as usize;
        let plan = resolve(signals(Some(ram), 192));
        assert_eq!(plan.ceiling_bytes, auto_capacity_bytes(Some(ram)));
        assert!(plan.ceiling_bytes < ram / 4);
    }

    #[test]
    fn ceiling_floor_takes_precedence_over_a_small_memory_share() {
        // The pool's share of 1 GiB is below the floor, and half of 1 GiB is
        // above it, so the floor sets the ceiling.
        let plan = resolve(signals(Some(GIB as usize), 2));
        assert_eq!(plan.ceiling_bytes, saturating_usize(AUTO_FLOOR_BYTES));
    }

    #[test]
    fn ceiling_floor_yields_to_half_of_memory_on_a_tiny_host() {
        // Half of 128 MiB is below the floor, so half of memory sets the ceiling.
        let plan = resolve(signals(Some(128 * MIB as usize), 1));
        assert_eq!(plan.ceiling_bytes, 64 * MIB as usize);
    }

    #[test]
    fn automatic_ceiling_never_exceeds_detected_memory() {
        for mib in [1, 16, 128, 256, 511, 512, 1024, 4096] {
            let ram = (mib * MIB) as usize;
            let ceiling = resolve(signals(Some(ram), 1)).ceiling_bytes;
            assert!(ceiling <= ram / FLOOR_MAX_RAM_DIVISOR, "ram {mib} MiB");
        }
    }

    #[test]
    fn undetected_memory_uses_the_fallback_ceiling() {
        let plan = resolve(signals(None, 4));
        assert_eq!(plan.ceiling_bytes, auto_capacity_bytes(None));
        assert!(!plan.memory_detected);
    }

    #[test]
    fn zero_detected_memory_is_treated_as_undetected() {
        let plan = resolve(signals(Some(0), 4));
        assert_eq!(plan.ceiling_bytes, auto_capacity_bytes(None));
        assert!(!plan.memory_detected);
    }

    #[test]
    fn caller_supplied_ceiling_replaces_the_detected_share() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.ceiling_override_bytes = Some(16 * MIB as usize);

        assert_eq!(resolve(s).ceiling_bytes, 16 * MIB as usize);
    }

    #[test]
    fn descriptor_cap_does_not_lower_a_derived_concurrency_target() {
        // 200 Gbps seeds 500 in flight; with 128 connections per host, requests
        // beyond 128 wait for a connection instead.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(200.0);
        s.connection_cap = cap(128);
        let plan = resolve(s);

        assert_eq!(plan.concurrency, seed_from_gbps(200.0));
        assert_eq!(plan.connection_cap, 128);
    }

    #[test]
    fn ceiling_does_not_lower_a_derived_concurrency_target() {
        // The 256 MiB floor holds far fewer 8 MiB parts than 100 Gbps seeds;
        // memory admission makes the rest wait instead.
        let mut s = signals(Some(GIB as usize), 2);
        s.throughput_gbps = Some(100.0);
        let plan = resolve(s);

        assert_eq!(plan.ceiling_bytes, saturating_usize(AUTO_FLOOR_BYTES));
        assert_eq!(plan.concurrency, seed_from_gbps(100.0));
    }

    #[test]
    fn configured_concurrency_is_used_as_given() {
        let mut s = signals(Some(GIB as usize), 2);
        s.explicit_concurrency = Some(500);
        let plan = resolve(s);

        assert_eq!(plan.concurrency, 500);
        assert_eq!(
            plan.concurrency_source,
            ConcurrencySource::ConfiguredConcurrency
        );
    }

    #[test]
    fn configured_concurrency_reports_no_throughput() {
        // The family estimate is not what the target came from.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.instance_type = Some("m6idn.16xlarge");
        s.explicit_concurrency = Some(64);

        assert_eq!(resolve(s).throughput_gbps, None);
    }

    #[test]
    fn zero_configured_concurrency_resolves_to_one() {
        // At zero the scheduler would be at capacity with nothing in flight and
        // never dispatch queued work.
        let mut s = signals(Some(8 * GIB as usize), 4);
        s.explicit_concurrency = Some(0);

        assert_eq!(resolve(s).concurrency, 1);
    }

    #[test]
    fn concurrency_source_names_the_signal_the_target_came_from() {
        let mut configured = signals(Some(8 * GIB as usize), 8);
        configured.explicit_concurrency = Some(4);
        assert_eq!(
            resolve(configured).concurrency_source,
            ConcurrencySource::ConfiguredConcurrency
        );

        let mut throughput = signals(Some(8 * GIB as usize), 8);
        throughput.throughput_gbps = Some(100.0);
        assert_eq!(
            resolve(throughput).concurrency_source,
            ConcurrencySource::ConfiguredThroughput
        );

        let mut family = signals(Some(256 * GIB as usize), 64);
        family.instance_type = Some("c5n.18xlarge");
        assert_eq!(
            resolve(family).concurrency_source,
            ConcurrencySource::InstanceFamily
        );

        // No throughput and an unrecognized family leaves only vCPU scaling.
        assert_eq!(
            resolve(signals(Some(8 * GIB as usize), 8)).concurrency_source,
            ConcurrencySource::VcpuFallback
        );
    }

    #[test]
    fn part_size_is_the_default_unless_configured() {
        // Neither a large nor a small ceiling changes the part size.
        let mut large = signals(Some(256 * GIB as usize), 64);
        large.throughput_gbps = Some(100.0);
        assert_eq!(resolve(large).part_size, DEFAULT_PART_SIZE);

        assert_eq!(
            resolve(signals(Some(GIB as usize), 2)).part_size,
            DEFAULT_PART_SIZE
        );
    }

    #[test]
    fn configured_part_size_is_used_as_given() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.explicit_part_size = Some(64 * MIB);
        let plan = resolve(s);

        assert_eq!(plan.part_size, 64 * MIB);
        assert!(!plan.part_exceeds_ceiling);
    }

    #[test]
    fn configured_part_size_beyond_the_ceiling_is_reported() {
        let mut s = signals(Some(GIB as usize), 2);
        s.explicit_part_size = Some(512 * MIB);
        let plan = resolve(s);

        assert_eq!(plan.part_size, 512 * MIB);
        assert!(plan.part_exceeds_ceiling);
    }

    #[test]
    fn part_size_equal_to_the_ceiling_is_not_reported() {
        // One part fits; requests wait for each other instead.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.ceiling_override_bytes = Some(DEFAULT_PART_SIZE as usize);
        let plan = resolve(s);

        assert!(!plan.part_exceeds_ceiling);
        assert!(!plan.memory_detected);
    }

    #[test]
    fn configured_pool_smaller_than_the_default_part_is_reported() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.ceiling_override_bytes = Some(4 * MIB as usize);

        assert!(resolve(s).part_exceeds_ceiling);
    }

    #[test]
    fn automatic_ceiling_too_small_for_the_default_part_is_reported() {
        // Half of 8 MiB is below one 8 MiB part.
        let plan = resolve(signals(Some(8 * MIB as usize), 1));

        assert_eq!(plan.ceiling_bytes, 4 * MIB as usize);
        assert!(plan.part_exceeds_ceiling);
    }

    #[test]
    fn connection_cap_is_not_narrowed_to_the_concurrency_target() {
        // A small target leaves the descriptor-derived cap unchanged.
        let mut s = signals(Some(8 * GIB as usize), 4);
        s.explicit_concurrency = Some(12);
        let plan = resolve(s);

        assert_eq!(plan.concurrency, 12);
        assert_eq!(plan.connection_cap, 4096);
    }

    #[test]
    fn connection_cap_is_bounded_by_the_descriptor_cap() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(200.0);
        s.connection_cap = cap(64);

        assert_eq!(resolve(s).connection_cap, 64);
    }

    #[test]
    fn every_signal_combination_resolves_within_its_contract() {
        // Sweeps the extremes of each independent signal: the target and cap are
        // at least one, the cap is the descriptor-derived cap, a configured
        // target is kept, and part size is the configured value or the default.
        let rams = [
            None,
            Some(0),
            Some(64 * MIB as usize),
            Some(768 * GIB as usize),
        ];
        let parts = [None, Some(5 * MIB), Some(5 * GIB), Some(u64::MAX)];
        let concurrencies = [None, Some(0), Some(1), Some(usize::MAX)];

        for ram in rams {
            for part in parts {
                for concurrency in concurrencies {
                    for descriptor_cap in [1, 10, 4096] {
                        let mut s = signals(ram, 1);
                        s.explicit_part_size = part;
                        s.explicit_concurrency = concurrency;
                        s.connection_cap = cap(descriptor_cap);
                        let plan = resolve(s);

                        assert!(plan.concurrency >= 1);
                        assert!(plan.ceiling_bytes > 0);
                        assert_eq!(plan.connection_cap, descriptor_cap);
                        if let Some(configured) = concurrency {
                            assert_eq!(plan.concurrency, configured.max(1));
                        }
                        assert_eq!(plan.part_size, part.unwrap_or(DEFAULT_PART_SIZE));
                    }
                }
            }
        }
    }

    #[test]
    fn saturating_narrowing_matches_the_target_width() {
        assert_eq!(saturating_usize(0), 0);
        assert_eq!(saturating_usize(MIB), MIB as usize);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(saturating_usize(u64::MAX), u64::MAX as usize);
        #[cfg(target_pointer_width = "32")]
        assert_eq!(saturating_usize(u64::MAX), usize::MAX);
    }
}
