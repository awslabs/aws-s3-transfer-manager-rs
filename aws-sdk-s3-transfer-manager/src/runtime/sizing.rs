/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Construction-time resource sizing policy.
//!
//! [`resolve`] turns the machine and configuration signals available when a
//! client is built into the memory ceiling, concurrency target, part size, and
//! per-host connection cap the client runs with. It is policy over the facts
//! that [`super::platform`] detects, and performs no detection itself.
//!
//! Resolution is one pass in a fixed order, so each quantity depends only on
//! quantities already resolved. Part size is not derived: it is the configured
//! value when set and a fixed default otherwise, which keeps it an input to the
//! concurrency bound rather than an output of it.
//!
//! Two rules govern the pass. An explicitly configured value is used as given
//! and is never narrowed. Every derived value is bounded by what the binding
//! constraint can actually sustain, so a client does not reserve capability it
//! cannot use.

use crate::metrics::unit::ByteUnit;

use super::platform::{
    resolve_auto_concurrency, seed_from_gbps, AutoConcurrencySource, ResolvedAutoConcurrency,
};

/// Part-sized buffers one concurrency slot may hold: one filling from the
/// source, one in flight to S3, and one draining to the consumer.
const BUFFERS_PER_SLOT: u64 = 3;

/// Part size for uploads and downloads when none is configured.
pub(crate) const DEFAULT_PART_SIZE: u64 = 8 * ByteUnit::Mebibyte.as_bytes_u64();

/// Divisor applied to detected memory when choosing the automatic ceiling.
const RAM_SHARE_DIVISOR: usize = 4;

/// Upper bound on the automatic ceiling.
const AUTO_CEILING_MAX_BYTES: u64 = 32 * ByteUnit::Gibibyte.as_bytes_u64();

/// Lower bound on the automatic ceiling.
///
/// Takes precedence over the detected-memory share, so a host with very little
/// memory still receives a ceiling that can fund one concurrency slot.
const AUTO_FLOOR_BYTES: u64 = 256 * ByteUnit::Mebibyte.as_bytes_u64();

/// Automatic ceiling when memory cannot be detected.
const NO_RAM_FALLBACK_BYTES: u64 = 2 * ByteUnit::Gibibyte.as_bytes_u64();

/// In-flight work items one connection sustains.
///
/// Bounds the concurrency target by the connection cap: a work item occupies a
/// connection for the duration of its request, so a target above the reachable
/// connection count cannot be realized.
const INFLIGHT_PER_CONNECTION: usize = 1;

/// Signals available when a client is constructed.
///
/// `descriptor_cap` is the descriptor-derived per-host connection cap, which is
/// independent of the concurrency target. `ceiling_override_bytes` carries the
/// capacity of a caller-supplied pool or byte limit, which replaces the
/// detected-memory ceiling. The `explicit_*` fields carry configured values,
/// which are used as given.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Signals<'a> {
    /// Configured throughput target in Gbps, if set and valid.
    pub(crate) throughput_gbps: Option<f64>,
    /// EC2 instance type, used to estimate throughput when none is configured.
    pub(crate) instance_type: Option<&'a str>,
    /// Usable vCPU count.
    pub(crate) vcpus: usize,
    /// Usable memory in bytes, or `None` when undetectable.
    pub(crate) ram_bytes: Option<usize>,
    /// Descriptor-derived per-host connection cap.
    pub(crate) descriptor_cap: usize,
    /// Configured concurrency target.
    pub(crate) explicit_concurrency: Option<usize>,
    /// Configured part size.
    pub(crate) explicit_part_size: Option<u64>,
    /// Capacity of a caller-supplied memory ceiling.
    pub(crate) ceiling_override_bytes: Option<usize>,
}

/// Which signal the concurrency target was derived from, before any bound.
///
/// Distinct from [`ConcurrencyBound`], which reports what narrowed that target.
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

/// What bounded the concurrency target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConcurrencyBound {
    /// The configured target was used as given.
    Configured,
    /// The throughput estimate was not reduced.
    Throughput,
    /// The connection cap reachable with the descriptor limit.
    Connections,
    /// The part demand the memory ceiling can fund.
    Memory,
}

impl ConcurrencyBound {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Configured => "configured",
            Self::Throughput => "throughput",
            Self::Connections => "connections",
            Self::Memory => "memory",
        }
    }
}

/// Resolved sizing for one client.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Sizing {
    /// Admission ceiling for the shared buffer pool.
    pub(crate) ceiling_bytes: usize,
    /// In-flight work target installed in the scheduler.
    pub(crate) concurrency: usize,
    /// Part size for uploads and downloads.
    pub(crate) part_size: u64,
    /// Maximum connections to one origin.
    pub(crate) connection_cap: usize,
    /// Largest concurrency the ceiling funds at `part_size`.
    ///
    /// A target above this value is bounded by memory admission rather than by
    /// the scheduler.
    pub(crate) max_funded_concurrency: usize,
    /// Throughput the pass resolved, in Gbps, when one was available.
    pub(crate) throughput_gbps: Option<f64>,
    /// Which signal [`Self::concurrency`] was derived from.
    pub(crate) concurrency_source: ConcurrencySource,
    /// Which signal bounded [`Self::concurrency`].
    pub(crate) concurrency_bound: ConcurrencyBound,
    /// Whether memory was detected rather than assumed.
    pub(crate) memory_detected: bool,
    /// Whether the configured concurrency target exceeds what the ceiling and
    /// connection cap sustain.
    pub(crate) configured_concurrency_unfunded: bool,
    /// Whether the ceiling cannot fund one concurrency slot at the configured
    /// part size.
    pub(crate) configured_part_size_unfunded: bool,
}

/// Resolves sizing from one set of signals.
///
/// Never panics and performs no I/O. All arithmetic saturates, so no
/// combination of signals produces a zero concurrency target.
pub(crate) fn resolve(signals: Signals<'_>) -> Sizing {
    let auto = resolve_auto_concurrency(signals.instance_type, signals.vcpus);
    let configured_throughput = configured_throughput(&signals);
    let throughput_gbps = configured_throughput.or(auto.estimated_gbps);
    let ceiling_bytes = ceiling(&signals);
    let part_size = signals.explicit_part_size.unwrap_or(DEFAULT_PART_SIZE);

    let connections_bound = signals
        .descriptor_cap
        .saturating_mul(INFLIGHT_PER_CONNECTION);
    let memory_bound = funded_concurrency(ceiling_bytes, part_size);

    let (concurrency, concurrency_bound, configured_concurrency_unfunded) =
        match signals.explicit_concurrency {
            // A configured target is authoritative. Memory admission and
            // connection acquisition still bound execution, so an unfundable
            // target costs backpressure rather than correctness.
            Some(configured) => {
                let target = configured.max(1);
                let unfunded = target > connections_bound.min(memory_bound);
                (target, ConcurrencyBound::Configured, unfunded)
            }
            None => {
                let target = derived_concurrency(&auto, throughput_gbps);
                let bounded = target.min(connections_bound).min(memory_bound).max(1);
                let bound = if bounded == target {
                    ConcurrencyBound::Throughput
                } else if connections_bound <= memory_bound {
                    ConcurrencyBound::Connections
                } else {
                    ConcurrencyBound::Memory
                };
                (bounded, bound, false)
            }
        };

    Sizing {
        ceiling_bytes,
        concurrency,
        part_size,
        // A connection beyond the concurrency target has no work to carry.
        connection_cap: signals.descriptor_cap.min(concurrency).max(1),
        max_funded_concurrency: memory_bound,
        throughput_gbps,
        concurrency_source: if signals.explicit_concurrency.is_some() {
            ConcurrencySource::ConfiguredConcurrency
        } else if configured_throughput.is_some() {
            ConcurrencySource::ConfiguredThroughput
        } else {
            auto.source.into()
        },
        concurrency_bound,
        memory_detected: signals.ram_bytes.is_some_and(|ram| ram > 0),
        configured_concurrency_unfunded,
        configured_part_size_unfunded: signals
            .explicit_part_size
            .is_some_and(|part| BUFFERS_PER_SLOT.saturating_mul(part) > ceiling_bytes as u64),
    }
}

/// Configured throughput in Gbps, or `None` when none was set.
///
/// A value that is not finite or not positive is treated as unset, matching the
/// sentinel that distinguishes "no target" from a target of zero.
fn configured_throughput(signals: &Signals<'_>) -> Option<f64> {
    signals
        .throughput_gbps
        .filter(|gbps| gbps.is_finite() && *gbps > 0.0)
}

/// Admission ceiling in bytes.
///
/// A caller-supplied ceiling is used as given. Otherwise the ceiling is a share
/// of detected memory, bounded above and below, and a fixed value when memory
/// cannot be detected. The ceiling bounds admission only; it is not reserved at
/// construction, so it does not reflect expected demand.
fn ceiling(signals: &Signals<'_>) -> usize {
    if let Some(override_bytes) = signals.ceiling_override_bytes {
        return override_bytes;
    }
    match signals.ram_bytes.filter(|&ram| ram > 0) {
        Some(ram) => {
            let share = (ram / RAM_SHARE_DIVISOR) as u64;
            saturating_usize(share.clamp(AUTO_FLOOR_BYTES, AUTO_CEILING_MAX_BYTES))
        }
        None => saturating_usize(NO_RAM_FALLBACK_BYTES),
    }
}

/// Concurrency target before any bound is applied.
fn derived_concurrency(auto: &ResolvedAutoConcurrency, throughput_gbps: Option<f64>) -> usize {
    match throughput_gbps {
        Some(gbps) => seed_from_gbps(gbps),
        None => auto.target,
    }
}

/// Concurrency slots a ceiling funds at a given part size, at least one.
fn funded_concurrency(ceiling_bytes: usize, part_size: u64) -> usize {
    let per_slot = BUFFERS_PER_SLOT.saturating_mul(part_size);
    if per_slot == 0 {
        return usize::MAX;
    }
    // The quotient never exceeds `ceiling_bytes`, so it always fits `usize`.
    ((ceiling_bytes as u64 / per_slot) as usize).max(1)
}

/// Narrows a byte count to `usize`, saturating on targets where it does not fit.
fn saturating_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    /// Signals for a host with ample descriptors and no configured overrides.
    fn signals<'a>(ram_bytes: Option<usize>, vcpus: usize) -> Signals<'a> {
        Signals {
            throughput_gbps: None,
            instance_type: None,
            vcpus,
            ram_bytes,
            descriptor_cap: 4096,
            explicit_concurrency: None,
            explicit_part_size: None,
            ceiling_override_bytes: None,
        }
    }

    #[test]
    fn configured_throughput_is_the_sizing_root() {
        // A configured target drives concurrency through the same seed the
        // instance-family estimate uses, so both paths agree at equal Gbps.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(100.0);
        let sizing = resolve(s);

        assert_eq!(sizing.throughput_gbps, Some(100.0));
        assert_eq!(sizing.concurrency, seed_from_gbps(100.0));
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Throughput);
    }

    #[test]
    fn configured_throughput_takes_precedence_over_the_family_estimate() {
        // Both signals are present; the configured value wins so a caller can
        // correct a detection that does not match the host's real capability.
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
        let sizing = resolve(s);

        assert_eq!(sizing.throughput_gbps, Some(100.0));
        assert_eq!(sizing.concurrency, seed_from_gbps(100.0));
    }

    #[test]
    fn invalid_configured_throughput_is_treated_as_unset() {
        // Zero is the unset sentinel; the remaining values cannot describe a
        // rate, and accepting any of them would produce a target of one.
        for gbps in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut s = signals(Some(8 * GIB as usize), 4);
            s.throughput_gbps = Some(gbps);
            assert_eq!(resolve(s).throughput_gbps, None, "gbps {gbps}");
        }
    }

    #[test]
    fn unknown_family_falls_back_to_vcpu_scaling() {
        let mut s = signals(Some(64 * GIB as usize), 16);
        s.instance_type = Some("gcp-n2-standard-16");
        let sizing = resolve(s);

        assert_eq!(sizing.throughput_gbps, None);
        assert_eq!(
            sizing.concurrency,
            resolve_auto_concurrency(Some("gcp-n2-standard-16"), 16).target
        );
    }

    #[test]
    fn ceiling_is_a_share_of_detected_memory() {
        let sizing = resolve(signals(Some(64 * GIB as usize), 16));
        assert_eq!(sizing.ceiling_bytes, 64 * GIB as usize / RAM_SHARE_DIVISOR);
        assert!(sizing.memory_detected);
    }

    #[test]
    fn ceiling_is_capped_on_a_large_host() {
        let sizing = resolve(signals(Some(768 * GIB as usize), 192));
        assert_eq!(
            sizing.ceiling_bytes,
            saturating_usize(AUTO_CEILING_MAX_BYTES)
        );
    }

    #[test]
    fn ceiling_floor_takes_precedence_over_a_small_memory_share() {
        // A quarter of 1 GiB is below the floor. The floor wins, so the pool can
        // still fund a slot; the ceiling bounds admission and reserves nothing.
        let sizing = resolve(signals(Some(GIB as usize), 2));
        assert_eq!(sizing.ceiling_bytes, saturating_usize(AUTO_FLOOR_BYTES));
    }

    #[test]
    fn undetected_memory_uses_the_fallback_ceiling() {
        let sizing = resolve(signals(None, 4));
        assert_eq!(
            sizing.ceiling_bytes,
            saturating_usize(NO_RAM_FALLBACK_BYTES)
        );
        assert!(!sizing.memory_detected);
    }

    #[test]
    fn zero_detected_memory_is_treated_as_undetected() {
        let sizing = resolve(signals(Some(0), 4));
        assert_eq!(
            sizing.ceiling_bytes,
            saturating_usize(NO_RAM_FALLBACK_BYTES)
        );
        assert!(!sizing.memory_detected);
    }

    #[test]
    fn caller_supplied_ceiling_replaces_the_detected_share() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.ceiling_override_bytes = Some(16 * MIB as usize);

        assert_eq!(resolve(s).ceiling_bytes, 16 * MIB as usize);
    }

    #[test]
    fn descriptor_cap_bounds_a_derived_concurrency_target() {
        // 200 Gbps seeds 500 in-flight, but 128 connections cannot carry it.
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(200.0);
        s.descriptor_cap = 128;
        let sizing = resolve(s);

        assert_eq!(sizing.concurrency, 128);
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Connections);
        assert_eq!(sizing.connection_cap, 128);
    }

    #[test]
    fn ceiling_bounds_a_derived_concurrency_target() {
        // The floor ceiling funds 256 MiB / (3 x 8 MiB) slots, far below the
        // target 100 Gbps seeds.
        let mut s = signals(Some(GIB as usize), 2);
        s.throughput_gbps = Some(100.0);
        let sizing = resolve(s);

        let funded = funded_concurrency(saturating_usize(AUTO_FLOOR_BYTES), DEFAULT_PART_SIZE);
        assert_eq!(sizing.concurrency, funded);
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Memory);
    }

    #[test]
    fn configured_concurrency_is_not_narrowed_by_a_bound() {
        // Configured values are authoritative. The target survives a ceiling
        // that cannot fund it, and the condition is reported instead.
        let mut s = signals(Some(GIB as usize), 2);
        s.explicit_concurrency = Some(500);
        let sizing = resolve(s);

        assert_eq!(sizing.concurrency, 500);
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Configured);
        assert!(sizing.configured_concurrency_unfunded);
    }

    #[test]
    fn configured_concurrency_within_the_bounds_is_not_reported_unfunded() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.explicit_concurrency = Some(64);
        let sizing = resolve(s);

        assert_eq!(sizing.concurrency, 64);
        assert!(!sizing.configured_concurrency_unfunded);
    }

    #[test]
    fn zero_configured_concurrency_resolves_to_one() {
        // The scheduler requires a target of at least one: at zero, "at
        // capacity" would hold with nothing in flight and strand queued work.
        let mut s = signals(Some(8 * GIB as usize), 4);
        s.explicit_concurrency = Some(0);

        assert_eq!(resolve(s).concurrency, 1);
    }

    #[test]
    fn concurrency_source_names_the_signal_the_target_came_from() {
        // Reported independently of what bounded the target, so a log reader can
        // tell a vCPU-fallback seed from a family estimate.
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
        // Part size does not respond to the ceiling or the concurrency target.
        // A ceiling large enough to fund a larger part does not grow it, and a
        // ceiling too small to fund it across the derived target lowers
        // concurrency instead.
        let mut large = signals(Some(256 * GIB as usize), 64);
        large.throughput_gbps = Some(100.0);
        assert_eq!(resolve(large).part_size, DEFAULT_PART_SIZE);

        let sizing = resolve(signals(Some(GIB as usize), 2));
        assert_eq!(sizing.part_size, DEFAULT_PART_SIZE);
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Memory);
        assert_eq!(
            sizing.concurrency,
            funded_concurrency(sizing.ceiling_bytes, DEFAULT_PART_SIZE)
        );
    }

    #[test]
    fn configured_part_size_is_used_as_given() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.explicit_part_size = Some(64 * MIB);
        let sizing = resolve(s);

        assert_eq!(sizing.part_size, 64 * MIB);
        assert!(!sizing.configured_part_size_unfunded);
    }

    #[test]
    fn configured_part_size_beyond_the_ceiling_is_reported() {
        let mut s = signals(Some(GIB as usize), 2);
        s.explicit_part_size = Some(512 * MIB);
        let sizing = resolve(s);

        assert_eq!(sizing.part_size, 512 * MIB);
        assert!(sizing.configured_part_size_unfunded);
    }

    #[test]
    fn configured_part_size_bounds_concurrency_before_it_is_resolved() {
        // The configured part size sizes the memory bound, so a large part
        // reduces the funded slot count.
        let mut s = signals(Some(8 * GIB as usize), 8);
        s.throughput_gbps = Some(100.0);
        s.explicit_part_size = Some(256 * MIB);
        let sizing = resolve(s);

        assert_eq!(
            sizing.concurrency,
            funded_concurrency(sizing.ceiling_bytes, 256 * MIB)
        );
        assert_eq!(sizing.concurrency_bound, ConcurrencyBound::Memory);
    }

    #[test]
    fn connection_cap_never_exceeds_the_concurrency_target() {
        // Connections beyond the target have no work to carry.
        let mut s = signals(Some(8 * GIB as usize), 4);
        s.explicit_concurrency = Some(12);
        let sizing = resolve(s);

        assert_eq!(sizing.concurrency, 12);
        assert_eq!(sizing.connection_cap, 12);
    }

    #[test]
    fn connection_cap_is_bounded_by_the_descriptor_cap() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(200.0);
        s.descriptor_cap = 64;

        assert_eq!(resolve(s).connection_cap, 64);
    }

    #[test]
    fn funded_concurrency_is_published_for_the_resolved_part_size() {
        let mut s = signals(Some(256 * GIB as usize), 64);
        s.throughput_gbps = Some(100.0);
        let sizing = resolve(s);

        assert_eq!(
            sizing.max_funded_concurrency,
            funded_concurrency(sizing.ceiling_bytes, sizing.part_size)
        );
        assert!(sizing.max_funded_concurrency >= sizing.concurrency);
    }

    #[test]
    fn every_signal_combination_resolves_within_its_contract() {
        // Sweeps the extremes of each independent signal. Pins the invariants
        // the caller relies on: a usable concurrency target, the default part
        // size when none is configured, and a cap that carries the target.
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
                        s.descriptor_cap = descriptor_cap;
                        let sizing = resolve(s);

                        assert!(sizing.concurrency >= 1);
                        assert!(sizing.ceiling_bytes > 0);
                        assert!(sizing.connection_cap >= 1);
                        assert!(sizing.connection_cap <= sizing.concurrency);
                        assert!(sizing.max_funded_concurrency >= 1);
                        assert_eq!(sizing.part_size, part.unwrap_or(DEFAULT_PART_SIZE));
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
