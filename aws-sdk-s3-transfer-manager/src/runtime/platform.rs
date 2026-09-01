/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Machine-resource detection for sizing the connection and memory budgets.
//!
//! Detection is best-effort: a failure yields a conservative cap, never an
//! unbounded one.

use crate::metrics::unit::ByteUnit;
use std::path::{Path, PathBuf};

// Connection-cap sizing from the descriptor limit and memory budget. The
// consumer is the managed-runtime connection pool, which caps `max_connections`
// from these. The pool wiring does not exist yet, so the fd-budget items below
// carry `#[allow(dead_code)]`; `MIN_CONN`/`ABSOLUTE_MAX_CONN` are already live as
// the concurrency-seed bounds (in-flight requests are connection-bound under
// HTTP/1, so the connection bounds bound the seed too).

/// Fraction of the process file-descriptor limit the connection pool may use.
/// The remainder is headroom for disk sinks, the directory walker, logging, and
/// other descriptors the process holds.
#[allow(dead_code)]
const POOL_FD_BUDGET_FRACTION: f64 = 0.5;

/// Ceiling on pooled connections regardless of descriptor headroom, and the
/// upper bound on the concurrency seed. Bounds request fan-out against a single
/// S3 endpoint, and is the cap on platforms with no low descriptor limit.
const ABSOLUTE_MAX_CONN: usize = 10_000;

/// Floor so a low descriptor limit does not cap the pool below a usable count,
/// and the lower bound on the concurrency seed.
const MIN_CONN: usize = 10;

/// Global cap on pooled connections from the descriptor limit alone, used where
/// the memory budget is not yet known (the runtime-builder default). Machine-
/// derived and independent of the throughput target. `clamp(POOL_FD_BUDGET_FRACTION
/// x RLIMIT_NOFILE, MIN_CONN, ABSOLUTE_MAX_CONN)` on Unix; `ABSOLUTE_MAX_CONN`
/// where there is no low descriptor limit or detection fails.
#[allow(dead_code)]
pub(crate) fn connection_cap() -> usize {
    cap(fd_limit(), usize::MAX)
}

/// Global connection cap including the memory term: a connection that can never
/// hold a chunk is wasted, so the budget caps connections at the chunk count it
/// can fund. `clamp(min(fd_budget, budget_capacity / chunk), MIN_CONN,
/// ABSOLUTE_MAX_CONN)`.
#[allow(dead_code)]
pub(crate) fn connection_cap_with_memory(
    budget_capacity_bytes: usize,
    chunk_bytes: usize,
) -> usize {
    cap(fd_limit(), budget_capacity_bytes / chunk_bytes.max(1))
}

/// Apply the descriptor fraction, take the smaller of it and the memory-funded
/// connection count, and clamp to `[MIN_CONN, ABSOLUTE_MAX_CONN]`.
#[allow(dead_code)]
fn cap(fd_limit: Option<usize>, mem_conns: usize) -> usize {
    let fd_budget = fd_limit
        .map(|n| (n as f64 * POOL_FD_BUDGET_FRACTION) as usize)
        .unwrap_or(ABSOLUTE_MAX_CONN);
    fd_budget.min(mem_conns).clamp(MIN_CONN, ABSOLUTE_MAX_CONN)
}

/// Soft `RLIMIT_NOFILE` on Unix; `None` where there is no low descriptor limit
/// or detection fails.
///
/// Ref: getrlimit(2) <https://man7.org/linux/man-pages/man2/getrlimit.2.html>
#[allow(dead_code)]
#[cfg(unix)]
fn fd_limit() -> Option<usize> {
    use nix::sys::resource::{getrlimit, Resource};
    match getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft, _hard)) if soft > 0 => usize::try_from(soft).ok(),
        _ => None,
    }
}

#[allow(dead_code)]
#[cfg(not(unix))]
fn fd_limit() -> Option<usize> {
    None
}

/// Share of detected RAM the memory budget may take under `Auto`. The budget is
/// a good-citizen ceiling: the remainder is left for the OS page cache
/// (download-to-disk), the rest of this process, and any co-tenant on the box.
const SAFE_MEM_FRACTION: f64 = 0.25;

/// Floor on the resolved budget. Funds a usable prefetch pipeline (64 chunks at
/// the 8 MiB accounting unit) on a small or memory-constrained box.
const MIN_BUDGET_BYTES: usize = 512 * ByteUnit::Mebibyte.as_bytes_usize();

/// Ceiling on the resolved budget. Beyond this, more buffer does not raise
/// sustained throughput, so a larger box gains nothing and its unclaimed share
/// stays available to everything else. Caps the effective fraction on big boxes.
const MAX_BUDGET_BYTES: usize = 32 * ByteUnit::Gibibyte.as_bytes_usize();

/// Budget when RAM cannot be detected (non-Linux, or a read failure). Bounded,
/// large enough for one transfer's pipeline.
const UNDETECTABLE_MEM_BYTES: usize = 2 * ByteUnit::Gibibyte.as_bytes_usize();

/// Memory budget under `MemoryBudgetConfig::Auto`: `SAFE_MEM_FRACTION` of
/// detected RAM, rounded to a power of two and clamped to
/// `[MIN_BUDGET_BYTES, MAX_BUDGET_BYTES]`; `UNDETECTABLE_MEM_BYTES` when RAM is
/// unknown. `ram_bytes` comes from the [`MachineProfile`] so detection happens
/// once, not re-read here.
///
/// The budget is a backstop, not the operating point: the concurrency
/// controller settles at line rate well below it, and it binds only when a slow
/// consumer backs parts up in the prefetch buffer. RAM is an imprecise proxy for
/// that ceiling — network bandwidth sets the real pipeline depth, and bandwidth
/// is uncorrelated with RAM across instance families (m5.24xlarge and
/// m5n.24xlarge share 384 GiB of RAM but differ 4x in bandwidth). The clamp
/// keeps the estimate safe at both ends; NIC-aware sizing is a separate refinement.
pub(crate) fn machine_safe_mem(ram_bytes: Option<usize>) -> usize {
    auto_budget(ram_bytes)
}

/// `Auto` policy as a pure function of detected RAM.
fn auto_budget(ram: Option<usize>) -> usize {
    match ram {
        Some(bytes) => {
            round_pow2(scale(bytes, SAFE_MEM_FRACTION)).clamp(MIN_BUDGET_BYTES, MAX_BUDGET_BYTES)
        }
        None => UNDETECTABLE_MEM_BYTES,
    }
}

/// Memory budget for an explicit `MemoryBudgetConfig::Fraction`. The fraction is
/// clamped to `(0.0, 1.0]` (a non-finite or non-positive value falls back to
/// `SAFE_MEM_FRACTION`); the result is floored at `MIN_BUDGET_BYTES` but not
/// capped — an explicit fraction is taken at the caller's word.
/// `UNDETECTABLE_MEM_BYTES` when RAM is unknown. `ram_bytes` comes from the
/// [`MachineProfile`].
pub(crate) fn mem_for_fraction(ram_bytes: Option<usize>, fraction: f64) -> usize {
    if !(fraction.is_finite() && fraction > 0.0) || fraction > 1.0 {
        tracing::debug!(
            requested = fraction,
            "memory budget fraction outside (0.0, 1.0]; clamping"
        );
    }
    mem_for_fraction_from(ram_bytes, fraction)
}

/// `Fraction` policy as a pure function of detected RAM.
fn mem_for_fraction_from(ram: Option<usize>, fraction: f64) -> usize {
    let fraction = if fraction.is_finite() && fraction > 0.0 {
        fraction.min(1.0)
    } else {
        SAFE_MEM_FRACTION
    };
    match ram {
        Some(bytes) => scale(bytes, fraction).max(MIN_BUDGET_BYTES),
        None => UNDETECTABLE_MEM_BYTES,
    }
}

/// Apply a fraction to a byte count. The `f64` cast saturates rather than
/// overflowing; callers validate the fraction first.
fn scale(bytes: usize, fraction: f64) -> usize {
    (bytes as f64 * fraction) as usize
}

/// Round to the nearer power of two; ties round up. `0` maps to `0`. Rounding to
/// nearest (rather than `next_power_of_two`'s round-up) keeps the budget near the
/// intended fraction instead of overshooting when the input sits just above a
/// power of two.
fn round_pow2(n: usize) -> usize {
    match n.checked_next_power_of_two() {
        Some(upper) if upper == n => n,
        Some(upper) => {
            let lower = upper >> 1;
            if n - lower < upper - n {
                lower
            } else {
                upper
            }
        }
        // `n` exceeds the largest power of two: that power is the nearest.
        None => 1usize << (usize::BITS - 1),
    }
}

/// Usable RAM from Linux-compatible procfs and cgroup interfaces.
///
/// Android exposes the same kernel interfaces even though its runtime page
/// size and userspace environment may differ from a conventional Linux host.
/// The smaller of physical memory and the process limit wins when both exist.
#[cfg(any(target_os = "android", target_os = "linux"))]
pub(crate) fn available_ram() -> Option<usize> {
    match (meminfo_total(), cgroup_mem_limit()) {
        (Some(total), Some(cgroup)) => Some(total.min(cgroup)),
        (total, None) => total,
        (None, cgroup) => cgroup,
    }
}

#[cfg(not(any(
    target_os = "android",
    target_os = "freebsd",
    target_os = "linux",
    target_os = "macos",
    target_os = "windows"
)))]
pub(crate) fn available_ram() -> Option<usize> {
    None
}

#[cfg(target_os = "freebsd")]
pub(crate) fn available_ram() -> Option<usize> {
    // hw.physmem is physical memory in bytes.
    // Ref: sysctlbyname(3)
    // <https://man.freebsd.org/cgi/man.cgi?query=sysctlbyname&sektion=3>
    let mut bytes: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `bytes` is writable for the input value of `len`; `hw.physmem`
    // is nul-terminated, and null `newp` with zero `newlen` performs a read.
    let result = unsafe {
        libc::sysctlbyname(
            c"hw.physmem".as_ptr(),
            (&mut bytes as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || len != std::mem::size_of::<u64>() || bytes == 0 {
        return None;
    }
    usize::try_from(bytes).ok()
}

#[cfg(target_os = "macos")]
pub(crate) fn available_ram() -> Option<usize> {
    // hw.memsize is total physical memory in bytes; macOS has no cgroup.
    // Ref: sysctl(3), hw.memsize key
    // <https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/sysctl.3.html>
    let mut bytes: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: sysctlbyname writes a u64 into `bytes`; `len` holds its size. The
    // name is a valid nul-terminated string.
    let rc = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut bytes as *mut u64).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && bytes > 0 {
        usize::try_from(bytes).ok()
    } else {
        None
    }
}

#[cfg(target_os = "windows")]
pub(crate) fn available_ram() -> Option<usize> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    // ullTotalPhys is total physical memory in bytes.
    // Ref: GlobalMemoryStatusEx / MEMORYSTATUSEX
    // <https://learn.microsoft.com/windows/win32/api/sysinfoapi/nf-sysinfoapi-globalmemorystatusex>
    // SAFETY: a zeroed MEMORYSTATUSEX with dwLength set is the documented input;
    // GlobalMemoryStatusEx fills it and returns nonzero on success.
    let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    status.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
    let ok = unsafe { GlobalMemoryStatusEx(&mut status) };
    if ok != 0 && status.ullTotalPhys > 0 {
        usize::try_from(status.ullTotalPhys).ok()
    } else {
        None
    }
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn meminfo_total() -> Option<usize> {
    parse_meminfo_total(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// Effective cgroup memory limit for this process, container-aware: resolve the
/// process's cgroup path from /proc/self/cgroup, find the controller mount from
/// /proc/self/mountinfo, and take the min of `memory.max` (v2) /
/// `memory.limit_in_bytes` (v1) from the leaf cgroup up to the mount root (a
/// parent cgroup may impose a tighter limit). Tries v2, then v1 (hybrid). `None`
/// when no limit is set, then `available_ram` uses physical RAM.
///
/// Refs: cgroups(7) /proc/[pid]/cgroup <https://man7.org/linux/man-pages/man7/cgroups.7.html>;
/// cgroup v2 memory.max <https://docs.kernel.org/admin-guide/cgroup-v2.html>;
/// cgroup v1 memory.limit_in_bytes <https://docs.kernel.org/admin-guide/cgroup-v1/memory.html>
#[cfg(any(target_os = "android", target_os = "linux"))]
fn cgroup_mem_limit() -> Option<usize> {
    let proc_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    min_limit(&cgroup_v2_files(&proc_cgroup, &mountinfo))
        .or_else(|| min_limit(&cgroup_v1_files(&proc_cgroup, &mountinfo)))
}

/// Read each candidate limit file and return the smallest real limit; "max" and
/// the v1 unlimited sentinel are ignored ("max" via `parse_cgroup_limit`, the
/// sentinel via `min` with physical RAM in `available_ram`).
#[cfg(any(target_os = "android", target_os = "linux"))]
fn min_limit(files: &[PathBuf]) -> Option<usize> {
    files
        .iter()
        .filter_map(|f| std::fs::read_to_string(f).ok())
        .filter_map(|s| parse_cgroup_limit(&s))
        .min()
}

/// Parse `MemTotal` (kB) from /proc/meminfo into bytes. The kernel always emits
/// this value in kB.
///
/// Ref: proc_meminfo(5) <https://man7.org/linux/man-pages/man5/proc_meminfo.5.html>
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn parse_meminfo_total(meminfo: &str) -> Option<usize> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    line.split_whitespace()
        .nth(1)?
        .parse::<usize>()
        .ok()?
        .checked_mul(1024)
}

/// Parse a cgroup memory-limit value into bytes. "max" (v2 unlimited) yields
/// `None`. The v1 unlimited sentinel is a huge number that `available_ram`
/// discards via `min` with physical RAM, so it needs no special case.
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn parse_cgroup_limit(raw: &str) -> Option<usize> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse().ok()
}

/// `memory.max` candidate paths for the cgroup v2 unified hierarchy, leaf-first.
/// Empty when this process has no v2 cgroup line.
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn cgroup_v2_files(proc_cgroup: &str, mountinfo: &str) -> Vec<PathBuf> {
    let Some(path) = cgroup_v2_path(proc_cgroup) else {
        return Vec::new();
    };
    let mount =
        mountinfo_mount(mountinfo, "cgroup2", None).unwrap_or_else(|| "/sys/fs/cgroup".to_string());
    walk_paths(&mount, &path, "memory.max")
}

/// `memory.limit_in_bytes` candidate paths for the cgroup v1 memory controller,
/// leaf-first. Empty when this process has no v1 memory cgroup line.
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn cgroup_v1_files(proc_cgroup: &str, mountinfo: &str) -> Vec<PathBuf> {
    let Some(path) = cgroup_v1_memory_path(proc_cgroup) else {
        return Vec::new();
    };
    let mount = mountinfo_mount(mountinfo, "cgroup", Some("memory"))
        .unwrap_or_else(|| "/sys/fs/cgroup/memory".to_string());
    walk_paths(&mount, &path, "memory.limit_in_bytes")
}

/// The cgroup path from a v2 unified line (`0::<path>`).
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn cgroup_v2_path(proc_cgroup: &str) -> Option<String> {
    proc_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        match (fields.next(), fields.next(), fields.next()) {
            (Some("0"), Some(""), Some(path)) => Some(path.to_string()),
            _ => None,
        }
    })
}

/// The cgroup path from the v1 line whose controllers include `memory`
/// (`<id>:<controllers>:<path>`).
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn cgroup_v1_memory_path(proc_cgroup: &str) -> Option<String> {
    proc_cgroup.lines().find_map(|line| {
        let mut fields = line.splitn(3, ':');
        let (_id, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
        controllers
            .split(',')
            .any(|c| c == "memory")
            .then(|| path.to_string())
    })
}

/// Mount point of the first /proc/self/mountinfo entry matching `fstype` and, if
/// given, containing `super_opt` in its super options. mountinfo splits on " - "
/// into `<...> mountpoint options [optional]` and `<fstype> <source> <superopts>`.
///
/// Ref: proc_pid_mountinfo(5) <https://man7.org/linux/man-pages/man5/proc_pid_mountinfo.5.html>
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn mountinfo_mount(mountinfo: &str, fstype: &str, super_opt: Option<&str>) -> Option<String> {
    mountinfo.lines().find_map(|line| {
        let (left, right) = line.split_once(" - ")?;
        let mut right_fields = right.split_whitespace();
        if right_fields.next()? != fstype {
            return None;
        }
        if let Some(opt) = super_opt {
            let superopts = right_fields.nth(1).unwrap_or("");
            if !superopts.split(',').any(|o| o == opt) {
                return None;
            }
        }
        // mountpoint is the 5th field of the left part.
        left.split_whitespace().nth(4).map(str::to_string)
    })
}

/// Limit-file paths from `<mount><cgroup_path>` up to `<mount>`, leaf-first, so a
/// tighter parent limit is included.
#[cfg_attr(not(any(target_os = "android", target_os = "linux")), allow(dead_code))]
fn walk_paths(mount: &str, cgroup_path: &str, filename: &str) -> Vec<PathBuf> {
    let mount = Path::new(mount);
    let rel = cgroup_path.trim_start_matches('/');
    let mut dir = if rel.is_empty() {
        mount.to_path_buf()
    } else {
        mount.join(rel)
    };
    let mut out = Vec::new();
    loop {
        out.push(dir.join(filename));
        if dir == mount {
            break;
        }
        match dir.parent() {
            Some(parent) if parent.starts_with(mount) => dir = parent.to_path_buf(),
            _ => break,
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Concurrency seeding
//
// Resolves `ConcurrencyMode::Auto` (and `TargetThroughput`) to a fixed in-flight
// request target. The target is derived from an estimate of the machine's
// network bandwidth: recognized EC2 instance families map to a per-vCPU Gbps rate
// (NIC bandwidth scales linearly with vCPU count within a family), and
// `target = ceil(gbps / GBPS_PER_CONN)`.
//
// `N` is target *in-flight requests* (the scheduler's `poll_work` gate), which
// the pool sizes connections separately from. In-flight work is nonetheless
// connection-bound under HTTP/1 (one request per connection on the wire), so the
// connection bounds `[MIN_CONN, ABSOLUTE_MAX_CONN]` are the correct bounds for
// the seed and are reused rather than duplicated.
// ---------------------------------------------------------------------------

/// Assumed goodput per in-flight request, in Gbps. Matches CRT's per-connection
/// figure (`100 / 250`).
const GBPS_PER_CONN: f64 = 0.4;

/// In-flight requests per vCPU for the fallback seed, used when the instance
/// family is unknown (unrecognized type, or detection failed / off-EC2). Without
/// a NIC estimate the seed can't be bandwidth-derived, so it scales concurrency
/// directly by vCPU. In-flight requests are roughly connection-count, so the
/// slope is kept modest; the `[FALLBACK_MIN, ABSOLUTE_MAX_CONN]` clamp keeps a
/// small box off the floor and a large one bounded.
const FALLBACK_INFLIGHT_PER_VCPU: usize = 5;

/// Floor for the fallback seed. A small unknown box (2-6 vCPU) would otherwise
/// seed below common general-purpose defaults (CRT's 10 Gbps target resolves to
/// 25 connections). Floors the fallback so an unknown box gets a usable default,
/// while the recognized-family estimate keeps its own lower `MIN_CONN` floor (a
/// genuinely small recognized box should seed low).
const FALLBACK_MIN: usize = 32;

/// Peak network bandwidth of each EC2 instance family, as the raw spec of its
/// largest member: `(family, max_nic_gbps, vcpus_at_that_size)`.
///
/// There is no formula for a family's bandwidth — it is assigned per hardware
/// generation and read from the published EC2 network-performance spec (e.g.
/// c6gn tops out at 100 Gbps, c7gn at 200, same name stem). So this is a lookup
/// table, and each row is the spec verbatim: `("m6idn", 200.0, 128)` means
/// "m6idn's largest size delivers 200 Gbps on 128 vCPU." Storing the two source
/// numbers (rather than a pre-divided per-vCPU rate) keeps every row directly
/// checkable against the docs and avoids hand-computed quotients.
///
/// Within a family NIC bandwidth scales linearly with vCPU count, so the flagship
/// reproduces every smaller size: `gbps = max_gbps * (local_vcpus / vcpus_at_max)`
/// (see [`family_gbps_per_vcpu`]).
///
/// **Adding a family:** if a family's NIC is large enough that the seed matters,
/// add a row with its flagship's Gbps and vCPU count from the spec. Families left
/// out fall through to the vCPU-scaled fallback (fine — they are modest-NIC boxes
/// where the seed is low-stakes). Sustained figures only; burstable ("up to")
/// families are excluded so we never seed off a burst ceiling.
const FAMILY_NIC_GBPS: &[(&str, f64, u32)] = &[
    // Ultra-high (GPU / ML training / inference accelerators)
    ("p6-b300", 6400.0, 192),
    ("p5", 3200.0, 192),
    ("p5en", 3200.0, 192),
    ("p6-b200", 3200.0, 192),
    ("trn1n", 1600.0, 128),
    ("p6e-gb200", 1700.0, 144),
    ("g7e", 1600.0, 192),
    ("trn1", 800.0, 128),
    ("dl1", 400.0, 96),
    ("p4d", 400.0, 96),
    ("p4de", 400.0, 96),
    ("g7", 700.0, 192),
    // Network-optimized (Graviton *gn, bandwidth-boost *gb)
    ("c7gn", 200.0, 64),
    ("c8gn", 600.0, 192),
    ("m8gn", 600.0, 192),
    ("r8gn", 600.0, 192),
    ("hpc7g", 200.0, 64),
    ("c8gb", 400.0, 192),
    ("m8gb", 400.0, 192),
    ("r8gb", 400.0, 192),
    ("m8azn", 200.0, 96),
    // Network-optimized *n / *dn / *idn (the S3-throughput workhorses)
    ("c6in", 200.0, 128),
    ("c8in", 600.0, 384),
    ("m6idn", 200.0, 128),
    ("m6in", 200.0, 128),
    ("m8idn", 600.0, 384),
    ("m8in", 600.0, 384),
    ("r6idn", 200.0, 128),
    ("r6in", 200.0, 128),
    ("r8idn", 600.0, 384),
    ("r8in", 600.0, 384),
    ("c6gn", 100.0, 64),
    ("im4gn", 100.0, 64),
    ("i8ge", 300.0, 192),
    ("c5n", 100.0, 72),
    // 100 Gbps-class general / storage / inference / *ib bandwidth-boost
    ("i3en", 100.0, 96),
    ("inf1", 100.0, 96),
    ("p3dn", 100.0, 96),
    ("c8ib", 400.0, 384),
    ("m8ib", 400.0, 384),
    ("m8idb", 400.0, 384),
    ("r8ib", 400.0, 384),
    ("r8idb", 400.0, 384),
    ("x2idn", 100.0, 128),
    ("x2iedn", 100.0, 128),
    ("i4i", 75.0, 128),
    ("f2", 100.0, 192),
    ("i7ie", 100.0, 192),
    ("inf2", 100.0, 192),
];

/// Machine facts detected once, off the client-construction hot path (in the
/// async config loader), and consumed when resolving auto-sized settings.
#[derive(Debug, Clone)]
pub(crate) struct MachineProfile {
    /// EC2 instance type (e.g. `"m6idn.16xlarge"`), from DMI or IMDS. `None`
    /// off-EC2 or when detection failed.
    pub(crate) instance_type: Option<String>,
    /// Usable vCPU count, honoring cgroup CPU quota and affinity.
    pub(crate) vcpus: usize,
    /// Usable RAM in bytes (cgroup-aware), or `None` when undetectable. Sizes the
    /// `Auto`/`Fraction` memory budget.
    pub(crate) ram_bytes: Option<usize>,
}

impl MachineProfile {
    /// Detect machine facts from local sources only: DMI instance type, vCPU
    /// count, and RAM. All are cheap pseudo-file reads (sysfs / procfs) or a
    /// syscall — no network — so this is safe to call synchronously on the
    /// `Client::new` path when no loader-detected profile is present.
    ///
    /// This does not attempt the IMDS instance-type fallback: IMDS is a network
    /// call and belongs to the async config loader, which assembles its own
    /// profile from these same primitives plus the IMDS step. A local DMI
    /// `NotEc2`/`Unknown` result therefore collapses to `instance_type: None`
    /// here — correct on the bypass path, where there is no IMDS to gate.
    pub(crate) fn detect_local() -> Self {
        let instance_type = match detect_instance_type_dmi() {
            DmiDetection::Instance(ty) => Some(ty),
            DmiDetection::NotEc2 | DmiDetection::Unknown => None,
        };
        Self {
            instance_type,
            vcpus: local_vcpus(),
            ram_bytes: available_ram(),
        }
    }
}

/// Usable vCPU count. `std::thread::available_parallelism` honors both the CPU
/// affinity mask and the cgroup CPU quota (CFS bandwidth) on Linux, so a
/// CPU-limited container reports its slice, not the host's core count. Falls
/// back to 1 if the count can't be determined.
pub(crate) fn local_vcpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Outcome of local (DMI) instance-type detection. Distinguishes "known" and
/// "definitely not EC2" from "couldn't tell" so the caller can decide whether an
/// IMDS network fallback is worthwhile: a positive non-EC2 reading (readable DMI,
/// vendor is not Amazon) means IMDS would fail too, so it is skipped; only an
/// `Unknown` (unreadable DMI, e.g. a container or non-Linux) is worth an IMDS try.
// `Instance` and `NotEc2` are only constructed on Linux (the DMI-bearing
// target); the non-Linux stub returns `Unknown`. Allow dead code so non-Linux
// dev builds stay warning-clean without hiding real dead code on Linux.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DmiDetection {
    /// Instance type read from DMI.
    Instance(String),
    /// DMI is readable and says this host is not EC2.
    NotEc2,
    /// DMI could not be read (container without DMI, non-Linux); inconclusive.
    Unknown,
}

/// Classify DMI reads into a [`DmiDetection`]. `vendor` is the `sys_vendor`
/// contents; `product` is `product_name`, `None` when that read failed. Split
/// from the file I/O so the three-state logic is testable without `/sys`.
///
/// A readable vendor that is not "Amazon EC2" is a definitive `NotEc2` (skip
/// IMDS). Vendor is EC2 but product is missing/empty ⇒ `Unknown` (let IMDS try).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn classify_dmi(vendor: &str, product: Option<&str>) -> DmiDetection {
    if !vendor.trim().eq_ignore_ascii_case("Amazon EC2") {
        return DmiDetection::NotEc2;
    }
    match product.map(str::trim) {
        Some(ty) if !ty.is_empty() => DmiDetection::Instance(ty.to_string()),
        _ => DmiDetection::Unknown,
    }
}

/// Detect the EC2 instance type from DMI, without any network call.
///
/// Reads `/sys/devices/virtual/dmi/id/sys_vendor` to confirm the host is EC2,
/// then `product_name` for the instance type. Mirrors the CRT S3 client's
/// primary detection path. Returns [`DmiDetection`] so the caller can gate the
/// IMDS fallback on `Unknown` vs `NotEc2`.
#[cfg(target_os = "linux")]
pub(crate) fn detect_instance_type_dmi() -> DmiDetection {
    let Ok(vendor) = std::fs::read_to_string("/sys/devices/virtual/dmi/id/sys_vendor") else {
        return DmiDetection::Unknown;
    };
    let product = std::fs::read_to_string("/sys/devices/virtual/dmi/id/product_name").ok();
    classify_dmi(&vendor, product.as_deref())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn detect_instance_type_dmi() -> DmiDetection {
    DmiDetection::Unknown
}

/// Per-vCPU Gbps for an instance type's family, or `None` if the family is not
/// in [`FAMILY_NIC_GBPS`]. The family is the substring before the first `.`
/// (`m6idn.16xlarge` -> `m6idn`); the rate is the flagship's
/// `max_nic_gbps / vcpus_at_max` (the linear within-family scale).
fn family_gbps_per_vcpu(instance_type: &str) -> Option<f64> {
    let family = instance_type.split('.').next()?;
    FAMILY_NIC_GBPS
        .iter()
        .find(|(name, _, _)| name.eq_ignore_ascii_case(family))
        .map(|(_, max_gbps, vcpus_at_max)| max_gbps / *vcpus_at_max as f64)
}

/// In-flight-request target from a throughput estimate: `ceil(gbps /
/// GBPS_PER_CONN)`, clamped to `[MIN_CONN, ABSOLUTE_MAX_CONN]`.
pub(crate) fn seed_from_gbps(gbps: f64) -> usize {
    let n = (gbps / GBPS_PER_CONN).ceil();
    // f64 -> usize saturates negatives/NaN to 0; the clamp floor fixes that.
    (n as usize).clamp(MIN_CONN, ABSOLUTE_MAX_CONN)
}

/// Resolve the `Auto` concurrency seed from detected machine facts.
///
/// Recognized family -> bandwidth estimate (`per_vcpu x vcpus`) -> connection
/// derivation, clamped `[MIN_CONN, ABSOLUTE_MAX_CONN]`. Unknown/undetected ->
/// `FALLBACK_INFLIGHT_PER_VCPU x vcpus` (a concurrency heuristic, not a bandwidth
/// estimate), clamped `[FALLBACK_MIN, ABSOLUTE_MAX_CONN]` so an unknown box gets a
/// usable default. Pure function of its inputs.
pub(crate) fn auto_concurrency_seed(instance_type: Option<&str>, vcpus: usize) -> usize {
    match instance_type.and_then(family_gbps_per_vcpu) {
        Some(per_vcpu) => seed_from_gbps(per_vcpu * vcpus as f64),
        None => (FALLBACK_INFLIGHT_PER_VCPU * vcpus).clamp(FALLBACK_MIN, ABSOLUTE_MAX_CONN),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: usize = 1024 * 1024 * 1024;

    #[test]
    fn test_cap_none_is_absolute_max() {
        // No detection (non-Unix, or getrlimit failed) -> absolute ceiling.
        assert_eq!(cap(None, usize::MAX), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_cap_low_fd_floored() {
        // 8 descriptors -> 4 by fraction -> floored to MIN_CONN.
        assert_eq!(cap(Some(8), usize::MAX), MIN_CONN);
    }

    #[test]
    fn test_cap_mid_fd_is_fraction() {
        assert_eq!(cap(Some(1024), usize::MAX), 512);
    }

    #[test]
    fn test_cap_high_fd_clamped() {
        // 40k descriptors -> 20k by fraction -> clamped to the absolute ceiling.
        assert_eq!(cap(Some(40_000), usize::MAX), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_cap_infinite_fd_saturates() {
        // RLIM_INFINITY surfaces as usize::MAX; the fraction saturates, then clamps.
        assert_eq!(cap(Some(usize::MAX), usize::MAX), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_cap_memory_term_binds() {
        // FD allows 512 (1024 x 0.5) but the budget funds only 100 chunks.
        assert_eq!(cap(Some(1024), 100), 100);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "connection_cap calls getrlimit, a syscall miri cannot emulate"
    )]
    fn test_connection_cap_within_bounds() {
        assert!((MIN_CONN..=ABSOLUTE_MAX_CONN).contains(&connection_cap()));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "connection_cap_with_memory calls getrlimit, a syscall miri cannot emulate"
    )]
    fn test_connection_cap_with_memory_is_bounded_by_chunks() {
        // 16-chunk budget caps connections at 16 (or lower if descriptors are scarce).
        let chunk = 8 * 1024 * 1024;
        let cap = connection_cap_with_memory(16 * chunk, chunk);
        assert!((MIN_CONN..=16).contains(&cap));
    }

    const MIB: usize = 1024 * 1024;

    #[test]
    fn test_auto_budget_undetectable_fallback() {
        assert_eq!(auto_budget(None), UNDETECTABLE_MEM_BYTES);
    }

    #[test]
    fn test_auto_budget_tiers() {
        // Reported RAM runs a little under the marketed size (firmware/kernel
        // reserve), so 0.25x lands just under a power of two; nearest-rounding
        // recovers the intended tier. (marketed GiB, reported GiB, expected budget)
        let cases = [
            (1.0, 1.0, 512 * MIB),    // floor
            (2.0, 1.8, 512 * MIB),    // 0.45 GiB -> 512 MiB
            (4.0, 3.7, GIB),          // 0.93 GiB -> 1 GiB
            (8.0, 7.7, 2 * GIB),      // 1.93 GiB -> 2 GiB
            (16.0, 15.3, 4 * GIB),    // 3.83 GiB -> 4 GiB
            (32.0, 30.6, 8 * GIB),    // 7.65 GiB -> 8 GiB
            (64.0, 61.0, 16 * GIB),   // 15.25 GiB -> 16 GiB
            (128.0, 123.0, 32 * GIB), // 30.75 GiB -> 32 GiB (cap)
            (256.0, 250.0, 32 * GIB), // would be 62 GiB -> capped
            (512.0, 504.0, 32 * GIB), // capped
        ];
        for (marketed, reported_gib, expected) in cases {
            let ram = (reported_gib * GIB as f64) as usize;
            assert_eq!(
                auto_budget(Some(ram)),
                expected,
                "marketed {marketed} GiB (reported {reported_gib})"
            );
        }
    }

    #[test]
    fn test_auto_budget_floor_and_cap_bind() {
        assert_eq!(auto_budget(Some(0)), MIN_BUDGET_BYTES);
        assert_eq!(auto_budget(Some(usize::MAX)), MAX_BUDGET_BYTES);
    }

    #[test]
    fn test_fraction_applied_uncapped() {
        // An explicit fraction is taken at the caller's word: no power-of-two
        // rounding, no ceiling.
        assert_eq!(mem_for_fraction_from(Some(64 * GIB), 0.5), 32 * GIB);
        assert_eq!(mem_for_fraction_from(Some(200 * GIB), 0.25), 50 * GIB);
    }

    #[test]
    fn test_fraction_floored() {
        // 1 GiB x 0.25 = 256 MiB -> floored.
        assert_eq!(mem_for_fraction_from(Some(GIB), 0.25), MIN_BUDGET_BYTES);
    }

    #[test]
    fn test_fraction_invalid_falls_back_to_safe() {
        // Non-finite, non-positive, or >1.0 fall back / clamp rather than
        // producing a zero or unbounded budget.
        for bad in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
            assert_eq!(
                mem_for_fraction_from(Some(64 * GIB), bad),
                mem_for_fraction_from(Some(64 * GIB), SAFE_MEM_FRACTION),
                "fraction {bad} should fall back to SAFE_MEM_FRACTION"
            );
        }
        assert_eq!(mem_for_fraction_from(Some(64 * GIB), 2.0), 64 * GIB);
    }

    #[test]
    fn test_round_pow2() {
        assert_eq!(round_pow2(0), 0);
        assert_eq!(round_pow2(1), 1);
        assert_eq!(round_pow2(2), 2);
        assert_eq!(round_pow2(3), 4); // tie-ish, closer to 4
        assert_eq!(round_pow2(5), 4);
        assert_eq!(round_pow2(6), 8); // tie rounds up
        assert_eq!(round_pow2(7), 8);
        assert_eq!(round_pow2(100), 128);
        assert_eq!(round_pow2(usize::MAX), 1usize << (usize::BITS - 1));
    }

    #[test]
    fn test_parse_meminfo_total() {
        let s = "MemFree: 100 kB\nMemTotal:    65536000 kB\nBuffers: 5 kB\n";
        assert_eq!(parse_meminfo_total(s), Some(65536000 * 1024));
    }

    #[test]
    fn test_parse_meminfo_missing() {
        assert_eq!(parse_meminfo_total("Foo: 1 kB\n"), None);
    }

    #[test]
    fn test_parse_cgroup_max_is_none() {
        assert_eq!(parse_cgroup_limit("max\n"), None);
    }

    #[test]
    fn test_parse_cgroup_value() {
        assert_eq!(parse_cgroup_limit("4294967296\n"), Some(4_294_967_296));
    }

    #[test]
    fn test_cgroup_v2_path() {
        assert_eq!(
            cgroup_v2_path("0::/foo/bar\n"),
            Some("/foo/bar".to_string())
        );
        assert_eq!(cgroup_v2_path("0::/\n"), Some("/".to_string()));
        // A pure-v1 file has no "0::" line.
        assert_eq!(cgroup_v2_path("11:memory:/x\n"), None);
    }

    #[test]
    fn test_cgroup_v1_memory_path() {
        let f = "12:pids:/a\n4:cpu,cpuacct:/b\n3:memory:/foo/bar\n";
        assert_eq!(cgroup_v1_memory_path(f), Some("/foo/bar".to_string()));
        assert_eq!(cgroup_v1_memory_path("0::/unified\n"), None);
    }

    #[test]
    fn test_mountinfo_mount_v2() {
        let mi = "35 24 0:30 / /sys/fs/cgroup rw,nosuid - cgroup2 cgroup2 rw,nsdelegate\n";
        assert_eq!(
            mountinfo_mount(mi, "cgroup2", None),
            Some("/sys/fs/cgroup".to_string())
        );
    }

    #[test]
    fn test_mountinfo_mount_v1_memory() {
        let mi = "30 24 0:26 / /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu\n\
                  31 24 0:27 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n";
        assert_eq!(
            mountinfo_mount(mi, "cgroup", Some("memory")),
            Some("/sys/fs/cgroup/memory".to_string())
        );
        // The cpu controller is not the memory one.
        assert_eq!(mountinfo_mount(mi, "cgroup", Some("hugetlb")), None);
    }

    #[test]
    fn test_walk_paths_subcgroup() {
        assert_eq!(
            walk_paths("/sys/fs/cgroup", "/foo/bar", "memory.max"),
            vec![
                PathBuf::from("/sys/fs/cgroup/foo/bar/memory.max"),
                PathBuf::from("/sys/fs/cgroup/foo/memory.max"),
                PathBuf::from("/sys/fs/cgroup/memory.max"),
            ]
        );
    }

    #[test]
    fn test_walk_paths_root() {
        // A namespaced container sees its cgroup as "/": only the mount root.
        assert_eq!(
            walk_paths("/sys/fs/cgroup", "/", "memory.max"),
            vec![PathBuf::from("/sys/fs/cgroup/memory.max")]
        );
    }

    #[cfg(any(
        target_os = "freebsd",
        target_os = "linux",
        target_os = "macos",
        target_os = "windows"
    ))]
    #[test]
    #[cfg_attr(
        miri,
        ignore = "available_ram reads /proc and calls platform syscalls miri cannot emulate"
    )]
    fn test_available_ram_detected_on_supported_platforms() {
        // The detected machine has at least 1 GiB; guards the platform syscall.
        assert!(available_ram().expect("RAM detected") >= GIB);
    }

    // --- concurrency seeding ---

    #[test]
    fn test_family_lookup_parses_family_from_type() {
        // Family is the prefix before the first '.'; size-independent.
        assert_eq!(family_gbps_per_vcpu("m6idn.16xlarge"), Some(1.5625));
        assert_eq!(family_gbps_per_vcpu("m6idn.32xlarge"), Some(1.5625));
        assert_eq!(family_gbps_per_vcpu("m6idn.metal"), Some(1.5625));
        // Case-insensitive.
        assert_eq!(family_gbps_per_vcpu("C8GN.48XLARGE"), Some(3.125));
        // Unknown family and malformed strings miss.
        assert_eq!(family_gbps_per_vcpu("t3.large"), None);
        assert_eq!(family_gbps_per_vcpu("nonsense"), None);
    }

    #[test]
    fn test_seed_recognized_family_scales_with_vcpu() {
        // m6idn: 1.5625 Gbps/vCPU, GBPS_PER_CONN=0.4.
        // 16xlarge = 64 vCPU -> 100 Gbps -> ceil(100/0.4) = 250.
        assert_eq!(auto_concurrency_seed(Some("m6idn.16xlarge"), 64), 250);
        // 32xlarge = 128 vCPU -> 200 Gbps -> 500.
        assert_eq!(auto_concurrency_seed(Some("m6idn.32xlarge"), 128), 500);
        // c8gn: 3.125 Gbps/vCPU. 16xlarge = 64 vCPU -> 200 Gbps -> 500.
        assert_eq!(auto_concurrency_seed(Some("c8gn.16xlarge"), 64), 500);
    }

    #[test]
    fn test_seed_uses_realized_vcpu_not_instance_size() {
        // A CPU-limited container on an m6idn.32xlarge reports its slice via
        // available_parallelism. Seed scales to the slice, not the host: a
        // 4-vCPU slice can't drive the box's 200 Gbps NIC. 4 * 1.5625 = 6.25
        // Gbps -> ceil(6.25/0.4) = 16.
        assert_eq!(auto_concurrency_seed(Some("m6idn.32xlarge"), 4), 16);
    }

    #[test]
    fn test_seed_recognizes_hyphenated_family() {
        // The family split is on '.', so a hyphenated family (`p6-b300`) must
        // still be recognized. Pick a vCPU count where the recognized result is
        // distinct from BOTH the clamp ceiling and the fallback, so the test
        // fails if the hyphenated key stops matching (falling through to
        // fallback) rather than passing via a coincident clamp.
        // p6-b300 = 6400/192 = 33.333 Gbps/vCPU. At 24 vCPU: 800 Gbps ->
        // ceil(800/0.4) = 2000. Fallback would be 5*24 = 120. Distinct.
        assert_eq!(auto_concurrency_seed(Some("p6-b300.6xlarge"), 24), 2000);
        assert_ne!(
            auto_concurrency_seed(Some("p6-b300.6xlarge"), 24),
            FALLBACK_INFLIGHT_PER_VCPU * 24
        );
    }

    #[test]
    fn test_seed_fallback_scales_with_vcpu() {
        // Unknown family -> FALLBACK_INFLIGHT_PER_VCPU (5) * vcpus, above the
        // FALLBACK_MIN (32) floor.
        assert_eq!(auto_concurrency_seed(Some("t3.2xlarge"), 8), 40);
        assert_eq!(auto_concurrency_seed(Some("gcp-n2-standard-64"), 64), 320);
        // No detection at all falls through the same path.
        assert_eq!(auto_concurrency_seed(None, 16), 80);
    }

    #[test]
    fn test_seed_fallback_floored_for_small_box() {
        // A small unknown box would seed below the general-purpose default; the
        // FALLBACK_MIN floor (32) lifts it. 4 vCPU * 5 = 20 -> floored to 32.
        assert_eq!(auto_concurrency_seed(None, 4), FALLBACK_MIN);
        assert_eq!(
            auto_concurrency_seed(Some("gcp-n2-standard-4"), 4),
            FALLBACK_MIN
        );
        // The floor governs up to 6 vCPU (6 * 5 = 30 < 32); 7 vCPU crosses it.
        assert_eq!(auto_concurrency_seed(None, 6), FALLBACK_MIN);
        assert_eq!(auto_concurrency_seed(None, 7), 35);
    }

    #[test]
    fn test_seed_clamps_to_bounds() {
        // Tiny unknown machine: fallback floors to FALLBACK_MIN (32).
        assert_eq!(auto_concurrency_seed(None, 1), FALLBACK_MIN);
        assert_eq!(auto_concurrency_seed(Some("t3.micro"), 2), FALLBACK_MIN);
        // Recognized tiny estimate floors to the lower MIN_CONN (a genuinely
        // small recognized box should seed low, unlike the unknown-box default).
        assert_eq!(auto_concurrency_seed(Some("i4i.large"), 2), MIN_CONN);
        // Enormous estimate caps at ABSOLUTE_MAX_CONN.
        assert_eq!(
            auto_concurrency_seed(Some("p6-b300.48xlarge"), 100_000),
            ABSOLUTE_MAX_CONN
        );
    }

    #[test]
    fn test_seed_from_gbps_edges() {
        assert_eq!(seed_from_gbps(100.0), 250);
        assert_eq!(seed_from_gbps(0.0), MIN_CONN); // floors, no underflow
        assert_eq!(seed_from_gbps(f64::NAN), MIN_CONN); // NaN -> 0 -> floor
        assert_eq!(seed_from_gbps(1.0e9), ABSOLUTE_MAX_CONN); // caps
    }

    #[test]
    fn test_family_rate_is_flagship_gbps_over_vcpu() {
        // The per-vCPU rate is exactly the flagship spec divided out. Seeding at
        // the flagship vCPU count reproduces the family's headline NIC number.
        // m6idn flagship: 200 Gbps @ 128 vCPU -> ceil(200/0.4) = 500.
        assert_eq!(auto_concurrency_seed(Some("m6idn.metal"), 128), 500);
        // c8gn flagship: 600 Gbps @ 192 vCPU -> ceil(600/0.4) = 1500.
        assert_eq!(auto_concurrency_seed(Some("c8gn.48xlarge"), 192), 1500);
        // i4i flagship: 75 Gbps @ 128 vCPU -> ceil(75/0.4) = 188.
        assert_eq!(auto_concurrency_seed(Some("i4i.32xlarge"), 128), 188);
    }

    #[test]
    fn test_family_nic_table_is_well_formed() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for (family, gbps, vcpus) in FAMILY_NIC_GBPS {
            assert!(seen.insert(*family), "duplicate family entry: {family:?}");
            assert!(!family.is_empty(), "empty family name");
            assert!(
                !family.contains('.'),
                "family {family:?} must be a bare family, not an instance type"
            );
            assert_eq!(
                family.to_ascii_lowercase(),
                *family,
                "family {family:?} must be lowercase for case-insensitive lookup"
            );
            // Sanity bounds: sustained NIC figures are >0 and within EC2's range,
            // vCPU counts are plausible flagship sizes.
            assert!(*gbps > 0.0 && *gbps <= 6400.0, "{family}: gbps {gbps}");
            assert!(*vcpus > 0 && *vcpus <= 1024, "{family}: vcpus {vcpus}");
        }
    }

    #[test]
    fn test_local_vcpus_is_at_least_one() {
        assert!(local_vcpus() >= 1);
    }

    #[cfg_attr(
        miri,
        ignore = "detect_local reads /sys and /proc and calls platform syscalls miri cannot emulate"
    )]
    #[test]
    fn test_detect_local_fills_profile() {
        // Local-only detection: vCPU is always present; instance_type/ram are
        // environment-dependent (None off-EC2 / on read failure), so we only
        // assert the always-true invariant and that it does not panic.
        let p = MachineProfile::detect_local();
        assert!(p.vcpus >= 1);
        // No IMDS on this path: off-EC2, instance_type must be None (DMI-only).
        // On EC2 it may be Some; either is valid, so this is not asserted here.
    }

    #[test]
    fn test_machine_safe_mem_takes_ram_from_caller() {
        // The wrapper no longer reads the environment; it sizes from the passed
        // RAM (from the MachineProfile). 64 GiB -> 25% -> 16 GiB (pow2, in range).
        assert_eq!(machine_safe_mem(Some(64 * GIB)), 16 * GIB);
        assert_eq!(machine_safe_mem(None), UNDETECTABLE_MEM_BYTES);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn test_dmi_detection_unknown_off_linux() {
        assert_eq!(detect_instance_type_dmi(), DmiDetection::Unknown);
    }

    #[test]
    fn test_classify_dmi_three_states() {
        // EC2 vendor + product -> Instance (trimmed, case-insensitive vendor).
        assert_eq!(
            classify_dmi("Amazon EC2\n", Some("m6idn.16xlarge\n")),
            DmiDetection::Instance("m6idn.16xlarge".to_string())
        );
        assert_eq!(
            classify_dmi("amazon ec2", Some("c7gn.16xlarge")),
            DmiDetection::Instance("c7gn.16xlarge".to_string())
        );
        // Readable non-EC2 vendor -> NotEc2 (the gate that skips IMDS).
        assert_eq!(
            classify_dmi("QEMU", Some("Standard PC")),
            DmiDetection::NotEc2
        );
        assert_eq!(classify_dmi("", Some("whatever")), DmiDetection::NotEc2);
        // EC2 vendor but missing/empty product -> Unknown (let IMDS try).
        assert_eq!(classify_dmi("Amazon EC2", None), DmiDetection::Unknown);
        assert_eq!(
            classify_dmi("Amazon EC2", Some("  ")),
            DmiDetection::Unknown
        );
    }
}
