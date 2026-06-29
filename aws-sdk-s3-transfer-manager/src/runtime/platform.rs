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
// from these. Each item below carries `#[allow(dead_code)]` until that wiring
// exists.

/// Fraction of the process file-descriptor limit the connection pool may use.
/// The remainder is headroom for disk sinks, the directory walker, logging, and
/// other descriptors the process holds.
#[allow(dead_code)]
const POOL_FD_BUDGET_FRACTION: f64 = 0.5;

/// Ceiling on pooled connections regardless of descriptor headroom. Bounds the
/// request fan-out against a single S3 endpoint, and is the cap on platforms
/// with no low descriptor limit.
#[allow(dead_code)]
const ABSOLUTE_MAX_CONN: usize = 10_000;

/// Floor so a low descriptor limit does not cap the pool below a usable count.
#[allow(dead_code)]
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
/// unknown.
///
/// The budget is a backstop, not the operating point: the concurrency
/// controller settles at line rate well below it, and it binds only when a slow
/// consumer backs parts up in the prefetch ring. RAM is an imprecise proxy for
/// that ceiling — network bandwidth sets the real pipeline depth, and bandwidth
/// is uncorrelated with RAM across instance families (m5.24xlarge and
/// m5n.24xlarge share 384 GiB of RAM but differ 4x in bandwidth). The clamp
/// keeps the estimate safe at both ends; NIC-aware sizing is a separate refinement.
pub(crate) fn machine_safe_mem() -> usize {
    auto_budget(available_ram())
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
/// `UNDETECTABLE_MEM_BYTES` when RAM is unknown.
pub(crate) fn mem_for_fraction(fraction: f64) -> usize {
    mem_for_fraction_from(available_ram(), fraction)
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

/// Usable RAM: the smaller of the cgroup memory limit and physical RAM. Linux
/// only; `None` elsewhere or on a read failure, in which case the caller falls
/// back to a conservative default.
#[cfg(target_os = "linux")]
fn available_ram() -> Option<usize> {
    match (meminfo_total(), cgroup_mem_limit()) {
        (Some(total), Some(cgroup)) => Some(total.min(cgroup)),
        (total, None) => total,
        (None, cgroup) => cgroup,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn available_ram() -> Option<usize> {
    None
}

#[cfg(target_os = "macos")]
fn available_ram() -> Option<usize> {
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
fn available_ram() -> Option<usize> {
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

#[cfg(target_os = "linux")]
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
#[cfg(target_os = "linux")]
fn cgroup_mem_limit() -> Option<usize> {
    let proc_cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    min_limit(&cgroup_v2_files(&proc_cgroup, &mountinfo))
        .or_else(|| min_limit(&cgroup_v1_files(&proc_cgroup, &mountinfo)))
}

/// Read each candidate limit file and return the smallest real limit; "max" and
/// the v1 unlimited sentinel are ignored ("max" via `parse_cgroup_limit`, the
/// sentinel via `min` with physical RAM in `available_ram`).
#[cfg(target_os = "linux")]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cgroup_limit(raw: &str) -> Option<usize> {
    let raw = raw.trim();
    if raw == "max" {
        return None;
    }
    raw.parse().ok()
}

/// `memory.max` candidate paths for the cgroup v2 unified hierarchy, leaf-first.
/// Empty when this process has no v2 cgroup line.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_v1_files(proc_cgroup: &str, mountinfo: &str) -> Vec<PathBuf> {
    let Some(path) = cgroup_v1_memory_path(proc_cgroup) else {
        return Vec::new();
    };
    let mount = mountinfo_mount(mountinfo, "cgroup", Some("memory"))
        .unwrap_or_else(|| "/sys/fs/cgroup/memory".to_string());
    walk_paths(&mount, &path, "memory.limit_in_bytes")
}

/// The cgroup path from a v2 unified line (`0::<path>`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
    fn test_connection_cap_within_bounds() {
        assert!((MIN_CONN..=ABSOLUTE_MAX_CONN).contains(&connection_cap()));
    }

    #[test]
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

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    #[test]
    fn test_available_ram_detected_on_supported_platforms() {
        // The detected machine has at least 1 GiB; guards the platform syscall.
        assert!(available_ram().expect("RAM detected") >= GIB);
    }
}
