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

/// Fraction of the process file-descriptor limit the connection pool may use.
/// The remainder is headroom for disk sinks, the directory walker, logging, and
/// other descriptors the process holds.
const POOL_FD_BUDGET_FRACTION: f64 = 0.5;

/// Ceiling on pooled connections regardless of descriptor headroom. Bounds the
/// request fan-out against a single S3 endpoint, and is the cap on platforms
/// with no low descriptor limit.
const ABSOLUTE_MAX_CONN: usize = 10_000;

/// Floor so a low descriptor limit does not cap the pool below a usable count.
const MIN_CONN: usize = 10;

/// Global cap on pooled connections from the descriptor limit alone, used where
/// the memory budget is not yet known (the runtime-builder default). Machine-
/// derived and independent of the throughput target. `clamp(POOL_FD_BUDGET_FRACTION
/// x RLIMIT_NOFILE, MIN_CONN, ABSOLUTE_MAX_CONN)` on Unix; `ABSOLUTE_MAX_CONN`
/// where there is no low descriptor limit or detection fails.
pub(crate) fn connection_cap() -> usize {
    cap(fd_limit(), usize::MAX)
}

/// Global connection cap including the memory term: a connection that can never
/// hold a chunk is wasted, so the budget caps connections at the chunk count it
/// can fund. `clamp(min(fd_budget, budget_capacity / chunk), MIN_CONN,
/// ABSOLUTE_MAX_CONN)`.
pub(crate) fn connection_cap_with_memory(
    budget_capacity_bytes: usize,
    chunk_bytes: usize,
) -> usize {
    cap(fd_limit(), budget_capacity_bytes / chunk_bytes.max(1))
}

/// Apply the descriptor fraction, take the smaller of it and the memory-funded
/// connection count, and clamp to `[MIN_CONN, ABSOLUTE_MAX_CONN]`.
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
#[cfg(unix)]
fn fd_limit() -> Option<usize> {
    use nix::sys::resource::{getrlimit, Resource};
    match getrlimit(Resource::RLIMIT_NOFILE) {
        Ok((soft, _hard)) if soft > 0 => usize::try_from(soft).ok(),
        _ => None,
    }
}

#[cfg(not(unix))]
fn fd_limit() -> Option<usize> {
    None
}

/// Fraction of detected RAM the memory budget may use under `Auto`. The
/// remainder leaves room for the OS page cache (download-to-disk) and the rest
/// of the process.
const SAFE_MEM_FRACTION: f64 = 0.25;

/// Floor on the resolved budget so a small or mis-detected machine still gets a
/// workable amount.
const MIN_USABLE_MEM_BYTES: usize = ByteUnit::Gibibyte.as_bytes_usize();

/// Budget when RAM cannot be detected (non-Linux, or a read failure). Bounded,
/// but large enough for one transfer's pipeline.
const UNDETECTABLE_MEM_BYTES: usize = 2 * ByteUnit::Gibibyte.as_bytes_usize();

/// Memory budget under `MemoryBudgetConfig::Auto`: `SAFE_MEM_FRACTION` of
/// detected RAM, floored at `MIN_USABLE_MEM_BYTES`; `UNDETECTABLE_MEM_BYTES`
/// when RAM is unknown.
pub(crate) fn machine_safe_mem() -> usize {
    mem_for_fraction(SAFE_MEM_FRACTION)
}

/// Memory budget for an explicit RAM fraction (`MemoryBudgetConfig::Fraction`),
/// floored the same way; `UNDETECTABLE_MEM_BYTES` when RAM is unknown.
pub(crate) fn mem_for_fraction(fraction: f64) -> usize {
    mem_from_ram(available_ram(), fraction)
}

/// Apply a fraction to a detected RAM size and floor it.
fn mem_from_ram(ram: Option<usize>, fraction: f64) -> usize {
    match ram {
        Some(bytes) => ((bytes as f64 * fraction) as usize).max(MIN_USABLE_MEM_BYTES),
        None => UNDETECTABLE_MEM_BYTES,
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

    #[test]
    fn test_mem_undetectable_fallback() {
        assert_eq!(
            mem_from_ram(None, SAFE_MEM_FRACTION),
            UNDETECTABLE_MEM_BYTES
        );
    }

    #[test]
    fn test_mem_fraction_applied() {
        assert_eq!(mem_from_ram(Some(64 * GIB), 0.25), 16 * GIB);
    }

    #[test]
    fn test_mem_floored() {
        // 512 MiB x 0.25 = 128 MiB -> floored to MIN_USABLE_MEM_BYTES.
        assert_eq!(
            mem_from_ram(Some(512 * 1024 * 1024), 0.25),
            MIN_USABLE_MEM_BYTES
        );
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
