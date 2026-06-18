/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Machine-resource detection for sizing the connection and memory budgets.
//!
//! Detection is best-effort: a failure yields a conservative cap, never an
//! unbounded one.

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

/// Global cap on pooled connections. Machine-derived and independent of the
/// throughput target, so it bounds machine descriptor use and S3 request fan-out
/// on its own. `clamp(POOL_FD_BUDGET_FRACTION x RLIMIT_NOFILE, MIN_CONN,
/// ABSOLUTE_MAX_CONN)` on Unix; `ABSOLUTE_MAX_CONN` where there is no low
/// descriptor limit or detection fails. Budget sizing later tightens this with
/// the memory term `min(.., budget_capacity / chunk)`.
pub(crate) fn connection_cap() -> usize {
    cap_from_fd(fd_limit())
}

/// Apply the fraction and clamp to a detected descriptor limit. `None` falls
/// back to the absolute ceiling.
fn cap_from_fd(fd_limit: Option<usize>) -> usize {
    fd_limit
        .map(|n| (n as f64 * POOL_FD_BUDGET_FRACTION) as usize)
        .unwrap_or(ABSOLUTE_MAX_CONN)
        .clamp(MIN_CONN, ABSOLUTE_MAX_CONN)
}

/// Soft `RLIMIT_NOFILE` on Unix; `None` where there is no low descriptor limit
/// or detection fails.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cap_none_is_absolute_max() {
        // No detection (non-Unix, or getrlimit failed) -> absolute ceiling.
        assert_eq!(cap_from_fd(None), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_cap_low_limit_floored() {
        // 8 descriptors -> 4 by fraction -> floored to MIN_CONN.
        assert_eq!(cap_from_fd(Some(8)), MIN_CONN);
    }

    #[test]
    fn test_cap_mid_limit_is_fraction() {
        assert_eq!(cap_from_fd(Some(1024)), 512);
    }

    #[test]
    fn test_cap_high_limit_clamped() {
        // 40k descriptors -> 20k by fraction -> clamped to the absolute ceiling.
        assert_eq!(cap_from_fd(Some(40_000)), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_cap_infinite_limit_saturates_to_absolute_max() {
        // RLIM_INFINITY surfaces as usize::MAX; the fraction saturates, then clamps.
        assert_eq!(cap_from_fd(Some(usize::MAX)), ABSOLUTE_MAX_CONN);
    }

    #[test]
    fn test_connection_cap_within_bounds() {
        assert!((MIN_CONN..=ABSOLUTE_MAX_CONN).contains(&connection_cap()));
    }
}
