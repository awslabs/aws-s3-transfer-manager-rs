/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Process-level diagnostic settings resolved before runtime construction.

use std::env::VarError;
use std::time::Duration;

/// Process environment variable containing transfer-manager diagnostic settings.
const DIAGNOSTICS_ENV: &str = "AWS_S3_TM_DIAGNOSTICS";

/// Smallest periodic reporting interval accepted by memory diagnostics.
const MIN_MEMORY_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(100);

/// Highest memory detail level understood by this binary.
const MAX_MEMORY_DETAIL_LEVEL: u64 = 1;

/// Immutable diagnostic policy for one transfer-manager runtime domain.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DiagnosticsConfig {
    memory: MemoryDiagnosticsConfig,
}

impl DiagnosticsConfig {
    /// Resolves the process environment without allowing invalid settings to fail construction.
    pub(crate) fn from_env() -> Self {
        Self::from_environment_value(std::env::var(DIAGNOSTICS_ENV))
    }

    /// Resolves one environment lookup result.
    fn from_environment_value(value: Result<String, VarError>) -> Self {
        match value {
            Ok(value) => Self::parse(&value),
            Err(VarError::NotPresent) => Self::default(),
            Err(VarError::NotUnicode(_)) => {
                tracing::warn!(
                    target: crate::telemetry::TARGET_MEMORY,
                    variable = DIAGNOSTICS_ENV,
                    "ignored non-Unicode transfer-manager diagnostics configuration"
                );
                Self::default()
            }
        }
    }

    /// Returns the memory-subsystem policy.
    pub(crate) fn memory(self) -> MemoryDiagnosticsConfig {
        self.memory
    }

    /// Parses comma-separated `key=value` settings from one Unicode value.
    fn parse(value: &str) -> Self {
        let mut config = Self::default();
        for entry in value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let Some((key, value)) = entry.split_once('=') else {
                warn_invalid_entry(entry);
                continue;
            };
            let key = key.trim();
            let value = value.trim();
            match key {
                "memory.snapshot" => match parse_memory_snapshot(value) {
                    Ok(interval) => config.memory.snapshot_interval = interval,
                    Err(()) => warn_invalid_setting(key, value),
                },
                "memory.detail" => match value.parse::<u64>() {
                    Ok(requested) => {
                        let effective = requested.min(MAX_MEMORY_DETAIL_LEVEL);
                        if requested != effective {
                            tracing::warn!(
                                target: crate::telemetry::TARGET_MEMORY,
                                variable = DIAGNOSTICS_ENV,
                                setting = key,
                                requested,
                                effective,
                                "clamped unsupported transfer-manager diagnostic detail level"
                            );
                        }
                        config.memory.detail = MemoryDiagnosticDetail::from_level(effective);
                    }
                    Err(_) => warn_invalid_setting(key, value),
                },
                _ => {}
            }
        }
        config
    }
}

/// Immutable diagnostic policy consumed by the memory subsystem.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MemoryDiagnosticsConfig {
    snapshot_interval: Option<Duration>,
    detail: MemoryDiagnosticDetail,
}

impl MemoryDiagnosticsConfig {
    /// Constructs an explicit policy for deterministic internal tests.
    #[cfg(test)]
    pub(crate) fn for_test(snapshot_interval: Option<Duration>, detail_level: u64) -> Self {
        Self {
            snapshot_interval,
            detail: MemoryDiagnosticDetail::from_level(detail_level.min(MAX_MEMORY_DETAIL_LEVEL)),
        }
    }

    /// Returns the periodic memory snapshot interval.
    pub(crate) fn snapshot_interval(self) -> Option<Duration> {
        self.snapshot_interval
    }

    /// Returns the effective documented detail level.
    pub(crate) fn detail_level(self) -> u8 {
        self.detail as u8
    }

    /// Returns whether successful optimistic acquisitions record scan work.
    pub(crate) fn enable_detailed_counters(self) -> bool {
        self.detail >= MemoryDiagnosticDetail::AllocatorScans
    }
}

/// Cumulative memory-diagnostic detail enabled for one pool.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
enum MemoryDiagnosticDetail {
    /// Stable gauges and counters that update only on pressure or lifecycle events.
    #[default]
    Baseline = 0,
    /// Per-acquisition optimistic attempts and bitmap words inspected.
    AllocatorScans = 1,
}

impl MemoryDiagnosticDetail {
    /// Converts one requested level, saturating at the highest supported detail.
    fn from_level(level: u64) -> Self {
        match level {
            0 => Self::Baseline,
            _ => Self::AllocatorScans,
        }
    }
}

/// Parses `off` or one positive integer millisecond duration.
fn parse_memory_snapshot(value: &str) -> Result<Option<Duration>, ()> {
    if value == "off" {
        return Ok(None);
    }
    let millis = value
        .strip_suffix("ms")
        .ok_or(())?
        .parse::<u64>()
        .map_err(|_| ())?;
    if millis == 0 {
        return Err(());
    }
    let requested = Duration::from_millis(millis);
    if requested < MIN_MEMORY_SNAPSHOT_INTERVAL {
        tracing::warn!(
            target: crate::telemetry::TARGET_MEMORY,
            variable = DIAGNOSTICS_ENV,
            setting = "memory.snapshot",
            requested_ms = millis,
            effective_ms = MIN_MEMORY_SNAPSHOT_INTERVAL.as_millis(),
            "raised transfer-manager diagnostic interval to its minimum"
        );
    }
    Ok(Some(requested.max(MIN_MEMORY_SNAPSHOT_INTERVAL)))
}

/// Reports one malformed comma-separated entry.
fn warn_invalid_entry(entry: &str) {
    tracing::warn!(
        target: crate::telemetry::TARGET_MEMORY,
        variable = DIAGNOSTICS_ENV,
        entry,
        "ignored malformed transfer-manager diagnostic setting"
    );
}

/// Reports one recognized setting with an unsupported value.
fn warn_invalid_setting(setting: &str, value: &str) {
    tracing::warn!(
        target: crate::telemetry::TARGET_MEMORY,
        variable = DIAGNOSTICS_ENV,
        setting,
        value,
        "ignored invalid transfer-manager diagnostic setting"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_unknown_settings_keep_defaults() {
        assert_eq!(DiagnosticsConfig::parse(""), DiagnosticsConfig::default());
        assert_eq!(
            DiagnosticsConfig::parse("scheduler.snapshot=1000ms"),
            DiagnosticsConfig::default()
        );
    }

    #[test]
    fn environment_resolution_uses_the_same_parser_and_defaults() {
        assert_eq!(
            DiagnosticsConfig::from_environment_value(Err(VarError::NotPresent)),
            DiagnosticsConfig::default()
        );
        assert_eq!(
            DiagnosticsConfig::from_environment_value(Ok(
                "memory.snapshot=250ms,memory.detail=1".to_owned()
            ))
            .memory(),
            MemoryDiagnosticsConfig::for_test(Some(Duration::from_millis(250)), 1)
        );
        assert_eq!(
            DiagnosticsConfig::from_environment_value(Err(VarError::NotUnicode(
                std::ffi::OsString::from("invalid")
            ))),
            DiagnosticsConfig::default()
        );
    }

    #[test]
    fn memory_snapshot_accepts_off_and_integer_milliseconds() {
        assert_eq!(
            DiagnosticsConfig::parse("memory.snapshot=250ms")
                .memory()
                .snapshot_interval(),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            DiagnosticsConfig::parse("memory.snapshot=1ms")
                .memory()
                .snapshot_interval(),
            Some(MIN_MEMORY_SNAPSHOT_INTERVAL)
        );
        assert_eq!(
            DiagnosticsConfig::parse("memory.snapshot=250ms,memory.snapshot=off")
                .memory()
                .snapshot_interval(),
            None
        );
    }

    #[test]
    fn invalid_snapshot_values_do_not_replace_valid_settings() {
        let config = DiagnosticsConfig::parse(
            "memory.snapshot=250ms,memory.snapshot=250,memory.snapshot=0ms",
        );
        assert_eq!(
            config.memory().snapshot_interval(),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn snapshot_parser_rejects_missing_suffix_invalid_units_and_overflow() {
        assert_eq!(parse_memory_snapshot("250"), Err(()));
        assert_eq!(parse_memory_snapshot("1s"), Err(()));
        assert_eq!(parse_memory_snapshot("18446744073709551616ms"), Err(()));
    }

    #[test]
    fn memory_detail_levels_are_cumulative_and_clamped() {
        assert_eq!(
            DiagnosticsConfig::parse("memory.detail=0")
                .memory()
                .detail_level(),
            0
        );
        assert_eq!(
            DiagnosticsConfig::parse("memory.detail=1")
                .memory()
                .detail_level(),
            1
        );
        assert_eq!(
            DiagnosticsConfig::parse("memory.detail=2")
                .memory()
                .detail_level(),
            1
        );
    }

    #[test]
    fn detail_conversion_saturates_at_the_highest_supported_level() {
        assert_eq!(
            MemoryDiagnosticDetail::from_level(u64::MAX),
            MemoryDiagnosticDetail::AllocatorScans
        );
    }

    #[test]
    fn snapshot_and_detail_controls_are_independent() {
        let snapshot = DiagnosticsConfig::parse("memory.snapshot=250ms").memory();
        assert_eq!(
            snapshot.snapshot_interval(),
            Some(Duration::from_millis(250))
        );
        assert_eq!(snapshot.detail_level(), 0);

        let detailed = DiagnosticsConfig::parse("memory.detail=1").memory();
        assert_eq!(detailed.snapshot_interval(), None);
        assert_eq!(detailed.detail_level(), 1);
    }

    #[test]
    fn settings_are_trimmed_and_last_valid_assignment_wins() {
        let config = DiagnosticsConfig::parse(
            " memory.detail = 1 , memory.snapshot = 500ms , memory.detail = 0 ",
        );
        assert_eq!(config.memory().detail_level(), 0);
        assert_eq!(
            config.memory().snapshot_interval(),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn malformed_entries_do_not_change_other_settings() {
        let config = DiagnosticsConfig::parse("bad-entry,memory.detail=1,memory.snapshot=broken");
        assert_eq!(config.memory().detail_level(), 1);
        assert_eq!(config.memory().snapshot_interval(), None);
    }
}
