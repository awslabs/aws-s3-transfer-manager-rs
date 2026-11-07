/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! UTC-timestamped stderr progress messages and opt-in developer diagnostics.
//!
//! Only records from this binary are emitted. Dependency HTTP logging is excluded
//! so increasing verbosity cannot emit request headers or authentication data.

use log::{LevelFilter, Log, Metadata, Record};
use std::io::{self, Write};
use time::OffsetDateTime;

/// Stderr logger with UTC millisecond timestamps, restricted to this binary.
struct Diagnostics;

impl Log for Diagnostics {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        (metadata.target() == "tmdev" || metadata.target().starts_with("tmdev::"))
            && metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            let timestamp = OffsetDateTime::now_utc();
            let _ = writeln!(
                io::stderr().lock(),
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z tmdev: {}: {}",
                timestamp.year(),
                u8::from(timestamp.month()),
                timestamp.day(),
                timestamp.hour(),
                timestamp.minute(),
                timestamp.second(),
                timestamp.millisecond(),
                record.level(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

/// Installs the stderr logger once; verbose mode adds debug records.
///
/// # Panics
///
/// Panics if a process-wide logger has already been installed.
pub(crate) fn init(verbose: bool) {
    static LOGGER: Diagnostics = Diagnostics;
    log::set_logger(&LOGGER).expect("diagnostics must be initialized once");
    log::set_max_level(if verbose {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    });
}
