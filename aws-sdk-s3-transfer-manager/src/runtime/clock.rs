/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The monotonic clock that timestamps transfer-manager state.
//!
//! Every [`TransferInstant`] that the transfer manager records comes from a
//! [`Clock`], and durations are measured only between instants from one
//! clock. A client owns one clock and passes it to the state it creates; a
//! buffer pool owns its own.
//!
//! On `wasm32-unknown-unknown`, which has no standard monotonic clock, a clock
//! reads the `wasm::WasmRuntime` that configured it. On every other target it
//! reads [`std::time::Instant`] and holds no state.

use std::time::Duration;

use crate::types::TransferInstant;

/// A source of [`TransferInstant`] values.
#[derive(Clone, Debug)]
pub(crate) struct Clock {
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    runtime: crate::wasm::SharedWasmRuntime,
}

impl Clock {
    /// Returns the operating system's monotonic clock.
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub(crate) const fn system() -> Self {
        Self {}
    }

    /// Returns a clock that reads `runtime`.
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub(crate) fn from_runtime(runtime: crate::wasm::SharedWasmRuntime) -> Self {
        Self { runtime }
    }

    /// Returns the current time.
    pub(crate) fn now(&self) -> TransferInstant {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        return TransferInstant::now();
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        return TransferInstant::now(&self.runtime);
    }

    /// Returns the time elapsed since `earlier`, or zero if `earlier` is later
    /// than the current time.
    pub(crate) fn elapsed(&self, earlier: TransferInstant) -> Duration {
        self.now().saturating_duration_since(earlier)
    }
}
