/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The executor that runs a shared-executor runtime's worker loops.
//!
//! On `wasm32-unknown-unknown`, which has no tokio runtime, a spawner submits
//! futures to the `wasm::WasmRuntime` that configured it. On every other target
//! it spawns onto the ambient tokio runtime and holds no state.

use std::future::Future;

/// A handle to the executor that runs worker loops.
#[derive(Debug)]
pub(crate) struct Spawner {
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    runtime: crate::wasm::SharedWasmRuntime,
}

impl Spawner {
    /// Returns a spawner for the ambient tokio runtime.
    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    pub(crate) const fn tokio() -> Self {
        Self {}
    }

    /// Returns a spawner that submits futures to `runtime`.
    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    pub(crate) fn from_runtime(runtime: crate::wasm::SharedWasmRuntime) -> Self {
        Self { runtime }
    }

    /// Runs `future` in the background without awaiting its output.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime on a target that uses one, or
    /// if the `wasm::WasmRuntime` rejects the future.
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
        drop(tokio::spawn(future));
        #[cfg(all(target_family = "wasm", target_os = "unknown"))]
        crate::wasm::WasmRuntime::spawn(&self.runtime, Box::pin(future));
    }

    /// Whether the target runs threads in parallel.
    ///
    /// False on wasm targets, whose threads are unavailable or cooperative.
    pub(crate) const fn parallel_threads(&self) -> bool {
        !cfg!(target_family = "wasm")
    }
}
