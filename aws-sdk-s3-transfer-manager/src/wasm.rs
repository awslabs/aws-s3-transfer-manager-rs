/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Host capabilities for `wasm32-unknown-unknown`.
//!
//! The target defines a WebAssembly module with no operating system. The
//! standard library cannot read a monotonic clock or start threads there, and
//! the transfer manager assumes no JavaScript environment, WASI interface,
//! executor, or host import. The application supplies the missing capabilities
//! by installing one [`WasmRuntime`] with [`install_runtime`] before it
//! constructs a [`Client`](crate::Client), a
//! [`BufferPool`](crate::memory::BufferPool), or any other transfer-manager
//! value.
//!
//! The S3 configuration supplies the remaining host services through the
//! Smithy interfaces it already defines:
//!
//! - an HTTP client;
//! - an [`AsyncSleep`](aws_sdk_s3::config::AsyncSleep) implementation, which
//!   also paces transfer-manager retries and request deadlines;
//! - a wall-clock [`TimeSource`](aws_types::sdk_config::TimeSource);
//! - credentials and a region.
//!
//! The transfer manager neither requires nor creates a tokio runtime on this
//! target. It does not drive the host's event loop: the host must keep polling
//! spawned futures while transfers are active.
//!
//! # Examples
//!
//! A runtime that forwards to an embedding host's clock and executor:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use aws_sdk_s3_transfer_manager::wasm::{self, BoxFuture, WasmRuntime};
//!
//! # fn host_monotonic_nanos() -> u64 { 0 }
//! # fn host_executor_spawn(_future: BoxFuture) {}
//! #[derive(Debug)]
//! struct HostRuntime;
//!
//! impl WasmRuntime for HostRuntime {
//!     fn monotonic_now(&self) -> Duration {
//!         Duration::from_nanos(host_monotonic_nanos())
//!     }
//!
//!     fn spawn(&self, future: BoxFuture) {
//!         host_executor_spawn(future);
//!     }
//! }
//!
//! wasm::install_runtime(HostRuntime).expect("runtime installed once");
//! ```
//!
//! A JavaScript host can implement the same trait in application code, for
//! example with `wasm-bindgen`:
//!
//! ```ignore
//! impl WasmRuntime for JsRuntime {
//!     fn monotonic_now(&self) -> Duration {
//!         // `performance.now()` is monotonic and measured in milliseconds.
//!         Duration::from_secs_f64(performance().now() / 1000.0)
//!     }
//!
//!     fn spawn(&self, future: BoxFuture) {
//!         wasm_bindgen_futures::spawn_local(future);
//!     }
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

pub(crate) mod instant;

/// A transfer-manager future submitted to [`WasmRuntime::spawn`].
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Monotonic time and future scheduling supplied by the host application.
///
/// One runtime serves every client, transfer, and buffer pool in the module.
/// An application that needs several execution domains installs a runtime that
/// routes futures itself.
///
/// The `Send` and `Sync` bounds match the transfer manager's own futures and
/// Smithy's HTTP and sleep interfaces. A single-threaded host satisfies them
/// without running futures on more than one thread.
pub trait WasmRuntime: Send + Sync + fmt::Debug + 'static {
    /// Returns the time elapsed since a runtime-defined monotonic epoch.
    ///
    /// Every call in the module uses the same epoch. Returned values never
    /// decrease, are comparable across threads, and do not wrap. The epoch has
    /// no wall-clock meaning: the transfer manager uses only differences and
    /// ordering. Resolution may be coarser than one nanosecond.
    ///
    /// The method must not block or wait for asynchronous work.
    fn monotonic_now(&self) -> Duration;

    /// Schedules `future` to be polled to completion.
    ///
    /// The runtime polls the future after this call returns, never from
    /// within it, and polls it again each time its waker is woken. A future
    /// that the runtime drops before completion leaves its transfer pending.
    /// The runtime may run futures on one thread or several. It may panic if
    /// it cannot accept the future.
    fn spawn(&self, future: BoxFuture);
}

/// The error returned when a module installs a second [`WasmRuntime`].
///
/// The runtime installed first remains active.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeAlreadyInstalled;

impl fmt::Display for RuntimeAlreadyInstalled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a transfer-manager wasm runtime is already installed")
    }
}

impl std::error::Error for RuntimeAlreadyInstalled {}

static RUNTIME: OnceLock<Box<dyn WasmRuntime>> = OnceLock::new();

/// Installs the module's [`WasmRuntime`].
///
/// Installation succeeds once per module. The runtime cannot be replaced or
/// removed afterwards.
///
/// # Errors
///
/// Returns [`RuntimeAlreadyInstalled`] and drops `runtime` if a runtime is
/// already installed.
pub fn install_runtime(runtime: impl WasmRuntime) -> Result<(), RuntimeAlreadyInstalled> {
    RUNTIME
        .set(Box::new(runtime))
        .map_err(|_| RuntimeAlreadyInstalled)
}

/// Returns the installed runtime.
///
/// # Panics
///
/// Panics if [`install_runtime`] has not been called.
#[track_caller]
pub(crate) fn runtime() -> &'static dyn WasmRuntime {
    match RUNTIME.get() {
        Some(runtime) => runtime.as_ref(),
        None => panic!(
            "wasm32-unknown-unknown requires wasm::install_runtime before constructing \
             transfer-manager values"
        ),
    }
}
