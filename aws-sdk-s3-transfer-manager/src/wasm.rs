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
//! as a [`WasmRuntime`] in each client's configuration, with
//! [`RuntimeMode::wasm`](crate::types::RuntimeMode::wasm). A
//! [`BufferPool`](crate::memory::BufferPool) built for
//! [`MemoryConfig::Explicit`](crate::types::MemoryConfig::Explicit) takes one
//! from
//! [`BufferPoolBuilder::wasm_runtime`](crate::memory::BufferPoolBuilder::wasm_runtime).
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
//! A runtime that forwards to an embedding host's clock and executor, used by
//! one client:
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use aws_sdk_s3_transfer_manager::types::RuntimeMode;
//! use aws_sdk_s3_transfer_manager::wasm::{BoxFuture, WasmRuntime};
//! use aws_sdk_s3_transfer_manager::{Client, Config};
//!
//! # fn host_monotonic_nanos() -> u64 { 0 }
//! # fn host_executor_spawn(_future: BoxFuture) {}
//! # fn sdk_config() -> aws_types::SdkConfig { unimplemented!() }
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
//! let config = Config::builder()
//!     .s3_config(&sdk_config())
//!     .runtime_mode(RuntimeMode::wasm(HostRuntime))
//!     .build();
//! let client = Client::new(config);
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
use std::sync::Arc;
use std::time::Duration;

pub(crate) mod instant;

/// A transfer-manager future submitted to [`WasmRuntime::spawn`].
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Monotonic time and future scheduling supplied by the host application.
///
/// A client schedules its futures on, and reads time from, the runtime in its
/// [`RuntimeMode`](crate::types::RuntimeMode). Clients may share one runtime
/// or use separate ones. A [`TransferInstant`](crate::types::TransferInstant)
/// is comparable only with instants from runtimes that share its epoch.
///
/// The `Send` and `Sync` bounds match the transfer manager's own futures and
/// Smithy's HTTP and sleep interfaces. A single-threaded host satisfies them
/// without running futures on more than one thread.
pub trait WasmRuntime: Send + Sync + fmt::Debug + 'static {
    /// Returns the time elapsed since a runtime-defined monotonic epoch.
    ///
    /// Every call on this runtime uses the same epoch. Returned values never
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

/// A cloneable [`WasmRuntime`] that clients and buffer pools can share.
///
/// Clones refer to the same runtime.
#[derive(Clone, Debug)]
pub struct SharedWasmRuntime(Arc<dyn WasmRuntime>);

impl SharedWasmRuntime {
    /// Wraps `runtime` for sharing.
    pub fn new(runtime: impl WasmRuntime) -> Self {
        Self(Arc::new(runtime))
    }
}

impl WasmRuntime for SharedWasmRuntime {
    fn monotonic_now(&self) -> Duration {
        self.0.monotonic_now()
    }

    fn spawn(&self, future: BoxFuture) {
        self.0.spawn(future);
    }
}
