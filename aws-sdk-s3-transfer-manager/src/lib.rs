/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/* Automatically managed default lints */
#![cfg_attr(docsrs, feature(doc_cfg))]
/* End of automatically managed default lints */
#![warn(
    missing_debug_implementations,
    missing_docs,
    rustdoc::missing_crate_level_docs,
    unreachable_pub,
    rust_2018_idioms
)]

//! An Amazon S3 client focused on maximizing throughput and network utilization.
//!
//! AWS S3 Transfer Manager is a high level abstraction over the base Amazon S3
//! [service API]. Transfer operations such as upload or download are automatically
//! split into concurrent requests to accelerate performance.
//!
//! [service API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
//!
//! # Modeled types and configuration
//!
//! S3 request values, response metadata, and enums live in [`types`], and
//! their builders in [`types::builders`]. Operation modules re-export the
//! modeled types and builders they use.
//! Configure a client from shared [`aws_types::SdkConfig`] using
//! [`S3ClientConfig`], or load configuration with [`from_env`].
//!
//! The `sdk-v1` feature enables conversions between modeled values and AWS SDK
//! for Rust S3 v1 types, conversion from an S3 configuration builder, and
//! [`S3WalkContextBuilder`](crate::io::walk::S3WalkContextBuilder) construction
//! from an SDK client.
//!
//! # Examples
//!
//! Load the default configuration:
//!
//! ```no_run
//! # async fn example() {
//! let config = aws_sdk_s3_transfer_manager::from_env().load().await;
//! let client = aws_sdk_s3_transfer_manager::Client::new(config);
//! # }
//! ```
//!
//! Download a bucket to a local directory:
//!
//! ```no_run
//! # async fn example() -> Result<(), aws_sdk_s3_transfer_manager::error::Error> {
//! let config = aws_sdk_s3_transfer_manager::from_env().load().await;
//! let client = aws_sdk_s3_transfer_manager::Client::new(config);
//!
//! let handle = client
//!     .download_objects()
//!     .bucket("my-bucket")
//!     .destination("/tmp/my-bucket")
//!     .initiate()?;
//!
//! // wait for transfer to complete
//! handle.join().await?;
//!
//! # Ok(())
//! # }
//!
//! ```
//!
//! See the documentation for each client operation for more information:
//!
//! * [`download`](crate::Client::download) - download a single object
//! * [`upload`](crate::Client::upload) - upload a single object
//! * [`download_objects`](crate::Client::download_objects) - download an entire bucket or prefix to a local directory
//! * [`upload_objects`](crate::Client::upload_objects) - upload an entire local directory to a bucket
//!
//! # WebAssembly
//!
//! The crate builds for `wasm32-wasip1`, `wasm32-wasip2`, `wasm32-wasip3`, and
//! `wasm32-unknown-unknown`. Other wasm targets are not supported.
//!
//! | Target                   | Executor                      | Monotonic clock               | Filesystem     | HTTP                                         |
//! | ------------------------ | ----------------------------- | ----------------------------- | -------------- | -------------------------------------------- |
//! | `wasm32-wasip1`          | caller's tokio runtime        | WASI                          | WASI preview 1 | caller-supplied; no standard network interface |
//! | `wasm32-wasip2`          | caller's tokio runtime        | WASI                          | WASI preview 2 | caller-supplied, for example over `wasi:http` |
//! | `wasm32-wasip3`          | caller's tokio runtime        | WASI                          | WASI preview 3 | caller-supplied                              |
//! | `wasm32-unknown-unknown` | installed `wasm::WasmRuntime` | installed `wasm::WasmRuntime` | none           | caller-supplied                              |
//!
//! On every wasm target:
//!
//! - The AWS SDK's default HTTPS client is not built. Requests fail unless the
//!   [`aws_types::SdkConfig`] or S3 configuration supplies an HTTP client.
//! - [`RuntimeMode::Managed`](crate::types::RuntimeMode::Managed) behaves as
//!   [`RuntimeMode::MultiThreadTokio`](crate::types::RuntimeMode::MultiThreadTokio).
//!   File reads and writes run inline on the thread that polls the transfer.
//! - Memory detection is unavailable, so
//!   [`MemoryBudgetConfig::Auto`](crate::types::MemoryBudgetConfig::Auto) uses
//!   a fallback: 2 GiB on the WASI targets and 64 MiB on
//!   `wasm32-unknown-unknown`. Use
//!   [`MemoryBudgetConfig::Limit`](crate::types::MemoryBudgetConfig::Limit) to
//!   fit the module's linear memory.
//!
//! On the WASI targets, build with `RUSTFLAGS="--cfg tokio_unstable"`. The S3
//! client's tokio integration enables tokio's `fs` feature, which tokio accepts
//! on wasm only with that flag. Transfers run on the caller's tokio runtime,
//! which may be current-thread.
//!
//! ## `wasm32-unknown-unknown`
//!
//! This target has no operating system and implies no JavaScript or WASI host.
//! It needs no build flag.
//!
//! - Before constructing a client or a
//!   [`BufferPool`](crate::memory::BufferPool), install a `wasm::WasmRuntime`
//!   with `wasm::install_runtime`. The runtime supplies a monotonic clock and
//!   schedules the transfer manager's futures. The `wasm` module exists only on
//!   this target.
//! - The S3 configuration must supply an HTTP client, an
//!   [`AsyncSleep`](aws_sdk_s3::config::AsyncSleep) implementation, a
//!   wall-clock [`TimeSource`](aws_types::sdk_config::TimeSource), credentials,
//!   and a region. [`from_env`] cannot discover them on this target.
//! - The transfer manager neither requires nor creates a tokio runtime, and it
//!   does not drive the host's event loop. The host must keep polling spawned
//!   futures while transfers are active.
//! - There is no filesystem. `write_to_path` and `write_to_file` are absent.
//!   Operations on local files, such as
//!   [`InputStream::from_path`](crate::io::InputStream::from_path) and the
//!   directory operations [`upload_objects`](crate::Client::upload_objects)
//!   and [`download_objects`](crate::Client::download_objects), fail when they
//!   access the filesystem. Upload from memory or a custom stream, and read
//!   downloads from their body stream.
//! - SigV4a signing is not enabled, because it needs an entropy source that the
//!   target does not define. An application that enables the `aws-sdk-s3`
//!   `sigv4a` feature must also select an entropy backend for `getrandom`.
//!
//! # Runtime diagnostics
//!
//! `AWS_S3_TM_DIAGNOSTICS` enables optional diagnostic collection without
//! changing the client configuration API. Settings are namespaced by subsystem
//! and resolved once when a client configuration is built. A standalone
//! [`memory::BufferPool`] resolves only the memory settings.
//!
//! The value is a comma-separated list of case-sensitive `key=value` settings:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms,memory.detail=1,transfer.detail=1
//! ```
//!
//! Whitespace around entries, keys, and values is ignored. Later valid
//! assignments replace earlier ones. Unknown keys are ignored for forward
//! compatibility. A malformed recognized setting produces a warning and keeps
//! the preceding value. Unsupported detail levels use the highest level
//! understood by this version and produce a warning.
//!
//! The environment variable controls collection cost. Tracing filters
//! independently control whether collected records are emitted.
//!
//! ## Memory pool
//!
//! Memory diagnostics report pool capacity, admission pressure, allocation
//! fallback, preparation, and reclamation.
//!
//! - `memory.snapshot=off` disables periodic reports. A positive integer
//!   followed by `ms` enables reports; values below `100ms` use `100ms`.
//! - `memory.detail=0` uses counters updated only at pressure and lifecycle
//!   boundaries.
//! - `memory.detail=1` also counts every optimistic allocation attempt and
//!   bitmap word inspected, adding relaxed atomic updates to the acquisition
//!   path.
//!
//! Memory snapshots use the `aws_sdk_s3_transfer_manager::memory` tracing target
//! at `DEBUG`:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms
//! RUST_LOG=aws_sdk_s3_transfer_manager::memory=debug
//! ```
//!
//! Snapshot reporting reuses the memory pool's maintenance thread. It does not
//! create a diagnostics-only thread. Detailed counters and periodic reporting
//! are independent; both are disabled by default.
//!
//! ## Transfer state machines
//!
//! Transfer diagnostics report where an operation spends time and how work
//! moves through its direction-specific state machine.
//!
//! - `transfer.detail=0` disables optional transfer-state collection.
//! - `transfer.detail=1` collects aggregate summaries from supported transfer
//!   state machines. Multipart uploads currently provide this summary.
//! - `transfer.detail=2` also collects individual state transitions.
//!
//! Transfer summaries use the `aws_sdk_s3_transfer_manager::transfer` tracing
//! target at `DEBUG`; level-2 transitions use the same target at `TRACE`:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=transfer.detail=1
//! RUST_LOG=aws_sdk_s3_transfer_manager::transfer=debug
//! ```

/// Error types emitted by `aws-sdk-s3-transfer-manager`
pub mod error;

/// Common types used by `aws-sdk-s3-transfer-manager`
pub mod types;

/// Modeled S3 values and builders.
pub(crate) mod model;
pub(crate) mod sdk_v1;

/// Types and helpers for I/O
pub mod io;

/// Transfer manager client
pub mod client;

/// Transfer manager operations
pub mod operation;

/// Transfer manager configuration
pub mod config;

/// Payload-memory configuration and shared pooled storage.
///
/// Transfer-manager clients construct a pool automatically by default.
/// Applications that need to share one memory budget across clients or another
/// component can construct a [`BufferPool`](crate::memory::BufferPool) and
/// install it through
/// [`MemoryConfig::Explicit`](crate::memory::MemoryConfig::Explicit).
///
/// [`BufferPool::metrics`](crate::memory::BufferPool::metrics) returns current
/// accounting gauges without enabling diagnostic sampling. Buffer-pool events
/// use the `aws_sdk_s3_transfer_manager::memory` tracing target.
pub mod memory {
    pub use crate::runtime::buffer_pool::{
        AcquireError, BufferPool, BufferPoolBuildError, BufferPoolBuilder, PooledBufMut,
        Reservation, ReserveError, ReserveFuture, SegmentedBytes,
    };
    pub use crate::types::{MemoryBudgetConfig, MemoryConfig};
}

/// Test-only entry points used by external fuzz targets.
#[cfg(s3_tm_fuzz)]
#[doc(hidden)]
pub mod __fuzz {
    /// Runs one encoded buffer-pool operation sequence.
    pub fn buffer_pool_operations(data: &[u8]) {
        crate::runtime::buffer_pool::run_fuzz_input(data);
    }

    /// Runs one encoded buffer-pool placement sequence.
    pub fn buffer_pool_placement(data: &[u8]) {
        crate::runtime::buffer_pool::run_placement_fuzz_input(data);
    }
}

/// HTTP related components and utils
pub(crate) mod http;

/// Transfer types
pub(crate) mod transfer;

/// Work scheduler
pub(crate) mod scheduler;

/// Execution runtime
pub(crate) mod runtime;

/// Telemetry target constants
pub(crate) mod telemetry;

/// Metrics
pub mod metrics;

/// Body-read retry loop layered over the latency deadline guard
pub(crate) mod retry;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
pub mod wasm;

pub use self::client::Client;
use self::config::loader::ConfigLoader;
pub use self::config::Config;
pub use self::config::S3ClientConfig;
pub use self::transfer::SchedulingCtl;

/// Create a config loader
pub fn from_env() -> ConfigLoader {
    ConfigLoader::default()
}

#[cfg(test)]
mod tests;
