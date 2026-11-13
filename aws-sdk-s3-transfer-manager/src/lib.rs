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
//! # Configuration
//!
//! [`from_env`] returns a [`ConfigLoader`], which loads
//! S3 client configuration from the shared AWS configuration sources, detects
//! the host to size the automatic defaults, and applies any settings made on
//! it. [`ConfigLoader::s3_config`](config::ConfigLoader::s3_config) replaces
//! the shared-configuration load with an existing [`aws_types::SdkConfig`] or
//! [`S3ClientConfig`].
//!
//! [`Config::builder`] constructs a configuration from explicit values
//! instead. It requires an [`S3ClientConfig`], loads no shared configuration,
//! and makes no EC2 instance metadata request; the client detects the host
//! from local system information only. Use it when the S3 configuration is
//! already complete and neither environment loading nor network detection is
//! wanted.
//!
//! # Modeled types
//!
//! S3 request values, response metadata, and enums live in [`types`], and
//! their builders in [`types::builders`]. Operation modules re-export the
//! modeled types and builders they use.
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
//! * [`upload_objects`](crate::Client::upload_objects) - upload the files in a local directory to a bucket.
//!   By default only the directory's immediate files are uploaded; supply
//!   [`FsWalker::builder().recursive(true)`](crate::io::walk::FsWalkerBuilder::recursive)
//!   to upload the whole tree.
//!
//! # WebAssembly
//!
//! The crate builds for `wasm32-wasip1`, `wasm32-wasip2`, and `wasm32-wasip3`.
//! Other wasm targets are not supported. On these targets:
//!
//! - Build with `RUSTFLAGS="--cfg tokio_unstable"`. The S3 client's tokio
//!   integration enables tokio's `fs` feature, which tokio accepts on wasm only
//!   with that flag.
//! - The AWS SDK's default HTTPS client is not built. Requests fail unless the
//!   [`aws_types::SdkConfig`] or S3 configuration supplies an HTTP client, for
//!   example one built on `wasi:http`. `wasm32-wasip1` has no networking
//!   interface for such a client.
//! - [`Managed`] execution behaves as
//!   [`RuntimeConfig::multi_thread_tokio`], and a current-thread tokio runtime
//!   is supported. [`Client::shutdown_completion`] returns `None`. File reads
//!   and writes run inline on the thread that polls the transfer.
//! - The `__unstable_ext` feature, which adopts a prebuilt managed runtime, is
//!   unavailable.
//! - Memory detection is unavailable, so
//!   [`MemoryBudgetConfig::Auto`](crate::types::MemoryBudgetConfig::Auto) uses
//!   its 2 GiB fallback. Use
//!   [`MemoryBudgetConfig::Limit`](crate::types::MemoryBudgetConfig::Limit) to
//!   fit the module's linear memory.
//!
//! # Runtime diagnostics
//!
//! Tracing output and `AWS_S3_TM_DIAGNOSTICS` are diagnostics, not stable API:
//! tracing targets, event and span names, their fields, and the settings the
//! environment variable accepts may change in minor releases. Events are
//! grouped under these subsystem targets:
//!
//! - `aws_sdk_s3_transfer_manager::concurrency`: concurrency resolution and
//!   control.
//! - `aws_sdk_s3_transfer_manager::scheduling`: scheduler capacity decisions,
//!   worker pool growth, and memory-budget admission.
//! - `aws_sdk_s3_transfer_manager::memory`: buffer-pool admission, physical
//!   storage, and reclamation.
//! - `aws_sdk_s3_transfer_manager::execution`: per-work-item dispatch and
//!   completion.
//! - `aws_sdk_s3_transfer_manager::transfer`: transfer lifecycle and state
//!   transitions.
//!
//! Other events use the target of the module that emits them.
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

/// DNS-related types and functionality
pub mod dns;

/// Experimental integrations with version-coupled managed execution.
#[cfg(feature = "__unstable_ext")]
pub mod unstable;

#[cfg(all(feature = "__unstable_ext", target_family = "wasm"))]
compile_error!("`__unstable_ext` adopts a managed runtime, which wasm targets cannot run");

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

pub use self::client::Client;
use self::config::ConfigLoader;
pub use self::config::S3ClientConfig;
pub use self::config::{Config, Managed, RuntimeConfig};
pub use self::runtime::{ShutdownCompletion, ShutdownError, ShutdownErrorKind};
pub use self::transfer::SchedulingCtl;

/// Create a [`ConfigLoader`] that loads configuration from the environment.
///
/// This is the usual way to configure a [`Client`]; see
/// [`ConfigLoader`] for what [`load`](ConfigLoader::load) resolves.
pub fn from_env() -> ConfigLoader {
    ConfigLoader::default()
}

#[cfg(test)]
mod tests;
