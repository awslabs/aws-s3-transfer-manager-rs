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
// Modules with children use `foo.rs` next to a `foo/` directory, not `foo/mod.rs`.
#![warn(clippy::mod_module_files)]

//! Transfer Manager for Amazon S3 - a high-performance data plane client for uploading and downloading
//! objects and directories.
//!
//! The transfer manager splits large transfers into concurrent part requests over the Amazon S3
//! [service API], retries each failed part individually, and sizes its concurrency and memory use to the
//! host. [`Client`] starts transfers; each returns a handle to wait for, control or abort the transfer.
//!
//! [service API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
//!
//! # Examples
//!
//! Load the default configuration:
//!
//! ```no_run
//! # async fn example() {
//! let config = aws_s3_transfer::from_env().load().await;
//! let client = aws_s3_transfer::Client::new(config);
//! # }
//! ```
//!
//! Download a bucket to a local directory:
//!
//! ```no_run
//! # async fn example() -> Result<(), aws_s3_transfer::error::Error> {
//! let config = aws_s3_transfer::from_env().load().await;
//! let client = aws_s3_transfer::Client::new(config);
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
//! # Operations
//!
//! See the documentation for each client operation for more information:
//!
//! * [`download`](crate::Client::download) - download a single object
//! * [`upload`](crate::Client::upload) - upload a single object
//! * [`download_objects`](crate::Client::download_objects) - download a bucket, or the objects
//!   under a prefix set with [`S3WalkerBuilder::prefix`](crate::io::walk::S3WalkerBuilder::prefix),
//!   to a local directory
//! * [`upload_objects`](crate::Client::upload_objects) - upload the files in a local directory to a bucket.
//!   By default only the directory's immediate files are uploaded; supply
//!   [`FsWalker::builder().recursive(true)`](crate::io::walk::FsWalkerBuilder::recursive)
//!   to upload the whole tree.
//!
//! # Configuration
//!
//! [`from_env`] returns a [`ConfigLoader`], which loads
//! S3 client configuration from the shared AWS configuration sources, detects
//! the host to size the automatic defaults, and applies any settings made on
//! it. [`ConfigLoader::s3_config`](config::ConfigLoader::s3_config) replaces
//! the shared-configuration load with an existing [`aws_types::SdkConfig`] or
//! [`S3ClientConfig`](config::S3ClientConfig).
//!
//! [`Config::builder`] constructs a configuration from explicit values
//! instead. It requires an [`S3ClientConfig`](config::S3ClientConfig), loads no shared configuration,
//! and makes no EC2 instance metadata request; the client detects the host
//! from local system information only. Use it when the S3 configuration is
//! already complete and neither environment loading nor network detection is
//! wanted.
//!
//! [`RuntimeConfig`](config::RuntimeConfig) selects where transfer work runs,
//! which also determines the HTTP transport used when no HTTP client is
//! configured; see [Runtime and HTTP](#runtime-and-http).
//!
//! # Integrity
//!
//! Uploads send the checksums selected by
//! [`ChecksumStrategy`](crate::types::ChecksumStrategy), which S3
//! verifies against the bytes it receives. An expected value set with
//! [`ChecksumStrategy::with_expected_value`](crate::types::ChecksumStrategy::with_expected_value)
//! has S3 check the uploaded object against its source.
//!
//! A completed download reports whether the bytes it delivered were validated
//! against S3's checksums:
//! [`DownloadOutput::integrity_checks`](crate::operation::download::DownloadOutput::integrity_checks)
//! returns [`IntegrityChecks`](crate::types::IntegrityChecks), whose
//! [`ChecksumValidation`](crate::types::ChecksumValidation) is `Validated` or
//! says why the bytes were not validated. Validation covers the bytes as
//! received from S3, not the destination they were written to. A checksum
//! mismatch fails `join()` with
//! [`ErrorKind::IntegrityError`](crate::error::ErrorKind::IntegrityError).
//! Streamed bytes can arrive before the final check, so a download has
//! succeeded only when `join()` returns `Ok`.
//!
//! A successful download to a path means the operating system accepted every
//! byte and the file was renamed into place; the transfer manager does not sync
//! the data to stable storage. The [`download`](crate::operation::download)
//! module describes when each check applies.
//!
//! # Runtime and HTTP
//!
//! By default, transfer work runs on threads the transfer manager creates and
//! owns ([`RuntimeConfig::managed`](config::RuntimeConfig::managed)), and
//! requests use the transfer manager's own HTTP transport, which drives each
//! connection on the thread that opened it.
//! [`RuntimeConfig::multi_thread_tokio`](config::RuntimeConfig::multi_thread_tokio)
//! runs transfer work on the caller's Tokio runtime instead, and requests use
//! the AWS SDK's default HTTP client.
//!
//! An HTTP client set on the [`aws_types::SdkConfig`] or S3 configuration is
//! used with either runtime, in place of the transfer manager's transport or
//! the SDK default. To change TLS, DNS or connection settings, configure an
//! HTTP client. [`RuntimeConfig`](config::RuntimeConfig#http-transport)
//! describes what each transport provides.
//!
//! `aws-s3-transfer-ext` is an experimental crate that adopts a
//! managed runtime built by the caller; its API may change in any release.
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
//! - [`Managed`](config::Managed) execution behaves as
//!   [`RuntimeConfig::multi_thread_tokio`](config::RuntimeConfig::multi_thread_tokio), and a current-thread tokio runtime
//!   is supported. [`Client::shutdown_completion`] returns `None`. File reads
//!   and writes run inline on the thread that polls the transfer.
//! - The `__unstable_ext` feature, which adopts a prebuilt managed runtime, is
//!   unavailable.
//! - Memory detection is unavailable, so
//!   [`MemoryBudgetConfig::Auto`](crate::config::MemoryBudgetConfig::Auto) uses
//!   its 2 GiB fallback. Use
//!   [`MemoryBudgetConfig::Limit`](crate::config::MemoryBudgetConfig::Limit) to
//!   fit the module's linear memory.
//!
//! # Runtime diagnostics
//!
//! Tracing output and `AWS_S3_TM_DIAGNOSTICS` are diagnostics, not stable API:
//! tracing targets, event and span names, their fields, and the settings the
//! environment variable accepts may change in minor releases. Events are
//! grouped under these subsystem targets:
//!
//! - `aws_s3_transfer::concurrency`: concurrency resolution and
//!   control.
//! - `aws_s3_transfer::scheduling`: scheduler capacity decisions,
//!   worker pool growth, and memory-budget admission.
//! - `aws_s3_transfer::memory`: buffer-pool admission, physical
//!   storage, and reclamation.
//! - `aws_s3_transfer::execution`: per-work-item dispatch and
//!   completion.
//! - `aws_s3_transfer::transfer`: transfer lifecycle and state
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
//! Memory snapshots use the `aws_s3_transfer::memory` tracing target
//! at `DEBUG`:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms
//! RUST_LOG=aws_s3_transfer::memory=debug
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
//! - `transfer.detail=1` collects an aggregate summary for each upload,
//!   download, `upload_objects` and `download_objects` transfer.
//! - `transfer.detail=2` also collects individual state transitions.
//!
//! Transfer summaries use the operation's tracing target at `DEBUG`;
//! level-2 transitions use the same target at `TRACE`. Each operation logs
//! under its own target below `aws_s3_transfer::transfer`, so the
//! `transfer` target enables all of them:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=transfer.detail=1
//! RUST_LOG=aws_s3_transfer::transfer=debug
//! ```

/// Errors returned by transfers, and how to classify them.
pub mod error;

/// S3 values and response metadata, transfer policies and outcomes, and byte-size and
/// throughput units.
pub mod types;

/// Modeled S3 values and builders.
// `model` and `sdk_v1` are generated directories that tools/scripts/install_model.py
// replaces whole, each with a `mod.rs`.
#[allow(clippy::mod_module_files)]
pub(crate) mod model;
#[allow(clippy::mod_module_files)]
pub(crate) mod sdk_v1;

/// Upload bodies, custom part streams, and walkers for local directories and S3 buckets.
pub mod io;

/// The transfer manager client and its operations.
pub mod client;

/// Inputs, builders, handles and outputs for each operation.
pub mod operation;

/// Client configuration and loading: S3 settings, part size, concurrency, read-ahead, runtime
/// and memory.
pub mod config;

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
/// [`MemoryConfig::Explicit`](crate::config::MemoryConfig::Explicit).
///
/// [`BufferPool::metrics`](crate::memory::BufferPool::metrics) returns current
/// accounting gauges without enabling diagnostic sampling. Buffer-pool events
/// use the `aws_s3_transfer::memory` tracing target.
pub mod memory {
    pub use crate::runtime::buffer_pool::{
        BufferPool, BufferPoolBuildError, BufferPoolBuilder, SegmentedBytes,
    };
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

/// Allocation-API access for out-of-crate benchmarks, built only under `cfg(s3_tm_bench)`.
#[cfg(s3_tm_bench)]
#[doc(hidden)]
pub mod __bench {
    pub use crate::runtime::buffer_pool::bench_util::{
        AcquireError, BufferPoolExt, PooledBufMut, Reservation, ReserveError, ReserveFuture,
    };
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

/// Transfer progress and I/O metrics, and client memory metrics.
pub mod metrics;

/// Body-read retry loop layered over the latency deadline guard
pub(crate) mod retry;

pub use self::client::Client;
pub use self::config::Config;
use self::config::ConfigLoader;
pub use self::runtime::{ShutdownCompletion, ShutdownError, ShutdownErrorKind};
pub use self::transfer::SchedulingCtl;

/// Create a [`ConfigLoader`] that loads configuration from the environment.
///
/// This is the usual way to configure a [`Client`]; see
/// [`ConfigLoader`] for what [`load`](ConfigLoader::load) resolves.
pub fn from_env() -> ConfigLoader {
    ConfigLoader::default()
}

/// Compiles the package README's examples as doctests.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
mod tests;
