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
//! # Diagnostics
//!
//! `AWS_S3_TM_DIAGNOSTICS` enables opt-in runtime diagnostics. Its value is a
//! comma-separated list of `key=value` settings resolved when a client
//! configuration or standalone [`memory::BufferPool`] is built:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms,memory.detail=1
//! ```
//!
//! The supported memory settings are:
//!
//! - `memory.snapshot=off` disables periodic reports. A positive integer
//!   followed by `ms` enables reports; values below `100ms` use `100ms`.
//! - `memory.detail=0` keeps the default low-frequency counters.
//! - `memory.detail=1` also counts every optimistic allocation attempt and
//!   bitmap word inspected, adding relaxed atomic updates to the acquisition
//!   path.
//!
//! Settings are case-sensitive. Whitespace around entries, keys, and values is
//! ignored. Later valid assignments replace earlier ones. Unknown keys are
//! ignored for forward compatibility; malformed recognized settings produce a
//! warning and retain the preceding value. Unsupported detail levels use the
//! highest level understood by this version and produce a warning.
//!
//! Periodic reports use the `aws_sdk_s3_transfer_manager::memory` tracing target
//! at `DEBUG`. For example, enable one-second baseline snapshots with:
//!
//! ```text
//! AWS_S3_TM_DIAGNOSTICS=memory.snapshot=1000ms
//! RUST_LOG=aws_sdk_s3_transfer_manager::memory=debug
//! ```
//!
//! Snapshot reporting reuses the memory pool's maintenance thread. It does not
//! create a diagnostics-only thread. Detailed counters and periodic reporting
//! are independent; both are disabled by default.

/// Error types emitted by `aws-sdk-s3-transfer-manager`
pub mod error;

/// Common types used by `aws-sdk-s3-transfer-manager`
pub mod types;

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
use self::config::loader::ConfigLoader;
pub use self::config::Config;
pub use self::config::S3ClientConfig;
pub use self::transfer::SchedulingCtl;

/// Create a config loader
pub fn from_env() -> ConfigLoader {
    ConfigLoader::default()
}
