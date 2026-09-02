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
//! # Logging
//!
//! Logs and spans are emitted through `tracing`. Rather than one target per module,
//! events and spans are grouped onto four targets by concern, so a filter can select a
//! concern without naming internal module paths:
//!
//! | Target | Covers |
//! |---|---|
//! | `aws_sdk_s3_transfer_manager::transfer` | transfer lifecycle, per-request spans, directory walking |
//! | `aws_sdk_s3_transfer_manager::execution` | per-work-item dispatch, completion, and panics |
//! | `aws_sdk_s3_transfer_manager::scheduling` | scheduler capacity, worker growth, submission handoff, memory-budget admission |
//! | `aws_sdk_s3_transfer_manager::concurrency` | concurrency-controller decisions |
//!
//! To follow transfer activity without the scheduling and dispatch streams:
//!
//! ```text
//! RUST_LOG=info,aws_sdk_s3_transfer_manager::transfer=debug
//! ```
//!
//! Work executes on separate threads, so spans from one transfer do not nest under a
//! single parent span. Where a line names a transfer it does so with a `tid` field rather
//! than by nesting; the poll and execute spans that bracket every work item both carry one.

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
