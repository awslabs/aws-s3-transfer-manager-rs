/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/* Automatically managed default lints */
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
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
//!     .send()
//!     .await?;
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

/// Default in-flight concurrency
pub(crate) const DEFAULT_CONCURRENCY: usize = 128;

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

/// Tower related middleware and components
pub(crate) mod middleware;

/// HTTP related components and utils
pub(crate) mod http;

/// Internal runtime components
pub(crate) mod runtime;

/// Metrics
pub mod metrics;

pub use self::client::Client;
use self::config::loader::ConfigLoader;
pub use self::config::Config;

/// Create a config loader
pub fn from_env() -> ConfigLoader {
    ConfigLoader::default()
}
