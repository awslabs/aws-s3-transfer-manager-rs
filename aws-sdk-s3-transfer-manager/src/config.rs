/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_runtime::user_agent::FrameworkMetadata;
use std::cmp;

use crate::metrics::unit::ByteUnit;
use crate::types::{ConcurrencyMode, MemoryBudgetConfig, PartSize};

pub(crate) mod loader;

/// Minimum upload part size in bytes
pub(crate) const MIN_MULTIPART_PART_SIZE_BYTES: u64 = 5 * ByteUnit::Mebibyte.as_bytes_u64();

// FIXME - should target throughput be configurable for upload and download independently?

/// S3 client configuration for the transfer manager.
///
/// Wraps an `aws_sdk_s3::config::Builder` with transfer-manager-specific
/// options. The transfer manager builds the S3 client from this configuration,
/// injecting runtime-optimized HTTP transport by default.
pub struct S3ClientConfig {
    pub(crate) builder: aws_sdk_s3::config::Builder,
    pub(crate) enable_runtime_http: bool,
}

impl S3ClientConfig {
    /// Create a new `S3ClientConfig` from an SDK config builder.
    pub fn new(builder: aws_sdk_s3::config::Builder) -> Self {
        Self {
            builder,
            enable_runtime_http: true,
        }
    }

    /// Control whether the transfer manager manages HTTP transport.
    ///
    /// When `true` (default), the runtime injects an HTTP client optimized
    /// for its execution model (e.g. per-thread connection pools on managed
    /// threads). When `false`, the HTTP client already set on the builder
    /// is used as-is.
    pub fn enable_runtime_http(mut self, enable: bool) -> Self {
        self.enable_runtime_http = enable;
        self
    }
}

impl std::fmt::Debug for S3ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ClientConfig")
            .field("enable_runtime_http", &self.enable_runtime_http)
            .finish_non_exhaustive()
    }
}

/// How the S3 client is provided to the transfer manager.
#[derive(Debug)]
pub(crate) enum S3ClientSource {
    /// User provided a finished S3 client. Use as-is.
    Provided(aws_sdk_s3::Client),
    /// Build the S3 client from config, injecting runtime components.
    FromConfig(Box<S3ClientConfig>),
}

/// Configuration for a [`Client`](crate::client::Client)
#[derive(Debug)]
pub struct Config {
    multipart_threshold: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencyMode,
    memory_budget: MemoryBudgetConfig,
    framework_metadata: Option<FrameworkMetadata>,
    s3_client_source: Option<S3ClientSource>,
    #[cfg(feature = "dial9")]
    pub(crate) telemetry_guard: Option<dial9_tokio_telemetry::telemetry::TelemetryGuard>,
}

impl Config {
    /// Create a new `Config` builder
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Returns a reference to the multipart upload threshold part size
    pub fn multipart_threshold(&self) -> &PartSize {
        &self.multipart_threshold
    }

    /// Returns a reference to the target part size to use for transfer operations
    pub fn part_size(&self) -> &PartSize {
        &self.target_part_size
    }

    /// Returns the concurrency mode to use for transfer operations.
    ///
    /// This is the mode used for concurrent in-flight requests across _all_ operations.
    pub fn concurrency(&self) -> &ConcurrencyMode {
        &self.concurrency
    }

    /// Returns the memory budget configuration.
    pub fn memory_budget(&self) -> &MemoryBudgetConfig {
        &self.memory_budget
    }

    /// Returns the framework metadata setting when using transfer manager.
    #[doc(hidden)]
    pub fn framework_metadata(&self) -> Option<&FrameworkMetadata> {
        self.framework_metadata.as_ref()
    }

    /// Consume the S3 client source, returning it.
    pub(crate) fn take_s3_client_source(&mut self) -> S3ClientSource {
        self.s3_client_source
            .take()
            .expect("s3 client source already taken")
    }

    /// Take the telemetry guard, if set.
    #[cfg(feature = "dial9")]
    pub(crate) fn take_telemetry_guard(
        &mut self,
    ) -> Option<dial9_tokio_telemetry::telemetry::TelemetryGuard> {
        self.telemetry_guard.take()
    }
}

/// Fluent style builder for [Config]
#[derive(Debug, Default)]
pub struct Builder {
    multipart_threshold_part_size: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencyMode,
    memory_budget: MemoryBudgetConfig,
    pub(crate) framework_metadata: Option<FrameworkMetadata>,
    client: Option<aws_sdk_s3::Client>,
    s3_client_config: Option<S3ClientConfig>,
    #[cfg(feature = "dial9")]
    telemetry_guard: Option<dial9_tokio_telemetry::telemetry::TelemetryGuard>,
}

impl Builder {
    /// Minimum object size that should trigger a multipart upload.
    ///
    /// The minimum part size is 5 MiB, any part size less than that will be rounded up.
    /// Default is [PartSize::Auto]
    pub fn multipart_threshold(self, threshold: PartSize) -> Self {
        let threshold = match threshold {
            PartSize::Target(part_size) => {
                PartSize::Target(cmp::max(part_size, MIN_MULTIPART_PART_SIZE_BYTES))
            }
            tps => tps,
        };

        self.set_multipart_threshold(threshold)
    }

    /// The target size of each part when using a multipart upload to complete the request.
    ///
    /// When a request's content length is les than [`multipart_threshold`],
    /// this setting is ignored and a single [`PutObject`] request will be made instead.
    ///
    /// NOTE: The actual part size used may be larger than the configured part size if
    /// the current value would result in more than 10,000 parts for an upload request.
    ///
    /// Default is [PartSize::Auto]
    ///
    /// [`multipart_threshold`]: method@Self::multipart_threshold
    /// [`PutObject`]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
    pub fn part_size(self, part_size: PartSize) -> Self {
        let threshold = match part_size {
            PartSize::Target(part_size) => {
                PartSize::Target(cmp::max(part_size, MIN_MULTIPART_PART_SIZE_BYTES))
            }
            tps => tps,
        };

        self.set_target_part_size(threshold)
    }

    /// Minimum object size that should trigger a multipart upload.
    ///
    /// NOTE: This does not validate the setting and is meant for internal use only.
    pub(crate) fn set_multipart_threshold(mut self, threshold: PartSize) -> Self {
        self.multipart_threshold_part_size = threshold;
        self
    }

    /// Target part size for a multipart upload.
    ///
    /// NOTE: This does not validate the setting and is meant for internal use only.
    pub(crate) fn set_target_part_size(mut self, threshold: PartSize) -> Self {
        self.target_part_size = threshold;
        self
    }

    /// Set the concurrency mode this client should use.
    ///
    /// This sets the mode used for concurrent in-flight requests across _all_ operations.
    /// Default is [ConcurrencyMode::Auto].
    pub fn concurrency(mut self, mode: ConcurrencyMode) -> Self {
        self.concurrency = mode;
        self
    }

    /// Set the memory budget: an upper bound on memory used for in-flight and
    /// buffered transfer data. At the limit transfers backpressure rather than
    /// fail. Default is [`MemoryBudgetConfig::Auto`] (a safe fraction of detected
    /// RAM). Takes effect only when the transfer manager owns its runtime.
    pub fn memory_budget(mut self, budget: MemoryBudgetConfig) -> Self {
        self.memory_budget = budget;
        self
    }

    /// Sets the framework metadata for the transfer manager.
    ///
    /// This _optional_ name is used to identify the framework using transfer manager in the user agent that
    /// gets sent along with requests.
    #[doc(hidden)]
    pub fn framework_metadata(mut self, framework_metadata: Option<FrameworkMetadata>) -> Self {
        self.framework_metadata = framework_metadata;
        self
    }

    /// Set an explicit S3 client to use.
    ///
    /// Either this or [`s3_config`](Self::s3_config) must be set.
    #[doc(hidden)]
    pub fn client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the S3 client configuration.
    ///
    /// The transfer manager builds the S3 client from this configuration,
    /// injecting runtime-optimized HTTP transport by default. Use
    /// [`S3ClientConfig::enable_runtime_http`] to opt out.
    ///
    /// Either this or [`client`](Self::client) must be set.
    pub fn s3_config(mut self, config: S3ClientConfig) -> Self {
        self.s3_client_config = Some(config);
        self
    }

    /// Set a dial9 telemetry guard for runtime tracing.
    ///
    /// When set, each managed worker runtime will be traced via dial9-tokio-telemetry.
    #[cfg(feature = "dial9")]
    pub fn telemetry_guard(
        mut self,
        guard: dial9_tokio_telemetry::telemetry::TelemetryGuard,
    ) -> Self {
        self.telemetry_guard = Some(guard);
        self
    }

    /// Consumes the builder and constructs a [`Config`]
    pub fn build(self) -> Config {
        let s3_client_source = match (self.client, self.s3_client_config) {
            (Some(client), _) => S3ClientSource::Provided(client),
            (None, Some(config)) => S3ClientSource::FromConfig(Box::new(config)),
            (None, None) => panic!("either client() or s3_config() must be set"),
        };
        Config {
            multipart_threshold: self.multipart_threshold_part_size,
            target_part_size: self.target_part_size,
            concurrency: self.concurrency,
            memory_budget: self.memory_budget,
            framework_metadata: self.framework_metadata,
            s3_client_source: Some(s3_client_source),
            #[cfg(feature = "dial9")]
            telemetry_guard: self.telemetry_guard,
        }
    }
}
