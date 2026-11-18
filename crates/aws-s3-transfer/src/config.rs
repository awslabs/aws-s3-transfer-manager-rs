/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_types::error::operation::BuildError;
use std::cmp;

use crate::types::ByteSize;

mod runtime;
pub use runtime::{
    ConcurrencyMode, Managed, MemoryBudgetConfig, MemoryConfig, RuntimeConfig, TargetThroughput,
};

mod diagnostics;
pub(crate) use diagnostics::{
    DiagnosticsConfig, MemoryDiagnosticsConfig, TransferDiagnosticsConfig,
};

mod loader;
pub use loader::ConfigLoader;
pub(crate) mod user_agent;

/// Minimum upload part size
pub(crate) const MIN_MULTIPART_PART_SIZE: ByteSize = ByteSize::mib(5);

/// The target part size for an upload or download request.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum PartSize {
    /// Automatically configure an optimal target part size based on the execution environment.
    #[default]
    Auto,

    /// Target part size explicitly given.
    ///
    /// NOTE: This is a suggestion and will be used if possible but may be adjusted for an individual request
    /// as required by the underlying API.
    Target(ByteSize),
}

/// How far a download fetches ahead of the data its consumer has taken.
///
/// A download fetches parts concurrently and holds each one until the consumer
/// takes it: a streamed body reads it, or a file destination writes it.
/// Read-ahead limits how many parts one download fetches and holds before the
/// consumer takes them. When the consumer falls that far behind, the download
/// stops fetching new parts until the consumer takes more. The client's memory
/// budget applies across all transfers in addition to this limit.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReadAhead {
    /// A limit that doesn't slow a consumer that keeps pace, so the memory
    /// budget is the bound in practice. This is the default.
    #[default]
    Auto,

    /// At most `n` parts beyond the one the consumer is waiting for; `0`
    /// fetches only that part, one at a time.
    ///
    /// Use a small value when a consumer may stop reading early or reads
    /// slowly. Change it while a download runs with
    /// [`DownloadIoCtl::set_read_ahead`](crate::operation::download::DownloadIoCtl::set_read_ahead).
    Parts(usize),
}

/// S3 client configuration for the transfer manager.
///
/// The transfer manager builds its S3 client from this configuration and keeps
/// any HTTP client the configuration sets. With no HTTP client configured, managed execution
/// supplies the transfer manager's transport and
/// [`RuntimeConfig::multi_thread_tokio`] uses the SDK default; see
/// [`RuntimeConfig`](RuntimeConfig#http-transport).
///
/// With the `sdk-v1` feature, this also converts from
/// `aws_sdk_s3::config::Builder` for S3-specific configuration.
///
/// ```no_run
/// # async fn example() -> Result<(), aws_smithy_types::error::operation::BuildError> {
/// use aws_config::BehaviorVersion;
///
/// let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
/// let config = aws_sdk_s3_transfer_manager::Config::builder()
///     .s3_config(&sdk_config)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct S3ClientConfig {
    pub(crate) config: aws_sdk_s3::Config,
}

impl S3ClientConfig {
    /// Create S3 client configuration from shared AWS configuration.
    ///
    /// With `sdk-v1` enabled, an `aws_sdk_s3::config::Builder` is also accepted.
    pub fn new(config: impl Into<Self>) -> Self {
        config.into()
    }

    pub(crate) fn from_sdk_builder(builder: aws_sdk_s3::config::Builder) -> Self {
        Self {
            config: builder.build(),
        }
    }
}

impl From<&aws_types::SdkConfig> for S3ClientConfig {
    fn from(sdk_config: &aws_types::SdkConfig) -> Self {
        Self::from_sdk_builder(aws_sdk_s3::config::Builder::from(sdk_config))
    }
}

#[cfg(feature = "sdk-v1")]
impl From<aws_sdk_s3::config::Builder> for S3ClientConfig {
    fn from(builder: aws_sdk_s3::config::Builder) -> Self {
        Self::from_sdk_builder(builder)
    }
}

impl std::fmt::Debug for S3ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ClientConfig").finish_non_exhaustive()
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
    runtime_config: RuntimeConfig,
    read_ahead: ReadAhead,
    memory: MemoryConfig,
    diagnostics: DiagnosticsConfig,
    s3_client_source: Option<S3ClientSource>,
    /// Machine facts detected once by the async config loader, off the
    /// `Client::new` hot path. `None` when the config was built directly
    /// (bypassing the loader); consumers then fall back to cheap local detection.
    machine_profile: Option<crate::runtime::platform::MachineProfile>,
    /// Interfaces the runtime-provided transport binds its connections to;
    /// empty leaves interface selection to OS routing.
    pub(crate) network_interfaces: Vec<String>,
}

impl Config {
    /// Create a new `Config` builder.
    ///
    /// The builder uses only the values it is given: it loads no shared AWS
    /// configuration and does not query EC2 instance metadata. Use
    /// [`from_env`](crate::from_env) to load configuration from the environment.
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

    /// The [`ConcurrencyMode::TargetThroughput`] target in Gbps, if configured.
    pub(crate) fn throughput_target_gbps(&self) -> Option<f64> {
        match &self.concurrency {
            ConcurrencyMode::TargetThroughput(target) => Some(target.as_gbps()),
            _ => None,
        }
    }

    /// Returns the execution configuration for transfer work.
    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }

    #[cfg(not(target_family = "wasm"))]
    pub(crate) fn managed_config_mut(&mut self) -> Option<&mut Managed> {
        self.runtime_config.managed_config_mut()
    }

    /// Returns the read-ahead mode used for downloads.
    ///
    /// This is the client default; a download may override it per request via
    /// [`DownloadInput`](crate::operation::download::DownloadInput).
    pub fn read_ahead(&self) -> &ReadAhead {
        &self.read_ahead
    }

    /// Returns the payload-memory configuration.
    pub fn memory(&self) -> &MemoryConfig {
        &self.memory
    }

    /// Returns the diagnostic policy fixed when this configuration was built.
    pub(crate) fn diagnostics(&self) -> DiagnosticsConfig {
        self.diagnostics
    }

    /// Machine facts detected by the async config loader, if this config was
    /// built through it. `None` when built directly.
    pub(crate) fn machine_profile(&self) -> Option<&crate::runtime::platform::MachineProfile> {
        self.machine_profile.as_ref()
    }

    /// HTTP options for the runtime-provided transport, or `None` when the
    /// managed transport would not be installed: HTTP is already configured,
    /// a finished S3 client was supplied, execution uses the caller's runtime,
    /// or the target is wasm, which has no managed transport.
    pub(crate) fn runtime_http(
        &self,
        max_connections_per_host: usize,
    ) -> Option<crate::runtime::RuntimeHttpOptions> {
        if cfg!(target_family = "wasm") {
            return None;
        }
        match self.s3_client_source.as_ref()? {
            S3ClientSource::FromConfig(config)
                if self.runtime_config.managed_config().is_some()
                    && config.config.http_client().is_none() =>
            {
                Some(crate::runtime::RuntimeHttpOptions {
                    network_interfaces: self.network_interfaces.clone(),
                    max_connections_per_host,
                })
            }
            _ => None,
        }
    }

    /// Consume the S3 client source, returning it.
    pub(crate) fn take_s3_client_source(&mut self) -> S3ClientSource {
        self.s3_client_source
            .take()
            .expect("s3 client source already taken")
    }
}

/// Fluent style builder for [Config]
#[derive(Debug, Default)]
pub struct Builder {
    multipart_threshold_part_size: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencyMode,
    runtime_config: RuntimeConfig,
    read_ahead: ReadAhead,
    memory: MemoryConfig,
    diagnostics: Option<DiagnosticsConfig>,
    client: Option<aws_sdk_s3::Client>,
    s3_client_config: Option<S3ClientConfig>,
    machine_profile: Option<crate::runtime::platform::MachineProfile>,
}

impl Builder {
    /// Minimum object size that should trigger a multipart upload.
    ///
    /// The minimum part size is 5 MiB, any part size less than that will be rounded up.
    /// Default is [PartSize::Auto]
    pub fn multipart_threshold(self, threshold: PartSize) -> Self {
        let threshold = match threshold {
            PartSize::Target(part_size) => {
                PartSize::Target(cmp::max(part_size, MIN_MULTIPART_PART_SIZE))
            }
            tps => tps,
        };

        self.set_multipart_threshold(threshold)
    }

    /// The target size of each part when using a multipart upload to complete the request.
    ///
    /// When a request's content length is less than [`multipart_threshold`],
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
                PartSize::Target(cmp::max(part_size, MIN_MULTIPART_PART_SIZE))
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

    /// Sets execution for transfer work.
    ///
    /// Defaults to owned managed execution.
    pub fn runtime_config(mut self, config: RuntimeConfig) -> Self {
        self.set_runtime_config(Some(config));
        self
    }

    /// Sets execution configuration, or restores the default with `None`.
    pub fn set_runtime_config(&mut self, config: Option<RuntimeConfig>) -> &mut Self {
        self.runtime_config = config.unwrap_or_default();
        self
    }

    /// Set how far downloads may prefetch ahead of the consumer.
    ///
    /// This is the client default for all downloads; a request may override it via
    /// [`DownloadInput`](crate::operation::download::DownloadInput), or a running
    /// transfer may adjust it via
    /// [`DownloadIoCtl`](crate::operation::download::DownloadIoCtl).
    /// Default is [ReadAhead::Auto].
    pub fn read_ahead(mut self, mode: ReadAhead) -> Self {
        self.read_ahead = mode;
        self
    }

    /// Set the payload-memory configuration.
    ///
    /// Default is [`MemoryConfig::Auto`].
    pub fn memory(mut self, memory: MemoryConfig) -> Self {
        self.memory = memory;
        self
    }

    /// Installs deterministic diagnostics for internal tests.
    #[cfg(test)]
    pub(crate) fn diagnostics_for_test(
        mut self,
        memory: MemoryDiagnosticsConfig,
        transfer_detail_level: u64,
    ) -> Self {
        self.diagnostics = Some(DiagnosticsConfig::for_test(memory, transfer_detail_level));
        self
    }

    /// Inject a finished SDK client for library unit fixtures.
    #[cfg(test)]
    pub(crate) fn sdk_client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the S3 client configuration.
    ///
    /// The transfer manager preserves a configured HTTP client. With no
    /// configured HTTP, managed execution installs its default transport;
    /// caller-runtime execution uses the SDK default.
    ///
    /// Required: [`build`](Self::build) fails without it.
    pub fn s3_config(mut self, config: impl Into<S3ClientConfig>) -> Self {
        self.s3_client_config = Some(config.into());
        self
    }

    /// Set the machine profile detected by the async config loader. Internal:
    /// populated by [`ConfigLoader`], not a public builder method.
    pub(crate) fn machine_profile(
        mut self,
        profile: Option<crate::runtime::platform::MachineProfile>,
    ) -> Self {
        self.machine_profile = profile;
        self
    }

    /// Consumes the builder and constructs a [`Config`].
    ///
    /// # Errors
    ///
    /// Returns a [`BuildError`] if [`s3_config`](Self::s3_config) was not set.
    pub fn build(self) -> Result<Config, BuildError> {
        let s3_client_source = match (self.client, self.s3_client_config) {
            (Some(client), _) => S3ClientSource::Provided(client),
            (None, Some(config)) => S3ClientSource::FromConfig(Box::new(config)),
            (None, None) => {
                return Err(BuildError::missing_field(
                    "s3_config",
                    "S3 client configuration is required",
                ))
            }
        };
        Ok(Config {
            multipart_threshold: self.multipart_threshold_part_size,
            target_part_size: self.target_part_size,
            concurrency: self.concurrency,
            runtime_config: self.runtime_config,
            read_ahead: self.read_ahead,
            memory: self.memory,
            diagnostics: self.diagnostics.unwrap_or_else(DiagnosticsConfig::from_env),
            s3_client_source: Some(s3_client_source),
            machine_profile: self.machine_profile,
            network_interfaces: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_config_builder() -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::new()
    }

    #[test]
    fn s3_client_config_converts_from_sdk_config_and_builder() {
        let region = aws_types::region::Region::from_static("us-west-2");
        let sdk_config = aws_types::SdkConfig::builder()
            .region(region.clone())
            .build();
        let from_sdk = S3ClientConfig::from(&sdk_config);
        assert_eq!(from_sdk.config.region(), Some(&region));

        let builder = s3_config_builder().region(region.clone());
        #[cfg(feature = "sdk-v1")]
        let from_builder = S3ClientConfig::new(builder);
        #[cfg(not(feature = "sdk-v1"))]
        let from_builder = S3ClientConfig::from_sdk_builder(builder);
        assert_eq!(from_builder.config.region(), Some(&region));
    }

    #[test]
    fn runtime_http_present_for_s3_config() {
        let config = Config::builder()
            .s3_config(S3ClientConfig::from_sdk_builder(s3_config_builder()))
            .build()
            .expect("valid config");
        let http = config.runtime_http(64).expect("runtime HTTP enabled");
        assert!(http.network_interfaces.is_empty());
        assert_eq!(http.max_connections_per_host, 64);
    }

    #[test]
    fn runtime_http_absent_without_runtime_transport() {
        let shared = Config::builder()
            .s3_config(S3ClientConfig::from_sdk_builder(s3_config_builder()))
            .runtime_config(RuntimeConfig::multi_thread_tokio())
            .build()
            .expect("valid config");
        assert!(shared.runtime_http(64).is_none());

        let configured = Config::builder()
            .s3_config(S3ClientConfig::from_sdk_builder(
                aws_smithy_mocks::mock_client!(aws_sdk_s3, [])
                    .config()
                    .to_builder(),
            ))
            .build()
            .expect("valid config");
        assert!(configured.runtime_http(64).is_none());

        // A mock client: a real one would build the default HTTPS client, whose
        // TLS initialization is foreign code that miri cannot run.
        let provided = Config::builder()
            .sdk_client(aws_smithy_mocks::mock_client!(aws_sdk_s3, []))
            .build()
            .expect("valid config");
        assert!(provided.runtime_http(64).is_none());
    }

    #[test]
    fn build_without_s3_config_is_an_error() {
        let error = Config::builder()
            .build()
            .expect_err("s3_config is required");
        assert!(
            error.to_string().contains("s3_config"),
            "error does not name the missing field: {error}"
        );
    }
}
