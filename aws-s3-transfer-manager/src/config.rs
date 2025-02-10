/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_runtime::user_agent::FrameworkMetadata;
use std::cmp;

use crate::metrics::unit::ByteUnit;
use crate::types::{ConcurrencyMode, PartSize};

pub(crate) mod loader;

/// Minimum upload part size in bytes
pub(crate) const MIN_MULTIPART_PART_SIZE_BYTES: u64 = 5 * ByteUnit::Mebibyte.as_bytes_u64();

// FIXME - should target throughput be configurable for upload and download independently?

/// Configuration for a [`Client`](crate::client::Client)
#[derive(Debug, Clone)]
pub struct Config {
    multipart_threshold: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencyMode,
    framework_metadata: Option<FrameworkMetadata>,
    client: aws_sdk_s3::client::Client,
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

    /// Returns the framework metadata setting when using transfer manager.
    #[doc(hidden)]
    pub fn framework_metadata(&self) -> Option<&FrameworkMetadata> {
        self.framework_metadata.as_ref()
    }

    /// The Amazon S3 client instance that will be used to send requests to S3.
    pub fn client(&self) -> &aws_sdk_s3::Client {
        &self.client
    }
}

/// Fluent style builder for [Config]
#[derive(Debug, Clone, Default)]
pub struct Builder {
    multipart_threshold_part_size: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencyMode,
    framework_metadata: Option<FrameworkMetadata>,
    client: Option<aws_sdk_s3::Client>,
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
    pub fn client(mut self, client: aws_sdk_s3::Client) -> Self {
        // TODO - decide the approach here:
        // - Convert the client to build to modify it based on other configs for transfer manager
        // - Instead of taking the client, take sdk-config/s3-config/builder?
        self.client = Some(client);
        self
    }

    /// Consumes the builder and constructs a [`Config`]
    pub fn build(self) -> Config {
        Config {
            multipart_threshold: self.multipart_threshold_part_size,
            target_part_size: self.target_part_size,
            concurrency: self.concurrency,
            framework_metadata: self.framework_metadata,
            client: self.client.expect("client set"),
        }
    }
}
