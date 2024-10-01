/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::unit::ByteUnit;
use crate::types::{ConcurrencySetting, PartSize};
use std::cmp;

pub(crate) mod loader;

/// Minimum upload part size in bytes
const MIN_MULTIPART_PART_SIZE_BYTES: u64 = 5 * ByteUnit::Mebibyte.as_bytes_u64();

/// Configuration for a [`Client`](crate::client::Client)
#[derive(Debug, Clone)]
pub struct Config {
    multipart_threshold: PartSize,
    target_part_size: PartSize,
    concurrency: ConcurrencySetting,
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

    /// Returns the concurrency setting to use for transfer operations.
    /// This is the maximum number of in-flight requests allowed across _all_ operations.
    pub fn concurrency(&self) -> &ConcurrencySetting {
        &self.concurrency
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
    concurrency: ConcurrencySetting,
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

    /// Set the concurrency level this component is allowed to use.
    ///
    /// This sets the maximum number of concurrent in-flight requests across _all_ operations.
    /// Default is [ConcurrencySetting::Auto].
    pub fn concurrency(mut self, concurrency: ConcurrencySetting) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set an explicit S3 client to use.
    pub fn client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Consumes the builder and constructs a [`Config`](crate::config::Config)
    pub fn build(self) -> Config {
        Config {
            multipart_threshold: self.multipart_threshold_part_size,
            target_part_size: self.target_part_size,
            concurrency: self.concurrency,
            client: self.client.expect("client set"),
        }
    }
}
