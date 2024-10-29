/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_smithy_experimental::hyper_1_0::{CryptoMode, HyperClientBuilder};

use crate::config::Builder;
use crate::{
    http,
    types::{ConcurrencySetting, PartSize},
    Config,
};

/// Load transfer manager [`Config`] from the environment.
#[derive(Default, Debug)]
pub struct ConfigLoader {
    builder: Builder,
}

impl ConfigLoader {
    /// Minimum object size that should trigger a multipart upload.
    ///
    /// The minimum part size is 5 MiB, any part size less than that will be rounded up.
    /// Default is [PartSize::Auto]
    pub fn multipart_threshold(mut self, threshold: PartSize) -> Self {
        self.builder = self.builder.multipart_threshold(threshold);
        self
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
    pub fn part_size(mut self, part_size: PartSize) -> Self {
        self.builder = self.builder.part_size(part_size);
        self
    }

    /// Set the concurrency level this component is allowed to use.
    ///
    /// This sets the maximum number of concurrent in-flight requests.
    /// Default is [ConcurrencySetting::Auto].
    pub fn concurrency(mut self, concurrency: ConcurrencySetting) -> Self {
        self.builder = self.builder.concurrency(concurrency);
        self
    }

    /// Load the default configuration
    ///
    /// If fields have been overridden during builder construction, the override values will be
    /// used. Otherwise, the default values for each field will be provided.
    pub async fn load(self) -> Config {
        let interfaces = vec!["ens5".to_string(), "ens6".to_string()];
        let mut clients = Vec::new();
        for interface in interfaces {
            let http_client = HyperClientBuilder::new()
                .crypto_mode(CryptoMode::AwsLc)
                .build_https(interface);

            let shared_config = aws_config::from_env().http_client(http_client).load().await;
            let s3_client = aws_sdk_s3::Client::new(&shared_config);
            clients.push(s3_client);
        }
        let builder = self.builder.client(clients);
        builder.build()
    }
}
