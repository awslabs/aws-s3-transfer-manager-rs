/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_config::{AppName, BehaviorVersion};
use aws_sdk_s3::config::{Intercept, IntoShared};
use aws_smithy_runtime::client::sdk_feature::SmithySdkFeature;

use crate::config::Builder;
use crate::{
    http,
    types::{ConcurrencySetting, PartSize},
    Config,
};

#[derive(Debug)]
struct TransferManagerFeatureInterceptor;

impl Intercept for TransferManagerFeatureInterceptor {
    fn name(&self) -> &'static str {
        "TransferManagerFeature"
    }

    fn read_before_execution(
        &self,
        _ctx: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<'_>,
        cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        cfg.interceptor_state()
            // .store_put(AppName::new("crt-dengket").unwrap())
            // .store_append(AwsSdkFeature::S3Transfer)
            .store_append::<SmithySdkFeature>(SmithySdkFeature::ProtocolRpcV2Cbor)
            .store_append::<SmithySdkFeature>(SmithySdkFeature::Waiter);
        Ok(())
    }
}

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

    /// Sets the name of the app that is using the client.
    ///
    /// This _optional_ name is used to identify the application in the user agent that
    /// gets sent along with requests.
    pub fn app_name(mut self, app_name: Option<AppName>) -> Self {
        self.builder = self.builder.app_name(app_name);
        self
    }

    /// Load the default configuration
    ///
    /// If fields have been overridden during builder construction, the override values will be
    /// used. Otherwise, the default values for each field will be provided.
    pub async fn load(self) -> Config {
        let mut config_loader =
            aws_config::defaults(BehaviorVersion::latest()).http_client(http::default_client());
        if let Some(app_name) = self.builder.app_name.as_ref() {
            config_loader = config_loader.app_name(app_name.clone());
        }
        let shared_config = config_loader.load().await;

        let mut sdk_client_builder = aws_sdk_s3::config::Builder::from(&shared_config);
        sdk_client_builder.push_interceptor(TransferManagerFeatureInterceptor.into_shared());
        let builder = self
            .builder
            .client(aws_sdk_s3::Client::from_conf(sdk_client_builder.build()));
        builder.build()
    }
}
