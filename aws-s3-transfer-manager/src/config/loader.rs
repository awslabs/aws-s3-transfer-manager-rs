/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_config::BehaviorVersion;
use aws_runtime::sdk_feature::AwsSdkFeature;
use aws_runtime::user_agent::{ApiMetadata, AwsUserAgent, FrameworkMetadata};
use aws_sdk_s3::config::{Intercept, IntoShared};
use aws_types::os_shim_internal::Env;

use crate::config::Builder;
use crate::{
    http,
    types::{ConcurrencySetting, PartSize},
    Config,
};

#[derive(Debug)]
struct S3TransferManagerInterceptor {
    frame_work_meta_data: Option<FrameworkMetadata>,
}

impl Intercept for S3TransferManagerInterceptor {
    fn name(&self) -> &'static str {
        "S3TransferManager"
    }

    fn read_before_execution(
        &self,
        _ctx: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<'_>,
        cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        // Assume the interceptor only be added to the client constructed by the loader.
        // In this case, there should not be any user agent was sent before this interceptor starts.
        // Create our own user agent with S3Transfer feature and user passed-in framework_meta_data if any.
        cfg.interceptor_state()
            .store_append(AwsSdkFeature::S3Transfer);
        let api_metadata = cfg.load::<ApiMetadata>().unwrap();
        // TODO: maybe APP Name someday
        let mut ua = AwsUserAgent::new_from_environment(Env::real(), api_metadata.clone());
        if let Some(framework_metadata) = self.frame_work_meta_data.clone() {
            ua = ua.with_framework_metadata(framework_metadata);
        }

        cfg.interceptor_state().store_put(ua);

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
    pub fn frame_metadata(mut self, frame_metadata: Option<FrameworkMetadata>) -> Self {
        self.builder = self.builder.frame_metadata(frame_metadata);
        self
    }

    /// Load the default configuration
    ///
    /// If fields have been overridden during builder construction, the override values will be
    /// used. Otherwise, the default values for each field will be provided.
    pub async fn load(self) -> Config {
        let shared_config = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http::default_client())
            .load()
            .await;

        let mut sdk_client_builder = aws_sdk_s3::config::Builder::from(&shared_config);

        let interceptor = S3TransferManagerInterceptor {
            frame_work_meta_data: self.builder.frame_metadata.clone(),
        };
        sdk_client_builder.push_interceptor(S3TransferManagerInterceptor::into_shared(interceptor));
        let builder = self
            .builder
            .client(aws_sdk_s3::Client::from_conf(sdk_client_builder.build()));
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{ConcurrencySetting, PartSize};
    use aws_sdk_s3::config::Intercept;

    #[tokio::test]
    async fn load_with_frame_metadata_and_interceptor() {
        let config = crate::from_env()
            .concurrency(ConcurrencySetting::Explicit(123))
            .part_size(PartSize::Target(8))
            .load()
            .await;
        let sdk_s3_config = config.client().config();
        let tm_interceptor_exists = sdk_s3_config
            .interceptors()
            .any(|item| item.name() == "S3TransferManager");
        assert!(tm_interceptor_exists);
    }

    /* TODO: test the framework metadata added correctly */
}
