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
use crate::types::TargetThroughput;
use crate::{http, types::PartSize, Config};

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

    /// Set the target throughput this client should aim for.
    ///
    /// This sets the target throughput of concurrent in-flight requests across _all_ operations.
    /// Default is [TargetThroughput::Auto].
    pub fn target_throughput(mut self, target_throughput: TargetThroughput) -> Self {
        self.builder = self.builder.target_throughput(target_throughput);
        self
    }

    /// Sets the framework metadata for the transfer manager.
    ///
    /// This _optional_ name is used to identify the framework using transfer manager in the user agent that
    /// gets sent along with requests.
    #[doc(hidden)]
    pub fn framework_metadata(mut self, framework_metadata: Option<FrameworkMetadata>) -> Self {
        self.builder = self.builder.framework_metadata(framework_metadata);
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
            frame_work_meta_data: self.builder.framework_metadata.clone(),
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
    use std::borrow::Cow;

    use crate::types::PartSize;
    use aws_config::Region;
    use aws_runtime::user_agent::FrameworkMetadata;
    use aws_sdk_s3::config::Intercept;
    use aws_smithy_runtime::client::http::test_util::capture_request;

    #[tokio::test]
    async fn load_with_interceptor() {
        let config = crate::from_env()
            .part_size(PartSize::Target(8))
            .load()
            .await;
        let sdk_s3_config = config.client().config();
        let tm_interceptor_exists = sdk_s3_config
            .interceptors()
            .any(|item| item.name() == "S3TransferManager");
        assert!(tm_interceptor_exists);
    }

    #[tokio::test]
    async fn load_with_interceptor_and_framework_metadata() {
        let (http_client, captured_request) = capture_request(None);
        let config = crate::from_env()
            .part_size(PartSize::Target(8))
            .framework_metadata(Some(
                FrameworkMetadata::new("some-framework", Some(Cow::Borrowed("1.3"))).unwrap(),
            ))
            .load()
            .await;
        // Inject the captured request to the http client to capture the request made from transfer manager.
        let sdk_s3_config = config
            .client()
            .config()
            .to_builder()
            .http_client(http_client)
            .region(Region::from_static("us-west-2"))
            .with_test_defaults()
            .build();

        let capture_request_config = crate::Config::builder()
            .client(aws_sdk_s3::Client::from_conf(sdk_s3_config))
            .part_size(PartSize::Target(8))
            .build();

        let transfer_manager = crate::Client::new(capture_request_config);

        let mut handle = transfer_manager
            .download()
            .bucket("foo")
            .key("bar")
            .initiate()
            .unwrap();
        // Expect to fail
        let _ = handle.body_mut().next().await;
        // Check the request made contains the expected framework meta data in user agent.
        let expected_req = captured_request.expect_request();
        let user_agent = expected_req.headers().get("x-amz-user-agent").unwrap();
        assert!(user_agent.contains("lib/some-framework/1.3"));
    }
}
