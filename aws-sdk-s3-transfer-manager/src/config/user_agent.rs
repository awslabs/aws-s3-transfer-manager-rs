/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! User agent attribution for requests the transfer manager issues.

use aws_runtime::sdk_feature::AwsSdkFeature;
use aws_runtime::user_agent::{AdditionalMetadata, ApiMetadata, AwsUserAgent, FrameworkMetadata};
use aws_sdk_s3::config::{AppName, Intercept, IntoShared};
use aws_types::os_shim_internal::Env;
use std::sync::Arc;

use crate::runtime::platform::MachineProfile;
use crate::types::{ConcurrencyMode, MemoryConfig, PartSize};
use crate::Config;

/// This crate's name and version, as the `md/` pair the user agent carries. Neither
/// `aws-sdk-rust/…` nor `api/s3/…` carries the transfer manager's version, and `rust-tm`
/// distinguishes it from the CRT-backed one.
const RUST_TM_VERSION: &str = concat!("rust-tm#", env!("CARGO_PKG_VERSION"));

/// Identifies our interceptor on a client that already carries one.
const INTERCEPTOR_NAME: &str = "S3TransferManager";

/// Identifies the per-operation interceptor on an operation's request.
const OPERATION_INTERCEPTOR_NAME: &str = "S3TransferManagerOperation";

/// The features that describe the client's configuration and environment, carried on every
/// request it issues.
pub(crate) fn client_features(config: &Config, profile: &MachineProfile) -> Vec<AwsSdkFeature> {
    let mut features = Vec::new();
    if matches!(config.part_size(), PartSize::Target(_)) {
        features.push(AwsSdkFeature::S3CustomPartSize);
    }
    if !matches!(config.concurrency(), ConcurrencyMode::Auto) {
        features.push(AwsSdkFeature::S3CustomThroughput);
    }
    if matches!(config.memory(), MemoryConfig::Explicit(_)) {
        features.push(AwsSdkFeature::S3CustomMemoryLimit);
    }
    if profile.instance_type.is_some() {
        features.push(AwsSdkFeature::S3OnEc2);
    }
    features
}

/// Install the transfer manager's user agent attribution on an S3 client builder.
pub(crate) fn install(
    builder: &mut aws_sdk_s3::config::Builder,
    framework_metadata: Option<FrameworkMetadata>,
    features: Vec<AwsSdkFeature>,
) {
    builder.push_interceptor(
        S3TransferManagerInterceptor {
            framework_metadata,
            features,
        }
        .into_shared(),
    );
}

/// The features one operation adds to the requests it issues, on top of the client's.
///
/// Applied to a client derived for that operation, so they never mark the shared client.
#[derive(Clone, Debug, Default)]
pub(crate) struct OperationFeatures(Arc<[AwsSdkFeature]>);

impl OperationFeatures {
    pub(crate) fn new(features: impl IntoIterator<Item = AwsSdkFeature>) -> Self {
        Self(features.into_iter().collect())
    }

    /// A client that adds the features to every request it issues.
    pub(crate) fn apply_to_client(&self, client: &aws_sdk_s3::Client) -> aws_sdk_s3::Client {
        if self.0.is_empty() {
            return client.clone();
        }
        let builder = client.config().to_builder().interceptor(self.clone());
        aws_sdk_s3::Client::from_conf(builder.build())
    }
}

impl Intercept for OperationFeatures {
    fn name(&self) -> &'static str {
        OPERATION_INTERCEPTOR_NAME
    }

    fn read_before_execution(
        &self,
        _ctx: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<'_>,
        cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        for feature in self.0.iter() {
            cfg.interceptor_state().store_append(feature.clone());
        }
        Ok(())
    }
}

/// Install on a client the caller handed over, rebuilding it from its own configuration.
///
/// `Config::to_builder` carries the config layer, runtime components, runtime plugins and
/// behavior version across as a whole, so the rebuilt client differs from the original only by
/// the added interceptor.
///
/// A client that already carries one is returned untouched: the `md/` section is appended to a
/// list, so a second installation emits a second copy. That case is reached whenever a client
/// built by one transfer manager is handed to another, which the composite operations do for
/// every child transfer.
pub(crate) fn install_on_client(
    client: aws_sdk_s3::Client,
    framework_metadata: Option<FrameworkMetadata>,
    features: Vec<AwsSdkFeature>,
) -> aws_sdk_s3::Client {
    if client
        .config()
        .interceptors()
        .any(|interceptor| interceptor.name() == INTERCEPTOR_NAME)
    {
        return client;
    }
    let mut builder = client.config().to_builder();
    install(&mut builder, framework_metadata, features);
    aws_sdk_s3::Client::from_conf(builder.build())
}

#[derive(Debug)]
struct S3TransferManagerInterceptor {
    framework_metadata: Option<FrameworkMetadata>,
    features: Vec<AwsSdkFeature>,
}

impl Intercept for S3TransferManagerInterceptor {
    fn name(&self) -> &'static str {
        INTERCEPTOR_NAME
    }

    /// The SDK's own `UserAgentInterceptor` runs later and yields to any [`AwsUserAgent`]
    /// already in the bag, so this must extend what it finds rather than replace it, and must
    /// copy the customer's [`AppName`] itself when it builds the base — the SDK's
    /// `set_app_name` is below the yield it takes.
    fn read_before_execution(
        &self,
        _ctx: &aws_sdk_s3::config::interceptors::BeforeSerializationInterceptorContextRef<'_>,
        cfg: &mut aws_sdk_s3::config::ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        cfg.interceptor_state()
            .store_append(AwsSdkFeature::S3Transfer);
        for feature in &self.features {
            cfg.interceptor_state().store_append(feature.clone());
        }

        let existing = cfg.load::<AwsUserAgent>().cloned();
        let base = existing.or_else(|| {
            cfg.load::<ApiMetadata>().cloned().map(|api_metadata| {
                let mut ua = AwsUserAgent::new_from_environment(Env::real(), api_metadata);
                if let Some(app_name) = cfg.load::<AppName>().cloned() {
                    ua.set_app_name(app_name);
                }
                ua
            })
        });
        // No `ApiMetadata` to build from: leave it to the SDK, which raises its own error.
        let Some(mut ua) = base else { return Ok(()) };

        // `RUST_TM_VERSION` is a compile-time constant of legal metadata characters, so the
        // error arm is unreachable; it is handled rather than unwrapped because attribution must
        // never fail a transfer.
        if let Ok(metadata) = AdditionalMetadata::new(RUST_TM_VERSION) {
            ua = ua.with_additional_metadata(metadata);
        }

        if let Some(framework_metadata) = self.framework_metadata.clone() {
            ua = ua.with_framework_metadata(framework_metadata);
        }

        cfg.interceptor_state().store_put(ua);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{ConcurrencyMode, MemoryConfig, PartSize, RuntimeMode};
    use aws_config::{BehaviorVersion, Region};
    use aws_runtime::user_agent::{AwsUserAgent, BusinessMetric, FrameworkMetadata};
    use aws_sdk_s3::config::{
        AppName, Credentials, Intercept, IntoShared, SharedCredentialsProvider,
    };
    use aws_smithy_http_client::test_util::capture_request;
    use std::borrow::Cow;

    fn test_credentials() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(Credentials::new(
            "ANOTREAL",
            "notrealrnrELgWzOk3IfjzDKtFBhDby",
            None,
            None,
            "test",
        ))
    }

    /// Drives one download through a transfer manager built from `s3_config(..)` and
    /// returns the `x-amz-user-agent` it sent. That path is the one a caller who brings
    /// their own S3 configuration takes, and installation covers it because
    /// [`install`](super::install) runs in `Client::new`.
    ///
    /// Asserts on `x-amz-user-agent` rather than `User-Agent` because below
    /// `aws-runtime` 1.8.1 the latter renders a short form carrying none of the
    /// attribution sections.
    ///
    /// Note the S3 client is configured *without* `with_test_defaults()`, which would
    /// seed `AwsUserAgent::for_tests()` into the config bag and short-circuit the
    /// assembly under test; credentials and behavior version are set directly instead.
    async fn captured_user_agent(
        runtime_mode: RuntimeMode,
        framework_metadata: Option<FrameworkMetadata>,
        tweak: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
    ) -> String {
        let (http_client, captured) = capture_request(None);
        let builder = aws_sdk_s3::config::Builder::default()
            .http_client(http_client)
            .region(Region::from_static("us-west-2"))
            .credentials_provider(test_credentials())
            .behavior_version(BehaviorVersion::latest());

        // The capture-request client is the transport under test, so the runtime must
        // not substitute its own.
        let s3_config = crate::config::S3ClientConfig::from_sdk_builder(tweak(builder))
            .enable_runtime_http(false);

        let tm = crate::Client::new(
            crate::Config::builder()
                .runtime_mode(runtime_mode)
                .framework_metadata(framework_metadata)
                .s3_config(s3_config)
                .build(),
        );

        let mut handle = tm.download().bucket("foo").key("bar").initiate().unwrap();
        // The captured-request client returns a canned response, so the transfer is
        // expected to fail. Only the request it sent matters here.
        let _ = handle.body_mut().next().await;

        captured
            .expect_request()
            .headers()
            .get("x-amz-user-agent")
            .expect("user agent header is set")
            .to_string()
    }

    /// The app id is the customer's slot, and the SDK's own interceptor is the only place
    /// that copies it out of the config bag. Filling the bag ahead of that interceptor
    /// makes it yield, so the copy has to happen here instead.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn preserves_customer_app_name() {
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, None, |b| {
            b.app_name(AppName::new("my-app").unwrap())
        })
        .await;
        assert!(ua.contains("app/my-app"), "app id missing from {ua:?}");
    }

    /// Which transfer manager issued the request is otherwise unanswerable from the
    /// header: `aws-sdk-rust/…` is the SDK core version and `api/s3/…` the S3 client's.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_transfer_manager_version() {
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, None, |b| b).await;
        let expected = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(ua.contains(expected), "{expected:?} missing from {ua:?}");
    }

    /// The `md/` section is appended to a list, so a second copy of the interceptor adds a
    /// second copy rather than replacing it. This covers the path we build ourselves;
    /// [`an_already_instrumented_client_is_not_attributed_twice`] covers the other one.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_our_marker_once() {
        let ua = captured_user_agent(RuntimeMode::Managed, None, |b| b).await;
        assert_eq!(ua.matches("md/rust-tm#").count(), 1, "{ua:?}");
    }

    /// A framework adding its own attribution must not cost ours, so both belong in the same
    /// assertion. This is also the `s3_config(..)` path.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn framework_metadata_joins_our_attribution() {
        let framework =
            FrameworkMetadata::new("some-framework", Some(Cow::Borrowed("1.3"))).unwrap();
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, Some(framework), |b| b).await;
        assert!(
            ua.contains("lib/some-framework/1.3"),
            "framework metadata missing from {ua:?}"
        );
        let ours = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(
            ua.contains(ours),
            "our attribution was displaced by the framework's: {ua:?}"
        );
    }

    /// A user agent already in the config bag is extended, not replaced — otherwise a
    /// caller's own interceptor (or a test pinning the header) is silently discarded.
    /// `with_test_defaults()` seeds `AwsUserAgent::for_tests()`, which is that case.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn preserves_user_agent_already_in_config_bag() {
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, None, |b| {
            b.with_test_defaults()
        })
        .await;
        assert!(
            ua.contains("api/test-service/0.123"),
            "seeded user agent was replaced: {ua:?}"
        );
    }

    /// The business metric is what attributes a request to a transfer manager at all, and
    /// it reaches the header only if this interceptor ran — so this covers installation as
    /// much as the metric itself.
    ///
    /// `G` is `BusinessMetric::S3Transfer`'s id, which upstream derives from the variant's
    /// position in its enum. So this also fails if `aws-runtime` inserts a variant ahead of
    /// it, in which case the id moved and the expectation follows it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_s3_transfer_business_metric() {
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, None, |b| b).await;
        let metrics = ua
            .split_whitespace()
            .find_map(|section| section.strip_prefix("m/"))
            .expect("user agent carries a metrics section");
        assert!(
            metrics.split(',').any(|id| id == "G"),
            "S3Transfer metric missing from {metrics:?} in {ua:?}"
        );
    }

    /// Builds a client the way a caller who brings their own does, over a capture transport.
    fn provided_client(
        http_client: impl aws_sdk_s3::config::HttpClient + 'static,
    ) -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::default()
                .http_client(http_client)
                .region(Region::from_static("us-west-2"))
                .credentials_provider(test_credentials())
                .behavior_version(BehaviorVersion::latest())
                .build(),
        )
    }

    /// Drives one download through a transfer manager built on a client the caller supplied.
    async fn download_through(client: aws_sdk_s3::Client) {
        let tm = crate::Client::new(
            crate::Config::builder()
                .runtime_mode(RuntimeMode::MultiThreadTokio)
                .part_size(PartSize::Target(8 << 20))
                .sdk_client(client)
                .build(),
        );
        let mut handle = tm.download().bucket("foo").key("bar").initiate().unwrap();
        let _ = handle.body_mut().next().await;
    }

    /// A caller who hands over a finished client gets attribution too: the client is rebuilt
    /// from its own configuration with our interceptor added, since one cannot be pushed onto a
    /// client that is already built.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn provided_client_gets_attribution() {
        let (http_client, captured) = capture_request(None);
        download_through(provided_client(http_client)).await;
        let ua = captured
            .expect_request()
            .headers()
            .get("x-amz-user-agent")
            .expect("user agent header is set")
            .to_string();

        let ours = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(ua.contains(ours), "{ours:?} missing from {ua:?}");
        let metrics = ua
            .split_whitespace()
            .find_map(|section| section.strip_prefix("m/"))
            .expect("user agent carries a metrics section");
        for metric in [BusinessMetric::S3Transfer, BusinessMetric::S3CustomPartSize] {
            let id = id_of(metric);
            assert!(
                metrics.split(',').any(|m| m == id),
                "{id} missing from {metrics:?} in {ua:?}"
            );
        }
    }

    /// Rebuilding a caller's client must not cost them anything they configured on it, so this
    /// asserts one property from each place a setting can live: the config layer (`app_name`,
    /// `force_path_style`), the endpoint resolver (`endpoint_url`), the identity and auth
    /// resolvers (the credentials and region the request is signed with), a runtime component
    /// they registered (their own interceptor), and the transport — the captured request only
    /// exists at all if their `http_client` survived.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn rebuilding_a_provided_client_keeps_its_configuration() {
        let (http_client, captured) = capture_request(None);
        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::default()
                .http_client(http_client)
                .region(Region::from_static("eu-central-1"))
                .credentials_provider(test_credentials())
                .behavior_version(BehaviorVersion::latest())
                .app_name(AppName::new("callers-app").unwrap())
                .endpoint_url("https://s3.example.internal")
                .force_path_style(true)
                .interceptor(MarkHeader)
                .build(),
        );
        download_through(client).await;
        let request = captured.expect_request();

        // Endpoint resolver and the S3-specific addressing flag.
        assert_eq!(
            request.uri(),
            "https://s3.example.internal/foo/bar?x-id=GetObject",
            "endpoint or path style lost in the rebuild"
        );
        // Auth: the credential scope names their region, and the credential their access key.
        let authorization = request
            .headers()
            .get("authorization")
            .expect("request is signed");
        assert!(
            authorization.contains("ANOTREAL/") && authorization.contains("eu-central-1/s3/"),
            "credentials or region lost in the rebuild: {authorization:?}"
        );
        // A runtime component the caller registered.
        assert_eq!(
            request.headers().get("x-callers-interceptor"),
            Some("ran"),
            "the caller's interceptor was lost in the rebuild"
        );

        let ua = request
            .headers()
            .get("x-amz-user-agent")
            .expect("user agent header is set")
            .to_string();
        assert!(ua.contains("app/callers-app"), "app name lost: {ua:?}");
        let ours = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(ua.contains(ours), "{ours:?} missing from {ua:?}");
    }

    /// Stands in for whatever a caller registers on their own client: it proves the rebuild kept
    /// their runtime components by leaving a mark on the request.
    #[derive(Debug)]
    struct MarkHeader;

    impl Intercept for MarkHeader {
        fn name(&self) -> &'static str {
            "MarkHeader"
        }

        fn modify_before_signing(
            &self,
            ctx: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
            _components: &aws_sdk_s3::config::RuntimeComponents,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            ctx.request_mut()
                .headers_mut()
                .insert("x-callers-interceptor", "ran");
            Ok(())
        }
    }

    /// Handing a transfer manager's own client to a second one must not attribute it twice —
    /// the `md/` section is appended to a list. This is the shape the composite operations
    /// create: `download_objects` gives `handle.s3_client` to each child transfer.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_already_instrumented_client_is_not_attributed_twice() {
        let (http_client, captured) = capture_request(None);
        let first = crate::Client::new(
            crate::Config::builder()
                .runtime_mode(RuntimeMode::MultiThreadTokio)
                .sdk_client(provided_client(http_client))
                .build(),
        );
        // The clone a child transfer would be handed.
        download_through(first.handle.s3_client.clone()).await;
        let ua = captured
            .expect_request()
            .headers()
            .get("x-amz-user-agent")
            .expect("user agent header is set")
            .to_string();

        assert_eq!(ua.matches("md/rust-tm#").count(), 1, "{ua:?}");
    }

    /// Writes both user agent headers verbatim before signing, which is where a caller
    /// who needs to own the whole string has to do it: `x-amz-user-agent` is signed, and
    /// the SDK *appends* to both headers, so owning them means replacing the values.
    #[derive(Debug)]
    struct OverrideUserAgent;

    impl Intercept for OverrideUserAgent {
        fn name(&self) -> &'static str {
            "OverrideUserAgent"
        }

        fn modify_before_signing(
            &self,
            ctx: &mut aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextMut<'_>,
            _components: &aws_sdk_s3::config::RuntimeComponents,
            _cfg: &mut aws_sdk_s3::config::ConfigBag,
        ) -> Result<(), aws_sdk_s3::error::BoxError> {
            let headers = ctx.request_mut().headers_mut();
            headers.insert("user-agent", OWNED_USER_AGENT);
            headers.insert("x-amz-user-agent", OWNED_USER_AGENT);
            Ok(())
        }
    }

    /// A stand-in for whatever the caller writes, not a conformant user agent — it omits the
    /// `ua/`, `os/` and `lang/` sections the grammar requires. What is under test is that the
    /// value survives to the wire verbatim.
    const OWNED_USER_AGENT: &str = "aws-cli/2.36.23 md/command#s3.cp";

    /// A caller that needs a leading product token cannot get one through the config
    /// bag — everything stored there still renders through `aws_ua_header()`, whose
    /// leading `aws-sdk-rust/…` comes from a private field. Owning the headers is the
    /// remaining option, and it needs nothing from us: our attribution composes into the
    /// bag and never touches headers, so it cannot race a caller who does.
    ///
    /// This does rest on interceptor ordering — both hooks run at
    /// `modify_before_signing`, and a config-pushed interceptor runs after the ones the
    /// client's default runtime plugin registers. That holds but is not contractual, so
    /// this test is the tripwire if it changes.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_caller_can_own_both_headers() {
        let ua = captured_user_agent(RuntimeMode::MultiThreadTokio, None, |mut b| {
            b.push_interceptor(OverrideUserAgent.into_shared());
            b
        })
        .await;
        assert_eq!(ua, OWNED_USER_AGENT);
    }

    type Seen = std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<String>)>>>;

    /// The id `aws-runtime` encodes `metric` as, so these tests do not repeat its table.
    fn id_of(metric: BusinessMetric) -> String {
        let header = AwsUserAgent::for_tests()
            .with_business_metric(metric)
            .aws_ua_header();
        metric_ids(&header).remove(0)
    }

    /// The business metric ids in a user agent.
    fn metric_ids(ua: &str) -> Vec<String> {
        ua.split_whitespace()
            .find_map(|section| section.strip_prefix("m/"))
            .map(|metrics| metrics.split(',').map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// A transport that records each request's URI and metric ids, answers listings with one
    /// object and fails everything else, which is enough for a request to be issued and seen.
    fn recording_client() -> (aws_smithy_runtime_api::client::http::SharedHttpClient, Seen) {
        const LISTING: &str = "<ListBucketResult><Name>foo</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>a.txt</Key><Size>1</Size><LastModified>2020-01-01T00:00:00.000Z</LastModified></Contents></ListBucketResult>";
        let seen = Seen::default();
        let recorded = seen.clone();
        let client = aws_smithy_http_client::test_util::infallible_client_fn(move |req| {
            let uri = req.uri().to_string();
            let ua = req.headers()["x-amz-user-agent"].to_str().unwrap();
            recorded.lock().unwrap().push((uri.clone(), metric_ids(ua)));
            let (status, body) = if uri.contains("list-type=2") {
                (200, LISTING)
            } else {
                (403, "<Error><Code>AccessDenied</Code></Error>")
            };
            http::Response::builder().status(status).body(body).unwrap()
        });
        (client, seen)
    }

    fn tm_over(
        http_client: aws_smithy_runtime_api::client::http::SharedHttpClient,
        tweak: impl FnOnce(crate::config::Builder) -> crate::config::Builder,
    ) -> crate::Client {
        let shared = aws_types::SdkConfig::builder()
            .http_client(http_client)
            .region(Region::from_static("us-west-2"))
            .credentials_provider(test_credentials())
            .behavior_version(BehaviorVersion::latest())
            .build();
        let s3_config = crate::config::S3ClientConfig::new(&shared).enable_runtime_http(false);
        crate::Client::new(
            tweak(
                crate::Config::builder()
                    .runtime_mode(RuntimeMode::MultiThreadTokio)
                    .machine_profile(Some(crate::runtime::platform::MachineProfile {
                        instance_type: None,
                        vcpus: 4,
                        ram_bytes: None,
                    }))
                    .s3_config(s3_config),
            )
            .build(),
        )
    }

    /// The metric ids of the only request a streaming download issues.
    async fn download_ids(
        tweak: impl FnOnce(crate::config::Builder) -> crate::config::Builder,
    ) -> Vec<String> {
        let (http_client, seen) = recording_client();
        let tm = tm_over(http_client, tweak);
        let mut handle = tm.download().bucket("foo").key("bar").initiate().unwrap();
        let _ = handle.body_mut().next().await;
        let seen = seen.lock().unwrap();
        seen[0].1.clone()
    }

    fn has(ids: &[String], metric: BusinessMetric) -> bool {
        ids.contains(&id_of(metric))
    }

    const ATTRIBUTION: [BusinessMetric; 7] = [
        BusinessMetric::S3CustomPartSize,
        BusinessMetric::S3CustomThroughput,
        BusinessMetric::S3CustomMemoryLimit,
        BusinessMetric::S3OnEc2,
        BusinessMetric::S3FilePath,
        BusinessMetric::S3TransferUploadDirectory,
        BusinessMetric::S3TransferDownloadDirectory,
    ];

    /// The attribution ids a transfer manager's requests carry for its configuration, and for
    /// the operation that issued them.
    async fn attribution_ids(
        tweak: impl FnOnce(crate::config::Builder) -> crate::config::Builder,
    ) -> Vec<String> {
        let mut ids = download_ids(tweak).await;
        let ours = ATTRIBUTION.map(id_of);
        ids.retain(|id| ours.contains(id));
        ids
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn configuration_metrics_follow_the_configuration() {
        type Tweak = Box<dyn FnOnce(crate::config::Builder) -> crate::config::Builder>;
        let pool = crate::runtime::buffer_pool::BufferPool::from_capacity(
            crate::types::MemoryBudgetConfig::Limit(256 * 1024 * 1024),
            None,
            Default::default(),
        )
        .unwrap();
        let ec2 = crate::runtime::platform::MachineProfile {
            instance_type: Some("m6i.large".to_owned()),
            vcpus: 2,
            ram_bytes: None,
        };
        let cases: Vec<(&str, Tweak, &[BusinessMetric])> = vec![
            ("defaults", Box::new(|b| b), &[]),
            (
                "part size",
                Box::new(|b| b.part_size(PartSize::Target(8 << 20))),
                &[BusinessMetric::S3CustomPartSize],
            ),
            (
                "concurrency",
                Box::new(|b| b.concurrency(ConcurrencyMode::Explicit(4))),
                &[BusinessMetric::S3CustomThroughput],
            ),
            (
                "memory",
                Box::new(|b| b.memory(MemoryConfig::Explicit(pool))),
                &[BusinessMetric::S3CustomMemoryLimit],
            ),
            ("ec2", Box::new(|b| b.machine_profile(Some(ec2))), &[BusinessMetric::S3OnEc2]),
        ];
        for (name, tweak, expected) in cases {
            let expected: Vec<String> = expected.iter().cloned().map(id_of).collect();
            assert_eq!(expected, attribution_ids(tweak).await, "{name}");
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn upload_objects_emits_upload_directory_and_file_path() {
        let (http_client, seen) = recording_client();
        let tm = tm_over(http_client, |b| b);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();
        let _ = tm
            .upload_objects()
            .bucket("foo")
            .source(dir.path())
            .initiate()
            .unwrap()
            .join()
            .await;
        let children = seen.lock().unwrap().len();
        let body = crate::io::InputStream::from(bytes::Bytes::from_static(b"x"));
        let _ = tm
            .upload()
            .bucket("foo")
            .key("k")
            .body(body)
            .initiate()
            .unwrap()
            .join()
            .await;

        let seen = seen.lock().unwrap();
        assert!(children > 0);
        for (uri, ids) in &seen[..children] {
            assert!(
                has(ids, BusinessMetric::S3TransferUploadDirectory) && has(ids, BusinessMetric::S3FilePath) && !has(ids, BusinessMetric::S3TransferDownloadDirectory),
                "{uri}: {ids:?}"
            );
        }
        // The markers belong to the operation: a later plain upload from the same client has none.
        let ids = &seen[children].1;
        assert!(
            has(ids, BusinessMetric::S3Transfer) && !has(ids, BusinessMetric::S3TransferUploadDirectory) && !has(ids, BusinessMetric::S3FilePath),
            "{ids:?}"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn download_objects_emits_download_directory_and_file_path_on_objects() {
        let (http_client, seen) = recording_client();
        let tm = tm_over(http_client, |b| b);
        let dir = tempfile::tempdir().unwrap();
        let _ = tm
            .download_objects()
            .bucket("foo")
            .destination(dir.path())
            .initiate()
            .unwrap()
            .join()
            .await;
        let children = seen.lock().unwrap().len();
        let mut stream = tm.download().bucket("foo").key("bar").initiate().unwrap();
        let _ = stream.body_mut().next().await;

        let seen = seen.lock().unwrap();
        let (listings, objects): (Vec<_>, Vec<_>) = seen[..children]
            .iter()
            .partition(|(uri, _)| uri.contains("list-type=2"));
        assert!(!listings.is_empty() && !objects.is_empty(), "{seen:?}");
        for (uri, ids) in listings {
            assert!(
                has(ids, BusinessMetric::S3TransferDownloadDirectory) && !has(ids, BusinessMetric::S3FilePath) && !has(ids, BusinessMetric::S3TransferUploadDirectory),
                "{uri}: {ids:?}"
            );
        }
        for (uri, ids) in objects {
            assert!(
                has(ids, BusinessMetric::S3TransferDownloadDirectory) && has(ids, BusinessMetric::S3FilePath) && !has(ids, BusinessMetric::S3TransferUploadDirectory),
                "{uri}: {ids:?}"
            );
        }
        // The markers belong to the operation: a later plain download from the same client has none.
        let ids = &seen[children].1;
        assert!(
            has(ids, BusinessMetric::S3Transfer) && !has(ids, BusinessMetric::S3TransferDownloadDirectory) && !has(ids, BusinessMetric::S3FilePath),
            "{ids:?}"
        );
    }
}
