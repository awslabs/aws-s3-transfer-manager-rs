/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! User agent attribution for requests the transfer manager issues.

use crate::types::RuntimeMode;
use aws_runtime::sdk_feature::AwsSdkFeature;
use aws_runtime::user_agent::{AdditionalMetadata, ApiMetadata, AwsUserAgent, FrameworkMetadata};
use aws_sdk_s3::config::{AppName, Intercept, IntoShared};
use aws_types::os_shim_internal::Env;

/// This crate's identity in the user agent. Neither `aws-sdk-rust/…` nor `api/s3/…` carries
/// the transfer manager's version, and `rust-tm` distinguishes it from the CRT-backed one.
const TM_METADATA: &str = concat!("rust-tm#", env!("CARGO_PKG_VERSION"));

/// Which runtime implementation is in use, not how it is tuned.
fn runtime_mode_metadata(runtime_mode: &RuntimeMode) -> &'static str {
    match runtime_mode {
        RuntimeMode::Managed => "rust-tm-rt#managed",
        RuntimeMode::MultiThreadTokio => "rust-tm-rt#tokio-mt",
    }
}

/// Adds an `md/` section, dropping it if it does not validate — attribution must never fail
/// a transfer.
fn with_metadata(ua: AwsUserAgent, value: &'static str) -> AwsUserAgent {
    match AdditionalMetadata::new(value) {
        Ok(metadata) => ua.with_additional_metadata(metadata),
        Err(_) => ua,
    }
}

/// Install the transfer manager's user agent attribution on an S3 client builder.
///
/// Called once per client from [`Client::new`](crate::Client::new), the one place both
/// construction paths arrive as a builder we own, so each section appears exactly once.
pub(crate) fn install(
    builder: &mut aws_sdk_s3::config::Builder,
    framework_metadata: Option<FrameworkMetadata>,
    runtime_mode: RuntimeMode,
) {
    builder.push_interceptor(
        S3TransferManagerInterceptor {
            framework_metadata,
            runtime_mode,
        }
        .into_shared(),
    );
}

#[derive(Debug)]
struct S3TransferManagerInterceptor {
    framework_metadata: Option<FrameworkMetadata>,
    runtime_mode: RuntimeMode,
}

impl Intercept for S3TransferManagerInterceptor {
    fn name(&self) -> &'static str {
        "S3TransferManager"
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

        ua = with_metadata(ua, TM_METADATA);
        ua = with_metadata(ua, runtime_mode_metadata(&self.runtime_mode));

        if let Some(framework_metadata) = self.framework_metadata.clone() {
            ua = ua.with_framework_metadata(framework_metadata);
        }

        cfg.interceptor_state().store_put(ua);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::types::RuntimeMode;
    use aws_config::{BehaviorVersion, Region};
    use aws_runtime::user_agent::FrameworkMetadata;
    use aws_sdk_s3::config::{
        AppName, Credentials, Intercept, IntoShared, SharedCredentialsProvider,
    };
    use aws_smithy_runtime::client::http::test_util::capture_request;
    use std::borrow::Cow;

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
    fn test_credentials() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(Credentials::new(
            "ANOTREAL",
            "notrealrnrELgWzOk3IfjzDKtFBhDby",
            None,
            None,
            "test",
        ))
    }

    async fn captured_user_agent(
        tm_tweak: impl FnOnce(crate::config::Builder) -> crate::config::Builder,
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
        let s3_config =
            crate::config::S3ClientConfig::new(tweak(builder)).enable_runtime_http(false);

        let tm = crate::Client::new(
            tm_tweak(crate::Config::builder())
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
        let ua = captured_user_agent(
            |c| c.runtime_mode(RuntimeMode::MultiThreadTokio),
            |b| b.app_name(AppName::new("my-app").unwrap()),
        )
        .await;
        assert!(ua.contains("app/my-app"), "app id missing from {ua:?}");
    }

    /// Which transfer manager issued the request is otherwise unanswerable from the
    /// header: `aws-sdk-rust/…` is the SDK core version and `api/s3/…` the S3 client's.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_transfer_manager_version() {
        let ua =
            captured_user_agent(|c| c.runtime_mode(RuntimeMode::MultiThreadTokio), |b| b).await;
        let expected = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(ua.contains(expected), "{expected:?} missing from {ua:?}");
    }

    /// Which runtime the manager is on decides whether it owns its threads or borrows the
    /// caller's, and `MultiThreadTokio` is slower by design — so a caller reporting poor
    /// throughput is answered by this section before anything else. `Managed` is the default.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_runtime_mode() {
        let ua = captured_user_agent(|c| c.runtime_mode(RuntimeMode::Managed), |b| b).await;
        assert!(
            ua.contains("md/rust-tm-rt#managed"),
            "runtime mode missing from {ua:?}"
        );
    }

    /// The other half of [`emits_runtime_mode`]: the section has to distinguish the two
    /// modes, not merely be present.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_runtime_mode_for_tokio_runtime() {
        let ua =
            captured_user_agent(|c| c.runtime_mode(RuntimeMode::MultiThreadTokio), |b| b).await;
        assert!(
            ua.contains("md/rust-tm-rt#tokio-mt"),
            "runtime mode missing or wrong in {ua:?}"
        );
    }

    /// Both `md/` sections are appended to a list, so a second copy of the interceptor adds a
    /// second copy of each rather than replacing it. A single installation site is what keeps
    /// that from happening; this is the tripwire if one is ever added.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn emits_each_marker_once() {
        let ua = captured_user_agent(|c| c.runtime_mode(RuntimeMode::Managed), |b| b).await;
        assert_eq!(ua.matches("md/rust-tm#").count(), 1, "{ua:?}");
        assert_eq!(ua.matches("md/rust-tm-rt#").count(), 1, "{ua:?}");
    }

    /// A framework adding its own attribution must not cost ours, so both belong in the same
    /// assertion. This is also the `s3_config(..)` path.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn framework_metadata_joins_our_attribution() {
        let framework =
            FrameworkMetadata::new("some-framework", Some(Cow::Borrowed("1.3"))).unwrap();
        let ua = captured_user_agent(
            |c| {
                c.runtime_mode(RuntimeMode::MultiThreadTokio)
                    .framework_metadata(framework)
            },
            |b| b,
        )
        .await;
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
        let ua = captured_user_agent(
            |c| c.runtime_mode(RuntimeMode::MultiThreadTokio),
            |b| b.with_test_defaults(),
        )
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
        let ua =
            captured_user_agent(|c| c.runtime_mode(RuntimeMode::MultiThreadTokio), |b| b).await;
        let metrics = ua
            .split_whitespace()
            .find_map(|section| section.strip_prefix("m/"))
            .expect("user agent carries a metrics section");
        assert!(
            metrics.split(',').any(|id| id == "G"),
            "S3Transfer metric missing from {metrics:?} in {ua:?}"
        );
    }

    /// A client the caller hands over is used as built, and interceptors cannot be pushed onto
    /// a finished client — so this path carries none of our attribution. Asserted rather than
    /// left implicit, because the sections are absent by construction and a reader has no way
    /// to tell that from an oversight.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn provided_client_carries_no_attribution() {
        let (http_client, captured) = capture_request(None);
        let s3_client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::default()
                .http_client(http_client)
                .region(Region::from_static("us-west-2"))
                .credentials_provider(test_credentials())
                .behavior_version(BehaviorVersion::latest())
                .build(),
        );

        let tm = crate::Client::new(
            crate::Config::builder()
                .runtime_mode(RuntimeMode::MultiThreadTokio)
                .client(s3_client)
                .build(),
        );
        let mut handle = tm.download().bucket("foo").key("bar").initiate().unwrap();
        let _ = handle.body_mut().next().await;

        let ua = captured
            .expect_request()
            .headers()
            .get("x-amz-user-agent")
            .expect("user agent header is set")
            .to_string();
        // Guards the assertions below: they are absences, and would pass just as well on a
        // header that was never assembled.
        assert!(ua.contains("api/s3/"), "not a real user agent: {ua:?}");
        assert!(
            !ua.contains("md/rust-tm"),
            "provided client unexpectedly carries our attribution: {ua:?}"
        );
        let metrics = ua
            .split_whitespace()
            .find_map(|section| section.strip_prefix("m/"))
            .expect("user agent carries a metrics section");
        assert!(
            !metrics.split(',').any(|id| id == "G"),
            "provided client unexpectedly carries the S3Transfer metric: {ua:?}"
        );
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

    /// Spelled the way the shared grammar allows — `sdk-metadata = "aws-sdk-" sdk-name
    /// "/" version`, and `sdk-name` already enumerates `cli` — so this fixture is not a
    /// non-conformant string for anyone to copy.
    const OWNED_USER_AGENT: &str = "aws-sdk-cli/2.36.23 md/command#s3.cp";

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
        let ua = captured_user_agent(
            |c| c.runtime_mode(RuntimeMode::MultiThreadTokio),
            |mut b| {
                b.push_interceptor(OverrideUserAgent.into_shared());
                b
            },
        )
        .await;
        assert_eq!(ua, OWNED_USER_AGENT);
    }
}
