/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_config::BehaviorVersion;
use aws_runtime::user_agent::FrameworkMetadata;

use crate::config::{Builder, Config};
use crate::types::{ConcurrencyMode, MemoryBudgetConfig, PartSize, RuntimeMode};

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
    pub fn part_size(mut self, part_size: PartSize) -> Self {
        self.builder = self.builder.part_size(part_size);
        self
    }

    /// Set the concurrency mode this client should use.
    ///
    /// This sets the mode used for concurrent in-flight requests across _all_ operations.
    /// Default is [ConcurrencyMode::Auto].
    pub fn concurrency(mut self, mode: ConcurrencyMode) -> Self {
        self.builder = self.builder.concurrency(mode);
        self
    }

    /// Set the execution runtime this client should use.
    ///
    /// Default is [RuntimeMode::Managed].
    pub fn runtime_mode(mut self, mode: RuntimeMode) -> Self {
        self.builder = self.builder.runtime_mode(mode);
        self
    }

    /// Set the memory budget: an upper bound on memory used for in-flight and
    /// buffered transfer data. At the limit transfers backpressure rather than
    /// fail. Default is [`MemoryBudgetConfig::Auto`].
    pub fn memory_budget(mut self, budget: MemoryBudgetConfig) -> Self {
        self.builder = self.builder.memory_budget(budget);
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

    /// Set a dial9 handle for runtime tracing.
    #[cfg(feature = "dial9")]
    pub fn dial9_handle(mut self, handle: dial9::Dial9Handle) -> Self {
        self.builder = self.builder.dial9_handle(handle);
        self
    }

    /// Load the default configuration
    ///
    /// If fields have been overridden during builder construction, the override values will be
    /// used. Otherwise, the default values for each field will be provided.
    pub async fn load(self) -> Config {
        let shared_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        // Detect machine facts here, on the async load path, so client
        // construction stays free of blocking DMI reads and network IMDS calls.
        let profile = detect_machine_profile().await;

        let sdk_client_builder = aws_sdk_s3::config::Builder::from(&shared_config);

        let builder = self
            .builder
            .machine_profile(Some(profile))
            .s3_config(crate::config::S3ClientConfig::new(sdk_client_builder));
        builder.build()
    }
}

/// Detect the machine profile for auto-sizing: instance type via local DMI, then
/// IMDS *only* when DMI is inconclusive, plus the usable vCPU count. Detection
/// failure yields `None` for the instance type and consumers fall back to
/// vCPU-scaled defaults.
///
/// The IMDS fallback is gated on DMI being inconclusive. On **Linux**, DMI
/// answers "is this EC2" locally: a readable `NotEc2` reading skips IMDS (no
/// network call on a Linux laptop, on-prem host, or another cloud); only an
/// `Unknown` (a container without DMI) probes IMDS. On **non-Linux** there is no
/// DMI path, so detection is always `Unknown` and IMDS is always attempted —
/// this is deliberate (EC2 does run Windows/macOS, and IMDS is the only detection
/// path there), the cost being that a non-EC2 non-Linux host makes one IMDS probe
/// per client build. In all cases the probe honors `AWS_EC2_METADATA_DISABLED`
/// and is bounded to a single short attempt (~500ms), so a miss is cheap.
async fn detect_machine_profile() -> crate::runtime::platform::MachineProfile {
    use crate::runtime::platform::{self, DmiDetection};
    // `detect_instance_type_dmi` reads DMI attributes with synchronous `std::fs`,
    // which runs inline on the async worker rather than via `spawn_blocking`. This
    // is intentional: the reads target `/sys/.../dmi/id/*`, which are RAM-backed
    // sysfs pseudo-files served by the kernel from the SMBIOS table parsed at boot
    // (no disk I/O), and this runs once at client construction. The `spawn_blocking`
    // hop would cost more than the reads it guards. (Ref: sysfs(5); the crate's
    // hot-path fs is handled separately by the blocking-fs work.)
    let instance_type = match platform::detect_instance_type_dmi() {
        DmiDetection::Instance(ty) => Some(ty),
        DmiDetection::NotEc2 => None,
        DmiDetection::Unknown => imds_instance_type().await,
    };
    platform::MachineProfile {
        instance_type,
        vcpus: platform::local_vcpus(),
        ram_bytes: platform::available_ram(),
    }
}

/// Whether the user disabled EC2 IMDS via `AWS_EC2_METADATA_DISABLED=true`. The
/// raw `imds::Client` does not honor this itself (only the region/credentials
/// providers do), so we check it before constructing one.
fn imds_disabled() -> bool {
    imds_disabled_value(std::env::var("AWS_EC2_METADATA_DISABLED").ok().as_deref())
}

/// Pure predicate for the disable flag, split out so it is testable without
/// mutating the process environment (which would race concurrent tests).
///
/// Lenient: accepts the common truthy spellings (`true`, `1`, `yes`, `on`,
/// case-insensitive). Anything else — including unset, `false`, `0`, or an
/// unrecognized value — leaves IMDS enabled.
fn imds_disabled_value(raw: Option<&str>) -> bool {
    matches!(
        raw.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true" | "1" | "yes" | "on")
    )
}

/// Query the EC2 instance type from IMDS. `None` on any failure (IMDS disabled,
/// not on EC2, hop-limited, timeout). IMDS returns only the instance-type string
/// — no bandwidth data — so it feeds the family table like the DMI path.
///
/// Bounded to a single attempt with short connect/read timeouts: this only runs
/// when DMI was inconclusive, and a non-EC2 host must not pay the library default
/// (4 attempts, 1s connect each). On EC2 the endpoint is link-local and answers
/// in microseconds, so one short attempt is ample.
async fn imds_instance_type() -> Option<String> {
    if imds_disabled() {
        tracing::debug!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            "IMDS disabled via AWS_EC2_METADATA_DISABLED; using vCPU-scaled fallback",
        );
        return None;
    }
    let client = aws_config::imds::Client::builder()
        .max_attempts(1)
        .connect_timeout(std::time::Duration::from_millis(500))
        .read_timeout(std::time::Duration::from_millis(500))
        .build();
    match client.get("/latest/meta-data/instance-type").await {
        Ok(ty) => {
            let ty = ty.as_ref().trim().to_string();
            (!ty.is_empty()).then_some(ty)
        }
        Err(err) => {
            tracing::debug!(
                target: crate::telemetry::TARGET_CONCURRENCY,
                error = %err,
                "IMDS instance-type lookup failed; using vCPU-scaled fallback",
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::PartSize;
    use aws_config::Region;
    use aws_sdk_s3::config::Intercept;
    use aws_smithy_runtime::client::http::test_util::capture_request;

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn load_with_interceptor() {
        let config = crate::from_env()
            .part_size(PartSize::Target(8))
            .load()
            .await;
        let tm_client = crate::Client::new(config);
        let sdk_s3_config = tm_client.handle.s3_client.config();
        let tm_interceptor_exists = sdk_s3_config
            .interceptors()
            .any(|item| item.name() == "S3TransferManager");
        assert!(tm_interceptor_exists);
    }

    // `load()` always populates a MachineProfile with a real vCPU count. Instance
    // type is environment-dependent (Some on EC2 with DMI, None off-EC2); we only
    // assert the profile exists and vCPU is sane, so the test is host-independent.
    //
    // Off-EC2 this exercises the bounded IMDS fallback (1 attempt, 500ms), which
    // fast-fails; that path is intentionally short so this stays quick in CI.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn load_populates_machine_profile() {
        let config = crate::from_env().load().await;
        let profile = config
            .machine_profile()
            .expect("loader populates a machine profile");
        assert!(profile.vcpus >= 1);
    }

    // Tests the pure parser, so it never touches the process environment and
    // cannot race the concurrent `load()`-based tests that read the same var.
    #[test]
    fn imds_disabled_parses_flag() {
        use super::imds_disabled_value;
        // Truthy spellings, case- and whitespace-insensitive.
        for truthy in ["true", "TRUE", "  true  ", "1", "yes", "YES", "on"] {
            assert!(imds_disabled_value(Some(truthy)), "{truthy:?} disables");
        }
        // Everything else leaves IMDS enabled.
        for falsy in ["false", "0", "no", "off", "", "  ", "enabled", "2"] {
            assert!(!imds_disabled_value(Some(falsy)), "{falsy:?} stays enabled");
        }
        assert!(!imds_disabled_value(None), "unset defaults to enabled");
    }

    /// The loader builds its own S3 configuration, so a captured request can only be had by
    /// extracting that configuration and rebuilding a client from it. What this covers is the
    /// loader path reaching the wire with our attribution; the sections themselves are owned by
    /// the tests in [`crate::config::user_agent`].
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn load_path_emits_attribution() {
        let (http_client, captured_request) = capture_request(None);
        let config = crate::from_env()
            .part_size(PartSize::Target(8))
            .load()
            .await;
        // Build the TM client so we can extract the S3 client's config with interceptors.
        let tm_client = crate::Client::new(config);
        let sdk_s3_config = tm_client
            .handle
            .s3_client
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
        let expected_req = captured_request.expect_request();
        let user_agent = expected_req.headers().get("x-amz-user-agent").unwrap();
        let ours = concat!("md/rust-tm#", env!("CARGO_PKG_VERSION"));
        assert!(
            user_agent.contains(ours),
            "loader path carries no attribution: {user_agent:?}"
        );
    }
}
