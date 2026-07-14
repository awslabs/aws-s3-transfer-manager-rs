/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::unit::ByteUnit;
use crate::runtime::ManagedThreadRuntime;
use crate::scheduler::{ConcurrencyController, FixedConcurrency, Scheduler};
use crate::telemetry::Telemetry;
use crate::types::{ConcurrencyMode, PartSize};
use crate::Config;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::memory::{MemoryBudget, BUDGET_CHUNK_BYTES};
use crate::runtime::ExecutionRuntime;

/// Non-binding budget for test handles, which size objects far below it, so the
/// budget never gates in state-machine and mock-SDK tests.
#[cfg(test)]
const TEST_MEMORY_BUDGET_BYTES: usize = 8 * ByteUnit::Gibibyte.as_bytes_usize();

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) handle: Arc<Handle>,
}

/// Shared state backing every transfer operation: configuration, the S3 client,
/// the scheduler, the execution runtime, the concurrency controller, telemetry,
/// and the memory budget.
#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) config: crate::Config,
    pub(crate) s3_client: aws_sdk_s3::Client,
    pub(crate) scheduler: Scheduler,
    pub(crate) runtime: Arc<dyn ExecutionRuntime>,
    pub(crate) controller: Arc<dyn ConcurrencyController>,
    pub(crate) telemetry: Arc<Telemetry>,
    pub(crate) memory_budget: Arc<MemoryBudget>,
}

impl Handle {
    /// Get the concrete minimum upload size in bytes to use to determine whether multipart uploads
    /// are enabled for a given request.
    pub(crate) fn mpu_threshold_bytes(&self) -> u64 {
        match self.config.multipart_threshold() {
            PartSize::Auto => 16 * ByteUnit::Mebibyte.as_bytes_u64(),
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Get the concrete target part size to use for uploads
    pub(crate) fn upload_part_size_bytes(&self) -> u64 {
        match self.config.part_size() {
            PartSize::Auto => 8 * ByteUnit::Mebibyte.as_bytes_u64(),
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Get the concrete target part size to use for downloads
    pub(crate) fn download_part_size_bytes(&self) -> u64 {
        match self.config.part_size() {
            PartSize::Auto => 5 * ByteUnit::Mebibyte.as_bytes_u64(),
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Whether the user pinned an explicit part size (vs leaving it `Auto`).
    /// When `Auto`, the transfer manager owns part sizing and may override it
    /// (e.g. align download ranges to an object's stored part size for
    /// validation); an explicit size is respected as set.
    pub(crate) fn user_set_part_size(&self) -> bool {
        matches!(self.config.part_size(), PartSize::Target(_))
    }

    /// Create a Handle for testing with a custom scheduler factory.
    #[cfg(test)]
    pub(crate) fn new_for_test(mut config: crate::Config, concurrency: usize) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let scheduler = Scheduler::new(weak.clone());
            let runtime: Arc<dyn ExecutionRuntime> =
                Arc::new(crate::runtime::TokioMultiThreadRuntime::new(weak.clone()));
            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => client,
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    aws_sdk_s3::Client::from_conf(s3_config.builder.build())
                }
            };
            Self {
                config,
                s3_client,
                scheduler,
                runtime,
                controller: Arc::new(crate::scheduler::FixedConcurrency::new(concurrency)),
                telemetry: Arc::new(Telemetry::new(std::time::Duration::from_millis(500))),
                memory_budget: MemoryBudget::new(TEST_MEMORY_BUDGET_BYTES, BUDGET_CHUNK_BYTES),
            }
        })
    }

    /// Test handle using the ambient tokio runtime (no OS threads spawned).
    ///
    /// Use for: state machine logic, poll_work/execute correctness, mock SDK
    /// interactions. Fast and deterministic.
    ///
    /// Does NOT exercise: managed thread dispatch, per-thread HTTP clients,
    /// cross-runtime wake semantics.
    #[cfg(test)]
    pub(crate) fn test_handle_tokio(config: crate::Config) -> Arc<Self> {
        Self::new_for_test(config, 128)
    }

    /// Test handle with real managed threads (4 OS threads).
    ///
    /// Use for: end-to-end dispatch/wake correctness, verifying behavior
    /// under real thread scheduling. Catches bugs like missing `set_pending`
    /// that only manifest when work is dispatched across thread boundaries.
    ///
    /// The outer test can use `#[tokio::test]` (single-thread) — managed
    /// threads own their own runtimes independently.
    #[cfg(test)]
    pub(crate) fn test_handle_managed(config: crate::Config) -> Arc<Self> {
        Self::new_for_test_with_runtime(config, 128, |weak| {
            Arc::new(
                crate::runtime::ManagedThreadRuntime::builder(weak)
                    .topology(crate::runtime::Topology::uniform(4))
                    .build(),
            )
        })
    }

    /// Create a Handle for testing with a custom runtime factory.
    #[cfg(test)]
    pub(crate) fn new_for_test_with_runtime(
        mut config: crate::Config,
        concurrency: usize,
        runtime_factory: impl FnOnce(std::sync::Weak<Handle>) -> Arc<dyn ExecutionRuntime>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| {
            let scheduler = Scheduler::new(weak.clone());
            let runtime = runtime_factory(weak.clone());
            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => client,
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    aws_sdk_s3::Client::from_conf(s3_config.builder.build())
                }
            };
            Self {
                config,
                s3_client,
                scheduler,
                runtime,
                controller: Arc::new(crate::scheduler::FixedConcurrency::new(concurrency)),
                telemetry: Arc::new(Telemetry::new(std::time::Duration::from_millis(500))),
                memory_budget: MemoryBudget::new(TEST_MEMORY_BUDGET_BYTES, BUDGET_CHUNK_BYTES),
            }
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}

impl Client {
    /// Creates a new client from a transfer manager config.
    pub fn new(mut config: Config) -> Client {
        // Machine facts for auto-sizing: use the loader-detected profile if the
        // config came through `ConfigLoader::load`, else detect locally (DMI +
        // vCPU + RAM — cheap pseudo-file reads, no network). Detected once and
        // shared by both the concurrency seed and the memory budget.
        let profile = config
            .machine_profile()
            .cloned()
            .unwrap_or_else(crate::runtime::platform::MachineProfile::detect_local);

        // 1. Create concurrency controller and telemetry
        let (controller, telemetry): (Arc<dyn ConcurrencyController>, _) = {
            // Resolve the concurrency target once, at construction. The preview
            // ships a fixed instance-aware seed rather than the adaptive
            // controller (which is left in the tree for a later release); see
            // `runtime::platform` concurrency seeding.
            let target = resolve_concurrency_target(config.concurrency(), &profile);
            (
                Arc::new(FixedConcurrency::new(target)),
                Arc::new(Telemetry::new(Duration::from_millis(500))),
            )
        };

        // 2. Build Handle with Arc::new_cyclic so scheduler and runtime
        //    can hold Weak<Handle> without creating a reference cycle.
        #[cfg(feature = "dial9")]
        let telemetry_guard = config.take_telemetry_guard().map(std::sync::Arc::new);

        // Resolve the budget once from the same detected RAM: it sizes the
        // Handle's MemoryBudget.
        let budget_capacity = config.memory_budget().resolve(profile.ram_bytes);

        let handle = Arc::new_cyclic(|weak_handle| {
            let scheduler = Scheduler::new(weak_handle.clone());
            let runtime: Arc<dyn ExecutionRuntime> = {
                #[allow(unused_mut)]
                let mut builder = ManagedThreadRuntime::builder(weak_handle.clone());
                #[cfg(feature = "dial9")]
                if let Some(guard) = telemetry_guard {
                    builder = builder.telemetry_guard(guard);
                }
                Arc::new(builder.build())
            };

            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => client,
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    let mut builder = s3_config.builder;
                    if s3_config.enable_runtime_http {
                        if let Some(http_client) = runtime.components().http_client() {
                            builder = builder.http_client(http_client.clone());
                        }
                    }
                    aws_sdk_s3::Client::from_conf(builder.build())
                }
            };

            Handle {
                config,
                s3_client,
                scheduler,
                runtime,
                controller,
                telemetry,
                memory_budget: MemoryBudget::new(budget_capacity, BUDGET_CHUNK_BYTES),
            }
        });
        Client { handle }
    }

    /// Returns the client's configuration
    pub fn config(&self) -> &Config {
        &self.handle.config
    }

    /// Snapshot the global memory budget's resolved ceiling and current admission
    /// state. For the configured intent (before resolution), see
    /// [`config().memory_budget()`](Config::memory_budget).
    pub fn memory_budget(&self) -> crate::types::MemoryBudgetSnapshot {
        self.handle.memory_budget.stats()
    }

    /// Upload a single object from S3.
    ///
    /// Constructs a fluent builder for the
    /// [`Upload`](crate::operation::upload::builders::UploadFluentBuilder) operation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::error::Error;
    /// use std::path::Path;
    /// use aws_sdk_s3_transfer_manager::io::InputStream;
    ///
    /// async fn upload_file(
    ///     client: &aws_sdk_s3_transfer_manager::Client,
    ///     path: impl AsRef<Path>
    /// ) -> Result<(), Box<dyn Error>> {
    ///     let stream = InputStream::from_path(path)?;
    ///     let handle = client.upload()
    ///         .bucket("my-bucket")
    ///         .key("my-key")
    ///         .body(stream)
    ///         .initiate()?;
    ///
    ///     // initiate() will return before the transfer is complete.
    ///     // Call the `join()` method on the returned handle to drive the transfer to completion.
    ///     // The handle can also be used to get progress, pause, or cancel the transfer, etc.
    ///     let response = handle.join().await?;
    ///     // ... do something with response
    ///     Ok(())
    /// }
    ///
    /// ```
    pub fn upload(&self) -> crate::operation::upload::builders::UploadFluentBuilder {
        crate::operation::upload::builders::UploadFluentBuilder::new(self.handle.clone())
    }

    /// Download a single object from S3.
    ///
    /// A single logical request may be split into many concurrent ranged `GetObject` requests
    /// to improve throughput.
    ///
    /// Constructs a fluent builder for the
    /// [`Download`](crate::operation::download::builders::DownloadFluentBuilder) operation.
    ///
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::error::Error;
    ///
    /// async fn get_object(client: &aws_sdk_s3_transfer_manager::Client) -> Result<(), Box<dyn Error>> {
    ///
    ///     let handle = client
    ///         .download()
    ///         .bucket("my-bucket")
    ///         .key("my-key")
    ///         .initiate()?;
    ///
    ///     // process data off handle...
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn download(&self) -> crate::operation::download::builders::DownloadFluentBuilder {
        crate::operation::download::builders::DownloadFluentBuilder::new(self.handle.clone())
    }

    /// Download multiple objects from an Amazon S3 bucket to a local directory
    ///
    /// Constructs a fluent builder for the
    /// [`DownloadObjects`](crate::operation::download_objects::builders::DownloadObjectsFluentBuilder) operation.
    ///
    /// # Examples
    /// ```no_run
    /// use std::path::PathBuf;
    /// use aws_sdk_s3_transfer_manager::error::Error;
    ///
    /// async fn download_bucket(
    ///     client: &aws_sdk_s3_transfer_manager::Client,
    ///     dest: PathBuf
    /// ) -> Result<(), Error> {
    ///
    ///     let handle = client
    ///         .download_objects()
    ///         .bucket("my-bucket")
    ///         .destination(dest)
    ///         .initiate()?;
    ///
    ///     // wait for transfer to complete
    ///     handle.join().await?;
    ///
    ///     Ok(())
    /// }
    ///
    /// ```
    pub fn download_objects(
        &self,
    ) -> crate::operation::download_objects::builders::DownloadObjectsFluentBuilder {
        crate::operation::download_objects::builders::DownloadObjectsFluentBuilder::new(
            self.handle.clone(),
        )
    }

    /// Upload multiple objects from a local directory to an Amazon S3 bucket
    ///
    /// Constructs a fluent builder for the
    /// [`UploadObjects`](crate::operation::upload_objects::builders::UploadObjectsFluentBuilder) operation.
    ///
    /// Examples
    /// ```no_run
    /// use std::path::Path;
    /// use aws_sdk_s3_transfer_manager::error::Error;
    /// use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
    ///
    /// async fn upload_directory(
    ///     client: &aws_sdk_s3_transfer_manager::Client,
    ///     source: &Path,
    /// ) -> Result<(), Error> {
    ///
    ///     let handle = client
    ///         .upload_objects()
    ///         .source(source)
    ///         .bucket("my-bucket")
    ///         .walker(FsWalker::builder().recursive(true).build())
    ///         .initiate()?;
    ///
    ///     // wait for transfer to complete
    ///     handle.join().await?;
    ///
    ///     Ok(())
    /// }
    ///
    /// ```
    pub fn upload_objects(
        &self,
    ) -> crate::operation::upload_objects::builders::UploadObjectsFluentBuilder {
        crate::operation::upload_objects::builders::UploadObjectsFluentBuilder::new(
            self.handle.clone(),
        )
    }
}

/// Resolve the fixed in-flight concurrency target from the concurrency mode and
/// the detected machine profile. Called by [`Client::new`] to build the
/// [`FixedConcurrency`] controller.
///
/// - [`ConcurrencyMode::Explicit`] — the caller's value verbatim.
/// - [`ConcurrencyMode::TargetThroughput`] — derived from the download target
///   (the scheduler has one global concurrency target; independent up/down
///   limiting is not yet supported).
/// - [`ConcurrencyMode::Auto`] — instance-aware seed from the profile's instance
///   type and vCPU count; falls back to a vCPU-scaled seed when the family is
///   unknown or the instance type was not detected.
fn resolve_concurrency_target(
    mode: &ConcurrencyMode,
    profile: &crate::runtime::platform::MachineProfile,
) -> usize {
    use crate::runtime::platform;
    match mode {
        // Guard the trait's `target >= 1` invariant: `Explicit(0)` would panic in
        // `FixedConcurrency::new`. Clamp to 1 rather than panic on a config value.
        ConcurrencyMode::Explicit(n) => (*n).max(1),
        ConcurrencyMode::TargetThroughput(t) => {
            let gbps = t.download().as_unit_per_sec(ByteUnit::Gigabit);
            let target = platform::seed_from_gbps(gbps);
            tracing::debug!(
                target: crate::telemetry::TARGET_CONCURRENCY,
                gbps,
                seed = target,
                "resolved TargetThroughput concurrency",
            );
            target
        }
        ConcurrencyMode::Auto => {
            let target =
                platform::auto_concurrency_seed(profile.instance_type.as_deref(), profile.vcpus);
            tracing::debug!(
                target: crate::telemetry::TARGET_CONCURRENCY,
                instance_type = ?profile.instance_type,
                vcpus = profile.vcpus,
                seed = target,
                "resolved Auto concurrency seed",
            );
            target
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> crate::Config {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        crate::Config::builder().client(s3_client).build()
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn handle_drop_shuts_down_runtime() {
        let handle = Handle::new_for_test(test_config(), 2);
        let weak = Arc::downgrade(&handle);
        drop(handle);
        assert!(weak.upgrade().is_none(), "Handle should be fully dropped");
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_clone_shares_handle() {
        let client = Client::new(test_config());
        let client2 = client.clone();
        assert!(Arc::ptr_eq(&client.handle, &client2.handle));
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn last_client_drop_releases_handle() {
        let client = Client::new(test_config());
        let weak = Arc::downgrade(&client.handle);
        let client2 = client.clone();
        drop(client);
        assert!(
            weak.upgrade().is_some(),
            "Handle alive while client2 exists"
        );
        drop(client2);
        assert!(weak.upgrade().is_none(), "Handle dropped after last client");
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn handle_drop_invalidates_weak_references() {
        let handle = Handle::new_for_test(test_config(), 2);
        let weak = Arc::downgrade(&handle);
        drop(handle);
        assert!(
            weak.upgrade().is_none(),
            "Weak should be invalid after Handle drop"
        );
    }

    // --- concurrency resolution wiring ---

    use crate::runtime::platform::MachineProfile;
    use crate::types::{ConcurrencyMode, TargetThroughput};

    /// A profile with the given instance type and vCPU count; RAM is irrelevant
    /// to concurrency resolution.
    fn profile(instance_type: Option<&str>, vcpus: usize) -> MachineProfile {
        MachineProfile {
            instance_type: instance_type.map(str::to_string),
            vcpus,
            ram_bytes: None,
        }
    }

    fn config_with(mode: ConcurrencyMode, profile: Option<MachineProfile>) -> crate::Config {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        crate::config::Config::builder()
            .client(s3_client)
            .concurrency(mode)
            .machine_profile(profile)
            .build()
    }

    #[test]
    fn resolve_explicit_is_verbatim() {
        assert_eq!(
            resolve_concurrency_target(&ConcurrencyMode::Explicit(42), &profile(None, 8)),
            42
        );
    }

    #[test]
    fn resolve_target_throughput_derives_from_download() {
        // 100 Gbps target -> ceil(100 / 0.4) = 250 in-flight.
        let mode =
            ConcurrencyMode::TargetThroughput(TargetThroughput::new_gigabits_per_sec(100));
        assert_eq!(resolve_concurrency_target(&mode, &profile(None, 8)), 250);
    }

    #[test]
    fn resolve_auto_uses_profile_instance_type() {
        // m6idn.16xlarge @ 64 vCPU -> 100 Gbps -> 250.
        let p = profile(Some("m6idn.16xlarge"), 64);
        assert_eq!(resolve_concurrency_target(&ConcurrencyMode::Auto, &p), 250);
    }

    #[test]
    fn resolve_auto_profile_without_instance_type_uses_vcpu_fallback() {
        // Detected vCPU but no instance type (DMI/IMDS miss): fallback 4 * 16 = 64.
        let p = profile(None, 16);
        assert_eq!(resolve_concurrency_target(&ConcurrencyMode::Auto, &p), 64);
    }

    #[test]
    fn resolve_explicit_zero_is_clamped_not_panic() {
        // `FixedConcurrency::new(0)` would panic; resolution must guard it.
        let p = profile(None, 8);
        assert_eq!(resolve_concurrency_target(&ConcurrencyMode::Explicit(0), &p), 1);
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_wires_resolved_target_into_controller() {
        // End-to-end: the resolved seed reaches the live controller's target().
        // c8gn.16xlarge: 3.125 Gbps/vCPU * 64 = 200 Gbps -> 500.
        let config = config_with(
            ConcurrencyMode::Auto,
            Some(profile(Some("c8gn.16xlarge"), 64)),
        );
        let client = Client::new(config);
        assert_eq!(client.handle.controller.target(), 500);
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_bypass_path_detects_locally() {
        // No profile on the config (built directly, not via the loader):
        // Client::new detects locally and still resolves a valid, clamped target.
        let config = config_with(ConcurrencyMode::Auto, None);
        let client = Client::new(config);
        assert!((10..=10_000).contains(&client.handle.controller.target()));
    }
}
