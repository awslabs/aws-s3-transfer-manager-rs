/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::unit::ByteUnit;
#[cfg(not(target_family = "wasm"))]
use crate::runtime::ManagedExecution;
use crate::scheduler::{ConcurrencyController, FixedConcurrency, Scheduler};
use crate::telemetry::Telemetry;
use crate::types::{ConcurrencyMode, MemoryConfig, PartSize};
use crate::Config;
use std::sync::Arc;
use std::time::Duration;

use crate::memory::BufferPool;
use crate::runtime::sizing::{self, ConcurrencyBound, ResourcePlan, Signals};
use crate::runtime::ExecutionRuntime;

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) handle: Arc<Handle>,
}

/// Shared state backing every transfer operation: configuration, the S3 client,
/// the scheduler, the execution runtime, the concurrency controller, telemetry,
/// and the shared payload-memory pool.
#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) config: crate::Config,
    pub(crate) s3_client: aws_sdk_s3::Client,
    pub(crate) scheduler: Scheduler,
    pub(crate) runtime: Arc<dyn ExecutionRuntime>,
    pub(crate) controller: Arc<dyn ConcurrencyController>,
    pub(crate) telemetry: Arc<Telemetry>,
    pub(crate) buffer_pool: BufferPool,
    /// Resource plan resolved when the client was constructed.
    pub(crate) plan: ResourcePlan,
    /// Per-bucket retry token bucket, cached so all operations and retries to a
    /// given bucket share one live token bucket. The SDK does not share a bucket
    /// across custom retry partitions, and a partition built per operation would
    /// start full every time, defeating the shared throttle budget and its refill.
    /// Unbounded: a client targets few distinct buckets, and entries are cheap
    /// (Arc-shared bucket + name). Invariant: an entry that operations may still be
    /// retrying against must not be dropped — replacing a live draining bucket with
    /// a fresh full one reintroduces the per-operation-fresh-bucket bug.
    retry_partitions: std::sync::Mutex<
        std::collections::HashMap<String, aws_sdk_s3::config::retry::RetryPartition>,
    >,
}

impl Handle {
    /// Reports newly registered scheduler work to client-owned subsystems.
    ///
    /// The callback runs while the scheduler holds its registry write lock.
    /// Keeping the adaptation here lets the scheduler publish lifecycle events
    /// without depending on the policy each client subsystem applies to them.
    pub(crate) fn scheduler_work_registered(&self) {
        self.buffer_pool.record_managed_activity();
    }

    /// Reports that the scheduler has no registered or dispatched work.
    ///
    /// The scheduler holds its registry read lock through this callback, so a
    /// concurrent registration either precedes this event or invalidates it
    /// through [`Self::scheduler_work_registered`].
    pub(crate) fn scheduler_became_idle(&self) {
        self.buffer_pool.record_global_idle();
    }

    /// Get the concrete minimum upload size in bytes to use to determine whether multipart uploads
    /// are enabled for a given request.
    pub(crate) fn mpu_threshold_bytes(&self) -> u64 {
        match self.config.multipart_threshold() {
            PartSize::Auto => 16 * ByteUnit::Mebibyte.as_bytes_u64(),
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Part size for uploads and downloads.
    pub(crate) fn part_size_bytes(&self) -> u64 {
        self.plan.part_size
    }

    /// Whether the user pinned an explicit part size (vs leaving it `Auto`).
    /// When `Auto`, the transfer manager owns part sizing and may override it
    /// (e.g. align download ranges to an object's stored part size for
    /// validation); an explicit size is respected as set.
    pub(crate) fn user_set_part_size(&self) -> bool {
        matches!(self.config.part_size(), PartSize::Target(_))
    }

    /// Per-operation config override that keys the retry token bucket by bucket.
    ///
    /// Returns an empty override when no bucket is set, which merges as a no-op.
    /// The partition is cached so all operations to the same bucket share one
    /// live token bucket (and its refill): under a total outage the sustained
    /// retry rate to S3 is `REFILL_RATE / throttling_retry_cost` (approx 2
    /// retries/sec per bucket), independent of concurrency.
    pub(crate) fn bucket_partition_override(
        &self,
        bucket: Option<&str>,
    ) -> aws_sdk_s3::config::Builder {
        let builder = aws_sdk_s3::config::Builder::default();
        match bucket {
            Some(b) => {
                let partition = {
                    let mut map = self.retry_partitions.lock().expect("lock poisoned");
                    map.entry(b.to_owned())
                        .or_insert_with(|| crate::retry::bucket_retry_partition(b))
                        .clone()
                };
                builder.retry_partition(partition)
            }
            None => builder,
        }
    }

    /// Per-operation config override for a transfer body (download GET or upload):
    /// bucket retry partition plus the tightened stalled-stream grace.
    fn transfer_override(&self, bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
        self.bucket_partition_override(bucket)
            .stalled_stream_protection(crate::retry::tightened_ssp())
    }

    /// Per-operation config override for a download GET: bucket retry partition
    /// plus the tightened stalled-stream grace on the response body. Intentionally
    /// identical to [`upload_override`](Self::upload_override) (both delegate to
    /// `transfer_override`); the two names document the call site's direction.
    pub(crate) fn download_get_override(
        &self,
        bucket: Option<&str>,
    ) -> aws_sdk_s3::config::Builder {
        self.transfer_override(bucket)
    }

    /// Per-operation config override for an upload (UploadPart / PutObject):
    /// bucket retry partition plus the tightened stalled-stream grace on the
    /// request body. Intentionally identical to
    /// [`download_get_override`](Self::download_get_override); the two names
    /// document the call site's direction.
    pub(crate) fn upload_override(&self, bucket: Option<&str>) -> aws_sdk_s3::config::Builder {
        self.transfer_override(bucket)
    }

    /// Create a Handle for testing on the ambient tokio runtime with a fixed
    /// concurrency target. The ergonomic common case; tests that need a custom
    /// runtime or a controller they drive directly use
    /// [`new_for_test_with_runtime`](Self::new_for_test_with_runtime).
    #[cfg(test)]
    pub(crate) fn new_for_test(config: crate::Config, concurrency: usize) -> Arc<Self> {
        Self::new_for_test_with_runtime(
            config,
            Arc::new(crate::scheduler::FixedConcurrency::new(concurrency)),
            |weak| Arc::new(crate::runtime::TokioMultiThreadRuntime::new(weak)),
        )
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

    /// Test handle with up to four managed threads from detected topology.
    ///
    /// Use for: end-to-end dispatch/wake correctness, verifying behavior
    /// under real thread scheduling. Catches bugs like missing `set_pending`
    /// that only manifest when work is dispatched across thread boundaries.
    ///
    /// The outer test can use `#[tokio::test]` (single-thread) — managed
    /// threads own their own runtimes independently.
    #[cfg(test)]
    pub(crate) fn test_handle_managed(config: crate::Config) -> Arc<Self> {
        Self::new_for_test_with_runtime(
            config,
            Arc::new(crate::scheduler::FixedConcurrency::new(128)),
            |weak| Arc::new(crate::runtime::ManagedExecution::for_test(weak, 4)),
        )
    }

    /// Create a Handle for testing with a custom concurrency controller and
    /// runtime factory. The controller is an axis because some tests drive its
    /// target directly (e.g. an adjustable controller); most pass a
    /// [`FixedConcurrency`](crate::scheduler::FixedConcurrency).
    #[cfg(test)]
    pub(crate) fn new_for_test_with_runtime(
        mut config: crate::Config,
        controller: Arc<dyn ConcurrencyController>,
        runtime_factory: impl FnOnce(std::sync::Weak<Handle>) -> Arc<dyn ExecutionRuntime>,
    ) -> Arc<Self> {
        // Most client tests exercise scheduling rather than memory admission.
        // Keep their automatic pool large enough not to become an incidental gate.
        const DEFAULT_TEST_MEMORY_BUDGET_BYTES: usize = 8 * ByteUnit::Gibibyte.as_bytes_usize();

        let memory_diagnostics = config.diagnostics().memory();
        let buffer_pool = match config.memory() {
            MemoryConfig::Auto => BufferPool::from_capacity(
                crate::types::MemoryBudgetConfig::Limit(DEFAULT_TEST_MEMORY_BUDGET_BYTES),
                None,
                memory_diagnostics,
            )
            .expect("test buffer-pool configuration must be valid"),
            MemoryConfig::Explicit(pool) => pool.clone(),
        };
        // Sized against this handle's own pool, so results do not depend on
        // host memory.
        let plan = sizing::resolve(Signals {
            throughput_gbps: config.throughput_target_gbps(),
            instance_type: None,
            vcpus: 1,
            ram_bytes: None,
            connection_cap: crate::runtime::platform::ConnectionCap::from_descriptor_limit(None),
            explicit_concurrency: Some(controller.target()),
            explicit_part_size: match config.part_size() {
                PartSize::Target(n) => Some(*n),
                PartSize::Auto => None,
            },
            ceiling_override_bytes: Some(
                usize::try_from(buffer_pool.metrics().configured_capacity_bytes())
                    .unwrap_or(usize::MAX),
            ),
        });
        Arc::new_cyclic(|weak| {
            let scheduler = Scheduler::new(weak.clone());
            let runtime = runtime_factory(weak.clone());
            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => client,
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    aws_sdk_s3::Client::from_conf(s3_config.config)
                }
            };
            Self {
                config,
                s3_client,
                scheduler,
                runtime,
                controller,
                telemetry: Arc::new(Telemetry::new(std::time::Duration::from_millis(500))),
                buffer_pool,
                plan,
                retry_partitions: std::sync::Mutex::new(std::collections::HashMap::new()),
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
    /// Returns a non-owning observer of managed-runtime shutdown completion.
    ///
    /// Returns `Some` for automatically created and adopted managed runtimes,
    /// and `None` for caller-owned Tokio execution. Retaining the observer does
    /// not keep this client or its runtime owner alive. Waiting does not request
    /// shutdown: release client clones and transfer ownership first.
    pub fn shutdown_completion(&self) -> Option<crate::ShutdownCompletion> {
        self.handle.runtime.shutdown_completion()
    }

    /// Creates a new client from a transfer manager config.
    pub fn new(mut config: Config) -> Client {
        // Machine facts for auto-sizing: use the loader-detected profile if the
        // config came through `ConfigLoader::load`, else detect locally (DMI +
        // vCPU + RAM — cheap pseudo-file reads, no network). Detected once and
        // shared by both the concurrency seed and the buffer pool.
        let profile = config
            .machine_profile()
            .cloned()
            .unwrap_or_else(crate::runtime::platform::MachineProfile::detect_local);

        let client_features = crate::config::user_agent::client_features(&config, &profile);

        let connection_cap = crate::runtime::platform::ConnectionCap::detect();
        let explicit_pool_ceiling = match config.memory() {
            MemoryConfig::Explicit(pool) => Some(
                usize::try_from(pool.metrics().configured_capacity_bytes()).unwrap_or(usize::MAX),
            ),
            MemoryConfig::Auto => None,
        };
        let plan = sizing::resolve(Signals {
            throughput_gbps: config.throughput_target_gbps(),
            instance_type: profile.instance_type.as_deref(),
            vcpus: profile.vcpus,
            ram_bytes: profile.ram_bytes,
            connection_cap,
            explicit_concurrency: match config.concurrency() {
                ConcurrencyMode::Explicit(n) => Some(*n),
                ConcurrencyMode::Auto | ConcurrencyMode::TargetThroughput(_) => None,
            },
            explicit_part_size: match config.part_size() {
                PartSize::Target(n) => Some(*n),
                PartSize::Auto => None,
            },
            ceiling_override_bytes: explicit_pool_ceiling,
        });

        let (controller, telemetry): (Arc<dyn ConcurrencyController>, _) = (
            Arc::new(FixedConcurrency::new(plan.concurrency)),
            Arc::new(Telemetry::new(Duration::from_millis(500))),
        );

        // 2. Build Handle with Arc::new_cyclic so scheduler and runtime
        //    can hold Weak<Handle> without creating a reference cycle.
        #[cfg(feature = "dial9")]
        let dial9_handle = config.take_dial9_handle();

        let memory_diagnostics = config.diagnostics().memory();
        let buffer_pool = match config.memory() {
            MemoryConfig::Auto => BufferPool::from_capacity(
                crate::types::MemoryBudgetConfig::Limit(plan.ceiling_bytes),
                profile.ram_bytes,
                memory_diagnostics,
            )
            .expect("memory configuration must resolve to supported pool geometry"),
            MemoryConfig::Explicit(pool) => pool.clone(),
        };
        // Runtime HTTP is built only when it will be installed on an S3 client
        // this transfer manager constructs.
        let runtime_http = config.runtime_http(plan.connection_cap);
        log_resource_sizing(
            &plan,
            &connection_cap,
            runtime_http.is_some(),
            config.ignored_throughput_target_gbps(),
        );
        // wasm targets have no managed execution; see `Managed`.
        #[cfg(not(target_family = "wasm"))]
        let managed = match config.managed_config_mut() {
            Some(managed) => {
                let owner = managed
                    .build(
                        #[cfg(feature = "dial9")]
                        dial9_handle,
                    )
                    .expect("failed to construct transfer manager execution");
                let components = crate::runtime::prepare_components(&owner, runtime_http)
                    .expect("failed to construct transfer manager HTTP transport");
                Some((owner, components))
            }
            None => None,
        };
        let handle = Arc::new_cyclic(|weak_handle| {
            let scheduler = Scheduler::new(weak_handle.clone());
            #[cfg(not(target_family = "wasm"))]
            let runtime: Arc<dyn ExecutionRuntime> = match managed {
                Some((owner, components)) => Arc::new(ManagedExecution::new(
                    weak_handle.clone(),
                    owner,
                    components,
                )),
                None => Arc::new(crate::runtime::TokioMultiThreadRuntime::new(
                    weak_handle.clone(),
                )),
            };
            #[cfg(target_family = "wasm")]
            let runtime: Arc<dyn ExecutionRuntime> = Arc::new(
                crate::runtime::TokioMultiThreadRuntime::new(weak_handle.clone()),
            );

            let s3_client = match config.take_s3_client_source() {
                crate::config::S3ClientSource::Provided(client) => {
                    crate::config::user_agent::install_on_client(
                        client,
                        config.framework_metadata().cloned(),
                        client_features,
                    )
                }
                crate::config::S3ClientSource::FromConfig(s3_config) => {
                    let mut builder = s3_config.config.to_builder();
                    if let Some(http_client) = runtime.components().http_client() {
                        builder = builder.http_client(http_client.clone());
                    }
                    crate::config::user_agent::install(
                        &mut builder,
                        config.framework_metadata().cloned(),
                        client_features,
                    );
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
                buffer_pool,
                plan,
                retry_partitions: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        });
        Client { handle }
    }

    /// Returns the client's configuration
    pub fn config(&self) -> &Config {
        &self.handle.config
    }

    /// Returns point-in-time operational metrics for this client.
    pub fn metrics(&self) -> crate::metrics::ClientMetrics {
        crate::metrics::ClientMetrics::new(self.handle.buffer_pool.metrics())
    }

    /// Upload a single object to S3.
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
    ///     // initiate() will return before the transfer is complete.
    ///     // Call the `join()` method on the returned handle to drive the transfer to completion.
    ///     // The handle can also be used to get progress, pause, or cancel the transfer, etc.
    ///     let response = handle.join().await?;
    ///     // ... do something with response
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

/// Logs the resolved sizing at `INFO`, and a `WARN` for each configured value
/// the host cannot fund.
///
/// `connection_cap_installed` is whether the cap is applied to a transport this
/// client builds.
fn log_resource_sizing(
    plan: &ResourcePlan,
    cap: &crate::runtime::platform::ConnectionCap,
    connection_cap_installed: bool,
    ignored_throughput_gbps: Option<f64>,
) {
    let soft = cap.descriptors.map(|limit| limit.soft);
    let hard = cap.descriptors.and_then(|limit| limit.hard);
    if let Some(gbps) = ignored_throughput_gbps {
        tracing::warn!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            configured_throughput_gbps = gbps,
            "configured throughput target is not finite and positive and was ignored; throughput \
             is estimated from the execution environment instead",
        );
    }
    tracing::info!(
        target: crate::telemetry::TARGET_CONCURRENCY,
        concurrency_target = plan.concurrency,
        source = plan.concurrency_source.as_str(),
        bounded_by = plan.concurrency_bound.as_str(),
        throughput_gbps = ?plan.throughput_gbps,
        memory_ceiling_bytes = plan.ceiling_bytes,
        memory_detected = plan.memory_detected,
        part_size_bytes = plan.part_size,
        max_connections_per_host = plan.connection_cap,
        connection_cap_installed,
        max_funded_concurrency = plan.max_funded_concurrency,
        soft_descriptor_limit = ?soft,
        hard_descriptor_limit = ?hard,
        "resolved resource sizing",
    );

    if plan.configured_concurrency_unfunded {
        tracing::warn!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            concurrency_target = plan.concurrency,
            max_funded_concurrency = plan.max_funded_concurrency,
            max_connections_per_host = plan.connection_cap,
            "configured concurrency target exceeds what the payload-memory ceiling and connection \
             cap sustain; requests beyond that point wait on memory admission or a connection",
        );
    }

    if plan.configured_part_size_unfunded {
        tracing::warn!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            part_size_bytes = plan.part_size,
            memory_ceiling_bytes = plan.ceiling_bytes,
            "configured part size exceeds what the payload-memory ceiling funds for a single \
             concurrency slot; raise the memory ceiling or lower the part size",
        );
    }

    if cap.limited_by_descriptors() && plan.concurrency_bound == ConcurrencyBound::Connections {
        tracing::warn!(
            target: crate::telemetry::TARGET_CONCURRENCY,
            max_connections_per_host = plan.connection_cap,
            concurrency_target = plan.concurrency,
            soft_descriptor_limit = ?soft,
            hard_descriptor_limit = ?hard,
            "concurrency is limited to half the soft file-descriptor limit (RLIMIT_NOFILE); raise \
             the soft limit to allow more connections",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeConfig;
    use aws_smithy_runtime_api::client::http::SharedHttpClient;

    fn test_config() -> crate::Config {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        crate::Config::builder().sdk_client(s3_client).build()
    }

    fn runtime_http_client(client: &Client) -> Option<&SharedHttpClient> {
        client.handle.runtime.components().http_client()
    }

    /// A configured transport that polls file bodies on a plain thread,
    /// retries the first PUT, and lets the test hold an active transfer.
    #[derive(Debug)]
    struct ThreadHttp {
        started: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        bodies: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        execution: Option<aws_smithy_execution::ManagedRuntimeHandle>,
        dispatches: Arc<std::sync::Mutex<Vec<aws_smithy_execution::RuntimePartitionId>>>,
    }

    impl aws_smithy_runtime_api::client::http::HttpConnector for ThreadHttp {
        fn call(
            &self,
            request: aws_smithy_runtime_api::client::orchestrator::HttpRequest,
        ) -> aws_smithy_runtime_api::client::http::HttpConnectorFuture {
            use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
            use aws_smithy_types::body::SdkBody;
            use http_body_1x::Body;
            use std::pin::Pin;

            assert_eq!(request.method(), "PUT");
            if let Some(execution) = &self.execution {
                let partition = execution
                    .current_partition()
                    .expect("transfer must execute on the adopted runtime");
                self.dispatches.lock().unwrap().push(partition.id());
            }
            let started = self.started.clone();
            let release = self.release.clone();
            let bodies = self.bodies.clone();
            let (completed, result) = tokio::sync::oneshot::channel();
            std::thread::spawn(move || {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let response = crate::runtime::test_util::block_on(async move {
                    let first = bodies.lock().unwrap().is_empty();
                    if first {
                        started.add_permits(1);
                        release.acquire().await.unwrap().forget();
                    }
                    let mut body = request.into_body();
                    let mut bytes = Vec::new();
                    while let Some(frame) =
                        futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
                    {
                        if let Ok(data) = frame.unwrap().into_data() {
                            bytes.extend_from_slice(&data);
                        }
                    }
                    bodies.lock().unwrap().push(bytes);
                    HttpResponse::try_from(
                        ::http::Response::builder()
                            .status(if first { 500 } else { 200 })
                            .header("etag", "\"test-etag\"")
                            .body(SdkBody::empty())
                            .unwrap(),
                    )
                    .unwrap()
                });
                let _ = completed.send(response);
            });
            aws_smithy_runtime_api::client::http::HttpConnectorFuture::new(async move {
                Ok(result.await.unwrap())
            })
        }
    }

    // Real SDK requests, body replay, and owner retention are exercised together:
    // the configured HTTP implementation polls outside managed execution.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn configured_sdk_http_uploads_files_and_retains_active_execution() {
        use aws_sdk_s3::config::{Credentials, Region, SharedCredentialsProvider};
        use aws_smithy_runtime_api::client::http::{http_client_fn, SharedHttpConnector};
        use std::io::Write;

        let caller = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let payload = vec![0x5a; 128 * 1024];
        file.write_all(&payload).unwrap();
        let mut checksum = aws_smithy_checksums::ChecksumAlgorithm::Crc32.into_impl();
        checksum.update(&payload);
        let checksum = checksum.header_value().to_str().unwrap().to_owned();

        let adopted: Option<(
            RuntimeConfig,
            bool,
            Option<aws_smithy_execution::ManagedRuntimeHandle>,
        )> = {
            #[cfg(feature = "__unstable_ext")]
            {
                let owner = aws_smithy_execution::ManagedRuntime::builder()
                    .topology(
                        aws_smithy_execution::topology::Topology::detect()
                            .unwrap()
                            .with_max_placements(1),
                    )
                    .thread_name_prefix("adopted-upload")
                    .pin_threads(false)
                    .build()
                    .unwrap();
                let execution = owner.handle();
                Some((
                    RuntimeConfig::managed(crate::unstable::managed(owner)),
                    true,
                    Some(execution),
                ))
            }
            #[cfg(not(feature = "__unstable_ext"))]
            {
                None
            }
        };
        for (runtime, managed, adopted) in [
            (RuntimeConfig::default(), true, None),
            (RuntimeConfig::multi_thread_tokio(), false, None),
        ]
        .into_iter()
        .chain(adopted)
        {
            caller.block_on(async {
                let started = Arc::new(tokio::sync::Semaphore::new(0));
                let release = Arc::new(tokio::sync::Semaphore::new(0));
                let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
                let dispatches = Arc::new(std::sync::Mutex::new(Vec::new()));
                let owner_watch = adopted
                    .as_ref()
                    .map(|execution| execution.partitions()[0].spawn(std::future::pending::<()>()));
                let connector = SharedHttpConnector::new(ThreadHttp {
                    started: started.clone(),
                    release: release.clone(),
                    bodies: bodies.clone(),
                    execution: adopted.clone(),
                    dispatches: dispatches.clone(),
                });
                let sdk = aws_types::SdkConfig::builder()
                    .http_client(http_client_fn(move |_, _| connector.clone()))
                    .region(Region::new("us-west-2"))
                    .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                        "test", "test", None, None, "test",
                    )))
                    .behavior_version(aws_config::BehaviorVersion::latest())
                    .build();
                let client = Client::new(
                    crate::Config::builder()
                        .s3_config(&sdk)
                        .runtime_config(runtime)
                        .build(),
                );
                let components = client.handle.runtime.components();
                assert!(
                    components.http_client().is_none(),
                    "unused managed HTTP must not be built"
                );
                assert_eq!(components.direct_io(), managed);
                assert!(
                    !components.direct_body_io(),
                    "configured HTTP controls its polling thread"
                );

                let completion = client.shutdown_completion();
                assert_eq!(completion.is_some(), managed);
                let weak = Arc::downgrade(&client.handle);
                let upload = client
                    .upload()
                    .bucket("test-bucket")
                    .key("file")
                    .checksum_strategy(crate::operation::upload::ChecksumStrategy::with_crc32(
                        &checksum,
                    ))
                    .body(crate::io::InputStream::from_path(file.path()).unwrap())
                    .initiate()
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), started.acquire())
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                drop(client);
                assert!(
                    weak.upgrade().is_some(),
                    "active transfer retains execution owner"
                );
                release.add_permits(1);
                let output = tokio::time::timeout(Duration::from_secs(10), upload.join())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    output.upload_id().is_none(),
                    "exercise single-PUT file body"
                );
                {
                    let captured = bodies.lock().unwrap();
                    assert_eq!(captured.len(), 2, "SDK retries the first PUT");
                    assert!(
                        captured.iter().all(|body| body == &payload),
                        "both attempts replay the file"
                    );
                }
                if let Some(execution) = &adopted {
                    let observed = dispatches.lock().unwrap();
                    assert_eq!(observed.len(), 2);
                    assert!(observed.iter().all(|id| id.runtime_id() == execution.id()));
                }
                tokio::time::timeout(Duration::from_secs(5), async {
                    // Terminal notification can precede the last work item's
                    // retirement.
                    while weak.upgrade().is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    panic!("finished transfer retains execution (managed={managed})")
                });
                if let Some(completion) = completion {
                    tokio::time::timeout(Duration::from_secs(5), completion.wait())
                        .await
                        .unwrap()
                        .unwrap();
                }
                if let Some(watch) = owner_watch {
                    let error = tokio::time::timeout(Duration::from_secs(5), watch)
                        .await
                        .unwrap()
                        .unwrap_err();
                    assert!(
                        error.is_cancelled(),
                        "retained handles must not retain ownership"
                    );
                }
            });
        }
    }

    #[cfg(all(feature = "__unstable_ext", not(miri)))]
    #[tokio::test]
    async fn adopted_runtime_without_configured_http_uses_the_managed_preset() {
        let owner = aws_smithy_execution::ManagedRuntime::builder()
            .topology(
                aws_smithy_execution::topology::Topology::detect()
                    .unwrap()
                    .with_max_placements(1),
            )
            .pin_threads(false)
            .build()
            .unwrap();
        let execution = owner.handle();
        let watch = execution.partitions()[0].spawn(std::future::pending::<()>());
        let mut s3 = mock_s3_config().config.to_builder();
        s3.set_http_client(None);
        let client = Client::new(
            crate::Config::builder()
                .s3_config(crate::config::S3ClientConfig::from_sdk_builder(s3))
                .runtime_config(RuntimeConfig::managed(crate::unstable::managed(owner)))
                .build(),
        );
        assert!(client
            .handle
            .config
            .runtime_config()
            .managed_config()
            .is_some());
        let components = client.handle.runtime.components();
        assert!(components.http_client().is_some());
        assert!(components.direct_io());
        assert!(components.direct_body_io());
        assert_eq!(
            execution.partitions()[0].spawn(async { 42 }).await.unwrap(),
            42,
        );
        drop(client);
        assert!(tokio::time::timeout(Duration::from_secs(5), watch)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
    }

    #[cfg(all(
        feature = "__unstable_ext",
        not(miri),
        any(
            target_os = "android",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "linux",
            target_os = "macos",
            target_os = "solaris",
            target_os = "tvos",
            target_os = "visionos",
            target_os = "watchos",
        )
    ))]
    #[tokio::test]
    async fn transport_construction_failure_drops_the_adopted_owner() {
        let owner = aws_smithy_execution::ManagedRuntime::builder()
            .topology(
                aws_smithy_execution::topology::Topology::detect()
                    .unwrap()
                    .with_max_placements(1),
            )
            .pin_threads(false)
            .build()
            .unwrap();
        let execution = owner.handle();
        let watch = execution.partitions()[0].spawn(std::future::pending::<()>());
        let mut s3 = mock_s3_config().config.to_builder();
        s3.set_http_client(None);
        let config = crate::Config::builder()
            .s3_config(
                crate::config::S3ClientConfig::from_sdk_builder(s3)
                    .network_interfaces(["bad\0interface"]),
            )
            .runtime_config(RuntimeConfig::managed(crate::unstable::managed(owner)))
            .build();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Client::new(config)));
        assert!(result.is_err());
        assert!(tokio::time::timeout(Duration::from_secs(5), watch)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
    }

    #[cfg(all(feature = "__unstable_ext", feature = "dial9", not(miri)))]
    #[tokio::test]
    async fn adopted_runtime_rejects_tm_recorder_and_releases_ownership() {
        let owner = aws_smithy_execution::ManagedRuntime::builder()
            .topology(
                aws_smithy_execution::topology::Topology::detect()
                    .unwrap()
                    .with_max_placements(1),
            )
            .pin_threads(false)
            .build()
            .unwrap();
        let execution = owner.handle();
        let watch = execution.partitions()[0].spawn(std::future::pending::<()>());
        let config = crate::Config::builder()
            .s3_config(mock_s3_config())
            .runtime_config(RuntimeConfig::managed(crate::unstable::managed(owner)))
            .dial9_handle(dial9::Dial9Handle::disabled())
            .build();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Client::new(config)));
        let panic = match result {
            Err(panic) => panic,
            Ok(_) => panic!("conflicting recorder must fail client construction"),
        };
        let message = panic.downcast_ref::<String>().unwrap();
        assert!(message.contains("configure Dial9 on the runtime builder before adoption"));
        assert!(tokio::time::timeout(Duration::from_secs(5), watch)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn shared_tokio_without_configured_http_uses_sdk_defaults() {
        let mut s3 = mock_s3_config().config.to_builder();
        s3.set_http_client(None);
        let client = Client::new(
            crate::Config::builder()
                .s3_config(crate::config::S3ClientConfig::from_sdk_builder(s3))
                .runtime_config(RuntimeConfig::multi_thread_tokio())
                .build(),
        );
        let components = client.handle.runtime.components();
        assert!(components.http_client().is_none());
        assert!(!components.direct_io());
        assert!(!components.direct_body_io());
    }

    fn mock_s3_config() -> crate::config::S3ClientConfig {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        crate::config::S3ClientConfig::from_sdk_builder(s3_client.config().to_builder())
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn managed_runtime_builds_http_for_s3_config() {
        let mut builder = mock_s3_config().config.to_builder();
        builder.set_http_client(None);
        let client = Client::new(
            crate::Config::builder()
                .s3_config(crate::config::S3ClientConfig::from_sdk_builder(builder))
                .build(),
        );
        assert!(runtime_http_client(&client).is_some());
        assert!(client.handle.runtime.components().direct_io());
        assert!(client.handle.runtime.components().direct_body_io());
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn managed_runtime_skips_http_for_provided_client() {
        let client = Client::new(test_config());
        assert!(runtime_http_client(&client).is_none());
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn managed_runtime_preserves_configured_http() {
        let config = crate::Config::builder().s3_config(mock_s3_config()).build();
        let client = Client::new(config);
        assert!(runtime_http_client(&client).is_none());
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
    fn test_client_uses_the_explicit_buffer_pool() {
        let pool = BufferPool::from_capacity(
            crate::types::MemoryBudgetConfig::Limit(16 * ByteUnit::Mebibyte.as_bytes_usize()),
            None,
            crate::config::MemoryDiagnosticsConfig::default(),
        )
        .unwrap();
        let carrier_size = pool.carrier_size();
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder()
            .sdk_client(s3_client)
            .memory(MemoryConfig::Explicit(pool.clone()))
            .build();

        let client = Client::new(config);
        let reservation = pool
            .try_reserve(carrier_size)
            .unwrap()
            .expect("explicit pool reservation");

        let client_metrics = client.metrics();
        let memory = client_metrics.memory();
        assert_eq!(memory.active_planned_demand_bytes(), carrier_size as u64);
        assert_eq!(
            memory.configured_capacity_bytes(),
            pool.metrics().configured_capacity_bytes()
        );
        drop(reservation);
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_client_automatic_pool_starts_unprepared() {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder()
            .sdk_client(s3_client)
            .machine_profile(Some(crate::runtime::platform::MachineProfile {
                instance_type: None,
                vcpus: 4,
                ram_bytes: Some(512 * ByteUnit::Mebibyte.as_bytes_usize()),
            }))
            .build();
        let client = Client::new(config);
        let client_metrics = client.metrics();
        let memory = client_metrics.memory();

        assert!(memory.configured_capacity_bytes() > 0);
        assert_eq!(memory.admission_used_bytes(), 0);
        assert_eq!(memory.prepared_capacity_bytes(), 0);
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_handle_test_constructor_preserves_an_explicit_pool() {
        let pool = BufferPool::from_capacity(
            crate::types::MemoryBudgetConfig::Limit(16 * ByteUnit::Mebibyte.as_bytes_usize()),
            None,
            crate::config::MemoryDiagnosticsConfig::default(),
        )
        .unwrap();
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder()
            .sdk_client(s3_client)
            .memory(MemoryConfig::Explicit(pool.clone()))
            .build();

        let handle = Handle::new_for_test(config, 2);
        let reservation = pool
            .try_reserve(pool.carrier_size())
            .unwrap()
            .expect("explicit pool reservation");

        assert_eq!(
            handle.buffer_pool.metrics().active_planned_demand_bytes(),
            pool.carrier_size() as u64
        );
        drop(reservation);
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

    use crate::runtime::platform::{self, MachineProfile};
    use crate::types::ConcurrencyMode;

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
            .sdk_client(s3_client)
            .concurrency(mode)
            .machine_profile(profile)
            .build()
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_wires_resolved_target_into_controller() {
        // The resolved target is the controller's target. Expected values come
        // from the pass because the descriptor cap depends on RLIMIT_NOFILE.
        let p = profile(Some("c8gn.16xlarge"), 64);
        let expected = sizing::resolve(Signals {
            throughput_gbps: None,
            instance_type: p.instance_type.as_deref(),
            vcpus: p.vcpus,
            ram_bytes: p.ram_bytes,
            connection_cap: platform::ConnectionCap::detect(),
            explicit_concurrency: None,
            explicit_part_size: None,
            ceiling_override_bytes: None,
        });

        let client = Client::new(config_with(ConcurrencyMode::Auto, Some(p)));

        assert_eq!(client.handle.controller.target(), expected.concurrency);
        assert_eq!(client.handle.part_size_bytes(), expected.part_size);
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_sizes_an_automatic_pool_from_the_resolved_ceiling() {
        // `MemoryConfig::Auto` uses the resolved ceiling.
        let p = profile(None, 4);
        let config = crate::config::Config::builder()
            .sdk_client(aws_smithy_mocks::mock_client!(aws_sdk_s3, []))
            .machine_profile(Some(p))
            .build();

        let client = Client::new(config);
        let configured = client
            .handle
            .buffer_pool
            .metrics()
            .configured_capacity_bytes();

        // Carrier rounding can only round the ceiling down, never above it.
        assert!(configured <= client.handle.plan.ceiling_bytes as u64);
        assert!(configured > 0);
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn configured_throughput_drives_the_resolved_target() {
        // A throughput target sets the controller's target.
        let client = Client::new(config_with(
            ConcurrencyMode::TargetThroughput(100.0),
            Some(profile(None, 4)),
        ));

        // Expected value from the pass, which bounds the 100 Gbps seed by the
        // host's descriptor cap and the fallback ceiling.
        let expected = sizing::resolve(Signals {
            throughput_gbps: Some(100.0),
            instance_type: None,
            vcpus: 4,
            ram_bytes: None,
            connection_cap: platform::ConnectionCap::detect(),
            explicit_concurrency: None,
            explicit_part_size: None,
            ceiling_override_bytes: None,
        });
        assert_eq!(client.handle.controller.target(), expected.concurrency);
        assert_eq!(client.handle.plan.throughput_gbps, Some(100.0));
    }

    #[test]
    fn invalid_throughput_target_reads_as_unset() {
        for gbps in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let config = config_with(ConcurrencyMode::TargetThroughput(gbps), None);
            assert_eq!(config.throughput_target_gbps(), None, "gbps {gbps}");
            assert!(config.ignored_throughput_target_gbps().is_some());
        }
    }

    // FIXME: crossbeam-epoch is incompatible with miri
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_bypass_path_detects_locally() {
        // No profile on the config (built directly, not via the loader):
        // Client::new detects locally. Off EC2 (the test host) DMI yields no
        // instance type, so the resolved target must equal the vCPU fallback for
        // the detected core count — pinning it, so a regression that stops
        // feeding local vCPU into resolution is caught (not just a clamp-range
        // check that holds by construction).
        let expected = sizing::resolve(Signals {
            throughput_gbps: None,
            instance_type: None,
            vcpus: platform::local_vcpus(),
            ram_bytes: platform::available_ram(),
            connection_cap: platform::ConnectionCap::detect(),
            explicit_concurrency: None,
            explicit_part_size: None,
            ceiling_override_bytes: None,
        });
        let client = Client::new(config_with(ConcurrencyMode::Auto, None));
        assert_eq!(client.handle.controller.target(), expected.concurrency);
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn retry_partition_is_cached_per_bucket() {
        // The retry partition (and its refilling token bucket) must be built once
        // per bucket and reused, not rebuilt per operation — a fresh partition
        // would carry a fresh full bucket, defeating the shared budget and refill.
        // The map is the observable of that reuse: repeated calls for one bucket
        // add exactly one entry, and distinct buckets each add their own. A
        // build-fresh-per-call regression never populates the map (stays empty).
        let handle = Handle::new_for_test(test_config(), 2);

        // Same bucket, three calls: one cache entry, populated once.
        for _ in 0..3 {
            let _ = handle.bucket_partition_override(Some("bucket-a"));
        }
        assert_eq!(
            handle.retry_partitions.lock().unwrap().len(),
            1,
            "repeated calls for one bucket must reuse a single cached partition"
        );

        // A second bucket adds its own entry; `None` adds nothing.
        let _ = handle.bucket_partition_override(Some("bucket-b"));
        let _ = handle.bucket_partition_override(None);
        let map = handle.retry_partitions.lock().unwrap();
        assert_eq!(map.len(), 2, "each distinct bucket caches one partition");
        assert!(map.contains_key("bucket-a") && map.contains_key("bucket-b"));
    }

    // --- runtime mode selection ---

    #[test]
    fn config_default_runtime_is_managed() {
        let config = config_with(ConcurrencyMode::Auto, None);
        assert!(config.runtime_config().managed_config().is_some());
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn client_new_with_managed_runtime() {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder()
            .sdk_client(s3_client)
            .runtime_config(RuntimeConfig::default())
            .build();
        let _client = Client::new(config);
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[tokio::test(flavor = "multi_thread")]
    async fn client_new_with_multi_thread_tokio_runtime() {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder()
            .sdk_client(s3_client)
            .runtime_config(RuntimeConfig::multi_thread_tokio())
            .build();
        let _client = Client::new(config);
    }
}
