/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Test harness for driving the transfer manager against a backend.
//!
//! Two ways to get a test client:
//!
//! - [`mock_tm`] / [`mock_tm_with`] — lightweight, mock-only. The default for
//!   tests that only need a mock server (download_objects, upload_objects, etc.).
//!
//! - [`Target`] — the mock x real-S3 x bucket-kind matrix, for tests that must
//!   run against real S3 (integrity, etc.). [`Target::connect`] yields a
//!   [`TmTestClient`] that exposes a transfer manager wired to that backend plus
//!   the bucket name to use.
//!
//! Real-S3 targets are compiled only under `--cfg e2e_test` and require the
//! account setup the existing e2e tests use (`S3_TEST_BUCKET_NAME_RS`). Mock
//! targets run in normal CI.
//!
//! Both paths let the transfer manager build its S3 client from an
//! [`S3ClientConfig`], as it does by default in production, so requests use the
//! HTTP transport the runtime installs (the managed runtime's connection pool)
//! rather than a client supplied by the test.
//!
//! [`S3ClientConfig`]: aws_sdk_s3_transfer_manager::config::S3ClientConfig

use aws_sdk_s3_transfer_manager::config::RuntimeConfig;
use aws_sdk_s3_transfer_manager::config::S3ClientConfig;
use aws_sdk_s3_transfer_manager::Client as TmClient;
use s3_mock_server::S3MockServer;

/// Installs a process-global tracing subscriber for test diagnosis, once.
///
/// Libtest captures events and prints them when a test fails or output capture
/// is disabled. `RUST_LOG` overrides the curated default. The default omits the
/// per-work-item `execution` target; enable it with `RUST_LOG` when needed.
///
/// Global (not a thread-local `set_default`) so it captures events from the
/// managed threads and tokio workers the transfer runs on, not just the test
/// thread. Idempotent: the first call wins, later calls are no-ops.
pub(crate) fn init_test_logs() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        const DEFAULT_FILTER: &str = "info,\
            aws_sdk_s3_transfer_manager::concurrency=debug,\
            aws_sdk_s3_transfer_manager::scheduling=debug,\
            aws_sdk_s3_transfer_manager::transfer=debug";

        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_FILTER));
        // try_init: another test (or the process) may have set a subscriber
        // already; a lost race is fine, the winner honors the same env.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_test_writer()
            .try_init();
    });
}

// ---------------------------------------------------------------------------
// Lightweight mock-only path
// ---------------------------------------------------------------------------

/// A transfer-manager client wired to a fresh in-memory mock server, with a bucket named
/// `test-bucket`.
///
/// The mock-only counterpart to [`Target`]: no bucket-kind / real-S3 / express
/// machinery, just the boilerplate every mock test repeats. Holds the server
/// and handle (for fault injection and direct inspection) alongside the client.
pub(crate) struct MockTm {
    pub(crate) server: S3MockServer,
    pub(crate) handle: s3_mock_server::ServerHandle,
    pub(crate) client: TmClient,
}

impl MockTm {
    /// Returns the bytes the mock server stores for `bucket`/`key`, read from its storage rather
    /// than through a GetObject request.
    ///
    /// Panics if no object is stored there or storage cannot be read.
    pub(crate) async fn stored_object(&self, bucket: &str, key: &str) -> Vec<u8> {
        self.server
            .get_object(bucket, key)
            .await
            .expect("read object from mock storage")
            .unwrap_or_else(|| panic!("no object stored at {bucket}/{key}"))
            .body
            .to_vec()
    }
}

/// Build a mock TM on the given runtime with default config.
pub(crate) async fn mock_tm(runtime: RuntimeConfig) -> MockTm {
    mock_tm_with(runtime, |b| b).await
}

/// Build a mock TM, applying `configure` to the TM Config builder (part size,
/// concurrency, ...). The runtime mode is set by the harness after `configure`.
pub(crate) async fn mock_tm_with(
    runtime: RuntimeConfig,
    configure: impl FnOnce(
        aws_sdk_s3_transfer_manager::config::Builder,
    ) -> aws_sdk_s3_transfer_manager::config::Builder,
) -> MockTm {
    mock_tm_with_s3_config(runtime, |shared| shared, configure).await
}

/// Build a mock TM, applying `configure_shared` to the mock server's shared
/// configuration, from which the transfer manager builds its S3 client, and
/// `configure` to the TM Config builder.
pub(crate) async fn mock_tm_with_s3_config(
    runtime: RuntimeConfig,
    configure_shared: impl FnOnce(aws_types::sdk_config::Builder) -> aws_types::sdk_config::Builder,
    configure: impl FnOnce(
        aws_sdk_s3_transfer_manager::config::Builder,
    ) -> aws_sdk_s3_transfer_manager::config::Builder,
) -> MockTm {
    init_test_logs();
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");
    server
        .create_bucket("test-bucket")
        .await
        .expect("create bucket");
    let s3_config = mock_s3_config(&handle, configure_shared).await;
    let cfg = configure(aws_sdk_s3_transfer_manager::Config::builder().s3_config(s3_config))
        .runtime_config(runtime)
        .build()
        .expect("valid config");
    MockTm {
        server,
        handle,
        client: TmClient::new(cfg),
    }
}

/// S3 client configuration for the mock server: its endpoint, test
/// credentials, region, and path-style addressing, with `configure_shared`
/// applied to the shared configuration. The transfer manager builds the client
/// from it and, unless `configure_shared` sets an HTTP client, installs the
/// runtime's HTTP transport.
async fn mock_s3_config(
    handle: &s3_mock_server::ServerHandle,
    configure_shared: impl FnOnce(aws_types::sdk_config::Builder) -> aws_types::sdk_config::Builder,
) -> S3ClientConfig {
    let shared = configure_shared(handle.shared_config().await.into_builder()).build();
    S3ClientConfig::new(&shared)
}

// ---------------------------------------------------------------------------
// Target-based path (mock x real-S3 x bucket-kind matrix)
// ---------------------------------------------------------------------------

/// Which backend serves S3 requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Backend {
    /// In-process `s3-mock-server` over a local HTTP socket.
    Mock,
    /// Real S3. Compiled only under `--cfg e2e_test`.
    #[cfg(e2e_test)]
    RealS3,
}

/// Which kind of bucket the target uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BucketKind {
    GeneralPurpose,
    /// A directory bucket. The mock treats a bucket whose name ends in `--x-s3` as one.
    Express,
}

/// A backend/bucket-kind pair a test runs against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Target {
    pub(crate) backend: Backend,
    pub(crate) bucket_kind: BucketKind,
}

impl Target {
    pub(crate) fn mock_gp() -> Self {
        Self {
            backend: Backend::Mock,
            bucket_kind: BucketKind::GeneralPurpose,
        }
    }

    pub(crate) fn mock_express() -> Self {
        Self {
            backend: Backend::Mock,
            bucket_kind: BucketKind::Express,
        }
    }

    #[cfg(e2e_test)]
    pub(crate) fn real_gp() -> Self {
        Self {
            backend: Backend::RealS3,
            bucket_kind: BucketKind::GeneralPurpose,
        }
    }

    #[cfg(e2e_test)]
    pub(crate) fn real_express() -> Self {
        Self {
            backend: Backend::RealS3,
            bucket_kind: BucketKind::Express,
        }
    }

    /// Connect to the backend with the TM's default part size.
    pub(crate) async fn connect(self) -> TmTestClient {
        self.connect_with(None).await
    }

    /// Mock-only: connect with the S3 client's `ResponseChecksumValidation` set to
    /// `WhenRequired`, so the SDK does NOT auto-enable `ChecksumMode` and an
    /// unset-mode download resolves to validation disabled.
    pub(crate) async fn connect_mock_when_required(self) -> TmTestClient {
        self.connect_mock_configured(None, |b| {
            b.response_checksum_validation(
                aws_smithy_types::checksum_config::ResponseChecksumValidation::WhenRequired,
            )
        })
        .await
    }

    /// Mock-only escape hatch: connect with an arbitrary override applied to the
    /// mock shared-config builder, for tests that need a non-default client
    /// (checksum validation, stalled-stream protection, timeouts, ...). Applies
    /// `configure` to the shared config, which the transfer manager then
    /// builds its S3 client from.
    pub(crate) async fn connect_mock_configured(
        self,
        part_size: Option<aws_sdk_s3_transfer_manager::config::PartSize>,
        configure: impl FnOnce(aws_types::sdk_config::Builder) -> aws_types::sdk_config::Builder,
    ) -> TmTestClient {
        assert_eq!(self.backend, Backend::Mock, "mock-only helper");
        TmTestClient::connect_mock_with(self.bucket_kind, part_size, Some(configure)).await
    }

    /// Connect, optionally pinning an explicit part size. A pinned size applies
    /// to both uploads and downloads, so multipart download ranges align to the
    /// uploaded part boundaries (the precondition for per-part validation).
    pub(crate) async fn connect_with(
        self,
        part_size: Option<aws_sdk_s3_transfer_manager::config::PartSize>,
    ) -> TmTestClient {
        match self.backend {
            Backend::Mock => TmTestClient::connect_mock(self.bucket_kind, part_size).await,
            #[cfg(e2e_test)]
            Backend::RealS3 => TmTestClient::connect_real(self.bucket_kind, part_size).await,
        }
    }
}

/// The number of requests in flight a test client is limited to when the test sets none: four
/// under `--cfg s3_tm_tsan`, and otherwise `None`, the transfer manager's automatic concurrency.
///
/// TSan instruments both the client and the in-process mock server. The automatic concurrency's
/// 32-request floor can then starve request bodies on the four-vCPU hosted runner until
/// stalled-stream protection fires. Four requests keep transfers concurrent without making
/// sanitizer overhead look like a transport stall.
fn default_concurrency() -> Option<usize> {
    if cfg!(s3_tm_tsan) {
        Some(4)
    } else {
        None
    }
}

/// Settings for a transfer manager client on a target's backend, from
/// [`TmTestClient::client_with`]. The default is the transfer manager's and the SDK's defaults.
#[derive(Debug, Clone, Default)]
pub(crate) struct ClientOptions {
    /// `Config::part_size`, in bytes.
    pub(crate) part_size: Option<u64>,
    /// `Config::multipart_threshold`, in bytes.
    pub(crate) multipart_threshold: Option<u64>,
    /// `ConcurrencyMode::Explicit` with this many requests.
    pub(crate) concurrency: Option<usize>,
    /// `ResponseChecksumValidation::WhenRequired`: a download that doesn't set a checksum mode
    /// is not validated.
    pub(crate) response_checksums_when_required: bool,
    /// `RequestChecksumCalculation::WhenRequired`: an upload without a checksum strategy sends
    /// no checksum.
    pub(crate) request_checksums_when_required: bool,
    /// Records every request attempt the client's S3 client sends.
    pub(crate) recorder: Option<crate::test_util::recorder::OperationRecorder>,
}

/// A transfer manager wired to a target, plus the bucket and (for mock targets)
/// the server handle used for fault injection and direct inspection.
pub(crate) struct TmTestClient {
    tm: TmClient,
    /// The configuration `tm`'s S3 client was built from, before any test override.
    shared_config: aws_types::SdkConfig,
    bucket: String,
    bucket_kind: BucketKind,
    /// A per-instance key prefix so concurrent tests sharing one (real) bucket do
    /// not collide on object keys. Each `connect()` gets a fresh prefix; callers
    /// pass logical keys (e.g. `"obj"`) and the harness namespaces them.
    key_prefix: String,
    /// Present for mock targets. Owns the server; held until `shutdown`.
    mock: Option<MockBackend>,
    /// Present for real-S3 targets; used for direct SDK calls (e.g. HeadObject).
    #[cfg(e2e_test)]
    s3_client: Option<aws_sdk_s3::Client>,
}

struct MockBackend {
    server: S3MockServer,
    handle: s3_mock_server::ServerHandle,
}

impl TmTestClient {
    async fn connect_mock(
        kind: BucketKind,
        part_size: Option<aws_sdk_s3_transfer_manager::config::PartSize>,
    ) -> Self {
        Self::connect_mock_with(
            kind,
            part_size,
            None::<fn(aws_types::sdk_config::Builder) -> aws_types::sdk_config::Builder>,
        )
        .await
    }

    /// Connect to the mock, optionally pinning a part size and applying an
    /// override to the shared-config builder. With no override the mock
    /// client uses SDK defaults (`ResponseChecksumValidation::WhenSupported`,
    /// stalled-stream protection enabled).
    async fn connect_mock_with(
        kind: BucketKind,
        part_size: Option<aws_sdk_s3_transfer_manager::config::PartSize>,
        configure: Option<
            impl FnOnce(aws_types::sdk_config::Builder) -> aws_types::sdk_config::Builder,
        >,
    ) -> Self {
        init_test_logs();
        let server = S3MockServer::builder()
            .with_in_memory_store()
            .with_request_journal()
            .build()
            .expect("build mock server");
        let handle = server.start().await.expect("start mock server");
        let base_config = handle.shared_config().await;
        let mut shared_config = base_config.clone();
        if let Some(configure) = configure {
            shared_config = configure(shared_config.into_builder()).build();
        }
        let bucket = match kind {
            BucketKind::GeneralPurpose => "test-bucket",
            BucketKind::Express => "test-bucket--usw2-az1--x-s3",
        };
        server.create_bucket(bucket).await.expect("create bucket");
        let mut builder = aws_sdk_s3_transfer_manager::Config::builder().s3_config(&shared_config);
        if let Some(ps) = part_size {
            builder = builder.part_size(ps);
        }
        if let Some(n) = default_concurrency() {
            builder = builder
                .concurrency(aws_sdk_s3_transfer_manager::config::ConcurrencyMode::Explicit(n));
        }
        Self {
            tm: TmClient::new(builder.build().expect("valid config")),
            shared_config: base_config,
            bucket: bucket.to_string(),
            bucket_kind: kind,
            key_prefix: format!("it-{}", uuid::Uuid::new_v4()),
            mock: Some(MockBackend { server, handle }),
            #[cfg(e2e_test)]
            s3_client: None,
        }
    }

    #[cfg(e2e_test)]
    async fn connect_real(
        kind: BucketKind,
        part_size: Option<aws_sdk_s3_transfer_manager::config::PartSize>,
    ) -> Self {
        init_test_logs();
        let bucket_name = option_env!("S3_TEST_BUCKET_NAME_RS")
            .unwrap_or("aws-s3-transfer-manager-rs-test-bucket")
            .to_owned();
        let bucket = match kind {
            BucketKind::GeneralPurpose => bucket_name,
            BucketKind::Express => format!("{bucket_name}--usw2-az1--x-s3"),
        };
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let s3_client = aws_sdk_s3::Client::new(&shared_config);
        let mut loader = aws_sdk_s3_transfer_manager::from_env();
        if let Some(ps) = part_size {
            loader = loader.part_size(ps);
        }
        Self {
            tm: TmClient::new(loader.load().await),
            shared_config,
            bucket,
            bucket_kind: kind,
            key_prefix: format!("it-{}", uuid::Uuid::new_v4()),
            mock: None,
            s3_client: Some(s3_client),
        }
    }

    /// The bucket name to target.
    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The kind of bucket [`bucket`](Self::bucket) is.
    pub(crate) fn bucket_kind(&self) -> BucketKind {
        self.bucket_kind
    }

    /// A new transfer manager client on this target's backend, with `options` applied.
    ///
    /// Built from the backend's base SDK configuration: the override and part size given to
    /// [`Target::connect_mock_configured`] or [`Target::connect_with`] don't apply to it. Without
    /// `options.concurrency`, it uses [`default_concurrency`].
    pub(crate) fn client_with(&self, options: ClientOptions) -> TmClient {
        use aws_sdk_s3_transfer_manager::config::{ConcurrencyMode, PartSize};
        use aws_sdk_s3_transfer_manager::types::ByteSize;
        let mut shared_config = self.shared_config.clone().into_builder();
        if options.response_checksums_when_required {
            shared_config.set_response_checksum_validation(Some(
                aws_smithy_types::checksum_config::ResponseChecksumValidation::WhenRequired,
            ));
        }
        if options.request_checksums_when_required {
            shared_config.set_request_checksum_calculation(Some(
                aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired,
            ));
        }
        let mut s3_config = aws_sdk_s3::config::Builder::from(&shared_config.build());
        if let Some(recorder) = options.recorder {
            s3_config = s3_config.interceptor(recorder);
        }
        let mut builder = aws_sdk_s3_transfer_manager::Config::builder().s3_config(
            aws_sdk_s3_transfer_manager::config::S3ClientConfig::new(s3_config),
        );
        if let Some(part_size) = options.part_size {
            builder = builder.part_size(PartSize::Target(ByteSize::new(part_size)));
        }
        if let Some(threshold) = options.multipart_threshold {
            builder = builder.multipart_threshold(PartSize::Target(ByteSize::new(threshold)));
        }
        if let Some(n) = options.concurrency.or(default_concurrency()) {
            builder = builder.concurrency(ConcurrencyMode::Explicit(n));
        }
        TmClient::new(builder.build().expect("valid config"))
    }

    /// An S3 client on this target's backend, for requests the transfer manager doesn't make.
    pub(crate) async fn s3_client(&self) -> aws_sdk_s3::Client {
        match &self.mock {
            Some(mock) => mock.handle.client().await,
            #[cfg(e2e_test)]
            None => self.s3().clone(),
            #[cfg(not(e2e_test))]
            None => unreachable!("every target is a mock without e2e_test"),
        }
    }

    /// Namespace a logical key with this instance's unique prefix so concurrent
    /// tests sharing one bucket do not collide. Keys live under `upload/` so the
    /// test bucket's lifecycle expiration policy reaps the objects (the same
    /// prefix the legacy e2e tests use; `pre-existing` fixtures are deliberately
    /// outside it and not reaped).
    pub(crate) fn key(&self, key: &str) -> String {
        format!("upload/{}/{}", self.key_prefix, key)
    }

    /// The mock server, for fault injection and direct inspection. `None` on a
    /// real-S3 target (faults cannot be injected into real S3).
    pub(crate) fn mock(&self) -> Option<&S3MockServer> {
        self.mock.as_ref().map(|m| &m.server)
    }

    /// The raw S3 client for direct SDK calls (e.g. HeadObject). Only available
    /// on real-S3 targets.
    #[cfg(e2e_test)]
    pub(crate) fn s3(&self) -> &aws_sdk_s3::Client {
        self.s3_client
            .as_ref()
            .expect("s3() requires a real-S3 target")
    }

    /// HeadObject of part `part_number` of `key`, in checksum mode.
    pub(crate) async fn head_part(
        &self,
        key: &str,
        part_number: i32,
    ) -> aws_sdk_s3::operation::head_object::HeadObjectOutput {
        self.s3_client()
            .await
            .head_object()
            .bucket(&self.bucket)
            .key(self.key(key))
            .part_number(part_number)
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "HeadObject of part {part_number} of {key}: {}",
                    crate::assertions::describe_sdk_error(&e)
                )
            })
    }

    /// Upload `data` under `key` with the given checksum strategy.
    pub(crate) async fn put(
        &self,
        key: &str,
        data: Vec<u8>,
        strategy: aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy,
    ) {
        self.tm
            .upload()
            .bucket(&self.bucket)
            .key(self.key(key))
            .checksum_strategy(strategy)
            .body(aws_sdk_s3_transfer_manager::io::InputStream::from(data))
            .initiate()
            .expect("initiate upload")
            .join()
            .await
            .expect("upload complete");
    }

    /// Upload the file at `path` under `key`, streaming from disk via
    /// `InputStream::from_path` (the body is never held in memory). Use this for
    /// large objects instead of `put`, which takes an in-memory buffer.
    pub(crate) async fn put_from_path(
        &self,
        key: &str,
        path: &std::path::Path,
        strategy: aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy,
    ) {
        let body = aws_sdk_s3_transfer_manager::io::InputStream::from_path(path)
            .expect("open upload source file");
        self.tm
            .upload()
            .bucket(&self.bucket)
            .key(self.key(key))
            .checksum_strategy(strategy)
            .body(body)
            .initiate()
            .expect("initiate upload")
            .join()
            .await
            .expect("upload complete");
    }

    /// Download `key`, fully draining the body. Returns the bytes and the output
    /// (carrying `integrity_checks()`). `checksum_mode` controls whether the
    /// request asks S3 to return and the SDK to validate stored checksums.
    pub(crate) async fn download(
        &self,
        key: &str,
        checksum_mode: Option<aws_sdk_s3_transfer_manager::types::ChecksumMode>,
    ) -> Result<
        (
            Vec<u8>,
            aws_sdk_s3_transfer_manager::operation::download::DownloadOutput,
        ),
        aws_sdk_s3_transfer_manager::error::Error,
    > {
        let mut builder = self.tm.download().bucket(&self.bucket).key(self.key(key));
        if let Some(mode) = checksum_mode {
            builder = builder.checksum_mode(mode);
        }
        let mut handle = builder.initiate().expect("initiate download");

        let mut data = Vec::new();
        while let Some(chunk) = handle.body_mut().next().await {
            match chunk {
                Ok(chunk) => data.extend_from_slice(&chunk.data.into_contiguous()),
                Err(e) => {
                    // Drive the transfer to terminal so join() surfaces the real error.
                    return Err(handle.join().await.err().unwrap_or(e));
                }
            }
        }
        let output = handle.join().await?;
        Ok((data, output))
    }

    /// Download `key` to a file via the managed file-sink path (writes to a
    /// `.s3tmp.` temp file, atomically renamed to `dest` on success; the temp is
    /// removed and `dest` is never created on failure). This exercises the
    /// validate-before-rename contract that the in-memory `download` path does
    /// not. Returns the join result so callers can assert success or error.
    pub(crate) async fn download_to_path(
        &self,
        key: &str,
        dest: &std::path::Path,
        checksum_mode: Option<aws_sdk_s3_transfer_manager::types::ChecksumMode>,
    ) -> Result<
        aws_sdk_s3_transfer_manager::operation::download::DownloadOutput,
        aws_sdk_s3_transfer_manager::error::Error,
    > {
        let mut builder = self.tm.download().bucket(&self.bucket).key(self.key(key));
        if let Some(mode) = checksum_mode {
            builder = builder.checksum_mode(mode);
        }
        builder.write_to_path(dest).await?.join().await
    }

    /// Release backend resources. Shuts the mock server down; a no-op for real S3.
    pub(crate) async fn shutdown(self) {
        if let Some(m) = self.mock {
            m.handle.shutdown().await.expect("shutdown mock server");
        }
    }
}

/// Declares a test function per target for each listed test body.
///
/// `target_tests! { name: [mock_gp, real_gp]; ... }` declares a module `name` holding one
/// `#[tokio::test]` per listed target, named after the target, that runs
/// `name(Target::<target>())`. The `real_*` functions are compiled only under `--cfg e2e_test`.
/// So the test `mpu_intact` on the mock's directory bucket is `...::mpu_intact::mock_express`.
///
/// Attributes written first as `#![...]` apply to every generated test, as in
/// `target_tests! { #![cfg_attr(s3_tm_tsan, ignore = "<reason>")] name: [mock_gp]; }`.
macro_rules! target_tests {
    ($(#![$attr:meta])* $( $name:ident: [$($target:ident),+ $(,)?]; )+) => {
        $crate::harness::target_tests!(@mods [$(#[$attr])*] $( $name: [$($target),+]; )+);
    };
    (@mods $attrs:tt $( $name:ident: [$($target:ident),+]; )+) => {
        $(
            mod $name {
                $( $crate::harness::target_tests!(@fn $attrs $name $target); )+
            }
        )+
    };
    (@fn [$($attrs:tt)*] $name:ident mock_gp) => {
        #[tokio::test(flavor = "multi_thread")]
        $($attrs)*
        async fn mock_gp() {
            super::$name($crate::harness::Target::mock_gp()).await;
        }
    };
    (@fn [$($attrs:tt)*] $name:ident mock_express) => {
        #[tokio::test(flavor = "multi_thread")]
        $($attrs)*
        async fn mock_express() {
            super::$name($crate::harness::Target::mock_express()).await;
        }
    };
    (@fn [$($attrs:tt)*] $name:ident real_gp) => {
        #[cfg(e2e_test)]
        #[tokio::test(flavor = "multi_thread")]
        $($attrs)*
        async fn real_gp() {
            super::$name($crate::harness::Target::real_gp()).await;
        }
    };
    (@fn [$($attrs:tt)*] $name:ident real_express) => {
        #[cfg(e2e_test)]
        #[tokio::test(flavor = "multi_thread")]
        $($attrs)*
        async fn real_express() {
            super::$name($crate::harness::Target::real_express()).await;
        }
    };
}
pub(crate) use target_tests;
