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

use aws_sdk_s3_transfer_manager::types::RuntimeMode;
use aws_sdk_s3_transfer_manager::Client as TmClient;
use s3_mock_server::S3MockServer;

/// Install a process-global tracing subscriber for e2e diagnosis, once.
///
/// A failing real-S3 test otherwise surfaces only a panic string; this makes the
/// run debuggable from its own logs, no re-run needed. Honors `RUST_LOG`;
/// defaults to a curated filter over the transfer manager's diagnostic targets —
/// the adaptive concurrency target (is it ramping?), scheduler/memory-budget
/// admission (is a reserve parking?), and transfer lifecycle. Deliberately omits
/// the per-work-item `execution` target (768 GETs × chunks is a firehose); reach
/// for it with `RUST_LOG` when a specific run needs it.
///
/// Global (not a thread-local `set_default`) so it captures events from the
/// managed threads and tokio workers the transfer runs on, not just the test
/// thread. Idempotent: the first call wins, later calls are no-ops.
pub(crate) fn init_e2e_logs() {
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
            .try_init();
    });
}

// ---------------------------------------------------------------------------
// Lightweight mock-only path
// ---------------------------------------------------------------------------

/// A transfer-manager client wired to a fresh in-memory mock server.
///
/// The mock-only counterpart to [`Target`]: no bucket-kind / real-S3 / express
/// machinery, just the boilerplate every mock test repeats. Holds the server
/// and handle (for fault injection and direct inspection) alongside the client.
pub(crate) struct MockTm {
    pub(crate) server: S3MockServer,
    pub(crate) handle: s3_mock_server::ServerHandle,
    pub(crate) client: TmClient,
}

/// Build a mock TM on the given runtime with default config.
pub(crate) async fn mock_tm(runtime: RuntimeMode) -> MockTm {
    mock_tm_with(runtime, |b| b).await
}

/// Build a mock TM, applying `configure` to the TM Config builder (part size,
/// concurrency, ...). The runtime mode is set by the harness after `configure`.
pub(crate) async fn mock_tm_with(
    runtime: RuntimeMode,
    configure: impl FnOnce(
        aws_sdk_s3_transfer_manager::config::Builder,
    ) -> aws_sdk_s3_transfer_manager::config::Builder,
) -> MockTm {
    init_e2e_logs();
    let server = S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");
    let s3_client = handle.client().await;
    let cfg = configure(aws_sdk_s3_transfer_manager::Config::builder().client(s3_client))
        .runtime_mode(runtime)
        .build();
    MockTm {
        server,
        handle,
        client: TmClient::new(cfg),
    }
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
    #[cfg(e2e_test)]
    Express,
}

/// A backend/bucket-kind pair a test runs against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Target {
    pub(crate) backend: Backend,
    /// Read only on the real-S3 path to pick the bucket name.
    #[cfg_attr(not(e2e_test), allow(dead_code))]
    pub(crate) bucket_kind: BucketKind,
}

impl Target {
    pub(crate) fn mock_gp() -> Self {
        Self {
            backend: Backend::Mock,
            bucket_kind: BucketKind::GeneralPurpose,
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
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
            )
        })
        .await
    }

    /// Mock-only escape hatch: connect with an arbitrary override applied to the
    /// mock S3 client's config builder, for tests that need a non-default client
    /// (checksum validation, stalled-stream protection, timeouts, ...). Builds the
    /// mock client, applies `configure`, then wires it into the transfer manager.
    pub(crate) async fn connect_mock_configured(
        self,
        part_size: Option<aws_sdk_s3_transfer_manager::types::PartSize>,
        configure: impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder,
    ) -> TmTestClient {
        assert_eq!(self.backend, Backend::Mock, "mock-only helper");
        TmTestClient::connect_mock_with(part_size, Some(configure)).await
    }

    /// Connect, optionally pinning an explicit part size. A pinned size applies
    /// to both uploads and downloads, so multipart download ranges align to the
    /// uploaded part boundaries (the precondition for per-part validation).
    pub(crate) async fn connect_with(
        self,
        part_size: Option<aws_sdk_s3_transfer_manager::types::PartSize>,
    ) -> TmTestClient {
        match self.backend {
            Backend::Mock => TmTestClient::connect_mock(part_size).await,
            #[cfg(e2e_test)]
            Backend::RealS3 => TmTestClient::connect_real(self.bucket_kind, part_size).await,
        }
    }
}

/// A transfer manager wired to a target, plus the bucket and (for mock targets)
/// the server handle used for fault injection and direct inspection.
pub(crate) struct TmTestClient {
    tm: TmClient,
    bucket: String,
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
    async fn connect_mock(part_size: Option<aws_sdk_s3_transfer_manager::types::PartSize>) -> Self {
        Self::connect_mock_with(
            part_size,
            None::<fn(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder>,
        )
        .await
    }

    /// Connect to the mock, optionally pinning a part size and applying an
    /// override to the S3 client's config builder. With no override the mock
    /// client uses SDK defaults (`ResponseChecksumValidation::WhenSupported`,
    /// stalled-stream protection enabled).
    async fn connect_mock_with(
        part_size: Option<aws_sdk_s3_transfer_manager::types::PartSize>,
        configure: Option<impl FnOnce(aws_sdk_s3::config::Builder) -> aws_sdk_s3::config::Builder>,
    ) -> Self {
        init_e2e_logs();
        let server = S3MockServer::builder()
            .with_in_memory_store()
            .build()
            .expect("build mock server");
        let handle = server.start().await.expect("start mock server");
        let mut s3_client = handle.client().await;
        if let Some(configure) = configure {
            let conf = configure(s3_client.config().to_builder()).build();
            s3_client = aws_sdk_s3::Client::from_conf(conf);
        }
        s3_client
            .create_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .ok();
        let mut builder = aws_sdk_s3_transfer_manager::Config::builder().client(s3_client);
        if let Some(ps) = part_size {
            builder = builder.part_size(ps);
        }
        Self {
            tm: TmClient::new(builder.build()),
            bucket: "test-bucket".to_string(),
            key_prefix: format!("it-{}", uuid::Uuid::new_v4()),
            mock: Some(MockBackend { server, handle }),
            #[cfg(e2e_test)]
            s3_client: None,
        }
    }

    #[cfg(e2e_test)]
    async fn connect_real(
        kind: BucketKind,
        part_size: Option<aws_sdk_s3_transfer_manager::types::PartSize>,
    ) -> Self {
        init_e2e_logs();
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
            bucket,
            key_prefix: format!("it-{}", uuid::Uuid::new_v4()),
            mock: None,
            s3_client: Some(s3_client),
        }
    }

    /// The bucket name to target.
    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
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
        checksum_mode: Option<aws_sdk_s3::types::ChecksumMode>,
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
                Ok(chunk) => data.extend_from_slice(&chunk.data.into_bytes()),
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
        checksum_mode: Option<aws_sdk_s3::types::ChecksumMode>,
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
