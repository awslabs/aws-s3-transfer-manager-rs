/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! S3 Mock Server implementation.

use crate::error::{Error, Result};
use crate::faults::{FaultTarget, FaultType, Occurrence};
use crate::s3s::{Inner, ServerState};
use crate::storage::filesystem::FilesystemStorage;
use crate::storage::in_memory::InMemoryStorage;
use crate::storage::StorageBackend;
use crate::Operation;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TEST_ACCESS_KEY: &str = "mock-akid";
const TEST_SECRET_KEY: &str = "mock-secret";

/// Configuration for the S3 Mock Server.
#[derive(Debug, Clone, Default)]
pub struct ServerConfig {
    /// Port to listen on. If None, an available port will be chosen.
    pub port: Option<u16>,
}

/// Handle for a running S3 Mock Server.
pub struct ServerHandle {
    /// Address the server is listening on.
    address: SocketAddr,

    /// Shutdown sender.
    shutdown_tx: oneshot::Sender<()>,

    /// Server task handle.
    server_task: JoinHandle<Result<()>>,
}

impl ServerHandle {
    /// Create a new ServerHandle.
    fn new(
        address: SocketAddr,
        shutdown_tx: oneshot::Sender<()>,
        server_task: JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            address,
            shutdown_tx,
            server_task,
        }
    }

    /// Get the address the server is listening on
    pub fn socket_addr(&self) -> SocketAddr {
        self.address
    }

    /// Shutdown the server.
    pub async fn shutdown(self) -> Result<()> {
        tracing::debug!(addr = %self.address, "shutting down mock server");
        let _ = self.shutdown_tx.send(());
        match self.server_task.await {
            Ok(result) => result,
            Err(err) => Err(Error::Internal(format!("Server task failed: {}", err))),
        }
    }

    /// Load shared AWS configuration for this mock server.
    pub async fn shared_config(&self) -> aws_config::SdkConfig {
        let endpoint_url = format!("http://127.0.0.1:{}", self.address.port());
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                TEST_ACCESS_KEY,
                TEST_SECRET_KEY,
                None,
                None,
                "mock-s3-server",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .load()
            .await
    }

    /// Create an S3 client configured to use this mock server.
    pub async fn client(&self) -> Client {
        let shared_config = self.shared_config().await;
        let config = aws_sdk_s3::config::Builder::from(&shared_config)
            // TODO - we could override the http client with a custom ResolveDns impl to avoid path style
            .force_path_style(true)
            .build();

        Client::from_conf(config)
    }
}

/// Builder for S3MockServer.
pub struct S3MockServerBuilder {
    /// Server configuration.
    config: ServerConfig,

    /// Storage backend.
    storage: Option<Arc<dyn StorageBackend>>,
}

impl Default for S3MockServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl S3MockServerBuilder {
    /// Create a new S3MockServerBuilder.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            storage: None,
        }
    }

    /// Use in-memory storage.
    pub fn with_in_memory_store(mut self) -> Self {
        self.storage = Some(Arc::new(InMemoryStorage::new()));
        self
    }

    // FIXME - replace these storage config options with configuring storage directly (e.g. for specifying cleanup options, etc)
    /// Use filesystem storage with the given path.
    pub async fn with_local_dir_store(mut self, path: impl AsRef<Path>) -> Result<Self> {
        self.storage = Some(Arc::new(FilesystemStorage::new(path).await?));
        Ok(self)
    }

    /// Set the port to listen on.
    pub fn with_port(mut self, port: u16) -> Self {
        self.config.port = Some(port);
        self
    }

    /// Build the S3MockServer.
    pub fn build(self) -> Result<S3MockServer> {
        let storage = self.storage.ok_or_else(|| {
            Error::InvalidConfiguration("Storage backend must be specified".to_string())
        })?;

        Ok(S3MockServer {
            storage,
            state: Arc::new(ServerState::default()),
            connect_reset: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            config: self.config,
        })
    }
}

/// Object data returned from direct storage inspection.
pub struct ObjectData {
    /// The object body.
    pub body: bytes::Bytes,
    /// Content type of the object.
    pub content_type: Option<String>,
    /// Size of the object in bytes.
    pub content_length: u64,
    /// ETag of the object.
    pub etag: String,
    /// Last modified time.
    pub last_modified: std::time::SystemTime,
    /// User-defined metadata.
    pub metadata: std::collections::HashMap<String, String>,
}

/// Summary information about an object in a listing.
pub struct ObjectListEntry {
    /// The object key.
    pub key: String,
    /// Size of the object in bytes.
    pub size: u64,
    /// Last modified time.
    pub last_modified: std::time::SystemTime,
    /// ETag of the object.
    pub etag: String,
}

/// Request for adding an object via the direct (non-S3-protocol) API.
pub struct AddObjectRequest {
    pub content: bytes::Bytes,
    pub content_type: Option<String>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
    pub last_modified: Option<std::time::SystemTime>,
}

impl AddObjectRequest {
    pub fn new(content: impl Into<bytes::Bytes>) -> Self {
        Self {
            content: content.into(),
            content_type: None,
            metadata: None,
            last_modified: None,
        }
    }

    pub fn content_type(mut self, ct: impl Into<String>) -> Self {
        self.content_type = Some(ct.into());
        self
    }

    pub fn metadata(mut self, meta: std::collections::HashMap<String, String>) -> Self {
        self.metadata = Some(meta);
        self
    }

    pub fn last_modified(mut self, time: std::time::SystemTime) -> Self {
        self.last_modified = Some(time);
        self
    }
}

/// S3 Mock Server.
pub struct S3MockServer {
    /// Storage backend.
    storage: Arc<dyn StorageBackend>,

    /// Faults, throttle and request counters (shared with the serving task).
    state: Arc<ServerState>,

    /// Connect-time reset: number of freshly accepted connections to abort (RST)
    /// immediately, before serving any request. Server-scoped because no request
    /// exists at connect time; decremented per reset. Shared with the serving task.
    connect_reset: Arc<std::sync::atomic::AtomicU64>,

    /// Server configuration.
    config: ServerConfig,
}

impl S3MockServer {
    /// Create a new S3MockServerBuilder.
    pub fn builder() -> S3MockServerBuilder {
        S3MockServerBuilder::new()
    }

    /// Add an object to the mock server storage.
    pub async fn add_object(
        &self,
        bucket: &str,
        key: &str,
        content: impl Into<bytes::Bytes>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<()> {
        let mut req = AddObjectRequest::new(content);
        req.metadata = metadata;
        self.add_object_with(bucket, key, req).await
    }

    /// Add an object with full control over metadata fields.
    pub async fn add_object_with(
        &self,
        bucket: &str,
        key: &str,
        request: AddObjectRequest,
    ) -> Result<()> {
        use crate::storage::StoreObjectRequest;
        use crate::types::ObjectIntegrityChecks;
        use futures::stream;

        let bytes = request.content;
        let stream = stream::once(async move { Ok(bytes) });
        let boxed_stream = Box::pin(stream);

        // Match HTTP PutObject path's default integrity checks so seeded
        // objects have the same HeadObject state (ETag via md5, default
        // CRC64NVME checksum) as objects uploaded via the S3 API.
        let integrity_checks = ObjectIntegrityChecks::new().with_md5().with_crc64nvme();

        let mut store_req =
            StoreObjectRequest::new(bucket, key.to_string(), boxed_stream, integrity_checks)
                .with_user_metadata(request.metadata.unwrap_or_default());
        store_req.content_type = request.content_type;
        store_req.last_modified = request.last_modified;

        self.storage.put_object(store_req).await?;
        Ok(())
    }

    /// Create a bucket in the mock server.
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.storage.create_bucket(bucket).await
    }

    /// Register a fault for the GetObject, PutObject and UploadPart requests for
    /// `(bucket, key)`; the same as [`insert_fault_for`](Self::insert_fault_for)
    /// with [`FaultTarget::object`].
    pub fn insert_fault(
        &self,
        bucket: &str,
        key: &str,
        fault: FaultType,
        skip: u32,
        occurrence: Occurrence,
    ) {
        self.insert_fault_for(FaultTarget::object(bucket, key), fault, skip, occurrence);
    }

    /// Register a fault for the requests that `target` matches. The faults of a
    /// target form an ordered queue consumed over successive matching requests:
    /// the first `skip` matching requests pass cleanly, then the fault fires per
    /// `occurrence`. `skip` is compared with the number of matching requests the
    /// target has seen since it was first registered, or last cleared with
    /// [`clear_fault`](Self::clear_fault) or [`clear_faults`](Self::clear_faults),
    /// not since this call. When several targets match a request, each counts it,
    /// and only the first target registered whose next fault is due fires.
    ///
    /// A fault is drawn when its handler starts, before the request is validated
    /// or served, so a request that then fails (for example with NoSuchKey) still
    /// consumes it. A request the rate throttle fails with SlowDown draws no
    /// fault and is not seen by any target. A fault that does not apply to the
    /// operation, such as a body fault on HeadObject, is consumed without effect.
    /// Firing is deterministic; every fire logs the request number under
    /// `target: "s3_mock_server::fault"`.
    pub fn insert_fault_for(
        &self,
        target: FaultTarget,
        fault: FaultType,
        skip: u32,
        occurrence: Occurrence,
    ) {
        self.state.faults.insert(target, fault, skip, occurrence);
    }

    /// Drop the entire fault queue for `(bucket, key)`, as registered by
    /// [`insert_fault`](Self::insert_fault).
    pub fn clear_fault(&self, bucket: &str, key: &str) {
        self.state.faults.clear(&FaultTarget::object(bucket, key));
    }

    /// Drop the fault queues of every target.
    pub fn clear_faults(&self) {
        self.state.faults.clear_all();
    }

    /// Number of requests received for `operation`, including throttled and
    /// failed ones. A request is counted when its handler starts; requests that
    /// fail authentication or request parsing are not counted.
    pub fn request_count(&self, operation: Operation) -> u64 {
        self.state.requests.get(operation)
    }

    /// Number of requests received per operation, for every operation with at
    /// least one request. Counted as [`request_count`](Self::request_count) counts.
    pub fn request_counts(&self) -> BTreeMap<Operation, u64> {
        self.state.requests.snapshot()
    }

    /// Reset every request count to zero.
    pub fn reset_request_counts(&self) {
        self.state.requests.reset();
    }

    /// Install a server-wide load-driven throttle admitting a sustained `rate`
    /// requests/sec with a `burst` allowance; a shed request returns 503
    /// `SlowDown`. Checked on every S3 operation except CreateBucket, DeleteBucket,
    /// HeadBucket and ListBuckets, before touching storage, and replaces any
    /// previously installed throttle.
    pub fn set_rate_throttle(&self, rate: f64, burst: f64) {
        *self.state.throttle.lock().unwrap() =
            Some(Arc::new(crate::throttle::RateThrottle::new(rate, burst)));
    }

    /// Abort (RST) the next `count` freshly accepted connections immediately,
    /// before serving any request, simulating a connect-time connection reset.
    /// Server-scoped because no request exists at connect time.
    pub fn reset_next_connections(&self, count: u64) {
        self.connect_reset
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if an object exists.
    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool> {
        Ok(self.storage.head_object(bucket, key).await?.is_some())
    }

    /// Get object content and metadata directly from storage.
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectData>> {
        use crate::storage::GetObjectRequest;
        use futures::StreamExt;

        let request = GetObjectRequest {
            bucket,
            key,
            range: None,
        };
        let response = match self.storage.get_object(request).await? {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut body = Vec::new();
        let mut stream = response.stream;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| Error::Internal(format!("Stream error: {}", e)))?;
            body.extend_from_slice(&chunk);
        }

        Ok(Some(ObjectData {
            body: bytes::Bytes::from(body),
            content_type: response.metadata.content_type,
            content_length: response.metadata.content_length,
            etag: response.metadata.etag,
            last_modified: response.metadata.last_modified,
            metadata: response.metadata.user_metadata,
        }))
    }

    /// List objects in a bucket with optional prefix.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectListEntry>> {
        use crate::storage::ListObjectsRequest;

        let request = ListObjectsRequest { bucket, prefix };
        let response = self.storage.list_objects(request).await?;
        Ok(response
            .objects
            .into_iter()
            .map(|o| ObjectListEntry {
                key: o.key,
                size: o.metadata.content_length,
                last_modified: o.metadata.last_modified,
                etag: o.metadata.etag,
            })
            .collect())
    }

    /// Delete an object.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.storage.delete_object(bucket, key).await
    }

    /// Remove all buckets, objects and in-flight uploads. Faults, the throttle and request
    /// counts are kept.
    pub async fn reset(&self) -> Result<()> {
        self.storage.reset().await
    }

    /// Start the server.
    pub async fn start(&self) -> Result<ServerHandle> {
        // Create the address to bind to
        // If port is not specified, use port 0 to get an available port
        let addr_str = format!("127.0.0.1:{}", self.config.port.unwrap_or(0));

        let listener = TcpListener::bind(&addr_str)
            .await
            .map_err(|e| Error::Internal(format!("Failed to bind to address: {}", e)))?;

        // Get the actual address we're bound to
        let addr = listener
            .local_addr()
            .map_err(|e| Error::Internal(format!("Failed to get local address: {}", e)))?;

        tracing::info!("S3MockServer listening on {}", addr);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let storage = self.storage.clone();
        let state = self.state.clone();
        let connect_reset = self.connect_reset.clone();
        let server_task = tokio::spawn(async move {
            let http_server = ConnBuilder::new(TokioExecutor::new());
            let graceful = hyper_util::server::graceful::GracefulShutdown::new();

            let inner = Inner::with_state(storage, state);
            let service = {
                let mut b = S3ServiceBuilder::new(inner);
                b.set_auth(SimpleAuth::from_single(TEST_ACCESS_KEY, TEST_SECRET_KEY));
                b.build()
            };
            loop {
                let (socket, peer) = tokio::select! {
                        res =  listener.accept() => {
                            match res {
                                Ok(conn) => conn,
                                Err(err) => {
                                    tracing::error!("error accepting connection: {err}");
                                    continue;
                                }
                            }
                        }
                        _ =  &mut shutdown_rx => {
                            tracing::debug!("shutdown signal received, breaking accept loop");
                            break;
                        }
                };
                tracing::trace!(port = %addr.port(), %peer, "accepted connection");

                // Connect-time reset: abort this connection immediately (RST)
                // before serving, if armed. Decrement the remaining count.
                {
                    use std::sync::atomic::Ordering;
                    let remaining = connect_reset.load(Ordering::Relaxed);
                    if remaining > 0
                        && connect_reset
                            .compare_exchange(
                                remaining,
                                remaining - 1,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                    {
                        let _ = socket.set_zero_linger();
                        drop(socket);
                        continue;
                    }
                }

                // Per-connection fault control: the socket wrapper reads it, the
                // service wrapper injects it into each request's extensions so the
                // handler can arm it for this connection.
                let fault = Arc::new(crate::socket_fault::ConnectionFault::new());
                let socket = crate::socket_fault::AbortAfterWrite::new(socket, fault.clone());
                let service =
                    crate::socket_fault::InjectConnectionFault::new(service.clone(), fault);
                let conn = http_server
                    .serve_connection(TokioIo::new(socket), service)
                    .into_owned();
                let conn = graceful.watch(conn);
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::trace!("connection error: {e}");
                    }
                });
            }

            tokio::select! {
                () = graceful.shutdown() => {
                     tracing::debug!("Gracefully shutdown!");
                },
                () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
                     tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
                }
            }

            tracing::info!("server is stopped");
            Ok(())
        });

        // Return the server handle
        Ok(ServerHandle::new(addr, shutdown_tx, server_task))
    }
}
