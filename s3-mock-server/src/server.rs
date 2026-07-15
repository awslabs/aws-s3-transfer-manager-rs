/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! S3 Mock Server implementation.

use crate::error::{Error, Result};
use crate::s3s::Inner;
use crate::storage::filesystem::FilesystemStorage;
use crate::storage::in_memory::InMemoryStorage;
use crate::storage::StorageBackend;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
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

    /// Create an S3 client configured to use this mock server.
    pub async fn client(&self) -> Client {
        let endpoint_url = format!("http://127.0.0.1:{}", self.address.port());
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
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
            .await;

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
            faults: Arc::new(crate::faults::FaultRegistry::default()),
            connect_reset: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            throttle: Arc::new(Mutex::new(None)),
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

    /// Key-scoped fault injection registry (shared with the serving task).
    faults: Arc<crate::faults::FaultRegistry>,

    /// Connect-time reset: number of freshly accepted connections to abort (RST)
    /// immediately, before serving any request. Server-scoped because no request
    /// exists at connect time; decremented per reset. Shared with the serving task.
    connect_reset: Arc<std::sync::atomic::AtomicU64>,

    /// Server-wide load-driven throttle. Shared with the serving task; `None`
    /// until one is installed via [`set_rate_throttle`](Self::set_rate_throttle).
    throttle: Arc<Mutex<Option<Arc<crate::throttle::RateThrottle>>>>,

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

    /// Register a fault for `(bucket, key)`. Faults form an ordered queue
    /// consumed over successive matching requests: the first `skip` matching
    /// requests pass cleanly, then the fault fires per `occurrence`. Firing is
    /// deterministic; every fire logs the request number under
    /// `target: "s3_mock_server::fault"`.
    pub fn insert_fault(
        &self,
        bucket: &str,
        key: &str,
        fault: crate::faults::FaultType,
        skip: u32,
        occurrence: crate::faults::Occurrence,
    ) {
        self.faults.insert(bucket, key, fault, skip, occurrence);
    }

    /// Drop the entire fault queue for `(bucket, key)`.
    pub fn clear_fault(&self, bucket: &str, key: &str) {
        self.faults.clear(bucket, key);
    }

    /// Install a server-wide load-driven throttle: a token bucket admitting a
    /// sustained `rate` requests/sec (with a `burst` allowance) and shedding the
    /// rest with 503 `SlowDown`, relenting as the client's arrival rate drops.
    ///
    /// Models S3's per-prefix request-rate limit: a high-fan-out burst arrives
    /// faster than the bucket refills and the excess is shed, and the transfer
    /// recovers only because its own backoff paces re-issues back under `rate`.
    /// Which request is shed depends on arrival timing; the behavior (over rate →
    /// shed, rate drops → recover) is deterministic. Applies to every S3 operation,
    /// before touching storage. Distinct from the per-`(bucket, key)` fault queue
    /// and from the persistent [`Always`](crate::faults::Occurrence) service-error
    /// fault, which never relents.
    pub fn set_rate_throttle(&self, rate: f64, burst: f64) {
        *self.throttle.lock().unwrap() =
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

    /// Reset all state (clear all buckets, objects, and in-flight uploads).
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
        let faults = self.faults.clone();
        let connect_reset = self.connect_reset.clone();
        let throttle = self.throttle.clone();
        let server_task = tokio::spawn(async move {
            let http_server = ConnBuilder::new(TokioExecutor::new());
            let graceful = hyper_util::server::graceful::GracefulShutdown::new();

            let inner = Inner::with_faults_and_throttle(storage, faults, throttle);
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
