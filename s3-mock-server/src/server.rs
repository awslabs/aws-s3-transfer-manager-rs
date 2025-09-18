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
            config: self.config,
        })
    }
}

/// S3 Mock Server.
pub struct S3MockServer {
    /// Storage backend.
    storage: Arc<dyn StorageBackend>,

    /// Server configuration.
    config: ServerConfig,
}

impl S3MockServer {
    /// Create a new S3MockServerBuilder.
    pub fn builder() -> S3MockServerBuilder {
        S3MockServerBuilder::new()
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
        let server_task = tokio::spawn(async move {
            let http_server = ConnBuilder::new(TokioExecutor::new());
            let graceful = hyper_util::server::graceful::GracefulShutdown::new();

            let inner = Inner::new(storage);
            let service = {
                let mut b = S3ServiceBuilder::new(inner);
                b.set_auth(SimpleAuth::from_single(TEST_ACCESS_KEY, TEST_SECRET_KEY));
                b.build().into_shared()
            };
            loop {
                let (socket, _) = tokio::select! {
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
                            break;
                        }
                };

                let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
                let conn = graceful.watch(conn.into_owned());
                tokio::spawn(async move {
                    let _ = conn.await;
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
