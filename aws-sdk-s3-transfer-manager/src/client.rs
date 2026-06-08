/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::unit::ByteUnit;
use crate::runtime::ManagedThreadRuntime;
use crate::scheduler::{
    AdaptiveConcurrencyController, AdaptiveConfig, ConcurrencyController, FixedConcurrency,
    Scheduler,
};
use crate::telemetry::Telemetry;
use crate::types::{ConcurrencyMode, PartSize};
use crate::Config;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::ExecutionRuntime;

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) handle: Arc<Handle>,
}

/// Whatever is needed to carry out operations, e.g. scheduler, budgets, config, env details, etc
#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) config: crate::Config,
    pub(crate) s3_client: aws_sdk_s3::Client,
    pub(crate) scheduler: Scheduler,
    pub(crate) runtime: Arc<dyn ExecutionRuntime>,
    pub(crate) controller: Arc<dyn ConcurrencyController>,
    pub(crate) telemetry: Arc<Telemetry>,
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
        // 1. Create concurrency controller and telemetry
        let (controller, telemetry): (Arc<dyn ConcurrencyController>, _) =
            match config.concurrency() {
                ConcurrencyMode::Explicit(n) => (
                    Arc::new(FixedConcurrency::new(*n)),
                    Arc::new(Telemetry::new(Duration::from_millis(500))),
                ),
                // TODO: implement support for target throughput
                _ => {
                    let adaptive_config = AdaptiveConfig::default();
                    let telemetry = Arc::new(Telemetry::new(adaptive_config.window.duration));
                    let controller = Arc::new(AdaptiveConcurrencyController::new(
                        adaptive_config,
                        Arc::clone(&telemetry.io_counters),
                    ));
                    (controller, telemetry)
                }
            };

        // 2. Build Handle with Arc::new_cyclic so scheduler and runtime
        //    can hold Weak<Handle> without creating a reference cycle.
        #[cfg(feature = "dial9")]
        let telemetry_guard = config.take_telemetry_guard().map(std::sync::Arc::new);

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
            }
        });
        Client { handle }
    }

    /// Returns the client's configuration
    pub fn config(&self) -> &Config {
        &self.handle.config
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
}
