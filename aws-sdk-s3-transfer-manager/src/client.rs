/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::runtime::ManagedThreadRuntime;
use crate::scheduler::{
    AdaptiveConcurrencyController, AdaptiveConfig, ConcurrencyController, FixedConcurrency,
    Scheduler, SchedulerBuilder,
};
use crate::telemetry::Telemetry;
use crate::types::{ConcurrencyMode, PartSize};
use crate::Config;
use crate::{metrics::unit::ByteUnit, DEFAULT_CONCURRENCY};
use std::sync::Arc;
use std::time::Duration;

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
    pub(crate) telemetry: Telemetry,
}

impl Handle {
    /// Get the concrete number of workers to use based on the concurrency setting.
    pub(crate) fn num_workers(&self) -> usize {
        match self.config.concurrency() {
            ConcurrencyMode::Explicit(concurrency) => *concurrency,
            _ => DEFAULT_CONCURRENCY,
        }
    }

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

    /// Create a Handle from a Config and Scheduler, resolving the S3 client.
    #[cfg(test)]
    pub(crate) fn with_config_and_scheduler(
        mut config: crate::Config,
        scheduler: Scheduler,
    ) -> Self {
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
            telemetry: Telemetry::new(std::time::Duration::from_millis(500)),
        }
    }
}

impl Client {
    /// Creates a new client from a transfer manager config.
    pub fn new(mut config: Config) -> Client {
        // 1. Create runtime (spawns threads, creates per-thread state)
        let (controller, telemetry): (Arc<dyn ConcurrencyController>, _) =
            match config.concurrency() {
                ConcurrencyMode::Explicit(n) => (
                    Arc::new(FixedConcurrency::new(*n)),
                    Telemetry::new(Duration::from_millis(500)),
                ),
                // TODO: implement support for target throughput
                _ => {
                    let adaptive_config = AdaptiveConfig::default();
                    let telemetry = Telemetry::new(adaptive_config.window.duration);
                    let controller = Arc::new(AdaptiveConcurrencyController::new(
                        adaptive_config,
                        Arc::clone(&telemetry.io_counters),
                    ));
                    (controller, telemetry)
                }
            };
        let scheduler = SchedulerBuilder::new(controller)
            // .build(|scheduler| Arc::new(TokioMultiThreadRuntime::new(scheduler)));
            .build(|scheduler| Arc::new(ManagedThreadRuntime::builder(scheduler).build()));

        // 2. Build S3 client, injecting runtime transport when appropriate
        let s3_client = match config.take_s3_client_source() {
            crate::config::S3ClientSource::Provided(client) => client,
            crate::config::S3ClientSource::FromConfig(s3_config) => {
                let mut builder = s3_config.builder;
                if s3_config.enable_runtime_http {
                    if let Some(http_client) = scheduler.runtime().components().http_client() {
                        builder = builder.http_client(http_client.clone());
                    }
                }
                aws_sdk_s3::Client::from_conf(builder.build())
            }
        };

        let handle = Arc::new(Handle {
            config,
            s3_client,
            scheduler,
            telemetry,
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
    ///         .send()
    ///         .await?;
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
    ///         .recursive(true)
    ///         .send()
    ///         .await?;
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
