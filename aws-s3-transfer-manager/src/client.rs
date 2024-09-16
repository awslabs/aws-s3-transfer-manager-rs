/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::{metrics, Config};
use crate::{
    types::{ConcurrencySetting, PartSize},
    DEFAULT_CONCURRENCY, MEBIBYTE,
};
use std::sync::Arc;

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) handle: Arc<Handle>,
}

/// Whatever is needed to carry out operations, e.g. scheduler, budgets, config, env details, etc
#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) config: crate::Config,
}

impl Handle {
    /// Get the concrete number of workers to use based on the concurrency setting.
    pub(crate) fn num_workers(&self) -> usize {
        match self.config.concurrency() {
            // TODO(aws-sdk-rust#1159): add logic for determining this
            ConcurrencySetting::Auto => DEFAULT_CONCURRENCY,
            ConcurrencySetting::Explicit(explicit) => *explicit,
        }
    }

    /// Get the concrete minimum upload size in bytes to use to determine whether multipart uploads
    /// are enabled for a given request.
    pub(crate) fn mpu_threshold_bytes(&self) -> u64 {
        match self.config.multipart_threshold() {
            PartSize::Auto => 16 * MEBIBYTE,
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Get the concrete target part size to use for uploads
    pub(crate) fn upload_part_size_bytes(&self) -> u64 {
        match self.config.part_size() {
            PartSize::Auto => 8 * MEBIBYTE,
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Get the concrete target part size to use for downloads
    pub(crate) fn download_part_size_bytes(&self) -> u64 {
        match self.config.part_size() {
            PartSize::Auto => 5 * MEBIBYTE,
            PartSize::Target(explicit) => *explicit,
        }
    }

    /// Get the concrete target throughput to reach for
    pub(crate) fn target_throughput(&self) -> metrics::Throughput {
        match self.config.target_throughput() {
            crate::types::TargetThroughput::Auto => metrics::Throughput::new_bytes_per_sec(
                10 * metrics::unit::Bytes::Gigabit.as_bytes_u64(),
            ),
            crate::types::TargetThroughput::Explicit(t) => *t,
        }
    }
}

impl Client {
    /// Creates a new client from a transfer manager config.
    pub fn new(config: Config) -> Client {
        let handle = Arc::new(Handle { config });

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
    /// use aws_s3_transfer_manager::io::InputStream;
    ///
    /// async fn upload_file(
    ///     client: &aws_s3_transfer_manager::Client,
    ///     path: impl AsRef<Path>
    /// ) -> Result<(), Box<dyn Error>> {
    ///     let stream = InputStream::from_path(path)?;
    ///     let handle = client.upload()
    ///         .bucket("my-bucket")
    ///         .key("my-key")
    ///         .body(stream)
    ///         .send()
    ///         .await?;
    ///
    ///     // send() may return before the transfer is complete.
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
    /// async fn get_object(client: &aws_s3_transfer_manager::Client) -> Result<(), Box<dyn Error>> {
    ///
    ///     let handle = client
    ///         .download()
    ///         .bucket("my-bucket")
    ///         .key("my-key")
    ///         .send()
    ///         .await?;
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
    /// use aws_s3_transfer_manager::error::Error;
    ///
    /// async fn download_bucket(
    ///     client: &aws_s3_transfer_manager::Client,
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
    /// use aws_s3_transfer_manager::operation::upload_objects::UploadObjectsError;
    ///
    /// async fn upload_directory(
    ///     client: &aws_s3_transfer_manager::Client,
    ///     source: &Path,
    /// ) -> Result<(), UploadObjectsError> {
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
