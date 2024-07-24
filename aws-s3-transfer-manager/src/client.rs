/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Abstractions for downloading objects from S3
pub mod downloader;
pub use downloader::Downloader;

use crate::Config;
use crate::{
    types::{ConcurrencySetting, PartSize},
    DEFAULT_CONCURRENCY, MEBIBYTE,
};
use std::sync::Arc;

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    handle: Arc<Handle>,
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
}

impl Client {
    /// Creates a new client from a transfer manager config.
    pub fn new(config: Config) -> Client {
        let handle = Arc::new(Handle { config });

        Client { handle }
    }

    /// Constructs a fluent builder for the
    /// [`Upload`](crate::operation::upload::builders::UploadFluentBuilder) operation.
    pub fn upload(&self) -> crate::operation::upload::builders::UploadFluentBuilder {
        crate::operation::upload::builders::UploadFluentBuilder::new(self.handle.clone())
    }
}
