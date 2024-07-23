/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Abstractions for downloading objects from S3
pub mod downloader;
use std::sync::Arc;

pub use downloader::Downloader;

/// Abstractions for downloading objects from Amazon S3
pub mod uploader;

pub use uploader::Uploader;

/// Transfer manager client for Amazon Simple Storage Service.
#[derive(Debug, Clone)]
pub struct Client {
    handle: Arc<Handle>,
}

/// Whatever is needed to carry out operations, e.g. scheduler, budgets, config, env details, etc
#[derive(Debug)]
pub(crate) struct Handle {
    pub(crate) config: crate::Config
}


impl Client {

    /// Constructs a fluent builder for the
    /// [`Upload`](crate::operation::upload::builders::UploadFluentBuilder) operation.
    pub fn upload(&self) -> crate::operation::upload::builders::UploadFluentBuilder {
        crate::operation::upload::builders::UploadFluentBuilder::new(self.handle.clone())
    }

}
