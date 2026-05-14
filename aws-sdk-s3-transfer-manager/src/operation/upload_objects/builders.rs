/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::io::walk::FsWalker;
use crate::types::FailedTransferPolicy;

use super::{UploadObjectsHandle, UploadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object upload.
///
/// Walk behavior (recursion, symbolic links, filters, sort order) is
/// configured by supplying a custom [`FsWalker`] via [`walker`](Self::walker).
/// When not set, the default walker is used: non-recursive, does not follow
/// symbolic links, no filter.
#[derive(Debug)]
pub struct UploadObjectsFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: UploadObjectsInputBuilder,
}

impl UploadObjectsFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: std::default::Default::default(),
        }
    }

    /// Initiate upload of multiple objects.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-upload-objects", fields(
        bucket = self.inner.bucket.as_deref().unwrap_or_default(),
        source = self.inner.source.as_deref().map(|p| p.to_str().unwrap_or_default()).unwrap_or_default(),
        key_prefix = self.inner.key_prefix.as_deref().unwrap_or_default(),
    ))]
    pub async fn send(self) -> Result<UploadObjectsHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::upload_objects::UploadObjects::orchestrate(self.handle, input)
    }

    /// The S3 bucket name that objects will upload to. Required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input);
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_bucket(input);
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn get_bucket(&self) -> Option<&str> {
        self.inner.get_bucket()
    }

    /// The local directory to upload from. Required.
    pub fn source(mut self, input: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.source(input);
        self
    }

    /// The local directory to upload from.
    pub fn set_source(mut self, input: Option<PathBuf>) -> Self {
        self.inner = self.inner.set_source(input);
        self
    }

    /// The local directory to upload from.
    pub fn get_source(&self) -> Option<&Path> {
        self.inner.get_source()
    }

    /// Walker configuration (recursion, symlink handling, filter, etc.).
    ///
    /// Pass an [`FsWalker`] built via [`FsWalker::builder`] to customize the
    /// walk. When not set, a default walker is used: non-recursive, does not
    /// follow symbolic links, no filter.
    pub fn walker(mut self, input: FsWalker) -> Self {
        self.inner = self.inner.walker(input);
        self
    }

    /// Walker configuration.
    pub fn set_walker(mut self, input: Option<FsWalker>) -> Self {
        self.inner = self.inner.set_walker(input);
        self
    }

    /// Walker configuration.
    pub fn get_walker(&self) -> Option<&FsWalker> {
        self.inner.get_walker()
    }

    /// The S3 key prefix to use for each object. Defaults to no prefix
    /// (files are uploaded to the root of the bucket).
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key_prefix(input);
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key_prefix(input);
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn get_key_prefix(&self) -> Option<&str> {
        self.inner.get_key_prefix()
    }

    /// Character used to group keys. Defaults to `/`.
    pub fn delimiter(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.delimiter(input);
        self
    }

    /// Character used to group keys.
    pub fn set_delimiter(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_delimiter(input);
        self
    }

    /// Character used to group keys.
    pub fn get_delimiter(&self) -> Option<&str> {
        self.inner.get_delimiter()
    }

    /// The failure policy to use when any individual object upload fails.
    /// Defaults to [`FailedTransferPolicy::Abort`].
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner = self.inner.failure_policy(input);
        self
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.get_failure_policy()
    }

    /// Per-request cap on concurrently-materialized child upload transfers.
    ///
    /// Acts as a memory backstop: the scheduler's hierarchical fair-share
    /// scheduling drives throughput and rate-limits the walker naturally,
    /// so this knob primarily bounds the working-set size of in-flight
    /// child handles. Defaults to 10000.
    pub fn max_concurrent_uploads(mut self, input: usize) -> Self {
        self.inner = self.inner.max_concurrent_uploads(input);
        self
    }

    /// Per-request cap on concurrently-materialized child upload transfers.
    /// See [`max_concurrent_uploads`](Self::max_concurrent_uploads).
    pub fn set_max_concurrent_uploads(mut self, input: Option<usize>) -> Self {
        self.inner = self.inner.set_max_concurrent_uploads(input);
        self
    }

    /// Returns the configured per-request cap on concurrently-materialized
    /// child upload transfers, if any.
    pub fn get_max_concurrent_uploads(&self) -> Option<usize> {
        self.inner.get_max_concurrent_uploads()
    }
}

impl crate::operation::upload_objects::input::UploadObjectsInputBuilder {
    /// Initiate upload of multiple objects using the given client.
    pub async fn send_with(
        self,
        client: &crate::Client,
    ) -> Result<UploadObjectsHandle, crate::error::Error> {
        let mut fluent_builder = client.upload_objects();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
