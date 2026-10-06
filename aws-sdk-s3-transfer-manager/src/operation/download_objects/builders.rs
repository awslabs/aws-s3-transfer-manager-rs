/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::io::walk::S3Walker;
use crate::types::FailedTransferPolicy;

use super::{DownloadObjectsHandle, DownloadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object download.
///
/// Walk behavior (prefix, filter, pagination) is configured by supplying a
/// custom [`S3Walker`] via [`walker`](Self::walker). When not set, a default
/// walker is used that filters out 0-byte folder markers.
#[derive(Debug)]
pub struct DownloadObjectsFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: DownloadObjectsInputBuilder,
}

impl DownloadObjectsFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: std::default::Default::default(),
        }
    }

    /// Initiate download of multiple objects.
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-download-objects", fields(
        bucket = self.inner.bucket.as_deref().unwrap_or_default(),
        destination = self.inner.destination.as_deref().map(|p| p.to_str().unwrap_or_default()).unwrap_or_default(),
        key_prefix = self.inner.key_prefix.as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download_objects::DownloadObjects::orchestrate(self.handle, input)
    }

    /// The S3 bucket name containing the object(s) to download. Required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.bucket(input);
        self
    }

    /// The S3 bucket name.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_bucket(input);
        self
    }

    /// The S3 bucket name.
    pub fn get_bucket(&self) -> Option<&str> {
        self.inner.get_bucket()
    }

    /// The local directory to download into. Required.
    pub fn destination(mut self, input: impl Into<PathBuf>) -> Self {
        self.inner = self.inner.destination(input);
        self
    }

    /// The local directory to download into.
    pub fn set_destination(mut self, input: Option<PathBuf>) -> Self {
        self.inner = self.inner.set_destination(input);
        self
    }

    /// The local directory to download into.
    pub fn get_destination(&self) -> Option<&Path> {
        self.inner.get_destination()
    }

    /// Limit the response to keys that begin with the given prefix.
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.inner = self.inner.key_prefix(input);
        self
    }

    /// Limit the response to keys that begin with the given prefix.
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.inner = self.inner.set_key_prefix(input);
        self
    }

    /// The key prefix.
    pub fn get_key_prefix(&self) -> Option<&str> {
        self.inner.get_key_prefix()
    }

    /// Character used to group keys.
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

    /// The failure policy to use when any individual object download fails.
    /// Defaults to [`FailedTransferPolicy::Abort`].
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner = self.inner.failure_policy(input);
        self
    }

    /// The failure policy.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.get_failure_policy()
    }

    /// Walker configuration (filter, pagination, etc.).
    pub fn walker(mut self, input: S3Walker) -> Self {
        self.inner = self.inner.walker(input);
        self
    }

    /// Walker configuration.
    pub fn set_walker(mut self, input: Option<S3Walker>) -> Self {
        self.inner = self.inner.set_walker(input);
        self
    }

    /// Walker configuration.
    pub fn get_walker(&self) -> Option<&S3Walker> {
        self.inner.get_walker()
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    /// Defaults to 512.
    pub fn max_concurrent_downloads(mut self, input: usize) -> Self {
        self.inner = self.inner.max_concurrent_downloads(input);
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn set_max_concurrent_downloads(mut self, input: Option<usize>) -> Self {
        self.inner = self.inner.set_max_concurrent_downloads(input);
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn get_max_concurrent_downloads(&self) -> Option<usize> {
        self.inner.get_max_concurrent_downloads()
    }
}

impl crate::operation::download_objects::input::DownloadObjectsInputBuilder {
    /// Initiate a download transfer for multiple objects with this input using the given client.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadObjectsHandle, crate::error::Error> {
        let mut fluent_builder = client.download_objects();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }
}
