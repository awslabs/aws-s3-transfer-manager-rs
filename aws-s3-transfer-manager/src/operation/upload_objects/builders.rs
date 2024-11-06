/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{path::PathBuf, sync::Arc};

use crate::types::{FailedTransferPolicy, UploadFilter};

use super::{UploadObjectsHandle, UploadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object upload
#[derive(Debug)]
pub struct UploadObjectsFluentBuilder {
    handle: Arc<crate::client::Handle>,
    inner: UploadObjectsInputBuilder,
}

// TODO(https://github.com/awslabs/aws-s3-transfer-manager-rs/issues/68):
// Use `Option<&str>` instead of `&Option<String>` consistently throughout the codebase

impl UploadObjectsFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: std::default::Default::default(),
        }
    }

    /// Initiate upload of multiple objects
    #[tracing::instrument(skip_all, level = "debug", name = "initiate-upload-objects", fields(
        bucket = self.inner.bucket.as_deref().unwrap_or_default(),
        source = self.inner.source.as_deref().map(|p| p.to_str().unwrap_or_default()).unwrap_or_default(),
        key_prefix = self.inner.key_prefix.as_deref().unwrap_or_default(),
    ))]
    pub async fn send(self) -> Result<UploadObjectsHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::upload_objects::UploadObjects::orchestrate(self.handle, input).await
    }

    /// The S3 bucket name that objects will upload to.
    /// Required.
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
    pub fn get_bucket(&self) -> &Option<String> {
        self.inner.get_bucket()
    }

    /// The local directory to upload from.
    /// Required.
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
    pub fn get_source(&self) -> &Option<PathBuf> {
        self.inner.get_source()
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    /// Defaults to false.
    pub fn recursive(mut self, input: bool) -> Self {
        self.inner = self.inner.recursive(input);
        self
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub fn get_recursive(&self) -> bool {
        self.inner.get_recursive()
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    /// Defaults to false.
    pub fn follow_symlinks(mut self, input: bool) -> Self {
        self.inner = self.inner.follow_symlinks(input);
        self
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    pub fn get_follow_symlinks(&self) -> bool {
        self.inner.get_follow_symlinks()
    }

    /// The filter for choosing which files to upload.
    /// If not provided, everything is uploaded.
    pub fn filter(mut self, input: impl Into<UploadFilter>) -> Self {
        self.inner = self.inner.filter(input);
        self
    }

    // TODO - download version of filter() takes Fn instead of Into. Be consistent

    // TODO - should we filter directories too? not just files?
    // only-files is simpler, and matches what DownloadFilter can do.
    // but filtering out a directory in 1 call is more efficient than filtering out N files within it.
    // we could add it later, via new property `dir_filter: UploadFilter`
    // or new bool `filter_dirs: bool`

    // TODO - TransferManager should prevent infinite recursion from symlinks.
    // Should it skip ALL symbolic links that point elsewhere within the upload dir?

    /// The filter for choosing which files to upload.
    pub fn set_filter(mut self, input: Option<UploadFilter>) -> Self {
        self.inner = self.inner.set_filter(input);
        self
    }

    /// The filter for choosing which files to upload.
    pub fn get_filter(&self) -> &Option<UploadFilter> {
        self.inner.get_filter()
    }

    /// The S3 key prefix to use for each object.
    /// If not provided, files will be uploaded to the root of the bucket.
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

    /// Character used to group keys.
    /// If not provided, the slash "/" character is used.
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
    /// Defaults to [`FailedTransferPolicy::Abort`]
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner = self.inner.failure_policy(input);
        self
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.get_failure_policy()
    }
}

impl crate::operation::upload_objects::input::UploadObjectsInputBuilder {
    /// Initiate upload of multiple objects using the given client
    pub async fn send_with(
        self,
        client: &crate::Client,
    ) -> Result<UploadObjectsHandle, crate::error::Error> {
        let mut fluent_builder = client.upload_objects();
        fluent_builder.inner = self;
        fluent_builder.send().await
    }
}
