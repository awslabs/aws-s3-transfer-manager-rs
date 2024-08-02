/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{path::PathBuf, sync::Arc};

use crate::types::{FailedTransferPolicy, UploadFilter};

use super::{UploadObjectsError, UploadObjectsHandle, UploadObjectsInputBuilder};

/// Fluent builder for constructing a multiple object upload
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

    /// Initiate upload
    pub async fn send(self) -> Result<UploadObjectsHandle, UploadObjectsError> {
        // FIXME - Err(UploadObjectsError) instead of .expect()
        let input = self.inner.build().expect("valid input");
        crate::operation::upload_objects::UploadObjects::orchestrate(self.handle, input).await
    }

    // TODO - should Builder getters be nice like the Input getters?
    // e.g. Option<&str> instead of &Option<String>

    // TODO - can we reduce the copy-pastastravaganza? only have 1 builder type?
    // macromagic? point to canonical documentation?

    /// The S3 bucket name that objects will upload to.
    /// Required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.inner.bucket = Some(input.into());
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.inner.bucket = input;
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn get_bucket(&self) -> &Option<String> {
        &self.inner.bucket
    }

    /// The local directory to upload from.
    /// Required.
    pub fn source(mut self, input: impl Into<PathBuf>) -> Self {
        self.inner.source = Some(input.into());
        self
    }

    /// The local directory to upload from.
    pub fn set_source(mut self, input: Option<PathBuf>) -> Self {
        self.inner.source = input;
        self
    }

    /// The local directory to upload from.
    pub fn get_source(&self) -> &Option<PathBuf> {
        &self.inner.source
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    /// Defaults to false.
    pub fn recursive(mut self, input: bool) -> Self {
        self.inner.recursive = input;
        self
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub fn get_recursive(&self) -> bool {
        self.inner.recursive
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    /// Defaults to false.
    pub fn follow_symbolic_links(mut self, input: bool) -> Self {
        self.inner.follow_symbolic_links = input;
        self
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    pub fn get_follow_symbolic_links(&self) -> bool {
        self.inner.follow_symbolic_links
    }

    /// The filter for choosing which files to upload.
    /// Given a Path and its Metadata, return true if it should be uploaded.
    ///
    /// If `recursive` is true, the filter is also evaluated on subdirectories
    /// (check with `metadata.is_dir()`). Return false to prevent recursion into
    /// a directory.
    ///
    /// If `follow_symbolic_links` is true, the filter is also evaluated on symbolic
    /// links (check with `metadata.is_symlink()`).
    pub fn filter(mut self, input: impl Into<UploadFilter>) -> Self {
        self.inner.filter = Some(input.into());
        self
    }

    // TODO - download version of filter() takes Fn instead of Into

    // TODO - IS it a good idea to filter directories too?
    // or just do files for simplicity? or because DownloadFilter can't do directories

    // TODO - We COULD just let the user handle recursion and symlinks themselves
    // via the filter ... but that's probably a footgun.

    // TODO - TransferManager should prevent infinite recursion from symlinks.
    // Should it skip ALL symbolic links that point elsewhere within the upload dir?

    // TODO - Should we be passing std::fs::Metadata? It avoids additional syscalls
    // from naive calls to path.is_dir(), path.is_symlink(), etc. Should
    // we pass our own custom type so we're not tied to std::fs::Metadata?

    /// The filter for choosing which files to upload.
    pub fn set_filter(mut self, input: Option<UploadFilter>) -> Self {
        self.inner.filter = input;
        self
    }

    /// The filter for choosing which files to upload.
    pub fn get_filter(&self) -> &Option<UploadFilter> {
        &self.inner.filter
    }

    /// The S3 key prefix to use for each object.
    /// If not provided, files will be uploaded to the root of the bucket.
    pub fn s3_prefix(mut self, input: impl Into<String>) -> Self {
        self.inner.s3_prefix = Some(input.into());
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn set_s3_prefix(mut self, input: Option<String>) -> Self {
        self.inner.s3_prefix = input;
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn get_s3_prefix(&self) -> &Option<String> {
        &self.inner.s3_prefix
    }

    /// The S3 delimiter.
    /// If not provided, the slash "/" character is used.
    pub fn s3_delimiter(mut self, input: impl Into<String>) -> Self {
        self.inner.s3_delimiter = Some(input.into());
        self
    }

    /// The S3 delimiter.
    pub fn set_s3_delimiter(mut self, input: Option<String>) -> Self {
        self.inner.s3_delimiter = input;
        self
    }

    /// The S3 delimiter.
    pub fn get_s3_delimiter(&self) -> &Option<String> {
        &self.inner.s3_delimiter
    }

    /// The failure policy to use when any individual object upload fails.
    /// Defaults to [`FailedTransferPolicy::Abort`]
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.inner.failure_policy = input;
        self
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        &self.inner.failure_policy
    }
}
