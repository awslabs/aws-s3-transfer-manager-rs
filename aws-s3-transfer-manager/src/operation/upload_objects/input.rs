/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

use crate::types::{FailedTransferPolicy, UploadFilter};

// TODO - consistent naming for "s3_delimiter" and "s3_prefix" between upload_objects and download_objects

// TODO - should required stuff in the Input struct (i.e. bucket) be Optional?

// TODO - should stuff like s3_delimiter (which defaults to "/") be an String or Option<String>,

// TODO - docs and examples on the interaction of prefix & delimiter

/// Input type for uploading multiple objects
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct UploadObjectsInput {
    /// The S3 bucket name that objects will upload to.
    pub bucket: Option<String>,

    /// The local directory to upload from.
    pub source: Option<PathBuf>,

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub recursive: bool,

    /// Whether to follow symbolic links when traversing the local file tree.
    pub follow_symlinks: bool,

    /// The filter for choosing which files to upload.
    pub filter: Option<UploadFilter>,

    /// The S3 key prefix to use for each object.
    pub s3_prefix: Option<String>,

    /// The S3 delimiter.
    pub s3_delimiter: Option<String>,

    /// The failure policy to use when any individual object upload fails.
    pub failure_policy: FailedTransferPolicy,
}

impl UploadObjectsInput {
    /// The S3 bucket name that objects will upload to.
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// The local directory to upload from.
    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub fn recursive(&self) -> bool {
        self.recursive
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    pub fn follow_symlinks(&self) -> bool {
        self.follow_symlinks
    }

    /// The filter for choosing which files to upload.
    pub fn filter(&self) -> Option<&UploadFilter> {
        self.filter.as_ref()
    }

    /// The S3 key prefix to use for each object.
    pub fn s3_prefix(&self) -> Option<&str> {
        self.s3_prefix.as_deref()
    }

    /// The S3 delimiter.
    pub fn s3_delimiter(&self) -> Option<&str> {
        self.s3_delimiter.as_deref()
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }
}

/// A builder for [UploadObjectsInput]
#[non_exhaustive]
#[derive(Clone, Default, Debug)]
pub struct UploadObjectsInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) source: Option<PathBuf>,
    pub(crate) recursive: bool,
    pub(crate) follow_symlinks: bool,
    pub(crate) filter: Option<UploadFilter>,
    pub(crate) s3_prefix: Option<String>,
    pub(crate) s3_delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
}

impl UploadObjectsInputBuilder {
    /// Consumes the builder and constructs an [`UploadObjectsInput`]
    pub fn build(
        self,
    ) -> Result<UploadObjectsInput, ::aws_smithy_types::error::operation::BuildError> {
        // TODO - validate required stuff (i.e. bucket)

        Ok(UploadObjectsInput {
            bucket: self.bucket,
            source: self.source,
            recursive: self.recursive,
            follow_symlinks: self.follow_symlinks,
            filter: self.filter,
            s3_prefix: self.s3_prefix,
            s3_delimiter: self.s3_delimiter.or(Some("/".into())),
            failure_policy: self.failure_policy,
        })
    }

    /// The S3 bucket name that objects will upload to.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.bucket = Some(input.into());
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.bucket = input;
        self
    }

    /// The S3 bucket name that objects will upload to.
    pub fn get_bucket(&self) -> &Option<String> {
        &self.bucket
    }

    /// The local directory to upload from.
    pub fn source(mut self, input: impl Into<PathBuf>) -> Self {
        self.source = Some(input.into());
        self
    }

    /// The local directory to upload from.
    pub fn set_source(mut self, input: Option<PathBuf>) -> Self {
        self.source = input;
        self
    }

    /// The local directory to upload from.
    pub fn get_source(&self) -> &Option<PathBuf> {
        &self.source
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub fn recursive(mut self, input: bool) -> Self {
        self.recursive = input;
        self
    }

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub fn get_recursive(&self) -> bool {
        self.recursive
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    pub fn follow_symlinks(mut self, input: bool) -> Self {
        self.follow_symlinks = input;
        self
    }

    /// Whether to follow symbolic links when traversing the local file tree.
    pub fn get_follow_symlinks(&self) -> bool {
        self.follow_symlinks
    }

    /// The filter for choosing which files to upload.
    pub fn filter(mut self, input: impl Into<UploadFilter>) -> Self {
        self.filter = Some(input.into());
        self
    }

    /// The filter for choosing which files to upload.
    pub fn set_filter(mut self, input: Option<UploadFilter>) -> Self {
        self.filter = input;
        self
    }

    /// The filter for choosing which files to upload.
    pub fn get_filter(&self) -> &Option<UploadFilter> {
        &self.filter
    }

    /// The S3 key prefix to use for each object.
    pub fn s3_prefix(mut self, input: impl Into<String>) -> Self {
        self.s3_prefix = Some(input.into());
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn set_s3_prefix(mut self, input: Option<String>) -> Self {
        self.s3_prefix = input;
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn get_s3_prefix(&self) -> &Option<String> {
        &self.s3_prefix
    }

    /// The S3 delimiter.
    pub fn s3_delimiter(mut self, input: impl Into<String>) -> Self {
        self.s3_delimiter = Some(input.into());
        self
    }

    /// The S3 delimiter.
    pub fn set_s3_delimiter(mut self, input: Option<String>) -> Self {
        self.s3_delimiter = input;
        self
    }

    /// The S3 delimiter.
    pub fn get_s3_delimiter(&self) -> &Option<String> {
        &self.s3_delimiter
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.failure_policy = input;
        self
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }
}
