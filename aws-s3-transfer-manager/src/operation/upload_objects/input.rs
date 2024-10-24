/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::{FailedTransferPolicy, UploadFilter};
use aws_smithy_types::error::operation::BuildError;

use std::path::{Path, PathBuf};

// TODO - docs and examples on the interaction of key_prefix & delimiter

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
    pub key_prefix: Option<String>,

    /// Character used to group keys.
    pub delimiter: Option<String>,

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
    pub fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }

    /// Character used to group keys.
    pub fn delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
    }

    /// The failure policy to use when any individual object upload fails.
    pub fn failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }
}

/// A builder for [`UploadObjectsInput`]
#[non_exhaustive]
#[derive(Clone, Default, Debug)]
pub struct UploadObjectsInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) source: Option<PathBuf>,
    pub(crate) recursive: bool,
    pub(crate) follow_symlinks: bool,
    pub(crate) filter: Option<UploadFilter>,
    pub(crate) key_prefix: Option<String>,
    pub(crate) delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
}

impl UploadObjectsInputBuilder {
    /// Consumes the builder and constructs an [`UploadObjectsInput`]
    pub fn build(
        self,
    ) -> Result<UploadObjectsInput, ::aws_smithy_types::error::operation::BuildError> {
        if self.bucket.is_none() {
            return Err(BuildError::missing_field("bucket", "A bucket is required"));
        }

        if self.source.is_none() {
            return Err(BuildError::missing_field(
                "source",
                "Source directory to upload is required",
            ));
        }

        Ok(UploadObjectsInput {
            bucket: self.bucket,
            source: self.source,
            recursive: self.recursive,
            follow_symlinks: self.follow_symlinks,
            filter: self.filter,
            key_prefix: self.key_prefix,
            delimiter: self.delimiter,
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
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.key_prefix = Some(input.into());
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.key_prefix = input;
        self
    }

    /// The S3 key prefix to use for each object.
    pub fn get_key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }

    /// Character used to group keys.
    pub fn delimiter(mut self, input: impl Into<String>) -> Self {
        self.delimiter = Some(input.into());
        self
    }

    /// Character used to group keys.
    pub fn set_delimiter(mut self, input: Option<String>) -> Self {
        self.delimiter = input;
        self
    }

    /// Character used to group keys.
    pub fn get_delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
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
