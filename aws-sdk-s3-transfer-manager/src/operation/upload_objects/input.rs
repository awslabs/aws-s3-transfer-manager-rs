/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::walk::FsWalker;
use crate::types::FailedTransferPolicy;
use aws_smithy_types::error::operation::BuildError;

use std::path::{Path, PathBuf};

/// Input type for uploading multiple objects.
///
/// Walk behavior (recursion, symbolic link handling, file filtering, sort
/// order, etc.) is configured by supplying a custom [`FsWalker`] via
/// [`walker`](UploadObjectsInputBuilder::walker). When no walker is
/// provided, a default walker is used: non-recursive, does not follow
/// symbolic links, no filter.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct UploadObjectsInput {
    /// The S3 bucket name that objects will upload to.
    pub bucket: Option<String>,

    /// The local directory to upload from.
    pub source: Option<PathBuf>,

    /// Walker configuration. See [`FsWalker`] for available options.
    /// `None` selects the default walker.
    pub walker: Option<FsWalker>,

    /// The S3 key prefix to use for each object.
    pub key_prefix: Option<String>,

    /// Character used to group keys.
    pub delimiter: Option<String>,

    /// The failure policy to use when any individual object upload fails.
    pub failure_policy: FailedTransferPolicy,

    /// Per-request cap on concurrently-materialized child upload transfers.
    ///
    /// Acts as a memory backstop: the scheduler's hierarchical fair-share
    /// scheduling drives throughput and rate-limits the walker naturally,
    /// so this knob primarily bounds the working-set size of in-flight
    /// child handles. Defaults to 512.
    pub max_concurrent_uploads: usize,
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

    /// Walker configuration. Returns `None` when the default walker should
    /// be used.
    pub fn walker(&self) -> Option<&FsWalker> {
        self.walker.as_ref()
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

    /// Returns the per-request cap on concurrently-materialized child
    /// upload transfers.
    pub fn max_concurrent_uploads(&self) -> usize {
        self.max_concurrent_uploads
    }
}

/// A builder for [`UploadObjectsInput`].
#[non_exhaustive]
#[derive(Clone, Default, Debug)]
pub struct UploadObjectsInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) source: Option<PathBuf>,
    pub(crate) walker: Option<FsWalker>,
    pub(crate) key_prefix: Option<String>,
    pub(crate) delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
    pub(crate) max_concurrent_uploads: Option<usize>,
}

impl UploadObjectsInputBuilder {
    /// Consume the builder and construct an [`UploadObjectsInput`].
    pub fn build(self) -> Result<UploadObjectsInput, BuildError> {
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
            walker: self.walker,
            key_prefix: self.key_prefix,
            delimiter: self.delimiter,
            failure_policy: self.failure_policy,
            max_concurrent_uploads: self
                .max_concurrent_uploads
                .unwrap_or(crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN),
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
    pub fn get_bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
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
    pub fn get_source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    /// Walker configuration (recursion, symlink handling, filter, etc.).
    ///
    /// Pass an [`FsWalker`] built via [`FsWalker::builder`] to customize the
    /// walk. When not set, a default walker is used: non-recursive, does not
    /// follow symbolic links, no filter.
    pub fn walker(mut self, input: FsWalker) -> Self {
        self.walker = Some(input);
        self
    }

    /// Walker configuration.
    pub fn set_walker(mut self, input: Option<FsWalker>) -> Self {
        self.walker = input;
        self
    }

    /// Walker configuration.
    pub fn get_walker(&self) -> Option<&FsWalker> {
        self.walker.as_ref()
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

    /// Per-request cap on concurrently-materialized child upload transfers.
    ///
    /// Acts as a memory backstop: the scheduler's hierarchical fair-share
    /// scheduling drives throughput and rate-limits the walker naturally,
    /// so this knob primarily bounds the working-set size of in-flight
    /// child handles. Defaults to 512.
    pub fn max_concurrent_uploads(mut self, input: usize) -> Self {
        self.max_concurrent_uploads = Some(input);
        self
    }

    /// Per-request cap on concurrently-materialized child upload transfers.
    /// See [`max_concurrent_uploads`](Self::max_concurrent_uploads).
    pub fn set_max_concurrent_uploads(mut self, input: Option<usize>) -> Self {
        self.max_concurrent_uploads = input;
        self
    }

    /// Returns the configured per-request cap on concurrently-materialized
    /// child upload transfers, if any.
    pub fn get_max_concurrent_uploads(&self) -> Option<usize> {
        self.max_concurrent_uploads
    }
}
