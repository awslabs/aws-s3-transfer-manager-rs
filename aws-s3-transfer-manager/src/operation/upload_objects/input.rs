/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};

use crate::types::{FailedTransferPolicy, UploadFilter};

/// Input type for uploading multiple objects
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct UploadObjectsInput {
    // TODO - consistent naming for "s3_delimiter" and "s3_prefix" between upload_objects and download_objects

    // TODO - should required stuff (i.e. bucket) be Optional?

    // TODO - should stuff with fallback values (i.e. s3_delimiter) be Optional?

    // TODO - should stuff like s3_delimiter (which defaults to "/") be an String or Option<String>,

    // TODO - docs and examples on the interaction of prefix & delimiter
    /// The S3 bucket name that objects will upload to.
    pub bucket: Option<String>,

    /// The local directory to upload from.
    pub source: Option<PathBuf>,

    /// Whether to recurse into subdirectories when traversing local file tree.
    pub recursive: bool,

    /// Whether to follow symbolic links when traversing the local file tree.
    pub follow_symbolic_links: bool,

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
    pub fn follow_symbolic_links(&self) -> bool {
        self.follow_symbolic_links
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
    pub(crate) follow_symbolic_links: bool,
    pub(crate) filter: Option<UploadFilter>,
    pub(crate) s3_prefix: Option<String>,
    pub(crate) s3_delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
}

impl UploadObjectsInputBuilder {
    // TODO - setters/getters

    /// Consumes the builder and constructs an [`UploadObjectsInput`]
    pub fn build(
        self,
    ) -> Result<UploadObjectsInput, ::aws_smithy_types::error::operation::BuildError> {
        // TODO - validate required stuff (i.e. bucket)

        Ok(UploadObjectsInput {
            bucket: self.bucket,
            source: self.source,
            recursive: self.recursive,
            follow_symbolic_links: self.follow_symbolic_links,
            filter: self.filter,
            s3_prefix: self.s3_prefix,
            s3_delimiter: self.s3_delimiter.or(Some("/".into())),
            failure_policy: self.failure_policy,
        })
    }
}

// impl UploadObjectsInputBuilder {
//     pub fn bucket(mut self, input: impl Into<String>) -> Self {
//         self.set_bucket(Some(input.into()))
//     }

//     pub fn set_bucket(mut self, input: Option<String>) -> Self {
//         self.model.bucket = input;
//         self
//     }

//     pub fn get_bucket(&self) -> Option<&str> {
//         self.model.bucket()
//     }

//     pub fn source(mut self, input: impl Into<PathBuf>) -> Self {
//         self.set_source(Some(input.into()))
//     }

//     pub fn set_source(mut self, input: Option<PathBuf>) -> Self {
//         self.model.source = input;
//         self
//     }

//     pub fn get_source(&self) -> Option<&Path> {
//         self.model.source()
//     }

//     pub fn follow_symbolic_links(mut self, input: bool) -> Self {
//         self.model.follow_symbolic_links = input;
//         self
//     }

//     pub fn get_follow_symbolic_links(&self) -> bool {
//         self.model.follow_symbolic_links
//     }

//     pub fn recursive(mut self, input: bool) -> Self {
//         self.model.recursive = input;
//         self
//     }

//     pub fn get_recursive(&self) -> bool {
//         self.model.recursive
//     }

//     pub fn s3_prefix(mut self, input: impl Into<String>) -> Self {
//         self.set_s3_prefix(Some(input.into()))
//     }

//     pub fn set_s3_prefix(mut self, input: Option<String>) -> Self {
//         self.model.s3_prefix = input;
//         self
//     }

//     pub fn get_s3_prefix(&self) -> &Option<String> {
//         &self.model.s3_prefix
//     }

//     pub fn s3_delimiter(mut self, input: impl Into<String>) -> Self {
//         self.set_s3_delimiter(Some(input.into()))
//     }

//     pub fn set_s3_delimiter(mut self, input: Option<String>) -> Self {
//         self.model.s3_delimiter = input;
//         self
//     }

//     pub fn get_s3_delimiter(&self) -> &Option<String> {
//         &self.model.s3_delimiter
//     }

//     // TODO: download version of this takes Fn instead of Into

//     pub fn filter(mut self, input: impl Into<UploadFilter>) -> Self {
//         self.set_filter(Some(input.into()))
//     }

//     pub fn set_filter(mut self, input: Option<UploadFilter>) -> Self {
//         self.model.filter = input;
//         self
//     }

//     pub fn get_filter(&self) -> &Option<UploadFilter> {
//         &self.model.filter
//     }

//     pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
//         self.model.failure_policy = input;
//         self
//     }

//     pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
//         self.model.failure_policy
//     }
// }
