/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::types::{DownloadFilter, FailedTransferPolicy};
use aws_smithy_types::error::operation::BuildError;

use std::{
    fmt,
    path::{Path, PathBuf},
};

/// Input type for downloading multiple objects
#[non_exhaustive]
#[derive(Clone)]
pub struct DownloadObjectsInput {
    /// The bucket name containing the object(s).
    pub bucket: Option<String>,

    /// The destination directory to which files should be downloaded
    pub destination: Option<PathBuf>,

    /// Limit the response to keys that begin with the given prefix
    pub key_prefix: Option<String>,

    /// Character used to group keys
    pub delimiter: Option<String>,

    /// The failure policy to use when any individual object download fails.
    pub failure_policy: FailedTransferPolicy,

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub filter: Option<DownloadFilter>,
}

impl DownloadObjectsInput {
    /// Creates a new builder-style object to manufacture [`DownloadObjectsInput`](crate::operation::download_objects::DownloadObjectsInput).
    pub fn builder() -> DownloadObjectsInputBuilder {
        DownloadObjectsInputBuilder::default()
    }

    /// The bucket name containing the object(s).
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// The destination directory to which files should be downloaded
    pub fn destination(&self) -> Option<&Path> {
        self.destination.as_deref()
    }

    /// Limit the response to keys that begin with the given prefix
    pub fn key_prefix(&self) -> Option<&str> {
        self.key_prefix.as_deref()
    }

    /// Character used to group keys
    pub fn delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
    }

    /// The failure policy to use when any individual object download fails.
    pub fn failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn filter(&self) -> Option<&DownloadFilter> {
        self.filter.as_ref()
    }
}

impl fmt::Debug for DownloadObjectsInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("DownloadObjectsInput");
        formatter.field("bucket", &self.bucket);
        formatter.field("destination", &self.destination);
        formatter.field("key_prefix", &self.key_prefix);
        formatter.field("delimiter", &self.delimiter);
        formatter.field("failure_policy", &self.failure_policy);
        formatter.field("filter", &self.filter.is_some());
        formatter.finish()
    }
}

/// A builder for [`DownloadObjectsInput`](crate::operation::download_objects::DownloadObjectsInput).
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct DownloadObjectsInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) destination: Option<PathBuf>,
    pub(crate) key_prefix: Option<String>,
    pub(crate) delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
    pub(crate) filter: Option<DownloadFilter>,
}

impl DownloadObjectsInputBuilder {
    /// Set the bucket name containing the object(s) to download.
    ///
    /// NOTE: A bucket name is required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.bucket = Some(input.into());
        self
    }

    /// Set the bucket name containing the object(s) to download.
    ///
    /// NOTE: A bucket name is required.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.bucket = input;
        self
    }

    /// The bucket name containing the object(s).
    pub fn get_bucket(&self) -> &Option<String> {
        &self.bucket
    }

    /// Set the destination directory to which files should be downloaded
    ///
    /// NOTE: A destination directory is required.
    pub fn destination(mut self, input: impl Into<PathBuf>) -> Self {
        self.destination = Some(input.into());
        self
    }

    /// Set the destination directory to which files should be downloaded
    ///
    /// NOTE: A destination directory is required.
    pub fn set_destination(mut self, input: Option<PathBuf>) -> Self {
        self.destination = input;
        self
    }

    /// The destination directory to which files should be downloaded
    pub fn get_destination(&self) -> &Option<PathBuf> {
        &self.destination
    }

    /// Limit the response to keys that begin with the given prefix
    pub fn key_prefix(mut self, input: impl Into<String>) -> Self {
        self.key_prefix = Some(input.into());
        self
    }

    /// Limit the response to keys that begin with the given prefix
    pub fn set_key_prefix(mut self, input: Option<String>) -> Self {
        self.key_prefix = input;
        self
    }

    /// The key_prefix name containing the object(s).
    pub fn get_key_prefix(&self) -> &Option<String> {
        &self.key_prefix
    }

    /// Character used to group keys
    pub fn delimiter(mut self, input: impl Into<String>) -> Self {
        self.delimiter = Some(input.into());
        self
    }

    /// Character used to group keys
    pub fn set_delimiter(mut self, input: Option<String>) -> Self {
        self.delimiter = input;
        self
    }

    /// Character used to group keys
    pub fn get_delimiter(&self) -> &Option<String> {
        &self.delimiter
    }

    /// The failure policy to use when any individual object download fails.
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.failure_policy = input;
        self
    }

    /// The failure policy to use when any individual object download fails.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn filter(
        mut self,
        input: impl Fn(&aws_sdk_s3::types::Object) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.filter = Some(DownloadFilter::from(input));
        self
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn set_filter(mut self, input: Option<DownloadFilter>) -> Self {
        self.filter = input;
        self
    }

    /// Filter unwanted S3 objects from being downloaded as part of the transfer.
    pub fn get_filter(&self) -> &Option<DownloadFilter> {
        &self.filter
    }

    /// Consumes the builder and constructs a [`DownloadObjectsInput`](crate::operation::download_objects::DownloadObjectsInput).
    pub fn build(self) -> Result<DownloadObjectsInput, BuildError> {
        if self.bucket.is_none() {
            return Err(BuildError::missing_field("bucket", "A bucket is required"));
        }

        if self.destination.is_none() {
            return Err(BuildError::missing_field(
                "destination",
                "Destination directory is required",
            ));
        }

        Result::Ok(DownloadObjectsInput {
            bucket: self.bucket,
            destination: self.destination,
            key_prefix: self.key_prefix,
            delimiter: self.delimiter,
            failure_policy: self.failure_policy,
            filter: self.filter,
        })
    }
}

impl fmt::Debug for DownloadObjectsInputBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("DownloadObjectsInputBuilder");
        formatter.field("bucket", &self.bucket);
        formatter.field("destination", &self.destination);
        formatter.field("key_prefix", &self.key_prefix);
        formatter.field("delimiter", &self.delimiter);
        formatter.field("failure_policy", &self.failure_policy);
        formatter.field("filter", &self.filter.is_some());
        formatter.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::DownloadObjectsInput;

    #[test]
    fn test_no_destination_error() {
        let err = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .build()
            .unwrap_err();

        let err_string = err.to_string();
        assert!(err_string.contains("Destination directory is required"));
    }

    #[test]
    fn test_no_bucket_error() {
        let err = DownloadObjectsInput::builder()
            .destination("/tmp/test")
            .build()
            .unwrap_err();

        let err_string = err.to_string();
        assert!(err_string.contains("A bucket is required"));
    }
}
