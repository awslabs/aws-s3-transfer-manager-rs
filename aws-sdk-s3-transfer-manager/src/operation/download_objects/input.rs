/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::io::walk::S3Walker;
use crate::types::FailedTransferPolicy;
use aws_smithy_types::error::operation::BuildError;

use std::{
    fmt,
    path::{Path, PathBuf},
};

/// Input type for downloading multiple objects from Amazon S3.
///
/// Walk behavior (prefix, filter, pagination) is configured by supplying an
/// [`S3Walker`] via [`walker`](DownloadObjectsInputBuilder::walker). When not
/// set, the default walker lists the whole bucket.
///
/// The walker's prefix, set with
/// [`S3WalkerBuilder::prefix`](crate::io::walk::S3WalkerBuilder::prefix),
/// scopes the listing and is stripped from each key to build its path under
/// [`destination`](Self::destination). Without a prefix, the full key is
/// used.
///
/// 0-byte objects whose key ends in `/` (folder markers) are skipped. A key
/// whose local path resolves outside the destination, or to the destination
/// directory itself, is not downloaded. It is reported as a failure and
/// handled by the [`failure_policy`](Self::failure_policy).
#[non_exhaustive]
#[derive(Clone)]
pub struct DownloadObjectsInput {
    /// The bucket name containing the object(s).
    pub bucket: Option<String>,

    /// The destination directory to which files should be downloaded.
    pub destination: Option<PathBuf>,

    /// Character used to group keys.
    pub delimiter: Option<String>,

    /// The failure policy to use when any individual object download fails.
    pub failure_policy: FailedTransferPolicy,

    /// Walker configuration. See [`S3Walker`] for available options.
    /// `None` selects the default walker (filters out 0-byte folder markers).
    pub walker: Option<S3Walker>,

    /// Per-request cap on concurrently-materialized child download transfers.
    ///
    /// Acts as a memory backstop: the scheduler's hierarchical fair-share
    /// scheduling drives throughput and rate-limits the walker naturally,
    /// so this knob primarily bounds the working-set size of in-flight
    /// child handles. Defaults to 512.
    pub max_concurrent_downloads: Option<usize>,
}

impl DownloadObjectsInput {
    /// Creates a new builder.
    pub fn builder() -> DownloadObjectsInputBuilder {
        DownloadObjectsInputBuilder::default()
    }

    /// The bucket name containing the object(s).
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// The destination directory to which files should be downloaded.
    pub fn destination(&self) -> Option<&Path> {
        self.destination.as_deref()
    }

    /// Character used to group keys.
    pub fn delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
    }

    /// The failure policy to use when any individual object download fails.
    pub fn failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }

    /// The walker configuration for listing objects.
    pub fn walker(&self) -> Option<&S3Walker> {
        self.walker.as_ref()
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn max_concurrent_downloads(&self) -> Option<usize> {
        self.max_concurrent_downloads
    }
}

impl fmt::Debug for DownloadObjectsInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadObjectsInput")
            .field("bucket", &self.bucket)
            .field("destination", &self.destination)
            .field("delimiter", &self.delimiter)
            .field("failure_policy", &self.failure_policy)
            .field("walker", &self.walker.is_some())
            .field("max_concurrent_downloads", &self.max_concurrent_downloads)
            .finish()
    }
}

/// A builder for [`DownloadObjectsInput`].
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct DownloadObjectsInputBuilder {
    pub(crate) bucket: Option<String>,
    pub(crate) destination: Option<PathBuf>,
    pub(crate) delimiter: Option<String>,
    pub(crate) failure_policy: FailedTransferPolicy,
    pub(crate) walker: Option<S3Walker>,
    pub(crate) max_concurrent_downloads: Option<usize>,
}

impl DownloadObjectsInputBuilder {
    /// Set the bucket name. Required.
    pub fn bucket(mut self, input: impl Into<String>) -> Self {
        self.bucket = Some(input.into());
        self
    }

    /// Set the bucket name.
    pub fn set_bucket(mut self, input: Option<String>) -> Self {
        self.bucket = input;
        self
    }

    /// The bucket name.
    pub fn get_bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// Set the destination directory. Required.
    pub fn destination(mut self, input: impl Into<PathBuf>) -> Self {
        self.destination = Some(input.into());
        self
    }

    /// Set the destination directory.
    pub fn set_destination(mut self, input: Option<PathBuf>) -> Self {
        self.destination = input;
        self
    }

    /// The destination directory.
    pub fn get_destination(&self) -> Option<&Path> {
        self.destination.as_deref()
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

    /// The failure policy to use when any individual object download fails.
    /// Defaults to [`FailedTransferPolicy::Abort`].
    pub fn failure_policy(mut self, input: FailedTransferPolicy) -> Self {
        self.failure_policy = input;
        self
    }

    /// Sets the failure policy to use when any individual object download fails.
    ///
    /// `None` selects the default, [`FailedTransferPolicy::Abort`].
    pub fn set_failure_policy(mut self, input: Option<FailedTransferPolicy>) -> Self {
        self.failure_policy = input.unwrap_or_default();
        self
    }

    /// The failure policy.
    pub fn get_failure_policy(&self) -> &FailedTransferPolicy {
        &self.failure_policy
    }

    /// Walker configuration (prefix, filter, pagination, etc.).
    ///
    /// Pass an [`S3Walker`] built via [`S3Walker::builder`] to customize the
    /// listing. Its prefix scopes the listing and is stripped from each key
    /// to build its local path. When not set, a default walker is used that
    /// filters out 0-byte folder markers.
    pub fn walker(mut self, input: S3Walker) -> Self {
        self.walker = Some(input);
        self
    }

    /// Walker configuration.
    pub fn set_walker(mut self, input: Option<S3Walker>) -> Self {
        self.walker = input;
        self
    }

    /// Walker configuration.
    pub fn get_walker(&self) -> Option<&S3Walker> {
        self.walker.as_ref()
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    /// Defaults to 512.
    pub fn max_concurrent_downloads(mut self, input: usize) -> Self {
        self.max_concurrent_downloads = Some(input);
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn set_max_concurrent_downloads(mut self, input: Option<usize>) -> Self {
        self.max_concurrent_downloads = input;
        self
    }

    /// Per-request cap on concurrently-materialized child download transfers.
    pub fn get_max_concurrent_downloads(&self) -> Option<usize> {
        self.max_concurrent_downloads
    }

    /// Consume the builder and construct a [`DownloadObjectsInput`].
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
        Ok(DownloadObjectsInput {
            bucket: self.bucket,
            destination: self.destination,
            delimiter: self.delimiter,
            failure_policy: self.failure_policy,
            walker: self.walker,
            max_concurrent_downloads: self.max_concurrent_downloads,
        })
    }
}

impl fmt::Debug for DownloadObjectsInputBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadObjectsInputBuilder")
            .field("bucket", &self.bucket)
            .field("destination", &self.destination)
            .field("delimiter", &self.delimiter)
            .field("failure_policy", &self.failure_policy)
            .field("walker", &self.walker.is_some())
            .field("max_concurrent_downloads", &self.max_concurrent_downloads)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::DownloadObjectsInput;
    use crate::types::FailedTransferPolicy;

    // `set_failure_policy` replaces the policy, and `None` restores the
    // default rather than keeping a previously set policy.
    #[test]
    fn set_failure_policy_replaces_and_none_restores_default() {
        let builder = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .set_failure_policy(Some(FailedTransferPolicy::Continue));
        assert_eq!(
            builder.get_failure_policy(),
            &FailedTransferPolicy::Continue
        );
        let input = builder
            .set_failure_policy(None)
            .build()
            .expect("valid input");
        assert_eq!(input.failure_policy(), &FailedTransferPolicy::Abort);
    }

    #[test]
    fn test_no_destination_error() {
        let err = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .build()
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Destination directory is required"));
    }

    #[test]
    fn test_no_bucket_error() {
        let err = DownloadObjectsInput::builder()
            .destination("/tmp/test")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("A bucket is required"));
    }

    /// Verify the pipeline-depth resolution expression used in `orchestrate()`
    /// clamps an explicit `0` to `1` (mirrors the global ConcurrencyMode::Explicit guard).
    #[test]
    fn max_concurrent_downloads_zero_clamped_to_one() {
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .max_concurrent_downloads(0)
            .build()
            .expect("valid input");
        // Replicate the resolution expression from `DownloadObjects::orchestrate`.
        let resolved = input
            .max_concurrent_downloads()
            .map(|n| n.max(1))
            .unwrap_or(crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN);
        assert_eq!(
            1, resolved,
            "an explicit 0 must be clamped to 1 to avoid wedging the transfer"
        );
    }

    #[test]
    fn max_concurrent_downloads_none_uses_default() {
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .build()
            .expect("valid input");
        let resolved = input
            .max_concurrent_downloads()
            .map(|n| n.max(1))
            .unwrap_or(crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN);
        assert_eq!(crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN, resolved);
    }

    #[test]
    fn max_concurrent_downloads_nonzero_preserved() {
        let input = DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination("/tmp/test")
            .max_concurrent_downloads(4)
            .build()
            .expect("valid input");
        let resolved = input
            .max_concurrent_downloads()
            .map(|n| n.max(1))
            .unwrap_or(crate::operation::DEFAULT_MAX_CONCURRENT_CHILDREN);
        assert_eq!(4, resolved);
    }
}
