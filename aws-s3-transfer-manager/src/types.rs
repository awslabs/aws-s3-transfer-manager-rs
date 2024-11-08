/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use core::fmt;
use std::{borrow::Cow, fs::Metadata, path::Path, sync::Arc};

/// The target part size for an upload or download request.
#[derive(Debug, Clone, Default)]
pub enum PartSize {
    /// Automatically configure an optimal target part size based on the execution environment.
    #[default]
    Auto,

    /// Target part size explicitly given.
    ///
    /// NOTE: This is a suggestion and will be used if possible but may be adjusted for an individual request
    /// as required by the underlying API.
    Target(u64),
}

/// The concurrency settings to use for a single upload or download request.
#[derive(Debug, Clone, Default)]
pub enum ConcurrencySetting {
    /// Automatically configure an optimal concurrency setting based on the execution environment.
    #[default]
    Auto,

    /// Explicitly configured concurrency setting.
    Explicit(usize),
}

/// Policy for how to handle a failed multipart upload
///
/// Default is to abort the upload.
#[derive(Debug, Clone, Default)]
pub enum FailedMultipartUploadPolicy {
    /// Abort the upload on any individual part failure
    #[default]
    AbortUpload,
    /// Retain any uploaded parts. The upload ID will be available in the response.
    Retain,
}

/// Describes the result of aborting an in-progress upload.
#[derive(Debug, Default)]
pub struct AbortedUpload {
    pub(crate) upload_id: Option<String>,
    pub(crate) request_charged: Option<aws_sdk_s3::types::RequestCharged>,
}

impl AbortedUpload {
    /// Get the multipart upload ID that was cancelled
    ///
    /// Not present for uploads that did not utilize a multipart upload
    pub fn upload_id(&self) -> &Option<String> {
        &self.upload_id
    }

    /// If present, indicates that the requester was successfully charged for the request.
    ///
    /// This functionality is not supported for directory buckets and is
    /// not present for uploads that did not utilize a multipart upload
    pub fn request_charged(&self) -> &Option<aws_sdk_s3::types::RequestCharged> {
        &self.request_charged
    }
}

/// Policy for how to handle a failure of any indiviudal object in a transfer
/// involving multiple objects.
///
/// Default is to abort the transfer.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FailedTransferPolicy {
    /// Abort the transfer on any individual failure to upload or download an object
    #[default]
    Abort,
    /// Continue the transfer. Any failure will be logged and the details of all failed
    /// objects will be available in the output after the transfer completes.
    Continue,
}

/// A filter for downloading objects from S3
#[derive(Clone)]
pub struct DownloadFilter {
    pub(crate) predicate: Arc<dyn Fn(&aws_sdk_s3::types::Object) -> bool + Send + Sync + 'static>,
}

impl fmt::Debug for DownloadFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("DownloadFilter");
        formatter.field("predicate", &"<closure>");
        formatter.finish()
    }
}

impl<F> From<F> for DownloadFilter
where
    F: Fn(&aws_sdk_s3::types::Object) -> bool + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        DownloadFilter {
            predicate: Arc::new(value),
        }
    }
}

impl Default for DownloadFilter {
    fn default() -> Self {
        Self {
            predicate: Arc::new(all_objects_filter),
        }
    }
}

/// Filter that returns all non-folder objects. A folder is a 0-byte object created
/// when a customer uses S3 console to create a folder, and it always ends with '/'.
fn all_objects_filter(obj: &aws_sdk_s3::types::Object) -> bool {
    let key = obj.key().unwrap_or("");
    let is_folder = key.ends_with('/') && obj.size().is_some() && obj.size().unwrap() == 0;
    !is_folder
}

/// Detailed information about a failed object download transfer
#[non_exhaustive]
#[derive(Debug)]
pub struct FailedDownloadTransfer {
    /// The input for the download object operation that failed
    pub(crate) input: crate::operation::download::DownloadInput,

    /// The error encountered downloading the object
    pub(crate) error: crate::error::Error,
}

impl FailedDownloadTransfer {
    /// The input for the download object operation that failed
    pub fn input(&self) -> &crate::operation::download::DownloadInput {
        &self.input
    }

    /// The error encountered downloading the object
    pub fn error(&self) -> &crate::error::Error {
        &self.error
    }
}

/// A filter for choosing which objects to upload to S3.
#[derive(Clone)]
pub struct UploadFilter {
    pub(crate) predicate: Arc<dyn Fn(&UploadFilterItem<'_>) -> bool + Send + Sync + 'static>,
}

impl fmt::Debug for UploadFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut formatter = f.debug_struct("UploadFilter");
        formatter.field("predicate", &"<closure>");
        formatter.finish()
    }
}

impl<F> From<F> for UploadFilter
where
    F: Fn(&UploadFilterItem<'_>) -> bool + Send + Sync + 'static,
{
    fn from(value: F) -> Self {
        UploadFilter {
            predicate: Arc::new(value),
        }
    }
}

fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .map(|name| name.to_string_lossy().starts_with('.'))
        .unwrap_or(false)
}

impl Default for UploadFilter {
    fn default() -> Self {
        Self {
            predicate: Arc::new(|item| {
                if is_hidden(item.path()) {
                    return false;
                }
                if !item.metadata.is_file() {
                    return false;
                }
                // At this point, `item` is a file that is not hidden.
                // If `symlink_metadata` is `Some`, we know the upload operation does not follow symbolic links.
                // The filter then retains `item` if it is not a symbolic link.
                item.symlink_metadata
                    .as_ref()
                    .map_or(true, |symlink_metadata| !symlink_metadata.is_symlink())
            }),
        }
    }
}

/// An item passed to [`UploadFilter`] for evaluation
#[non_exhaustive]
#[derive(Debug)]
pub struct UploadFilterItem<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) metadata: Metadata,
    pub(crate) symlink_metadata: Option<Metadata>,
}

impl<'a> UploadFilterItem<'a> {
    pub(crate) fn builder() -> UploadFilterItemBuilder<'a> {
        UploadFilterItemBuilder::default()
    }

    /// Full path to the file
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Metadata about the file located at `self.path`.
    ///
    /// Use this `Metadata` for queries like `is_dir()` and `is_file()`.
    /// Note that calling `is_symlink()` on this `Metadata` may not return the correct result,
    /// since it was obtained using `fs::metadata()`, which follows symlinks.
    ///
    /// To check if `self.path` is a symbolic link, use [`Self::symlink_metadata`] and
    /// call `.is_symlink()` on the returned `Metadata` if it is `Some`.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Metadata about the file located at `self.path` without following a symbolic link.
    ///
    /// Use this `Metadata` to query `is_symlink()`. It returns `None` if the upload operation
    /// does not follow symbolic links.
    pub fn symlink_metadata(&self) -> Option<&Metadata> {
        self.symlink_metadata.as_ref()
    }
}

#[derive(Debug, Default)]
pub(crate) struct UploadFilterItemBuilder<'a> {
    pub(crate) path: Option<Cow<'a, Path>>,
    pub(crate) metadata: Option<Metadata>,
    pub(crate) symlink_metadata: Option<Metadata>,
}

impl<'a> UploadFilterItemBuilder<'a> {
    // Set the full path for a path entry to be filtered.
    //
    // NOTE: A path is required.
    pub(crate) fn path(mut self, path: impl Into<Cow<'a, Path>>) -> Self {
        self.path = Some(path.into());
        self
    }

    // Set the `Metadata` for `self.path`, obtained via a call to `fs::metadata()`,
    // which follows symbolic links.
    //
    // NOTE: A metadata for the path is required.
    pub(crate) fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    // Set the `Metadata` for `self.path`, obtained via a call to `fs::symlink_metadata()`,
    // which does not follow symbolic links.
    //
    // This method MUST be called when the upload operation DOES NOT follow symbolic links,
    // as it determines whether the operation should skip a path entry if it is a symlink.
    pub(crate) fn symlink_metadata(mut self, symlink_metadata: Metadata) -> Self {
        self.symlink_metadata = Some(symlink_metadata);
        self
    }

    pub(crate) fn build(self) -> UploadFilterItem<'a> {
        UploadFilterItem {
            path: self.path.expect("required field `path` should be set"),
            metadata: self
                .metadata
                .expect("required field `metadata` should be set"),
            symlink_metadata: self.symlink_metadata,
        }
    }
}

/// Detailed information about a failed upload
#[non_exhaustive]
#[derive(Debug)]
pub struct FailedUploadTransfer {
    pub(crate) input: Option<crate::operation::upload::UploadInput>,
    pub(crate) error: crate::error::Error,
}

// TODO - Omit "Transfer" from struct name?
// "Transfer" is generic for "upload or download" but this already has "Upload" in the name
impl FailedUploadTransfer {
    /// The input for the failed object upload
    pub fn input(&self) -> Option<&crate::operation::upload::UploadInput> {
        self.input.as_ref()
    }

    /// The error encountered uploading the object
    pub fn error(&self) -> &crate::error::Error {
        &self.error
    }
}
