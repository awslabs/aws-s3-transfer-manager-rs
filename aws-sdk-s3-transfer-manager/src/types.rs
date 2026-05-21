/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use core::fmt;
use std::sync::Arc;

use crate::metrics::{unit::ByteUnit, Throughput};

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

/// The concurrency mode the client should use for executing requests.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum ConcurrencyMode {
    /// Automatically configured concurrency based on the execution environment.
    #[default]
    Auto,

    /// Explicitly configured throughput setting the client should aim for.
    ///
    /// In this mode, concurrency is limited by attempting to hit a throughput target.
    TargetThroughput(TargetThroughput),

    /// Explicit concurrency control
    Explicit(usize),
}

/// Throughput target(s)
#[derive(Debug, Clone)]
pub struct TargetThroughput {
    download: Throughput,
    upload: Throughput,
}

impl TargetThroughput {
    /// Create a new target throughput in Gigabits/sec that is
    /// the same for both uploads and downloads.
    pub fn new_gigabits_per_sec(gbps: u64) -> Self {
        let target = Throughput::new_bytes_per_sec(gbps * ByteUnit::Gigabit.as_bytes_u64());
        Self::new(target, target)
    }

    // TODO: we don't actually support limiting throughput by upload/download independently (yet), it will require updates to scheduler.

    /// Create a new target throughput using the given upload and download
    /// throughputs.
    fn new(download_target: Throughput, upload_target: Throughput) -> Self {
        Self {
            download: download_target,
            upload: upload_target,
        }
    }

    /// Get the target throughput for uploads
    pub fn upload(&self) -> &Throughput {
        &self.upload
    }

    /// Get the target throughput for downloads
    pub fn download(&self) -> &Throughput {
        &self.download
    }
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
    pub fn upload_id(&self) -> Option<&str> {
        self.upload_id.as_deref()
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

/// Detailed information about a failed object download
#[non_exhaustive]
#[derive(Debug)]
pub struct FailedDownload {
    /// The input for the download object operation that failed
    pub(crate) input: crate::operation::download::DownloadInput,

    /// The error encountered downloading the object
    pub(crate) error: crate::error::Error,
}

impl FailedDownload {
    /// The input for the download object operation that failed
    pub fn input(&self) -> &crate::operation::download::DownloadInput {
        &self.input
    }

    /// The error encountered downloading the object
    pub fn error(&self) -> &crate::error::Error {
        &self.error
    }
}

/// Detailed information about a failed upload
#[non_exhaustive]
#[derive(Debug)]
pub struct FailedUpload {
    pub(crate) input: Option<crate::operation::upload::UploadInput>,
    pub(crate) error: crate::error::Error,
    /// Local filesystem path of the source file that failed to upload.
    pub(crate) source_path: Option<std::path::PathBuf>,
}

impl FailedUpload {
    /// The input for the failed object upload
    pub fn input(&self) -> Option<&crate::operation::upload::UploadInput> {
        self.input.as_ref()
    }

    /// The error encountered uploading the object
    pub fn error(&self) -> &crate::error::Error {
        &self.error
    }

    /// Local filesystem path of the source file that failed to upload.
    pub fn source_path(&self) -> Option<&std::path::Path> {
        self.source_path.as_deref()
    }
}

/// Status of a transfer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransferStatus {
    /// Transfer is actively running.
    Active,
    /// Transfer completed successfully.
    Completed,
    /// Transfer failed with an error.
    Failed,
    /// Transfer was cancelled.
    Cancelled,
}

impl TransferStatus {
    /// Returns true if the transfer has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Active)
    }
}

/// Snapshot of transfer progress and IO metrics.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct TransferMetrics {
    /// Bytes sent over network (upload payload).
    pub network_tx: u64,
    /// Bytes received from network (download payload).
    pub network_rx: u64,
    /// Bytes read from disk.
    pub disk_read: u64,
    /// Bytes written to disk.
    pub disk_write: u64,
    /// Expected total payload bytes, if known.
    pub total_bytes: Option<u64>,
    /// When the transfer was initiated.
    pub started_at: std::time::Instant,
    /// When the transfer reached a terminal state, if it has.
    pub finished_at: Option<std::time::Instant>,
}

/// Type of the bucket for the transfer
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub(crate) enum BucketType {
    Standard,
    Express,
}

impl BucketType {
    pub(crate) fn from_bucket_name(bucket_name: &str) -> Self {
        if bucket_name.ends_with("--x-s3") {
            BucketType::Express
        } else {
            BucketType::Standard
        }
    }
}

/// Internal bucket representation carrying the name and resolved kind.
#[derive(Debug, Clone)]
pub(crate) struct Bucket {
    name: String,
    kind: BucketType,
}

impl Bucket {
    pub(crate) fn new(name: String) -> Self {
        let kind = BucketType::from_bucket_name(&name);
        Self { name, kind }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn kind(&self) -> BucketType {
        self.kind
    }
}
