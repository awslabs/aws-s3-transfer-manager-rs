/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::{unit::ByteUnit, Throughput};
use crate::runtime::buffer_pool::BufferPool;

/// The target part size for an upload or download request.
#[non_exhaustive]
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

/// Upper bound on memory the transfer manager uses for in-flight and buffered
/// transfer data. At the limit transfers backpressure rather than fail.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum MemoryBudgetConfig {
    /// Use up to one quarter of detected effective memory, capped at 32 GiB.
    ///
    /// Effective memory includes process or container limits where the platform
    /// exposes them. The result is rounded down to complete carriers. If
    /// detection is unavailable, the pool uses 2 GiB.
    #[default]
    Auto,
    /// Use the given fraction of detected effective memory.
    ///
    /// The fraction must be finite and in `(0.0, 1.0]`. This policy requires
    /// memory detection and does not apply the automatic 32 GiB cap.
    Fraction(f64),
    /// An explicit byte limit that bypasses memory detection.
    ///
    /// The value must fund at least one carrier. Use the [`ByteUnit`] helpers,
    /// for example
    /// `Limit(2 * ByteUnit::Gibibyte.as_bytes_usize())`.
    Limit(usize),
}

/// Selects the payload-memory pool used by a transfer-manager client.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum MemoryConfig {
    /// Construct an automatically sized pool when the client is created.
    #[default]
    Auto,
    /// Use an existing pool.
    ///
    /// Every client or component using a clone of this pool shares one
    /// admission ceiling, prepared-storage cache, and metrics domain.
    Explicit(BufferPool),
}

/// Selects the execution runtime the transfer manager runs on.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum RuntimeMode {
    /// Managed OS threads owned by the transfer manager (default, recommended).
    ///
    /// Spawns and owns its own worker threads; the performance path.
    #[default]
    Managed,
    /// Run on the caller's tokio multi-threaded runtime instead of spawning
    /// dedicated threads.
    ///
    /// An escape hatch for embedding the transfer manager where it must not
    /// spawn its own OS threads. This is not a performance tuning knob —
    /// `Managed` is faster by design; prefer it unless you specifically need
    /// the transfer manager to run on your existing runtime.
    ///
    /// Requires a multi-threaded runtime: transfer workers run via
    /// `tokio::spawn`, so on a current-thread runtime they serialize onto one
    /// thread and throughput collapses.
    MultiThreadTokio,
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

/// How far a download may prefetch ahead of the consumer.
///
/// Read-ahead is speculative issuance: parts fetched before the consumer has read
/// up to them. This bounds that speculation. It does not gate in-order delivery or
/// override the memory budget; those are separate bounds.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReadAhead {
    /// Read ahead as far as available memory allows.
    #[default]
    Auto,

    /// Read ahead at most `n` parts beyond the consumer's position. `0` fetches only
    /// the part the consumer is waiting on (no speculation). Still bounded by memory.
    Parts(usize),
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
#[non_exhaustive]
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

/// Policy for how to handle a failure of any individual object in a transfer
/// involving multiple objects.
///
/// Default is to abort the transfer.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FailedTransferPolicy {
    /// Abort the transfer on any individual failure to upload or download an object
    #[default]
    Abort,
    /// Continue the transfer. Any failure will be logged and the details of all failed
    /// objects will be available in the output after the transfer completes.
    Continue,
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

/// Object-integrity information for a completed download.
///
/// Carries the object's reported checksum values (as returned by S3) and the
/// outcome of checksum validation over the delivered bytes. Validation is a
/// completion-time fact, distinct from the discovery-time [`ObjectMetadata`].
///
/// A checksum mismatch is not represented here; it surfaces as an `Err` from the
/// download's `join()`. On a successful `DownloadOutput`, [`ChecksumValidation`]
/// distinguishes "validated" from "validation did not happen".
///
/// [`ObjectMetadata`]: crate::operation::download::ObjectMetadata
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct IntegrityChecks {
    checksum_crc32: Option<String>,
    checksum_crc32c: Option<String>,
    checksum_crc64_nvme: Option<String>,
    checksum_sha1: Option<String>,
    checksum_sha256: Option<String>,
    checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
    checksum_validation: ChecksumValidation,
}

impl IntegrityChecks {
    /// The outcome of checksum validation over the delivered object bytes.
    pub fn checksum_validation(&self) -> &ChecksumValidation {
        &self.checksum_validation
    }

    /// The base64-encoded CRC-32 checksum reported by S3, if present.
    pub fn checksum_crc32(&self) -> Option<&str> {
        self.checksum_crc32.as_deref()
    }

    /// The base64-encoded CRC-32C checksum reported by S3, if present.
    pub fn checksum_crc32c(&self) -> Option<&str> {
        self.checksum_crc32c.as_deref()
    }

    /// The base64-encoded CRC-64/NVME checksum reported by S3, if present.
    pub fn checksum_crc64_nvme(&self) -> Option<&str> {
        self.checksum_crc64_nvme.as_deref()
    }

    /// The base64-encoded SHA-1 checksum reported by S3, if present.
    pub fn checksum_sha1(&self) -> Option<&str> {
        self.checksum_sha1.as_deref()
    }

    /// The base64-encoded SHA-256 checksum reported by S3, if present.
    pub fn checksum_sha256(&self) -> Option<&str> {
        self.checksum_sha256.as_deref()
    }

    /// The object's checksum type (full-object vs composite), if reported.
    pub fn checksum_type(&self) -> Option<&aws_sdk_s3::types::ChecksumType> {
        self.checksum_type.as_ref()
    }
}

/// Whether the bytes delivered by a download were validated against a checksum.
///
/// `Validated` means the *entire* delivered object was covered by a checksum and
/// matched. Anything short of whole-object coverage is `NotValidated` with a
/// [`NotValidatedReason`]; there is no partial state.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksumValidation {
    /// Every delivered byte was validated against a checksum and matched.
    #[non_exhaustive]
    Validated {
        /// The algorithm used to validate.
        algorithm: aws_sdk_s3::types::ChecksumAlgorithm,
    },
    /// No whole-object validation occurred. `reason` explains why.
    #[non_exhaustive]
    NotValidated {
        /// Why validation did not cover the whole object.
        reason: NotValidatedReason,
    },
}

/// Why a download's bytes were not whole-object checksum-validated.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotValidatedReason {
    /// Checksum validation was not enabled for the request (client
    /// `ResponseChecksumValidation::WhenRequired` with no request override).
    Disabled,
    /// The object carries only a composite (`-N`) checksum, which cannot be
    /// validated against delivered bytes.
    CompositeChecksum,
    /// Some delivered bytes were validated but not the whole object.
    PartialCoverage,
    /// No checksum covered the delivered bytes (object has no stored checksum,
    /// or the requested range did not align to a stored part).
    Unavailable,
}

impl IntegrityChecks {
    /// Build from the object's reported checksum fields and a resolved verdict.
    pub(crate) fn new(
        checksum_crc32: Option<String>,
        checksum_crc32c: Option<String>,
        checksum_crc64_nvme: Option<String>,
        checksum_sha1: Option<String>,
        checksum_sha256: Option<String>,
        checksum_type: Option<aws_sdk_s3::types::ChecksumType>,
        checksum_validation: ChecksumValidation,
    ) -> Self {
        Self {
            checksum_crc32,
            checksum_crc32c,
            checksum_crc64_nvme,
            checksum_sha1,
            checksum_sha256,
            checksum_type,
            checksum_validation,
        }
    }
}
