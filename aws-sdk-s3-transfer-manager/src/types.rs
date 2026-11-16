/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt::{self, Display};
use std::ops;
use std::str::FromStr;
use std::time::Duration;

pub use crate::model::builders;
pub use crate::model::{
    ArchiveStatus, ChecksumAlgorithm, ChecksumMode, ChecksumType, ChunkMetadata, DownloadInput,
    Object, ObjectCannedAcl, ObjectLockLegalHoldStatus, ObjectLockMode, ObjectMetadata,
    ObjectStorageClass, Owner, ReplicationStatus, RequestCharged, RequestPayer, RestoreStatus,
    ServerSideEncryption, StorageClass, UploadInput, UploadOutput,
};

/// Policy for how to handle a failed multipart upload
///
/// Default is to abort the upload.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub enum FailedMultipartUploadPolicy {
    /// Abort the upload on any individual part failure
    #[default]
    AbortUpload,
    /// Leave the multipart upload open in S3, with any uploaded parts, instead of
    /// sending AbortMultipartUpload.
    ///
    /// When the upload fails, the error that
    /// [`UploadHandle::join`](crate::operation::upload::UploadHandle::join) returns carries the
    /// upload ID in [`Error::upload_id`](crate::error::Error::upload_id). After
    /// [`UploadHandle::abort`](crate::operation::upload::UploadHandle::abort), an
    /// [`AbortedUpload`] whose outcome is [`AbortOutcome::Retained`] carries it. S3 keeps, and
    /// bills for, the parts until the multipart upload is completed or aborted with that ID.
    Retain,
}

/// Describes the result of aborting an in-progress upload.
#[derive(Debug, Default)]
pub struct AbortedUpload {
    pub(crate) upload_id: Option<String>,
    pub(crate) request_charged: Option<crate::model::RequestCharged>,
    pub(crate) outcome: AbortOutcome,
    pub(crate) output: Option<crate::operation::upload::UploadOutput>,
}

/// What an abort established about the object.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AbortOutcome {
    /// The upload was stopped before S3 completes the object.
    #[default]
    Cancelled,
    /// S3 finished storing the object before the abort took effect. The object is not
    /// deleted; its metadata is available from [`AbortedUpload::output`].
    AlreadyCompleted,
    /// The upload was stopped before S3 completed the object, and its multipart upload
    /// was left open.
    Retained,
}

impl AbortedUpload {
    /// Returns whether the abort stopped the upload or the object already
    /// exists.
    pub fn outcome(&self) -> AbortOutcome {
        self.outcome
    }

    /// Returns the upload's output when [`outcome`](Self::outcome) is
    /// [`AbortOutcome::AlreadyCompleted`], and `None` otherwise.
    pub fn output(&self) -> Option<&crate::operation::upload::UploadOutput> {
        self.output.as_ref()
    }
    /// Get the multipart upload ID that was aborted, or that was left open when
    /// [`outcome`](Self::outcome) is [`AbortOutcome::Retained`].
    ///
    /// Not present for uploads that did not utilize a multipart upload
    pub fn upload_id(&self) -> Option<&str> {
        self.upload_id.as_deref()
    }

    /// If present, indicates that the requester was successfully charged for the request.
    ///
    /// This functionality is not supported for directory buckets and is
    /// not present for uploads that did not utilize a multipart upload
    pub fn request_charged(&self) -> &Option<crate::model::RequestCharged> {
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
    checksum_type: Option<crate::model::ChecksumType>,
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
    pub fn checksum_type(&self) -> Option<&crate::model::ChecksumType> {
        self.checksum_type.as_ref()
    }
}

/// Whether the bytes a download delivered were validated against S3's checksums.
///
/// Validation checks the bytes as they were received from S3, not the destination
/// they were written to. `Validated` means a response checksum covered every
/// delivered byte, and every one of those checksums matched. Anything less is
/// `NotValidated` with a [`NotValidatedReason`]; partial coverage is
/// `NotValidated { reason: PartialCoverage }`, not a separate state.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChecksumValidation {
    /// A response checksum covered every delivered byte, and every one of those
    /// checksums matched.
    #[non_exhaustive]
    Validated {
        /// The algorithm of the checksum that covered the lowest-offset delivered bytes.
        algorithm: crate::model::ChecksumAlgorithm,
    },
    /// Not every delivered byte was validated. `reason` says why.
    #[non_exhaustive]
    NotValidated {
        /// Why not every delivered byte was validated.
        reason: NotValidatedReason,
    },
}

/// Why not every byte a download delivered was validated.
///
/// When more than one reason holds, the download reports the first in this order:
/// `Disabled`, `PartialCoverage`, `CompositeChecksum`, `UnsupportedAlgorithm`,
/// `Unavailable`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotValidatedReason {
    /// Validation was not enabled: the request did not set `ChecksumMode::Enabled`,
    /// and the client's `ResponseChecksumValidation` is `WhenRequired`.
    Disabled,
    /// A response carried only a part-level checksum, a value ending in `-N` such as
    /// a composite upload's object checksum, which cannot be checked against the
    /// bytes the response delivered.
    CompositeChecksum,
    /// Some delivered bytes were validated, and some were not.
    PartialCoverage,
    /// No response carried a checksum that could be checked, for example because the
    /// object has no stored checksum. Also reported when no bytes were delivered.
    Unavailable,
    /// A response carried checksums only in algorithms the SDK does not compute,
    /// such as MD5.
    UnsupportedAlgorithm,
}

impl IntegrityChecks {
    /// Build from the object's reported checksum fields and a resolved verdict.
    pub(crate) fn new(
        checksum_crc32: Option<String>,
        checksum_crc32c: Option<String>,
        checksum_crc64_nvme: Option<String>,
        checksum_sha1: Option<String>,
        checksum_sha256: Option<String>,
        checksum_type: Option<crate::model::ChecksumType>,
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

// ===== Value types =====
/// A quantity of bytes.
///
/// Used wherever configuration takes a size, so the unit is part of the value
/// instead of the reader's guess. Positions, counters and buffer lengths stay
/// integers.
///
/// ```
/// use aws_sdk_s3_transfer_manager::types::ByteSize;
///
/// const PART: ByteSize = ByteSize::mib(8);
/// assert_eq!(PART.as_u64(), 8 * 1024 * 1024);
/// assert_eq!(PART.to_string(), "8 MiB");
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(u64);

impl ByteSize {
    /// A size of `bytes` bytes.
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// A size of `n` kibibytes (`n` * 2<sup>10</sup> bytes).
    ///
    /// # Panics
    ///
    /// Panics if the byte count exceeds `u64::MAX`. In a `const` context that
    /// is a compile error.
    pub const fn kib(n: u64) -> Self {
        Self(mul(n, ByteUnit::Kibibyte.as_bytes_u64()))
    }

    /// A size of `n` mebibytes (`n` * 2<sup>20</sup> bytes).
    ///
    /// # Panics
    ///
    /// Panics if the byte count exceeds `u64::MAX`. In a `const` context that
    /// is a compile error.
    pub const fn mib(n: u64) -> Self {
        Self(mul(n, ByteUnit::Mebibyte.as_bytes_u64()))
    }

    /// A size of `n` gibibytes (`n` * 2<sup>30</sup> bytes).
    ///
    /// # Panics
    ///
    /// Panics if the byte count exceeds `u64::MAX`. In a `const` context that
    /// is a compile error.
    pub const fn gib(n: u64) -> Self {
        Self(mul(n, ByteUnit::Gibibyte.as_bytes_u64()))
    }

    /// This size in bytes.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// This size in `unit`, as an `f64`.
    pub fn as_unit(self, unit: ByteUnit) -> f64 {
        unit.convert(self.0)
    }

    /// Returns a type that formats this size in a fixed `unit`.
    pub fn display_as(self, unit: ByteUnit) -> ByteSizeDisplayContext {
        ByteSizeDisplayContext {
            total_bytes: self.0,
            unit,
        }
    }
}

const fn mul(n: u64, unit_bytes: u64) -> u64 {
    match n.checked_mul(unit_bytes) {
        Some(bytes) => bytes,
        None => panic!("byte count exceeds u64::MAX"),
    }
}

impl fmt::Display for ByteSize {
    /// Formats in the largest binary unit that keeps the value at or above 1.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units = &[ByteUnit::Gibibyte, ByteUnit::Mebibyte, ByteUnit::Kibibyte];
        let mut unit = ByteUnit::Byte;
        for u in units {
            if self.0 >= u.as_bytes_u64() {
                unit = *u;
                break;
            }
        }
        fmt::Display::fmt(&self.display_as(unit), f)
    }
}

impl From<ByteSize> for u64 {
    fn from(size: ByteSize) -> u64 {
        size.0
    }
}

/// A unit a [`ByteSize`] or [`Throughput`] is displayed in.
///
/// The set is uneven: decimal bit units (`Kilobit`, `Megabit`, `Gigabit`) for
/// rates, and binary byte units (`Kibibyte`, `Mebibyte`, `Gibibyte`) for sizes.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteUnit {
    /// 1 byte
    Byte,
    /// 1000 bits (125 bytes)
    Kilobit,
    /// 2<sup>10</sup> bytes.
    Kibibyte,
    /// 125 * 10<sup>3</sup> bytes.
    Megabit,
    /// 2<sup>20</sup> bytes.
    Mebibyte,
    /// 125 * 10<sup>6</sup> bytes.
    Gigabit,
    /// 2<sup>30</sup> bytes.
    Gibibyte,
}

impl ByteUnit {
    /// Convert some number of bytes into this unit as an `f64`
    pub(crate) fn convert(&self, bytes: u64) -> f64 {
        bytes as f64 / self.as_bytes_u64() as f64
    }

    /// The number of bytes represented by this unit
    pub(crate) const fn as_bytes_u64(&self) -> u64 {
        match self {
            ByteUnit::Byte => 1,
            ByteUnit::Kilobit => 125,
            ByteUnit::Kibibyte => 1 << 10,
            ByteUnit::Megabit => 125_000,
            ByteUnit::Mebibyte => 1 << 20,
            ByteUnit::Gigabit => 125_000_000,
            ByteUnit::Gibibyte => 1 << 30,
        }
    }

    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            ByteUnit::Byte => "B",
            ByteUnit::Kilobit => "Kb",
            ByteUnit::Kibibyte => "KiB",
            ByteUnit::Megabit => "Mb",
            ByteUnit::Mebibyte => "MiB",
            ByteUnit::Gigabit => "Gb",
            ByteUnit::Gibibyte => "GiB",
        }
    }
}

impl AsRef<str> for ByteUnit {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for ByteUnit {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let unit = match s {
            "B" => ByteUnit::Byte,
            "Kb" => ByteUnit::Kilobit,
            "KiB" => ByteUnit::Kibibyte,
            "Mb" => ByteUnit::Megabit,
            "MiB" => ByteUnit::Mebibyte,
            "Gb" => ByteUnit::Gigabit,
            "GiB" => ByteUnit::Gibibyte,
            _ => {
                return Err(crate::error::invalid_input(format!(
                    "unknown byte unit '{}'",
                    s
                )))
            }
        };

        Ok(unit)
    }
}

/// Formats a byte count in a fixed [`ByteUnit`].
///
/// A count that is a whole multiple of the unit is displayed as an integer;
/// any other count is displayed with the formatter's precision, three
/// decimal places by default.
#[derive(Debug)]
pub struct ByteSizeDisplayContext {
    total_bytes: u64,
    unit: ByteUnit,
}

impl fmt::Display for ByteSizeDisplayContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.total_bytes.is_multiple_of(self.unit.as_bytes_u64()) {
            let converted = self.total_bytes / self.unit.as_bytes_u64();
            return write!(f, "{converted} {}", self.unit.as_str());
        }
        let precision = f.precision().unwrap_or(3);
        write!(
            f,
            "{1:.*} {2:}",
            precision,
            self.unit.convert(self.total_bytes),
            self.unit.as_str()
        )
    }
}

/// Measured bytes transferred over some duration
#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    bytes_transferred: u64,
    elapsed: Duration,
}

impl Throughput {
    /// Create a new throughput measurement with the given bytes read and time elapsed
    pub const fn new(bytes_transferred: u64, elapsed: Duration) -> Throughput {
        Throughput {
            bytes_transferred,
            elapsed,
        }
    }

    /// Create a new throughput measurement assuming a one second duration
    ///
    /// This is convenience for:
    ///
    /// ```
    /// use std::time::Duration;
    /// use aws_sdk_s3_transfer_manager::types::{ByteSize, Throughput};
    /// let bytes_transferred = ByteSize::mib(5).as_u64();
    /// assert_eq!(
    ///     Throughput::new(bytes_transferred, Duration::from_secs(1)),
    ///     Throughput::new_bytes_per_sec(bytes_transferred)
    /// );
    /// ```
    pub const fn new_bytes_per_sec(bytes_transferred: u64) -> Throughput {
        Self::new(bytes_transferred, Duration::from_secs(1))
    }

    /// Convert this throughput into a specific unit per second
    pub fn as_unit_per_sec(&self, unit: ByteUnit) -> f64 {
        (self.bytes_transferred as f64 / unit.as_bytes_u64() as f64) / self.elapsed.as_secs_f64()
    }

    /// Convert this throughput into bytes / sec
    pub fn as_bytes_per_sec(&self) -> f64 {
        self.as_unit_per_sec(ByteUnit::Byte)
    }

    /// Total bytes transferred
    pub const fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred
    }

    /// Returns a type that can be used to format/display this throughput in a particular unit
    pub fn display_as(&self, unit: ByteUnit) -> ThroughputDisplayContext<'_> {
        ThroughputDisplayContext {
            throughput: self,
            unit,
        }
    }
}

impl PartialEq for Throughput {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes_per_sec() == other.as_bytes_per_sec()
    }
}

impl PartialOrd for Throughput {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_bytes_per_sec()
            .partial_cmp(&other.as_bytes_per_sec())
    }
}

/// Add two throughputs
impl ops::Add for Throughput {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() + rhs.as_bytes_per_sec();

        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Subtract throughput
impl ops::Sub for Throughput {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() - rhs.as_bytes_per_sec();
        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Multiply throughput by a scalar
impl ops::Mul<u64> for Throughput {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred * rhs, self.elapsed)
    }
}

/// Divide throughput by a scalar
impl ops::Div<u64> for Throughput {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred / rhs, self.elapsed)
    }
}

impl fmt::Display for Throughput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = ThroughputDisplayContext {
            throughput: self,
            unit: ByteUnit::Megabit,
        };
        Display::fmt(&context, f)
    }
}

/// Formats a [`Throughput`] as a rate in a fixed [`ByteUnit`] per
/// second.
///
/// Created by [`Throughput::display_as`]. The rate is displayed with the
/// formatter's precision when one is given.
#[derive(Debug)]
pub struct ThroughputDisplayContext<'a> {
    throughput: &'a Throughput,
    unit: ByteUnit,
}

impl fmt::Display for ThroughputDisplayContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(precision) = f.precision() {
            write!(
                f,
                "{1:.*} {2:}/s",
                precision,
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        } else {
            write!(
                f,
                "{} {}/s",
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::TargetThroughput;

    #[test]
    fn target_throughput_accepts_finite_positive_gbps() {
        for gbps in [0.5, 1.0, 200.0] {
            let target = TargetThroughput::gbps(gbps).unwrap();
            assert_eq!(target.as_gbps(), gbps);
        }
    }

    #[test]
    fn target_throughput_rejects_values_that_are_not_a_rate() {
        for gbps in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(TargetThroughput::gbps(gbps).is_err(), "gbps {gbps}");
        }
    }
}

#[cfg(test)]
mod value_type_tests {
    use super::{ByteSize, ByteUnit, Throughput};
    use std::str::FromStr;
    use std::time::Duration;

    /// Every unit reports the same value through its byte and bit accessors,
    /// at both widths. The byte values are the unit definitions; a regression
    /// in either derivation changes one of the pairs.
    #[test]
    fn byte_unit_accessors_agree() {
        let units = [
            (ByteUnit::Byte, 1u64),
            (ByteUnit::Kilobit, 125),
            (ByteUnit::Kibibyte, 1024),
            (ByteUnit::Megabit, 125_000),
            (ByteUnit::Mebibyte, 1024 * 1024),
            (ByteUnit::Gigabit, 125_000_000),
            (ByteUnit::Gibibyte, 1024 * 1024 * 1024),
        ];
        for (unit, bytes) in units {
            assert_eq!(unit.as_bytes_u64(), bytes, "{unit:?} bytes");
        }
    }

    #[test]
    fn byte_size_constructors_and_display() {
        assert_eq!(ByteSize::kib(2).as_u64(), 2 * 1024);
        assert_eq!(ByteSize::mib(8).as_u64(), 8 * 1024 * 1024);
        assert_eq!(ByteSize::gib(3).as_u64(), 3 * 1024 * 1024 * 1024);

        // the largest binary unit that keeps the value at or above 1
        assert_eq!(ByteSize::new(512).to_string(), "512 B");
        assert_eq!(ByteSize::kib(4).to_string(), "4 KiB");
        assert_eq!(ByteSize::mib(8).to_string(), "8 MiB");
        assert_eq!(ByteSize::new(1_610_612_736).to_string(), "1.500 GiB");

        // a fixed unit on request
        assert_eq!(
            ByteSize::gib(1).display_as(ByteUnit::Mebibyte).to_string(),
            "1024 MiB"
        );
        assert_eq!(ByteSize::mib(1).as_unit(ByteUnit::Kibibyte), 1024.0);
    }

    #[test]
    #[should_panic(expected = "byte count exceeds u64::MAX")]
    fn byte_size_overflow_panics() {
        let _ = ByteSize::gib(u64::MAX);
    }

    #[test]
    fn test_throughput_display() {
        // default
        assert_eq!(
            "1 Mb/s",
            format!("{}", Throughput::new(125_000, Duration::from_secs(1)))
        );

        let t = Throughput::new(1_000_000, Duration::from_secs(1));

        // explicit
        assert_eq!("1000000 B/s", format!("{}", t.display_as(ByteUnit::Byte)));
        assert_eq!("8000 Kb/s", format!("{}", t.display_as(ByteUnit::Kilobit)));
        assert_eq!(
            "976.5625 KiB/s",
            format!("{}", t.display_as(ByteUnit::Kibibyte))
        );
        assert_eq!("8 Mb/s", format!("{}", t.display_as(ByteUnit::Megabit)));
        assert_eq!(
            "0.954 MiB/s",
            format!("{:.3}", t.display_as(ByteUnit::Mebibyte))
        );
        assert_eq!("0.008 Gb/s", format!("{}", t.display_as(ByteUnit::Gigabit)));
        assert_eq!(
            "0.00093 GiB/s",
            format!("{:.5}", t.display_as(ByteUnit::Gibibyte))
        );
    }

    #[test]
    fn test_from_str() {
        let units = &[
            ByteUnit::Byte,
            ByteUnit::Kilobit,
            ByteUnit::Kibibyte,
            ByteUnit::Megabit,
            ByteUnit::Mebibyte,
            ByteUnit::Gigabit,
            ByteUnit::Gibibyte,
        ];

        for u in units {
            let u2 = ByteUnit::from_str(u.as_str()).unwrap();
            assert_eq!(*u, u2);
        }

        assert!(ByteUnit::from_str("kb").is_err());
    }

    #[test]
    fn test_ops() {
        let t = Throughput::new_bytes_per_sec(ByteSize::mib(10).as_u64());
        let t2 = Throughput::new_bytes_per_sec(ByteSize::mib(5).as_u64());

        assert_eq!(
            t + t2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() + t2.bytes_transferred())
        );
        assert_eq!(t - t2, t2);
        assert_eq!(
            t * 2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() * 2)
        );
        assert_eq!(t / 2, t2);
    }

    #[test]
    fn test_byte_display_context() {
        assert_eq!("1 KiB", format!("{}", ByteSize::new(1024)));
        assert_eq!("1 MiB", format!("{}", ByteSize::new(1024 * 1024)));
        assert_eq!("1 GiB", format!("{}", ByteSize::new(1024 * 1024 * 1024)));

        assert_eq!("727 B", format!("{}", ByteSize::new(727)));
        assert_eq!(
            "0.710 KiB",
            format!("{}", ByteSize::new(727).display_as(ByteUnit::Kibibyte))
        );
        assert_eq!("3.420 KiB", format!("{}", ByteSize::new(3502)));
        assert_eq!("3.41992 KiB", format!("{:.5}", ByteSize::new(3502)));

        assert_eq!("7.201 MiB", format!("{}", ByteSize::new(7550498)));
        assert_eq!(
            "0.007 GiB",
            format!("{}", ByteSize::new(7550498).display_as(ByteUnit::Gibibyte))
        );

        assert_eq!("1.016 GiB", format!("{}", ByteSize::new(1091242563)));
        assert_eq!(
            "1040.690 MiB",
            format!(
                "{}",
                ByteSize::new(1091242563).display_as(ByteUnit::Mebibyte)
            )
        );
    }
}
