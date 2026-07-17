/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::ops::RangeInclusive;

use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::operation::RequestIdExt;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_types::retry::ErrorKind as RetryErrorKind;
use aws_types::request_id::RequestId;

use crate::types::{FailedDownload, FailedUpload};

/// A boxed error that is `Send` and `Sync`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors returned by this library.
///
/// NOTE: Use [`aws_smithy_types::error::display::DisplayErrorContext`] or similar to display
/// the entire error cause/source chain.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: BoxError,
    /// Optional metadata. `None` unless a builder attached service, chunk, or
    /// bulk-failure detail. Boxed to keep [`Error`] small.
    extra: Option<Box<ErrorExtra>>,
}

/// Optional metadata attached to an [`Error`].
#[derive(Debug, Default)]
struct ErrorExtra {
    /// Set when the error originated from an S3 service call.
    service: Option<ServiceMetadata>,
    /// Set when the error is attributable to a specific chunk of an object.
    location: Option<ChunkRef>,
    /// Per-object upload failures when this error aggregates a bulk upload.
    failed_uploads: Option<Vec<FailedUpload>>,
    /// Per-object download failures when this error aggregates a bulk download.
    failed_downloads: Option<Vec<FailedDownload>>,
    /// `true` when the underlying `SdkError` was a transient transport failure
    /// (a connect/read/write IO error or a client-side timeout) rather than a
    /// service response — a re-issuable failure the SDK's own retry may not have
    /// recovered (e.g. its shared retry token bucket was exhausted under a
    /// concurrent burst). Set at conversion from the typed `SdkError`, where the
    /// distinction is still available before type erasure. Never set for
    /// throttling/service errors — see `retry::classify_upload_part_retry`.
    transient_transport: bool,
}

/// General categories of transfer errors.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Operation input validation issues.
    InputInvalid,

    /// I/O errors. Covers local filesystem failures and transport-level failures
    /// reading an object body. When a body read fails for a specific chunk,
    /// [`Error::chunk`] identifies it.
    IOError,

    /// Some kind of internal runtime issue (e.g. task failure, poisoned mutex).
    RuntimeError,

    /// Object discovery failed for a reason that is not a service-call error, such
    /// as a response missing required metadata.
    ObjectNotDiscoverable,

    /// A request to S3 failed. The originating operation, service error code,
    /// message, and request ids are available via the accessors on [`Error`]
    /// ([`Error::operation_name`], [`Error::code`], [`Error::request_id`]).
    ServiceError,

    /// Object integrity validation failed: the received bytes did not match the
    /// expected checksum. See the wrapped [`IntegrityError`] for detail.
    IntegrityError(IntegrityError),

    /// A child of a bulk transfer (`upload_objects` / `download_objects`) failed.
    /// The individual failures are available via [`Error::failed_uploads`] /
    /// [`Error::failed_downloads`].
    ChildOperationFailed,

    /// The operation was cancelled, either because the user called `.abort()` on the
    /// handle, or a child operation failed under the abort policy.
    OperationCancelled,
}

/// Detail about an integrity (checksum) validation failure.
///
/// Each value is `Some` when it could be determined from the underlying
/// validation error. The underlying error is available via
/// [`std::error::Error::source`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IntegrityError {
    algorithm: Option<ChecksumAlgorithm>,
    expected: Option<String>,
    computed: Option<String>,
}

impl IntegrityError {
    /// The checksum algorithm that failed validation, if known.
    pub fn algorithm(&self) -> Option<&ChecksumAlgorithm> {
        self.algorithm.as_ref()
    }

    /// The checksum S3 reported for the object, base64-encoded, if known.
    pub fn expected(&self) -> Option<&str> {
        self.expected.as_deref()
    }

    /// The checksum computed over the received bytes, base64-encoded, if known.
    pub fn computed(&self) -> Option<&str> {
        self.computed.as_deref()
    }
}

/// Identifies the chunk of an object a failure is attributable to.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ChunkRef {
    seq: u64,
    range: Option<RangeInclusive<u64>>,
}

impl ChunkRef {
    pub(crate) fn new(seq: u64, range: Option<RangeInclusive<u64>>) -> Self {
        Self { seq, range }
    }

    /// Sequence number of the chunk within the transfer.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Byte range of the chunk within the object, if known.
    pub fn byte_range(&self) -> Option<&RangeInclusive<u64>> {
        self.range.as_ref()
    }
}

/// Service-call detail read from the concrete `SdkError`.
#[derive(Debug)]
struct ServiceMetadata {
    operation: &'static str,
    code: Option<String>,
    message: Option<String>,
    request_id: Option<String>,
    extended_request_id: Option<String>,
}

impl Error {
    /// Creates a new transfer [`Error`] from a known kind and an arbitrary source.
    pub fn new<E>(kind: ErrorKind, err: E) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            kind,
            source: err.into(),
            extra: None,
        }
    }

    /// Returns the corresponding [`ErrorKind`] for this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Test-only: a `ServiceError` flagged as transient transport, mirroring what
    /// `service_error` produces for an IO `DispatchFailure` (which cannot be
    /// constructed directly in a unit test).
    #[cfg(test)]
    pub(crate) fn test_transient_transport() -> Error {
        Error {
            kind: ErrorKind::ServiceError,
            source: "injected transient transport".into(),
            extra: Some(Box::new(ErrorExtra {
                transient_transport: true,
                ..Default::default()
            })),
        }
    }

    /// Test-only: a `ServiceError` carrying the given service error code,
    /// mirroring what `service_error` produces from an `SdkError` (which cannot
    /// be constructed directly in a unit test). Lets retry classifiers be tested
    /// against a code (e.g. throttle detection) without a live wire error.
    #[cfg(test)]
    pub(crate) fn test_service_error(code: &str) -> Error {
        Error {
            kind: ErrorKind::ServiceError,
            source: "injected service error".into(),
            extra: Some(Box::new(ErrorExtra {
                service: Some(ServiceMetadata {
                    operation: "TestOperation",
                    code: Some(code.to_owned()),
                    message: None,
                    request_id: None,
                    extended_request_id: None,
                }),
                ..Default::default()
            })),
        }
    }

    fn service(&self) -> Option<&ServiceMetadata> {
        self.extra.as_ref().and_then(|e| e.service.as_ref())
    }

    /// Lazily get a mutable reference to the boxed extra, allocating it on first use.
    fn extra_mut(&mut self) -> &mut ErrorExtra {
        self.extra
            .get_or_insert_with(|| Box::new(ErrorExtra::default()))
    }

    /// The S3 operation that produced this error, if it originated from a service
    /// call (e.g. `"GetObject"`, `"HeadObject"`, `"PutObject"`).
    pub fn operation_name(&self) -> Option<&str> {
        self.service().map(|m| m.operation)
    }

    /// The service error code, if any (e.g. `"NoSuchKey"`, `"AccessDenied"`).
    pub fn code(&self) -> Option<&str> {
        self.service().and_then(|m| m.code.as_deref())
    }

    /// The service error message, if any.
    pub fn message(&self) -> Option<&str> {
        self.service().and_then(|m| m.message.as_deref())
    }

    /// The AWS request id of the failed call, if any.
    pub fn request_id(&self) -> Option<&str> {
        self.service().and_then(|m| m.request_id.as_deref())
    }

    /// The AWS extended request id of the failed call, if any.
    pub fn extended_request_id(&self) -> Option<&str> {
        self.service()
            .and_then(|m| m.extended_request_id.as_deref())
    }

    /// Whether the service error code denotes a missing bucket, key, or
    /// multipart upload.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self.code(),
            Some("NotFound" | "NoSuchKey" | "NoSuchUpload" | "NoSuchBucket")
        )
    }

    /// Whether this error was a transient transport failure (connection IO error
    /// or client-side timeout) as opposed to a service response. Such failures
    /// are safe to re-issue and may not have been recovered by the SDK's own
    /// retry (e.g. its shared retry token bucket was exhausted under a concurrent
    /// burst). Always `false` for throttling and modeled service errors.
    pub(crate) fn is_transient_transport(&self) -> bool {
        self.extra.as_ref().is_some_and(|e| e.transient_transport)
    }

    /// The chunk this failure is attributable to, if known.
    pub fn chunk(&self) -> Option<&ChunkRef> {
        self.extra.as_ref().and_then(|e| e.location.as_ref())
    }

    /// Per-object upload failures, when this error aggregates a bulk upload.
    pub fn failed_uploads(&self) -> Option<&[FailedUpload]> {
        self.extra
            .as_ref()
            .and_then(|e| e.failed_uploads.as_deref())
    }

    /// Per-object download failures, when this error aggregates a bulk download.
    pub fn failed_downloads(&self) -> Option<&[FailedDownload]> {
        self.extra
            .as_ref()
            .and_then(|e| e.failed_downloads.as_deref())
    }

    /// Attaches chunk location, preserving kind and source.
    pub(crate) fn with_chunk(mut self, location: ChunkRef) -> Self {
        self.extra_mut().location = Some(location);
        self
    }

    /// Attaches per-object upload failures, preserving kind and source.
    pub(crate) fn with_failed_uploads(mut self, failed: Vec<FailedUpload>) -> Self {
        self.extra_mut().failed_uploads = Some(failed);
        self
    }

    /// Attaches per-object download failures, preserving kind and source.
    pub(crate) fn with_failed_downloads(mut self, failed: Vec<FailedDownload>) -> Self {
        self.extra_mut().failed_downloads = Some(failed);
        self
    }
}

impl fmt::Display for Error {
    // Renders this error's own context only. Source detail (the underlying
    // SdkError message, checksum values) is reached through
    // [`std::error::Error::source`], e.g. via `DisplayErrorContext`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::InputInvalid => write!(f, "invalid input")?,
            ErrorKind::IOError => write!(f, "I/O error")?,
            ErrorKind::RuntimeError => write!(f, "runtime error")?,
            ErrorKind::ObjectNotDiscoverable => write!(f, "object discovery failed")?,
            ErrorKind::ChildOperationFailed => {
                write!(f, "child operation failed")?;
                // A bulk transfer failed on one or more objects. Name the first
                // failure and the total so the top-level message is actionable;
                // the full per-object detail (including each object's own error)
                // is reached via `failed_uploads` / `failed_downloads`.
                let first_and_count = self
                    .failed_uploads()
                    .map(|f| {
                        let id = f
                            .first()
                            .and_then(|u| u.source_path())
                            .map(|p| p.display().to_string());
                        (id, f.len())
                    })
                    .or_else(|| {
                        self.failed_downloads().map(|f| {
                            let id = f.first().and_then(|d| d.input().key()).map(str::to_owned);
                            (id, f.len())
                        })
                    });
                if let Some((id, n)) = first_and_count {
                    match id {
                        Some(id) => write!(f, ": {id}")?,
                        None => write!(f, ": 1 object")?,
                    }
                    if n > 1 {
                        write!(f, " (and {} more)", n - 1)?;
                    }
                }
            }
            ErrorKind::OperationCancelled => write!(f, "operation cancelled")?,
            ErrorKind::ServiceError => {
                write!(f, "service error")?;
                if let Some(m) = self.service() {
                    write!(f, " calling {}", m.operation)?;
                    if let Some(c) = &m.code {
                        write!(f, " ({c})")?;
                    }
                }
            }
            ErrorKind::IntegrityError(ie) => {
                write!(f, "integrity error")?;
                if let Some(a) = &ie.algorithm {
                    write!(f, " ({a})")?;
                }
            }
        }

        if let Some(loc) = self.chunk() {
            match &loc.range {
                Some(r) => write!(f, " [chunk {}, bytes {}-{}]", loc.seq, r.start(), r.end())?,
                None => write!(f, " [chunk {}]", loc.seq)?,
            }
        }

        // Request ids are not part of the underlying SdkError's own Display, so
        // include them here to keep a logged error traceable.
        if let Some(m) = self.service() {
            match (&m.request_id, &m.extended_request_id) {
                (Some(rid), Some(ext)) => {
                    write!(f, " (request id: {rid}, extended request id: {ext})")?
                }
                (Some(rid), None) => write!(f, " (request id: {rid})")?,
                _ => {}
            }
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<crate::io::error::Error> for Error {
    fn from(value: crate::io::error::Error) -> Self {
        Self::new(ErrorKind::IOError, value)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new(ErrorKind::IOError, value)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::new(ErrorKind::RuntimeError, value)
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error
where
    T: Send + Sync + 'static,
{
    fn from(value: std::sync::PoisonError<T>) -> Self {
        Self::new(ErrorKind::RuntimeError, value)
    }
}

impl From<aws_smithy_types::error::operation::BuildError> for Error {
    fn from(value: aws_smithy_types::error::operation::BuildError) -> Self {
        Self::new(ErrorKind::InputInvalid, value)
    }
}

pub(crate) fn invalid_input<E>(err: E) -> Error
where
    E: Into<BoxError>,
{
    Error::new(ErrorKind::InputInvalid, err)
}

/// Object discovery failed for a non-service reason (e.g. a response missing
/// required metadata). Service-call failures during discovery convert from their
/// `SdkError` and become [`ErrorKind::ServiceError`].
pub(crate) fn discovery_failed<E>(err: E) -> Error
where
    E: Into<BoxError>,
{
    Error::new(ErrorKind::ObjectNotDiscoverable, err)
}

pub(crate) fn from_kind<E>(kind: ErrorKind) -> impl FnOnce(E) -> Error
where
    E: Into<BoxError>,
{
    |err| Error::new(kind, err)
}

/// Maps a body-stream error to an [`Error`]. A checksum mismatch becomes
/// [`ErrorKind::IntegrityError`]; any other stream failure becomes
/// [`ErrorKind::IOError`]. `algorithm` records which checksum was being validated
/// when known.
pub(crate) fn body_read_error<E>(err: E, algorithm: Option<ChecksumAlgorithm>) -> Error
where
    E: Into<BoxError>,
{
    let boxed = err.into();
    if let Some((expected, computed)) = checksum_mismatch_values(boxed.as_ref()) {
        return Error {
            kind: ErrorKind::IntegrityError(IntegrityError {
                algorithm,
                expected: Some(expected),
                computed: Some(computed),
            }),
            source: boxed,
            extra: None,
        };
    }
    Error::new(ErrorKind::IOError, boxed)
}

/// Walks the source chain for a smithy checksum-mismatch error, returning its
/// expected and computed checksums base64-encoded.
///
/// TODO(vnext): the `downcast_ref` couples us to a specific `aws-smithy-checksums`
/// version. It is `0.x`, so a minor bump produces a distinct `TypeId` and the
/// downcast silently misses (a mismatch would then misclassify as retryable I/O).
/// Replace with a stable, version-independent classification once smithy-rs
/// exposes one: https://github.com/smithy-lang/smithy-rs/issues/4718. The
/// `corrupt_body_*` integration tests assert `ErrorKind::IntegrityError` against
/// the linked version, so a break is CI-visible rather than silent in the interim.
fn checksum_mismatch_values(err: &(dyn std::error::Error + 'static)) -> Option<(String, String)> {
    use aws_smithy_checksums::body::validate::Error as ChecksumError;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = source {
        if let Some(ChecksumError::ChecksumMismatch { expected, actual }) =
            e.downcast_ref::<ChecksumError>()
        {
            return Some((
                aws_smithy_types::base64::encode(expected),
                aws_smithy_types::base64::encode(actual),
            ));
        }
        source = e.source();
    }
    None
}

/// Reads the operation name and service code/message/request ids into a
/// [`ServiceMetadata`].
fn service_metadata<E, R>(operation: &'static str, e: &SdkError<E, R>) -> ServiceMetadata
where
    E: ProvideErrorMetadata,
    SdkError<E, R>: RequestId + RequestIdExt,
{
    ServiceMetadata {
        operation,
        code: e.code().map(str::to_owned),
        message: e.message().map(str::to_owned),
        request_id: e.request_id().map(str::to_owned),
        extended_request_id: e.extended_request_id().map(str::to_owned),
    }
}

/// Whether a single [`ConnectorError`] frame denotes a transient transport
/// failure: an IO error, a client-side timeout, or one the http client tagged
/// [`RetryErrorKind::TransientError`] (an incomplete/truncated response, TLS
/// negotiation timeout, and similar). A bare `Other(None)` connector error
/// carries no marker and is not transient on its own.
fn connector_error_is_transient(conn: &ConnectorError) -> bool {
    conn.is_io() || conn.is_timeout() || conn.as_other() == Some(RetryErrorKind::TransientError)
}

/// Whether an `SdkError` is a transient transport failure — a connection-level
/// IO error, a client-side timeout, or a connector error the http client tagged
/// [`RetryErrorKind::TransientError`] (e.g. an incomplete/truncated HTTP
/// response) — rather than a service response. Captured here because the
/// flattening into [`ErrorKind::ServiceError`] erases the `SdkError` variant.
///
/// Walks the error source chain rather than inspecting only the outermost
/// [`ConnectorError`]. A nested dispatch can re-wrap a fully-classified inner
/// connector error inside an outer `ConnectorError::Other(None)`, which carries
/// no transient marker; the SDK's own `TransientErrorClassifier` inspects only
/// that outer frame and misclassifies the error as non-retryable. Scanning every
/// `ConnectorError` in the chain recovers the inner classification.
///
/// Deliberately excludes service responses: a 503 `SlowDown` arrives as a
/// `ServiceError`, not a `DispatchFailure`, so throttling is never classified
/// transient here (see `retry::classify_upload_part_retry` for why that matters).
fn is_sdk_transient_transport<E, R>(e: &SdkError<E, R>) -> bool {
    if let SdkError::TimeoutError(_) = e {
        return true;
    }
    // A DispatchFailure carries a ConnectorError; scan it and every nested
    // ConnectorError in the source chain for a transient classification.
    let conn = match e {
        SdkError::DispatchFailure(df) => df.as_connector_error(),
        _ => return false,
    };
    let Some(conn) = conn else { return false };
    if connector_error_is_transient(conn) {
        return true;
    }
    let mut source = std::error::Error::source(conn);
    while let Some(err) = source {
        if let Some(conn) = err.downcast_ref::<ConnectorError>() {
            if connector_error_is_transient(conn) {
                return true;
            }
        }
        source = err.source();
    }
    false
}

/// Converts an `SdkError` into a [`ErrorKind::ServiceError`], capturing the
/// operation name, service metadata, and whether it was a transient transport
/// failure.
fn service_error<E>(
    operation: &'static str,
    e: SdkError<E, aws_smithy_runtime_api::client::orchestrator::HttpResponse>,
) -> Error
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let service = service_metadata(operation, &e);
    let transient_transport = is_sdk_transient_transport(&e);
    Error {
        kind: ErrorKind::ServiceError,
        source: Box::new(e),
        extra: Some(Box::new(ErrorExtra {
            service: Some(service),
            transient_transport,
            ..Default::default()
        })),
    }
}

macro_rules! from_sdk_error {
    ($err:ty, $op:literal) => {
        impl From<SdkError<$err, aws_smithy_runtime_api::client::orchestrator::HttpResponse>>
            for Error
        {
            fn from(
                e: SdkError<$err, aws_smithy_runtime_api::client::orchestrator::HttpResponse>,
            ) -> Self {
                service_error($op, e)
            }
        }
    };
}

from_sdk_error!(GetObjectError, "GetObject");
from_sdk_error!(HeadObjectError, "HeadObject");
from_sdk_error!(PutObjectError, "PutObject");
from_sdk_error!(UploadPartError, "UploadPart");
from_sdk_error!(CreateMultipartUploadError, "CreateMultipartUpload");
from_sdk_error!(CompleteMultipartUploadError, "CompleteMultipartUpload");
from_sdk_error!(AbortMultipartUploadError, "AbortMultipartUpload");
from_sdk_error!(ListObjectsV2Error, "ListObjectsV2");

impl From<crate::io::walk::WalkError> for Error {
    /// Maps a directory-walk failure to a transfer error, preserving the
    /// `WalkError` as the source so its classification and path remain reachable
    /// via [`std::error::Error::source`].
    ///
    /// A `ListObjectsV2` service failure is recovered to a full
    /// [`ErrorKind::ServiceError`] (with operation, code, and request ids); an
    /// unreadable or non-directory source root is [`ErrorKind::InputInvalid`];
    /// per-entry filesystem failures are [`ErrorKind::IOError`].
    fn from(e: crate::io::walk::WalkError) -> Self {
        use crate::io::walk::WalkErrorKind;
        match e.kind() {
            WalkErrorKind::Service => {
                // The S3 walker's only service call is ListObjectsV2, so the
                // boxed source downcasts to that SdkError.
                match e.into_source().downcast::<SdkError<
                    ListObjectsV2Error,
                    aws_smithy_runtime_api::client::orchestrator::HttpResponse,
                >>() {
                    Ok(sdk) => Error::from(*sdk),
                    Err(src) => Error::new(ErrorKind::ObjectNotDiscoverable, src),
                }
            }
            WalkErrorKind::SourceUnreadable | WalkErrorKind::NotADirectory => {
                Error::new(ErrorKind::InputInvalid, e)
            }
            WalkErrorKind::Io
            | WalkErrorKind::PermissionDenied
            | WalkErrorKind::BrokenSymlink
            | WalkErrorKind::SymlinkCycle => Error::new(ErrorKind::IOError, e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a ServiceError with the given metadata for Display tests.
    fn service(
        operation: &'static str,
        code: Option<&str>,
        request_id: Option<&str>,
        extended_request_id: Option<&str>,
    ) -> Error {
        Error {
            kind: ErrorKind::ServiceError,
            source: "boom".into(),
            extra: Some(Box::new(ErrorExtra {
                service: Some(ServiceMetadata {
                    operation,
                    code: code.map(str::to_owned),
                    message: Some("the message".to_owned()),
                    request_id: request_id.map(str::to_owned),
                    extended_request_id: extended_request_id.map(str::to_owned),
                }),
                ..Default::default()
            })),
        }
    }

    #[test]
    fn display_plain_kind() {
        let e = Error::new(ErrorKind::OperationCancelled, "x");
        assert_eq!(e.to_string(), "operation cancelled");
    }

    #[test]
    fn display_service_error_full() {
        let e = service(
            "GetObject",
            Some("NoSuchKey"),
            Some("REQ123"),
            Some("EXT456"),
        );
        assert_eq!(
            e.to_string(),
            "service error calling GetObject (NoSuchKey) (request id: REQ123, extended request id: EXT456)"
        );
    }

    #[test]
    fn display_service_error_request_id_only() {
        let e = service("PutObject", Some("AccessDenied"), Some("REQ123"), None);
        assert_eq!(
            e.to_string(),
            "service error calling PutObject (AccessDenied) (request id: REQ123)"
        );
    }

    #[test]
    fn display_service_error_no_metadata() {
        let e = service("HeadObject", None, None, None);
        assert_eq!(e.to_string(), "service error calling HeadObject");
    }

    #[test]
    fn display_omits_source_message() {
        // The underlying message is reached via source(), not Display.
        let e = service("GetObject", Some("NoSuchKey"), None, None);
        assert!(!e.to_string().contains("the message"));
    }

    #[test]
    fn display_integrity_error() {
        let e = Error {
            kind: ErrorKind::IntegrityError(IntegrityError {
                algorithm: Some(ChecksumAlgorithm::Crc32),
                expected: Some("AAAA".to_owned()),
                computed: Some("BBBB".to_owned()),
            }),
            source: "mismatch".into(),
            extra: None,
        };
        // Algorithm is shown; expected/computed are reached via accessors/source.
        assert_eq!(e.to_string(), "integrity error (CRC32)");
    }

    #[test]
    fn display_with_chunk() {
        let e = Error::new(ErrorKind::IOError, "x").with_chunk(ChunkRef::new(3, Some(0..=1023)));
        assert_eq!(e.to_string(), "I/O error [chunk 3, bytes 0-1023]");
    }

    #[test]
    fn display_chunk_no_range() {
        let e = Error::new(ErrorKind::IOError, "x").with_chunk(ChunkRef::new(7, None));
        assert_eq!(e.to_string(), "I/O error [chunk 7]");
    }

    #[test]
    fn is_not_found_matches_codes() {
        assert!(service("GetObject", Some("NoSuchKey"), None, None).is_not_found());
        assert!(!service("GetObject", Some("AccessDenied"), None, None).is_not_found());
    }

    #[test]
    fn display_child_operation_failed_bare() {
        // No attached failure list: the bare kind message, no trailing detail.
        let e = Error::new(ErrorKind::ChildOperationFailed, "x");
        assert_eq!(e.to_string(), "child operation failed");
    }

    #[test]
    fn display_child_operation_failed_names_first_and_count() {
        let failed = vec![
            FailedUpload {
                input: None,
                error: Error::new(ErrorKind::ServiceError, "boom"),
                source_path: Some(std::path::PathBuf::from("/data/a.bin")),
            },
            FailedUpload {
                input: None,
                error: Error::new(ErrorKind::ServiceError, "boom"),
                source_path: Some(std::path::PathBuf::from("/data/b.bin")),
            },
        ];
        let e = Error::new(ErrorKind::ChildOperationFailed, "aborted").with_failed_uploads(failed);
        assert_eq!(
            e.to_string(),
            "child operation failed: /data/a.bin (and 1 more)"
        );
    }

    #[test]
    fn display_child_operation_failed_single_no_path() {
        // A failure list carrying no source path still reports a count.
        let failed = vec![FailedUpload {
            input: None,
            error: Error::new(ErrorKind::ServiceError, "boom"),
            source_path: None,
        }];
        let e = Error::new(ErrorKind::ChildOperationFailed, "aborted").with_failed_uploads(failed);
        assert_eq!(e.to_string(), "child operation failed: 1 object");
    }

    // --- transient-transport classification -----------------------------------

    use aws_sdk_s3::operation::get_object::GetObjectError;
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;

    fn dispatch(conn: ConnectorError) -> SdkError<GetObjectError, HttpResponse> {
        SdkError::dispatch_failure(conn)
    }

    #[test]
    fn transient_nested_transient_marker_under_other_none_is_transient() {
        // The Windows failure shape: the http client classified an incomplete
        // response as ConnectorError::Other(TransientError), but a nested dispatch
        // re-wrapped it inside an outer ConnectorError::Other(None). The outer
        // frame carries no marker; the classifier must walk the chain to the inner
        // marker. Inspecting only the outer frame (the previous behavior) would
        // classify this non-transient and abort a retryable error.
        let inner = ConnectorError::other(
            "incomplete message".into(),
            Some(RetryErrorKind::TransientError),
        );
        let inner_sdk: SdkError<GetObjectError, HttpResponse> = SdkError::dispatch_failure(inner);
        let outer = ConnectorError::other(Box::new(inner_sdk), None);
        assert!(
            is_sdk_transient_transport(&dispatch(outer)),
            "a TransientError marker nested under an outer Other(None) must classify transient"
        );
    }

    #[test]
    fn transient_outer_io_is_transient() {
        assert!(is_sdk_transient_transport(&dispatch(ConnectorError::io(
            "reset".into()
        ))));
    }

    #[test]
    fn transient_outer_timeout_is_transient() {
        assert!(is_sdk_transient_transport(&dispatch(
            ConnectorError::timeout("connect".into())
        )));
    }

    #[test]
    fn transient_bare_other_none_is_not_transient() {
        // A connector Other(None) with no transient marker anywhere in the chain
        // is not transient — the fix must not blanket-retry every Other error.
        assert!(!is_sdk_transient_transport(&dispatch(
            ConnectorError::other("unclassified".into(), None)
        )));
    }

    #[test]
    fn transient_user_error_is_not_transient() {
        // A user error (e.g. malformed request) must never be treated transient.
        assert!(!is_sdk_transient_transport(&dispatch(
            ConnectorError::user("bad request".into())
        )));
    }
}
