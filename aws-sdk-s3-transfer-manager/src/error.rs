/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

/// A boxed error that is `Send` and `Sync`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

use aws_sdk_s3::error::ProvideErrorMetadata;

/// Errors returned by this library
///
/// NOTE: Use [`aws_smithy_types::error::display::DisplayErrorContext`] or similar to display
/// the entire error cause/source chain.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    source: BoxError,
}

/// General categories of transfer errors.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Operation input validation issues
    InputInvalid,

    /// I/O errors
    IOError,

    /// Some kind of internal runtime issue (e.g. task failure, poisoned mutex, etc)
    RuntimeError,

    /// Object discovery failed
    ObjectNotDiscoverable,

    /// Failed to upload or download a chunk of an object
    ChunkFailed(ChunkFailed),

    /// Resource not found (e.g. bucket, key, multipart upload ID not found)
    NotFound,

    /// child operation failed (e.g. download of a single object as part of downloading all objects from a bucket)
    ChildOperationFailed,

    /// The operation is being canceled because the user explicitly called `.abort` on the handle,
    /// or a child operation failed with the abort policy.
    OperationCancelled,
}

/// Stores information about failed chunk
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkFailed {
    /// The ID of the chunk that failed.
    /// For downloads, this is the sequence number of the chunk,
    /// and for uploads, it is the upload ID.
    id: ChunkId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ChunkId {
    Download(u64),
    Upload(String),
}

impl ChunkFailed {
    // The sequence number of the chunk for download operation
    pub(crate) fn download_seq(&self) -> Option<u64> {
        match self.id {
            ChunkId::Download(seq) => Some(seq),
            _ => None,
        }
    }

    #[allow(dead_code)]
    // The upload ID of the chunk for upload operation
    pub(crate) fn upload_id(&self) -> Option<&str> {
        match &self.id {
            ChunkId::Upload(id) => Some(id),
            _ => None,
        }
    }
}

impl Error {
    /// Creates a new transfer [`Error`] from a known kind of error as well as an arbitrary error
    /// source.
    pub fn new<E>(kind: ErrorKind, err: E) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            kind,
            source: err.into(),
        }
    }

    /// Returns the corresponding [`ErrorKind`] for this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::InputInvalid => write!(f, "invalid input"),
            ErrorKind::IOError => write!(f, "I/O error"),
            ErrorKind::RuntimeError => write!(f, "runtime error"),
            ErrorKind::ObjectNotDiscoverable => write!(f, "object discovery failed"),
            ErrorKind::ChunkFailed(chunk_failed) => {
                write!(f, "failed to process chunk {:?}", chunk_failed.id)
            }
            ErrorKind::NotFound => write!(f, "resource not found"),
            ErrorKind::ChildOperationFailed => write!(f, "child operation failed"),
            ErrorKind::OperationCancelled => write!(f, "operation cancelled"),
        }
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

pub(crate) fn discovery_failed<E>(err: E) -> Error
where
    E: Into<BoxError>,
{
    Error::new(ErrorKind::ObjectNotDiscoverable, err)
}

pub(crate) fn chunk_failed<E>(id: ChunkId, err: E) -> Error
where
    E: Into<BoxError>,
{
    Error::new(ErrorKind::ChunkFailed(ChunkFailed { id }), err)
}

pub(crate) fn from_kind<E>(kind: ErrorKind) -> impl FnOnce(E) -> Error
where
    E: Into<BoxError>,
{
    |err| Error::new(kind, err)
}

impl<E, R> From<aws_sdk_s3::error::SdkError<E, R>> for Error
where
    E: std::error::Error + ProvideErrorMetadata + Send + Sync + 'static,
    R: Send + Sync + fmt::Debug + 'static,
{
    fn from(value: aws_sdk_s3::error::SdkError<E, R>) -> Self {
        // TODO - extract request id/metadata
        let kind = match value.code() {
            Some("NotFound" | "NoSuchKey" | "NoSuchUpload" | "NoSuchBucket") => ErrorKind::NotFound,
            // TODO - is this the rigth error kind? do we need something else?
            _ => ErrorKind::ChildOperationFailed,
        };

        Error::new(kind, value)
    }
}

static CANCELLATION_ERROR: &str =
    "at least one operation has been aborted, cancelling all ongoing requests";

pub(crate) fn operation_cancelled() -> Error {
    Error::new(ErrorKind::OperationCancelled, CANCELLATION_ERROR)
}
