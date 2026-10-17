/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Error types for the S3 Mock Server.
//!
//! Handlers and storage fail with an [`Error`], whose conversion into an `s3s::S3Error` is the
//! only place the server builds an S3 error response.

use std::borrow::Cow;
use std::io;
use std::sync::Arc;

use aws_smithy_checksums::ChecksumAlgorithm;
use http::StatusCode;
use s3s::S3ErrorCode;
use thiserror::Error;

use crate::bucket::BucketType;

/// Result type for S3 Mock Server operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for S3 Mock Server operations.
///
/// Each request error is answered with S3's error code. Errors of the server itself, such as
/// I/O errors, are answered with 500 `InternalError`.
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// The specified key does not exist.
    #[error("no such key")]
    NoSuchKey,

    /// The specified bucket does not exist.
    #[error("no such bucket")]
    NoSuchBucket,

    /// The specified upload does not exist.
    #[error("no such upload")]
    NoSuchUpload,

    /// A part to complete was not uploaded, or has another ETag.
    #[error("invalid part")]
    InvalidPart,

    /// A partNumber read names a part the object does not have.
    #[error("invalid part number")]
    InvalidPartNumber,

    /// The parts are not in the correct order.
    #[error("invalid part order")]
    InvalidPartOrder {
        /// The type of the upload's bucket: directory buckets word the error differently.
        bucket_type: BucketType,
    },

    /// A checksum or `Content-MD5` the request sent does not match the data.
    #[error("{} checksum mismatch", .algorithm.as_str())]
    BadDigest {
        /// The algorithm of the checksum, MD5 for `Content-MD5`.
        algorithm: ChecksumAlgorithm,
        /// Whether the checksum is the object checksum of a CompleteMultipartUpload.
        completion: bool,
    },

    /// A `Content-MD5` header is not the base64 encoding of an MD5 digest.
    #[error("invalid digest")]
    InvalidDigest,

    /// The range specified is invalid
    #[error("invalid range")]
    InvalidRange,

    /// An `If-Match` or `If-None-Match` condition did not hold.
    #[error("precondition failed")]
    PreconditionFailed,

    /// The bucket to delete has objects.
    #[error("bucket not empty")]
    BucketNotEmpty,

    /// The bucket name to create is taken.
    #[error("bucket already exists")]
    BucketAlreadyExists,

    /// The object's storage class does not allow the operation: a read of an archived object
    /// that has not been restored, or a restore of an object in another storage class.
    #[error("invalid object state")]
    InvalidObjectState {
        /// Whether the operation is a restore.
        restore: bool,
    },

    /// A restore of the object is already in progress.
    #[error("restore already in progress")]
    RestoreAlreadyInProgress,

    /// The request is invalid. Carries the message of the response, chosen where the request is
    /// checked.
    #[error("invalid request")]
    InvalidRequest(&'static str),

    /// An argument of the request is invalid. Carries the message of the response, chosen where
    /// the request is checked.
    #[error("invalid argument")]
    InvalidArgument(&'static str),

    /// The request's XML body does not match S3's schema.
    #[error("malformed XML")]
    MalformedXml,

    /// The request is not allowed.
    #[error("access denied")]
    AccessDenied,

    /// The throttle shed the request.
    #[error("slow down")]
    SlowDown,

    /// The server is already running.
    #[error("server already running")]
    ServerAlreadyRunning,

    /// The server failed to start.
    #[error("server failed to start: {0}")]
    ServerStartFailed(String),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[source] Arc<io::Error>),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// An internal error occurred.
    #[error("invalid server configuration: {0}")]
    InvalidConfiguration(String),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(Arc::new(error))
    }
}

/// S3's message for an internal error.
const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error. Please try again.";

/// The S3 error response to a request that fails with `error`, which it keeps as its source: the
/// code, HTTP status and message of each variant's response.
impl From<Error> for s3s::S3Error {
    fn from(error: Error) -> Self {
        let (code, status, message): (S3ErrorCode, StatusCode, Cow<'static, str>) = match &error {
            Error::NoSuchKey => (
                S3ErrorCode::NoSuchKey,
                StatusCode::NOT_FOUND,
                "The specified key does not exist.".into(),
            ),
            Error::NoSuchBucket => (
                S3ErrorCode::NoSuchBucket,
                StatusCode::NOT_FOUND,
                "The specified bucket does not exist".into(),
            ),
            Error::NoSuchUpload => (
                S3ErrorCode::NoSuchUpload,
                StatusCode::NOT_FOUND,
                "The specified upload does not exist. The upload ID may be invalid, or the upload \
                 may have been aborted or completed."
                    .into(),
            ),
            Error::InvalidPart => (
                S3ErrorCode::InvalidPart,
                StatusCode::BAD_REQUEST,
                "One or more of the specified parts could not be found.  The part may not have \
                 been uploaded, or the specified entity tag may not match the part's entity tag."
                    .into(),
            ),
            Error::InvalidPartNumber => (
                S3ErrorCode::Custom("InvalidPartNumber".into()),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "The requested partnumber is not satisfiable".into(),
            ),
            Error::InvalidPartOrder {
                bucket_type: BucketType::GeneralPurpose,
            } => (
                S3ErrorCode::InvalidPartOrder,
                StatusCode::BAD_REQUEST,
                "The list of parts was not in ascending order. Parts must be ordered by part \
                 number."
                    .into(),
            ),
            Error::InvalidPartOrder {
                bucket_type: BucketType::Directory,
            } => (
                S3ErrorCode::InvalidPartOrder,
                StatusCode::BAD_REQUEST,
                "The list of parts was not in ascending order or did not have consecutive part \
                 numbers. The parts list must be specified in order by part number, and must not \
                 have any gaps."
                    .into(),
            ),
            // CompleteMultipartUpload's message names the algorithm in lowercase.
            Error::BadDigest {
                algorithm,
                completion: true,
            } => (
                S3ErrorCode::BadDigest,
                StatusCode::BAD_REQUEST,
                format!(
                    "The {} you specified did not match the calculated checksum.",
                    algorithm.as_str()
                )
                .into(),
            ),
            #[allow(deprecated)]
            Error::BadDigest {
                algorithm: ChecksumAlgorithm::Md5,
                completion: false,
            } => (
                S3ErrorCode::BadDigest,
                StatusCode::BAD_REQUEST,
                "The Content-MD5 you specified did not match what we received.".into(),
            ),
            Error::BadDigest {
                algorithm,
                completion: false,
            } => (
                S3ErrorCode::BadDigest,
                StatusCode::BAD_REQUEST,
                format!(
                    "The {} you specified did not match the calculated checksum.",
                    algorithm.as_str().to_ascii_uppercase()
                )
                .into(),
            ),
            Error::InvalidDigest => (
                S3ErrorCode::InvalidDigest,
                StatusCode::BAD_REQUEST,
                "The Content-MD5 you specified was invalid.".into(),
            ),
            Error::InvalidRange => (
                S3ErrorCode::InvalidRange,
                StatusCode::RANGE_NOT_SATISFIABLE,
                "The requested range is not satisfiable".into(),
            ),
            Error::PreconditionFailed => (
                S3ErrorCode::PreconditionFailed,
                StatusCode::PRECONDITION_FAILED,
                "At least one of the pre-conditions you specified did not hold".into(),
            ),
            Error::BucketNotEmpty => (
                S3ErrorCode::BucketNotEmpty,
                StatusCode::CONFLICT,
                "The bucket you tried to delete is not empty".into(),
            ),
            Error::BucketAlreadyExists => (
                S3ErrorCode::BucketAlreadyExists,
                StatusCode::CONFLICT,
                "The requested bucket name is not available. The bucket namespace is shared by \
                 all users of the system. Please select a different name and try again."
                    .into(),
            ),
            Error::InvalidObjectState { restore: false } => (
                S3ErrorCode::InvalidObjectState,
                StatusCode::FORBIDDEN,
                "The operation is not valid for the object's storage class".into(),
            ),
            Error::InvalidObjectState { restore: true } => (
                S3ErrorCode::InvalidObjectState,
                StatusCode::FORBIDDEN,
                "Restore is not allowed for the object's current storage class".into(),
            ),
            Error::RestoreAlreadyInProgress => (
                S3ErrorCode::RestoreAlreadyInProgress,
                StatusCode::CONFLICT,
                "Object restore is already in progress".into(),
            ),
            Error::InvalidRequest(message) => (
                S3ErrorCode::InvalidRequest,
                StatusCode::BAD_REQUEST,
                (*message).into(),
            ),
            Error::InvalidArgument(message) => (
                S3ErrorCode::InvalidArgument,
                StatusCode::BAD_REQUEST,
                (*message).into(),
            ),
            Error::MalformedXml => (
                S3ErrorCode::MalformedXML,
                StatusCode::BAD_REQUEST,
                "The XML you provided was not well-formed or did not validate against our \
                 published schema"
                    .into(),
            ),
            Error::AccessDenied => (
                S3ErrorCode::AccessDenied,
                StatusCode::FORBIDDEN,
                "Access Denied".into(),
            ),
            Error::SlowDown => (
                S3ErrorCode::SlowDown,
                StatusCode::SERVICE_UNAVAILABLE,
                "Please reduce your request rate.".into(),
            ),
            Error::ServerAlreadyRunning
            | Error::ServerStartFailed(_)
            | Error::Io(_)
            | Error::Internal(_)
            | Error::InvalidConfiguration(_) => (
                S3ErrorCode::InternalError,
                StatusCode::INTERNAL_SERVER_ERROR,
                // S3's message does not describe the error, so the server adds it.
                format!("{INTERNAL_ERROR_MESSAGE} (s3-mock-server: {error})").into(),
            ),
        };
        let mut s3_error = s3s::S3Error::with_source(code, Box::new(error));
        s3_error.set_status_code(status);
        s3_error.set_message(message);
        s3_error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_errors_have_s3_message_and_keep_their_source() {
        let error = s3s::S3Error::from(Error::Internal("stream error".to_string()));
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
        assert_eq!(error.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(
            error.message(),
            Some(
                "We encountered an internal error. Please try again. (s3-mock-server: internal \
                 error: stream error)"
            )
        );
        assert_eq!(
            error.source().unwrap().to_string(),
            "internal error: stream error"
        );
    }
}
