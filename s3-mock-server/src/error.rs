/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Error types for the S3 Mock Server.
//!
//! Handlers, routes and storage fail with an [`Error`](enum@Error). Its conversion into an
//! `s3s::S3Error` builds every S3 error response, and [`Error::into_response`] writes the response
//! with the elements an `S3Error` cannot carry.

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

/// The error a request, or a call that seeds or inspects the server, fails with.
///
/// Each error is answered with S3's error code. Failures while serving a request, such as I/O
/// errors, are answered with 500 `InternalError`. Building and starting the server fail with a
/// [`ServerError`] instead.
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

    /// A request sets a storage class that the bucket does not have.
    #[error("invalid storage class")]
    InvalidStorageClass,

    /// The request uses functionality the server does not implement.
    #[error("not implemented")]
    NotImplemented,

    /// A request to a directory bucket sets `Content-MD5`, which directory buckets do not support.
    #[error("Content-MD5 not implemented")]
    ContentMd5NotImplemented,

    /// A key is longer than 1,024 bytes. Carries its length in bytes, reported in the error's
    /// `Size` element.
    #[error("key too long: {size} bytes")]
    KeyTooLong {
        /// The length of the key in bytes.
        size: usize,
    },

    /// A request's path or a key it names is not valid percent-encoded UTF-8.
    #[error("invalid URI")]
    InvalidUri,

    /// The request uses a parameter the bucket does not support.
    #[error("not supported: {0}")]
    NotSupported(&'static str),

    /// The range specified is invalid
    #[error("invalid range")]
    InvalidRange,

    /// An `If-Match` or `If-None-Match` condition did not hold.
    #[error("precondition failed")]
    PreconditionFailed,

    /// A RenameObject condition did not hold. Carries the name of the condition's header, reported
    /// in the error's `Condition` element.
    #[error("rename precondition failed: {0}")]
    RenamePreconditionFailed(&'static str),

    /// A RenameObject reuses the client token of an earlier rename with other parameters.
    #[error("idempotent parameter mismatch")]
    IdempotentParameterMismatch,

    /// The bucket to delete has objects.
    #[error("bucket not empty")]
    BucketNotEmpty,

    /// The bucket name to create is taken.
    #[error("bucket already exists")]
    BucketAlreadyExists,

    /// An append's write offset is not the current size of the object.
    #[error("invalid write offset")]
    InvalidWriteOffset,

    /// A chunked request body does not match its declared length, or its framing or trailer is
    /// malformed.
    #[error("incomplete body")]
    IncompleteBody,

    /// A chunk signature of a request body does not match.
    #[error("signature does not match")]
    SignatureDoesNotMatch,

    /// A request body does not match its `x-amz-content-sha256`.
    #[error("x-amz-content-sha256 mismatch")]
    XAmzContentSha256Mismatch,

    /// A chunk of a request body is larger than s3s accepts.
    #[error("entity too large")]
    EntityTooLarge,

    /// An append to an object that already has 10,000 parts.
    #[error("too many parts")]
    TooManyParts,

    /// A write does not carry a checksum of the required algorithm: an append, of an algorithm the
    /// object stores; an UploadPart, of the algorithm its upload was created with.
    #[error("checksum type mismatch: expected {expected}, got {actual}")]
    ChecksumTypeMismatch {
        /// Lowercase names of the algorithms required.
        expected: String,
        /// Lowercase name of the algorithm of the request's checksum. If it has none: `None`, or
        /// `null` in a general purpose bucket.
        actual: String,
        /// The type of the bucket. The message ends with a period only in a directory bucket.
        bucket_type: crate::BucketType,
    },

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

    /// The request has no body, and needs one.
    #[error("missing request body")]
    MissingRequestBody,

    /// The request is not allowed.
    #[error("access denied")]
    AccessDenied,

    /// A request names credentials the server does not have, or a session's credentials where the
    /// session is not valid.
    #[error("invalid access key ID")]
    InvalidAccessKeyId,

    /// The throttle shed the request.
    #[error("slow down")]
    SlowDown,

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[source] Arc<io::Error>),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// An error with any code, status and message, for faults with codes the server does not
    /// otherwise answer, such as 400 `RequestTimeout` or 503 `ServiceUnavailable`.
    #[error("{code}: {message}")]
    Custom {
        code: String,
        status: StatusCode,
        message: String,
    },
}

/// The error building or starting the server fails with.
#[derive(Error, Debug)]
pub enum ServerError {
    /// The server's configuration is invalid.
    #[error("invalid server configuration: {0}")]
    InvalidConfiguration(&'static str),

    /// An I/O error, such as binding the listening socket or creating the local-directory store.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(Arc::new(error))
    }
}

impl Error {
    /// The error for a request whose body could not be read:
    /// - the S3 error s3s reports for a body that does not match its headers: a wrong chunk
    ///   signature, payload hash, decoded length or chunk framing;
    /// - an internal error for any other failure, such as a client that disconnected.
    pub(crate) fn from_body_error(error: &(dyn std::error::Error + 'static)) -> Self {
        use s3s::stream::aws_chunked_stream::AwsChunkedStreamError;
        use s3s::stream::upload_stream::UploadStreamError;
        let mut cause = error;
        loop {
            if let Some(error) = cause.downcast_ref::<AwsChunkedStreamError>() {
                return match error {
                    AwsChunkedStreamError::Underlying(_) => break,
                    AwsChunkedStreamError::SignatureMismatch => Error::SignatureDoesNotMatch,
                    AwsChunkedStreamError::ChunkMetaTooLarge(..)
                    | AwsChunkedStreamError::ChunkDataTooLarge(..) => Error::EntityTooLarge,
                    _ => Error::IncompleteBody,
                };
            }
            if let Some(error) = cause.downcast_ref::<UploadStreamError>() {
                return match error {
                    UploadStreamError::Underlying(_) => break,
                    UploadStreamError::Sha256Mismatch => Error::XAmzContentSha256Mismatch,
                    UploadStreamError::LengthMismatch | UploadStreamError::Incomplete => {
                        Error::IncompleteBody
                    }
                };
            }
            // An `io::Error` wrapping another error does not report it as its source.
            let inner = cause
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .map(|inner| inner as &(dyn std::error::Error + 'static));
            match inner.or_else(|| cause.source()) {
                Some(next) => cause = next,
                None => break,
            }
        }
        Error::Internal(format!("Failed to read body: {error}"))
    }
}

/// S3's message for an internal error.
const INTERNAL_ERROR_MESSAGE: &str = "We encountered an internal error. Please try again.";

impl Error {
    /// The HTTP response for this error: the status, headers and XML error document of the
    /// `S3Error` it converts into, with the elements an `S3Error` cannot carry: `Condition` for a
    /// failed RenameObject condition, and `Size` and `MaxSizeAllowed` for a key that is too long.
    pub(crate) fn into_response(self) -> http::Response<s3s::Body> {
        let condition = match &self {
            Error::RenamePreconditionFailed(condition) => Some(*condition),
            _ => None,
        };
        let key_size = match &self {
            Error::KeyTooLong { size } => Some(*size),
            _ => None,
        };
        let error = s3s::S3Error::from(self);
        let mut body = Vec::new();
        let mut serializer = s3s::xml::Serializer::new(&mut body);
        serializer
            .decl()
            .and_then(|()| {
                serializer.element("Error", |s| {
                    s.content("Code", error.code().as_str())?;
                    s.content("Message", error.message().unwrap_or_default())?;
                    if let Some(condition) = condition {
                        s.content("Condition", condition)?;
                    }
                    if let Some(size) = key_size {
                        s.content("Size", &i64::try_from(size).unwrap_or(i64::MAX))?;
                        s.content("MaxSizeAllowed", &(crate::rename::MAX_KEY_LENGTH as i64))?;
                    }
                    Ok(())
                })
            })
            .expect("an error document serializes into memory");
        http::Response::builder()
            .status(
                error
                    .status_code()
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            )
            .header(http::header::CONTENT_TYPE, "application/xml")
            .body(s3s::Body::from(body))
            .expect("status and header are valid")
    }
}

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
            Error::InvalidStorageClass => (
                S3ErrorCode::InvalidStorageClass,
                StatusCode::BAD_REQUEST,
                "The storage class you specified is not valid".into(),
            ),
            Error::NotImplemented => (
                S3ErrorCode::NotImplemented,
                StatusCode::NOT_IMPLEMENTED,
                "A header you provided implies functionality that is not implemented".into(),
            ),
            Error::ContentMd5NotImplemented => (
                S3ErrorCode::NotImplemented,
                StatusCode::NOT_IMPLEMENTED,
                "This bucket does not support Content Md5 header.".into(),
            ),
            Error::KeyTooLong { .. } => (
                S3ErrorCode::KeyTooLongError,
                StatusCode::BAD_REQUEST,
                "Your key is too long".into(),
            ),
            Error::InvalidUri => (
                S3ErrorCode::InvalidURI,
                StatusCode::BAD_REQUEST,
                "The path is not valid percent-encoded UTF-8".into(),
            ),
            Error::NotSupported(message) => (
                S3ErrorCode::Custom("NotSupported".into()),
                StatusCode::NOT_IMPLEMENTED,
                (*message).into(),
            ),
            Error::InvalidRange => (
                S3ErrorCode::InvalidRange,
                StatusCode::RANGE_NOT_SATISFIABLE,
                "The requested range is not satisfiable".into(),
            ),
            Error::PreconditionFailed | Error::RenamePreconditionFailed(_) => (
                S3ErrorCode::PreconditionFailed,
                StatusCode::PRECONDITION_FAILED,
                "At least one of the pre-conditions you specified did not hold".into(),
            ),
            Error::IdempotentParameterMismatch => (
                S3ErrorCode::Custom("IdempotentParameterMismatch".into()),
                StatusCode::BAD_REQUEST,
                "Parameters on this idempotent request are inconsistent with parameters used in \
                 previous request(s)."
                    .into(),
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
            Error::TooManyParts => (
                S3ErrorCode::Custom("TooManyParts".into()),
                StatusCode::BAD_REQUEST,
                "You have attempted to add more parts than the maximum of 10000 that are allowed for \
                 this object. You can use the CopyObject operation to copy this object to another \
                 and then add more data to the newly copied object."
                    .into(),
            ),
            Error::IncompleteBody => (
                S3ErrorCode::IncompleteBody,
                StatusCode::BAD_REQUEST,
                "The request body terminated unexpectedly".into(),
            ),
            Error::SignatureDoesNotMatch => (
                S3ErrorCode::SignatureDoesNotMatch,
                StatusCode::FORBIDDEN,
                "The request signature we calculated does not match the signature you provided. \
                 Check your key and signing method."
                    .into(),
            ),
            Error::XAmzContentSha256Mismatch => (
                S3ErrorCode::Custom("XAmzContentSHA256Mismatch".into()),
                StatusCode::BAD_REQUEST,
                "The provided 'x-amz-content-sha256' header does not match what was computed.".into(),
            ),
            Error::EntityTooLarge => (
                S3ErrorCode::EntityTooLarge,
                StatusCode::BAD_REQUEST,
                "Your proposed upload exceeds the maximum allowed size".into(),
            ),
            Error::InvalidWriteOffset => (
                S3ErrorCode::Custom("InvalidWriteOffset".into()),
                StatusCode::BAD_REQUEST,
                "The write offset value that you specified does not match the current object size."
                    .into(),
            ),
            Error::ChecksumTypeMismatch {
                expected,
                actual,
                bucket_type,
            } => (
                S3ErrorCode::InvalidRequest,
                StatusCode::BAD_REQUEST,
                format!(
                    "Checksum Type mismatch occurred, expected checksum Type: {expected}, actual \
                     checksum Type: {actual}{}",
                    if *bucket_type == crate::BucketType::Directory {
                        "."
                    } else {
                        ""
                    }
                )
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
            Error::MissingRequestBody => (
                S3ErrorCode::MissingRequestBodyError,
                StatusCode::BAD_REQUEST,
                "Request Body is empty".into(),
            ),
            Error::AccessDenied => (
                S3ErrorCode::AccessDenied,
                StatusCode::FORBIDDEN,
                "Access Denied".into(),
            ),
            Error::InvalidAccessKeyId => (
                S3ErrorCode::InvalidAccessKeyId,
                StatusCode::FORBIDDEN,
                "The AWS Access Key Id you provided does not exist in our records.".into(),
            ),
            Error::SlowDown => (
                S3ErrorCode::SlowDown,
                StatusCode::SERVICE_UNAVAILABLE,
                "Please reduce your request rate.".into(),
            ),
            Error::Custom {
                code,
                status,
                message,
            } => (
                S3ErrorCode::Custom(code.clone().into()),
                *status,
                message.clone().into(),
            ),
            Error::Io(_) | Error::Internal(_) => (
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

    #[tokio::test]
    async fn error_responses_escape_xml() {
        let error = Error::Custom {
            code: "Custom".to_owned(),
            status: StatusCode::BAD_REQUEST,
            message: "a < b & c".to_owned(),
        };
        let body = error
            .into_response()
            .into_body()
            .store_all_limited(usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            body.contains("<Message>a &lt; b &amp; c</Message>"),
            "{body}"
        );
    }

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
