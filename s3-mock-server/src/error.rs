/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
//! Error types for the S3 Mock Server.

use std::io;
use thiserror::Error;

/// Result type for S3 Mock Server operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for S3 Mock Server operations.
#[derive(Error, Debug)]
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

    /// The specified part does not exist.
    #[error("no such part")]
    NoSuchPart,

    /// The part is invalid (e.g., ETag mismatch).
    #[error("invalid part")]
    InvalidPart,

    /// The parts are not in the correct order.
    #[error("invalid part order")]
    InvalidPartOrder,

    /// Checksum validation failed.
    #[error("checksum mismatch: {0}")]
    ChecksumMismatch(String),

    /// The range specified is invalid
    #[error("invalid range")]
    InvalidRange,

    /// The server is already running.
    #[error("server already running")]
    ServerAlreadyRunning,

    /// The server failed to start.
    #[error("server failed to start: {0}")]
    ServerStartFailed(String),

    /// An I/O error occurred.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// An internal error occurred.
    #[error("internal error: {0}")]
    Internal(String),

    /// An `If-Match` or `If-None-Match` condition did not hold.
    #[error("precondition failed")]
    PreconditionFailed,

    /// An internal error occurred.
    #[error("invalid server configuration: {0}")]
    InvalidConfiguration(String),
}

impl From<Error> for s3s::S3Error {
    fn from(error: Error) -> Self {
        let error_kind = match error {
            Error::NoSuchKey => s3s::S3ErrorCode::NoSuchKey,
            Error::NoSuchBucket => s3s::S3ErrorCode::NoSuchBucket,
            Error::NoSuchUpload => s3s::S3ErrorCode::NoSuchUpload,
            Error::NoSuchPart | Error::InvalidPart => s3s::S3ErrorCode::InvalidPart,
            Error::InvalidPartOrder => s3s::S3ErrorCode::InvalidPartOrder,
            Error::ChecksumMismatch(_) => s3s::S3ErrorCode::BadDigest,
            Error::InvalidRange => s3s::S3ErrorCode::InvalidRange,
            Error::PreconditionFailed => s3s::S3ErrorCode::PreconditionFailed,
            _ => s3s::S3ErrorCode::InternalError,
        };

        // S3's messages for the errors clients see most, which they may log or show.
        let message = match error {
            Error::NoSuchKey => Some("The specified key does not exist."),
            Error::NoSuchBucket => Some("The specified bucket does not exist"),
            Error::NoSuchUpload => Some("The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."),
            Error::InvalidRange => Some("The requested range is not satisfiable"),
            Error::PreconditionFailed => {
                Some("At least one of the pre-conditions you specified did not hold")
            }
            _ => None,
        };
        let mut error = s3s::S3Error::with_source(error_kind, Box::new(error));
        if let Some(message) = message {
            error.set_message(message);
        }
        error
    }
}
