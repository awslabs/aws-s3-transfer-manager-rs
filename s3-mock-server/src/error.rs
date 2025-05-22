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

    /// The parts are not in the correct order.
    #[error("invalid part order")]
    InvalidPartOrder,

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
}

impl From<Error> for s3s::S3Error {
    fn from(error: Error) -> Self {
        let error_kind = match error {
            Error::NoSuchKey => s3s::S3ErrorCode::NoSuchKey,
            Error::NoSuchBucket => s3s::S3ErrorCode::NoSuchBucket,
            Error::NoSuchUpload => s3s::S3ErrorCode::NoSuchUpload,
            Error::NoSuchPart => s3s::S3ErrorCode::InvalidPart,
            Error::InvalidPartOrder => s3s::S3ErrorCode::InvalidPartOrder,
            Error::InvalidRange => s3s::S3ErrorCode::InvalidRange,
            _ => s3s::S3ErrorCode::InternalError,
        };

        s3s::S3Error::with_source(error_kind, Box::new(error))
    }
}
