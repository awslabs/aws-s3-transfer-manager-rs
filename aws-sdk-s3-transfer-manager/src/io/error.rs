/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Formatter;
use std::io::Error as StdIoError;
use tokio::task::JoinError;

#[derive(Debug)]
pub(crate) enum ErrorKind {
    OffsetNotAlignedWithPartNumber(u64, u64),
    TaskFailed(JoinError),
    IoError(StdIoError),
}

/// Failure reading parts from an [`InputStream`](crate::io::InputStream).
#[derive(Debug)]
pub(crate) struct Error {
    kind: ErrorKind,
}

impl Error {
    pub(crate) fn offset_not_aligned_with_part_number(offset: u64, part_number: u64) -> Error {
        ErrorKind::OffsetNotAlignedWithPartNumber(offset, part_number).into()
    }
}

/// Converts to a transfer error.
///
/// A failed read is [`IOError`](crate::error::ErrorKind::IOError), with the
/// underlying [`std::io::Error`] as the transfer error's direct source, so the
/// cause chain names the failure once. A read task that panicked or was
/// cancelled, and a reader whose position disagrees with its part numbering,
/// are internal failures and are
/// [`RuntimeError`](crate::error::ErrorKind::RuntimeError).
impl From<Error> for crate::error::Error {
    fn from(err: Error) -> Self {
        use crate::error::ErrorKind as Kind;
        match err.kind {
            ErrorKind::IoError(err) => crate::error::Error::new(Kind::IOError, err),
            other => crate::error::Error::new(Kind::RuntimeError, Error { kind: other }),
        }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self { kind }
    }
}

impl From<StdIoError> for Error {
    fn from(err: StdIoError) -> Self {
        ErrorKind::IoError(err).into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::OffsetNotAlignedWithPartNumber(offset, part_number) => write!(
                f,
                "offset {} is not aligned with part number {}",
                offset, part_number,
            ),
            ErrorKind::IoError(_) => write!(f, "I/O error"),
            ErrorKind::TaskFailed(_) => write!(f, "task failed"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            ErrorKind::OffsetNotAlignedWithPartNumber(_, _) => None,
            ErrorKind::IoError(err) => Some(err as _),
            ErrorKind::TaskFailed(err) => Some(err as _),
        }
    }
}
impl From<Error> for StdIoError {
    fn from(err: Error) -> Self {
        StdIoError::other(err)
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        ErrorKind::TaskFailed(value).into()
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use crate::error::ErrorKind;

    #[test]
    fn io_failure_is_the_direct_source_of_the_transfer_error() {
        let err: crate::error::Error = Error::from(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "failed to open missing",
        ))
        .into();
        assert_eq!(err.kind(), &ErrorKind::IOError);
        let source = std::error::Error::source(&err)
            .and_then(|source| source.downcast_ref::<std::io::Error>())
            .expect("the std::io::Error is the direct source");
        assert_eq!(source.to_string(), "failed to open missing");
    }

    #[test]
    fn misaligned_offset_is_a_runtime_error_naming_the_offset() {
        let err: crate::error::Error = Error::offset_not_aligned_with_part_number(7, 2).into();
        assert_eq!(err.kind(), &ErrorKind::RuntimeError);
        let source = std::error::Error::source(&err).expect("source is retained");
        assert_eq!(
            source.to_string(),
            "offset 7 is not aligned with part number 2"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn failed_read_task_is_a_runtime_error() {
        let join_error = tokio::task::spawn_blocking(|| panic!("read task panicked"))
            .await
            .expect_err("the task panics");
        let err: crate::error::Error = Error::from(join_error).into();
        assert_eq!(err.kind(), &ErrorKind::RuntimeError);
        assert_eq!(
            std::error::Error::source(&err)
                .expect("source is retained")
                .to_string(),
            "task failed"
        );
    }
}
