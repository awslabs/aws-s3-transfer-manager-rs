/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Maps transfer manager errors onto the package's exception hierarchy.

use std::path::Path;

use aws_sdk_s3_transfer_manager::error::{Error, ErrorKind};
use pyo3::exceptions::PyOSError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

use crate::convert;

macro_rules! exception_types {
    ($($name:ident),* $(,)?) => {$(
        #[allow(non_snake_case)]
        fn $name(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
            static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
            TYPE.import(py, "aws_s3_transfer._errors", stringify!($name))
        }
    )*};
}

exception_types! {
    TransferError,
    InvalidInputError,
    ServiceError,
    NotFoundError,
    ChecksumMismatchError,
    TransferCancelledError,
    DirectoryTransferError,
}

/// Converts a transfer manager error into a Python exception.
pub(crate) fn to_pyerr(err: Error) -> PyErr {
    Python::attach(|py| match exception(py, &err) {
        Ok(exception) => PyErr::from_value(exception),
        Err(conversion_err) => conversion_err,
    })
}

/// Builds (without raising) the Python exception describing `err`.
pub(crate) fn exception<'py>(py: Python<'py>, err: &Error) -> PyResult<Bound<'py, PyAny>> {
    let message = describe(err);
    let exception = match err.kind() {
        ErrorKind::InputInvalid => InvalidInputError(py)?.call1((message,))?,
        ErrorKind::ServiceError => {
            let ty = if err.is_not_found() {
                NotFoundError(py)?
            } else {
                ServiceError(py)?
            };
            let exception = ty.call1((message,))?;
            exception.setattr("operation_name", err.operation_name())?;
            exception.setattr("code", err.code())?;
            exception.setattr("message", err.message())?;
            exception.setattr("request_id", err.request_id())?;
            exception.setattr("extended_request_id", err.extended_request_id())?;
            exception
        }
        ErrorKind::IntegrityError(integrity) => {
            let exception = ChecksumMismatchError(py)?.call1((message,))?;
            exception.setattr("algorithm", integrity.algorithm().map(|a| a.as_str()))?;
            exception.setattr("expected", integrity.expected())?;
            exception.setattr("computed", integrity.computed())?;
            exception
        }
        ErrorKind::OperationCancelled => TransferCancelledError(py)?.call1((message,))?,
        ErrorKind::ChildOperationFailed
            if err.failed_uploads().is_some() || err.failed_downloads().is_some() =>
        {
            let failures = match (err.failed_uploads(), err.failed_downloads()) {
                (Some(uploads), _) => convert::upload_failures(py, uploads)?,
                (_, Some(downloads)) => convert::download_failures(py, downloads)?,
                (None, None) => unreachable!("guarded above"),
            };
            let exception = DirectoryTransferError(py)?.call1((message,))?;
            exception.setattr("failures", failures)?;
            exception
        }
        _ => TransferError(py)?.call1((message,))?,
    };
    Ok(exception)
}

/// Renders an error on one line.
///
/// A response from S3 is summarized by its code and message; other errors are
/// followed by their chain of causes.
fn describe(err: &Error) -> String {
    let mut message = err.to_string();
    if err.code().is_some() {
        if let Some(service_message) = err.message() {
            message.push_str(": ");
            message.push_str(service_message);
        }
        return message;
    }
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !message.ends_with(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

/// Builds an `OSError` for a local path; Python picks the subclass from the error code.
pub(crate) fn os_error(err: std::io::Error, path: &Path) -> PyErr {
    let path = path.display().to_string();
    let Some(code) = err.raw_os_error() else {
        return PyOSError::new_err(format!("{err}: '{path}'"));
    };
    let message = err.to_string();
    let strerror = message
        .strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&message)
        .to_owned();
    // Windows reports a Win32 error code, which Python maps to an errno itself.
    if cfg!(windows) {
        PyOSError::new_err((0, strerror, path, code))
    } else {
        PyOSError::new_err((code, strerror, path))
    }
}
