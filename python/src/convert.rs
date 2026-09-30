/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Conversions between transfer manager types and the package's Python types.

use aws_sdk_s3_transfer_manager::operation::download::{DownloadOutput, ObjectMetadata};
use aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsOutput;
use aws_sdk_s3_transfer_manager::operation::upload::UploadOutput;
use aws_sdk_s3_transfer_manager::operation::upload_objects::UploadObjectsOutput;
use aws_sdk_s3_transfer_manager::types::{
    ChecksumValidation, FailedDownload, FailedUpload, IntegrityChecks, NotValidatedReason,
};
use aws_smithy_types::DateTime;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

use crate::download::OpenedStream;
use crate::error;

macro_rules! model_types {
    ($($name:ident),* $(,)?) => {$(
        #[allow(non_snake_case)]
        fn $name(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
            static TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();
            TYPE.import(py, "aws_s3_transfer._models", stringify!($name))
        }
    )*};
}

model_types! {
    UploadOutput,
    DownloadOutput,
    ObjectMetadata,
    IntegrityChecks,
    UploadDirectoryOutput,
    DownloadDirectoryOutput,
    UploadFailure,
    DownloadFailure,
}

/// Instantiates one of the package's model classes from keyword arguments.
macro_rules! model {
    ($py:expr, $ty:ident { $($field:ident: $value:expr),* $(,)? }) => {{
        let kwargs = PyDict::new($py);
        $(kwargs.set_item(stringify!($field), $value)?;)*
        $ty($py)?.call((), Some(&kwargs))
    }};
}

/// The result of an [`Operation`](crate::runtime::Operation), converted to a
/// Python object on the thread that receives it.
pub(crate) enum Output {
    None,
    Upload(UploadOutput),
    Download(DownloadOutput),
    UploadDirectory(UploadObjectsOutput),
    DownloadDirectory(DownloadObjectsOutput),
    Stream(OpenedStream),
}

impl<'py> IntoPyObject<'py> for Output {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Output::None => Ok(py.None().into_bound(py)),
            Output::Upload(output) => upload_output(py, &output),
            Output::Download(output) => download_output(py, &output),
            Output::UploadDirectory(output) => model!(
                py,
                UploadDirectoryOutput {
                    objects_uploaded: output.objects_uploaded(),
                    failures: upload_failures(py, output.failed_transfers())?,
                }
            ),
            Output::DownloadDirectory(output) => model!(
                py,
                DownloadDirectoryOutput {
                    objects_downloaded: output.objects_downloaded(),
                    failures: download_failures(py, output.failed_transfers())?,
                }
            ),
            Output::Stream(stream) => stream.into_pyobject(py),
        }
    }
}

fn upload_output<'py>(py: Python<'py>, output: &UploadOutput) -> PyResult<Bound<'py, PyAny>> {
    model!(
        py,
        UploadOutput {
            e_tag: output.e_tag(),
            version_id: output.version_id(),
            expiration: output.expiration(),
            checksum_crc32: output.checksum_crc32(),
            checksum_crc32c: output.checksum_crc32_c(),
            checksum_crc64nvme: output.checksum_crc64_nvme(),
            checksum_sha1: output.checksum_sha1(),
            checksum_sha256: output.checksum_sha256(),
            checksum_type: output.checksum_type().map(|v| v.as_str()),
            server_side_encryption: output.server_side_encryption().map(|v| v.as_str()),
            sse_customer_algorithm: output.sse_customer_algorithm(),
            sse_customer_key_md5: output.sse_customer_key_md5(),
            sse_kms_key_id: output.sse_kms_key_id(),
            sse_kms_encryption_context: output.sse_kms_encryption_context(),
            bucket_key_enabled: output.bucket_key_enabled(),
            request_charged: output.request_charged().map(|v| v.as_str()),
            upload_id: output.upload_id(),
        }
    )
}

fn download_output<'py>(py: Python<'py>, output: &DownloadOutput) -> PyResult<Bound<'py, PyAny>> {
    model!(
        py,
        DownloadOutput {
            metadata: object_metadata(py, &output.object_meta)?,
            integrity: integrity_checks(py, output.integrity_checks())?,
        }
    )
}

pub(crate) fn object_metadata<'py>(
    py: Python<'py>,
    meta: &ObjectMetadata,
) -> PyResult<Bound<'py, PyAny>> {
    model!(
        py,
        ObjectMetadata {
            object_size: meta.total_object_size(),
            content_type: meta.content_type.as_deref(),
            content_encoding: meta.content_encoding.as_deref(),
            content_language: meta.content_language.as_deref(),
            content_disposition: meta.content_disposition.as_deref(),
            cache_control: meta.cache_control.as_deref(),
            e_tag: meta.e_tag.as_deref(),
            last_modified: meta
                .last_modified
                .as_ref()
                .map(|d| to_datetime(py, d))
                .transpose()?,
            version_id: meta.version_id.as_deref(),
            metadata: meta.metadata.clone().unwrap_or_default(),
            storage_class: meta.storage_class.as_ref().map(|v| v.as_str()),
            server_side_encryption: meta.server_side_encryption.as_ref().map(|v| v.as_str()),
            sse_customer_algorithm: meta.sse_customer_algorithm.as_deref(),
            sse_customer_key_md5: meta.sse_customer_key_md5.as_deref(),
            sse_kms_key_id: meta.ssekms_key_id.as_deref(),
            bucket_key_enabled: meta.bucket_key_enabled,
            expiration: meta.expiration.as_deref(),
            expires: meta.expires_string.as_deref(),
            restore: meta.restore.as_deref(),
            website_redirect_location: meta.website_redirect_location.as_deref(),
            delete_marker: meta.delete_marker,
            missing_meta: meta.missing_meta,
            parts_count: meta.parts_count,
            replication_status: meta.replication_status.as_ref().map(|v| v.as_str()),
            request_charged: meta.request_charged.as_ref().map(|v| v.as_str()),
            object_lock_mode: meta.object_lock_mode.as_ref().map(|v| v.as_str()),
            object_lock_retain_until_date: meta
                .object_lock_retain_until_date
                .as_ref()
                .map(|d| to_datetime(py, d))
                .transpose()?,
            object_lock_legal_hold_status: meta
                .object_lock_legal_hold_status
                .as_ref()
                .map(|v| v.as_str()),
        }
    )
}

fn integrity_checks<'py>(py: Python<'py>, checks: &IntegrityChecks) -> PyResult<Bound<'py, PyAny>> {
    let (algorithm, not_validated_reason) = match checks.checksum_validation() {
        ChecksumValidation::Validated { algorithm, .. } => (Some(algorithm.as_str()), None),
        ChecksumValidation::NotValidated { reason, .. } => (None, Some(not_validated(reason))),
        _ => (None, None),
    };
    model!(
        py,
        IntegrityChecks {
            validated: algorithm.is_some(),
            algorithm: algorithm,
            not_validated_reason: not_validated_reason,
            checksum_crc32: checks.checksum_crc32(),
            checksum_crc32c: checks.checksum_crc32c(),
            checksum_crc64nvme: checks.checksum_crc64_nvme(),
            checksum_sha1: checks.checksum_sha1(),
            checksum_sha256: checks.checksum_sha256(),
            checksum_type: checks.checksum_type().map(|v| v.as_str()),
        }
    )
}

fn not_validated(reason: &NotValidatedReason) -> &'static str {
    match reason {
        NotValidatedReason::Disabled => "disabled",
        NotValidatedReason::CompositeChecksum => "composite_checksum",
        NotValidatedReason::PartialCoverage => "partial_coverage",
        NotValidatedReason::Unavailable => "unavailable",
        _ => "unknown",
    }
}

pub(crate) fn upload_failures<'py>(
    py: Python<'py>,
    failures: &[FailedUpload],
) -> PyResult<Bound<'py, PyTuple>> {
    let failures = failures
        .iter()
        .map(|failure| {
            let input = failure.input();
            model!(
                py,
                UploadFailure {
                    path: failure.source_path(),
                    bucket: input.and_then(|i| i.bucket()),
                    key: input.and_then(|i| i.key()),
                    error: error::exception(py, failure.error())?,
                }
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, failures)
}

pub(crate) fn download_failures<'py>(
    py: Python<'py>,
    failures: &[FailedDownload],
) -> PyResult<Bound<'py, PyTuple>> {
    let failures = failures
        .iter()
        .map(|failure| {
            model!(
                py,
                DownloadFailure {
                    bucket: failure.input().bucket(),
                    key: failure.input().key(),
                    error: error::exception(py, failure.error())?,
                }
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, failures)
}

struct DateTimeApi {
    datetime: Py<PyType>,
    timedelta: Py<PyType>,
    utc: Py<PyAny>,
    epoch: Py<PyAny>,
}

fn datetime_api(py: Python<'_>) -> PyResult<&DateTimeApi> {
    static API: PyOnceLock<DateTimeApi> = PyOnceLock::new();
    API.get_or_try_init(py, || {
        let module = py.import("datetime")?;
        let datetime = module.getattr("datetime")?.cast_into::<PyType>()?;
        let utc = module.getattr("timezone")?.getattr("utc")?;
        let epoch = datetime.call((1970, 1, 1, 0, 0, 0, 0, &utc), None)?;
        Ok(DateTimeApi {
            timedelta: module.getattr("timedelta")?.cast_into::<PyType>()?.unbind(),
            datetime: datetime.unbind(),
            utc: utc.unbind(),
            epoch: epoch.unbind(),
        })
    })
}

/// Converts to a timezone-aware `datetime` in UTC.
fn to_datetime<'py>(py: Python<'py>, value: &DateTime) -> PyResult<Bound<'py, PyAny>> {
    let api = datetime_api(py)?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("seconds", value.secs())?;
    kwargs.set_item("microseconds", value.subsec_nanos() / 1_000)?;
    let offset = api.timedelta.bind(py).call((), Some(&kwargs))?;
    api.epoch.bind(py).add(offset)
}

/// Converts a `datetime`, treating naive values as UTC.
pub(crate) fn from_datetime(value: &Bound<'_, PyAny>) -> PyResult<DateTime> {
    let py = value.py();
    let api = datetime_api(py)?;
    if !value.is_instance(api.datetime.bind(py))? {
        return Err(PyTypeError::new_err(format!(
            "expected a datetime, got {}",
            value.get_type().name()?
        )));
    }
    let value = if value.getattr("tzinfo")?.is_none() {
        let kwargs = PyDict::new(py);
        kwargs.set_item("tzinfo", api.utc.bind(py))?;
        value.call_method("replace", (), Some(&kwargs))?
    } else {
        value.clone()
    };
    let delta = value.sub(api.epoch.bind(py))?;
    let days: i64 = delta.getattr("days")?.extract()?;
    let seconds: i64 = delta.getattr("seconds")?.extract()?;
    let microseconds: u32 = delta.getattr("microseconds")?.extract()?;
    Ok(DateTime::from_secs_and_nanos(
        days * 86_400 + seconds,
        microseconds * 1_000,
    ))
}
