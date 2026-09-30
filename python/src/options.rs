/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-request options passed as keyword arguments from Python.

use std::collections::HashMap;

use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, ObjectCannedAcl, ObjectLockLegalHoldStatus,
    ObjectLockMode, RequestPayer, ServerSideEncryption, StorageClass,
};
use aws_sdk_s3_transfer_manager::operation::download::builders::DownloadFluentBuilder;
use aws_sdk_s3_transfer_manager::operation::upload::builders::UploadFluentBuilder;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::ReadAhead;
use aws_smithy_types::DateTime;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert;

/// A value accepted for an option.
trait OptionValue: Sized {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self>;
}

impl OptionValue for String {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        value.extract()
    }
}

impl OptionValue for bool {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        value.extract()
    }
}

impl OptionValue for HashMap<String, String> {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        value.extract()
    }
}

impl OptionValue for DateTime {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        convert::from_datetime(value)
    }
}

impl OptionValue for ReadAhead {
    fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(ReadAhead::Parts(value.extract()?))
    }
}

/// S3 enumerations are given by their wire value, e.g. `"STANDARD_IA"`.
macro_rules! impl_enum_option {
    ($($ty:ty),* $(,)?) => {$(
        impl OptionValue for $ty {
            fn extract(value: &Bound<'_, PyAny>) -> PyResult<Self> {
                Ok(<$ty>::from(value.extract::<String>()?.as_str()))
            }
        }
    )*};
}

impl_enum_option!(
    ChecksumAlgorithm,
    ChecksumMode,
    ChecksumType,
    ObjectCannedAcl,
    ObjectLockLegalHoldStatus,
    ObjectLockMode,
    RequestPayer,
    ServerSideEncryption,
    StorageClass,
);

/// Declares options that map one-to-one onto a fluent builder setter.
macro_rules! builder_options {
    ($name:ident for $builder:ty { $($key:ident: $ty:ty => $setter:ident,)* }) => {
        #[derive(Default)]
        struct $name {
            $($key: Option<$ty>,)*
        }

        impl $name {
            /// Stores `value` under `key`, returning `false` if `key` is not an option.
            fn set(&mut self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<bool> {
                match key {
                    $(stringify!($key) => self.$key = Some(extract(key, value)?),)*
                    _ => return Ok(false),
                }
                Ok(true)
            }

            fn apply(self, builder: $builder) -> $builder {
                builder$(.$setter(self.$key))*
            }
        }
    };
}

builder_options!(UploadFields for UploadFluentBuilder {
    acl: ObjectCannedAcl => set_acl,
    bucket_key_enabled: bool => set_bucket_key_enabled,
    cache_control: String => set_cache_control,
    content_disposition: String => set_content_disposition,
    content_encoding: String => set_content_encoding,
    content_language: String => set_content_language,
    content_md5: String => set_content_md5,
    content_type: String => set_content_type,
    expected_bucket_owner: String => set_expected_bucket_owner,
    expires: DateTime => set_expires,
    grant_full_control: String => set_grant_full_control,
    grant_read: String => set_grant_read,
    grant_read_acp: String => set_grant_read_acp,
    grant_write_acp: String => set_grant_write_acp,
    if_match: String => set_if_match,
    if_none_match: String => set_if_none_match,
    metadata: HashMap<String, String> => set_metadata,
    object_lock_legal_hold_status: ObjectLockLegalHoldStatus => set_object_lock_legal_hold_status,
    object_lock_mode: ObjectLockMode => set_object_lock_mode,
    object_lock_retain_until_date: DateTime => set_object_lock_retain_until_date,
    request_payer: RequestPayer => set_request_payer,
    server_side_encryption: ServerSideEncryption => set_server_side_encryption,
    sse_customer_algorithm: String => set_sse_customer_algorithm,
    sse_customer_key: String => set_sse_customer_key,
    sse_customer_key_md5: String => set_sse_customer_key_md5,
    sse_kms_encryption_context: String => set_sse_kms_encryption_context,
    sse_kms_key_id: String => set_sse_kms_key_id,
    storage_class: StorageClass => set_storage_class,
    tagging: String => set_tagging,
    website_redirect_location: String => set_website_redirect_location,
});

builder_options!(DownloadFields for DownloadFluentBuilder {
    checksum_mode: ChecksumMode => set_checksum_mode,
    expected_bucket_owner: String => set_expected_bucket_owner,
    if_match: String => set_if_match,
    if_modified_since: DateTime => set_if_modified_since,
    if_none_match: String => set_if_none_match,
    if_unmodified_since: DateTime => set_if_unmodified_since,
    range: String => set_range,
    read_ahead: ReadAhead => set_read_ahead,
    request_payer: RequestPayer => set_request_payer,
    response_cache_control: String => set_response_cache_control,
    response_content_disposition: String => set_response_content_disposition,
    response_content_encoding: String => set_response_content_encoding,
    response_content_language: String => set_response_content_language,
    response_content_type: String => set_response_content_type,
    response_expires: DateTime => set_response_expires,
    sse_customer_algorithm: String => set_sse_customer_algorithm,
    sse_customer_key: String => set_sse_customer_key,
    sse_customer_key_md5: String => set_sse_customer_key_md5,
    version_id: String => set_version_id,
});

fn extract<T: OptionValue>(key: &str, value: &Bound<'_, PyAny>) -> PyResult<T> {
    T::extract(value).map_err(|err| {
        let py = value.py();
        let wrapped = PyTypeError::new_err(format!("invalid value for option '{key}': {err}"));
        wrapped.set_cause(py, Some(err));
        wrapped
    })
}

/// Iterates `options`, skipping entries whose value is `None`.
fn for_each_option(
    options: Option<&Bound<'_, PyDict>>,
    mut set: impl FnMut(&str, &Bound<'_, PyAny>) -> PyResult<bool>,
) -> PyResult<()> {
    let Some(options) = options else {
        return Ok(());
    };
    for (key, value) in options.iter() {
        if value.is_none() {
            continue;
        }
        let key = key.extract::<String>()?;
        if !set(&key, &value)? {
            return Err(PyTypeError::new_err(format!(
                "got an unexpected keyword argument '{key}'"
            )));
        }
    }
    Ok(())
}

/// Options for uploading a single object.
#[derive(Default)]
pub(crate) struct UploadOptions {
    fields: UploadFields,
    checksum_strategy: Option<ChecksumStrategy>,
}

impl UploadOptions {
    pub(crate) fn extract(options: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut fields = UploadFields::default();
        let mut algorithm: Option<ChecksumAlgorithm> = None;
        let mut checksum_type: Option<ChecksumType> = None;
        let mut full_object_checksum: Option<String> = None;
        for_each_option(options, |key, value| {
            match key {
                "checksum_algorithm" => algorithm = Some(extract(key, value)?),
                "checksum_type" => checksum_type = Some(extract(key, value)?),
                "full_object_checksum" => full_object_checksum = Some(extract(key, value)?),
                _ => return fields.set(key, value),
            }
            Ok(true)
        })?;

        let checksum_strategy = match algorithm {
            Some(algorithm) => {
                let mut builder = ChecksumStrategy::builder().algorithm(algorithm);
                if let Some(checksum_type) = checksum_type {
                    builder = builder.type_if_multipart(checksum_type);
                }
                if let Some(checksum) = full_object_checksum {
                    builder = builder.full_object_checksum(checksum);
                }
                Some(
                    builder
                        .build()
                        .map_err(|err| PyValueError::new_err(err.to_string()))?,
                )
            }
            None if checksum_type.is_some() || full_object_checksum.is_some() => {
                return Err(PyValueError::new_err(
                    "checksum_type and full_object_checksum require checksum_algorithm",
                ));
            }
            None => None,
        };
        Ok(Self {
            fields,
            checksum_strategy,
        })
    }

    pub(crate) fn apply(self, builder: UploadFluentBuilder) -> UploadFluentBuilder {
        self.fields
            .apply(builder)
            .set_checksum_strategy(self.checksum_strategy)
    }
}

/// Options for downloading a single object.
#[derive(Default)]
pub(crate) struct DownloadOptions {
    fields: DownloadFields,
}

impl DownloadOptions {
    pub(crate) fn extract(options: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let mut fields = DownloadFields::default();
        for_each_option(options, |key, value| fields.set(key, value))?;
        Ok(Self { fields })
    }

    pub(crate) fn apply(self, builder: DownloadFluentBuilder) -> DownloadFluentBuilder {
        self.fields.apply(builder)
    }
}
