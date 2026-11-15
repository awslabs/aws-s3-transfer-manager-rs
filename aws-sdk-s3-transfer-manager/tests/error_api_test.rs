/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::operation::{
    abort_multipart_upload::AbortMultipartUploadError,
    complete_multipart_upload::CompleteMultipartUploadError,
    create_multipart_upload::CreateMultipartUploadError, get_object::GetObjectError,
    head_object::HeadObjectError, list_objects_v2::ListObjectsV2Error, put_object::PutObjectError,
    upload_part::UploadPartError,
};
use aws_sdk_s3_transfer_manager::error::Error;
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};

// `aws_sdk_s3` error types reach the public API only through the `sdk-v1` feature. A blanket
// `From<SdkError<E, HttpResponse>>` bounded by a crate-private trait is still a public impl for
// each concrete `E`, and the external-types check cannot see it, so this pins its absence.
static_assertions::assert_not_impl_any!(
    Error: From<SdkError<GetObjectError, HttpResponse>>,
    From<SdkError<HeadObjectError, HttpResponse>>,
    From<SdkError<PutObjectError, HttpResponse>>,
    From<SdkError<UploadPartError, HttpResponse>>,
    From<SdkError<CreateMultipartUploadError, HttpResponse>>,
    From<SdkError<CompleteMultipartUploadError, HttpResponse>>,
    From<SdkError<AbortMultipartUploadError, HttpResponse>>,
    From<SdkError<ListObjectsV2Error, HttpResponse>>,
);
