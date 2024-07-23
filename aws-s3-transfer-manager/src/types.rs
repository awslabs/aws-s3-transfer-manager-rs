/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// The target part size for an upload or download request.
#[derive(Debug, Clone, Default)]
pub enum PartSize {
    /// Automatically configure an optimal target part size based on the execution environment.
    #[default]
    Auto,

    /// Target part size explicitly given.
    ///
    /// NOTE: This is a suggestion and will be used if possible but may be adjusted for an individual request
    /// as required by the underlying API.
    Target(u64),
}

/// The concurrency settings to use for a single upload or download request.
#[derive(Debug, Clone, Default)]
pub enum ConcurrencySetting {
    /// Automatically configure an optimal concurrency setting based on the execution environment.
    #[default]
    Auto,

    /// Explicitly configured concurrency setting.
    Explicit(usize),
}

/// Policy for how to handle a failed multipart upload
///
/// Default is to abort the upload.
#[derive(Debug, Clone, Default)]
pub enum FailedMultipartUploadPolicy {
    /// Abort the upload on any individual part failure
    #[default]
    AbortUpload,
    /// Retain any uploaded parts. The upload ID will be available in the response.
    Retain,
}

/// Describes the result of aborting an in-progress upload.
#[derive(Debug, Default)]
pub struct AbortedUpload {
    pub(crate) upload_id: Option<String>,
    pub(crate) request_charged: Option<aws_sdk_s3::types::RequestCharged>,
}

impl AbortedUpload {
    /// Get the multipart upload ID that was cancelled
    ///
    /// Not present for uploads that did not utilize a multipart upload
    pub fn upload_id(&self) -> &Option<String> {
        &self.upload_id
    }

    /// If present, indicates that the requester was successfully charged for the request.
    ///
    /// This functionality is not supported for directory buckets and is
    /// not present for uploads that did not utilize a multipart upload
    pub fn request_charged(&self) -> &Option<aws_sdk_s3::types::RequestCharged> {
        &self.request_charged
    }
}
