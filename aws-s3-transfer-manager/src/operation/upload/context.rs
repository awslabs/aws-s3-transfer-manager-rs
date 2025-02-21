/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::upload::UploadInput;
use crate::runtime::scheduler::RequestType;
use std::ops::Deref;
use std::sync::Arc;

/// Internal context used to drive a single Upload operation
#[derive(Debug, Clone)]
pub(crate) struct UploadContext {
    /// reference to client handle used to do actual work
    pub(crate) handle: Arc<crate::client::Handle>,
    /// the original request (NOTE: the body will have been taken for processing, only the other fields remain)
    pub(crate) request: Arc<UploadInput>,

    pub(crate) request_type: RequestType,
}

impl UploadContext {
    /// The S3 client to use for SDK operations
    pub(crate) fn client(&self) -> &aws_sdk_s3::Client {
        self.handle.config.client()
    }

    /// The original request (sans the body as it will have been taken for processing)
    pub(crate) fn request(&self) -> &UploadInput {
        self.request.deref()
    }

    /// The original request (sans the body as it will have been taken for processing)
    pub(crate) fn request_type(&self) -> RequestType {
        self.request_type.clone()
    }
}
