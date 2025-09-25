/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::upload::UploadInput;
use crate::operation::TransferContext;
use crate::types::BucketType;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) type UploadContext = TransferContext<UploadState>;

impl UploadContext {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket_type: BucketType,
        req: UploadInput,
    ) -> Self {
        let state = Arc::new(UploadState {
            request: Arc::new(req),
            bucket_type,
        });
        TransferContext { handle, state }
    }
}

/// Internal context used to drive a single Upload operation
#[derive(Debug, Clone)]
pub(crate) struct UploadState {
    /// the original request (NOTE: the body will have been taken for processing, only the other fields remain)
    pub(crate) request: Arc<UploadInput>,

    /// Type of S3 bucket targeted by this operation
    pub(crate) bucket_type: BucketType,
}

impl UploadState {
    /// The original request (sans the body as it will have been taken for processing)
    pub(crate) fn request(&self) -> &UploadInput {
        self.request.deref()
    }

    /// Type of S3 bucket targeted by this operation
    pub(crate) fn bucket_type(&self) -> BucketType {
        self.bucket_type
    }
}
