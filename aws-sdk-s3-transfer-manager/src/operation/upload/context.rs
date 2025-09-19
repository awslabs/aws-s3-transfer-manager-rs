/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::metrics::aggregators::TransferMetrics;
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
        metrics: TransferMetrics,
    ) -> Self {
        let state = Arc::new(UploadState {
            request: Arc::new(req),
            bucket_type,
            metrics: Arc::new(metrics),
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
    /// Transfer metrics for this upload
    #[allow(unused)]
    pub(crate) metrics: Arc<TransferMetrics>,
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

    /// Returns the transfer metrics for this upload
    #[allow(unused)]
    pub(crate) fn metrics(&self) -> Arc<TransferMetrics> {
        self.metrics.clone()
    }
}
