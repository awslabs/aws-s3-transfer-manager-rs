/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Operation builders
pub mod builders;
mod checksum_strategy;
mod input;
mod output;

mod context;
mod handle;
mod service;
mod transfer;

pub use checksum_strategy::{ChecksumStrategy, ChecksumStrategyBuilder};
pub(crate) use transfer::UploadTransfer;

use crate::error;
use crate::types::BucketType;
use context::UploadContext;
pub use handle::UploadHandle;
/// Request type for uploads to Amazon S3
pub use input::{UploadInput, UploadInputBuilder};
/// Response type for uploads to Amazon S3
pub use output::{UploadOutput, UploadOutputBuilder};

use std::sync::Arc;

/// Operation struct for single object upload
#[derive(Clone, Default, Debug)]
pub(crate) struct Upload;

impl Upload {
    /// Execute a single `Upload` transfer operation
    pub(crate) fn orchestrate(
        handle: Arc<crate::client::Handle>,
        mut input: crate::operation::upload::UploadInput,
    ) -> Result<UploadHandle, error::Error> {
        use crate::scheduler::TransferId;
        use std::sync::atomic::{AtomicU64, Ordering};

        // TODO(redux): Consider where transfer ID generation should live
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let transfer_id = TransferId {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: None,
        };

        // TODO(redux): we were getting checksum behavior for free from SDK, moving to presigning and dedicated HTTP stack requires us to consider that
        if input.checksum_strategy.is_none() {
            // User didn't explicitly set checksum strategy.
            // If SDK is configured to send checksums: use default checksum strategy.
            // Else: continue with no checksums
            if handle
                .config
                .client()
                .config()
                .request_checksum_calculation()
                .cloned()
                .unwrap_or_default()
                == aws_sdk_s3::config::RequestChecksumCalculation::WhenSupported
            {
                input.checksum_strategy = Some(ChecksumStrategy::default());
            }
        }

        let stream = input.take_body();

        // TODO(redux): Relax this constraint - unknown content length implies MPU
        // MPU has max of 10K parts which requires us to know the upper bound
        // on the content length (today anyway). While true for file-based workloads,
        // the upper `size_hint` might not be equal to the actual bytes transferred.
        if stream.size_hint().upper().is_none() {
            return Err(crate::io::error::Error::upper_bound_size_hint_required().into());
        }

        let ctx = new_context(handle.clone(), input, stream);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let transfer = UploadTransfer::new(transfer_id, ctx, result_tx);
        handle
            .new_scheduler
            .enqueue_transfer(crate::scheduler::Transfer::Upload(transfer));

        Ok(UploadHandle::new(result_rx))
    }
}

fn new_context(
    handle: Arc<crate::client::Handle>,
    req: UploadInput,
    stream: crate::io::InputStream,
) -> UploadContext {
    UploadContext::new(
        handle,
        BucketType::from_bucket_name(req.bucket().expect("bucket is available")),
        req,
        stream,
    )
}

// TODO(redux): Key concerns from old impl that need addressing:
// - Single PutObject vs MPU threshold check (min_mpu_threshold, is_mpu_only)
// - 0 byte object edge case for MPU
// - Part size calculation: max(configured_part_size, content_length / 10000)
// - Tracing spans for send-upload-part, send-create-multipart-upload, etc.
// - Permit acquisition from old scheduler
