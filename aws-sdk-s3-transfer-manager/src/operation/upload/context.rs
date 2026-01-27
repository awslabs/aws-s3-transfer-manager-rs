/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::upload::UploadInput;
use crate::operation::TransferContext;
use crate::types::BucketType;
use aws_sdk_s3::types::CompletedPart;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use crate::io::part_reader::PartReader;
use crate::io::InputStream;
use crate::operation::upload::UploadOutputBuilder;

pub(crate) type UploadContext = TransferContext<UploadState>;

impl UploadContext {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket_type: BucketType,
        req: UploadInput,
        stream: InputStream,
    ) -> Self {
        // TODO(redux): For unknown content length (streaming uploads), this will need adjustment.
        // Currently we require known length. When we support unknown length:
        // - content_length becomes Option<u64>
        // - total_parts in Active state becomes Option<u64>
        // - Work generation continues until stream exhausted rather than counting parts
        let content_length = stream
            .size_hint()
            .upper()
            .expect("content_length required; unknown length not yet supported");

        let state = Arc::new(UploadState {
            request: Arc::new(req),
            bucket_type,
            work: Mutex::new(UploadWorkState::PendingInit {
                stream,
                content_length,
                init_in_flight: false,
            }),
        });
        TransferContext { handle, state }
    }
}

/// Internal context used to drive a single Upload operation
#[derive(Debug)]
pub(crate) struct UploadState {
    /// the original request (NOTE: the body will have been taken for processing, only the other fields remain)
    pub(crate) request: Arc<UploadInput>,

    /// Type of S3 bucket targeted by this operation
    pub(crate) bucket_type: BucketType,

    /// Mutable state for driving work forward
    pub(crate) work: Mutex<UploadWorkState>,
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

/// Mutable state for tracking upload work progress
#[derive(Debug)]
pub(crate) enum UploadWorkState {
    /// Waiting to start - need to call CreateMPU (or PutObject for small uploads)
    PendingInit {
        stream: InputStream,
        content_length: u64,
        init_in_flight: bool,
    },
    /// Data transfer in progress (uploading parts for MPU, or body for PutObject)
    Transferring {
        upload_id: String,
        part_reader: Arc<PartReader>,
        next_part: u64,
        total_parts: u64,
        parts_in_flight: usize,
        completed_parts: Vec<CompletedPart>,
        response_builder: UploadOutputBuilder,
    },
    /// All parts done, calling CompleteMPU (MPU only)
    Completing {
        upload_id: String,
        part_reader: Arc<PartReader>,
        completed_parts: Vec<CompletedPart>,
        response_builder: UploadOutputBuilder,
        complete_in_flight: bool,
    },
    /// Done
    Done,
}
