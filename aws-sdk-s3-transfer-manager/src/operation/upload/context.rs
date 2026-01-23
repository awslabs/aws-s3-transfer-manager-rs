/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use crate::operation::upload::UploadInput;
use crate::operation::TransferContext;
use crate::types::BucketType;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

pub(crate) type UploadContext = TransferContext<UploadState>;

impl UploadContext {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket_type: BucketType,
        req: UploadInput,
        content_length: u64,
    ) -> Self {
        let state = Arc::new(UploadState {
            request: Arc::new(req),
            bucket_type,
            work: Mutex::new(UploadWorkState::PendingInit {
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
#[derive(Debug, Default)]
pub(crate) enum UploadWorkState {
    /// Not yet initialized
    #[default]
    Uninitialized,
    /// Waiting to start - need to call CreateMPU
    PendingInit {
        // TODO: stream: InputStream,
        content_length: u64,
        init_in_flight: bool,
    },
    /// CreateMPU done, uploading parts
    Active {
        upload_id: String,
        // TODO: part_reader: Arc<PartReader>,
        next_part: u64,
        total_parts: u64,
        parts_in_flight: usize,
        // TODO: completed_parts: Vec<CompletedPart>,
        // TODO: response_builder: UploadOutputBuilder,
    },
    /// All parts done, calling CompleteMPU
    Completing { complete_in_flight: bool },
    /// Done
    Done,
}
