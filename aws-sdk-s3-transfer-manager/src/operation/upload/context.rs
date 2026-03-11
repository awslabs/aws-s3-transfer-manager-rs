/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::types::CompletedPart;
use std::sync::Arc;

use crate::io::part_reader::PartReader;
use crate::io::InputStream;
use crate::operation::upload::UploadOutputBuilder;

/// State machine for tracking upload work progress.
///
/// Represents the current phase of an upload operation.
#[derive(Debug)]
pub(crate) enum UploadState {
    /// Waiting to start - need to call CreateMPU (or PutObject for small uploads)
    PendingInit {
        stream: Option<InputStream>,
        content_length: Option<u64>,
        init_in_flight: bool,
    },
    /// Data transfer in progress (uploading parts for MPU)
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
        upload_id: Option<String>,
        part_reader: Option<Arc<PartReader>>,
        completed_parts: Option<Vec<CompletedPart>>,
        response_builder: Option<UploadOutputBuilder>,
        complete_in_flight: bool,
    },
    /// PutObject in flight (single request upload)
    PutObjectInFlight,
    /// Done
    Done,
}
