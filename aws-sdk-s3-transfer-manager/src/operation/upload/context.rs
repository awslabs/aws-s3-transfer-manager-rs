/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use aws_sdk_s3::types::CompletedPart;
use std::sync::Arc;

use crate::io::part_reader::PartReader;
use crate::io::InputStream;
use crate::operation::upload::UploadOutputBuilder;

/// How the `Transferring` phase decides it has dispatched every part.
///
/// A known length yields an exact part count up front; an unknown-length `PartStream` does not, so
/// its parts are dispatched speculatively and the phase ends when the reader reports end-of-stream.
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// Content length known up front: dispatch until `total_parts` have been issued.
    Known {
        total_parts: u64,
        /// Size declared by the source, sent as `MpuObjectSize` on complete. It does not come from
        /// our own part accounting, so it independently catches a part dropped or duplicated here.
        declared_object_size: u64,
    },
    /// Content length unknown: dispatch until the reader reports end-of-stream.
    Unknown,
}

impl PartPlan {
    /// Whether every part has been dispatched: a part count for `Known`, end-of-stream for
    /// `Unknown`.
    ///
    /// `parts_dispatched` counts speculative dispatches, which run ahead of what the reader has
    /// actually produced, so it answers only "is dispatch finished" — never "how many parts exist".
    pub(crate) fn all_dispatched(&self, parts_dispatched: u64, eof: bool) -> bool {
        match self {
            PartPlan::Known { total_parts, .. } => parts_dispatched >= *total_parts,
            PartPlan::Unknown => eof,
        }
    }
}

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
        parts_dispatched: u64,
        /// How this phase decides every part has been dispatched.
        plan: PartPlan,
        /// Unknown-length only: set once a read reports end-of-stream; gates further dispatch. Its
        /// timing is not correctness-critical — a late set only costs speculative reads that no-op.
        eof: bool,
        parts_in_flight: usize,
        completed_parts: Vec<CompletedPart>,
        response_builder: UploadOutputBuilder,
        /// Running total of bytes uploaded, summed as each part completes. For `Unknown` this is the
        /// object size sent on complete; for `Known` it is checked (`<=`) against the declared bound.
        bytes_uploaded: u64,
    },
    /// All parts done, calling CompleteMPU (MPU only)
    Completing {
        upload_id: Option<String>,
        part_reader: Option<Arc<PartReader>>,
        completed_parts: Option<Vec<CompletedPart>>,
        response_builder: Option<UploadOutputBuilder>,
        complete_in_flight: bool,
        /// See [`PartPlan`] for how the `MpuObjectSize` to send is chosen from this and the plan.
        plan: PartPlan,
        /// Total bytes uploaded. See `Transferring::bytes_uploaded`.
        bytes_uploaded: u64,
    },
    /// PutObject in flight (single request upload)
    PutObjectInFlight,
    /// Done
    Done,
}
