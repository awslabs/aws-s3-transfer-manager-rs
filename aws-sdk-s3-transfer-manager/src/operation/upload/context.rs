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
/// A source with a known length (a buffer or a file) yields an exact part count up front. A
/// `PartStream` of unknown length does not: parts are dispatched speculatively and the phase ends
/// when the reader reports end-of-stream. Modelling the two as one enum keeps the illegal
/// combination — a part count *and* an end-of-stream flag — unrepresentable.
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// Content length known up front: dispatch until `total_parts` have been issued.
    Known { total_parts: u64 },
    /// Content length unknown: dispatch until the reader reports end-of-stream.
    Unknown {
        /// Set once a read returns end-of-stream. Gates any further dispatch.
        eof: bool,
        /// Whether any part has been emitted yet.
        ///
        /// S3 rejects a `CompleteMultipartUpload` that lists no parts, so an empty stream must
        /// still upload one (empty) part. Several part-work items can be dispatched before any of
        /// them reads, so more than one can observe "end-of-stream, nothing emitted"; this flag is
        /// claimed by a check-and-set under the state lock so exactly one of them synthesizes
        /// part 1. Data-bearing parts set it too, so a non-empty stream never synthesizes one.
        first_part_emitted: bool,
    },
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
        parts_in_flight: usize,
        completed_parts: Vec<CompletedPart>,
        response_builder: UploadOutputBuilder,
        /// Full object size, carried through to CompleteMPU as `MpuObjectSize`.
        ///
        /// For [`PartPlan::Known`] this is the length declared by the source, set once at
        /// `CreateMultipartUpload` time. For [`PartPlan::Unknown`] there is no declared length, so
        /// it accumulates the byte length of each part actually uploaded and is exact only once
        /// end-of-stream is reached — which is the point at which it is read.
        object_size: u64,
    },
    /// All parts done, calling CompleteMPU (MPU only)
    Completing {
        upload_id: Option<String>,
        part_reader: Option<Arc<PartReader>>,
        completed_parts: Option<Vec<CompletedPart>>,
        response_builder: Option<UploadOutputBuilder>,
        complete_in_flight: bool,
        /// Final object size, sent as `MpuObjectSize`. See `Transferring::object_size`.
        object_size: u64,
    },
    /// PutObject in flight (single request upload)
    PutObjectInFlight,
    /// Done
    Done,
}
