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
///
/// The unknown-length variant carries no emitted-yet / end-of-stream flags. Those questions are
/// answered from the reader's own [`parts_yielded`](crate::io::part_reader::PartReader::parts_yielded)
/// count, which is incremented under the read lock — so a caller driving speculative concurrent
/// reads observes a count that is always ordered with end-of-stream, with no second lock of its own
/// to race against. Keeping the count on the reader is what makes "was the stream empty?" and "did
/// it exceed the part limit?" race-free.
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// Content length known up front: dispatch until `total_parts` have been issued.
    Known {
        total_parts: u64,
        /// The size declared by the source, sent as `MpuObjectSize` on complete. It did not come
        /// from our own part accounting, so it is an *independent* witness — it also catches a part
        /// this crate dropped or duplicated, which a sum of what we uploaded cannot.
        declared_object_size: u64,
    },
    /// Content length unknown: dispatch until the reader reports end-of-stream.
    Unknown,
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
        /// Unknown-length only: set once a read reports end-of-stream, to stop dispatch and let the
        /// phase drain and complete. Its *timing* is not correctness-critical — a late set only
        /// causes a few extra speculative reads that no-op — so it is a plain flag, not the racy
        /// emitted-flag it replaces. Emptiness is judged from the reader's `parts_yielded()`, not
        /// from here.
        eof: bool,
        parts_in_flight: usize,
        completed_parts: Vec<CompletedPart>,
        response_builder: UploadOutputBuilder,
        /// Running total of bytes actually uploaded, summed as each part completes. Consistent
        /// meaning on both paths: for `Unknown` it *is* the object size sent on complete; for
        /// `Known` it is checked (`<=`) against the declared upper bound before completing.
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
