/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};

use aws_sdk_s3::types::CompletedPart;
use smallvec::SmallVec;

use crate::io::part_reader::{NextPartFuture, PartReader};
use crate::io::InputStream;
use crate::operation::upload::UploadOutputBuilder;

/// How the `Transferring` phase decides it has dispatched every part.
///
/// A known length yields an exact part count up front. An unknown-length [`PartStream`] does not,
/// so its parts are dispatched speculatively and the phase ends when the reader reports
/// end-of-stream.
///
/// [`PartStream`]: crate::io::PartStream
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// Content length known up front: dispatch until `total_parts` have been issued.
    Known {
        total_parts: u64,
        /// Size declared by the source and sent as `MpuObjectSize` on completion.
        ///
        /// This value does not come from our part accounting, so it independently detects a part
        /// dropped or duplicated by the transfer manager.
        declared_object_size: u64,
    },
    /// Content length unknown: dispatch until the reader reports end-of-stream.
    Unknown,
}

impl PartPlan {
    /// Whether part dispatch is complete.
    ///
    /// A known-length source normally completes after its declared part count is dispatched.
    /// Observed end-of-stream also stops dispatch so a source that ends before its declared upper
    /// bound does not get polled repeatedly.
    ///
    /// `parts_dispatched` includes speculative work that may not yet have produced a part. It
    /// therefore answers only whether dispatch is finished, not how many parts the source contains.
    fn all_dispatched(&self, parts_dispatched: u64, eof: bool) -> bool {
        if eof {
            return true;
        }
        match self {
            Self::Known { total_parts, .. } => parts_dispatched >= *total_parts,
            Self::Unknown => false,
        }
    }
}

/// Preserves source wakeups while a pending part read moves back into transfer state.
///
/// A source may call its waker immediately before returning `Poll::Pending`. The read is not yet in
/// [`PendingPartReads`] at that instant, so the notification bit records the edge until parking
/// completes. Every wake also forwards to the scheduler.
pub(crate) struct PartReadWake {
    notified: AtomicBool,
    scheduler: Waker,
}

impl PartReadWake {
    pub(crate) fn new(scheduler: Waker) -> Arc<Self> {
        Arc::new(Self {
            notified: AtomicBool::new(false),
            scheduler,
        })
    }

    fn take_notification(&self) -> bool {
        self.notified.swap(false, Ordering::AcqRel)
    }

    /// Requeues a read whose source wake raced with insertion into transfer state.
    pub(crate) fn requeue_if_notified(&self) {
        if self.notified.load(Ordering::Acquire) {
            self.scheduler.wake_by_ref();
        }
    }
}

impl Wake for PartReadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.scheduler.wake_by_ref();
    }
}

impl std::fmt::Debug for PartReadWake {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartReadWake")
            .field("notified", &self.notified.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

/// One exact source operation retained across scheduler executions.
///
/// The future must not be recreated after returning `Poll::Pending`: it owns the source operation
/// and any progress already made by that operation.
#[derive(Debug)]
pub(crate) struct PendingPartRead {
    pub(crate) future: NextPartFuture,
    pub(crate) wake: Arc<PartReadWake>,
}

/// Pending source operations waiting for their registered wake.
///
/// Several upload-part work items may have been dispatched before the first custom-stream read
/// reports source blockage. The inline capacity covers that short race without imposing a hard
/// limit; larger cohorts spill to the heap.
#[derive(Debug, Default)]
struct PendingPartReads {
    entries: SmallVec<[PendingPartRead; 4]>,
}

impl PendingPartReads {
    fn push(&mut self, read: PendingPartRead) {
        self.entries.push(read);
    }

    fn take_notified(&mut self) -> Option<PendingPartRead> {
        let index = self
            .entries
            .iter()
            .position(|read| read.wake.take_notification())?;
        Some(self.entries.remove(index))
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Work retained by one scheduled `UploadPart` execution.
///
/// Fresh work starts a source read. Resumed work carries the exact future that previously returned
/// `Poll::Pending`.
#[derive(Debug)]
pub(crate) struct UploadPartWork {
    pending_read: Option<PendingPartRead>,
}

impl UploadPartWork {
    fn fresh() -> Self {
        Self { pending_read: None }
    }

    fn resumed(read: PendingPartRead) -> Self {
        Self {
            pending_read: Some(read),
        }
    }

    pub(crate) fn take_pending_read(&mut self) -> Option<PendingPartRead> {
        self.pending_read.take()
    }
}

/// State shared by multipart source reads and network uploads.
///
/// This groups counters and retained futures whose transitions must remain balanced:
///
/// - scheduling increments `parts_dispatched` and `parts_in_flight`;
/// - a dispatch that cannot start retracts both counters;
/// - a pending source read remains in flight while its future is retained;
/// - end-of-stream without a request and each completed upload decrement `parts_in_flight`;
/// - completion begins only after dispatch is closed and all retained or in-flight work drains.
#[derive(Debug)]
pub(crate) struct PartTransferState {
    /// Source reader shared by independently scheduled upload-part work.
    pub(crate) part_reader: Arc<PartReader>,
    /// Rule for deciding when no further source reads should be scheduled.
    plan: PartPlan,
    /// Number of upload-part work items admitted by `poll_work`.
    parts_dispatched: u64,
    /// Set after the source reports end-of-stream.
    eof: bool,
    /// Scheduled source reads or network uploads that have not retired.
    parts_in_flight: usize,
    /// Exact source futures retained after returning `Poll::Pending`.
    pending_reads: PendingPartReads,
    /// S3 completion records collected as network uploads finish.
    completed_parts: Vec<CompletedPart>,
    /// Bytes accepted by completed UploadPart requests.
    bytes_uploaded: u64,
}

impl PartTransferState {
    pub(crate) fn new(
        part_reader: Arc<PartReader>,
        plan: PartPlan,
        completed_parts_capacity: usize,
    ) -> Self {
        Self {
            part_reader,
            plan,
            parts_dispatched: 0,
            eof: false,
            parts_in_flight: 0,
            pending_reads: PendingPartReads::default(),
            completed_parts: Vec::with_capacity(completed_parts_capacity),
            bytes_uploaded: 0,
        }
    }

    /// Returns notified retained work before scheduling another source read.
    pub(crate) fn schedule_part(&mut self) -> Option<UploadPartWork> {
        if let Some(read) = self.pending_reads.take_notified() {
            return Some(UploadPartWork::resumed(read));
        }
        if self.part_reader.source_unavailable()
            || self.plan.all_dispatched(self.parts_dispatched, self.eof)
        {
            return None;
        }

        self.parts_dispatched = self
            .parts_dispatched
            .checked_add(1)
            .expect("multipart dispatch count overflow");
        self.parts_in_flight = self
            .parts_in_flight
            .checked_add(1)
            .expect("multipart in-flight count overflow");
        Some(UploadPartWork::fresh())
    }

    /// Retracts a fresh dispatch that could not begin a source operation.
    pub(crate) fn retract_scheduled_part(&mut self) {
        self.parts_dispatched = self
            .parts_dispatched
            .checked_sub(1)
            .expect("retracted multipart dispatch underflow");
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("retracted multipart in-flight underflow");
    }

    /// Retains one exact source operation without changing its in-flight accounting.
    pub(crate) fn park_read(&mut self, read: PendingPartRead) {
        self.pending_reads.push(read);
    }

    /// Records end-of-stream and returns whether this caller owns empty-stream synthesis.
    ///
    /// The false-to-true transition elects one caller. Empty-part synthesis applies only to an
    /// unknown-length stream because a declared size is an independent statement that the source
    /// contains data.
    pub(crate) fn observe_end_of_stream(&mut self, parts_yielded: u64) -> bool {
        if self.eof {
            return false;
        }
        self.eof = true;
        matches!(self.plan, PartPlan::Unknown) && parts_yielded == 0
    }

    /// Retires scheduled work that ended without starting an UploadPart request.
    pub(crate) fn finish_read_without_part(&mut self) {
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("multipart in-flight completion underflow");
    }

    /// Records one successfully uploaded part.
    pub(crate) fn complete_part(&mut self, part: CompletedPart, bytes_uploaded: u64) {
        self.completed_parts.push(part);
        self.bytes_uploaded = self
            .bytes_uploaded
            .checked_add(bytes_uploaded)
            .expect("uploaded byte count overflow");
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("multipart in-flight completion underflow");
    }

    /// Returns whether the source declared no content-length upper bound.
    pub(crate) fn has_unknown_content_length(&self) -> bool {
        matches!(self.plan, PartPlan::Unknown)
    }

    /// Returns whether dispatch is closed and all scheduled work has retired.
    pub(crate) fn is_complete(&self) -> bool {
        self.plan.all_dispatched(self.parts_dispatched, self.eof)
            && self.parts_in_flight == 0
            && self.pending_reads.is_empty()
    }

    /// Consumes a drained state and returns the fields needed by CompleteMultipartUpload.
    pub(crate) fn into_completion(self) -> (Arc<PartReader>, PartPlan, Vec<CompletedPart>, u64) {
        assert!(
            self.is_complete(),
            "multipart state completed while work remained"
        );
        (
            self.part_reader,
            self.plan,
            self.completed_parts,
            self.bytes_uploaded,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_counts(&self) -> (u64, usize, usize) {
        (
            self.parts_dispatched,
            self.parts_in_flight,
            self.pending_reads.entries.len(),
        )
    }
}

/// State machine for tracking upload work progress.
#[derive(Debug)]
pub(crate) enum UploadState {
    /// Waiting to start CreateMPU or PutObject.
    PendingInit {
        stream: Option<InputStream>,
        content_length: Option<u64>,
        init_in_flight: bool,
    },
    /// Multipart source production and part transmission are active.
    Transferring {
        upload_id: String,
        parts: PartTransferState,
        response_builder: UploadOutputBuilder,
    },
    /// All parts are done and CompleteMultipartUpload remains.
    Completing {
        upload_id: Option<String>,
        parts: Option<PartTransferState>,
        response_builder: Option<UploadOutputBuilder>,
        complete_in_flight: bool,
    },
    /// PutObject is in flight.
    PutObjectInFlight,
    /// The upload has completed.
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_of_stream_stops_known_and_unknown_dispatch() {
        let known = PartPlan::Known {
            total_parts: 10_000,
            declared_object_size: 100 * 1024 * 1024 * 1024,
        };
        assert!(!known.all_dispatched(1, false));
        assert!(known.all_dispatched(1, true));

        assert!(!PartPlan::Unknown.all_dispatched(10_000, false));
        assert!(PartPlan::Unknown.all_dispatched(1, true));
    }

    #[test]
    fn known_plan_can_complete_without_an_extra_end_of_stream_poll() {
        let known = PartPlan::Known {
            total_parts: 2,
            declared_object_size: 10,
        };
        assert!(!known.all_dispatched(1, false));
        assert!(known.all_dispatched(2, false));
    }

    #[test]
    fn wake_before_parking_remains_notified() {
        let wake = PartReadWake::new(Waker::noop().clone());
        Arc::clone(&wake).wake();

        assert!(wake.take_notification());
        assert!(!wake.take_notification());
    }
}
