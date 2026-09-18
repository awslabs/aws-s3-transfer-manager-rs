/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};

use aws_sdk_s3::types::CompletedPart;
use smallvec::SmallVec;

use crate::io::part_reader::{NextPartFuture, PartReader};
use crate::io::{InputStream, SizeHint};
#[cfg(test)]
use crate::operation::upload::diagnostics::PartTransferSummary;
use crate::operation::upload::diagnostics::{
    PartTransferPendingReason, PartTransferSnapshot, SourceReadObservation, UploadPartTiming,
    UploadTransferDiagnostics,
};
use crate::operation::upload::UploadOutputBuilder;

/// Source-size contract and multipart dispatch boundary.
///
/// [`PartStream`]: crate::io::PartStream
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// A byte or file source whose exact length defines its part count.
    Fixed {
        total_parts: u64,
        expected_size: u64,
    },
    /// A caller-provided stream whose part boundaries are independent of the configured part size.
    ///
    /// Dispatch continues until EOF. Equal bounds are an exact contract; a missing upper bound
    /// permits any size at or above `lower`.
    Streaming { lower: u64, upper: Option<u64> },
}

impl PartPlan {
    /// Whether part dispatch is complete.
    ///
    /// `parts_dispatched` includes speculative work that may not yet have produced a part. It
    /// therefore answers only whether dispatch is finished, not how many parts the source contains.
    fn all_dispatched(&self, parts_dispatched: u64, eof: bool) -> bool {
        match self {
            Self::Fixed { total_parts, .. } => parts_dispatched >= *total_parts,
            Self::Streaming { .. } => eof,
        }
    }

    /// Returns whether source part count is known independently of EOF.
    pub(crate) fn is_streaming(&self) -> bool {
        matches!(self, Self::Streaming { .. })
    }

    /// Returns whether a zero-byte stream satisfies its declared lower bound.
    fn permits_empty(&self) -> bool {
        self.bounds().0 == 0
    }

    /// Rejects a source as soon as its output exceeds the declared upper bound.
    fn validate_progress(&self, actual: u64) -> io::Result<()> {
        let (_, upper) = self.bounds();
        if let Some(upper) = upper {
            if actual > upper {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "upload stream produced {actual} bytes, exceeding its declared upper \
                         bound of {upper} bytes"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Validates the final source size against both bounds.
    pub(crate) fn validate_complete(&self, actual: u64) -> io::Result<()> {
        self.validate_progress(actual)?;
        let (lower, _) = self.bounds();
        if actual < lower {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "upload stream ended after {actual} bytes, below its declared lower bound of \
                     {lower} bytes"
                ),
            ));
        }
        Ok(())
    }

    fn bounds(&self) -> (u64, Option<u64>) {
        match self {
            Self::Fixed { expected_size, .. } => (*expected_size, Some(*expected_size)),
            Self::Streaming { lower, upper } => (*lower, *upper),
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
    pub(crate) timing: UploadPartTiming,
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
    timing: Option<UploadPartTiming>,
}

impl UploadPartWork {
    fn fresh(timing: UploadPartTiming) -> Self {
        Self {
            pending_read: None,
            timing: Some(timing),
        }
    }

    fn resumed(read: PendingPartRead) -> Self {
        Self {
            pending_read: Some(read),
            timing: None,
        }
    }

    pub(crate) fn take_pending_read(&mut self) -> Option<PendingPartRead> {
        self.pending_read.take()
    }

    pub(crate) fn take_timing(&mut self) -> UploadPartTiming {
        self.timing
            .take()
            .expect("fresh upload work should retain diagnostic timing")
    }

    pub(crate) fn is_resumed(&self) -> bool {
        self.pending_read.is_some()
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
    /// Bytes yielded by the source, including parts whose requests remain in flight.
    bytes_yielded: u64,
    /// Optional aggregate collection and transition-reporting policy.
    diagnostics: UploadTransferDiagnostics,
}

impl PartTransferState {
    pub(crate) fn new(
        part_reader: Arc<PartReader>,
        plan: PartPlan,
        completed_parts_capacity: usize,
        diagnostics: UploadTransferDiagnostics,
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
            bytes_yielded: 0,
            diagnostics,
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
        self.record_dispatch_closed();
        Some(UploadPartWork::fresh(self.diagnostics.part_scheduled()))
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
        self.record_dispatch_closed();
    }

    /// Retains one exact source operation without changing its in-flight accounting.
    pub(crate) fn park_read(&mut self, read: PendingPartRead) {
        self.pending_reads.push(read);
    }

    /// Records one source-future poll that could not yet produce a part.
    pub(crate) fn record_read_pending(&mut self, timing: &mut UploadPartTiming) {
        self.diagnostics.record_read_pending(timing);
    }

    /// Moves one active part from source preparation to UploadPart transmission.
    pub(crate) fn begin_upload(
        &mut self,
        presentation_segments: usize,
        timing: UploadPartTiming,
    ) -> SourceReadObservation {
        self.diagnostics.begin_upload(presentation_segments, timing)
    }

    /// Records one part before its UploadPart request starts.
    pub(crate) fn observe_part(&mut self, bytes: u64) -> io::Result<()> {
        self.bytes_yielded = self
            .bytes_yielded
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("upload stream byte count overflowed"))?;
        self.plan.validate_progress(self.bytes_yielded)
    }

    /// Records end-of-stream and returns whether this caller owns empty-stream synthesis.
    ///
    /// The false-to-true transition elects one caller. Empty-part synthesis is valid only when the
    /// source's lower bound permits zero bytes.
    pub(crate) fn observe_end_of_stream(&mut self, parts_yielded: u64) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        self.eof = true;
        self.record_dispatch_closed();
        self.plan.validate_complete(self.bytes_yielded)?;
        Ok(self.plan.permits_empty() && parts_yielded == 0)
    }

    /// Retires scheduled work that ended without starting an UploadPart request.
    pub(crate) fn finish_read_without_part(&mut self) {
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("multipart in-flight completion underflow");
    }

    /// Records one successfully uploaded part.
    pub(crate) fn complete_part(
        &mut self,
        part: CompletedPart,
        bytes_uploaded: u64,
        request_elapsed: Option<std::time::Duration>,
    ) {
        self.completed_parts.push(part);
        self.bytes_uploaded = self
            .bytes_uploaded
            .checked_add(bytes_uploaded)
            .expect("uploaded byte count overflow");
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("multipart in-flight completion underflow");
        self.diagnostics.complete_upload(request_elapsed);
    }

    /// Returns whether source part count is discovered by reading through EOF.
    pub(crate) fn is_streaming(&self) -> bool {
        self.plan.is_streaming()
    }

    /// Returns whether dispatch is closed and all scheduled work has retired.
    pub(crate) fn is_complete(&self) -> bool {
        self.plan.all_dispatched(self.parts_dispatched, self.eof)
            && self.parts_in_flight == 0
            && self.pending_reads.is_empty()
    }

    /// Consumes a drained state and returns the fields needed by CompleteMultipartUpload.
    pub(crate) fn into_completion(
        mut self,
    ) -> (
        Arc<PartReader>,
        PartPlan,
        Vec<CompletedPart>,
        u64,
        PartTransferSnapshot,
        UploadTransferDiagnostics,
    ) {
        assert!(
            self.is_complete(),
            "multipart state completed while work remained"
        );
        assert_eq!(
            self.bytes_yielded, self.bytes_uploaded,
            "multipart completion lost or duplicated source bytes"
        );
        let snapshot = self.snapshot();
        self.diagnostics.finish_body();
        (
            self.part_reader,
            self.plan,
            self.completed_parts,
            self.bytes_uploaded,
            snapshot,
            self.diagnostics,
        )
    }

    /// Returns a coherent view while the caller holds the upload-state lock.
    pub(crate) fn snapshot(&self) -> PartTransferSnapshot {
        PartTransferSnapshot {
            parts_dispatched: self.parts_dispatched,
            parts_in_flight: self.parts_in_flight,
            uploads_in_flight: self.diagnostics.uploads_in_flight(),
            pending_reads: self.pending_reads.entries.len(),
            completed_parts: self.completed_parts.len(),
            bytes_uploaded: self.bytes_uploaded,
            eof: self.eof,
            dispatch_closed: self.plan.all_dispatched(self.parts_dispatched, self.eof),
        }
    }

    /// Returns a coherent snapshot only when transition reporting is enabled.
    pub(crate) fn transition_snapshot(&self) -> Option<PartTransferSnapshot> {
        self.diagnostics.transition_snapshot(self.snapshot())
    }

    /// Classifies why `poll_work` cannot schedule another part from this state.
    pub(crate) fn pending_reason(&self) -> PartTransferPendingReason {
        if self.part_reader.source_unavailable() {
            PartTransferPendingReason::SourceUnavailable
        } else if self.plan.all_dispatched(self.parts_dispatched, self.eof) {
            PartTransferPendingReason::DispatchClosed
        } else {
            PartTransferPendingReason::NoReadyPart
        }
    }

    fn record_dispatch_closed(&mut self) {
        self.diagnostics
            .set_dispatch_closed(self.plan.all_dispatched(self.parts_dispatched, self.eof));
    }

    #[cfg(test)]
    pub(crate) fn test_counts(&self) -> (u64, usize, usize) {
        (
            self.parts_dispatched,
            self.parts_in_flight,
            self.pending_reads.entries.len(),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_summary(&self) -> PartTransferSummary {
        self.diagnostics
            .test_summary(self.snapshot())
            .expect("test transfer diagnostics should be enabled")
    }
}

/// State machine for tracking upload work progress.
#[derive(Debug)]
pub(crate) enum UploadState {
    /// Waiting to start CreateMPU or PutObject.
    PendingInit {
        stream: Option<InputStream>,
        size_hint: SizeHint,
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
    fn fixed_and_streaming_plans_close_at_their_own_boundaries() {
        let fixed = PartPlan::Fixed {
            total_parts: 10_000,
            expected_size: 100 * 1024 * 1024 * 1024,
        };
        assert!(!fixed.all_dispatched(1, true));
        assert!(fixed.all_dispatched(10_000, false));

        let streaming = PartPlan::Streaming {
            lower: 0,
            upper: Some(10),
        };
        assert!(!streaming.all_dispatched(10_000, false));
        assert!(streaming.all_dispatched(1, true));
    }

    #[test]
    fn fixed_plan_can_complete_without_an_extra_end_of_stream_poll() {
        let fixed = PartPlan::Fixed {
            total_parts: 2,
            expected_size: 10,
        };
        assert!(!fixed.all_dispatched(1, false));
        assert!(fixed.all_dispatched(2, false));
    }

    #[test]
    fn streaming_bounds_reject_underflow_and_overflow() {
        let bounded = PartPlan::Streaming {
            lower: 5,
            upper: Some(10),
        };
        assert!(bounded.validate_complete(4).is_err());
        assert!(bounded.validate_complete(5).is_ok());
        assert!(bounded.validate_complete(10).is_ok());
        assert!(bounded.validate_complete(11).is_err());
    }

    #[test]
    fn wake_before_parking_remains_notified() {
        let wake = PartReadWake::new(Waker::noop().clone());
        Arc::clone(&wake).wake();

        assert!(wake.take_notification());
        assert!(!wake.take_notification());
    }
}
