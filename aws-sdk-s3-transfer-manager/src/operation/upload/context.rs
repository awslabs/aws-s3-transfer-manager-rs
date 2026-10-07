/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};

use aws_sdk_s3::types::CompletedPart;
use smallvec::SmallVec;

use crate::io::part_reader::{NextPartFuture, PartReader};
use crate::io::{InputStream, SizeHint};
#[cfg(test)]
use crate::operation::upload::observability::MultipartTransferSummary;
use crate::operation::upload::observability::{
    SourceReadObservation, UploadExecutionState, UploadObservability, UploadPartTiming,
    UploadStateSnapshot,
};
use crate::operation::upload::UploadOutputBuilder;
use crate::transfer::{PendingCategory, PendingCause};

/// Why multipart dispatch cannot schedule another source part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartTransferPendingReason {
    /// A caller-provided source operation is retained until its registered wake.
    SourceUnavailable,
    /// Every known part was dispatched or the source reported end-of-stream.
    DispatchClosed,
    /// No retained source operation is ready and no new operation can start.
    NoReadyPart,
}

impl From<PartTransferPendingReason> for PendingCause {
    fn from(reason: PartTransferPendingReason) -> Self {
        match reason {
            PartTransferPendingReason::SourceUnavailable => {
                Self::new(PendingCategory::Source, "source_unavailable")
            }
            PartTransferPendingReason::DispatchClosed => Self::in_flight_work("part_completion"),
            PartTransferPendingReason::NoReadyPart => Self::in_flight_work("part_work"),
        }
    }
}

/// Multipart dispatch boundary and source-size declaration.
///
/// [`PartStream`]: crate::io::PartStream
#[derive(Debug)]
pub(crate) enum PartPlan {
    /// A manager-partitioned byte or file source whose exact length defines its part count.
    Known {
        total_parts: u64,
        /// Size declared by the source and sent as `MpuObjectSize` on completion.
        ///
        /// This value is independent of multipart accounting, allowing S3 to detect an assembled
        /// object whose size differs from the source declaration.
        declared_object_size: u64,
    },
    /// A caller-provided stream whose part boundaries require reading through end-of-stream.
    ///
    /// The size hint may still be exact. It constrains total bytes, but cannot determine how many
    /// caller-defined parts the stream will produce.
    UntilEof { size_hint: SizeHint },
}

impl PartPlan {
    /// Whether part dispatch is complete.
    ///
    /// `parts_dispatched` includes speculative work that may not yet have produced a part. It
    /// therefore answers only whether dispatch is finished, not how many parts the source contains.
    fn all_dispatched(&self, parts_dispatched: u64, eof: bool) -> bool {
        match self {
            Self::Known { total_parts, .. } => parts_dispatched >= *total_parts,
            Self::UntilEof { .. } => eof,
        }
    }

    /// Returns whether dispatch must observe end-of-stream.
    pub(crate) fn reads_until_eof(&self) -> bool {
        matches!(self, Self::UntilEof { .. })
    }

    /// Returns the declared bounds on total source bytes.
    fn source_size_hint(&self) -> SizeHint {
        match self {
            Self::Known {
                declared_object_size,
                ..
            } => SizeHint::exact(*declared_object_size),
            Self::UntilEof { size_hint } => *size_hint,
        }
    }

    /// Rejects a source as soon as its output exceeds the declared upper bound.
    fn validate_progress(&self, actual: u64) -> Result<(), SizeHintViolation> {
        if let Some(upper) = self.source_size_hint().upper() {
            if actual > upper {
                return Err(SizeHintViolation::AboveUpper { actual, upper });
            }
        }
        Ok(())
    }

    /// Validates the final source size against both bounds.
    fn validate_complete(&self, actual: u64) -> Result<(), SizeHintViolation> {
        self.validate_progress(actual)?;
        let lower = self.source_size_hint().lower();
        if actual < lower {
            return Err(SizeHintViolation::BelowLower { actual, lower });
        }
        Ok(())
    }

    /// Returns the object size CompleteMultipartUpload must independently validate.
    pub(crate) fn mpu_object_size(&self, bytes_read: u64) -> Result<u64, SizeHintViolation> {
        self.validate_complete(bytes_read)?;
        let size_hint = self.source_size_hint();

        if size_hint.upper() == Some(size_hint.lower()) {
            Ok(size_hint.lower())
        } else {
            Ok(bytes_read)
        }
    }
}

/// A caller's size declarations contradict each other, or the stream contradicted them.
#[derive(Debug)]
pub(crate) enum SizeHintViolation {
    InvalidBounds { lower: u64, upper: u64 },
    BelowLower { actual: u64, lower: u64 },
    AboveUpper { actual: u64, upper: u64 },
}

impl fmt::Display for SizeHintViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBounds { lower, upper } => write!(
                formatter,
                "upload stream lower size bound {lower} exceeds upper bound {upper}"
            ),
            Self::BelowLower { actual, lower } => write!(
                formatter,
                "upload stream ended after {actual} bytes, below its declared lower bound of \
                 {lower} bytes"
            ),
            Self::AboveUpper { actual, upper } => write!(
                formatter,
                "upload stream produced {actual} bytes, exceeding its declared upper bound of \
                 {upper} bytes"
            ),
        }
    }
}

impl std::error::Error for SizeHintViolation {}

/// The request's declared `content_length` cannot be the body's exact size.
#[derive(Debug)]
pub(crate) enum ContentLengthViolation {
    /// The declared length is negative.
    Negative { content_length: i64 },
    /// The declared length lies outside the body's own size bounds.
    OutsideBounds {
        content_length: u64,
        lower: u64,
        upper: Option<u64>,
    },
}

impl fmt::Display for ContentLengthViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negative { content_length } => write!(
                formatter,
                "upload content_length {content_length} is negative"
            ),
            Self::OutsideBounds {
                content_length,
                lower,
                upper: Some(upper),
            } if lower == upper => write!(
                formatter,
                "upload content_length {content_length} does not match the body's size of \
                 {lower} bytes"
            ),
            Self::OutsideBounds {
                content_length,
                lower,
                upper: Some(upper),
            } => write!(
                formatter,
                "upload content_length {content_length} is outside the body's size bounds of \
                 {lower} to {upper} bytes"
            ),
            Self::OutsideBounds {
                content_length,
                lower,
                upper: None,
            } => write!(
                formatter,
                "upload content_length {content_length} is below the body's lower size bound of \
                 {lower} bytes"
            ),
        }
    }
}

impl std::error::Error for ContentLengthViolation {}

/// Largest part number S3 accepts. Part numbers run from 1 to this value.
const MAX_PART_NUMBER: u64 = 10_000;

/// A source part's number cannot identify a distinct position in the object.
#[derive(Debug)]
pub(crate) enum PartNumberViolation {
    /// The number is outside `1..=`[`MAX_PART_NUMBER`].
    ///
    /// The message names the part limit and its remedy, because this and the part-count limit
    /// describe the same mistake and either can report it first: parts are claimed under the state
    /// lock after their reads return, which need not be the order the stream yielded them, so a
    /// stream of more than [`MAX_PART_NUMBER`] parts can have its part numbered
    /// `MAX_PART_NUMBER + 1` claimed before the count trips.
    OutsideRange { part_number: u64 },
    /// Two completed parts carry the same number.
    ///
    /// Found when the part list is assembled for `CompleteMultipartUpload`, rather than as each
    /// part is read: a per-upload set of claimed numbers would cost memory on every multipart
    /// upload to catch what the assembled list already shows.
    Repeated { part_number: u64 },
}

impl fmt::Display for PartNumberViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutsideRange { part_number } => write!(
                formatter,
                "upload stream produced part number {part_number}, outside the range 1 to the \
                 maximum of {MAX_PART_NUMBER} parts; number parts from 1 to {MAX_PART_NUMBER}, \
                 and combine source data into fewer, larger parts if there are more"
            ),
            Self::Repeated { part_number } => write!(
                formatter,
                "upload stream produced part number {part_number} more than once"
            ),
        }
    }
}

impl std::error::Error for PartNumberViolation {}

/// Checks that `part_number` identifies a position S3 accepts.
///
/// S3 uses a part's number both to identify the part and to place it in the object, so a number
/// outside 1..=[`MAX_PART_NUMBER`] cannot be used. Numbers may arrive in any order and need not be
/// consecutive. Repeats are caught by [`validate_distinct_part_numbers`] when the part list is
/// assembled.
pub(crate) fn validate_part_number(part_number: u64) -> Result<(), PartNumberViolation> {
    if part_number == 0 || part_number > MAX_PART_NUMBER {
        return Err(PartNumberViolation::OutsideRange { part_number });
    }
    Ok(())
}

/// Checks that no two completed parts share a part number.
///
/// `parts` must already be sorted by part number, as `CompleteMultipartUpload` requires, so any
/// repeat is adjacent. A repeated number would place two parts at one position in the object, and
/// the completed list would describe an object neither part produced.
pub(crate) fn validate_distinct_part_numbers(
    parts: &[CompletedPart],
) -> Result<(), PartNumberViolation> {
    for pair in parts.windows(2) {
        if pair[0].part_number() == pair[1].part_number() {
            return Err(PartNumberViolation::Repeated {
                part_number: pair[0].part_number().unwrap_or_default() as u64,
            });
        }
    }
    Ok(())
}

/// Validates the bounds captured before source polling begins.
pub(crate) fn validate_size_hint(size_hint: SizeHint) -> Result<(), SizeHintViolation> {
    if let Some(upper) = size_hint.upper() {
        if size_hint.lower() > upper {
            return Err(SizeHintViolation::InvalidBounds {
                lower: size_hint.lower(),
                upper,
            });
        }
    }
    Ok(())
}

/// Narrows a body's size bounds to the request's declared `content_length`.
///
/// A declared length is the exact number of bytes the body must produce, so the result is
/// [`SizeHint::exact`] of that length; with no declaration, `size_hint` is returned unchanged.
/// `size_hint` must already have passed [`validate_size_hint`]. A negative length, or one outside
/// `size_hint`'s bounds, is rejected.
pub(crate) fn apply_content_length(
    size_hint: SizeHint,
    content_length: Option<i64>,
) -> Result<SizeHint, ContentLengthViolation> {
    let Some(content_length) = content_length else {
        return Ok(size_hint);
    };
    let declared = u64::try_from(content_length)
        .map_err(|_| ContentLengthViolation::Negative { content_length })?;
    let within_upper = size_hint.upper().is_none_or(|upper| declared <= upper);
    if declared < size_hint.lower() || !within_upper {
        return Err(ContentLengthViolation::OutsideBounds {
            content_length: declared,
            lower: size_hint.lower(),
            upper: size_hint.upper(),
        });
    }
    Ok(SizeHint::exact(declared))
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
    kind: UploadPartWorkKind,
}

/// Source of one UploadPart work item.
#[derive(Debug)]
enum UploadPartWorkKind {
    /// A part supplied by the configured upload source.
    SourceRead,
    /// The canonical zero-byte part required to complete an empty MPU object.
    EmptyObject,
}

impl UploadPartWork {
    /// Starts a newly admitted source read.
    fn fresh(timing: UploadPartTiming) -> Self {
        Self {
            pending_read: None,
            timing: Some(timing),
            kind: UploadPartWorkKind::SourceRead,
        }
    }

    /// Resumes the exact source future retained after `Poll::Pending`.
    fn resumed(read: PendingPartRead) -> Self {
        Self {
            pending_read: Some(read),
            timing: None,
            kind: UploadPartWorkKind::SourceRead,
        }
    }

    /// Sends the canonical zero-byte part required for an empty MPU object.
    fn empty_object(timing: UploadPartTiming) -> Self {
        Self {
            pending_read: None,
            timing: Some(timing),
            kind: UploadPartWorkKind::EmptyObject,
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

    pub(crate) fn is_empty_object(&self) -> bool {
        matches!(self.kind, UploadPartWorkKind::EmptyObject)
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
    /// Number of `PartData` values returned by source reads.
    parts_read: u64,
    /// Bytes returned by source reads before UploadPart submission.
    bytes_read: u64,
    /// Bytes accepted by completed UploadPart requests.
    bytes_uploaded: u64,
    /// Optional aggregate collection and event-reporting policy.
    observability: UploadObservability,
}

impl PartTransferState {
    pub(crate) fn new(
        part_reader: Arc<PartReader>,
        plan: PartPlan,
        completed_parts_capacity: usize,
        observability: UploadObservability,
    ) -> Self {
        Self {
            part_reader,
            plan,
            parts_dispatched: 0,
            eof: false,
            parts_in_flight: 0,
            pending_reads: PendingPartReads::default(),
            completed_parts: Vec::with_capacity(completed_parts_capacity),
            parts_read: 0,
            bytes_read: 0,
            bytes_uploaded: 0,
            observability,
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
        Some(UploadPartWork::fresh(self.observability.part_scheduled()))
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
        self.observability.record_read_pending(timing);
    }

    /// Moves one active part from source preparation to UploadPart transmission.
    pub(crate) fn begin_upload(
        &mut self,
        presentation_segments: usize,
        timing: UploadPartTiming,
    ) -> SourceReadObservation {
        self.observability
            .begin_upload(presentation_segments, timing)
    }

    /// Records one source part before its UploadPart request starts.
    pub(crate) fn observe_part(&mut self, bytes: u64) -> Result<u64, SizeHintViolation> {
        self.parts_read = self
            .parts_read
            .checked_add(1)
            .expect("source part count overflow");
        self.bytes_read = self
            .bytes_read
            .checked_add(bytes)
            .expect("source byte count overflow");
        self.plan.validate_progress(self.bytes_read)?;
        Ok(self.parts_read)
    }

    /// Records end-of-stream and returns whether this call changed source state.
    pub(crate) fn record_end_of_stream(&mut self) -> bool {
        if self.eof {
            return false;
        }
        self.eof = true;
        self.record_dispatch_closed();
        true
    }

    /// Retires scheduled work that ended without starting an UploadPart request.
    ///
    /// Returns whether the drained source now requires its empty-object part.
    pub(crate) fn finish_read_without_part(&mut self) -> bool {
        self.parts_in_flight = self
            .parts_in_flight
            .checked_sub(1)
            .expect("multipart in-flight completion underflow");
        self.needs_empty_object_part()
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
        self.observability.complete_upload();
    }

    /// Returns whether source part count is discovered by reading through EOF.
    pub(crate) fn reads_until_eof(&self) -> bool {
        self.plan.reads_until_eof()
    }

    /// Returns whether source dispatch is closed and all source work has retired.
    fn source_drained(&self) -> bool {
        self.plan.all_dispatched(self.parts_dispatched, self.eof)
            && self.parts_in_flight == 0
            && self.pending_reads.is_empty()
    }

    /// Returns whether drained source state requires the single empty part S3 MPU expects.
    fn needs_empty_object_part(&self) -> bool {
        let source_returned_no_parts = self.parts_read == 0;

        // The empty-object part does not increment `parts_read`, but its completion is recorded here.
        // Requiring an empty list makes this predicate false after that part completes.
        let no_part_was_already_uploaded = self.completed_parts.is_empty();

        // A zero lower bound means an empty source satisfies its declared size bounds.
        let empty_satisfies_size_hint = self.plan.source_size_hint().lower() == 0;

        self.source_drained()
            && source_returned_no_parts
            && no_part_was_already_uploaded
            && empty_satisfies_size_hint
    }

    /// Starts the one zero-byte part required for a valid empty MPU source, when needed.
    pub(crate) fn maybe_start_empty_object_part(&mut self) -> Option<UploadPartWork> {
        if !self.needs_empty_object_part() {
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
        Some(UploadPartWork::empty_object(
            self.observability.part_scheduled(),
        ))
    }

    /// Returns whether dispatch is closed and all scheduled work has retired.
    pub(crate) fn is_complete(&self) -> bool {
        self.source_drained() && !self.needs_empty_object_part()
    }

    /// Consumes drained multipart state.
    pub(crate) fn into_completion(self) -> MultipartCompletion {
        assert!(
            self.is_complete(),
            "multipart state completed while work remained"
        );
        assert_eq!(
            self.bytes_read, self.bytes_uploaded,
            "multipart completion lost or duplicated source bytes"
        );
        let final_snapshot = self.snapshot();
        self.observability.finish_body();
        MultipartCompletion {
            part_reader: self.part_reader,
            plan: self.plan,
            completed_parts: self.completed_parts,
            bytes_read: self.bytes_read,
            final_snapshot,
        }
    }

    /// Returns a coherent view while the caller holds the upload-state lock.
    pub(crate) fn snapshot(&self) -> UploadStateSnapshot {
        UploadStateSnapshot {
            state: UploadExecutionState::Transferring,
            parts_dispatched: self.parts_dispatched,
            parts_in_flight: self.parts_in_flight,
            uploads_in_flight: self.observability.uploads_in_flight(),
            pending_reads: self.pending_reads.entries.len(),
            completed_parts: self.completed_parts.len(),
            bytes_read: self.bytes_read,
            bytes_uploaded: self.bytes_uploaded,
            eof: self.eof,
            dispatch_closed: self.plan.all_dispatched(self.parts_dispatched, self.eof),
        }
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
        self.observability
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
    pub(crate) fn test_summary(&self) -> MultipartTransferSummary {
        self.observability
            .test_summary(self.snapshot())
            .expect("test transfer diagnostics should be enabled")
    }
}

/// Drained multipart state consumed by CompleteMultipartUpload.
pub(crate) struct MultipartCompletion {
    pub(crate) part_reader: Arc<PartReader>,
    pub(crate) plan: PartPlan,
    pub(crate) completed_parts: Vec<CompletedPart>,
    pub(crate) bytes_read: u64,
    pub(crate) final_snapshot: UploadStateSnapshot,
}

/// State machine for tracking upload work progress.
///
/// A multipart upload ID is held from `Transferring` through `CompleteInFlight`
/// and leaves the state only when CompleteMultipartUpload succeeds.
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
    /// All parts are done; CompleteMultipartUpload has not been dispatched.
    Completing {
        upload_id: String,
        parts: PartTransferState,
        response_builder: UploadOutputBuilder,
    },
    /// CompleteMultipartUpload has been dispatched and has not succeeded.
    ///
    /// The multipart upload remains open in this state, including after the
    /// request fails or its execution is dropped, so it can still be aborted.
    CompleteInFlight { upload_id: String },
    /// PutObject is in flight.
    PutObjectInFlight,
    /// S3 has committed the object; no multipart upload remains open.
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_and_end_of_stream_plans_close_at_their_own_boundaries() {
        let known = PartPlan::Known {
            total_parts: 10_000,
            declared_object_size: 100 * 1024 * 1024 * 1024,
        };
        assert!(!known.all_dispatched(1, true));
        assert!(known.all_dispatched(10_000, false));

        let until_eof = PartPlan::UntilEof {
            size_hint: SizeHint::default().with_upper(Some(10)),
        };
        assert!(!until_eof.all_dispatched(10_000, false));
        assert!(until_eof.all_dispatched(1, true));
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
    fn end_of_stream_bounds_reject_underflow_and_overflow() {
        let bounded = PartPlan::UntilEof {
            size_hint: SizeHint::default().with_lower(5).with_upper(Some(10)),
        };
        assert!(bounded.validate_complete(4).is_err());
        assert!(bounded.validate_complete(5).is_ok());
        assert!(bounded.validate_complete(10).is_ok());
        assert!(bounded.validate_complete(11).is_err());
    }

    #[test]
    fn exact_size_remains_the_mpu_object_size_witness() {
        let exact = PartPlan::UntilEof {
            size_hint: SizeHint::exact(7),
        };
        assert_eq!(7, exact.mpu_object_size(7).unwrap());

        let bounded = PartPlan::UntilEof {
            size_hint: SizeHint::default().with_lower(5).with_upper(Some(10)),
        };
        assert_eq!(7, bounded.mpu_object_size(7).unwrap());
    }

    #[test]
    fn content_length_narrows_bounds_to_an_exact_size() {
        let bounds = |hint: SizeHint| (hint.lower(), hint.upper());
        let bounded = SizeHint::default().with_lower(5).with_upper(Some(10));
        assert_eq!(
            (5, Some(10)),
            bounds(apply_content_length(bounded, None).unwrap())
        );
        for content_length in [5, 7, 10] {
            let declared = content_length as u64;
            assert_eq!(
                (declared, Some(declared)),
                bounds(apply_content_length(bounded, Some(content_length)).unwrap())
            );
        }
        for content_length in [4, 11] {
            assert!(matches!(
                apply_content_length(bounded, Some(content_length)),
                Err(ContentLengthViolation::OutsideBounds { .. })
            ));
        }
        assert!(matches!(
            apply_content_length(SizeHint::default(), Some(-1)),
            Err(ContentLengthViolation::Negative { content_length: -1 })
        ));
        let largest = i64::MAX as u64;
        assert_eq!(
            (largest, Some(largest)),
            bounds(apply_content_length(SizeHint::default(), Some(i64::MAX)).unwrap())
        );
    }

    #[test]
    fn part_numbers_within_s3_range_are_accepted_in_any_order() {
        for part_number in [MAX_PART_NUMBER, 65, 64, 1, 2] {
            validate_part_number(part_number).unwrap();
        }
        for part_number in [0, MAX_PART_NUMBER + 1, (1 << 32) + 1, u64::MAX] {
            assert!(matches!(
                validate_part_number(part_number),
                Err(PartNumberViolation::OutsideRange { part_number: n }) if n == part_number
            ));
        }
    }

    #[test]
    fn repeated_part_numbers_are_found_in_the_assembled_part_list() {
        let part = |n: i32| CompletedPart::builder().part_number(n).build();

        validate_distinct_part_numbers(&[]).unwrap();
        validate_distinct_part_numbers(&[part(1)]).unwrap();
        validate_distinct_part_numbers(&[part(1), part(2), part(7)]).unwrap();
        assert!(matches!(
            validate_distinct_part_numbers(&[part(1), part(2), part(2), part(3)]),
            Err(PartNumberViolation::Repeated { part_number: 2 })
        ));
    }

    #[test]
    fn wake_before_parking_remains_notified() {
        let wake = PartReadWake::new(Waker::noop().clone());
        Arc::clone(&wake).wake();

        assert!(wake.take_notification());
        assert!(!wake.take_notification());
    }
}
