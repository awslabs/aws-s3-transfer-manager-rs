/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::BufMut;
use futures_util::future::{select, Either};

use super::input::copy_fields_to_get_object_request;

use crate::error::{self, ChunkRef, Error};
use crate::operation::download::body::{BodySlot, BodyWriter, ChunkOutput};
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::{DownloadState, PendingClaim};
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::read_ahead::ReadAhead;
use crate::operation::download::recv_buffer::{DrainMode, FillOutcome};
use crate::operation::download::DownloadInput;
use crate::runtime::buffer_pool::{
    AcquireError, BufferPool, PooledBufMut, Reservation, ReserveError, SegmentedBytes,
};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::BucketType;
use tracing::Instrument;

/// Download-specific work data.
#[derive(Debug)]
pub(crate) enum DownloadWork {
    Discovery,
    GetObjectRange {
        range: std::ops::RangeInclusive<u64>,
        slot: Option<BodySlot>,
        etag: Option<Arc<str>>,
    },
    /// Memory-pressure relief: flush the resident filled run to disk, returning
    /// its carrier charges before the transfer waits for more memory. Handled in
    /// `execute`; carries no data because it operates on the writer.
    DrainResident,
}

/// Bytes in the next chunk sliced from `remaining` at `part_size`: the reservation
/// size for that chunk. Peek only — does not advance `remaining`.
fn chunk_len(remaining: &std::ops::RangeInclusive<u64>, part_size: u64) -> usize {
    let start = *remaining.start();
    let end = *remaining.end();
    let chunk_end = cmp::min(start + part_size - 1, end);
    (chunk_end - start + 1) as usize
}

/// Copies one transport response into reserved pooled storage.
///
/// One mutable stream spans every foreign frame so writable carrier tails are
/// reused. A failed attempt drops that stream before returning to the retry
/// loop, restoring its direct acquisition authority to `reservation`.
async fn collect_response_body(
    pool: &BufferPool,
    body: aws_sdk_s3::primitives::ByteStream,
    reservation: &Reservation,
    expected_len: usize,
    is_active: impl Fn() -> bool,
) -> Result<SegmentedBytes, Error> {
    let mut pooled: Option<PooledBufMut> = None;
    let mut bytes_received = 0usize;
    let mut body_stream = body;

    while let Some(result) = body_stream.next().await {
        let data = result.map_err(|error| error::body_read_error(error, None))?;
        let remaining = expected_len
            .checked_sub(bytes_received)
            .ok_or_else(|| response_length_error(expected_len, bytes_received))?;
        if data.len() > remaining {
            return Err(response_length_error(
                expected_len,
                bytes_received.saturating_add(data.len()),
            ));
        }

        if !data.is_empty() {
            let output = match &mut pooled {
                Some(output) => output,
                slot @ None => slot.insert(
                    pool.acquire(reservation, data.len())
                        .map_err(pool_acquisition_error)?,
                ),
            };
            if output.remaining_mut() < data.len() {
                output.reserve(data.len()).map_err(pool_acquisition_error)?;
            }
            output.put_slice(&data);
            bytes_received += data.len();
        }

        if !is_active() {
            return Err(Error::new(
                error::ErrorKind::OperationCancelled,
                "transfer cancelled during body read",
            ));
        }
    }

    if bytes_received != expected_len {
        return Err(response_length_error(expected_len, bytes_received));
    }

    pooled
        .map(PooledBufMut::freeze)
        .ok_or_else(|| response_length_error(expected_len, bytes_received))
}

/// Maps a malformed response length to the body-read retry class.
fn response_length_error(expected: usize, received: usize) -> Error {
    Error::new(
        error::ErrorKind::IOError,
        format!("response body length mismatch: expected {expected} bytes, received {received}"),
    )
}

/// Pool acquisition cannot be repaired by reissuing the network request.
fn pool_acquisition_error(source: AcquireError) -> Error {
    Error::new(error::ErrorKind::RuntimeError, source)
}

/// Early return if transfer is terminal (failed/cancelled by another work item).
macro_rules! bail_if_terminal {
    ($self:expr) => {
        if !$self.inner.ctx.is_active() {
            // Bailing before any drain: no occupancy freed on this path. The transfer is
            // already terminal so `decrement_in_flight` will find `Terminal` and return false.
            let _ = $self.decrement_in_flight(0);
            return WorkOutcome::Cancelled;
        }
    };
}

/// Download transfer that generates and executes download work.
///
/// Clone shares all state via `Arc`.
#[derive(Debug, Clone)]
pub(crate) struct DownloadTransfer {
    inner: Arc<DownloadTransferInner>,
}

/// Internal state for download transfer.
#[derive(Debug)]
struct DownloadTransferInner {
    /// Common transfer lifecycle management
    ctx: TransferContext,
    /// State machine for work progression
    state: Mutex<DownloadState>,
    /// The original request
    request: Arc<DownloadInput>,
    /// Type of S3 bucket targeted by this operation.
    // TODO(vnext): unify bucket representation (name + kind) across operations.
    #[allow(dead_code)]
    bucket_type: BucketType,
    /// Chunk delivery + disk-write surface.
    writer: BodyWriter,
    /// Read-ahead window: bounds resident occupancy by holding `issued - released`
    /// under `read_ahead.window()`.
    read_ahead: ReadAhead,
    /// Object metadata from discovery (set once discovery completes)
    object_meta: std::sync::OnceLock<ObjectMetadata>,
    /// Object-integrity result (set once discovery completes)
    integrity_checks: std::sync::OnceLock<crate::types::IntegrityChecks>,
    /// Notified when discovery completes (success or failure)
    discovery_notify: tokio::sync::Notify,
}

impl DownloadTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        bucket_type: BucketType,
        input: DownloadInput,
        writer: BodyWriter,
    ) -> Self {
        // Resolve the read-ahead knob (per-request override, else client default)
        // to a window in parts before `ctx` and `input` are moved into the struct.
        let window =
            super::read_ahead::resolve_window(input.read_ahead(), ctx.handle.config.read_ahead());
        let read_ahead = ReadAhead::with_window(window);
        // Couple the disk drain batch to the initial window, so a window below the
        // segment size drains in smaller runs from the first part.
        writer.sync_drain_batch(window);
        let inner = Arc::new(DownloadTransferInner {
            read_ahead,
            ctx,
            state: Mutex::new(DownloadState::new()),
            request: Arc::new(input),
            bucket_type,
            writer,
            object_meta: std::sync::OnceLock::new(),
            integrity_checks: std::sync::OnceLock::new(),
            discovery_notify: tokio::sync::Notify::new(),
        });
        Self { inner }
    }

    /// Access the transfer context.
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    /// Get the transfer ID.
    pub(crate) fn id(&self) -> TransferId {
        self.inner.ctx.id
    }

    /// Body writer for chunk delivery and disk writes.
    pub(crate) fn writer(&self) -> &BodyWriter {
        &self.inner.writer
    }

    /// The read-ahead window. The gate reads `self.inner.read_ahead` directly and the
    /// control surface goes through [`set_read_ahead`](Self::set_read_ahead); this
    /// accessor is for tests.
    #[cfg(test)]
    pub(crate) fn read_ahead(&self) -> &ReadAhead {
        &self.inner.read_ahead
    }

    /// I/O controls for this transfer, for exercising the public control surface in
    /// tests. Production access is via
    /// [`DownloadHandle::io_ctl`](super::handle::DownloadHandle::io_ctl).
    #[cfg(test)]
    pub(crate) fn io_ctl(&self) -> super::handle::DownloadIoCtl<'_> {
        super::handle::DownloadIoCtl::for_test(self)
    }

    /// Apply a dynamic read-ahead change, resolving the public knob to a window. The
    /// next issuance-gate read observes the new value, and the disk drain batch is
    /// re-coupled to it so a window below the segment size drains in smaller runs.
    pub(crate) fn set_read_ahead(&self, mode: &crate::types::ReadAhead) {
        let window = super::read_ahead::window_parts_for(mode);
        self.inner.read_ahead.set_window(window);
        self.inner.writer.sync_drain_batch(window);
    }

    /// Release one part of read-ahead occupancy for a stream delivery, then wake the
    /// issuer.
    ///
    /// Called from `Body::next` after `poll_next` delivers a chunk (which frees exactly
    /// one part's payload). The release runs **under the state lock** — the same lock
    /// `poll_work` holds while it reads the gate and arms `set_pending` — so the
    /// mutator protocol `lock → mutate → unlock → try_wake` holds: either the gate
    /// observes this release and returns `Ready` without parking, or it parks and the
    /// `try_wake` below (after the lock is dropped) fires. Without the shared lock the
    /// two are unordered and the wake can be lost (the store-buffer race).
    ///
    /// The wake is unconditional, mirroring the disk completion path
    /// ([`decrement_in_flight`](Self::decrement_in_flight)): `try_wake` is a no-op
    /// unless the issuer actually parked, so there is no need to compute whether this
    /// release reopened the gate — waking always is correct and adds only an atomic
    /// swap on the (cold, per-part) delivery path.
    pub(crate) fn release_stream_occupancy(&self) {
        {
            let mut work = self.inner.state.lock().unwrap();
            if let DownloadState::Transferring { gate, .. } = &mut *work {
                gate.release(1);
            }
            // Terminal (or pre-transfer): no gate to release.
        }
        self.inner.ctx.try_wake();
    }

    /// Object metadata from discovery.
    pub(crate) fn object_meta(&self) -> Option<&ObjectMetadata> {
        self.inner.object_meta.get()
    }

    /// Object-integrity result from discovery.
    pub(crate) fn integrity_checks(&self) -> Option<&crate::types::IntegrityChecks> {
        self.inner.integrity_checks.get()
    }

    /// Notified when discovery completes.
    pub(crate) fn discovery_notify(&self) -> &tokio::sync::Notify {
        &self.inner.discovery_notify
    }

    /// Whether download checksum validation is in effect, resolved the same way
    /// the SDK's GetObject mutator does: enabled if the request set
    /// `ChecksumMode=Enabled`, or it is unset and the client's
    /// `ResponseChecksumValidation` is not `WhenRequired` (`WhenSupported`, the
    /// default, and unknown values enable it).
    fn validation_enabled(&self, input: &DownloadInput) -> bool {
        match input.checksum_mode {
            Some(aws_sdk_s3::types::ChecksumMode::Enabled) => true,
            _ => !matches!(
                self.ctx()
                    .s3_client()
                    .config()
                    .response_checksum_validation()
                    .copied()
                    .unwrap_or_default(),
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired
            ),
        }
    }

    /// Poll for the next work item.
    ///
    /// Returns:
    /// - `PollWork::Ready { .. }` - work available to execute
    /// - `PollWork::Pending` - waiting for in-flight work to complete
    /// - `PollWork::Done` - transfer complete
    #[tracing::instrument(level = "debug", skip(self), fields(tid = %self.id()))]
    pub(crate) fn poll_work(&self) -> PollWork {
        if !self.inner.ctx.is_active() {
            tracing::debug!("not active, returning Done");
            return PollWork::Done;
        }

        let mut state = self.inner.state.lock().unwrap();

        match &mut *state {
            DownloadState::PendingDiscovery => {
                *state = DownloadState::DiscoveryInFlight;
                PollWork::ready(IoRequest {
                    data: Some(Box::new(DownloadWork::Discovery)),
                })
            }
            DownloadState::DiscoveryInFlight => {
                self.inner.ctx.set_pending();
                PollWork::Pending
            }
            DownloadState::Transferring {
                remaining,
                ranges_in_flight,
                etag,
                part_size,
                gate,
                pending,
            } => {
                // Resolve the slot to issue into this poll, if one is ready. Two gates
                // compose in order: the per-transfer read-ahead window, then shared
                // memory admission, so issuance takes their min. The window is gated first
                // (via `gate.try_issue`) so a window-blocked transfer never holds a slice
                // of shared capacity it cannot use yet, starving other transfers.
                let slot = if pending.is_some() {
                    // A memory-blocked claim takes priority: the gate already admitted and
                    // counted its slot (held in `pending`, not re-gated), waiting only on
                    // its queued memory reservation.
                    match self.resume_pending_claim(pending) {
                        Ok(Some(slot)) => slot,
                        // Not granted yet; the reservation future registered the
                        // scheduler waker. Flush any resident run first so it
                        // does not retain memory while this claim is blocked.
                        Ok(None) => return self.poll_memory_blocked(),
                        Err(error) => return self.fail_memory_admission(state, error),
                    }
                } else if let Some(range) = remaining.as_ref() {
                    // Read-ahead gate. The gate bounds resident occupancy at
                    // `issued - released < window`, where `released` counts both delivery
                    // surfaces (stream pull and disk drain), so a disk download paces to
                    // its drain rate rather than to the in-order delivery cursor.
                    // `try_issue` reads and mutates the gate under this state lock; the
                    // consumer's `release` does the same, so the two are ordered and a
                    // release that reopens the gate cannot be lost against the
                    // `set_pending` below (the mutator protocol).
                    //
                    // The gate guards issuance only. Once every range is generated
                    // (`remaining` is None) the completion path below runs unconditionally,
                    // so a transfer whose last parts are still resident (e.g. a disk tail
                    // below the drain batch, freed only by the terminal drain in
                    // `complete`) does not block on the gate with nothing left to issue.
                    let window = self.inner.read_ahead.window();
                    if !gate.try_issue(window) {
                        // Gate closed: issuance is `window` parts ahead of what the
                        // consumer has freed and waits for a drain before claiming more.
                        // Trace: fires every gated poll in steady state.
                        tracing::trace!(
                            target: crate::telemetry::TARGET_TRANSFER,
                            issued = gate.issued(),
                            released = gate.released(),
                            in_flight = gate.resident(),
                            window,
                            "read-ahead gate closed: issuance paused until the consumer drains",
                        );
                        return self.park();
                    }

                    // Gate admitted (and counted) the slot. Claim it from the buffer and
                    // reserve its backing memory against the shared budget. A grant issues now;
                    // a queued reservation stashes the claimed slot in `pending` and
                    // waits until the reservation future wakes us. The gate's `issued`
                    // bump stays: the blocked claim occupies its one window seat, so it
                    // holds exactly the slot it will fill.
                    let range_len = chunk_len(range, *part_size);
                    match self.reserve_claim(range_len, pending) {
                        Ok(Some(slot)) => slot,
                        // Memory-blocked; `pending` now holds the claimed slot.
                        // Flush any resident run before returning `Pending`.
                        Ok(None) => return self.poll_memory_blocked(),
                        Err(error) => return self.fail_memory_admission(state, error),
                    }
                } else if *ranges_in_flight > 0 {
                    // All ranges generated, waiting for in-flight to complete.
                    return self.park();
                } else {
                    // No-data completion: the object carried no ranges (0-byte object
                    // whose discovery produced no initial chunk). Data-carrying terminal
                    // completions happen in `execute` via `finalize_completion`.
                    self.complete(state);
                    return PollWork::Done;
                };

                // A slot is ready. Commit the range it issues, sliced from the unchanged
                // `remaining` (identical for a fresh claim or a resumed pending one).
                // `part_size` is the stored part size for a validated multipart object
                // (so each range aligns to a stored part boundary and S3 returns the
                // part's checksum for the SDK to validate), else the configured download
                // part size. Set at discovery.
                let part_size = *part_size;
                let range = remaining
                    .as_ref()
                    .expect("a ready slot implies a remaining range to issue");
                let start = *range.start();
                let end = *range.end();
                let chunk_end = cmp::min(start + part_size - 1, end);
                let chunk_range = start..=chunk_end;

                *ranges_in_flight += 1;

                if chunk_end < end {
                    *remaining = Some((chunk_end + 1)..=end);
                } else {
                    // The final range was just issued: issuance is done and the transfer
                    // drains its in-flight tail, completing on the next empty poll with
                    // nothing in flight. Logged once per transfer.
                    *remaining = None;
                    tracing::debug!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        issued = gate.issued(),
                        ranges_in_flight = *ranges_in_flight,
                        "all ranges issued; draining in-flight tail",
                    );
                }

                PollWork::ready(IoRequest {
                    data: Some(Box::new(DownloadWork::GetObjectRange {
                        range: chunk_range,
                        slot: Some(slot),
                        etag: etag.clone(),
                    })),
                })
            }
            DownloadState::Terminal => PollWork::Done,
        }
    }

    /// Park the transfer: mark it pending so the scheduler stops polling it until a
    /// waker re-readies it: the consumer freeing occupancy, or a GET
    /// completion decrementing the in-flight count. Memory reservations use their
    /// own scheduler-backed task waker.
    fn park(&self) -> PollWork {
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    /// Returns work that relieves memory pressure before a reservation wait.
    ///
    /// A transfer blocked on memory while holding a drainable resident run has
    /// no other path to release that run: it is below the normal drain batch,
    /// and the blocked part prevents terminal drain. Across concurrent disk
    /// transfers this can deadlock. Flush the resident run first, and return
    /// `Pending` only when there is nothing to flush.
    ///
    /// The pending reservation future has already registered a scheduler waker,
    /// so this path does not arm the transfer context's separate edge-triggered
    /// wake flag. A `has_drainable_resident` guard keeps it from emitting empty
    /// drains when an in-flight gap blocks the prefix or stream delivery owns
    /// progress.
    fn poll_memory_blocked(&self) -> PollWork {
        if self.inner.writer.has_drainable_resident() {
            PollWork::ready(IoRequest {
                data: Some(Box::new(DownloadWork::DrainResident)),
            })
        } else {
            PollWork::Pending
        }
    }

    /// Poll the reservation for a previously claimed slot.
    ///
    /// The read-ahead gate already counted this slot, so a pending claim is not
    /// re-gated. `Ok(None)` means the future remains linked in the
    /// memory-admission FIFO.
    fn resume_pending_claim(
        &self,
        pending: &mut Option<PendingClaim>,
    ) -> Result<Option<BodySlot>, ReserveError> {
        let waker = self.inner.ctx.scheduler_waker();
        let mut context = Context::from_waker(&waker);
        let reservation =
            match Pin::new(&mut pending.as_mut().unwrap().reservation).poll(&mut context) {
                Poll::Ready(result) => result?,
                Poll::Pending => return Ok(None),
            };

        let mut claim = pending.take().unwrap();
        claim.slot.attach_reservation(reservation);
        Ok(Some(claim.slot))
    }

    /// Claim a slot and reserve its backing memory against the shared budget. The
    /// read-ahead gate has already admitted (and counted) this slot. Returns the slot
    /// with its reservation attached on an immediate grant; on a queued reservation,
    /// stashes the claimed slot in `pending` and returns `None`. The reservation
    /// future's scheduler waker requests the next poll when capacity is granted.
    fn reserve_claim(
        &self,
        range_len: usize,
        pending: &mut Option<PendingClaim>,
    ) -> Result<Option<BodySlot>, ReserveError> {
        let mut slot = self.inner.writer.claim();
        let mut reservation = self.inner.ctx.handle.buffer_pool.reserve(range_len);
        let waker = self.inner.ctx.scheduler_waker();
        let mut context = Context::from_waker(&waker);

        match Pin::new(&mut reservation).poll(&mut context) {
            Poll::Ready(Ok(reservation)) => {
                slot.attach_reservation(reservation);
                Ok(Some(slot))
            }
            Poll::Ready(Err(error)) => Err(error),
            Poll::Pending => {
                *pending = Some(PendingClaim { slot, reservation });
                Ok(None)
            }
        }
    }

    /// Reserve memory capacity for a discovery chunk.
    ///
    /// Discovery has already issued the GET and runs inside async `execute`, so
    /// it awaits the same cancellation-safe reservation future used by
    /// `poll_work`. The wait races terminal notification because per-transfer
    /// cancellation does not abort an executing future.
    ///
    /// The terminal notification future is registered before checking status:
    /// `notify_waiters` stores no permit, so reversing those operations can lose
    /// a cancellation between the check and polling both futures.
    async fn reserve_chunk(&self, len: usize) -> Result<Option<Reservation>, ReserveError> {
        let mut reservation = std::pin::pin!(self.inner.ctx.handle.buffer_pool.reserve(len));

        loop {
            let terminal_wake = self.inner.discovery_notify.notified();
            let terminal_wake = std::pin::pin!(terminal_wake);
            if !self.inner.ctx.is_active() {
                return Ok(None);
            }
            match select(reservation.as_mut(), terminal_wake).await {
                Either::Left((result, _)) => {
                    tracing::debug!(
                        target: crate::telemetry::TARGET_SCHEDULING,
                        tid = %self.inner.ctx.id,
                        len,
                        "discovery chunk reservation granted; resuming",
                    );
                    return result.map(Some);
                }
                Either::Right(((), _)) => {}
            }
        }
    }

    /// Convert a terminal memory-admission failure into the download lifecycle.
    fn fail_memory_admission(
        &self,
        state: std::sync::MutexGuard<'_, DownloadState>,
        source: ReserveError,
    ) -> PollWork {
        let _ = self.fail(
            state,
            error::Error::new(error::ErrorKind::RuntimeError, source),
        );
        PollWork::Done
    }

    #[tracing::instrument(level = "debug", skip(self, work), fields(tid = %self.id(), work = ?work.data))]
    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::Discovery => self.execute_discovery().await,
            DownloadWork::GetObjectRange { range, slot, etag } => {
                self.execute_get_range(
                    range.clone(),
                    slot.take().expect("slot already consumed"),
                    etag.clone(),
                )
                .await
            }
            DownloadWork::DrainResident => self.execute_drain_resident(),
        }
    }

    /// Flush the resident filled run to disk, returning its pooled carriers,
    /// then release the freed read-ahead occupancy. Emitted when a transfer
    /// would otherwise wait for memory admission while retaining a resident run.
    ///
    /// Unlike a fill-triggered drain, no GET completed here, so `ranges_in_flight` is
    /// untouched; only occupancy is released (matching the gate side of
    /// [`decrement_in_flight`](Self::decrement_in_flight), under the same state lock so
    /// the release is ordered against the issuer's park). Dropping the drained
    /// `ChunkOutput`s returns their carrier charges, which memory admission
    /// re-grants FIFO; the `on_completion -> generate_work` after this returns
    /// re-polls this transfer.
    fn execute_drain_resident(&self) -> WorkOutcome {
        let freed = match self.inner.writer.flush_resident() {
            Ok(f) => f,
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            }
        };
        {
            let mut work = self.inner.state.lock().unwrap();
            if let DownloadState::Transferring { gate, .. } = &mut *work {
                gate.release(freed);
            }
        }
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_discovery(&self) -> WorkOutcome {
        let input = self.inner.request.as_ref();

        // Resolve the effective validation state the same way the SDK's GetObject
        // mutator does: enabled if the request set ChecksumMode=Enabled, or the
        // request left it unset and the client's ResponseChecksumValidation is not
        // WhenRequired (WhenSupported, the default, and unknown values enable it).
        // Computed before discovery because it drives range alignment.
        let validation_enabled = self.validation_enabled(input);

        let discovery = match discover_obj(self, input, validation_enabled).await {
            Ok(d) => d,
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, e);
            }
        };

        let ObjectDiscovery {
            remaining,
            object_meta,
            initial_chunk,
            chunk_meta,
            effective_part_size,
        } = discovery;

        // Store object_meta for object_meta() and join()
        let _ = self.inner.object_meta.set(object_meta.clone());

        // Object checksum is known only when the whole object arrived in this
        // chunk. A larger multipart object's discovery chunk carries part 1's
        // checksum, which is not the object checksum.
        let whole_object_in_chunk = input.range.is_none() && remaining.is_none();
        let integrity = build_integrity_checks(
            chunk_meta.as_ref(),
            whole_object_in_chunk,
            validation_enabled,
        );
        let _ = self.inner.integrity_checks.set(integrity);

        // Notify waiters that discovery completed
        self.inner.discovery_notify.notify_waiters();

        let etag: Option<Arc<str>> = object_meta.e_tag.as_deref().map(Arc::from);

        // Optimization: Preallocate space for the full object/download size if there is a
        // destination/sink and it supports it. e.g. pre-allocate disk space for the full
        // download to avoid per-write metadata updates and late ENOSPC errors.
        let chunk_content_len = chunk_meta
            .as_ref()
            .and_then(|m| m.content_length)
            .and_then(|length| u64::try_from(length).ok())
            .unwrap_or(0);
        let total_size =
            chunk_content_len + remaining.as_ref().map_or(0, |r| r.end() - r.start() + 1);
        self.inner.writer.preallocate(total_size);
        self.inner.ctx.set_total_bytes(total_size);

        // If there's an initial chunk, claim seq BEFORE waking to prevent race
        // where poll_work exhausts the window before we can claim our seq.
        // A GET discovery always supplies metadata. A nonempty GET also
        // supplies one body with its already-validated allocation length.
        //
        // Reserve the discovery chunk's memory here too: it is already resident (the
        // discovery GET fetched it), so it must be accounted before any read-ahead
        // range issues. Awaiting the reservation head-first keeps the memory FIFO
        // ordered: under tight capacity the head part is admitted before the window
        // fans out — and backpressures by holding this chunk undelivered rather than
        // returning `Pending` to the scheduler (discovery runs in async `execute`,
        // not `poll_work`).
        let initial_work = match (initial_chunk, chunk_meta) {
            (Some(initial), Some(meta)) => {
                let mut slot = self.inner.writer.claim();
                match self.reserve_chunk(initial.expected_len).await {
                    Ok(Some(reservation)) => {
                        slot.attach_reservation(reservation);
                        Some((initial.body, initial.expected_len, meta, slot))
                    }
                    // Terminal (cancel/fail by another path) while reserving the discovery
                    // chunk: the transfer is already in its terminal state, so drop the
                    // claimed slot (waking the consumer) and return without transitioning
                    // to Transferring. `execute` reports Cancelled.
                    Ok(None) => {
                        drop(slot);
                        return WorkOutcome::Cancelled;
                    }
                    Err(error) => {
                        drop(slot);
                        let guard = self.inner.state.lock().unwrap();
                        return self.fail(
                            guard,
                            crate::error::Error::new(crate::error::ErrorKind::RuntimeError, error),
                        );
                    }
                }
            }
            (None, _) => None,
            (Some(_), None) => {
                panic!("invalid discovery state: initial_chunk present without chunk_meta")
            }
        };

        {
            // The discovery chunk, if present, is one claimed part already in flight.
            let initial = u64::from(initial_work.is_some());
            let mut work = self.inner.state.lock().unwrap();
            *work = DownloadState::Transferring {
                remaining,
                ranges_in_flight: initial as usize,
                etag: etag.clone(),
                part_size: effective_part_size,
                gate: super::context::OccupancyGate::with_issued(initial),
                pending: None,
            };
        }

        // State changed from DiscoveryInFlight - try to wake
        self.inner.ctx.try_wake();

        // If discovery returned an initial chunk, process it
        match initial_work {
            Some((stream, expected_len, chunk_meta, slot)) => {
                self.execute_read_discovery_body(stream, expected_len, slot, chunk_meta, etag)
                    .await
            }
            None => WorkOutcome::Success { data: None },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
        expected_len: usize,
        slot: BodySlot,
        chunk_meta: ChunkMetadata,
        etag: Option<Arc<str>>,
    ) -> WorkOutcome {
        let seq = slot.seq();
        let input = self.inner.request.as_ref();
        // The discovery GET already received this chunk's response headers; the
        // first read consumes its (lazy, not-yet-read) body stream with no extra
        // request. A retry has no stream to reuse, so it re-issues a ranged GET
        // for exactly the bytes this chunk covers, taken from the discovery
        // response's content-range. A partNumber=1 discovery maps to the same
        // byte range, so the re-issued aligned range returns the same per-part
        // checksum the SDK validates against.
        let reissue_range = chunk_meta
            .content_range
            .as_deref()
            .and_then(crate::http::header::parse_content_range);

        let mut initial = Some(stream);
        let recv_latencies = &self.inner.ctx.handle.telemetry.recv_latencies;
        // The deadline guards only the GET response (TTFB); the body-read is
        // untimed. The first attempt reuses the discovery body — no network send
        // — so it skips the `guarded` timer entirely (timing/recording a ~0µs
        // "send" would drag the TTFB mean toward zero). Only a genuine re-issue
        // goes through `guarded`, which times and records its TTFB.
        let reservation = slot.reservation();
        let result = crate::retry::retry(crate::retry::classify_body_retry, |allow_hedge| {
            let pre_issued = initial.take();
            let etag = etag.clone();
            let ctx = self.inner.ctx.clone();
            let reissue_range = reissue_range.clone();
            let mut builder =
                copy_fields_to_get_object_request(input, ctx.s3_client().get_object());
            if let Some(r) = reissue_range.as_ref() {
                builder = builder.set_range(Some(format!("bytes={}-{}", r.start(), r.end())));
            }
            if let Some(etag) = etag.as_ref() {
                builder = builder.if_match(etag.as_ref());
            }
            let req = builder
                .customize()
                .config_override(ctx.handle.download_get_override(input.bucket()));
            async move {
                // Obtain the body stream. First attempt reuses the discovery body
                // (no send, untimed); a re-issue is TTFB-guarded (times + records).
                let body = match pre_issued {
                    Some(s) => s,
                    None => {
                        // A re-issue without a byte range would fetch the whole
                        // object into this chunk's slot. S3 always returns a
                        // content-range for a ranged/partNumber GET, so a missing
                        // range is a contract violation — fail the chunk rather
                        // than issue an unbounded GET. Use a terminal kind
                        // (`RuntimeError`, NoRetry in `classify_body_retry`): the
                        // range cannot appear on a re-issue, so retrying would
                        // deterministically fail again and waste a backoff.
                        if reissue_range.is_none() {
                            return Err(crate::retry::GuardError::Inner(crate::error::Error::new(
                                crate::error::ErrorKind::RuntimeError,
                                "cannot re-issue discovery chunk: response carried no content-range",
                            )));
                        }
                        recv_latencies
                            .guarded(allow_hedge, async {
                                req.send()
                                    .await
                                    .map(|resp| resp.body)
                                    .map_err(crate::error::Error::from)
                            })
                            .await?
                    }
                };
                // Untimed: drain the body. Errors here are inner (retryable IO).
                collect_response_body(
                    &ctx.handle.buffer_pool,
                    body,
                    reservation,
                    expected_len,
                    || ctx.is_active(),
                )
                    .await
                    .map_err(crate::retry::GuardError::Inner)
            }
        })
        .instrument(tracing::debug_span!(
            target: crate::telemetry::TARGET_TRANSFER,
            "download-part",
            tid = %self.id()
        ))
        .await;

        let bytes = match result {
            Ok(val) => val,
            // Go terminal before any wake: fail() sets Terminal under the lock, so
            // a woken poll_work cannot observe ranges_in_flight==0 and complete()
            // the transfer over this error. The in-flight count is abandoned with
            // the Transferring state.
            Err(e) => {
                let guard = self.inner.state.lock().unwrap();
                return self.fail(guard, e.with_chunk(ChunkRef::new(seq, None)));
            }
        };

        let chunk = ChunkOutput {
            seq,
            offset: chunk_meta
                .content_range
                .as_deref()
                .and_then(crate::http::header::parse_content_range)
                .map(|r| *r.start())
                .unwrap_or(0),
            data: bytes,
            metadata: chunk_meta,
        };

        // Edge-triggered disk write: a fill that brings the segment's filled count to
        // the drain batch attempts a drain of the batch-ready run(s). `freed` is the
        // occupancy this drain released, accounted under the lock in decrement_in_flight.
        let mut freed = 0u64;
        if slot.fill(chunk) == FillOutcome::DrainReady {
            match self.inner.writer.drain(DrainMode::Batched) {
                Ok(f) => freed = f,
                Err(e) => {
                    // Go terminal before any wake (see fail_range).
                    let guard = self.inner.state.lock().unwrap();
                    return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
                }
            }
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        let bytes_received =
            u64::try_from(expected_len).expect("validated discovery length originated from u64");
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        let reached_terminal = self.decrement_in_flight(freed);
        if reached_terminal {
            return self.finalize_completion();
        }

        WorkOutcome::Success { data: None }
    }

    async fn execute_get_range(
        &self,
        range: std::ops::RangeInclusive<u64>,
        slot: BodySlot,
        etag: Option<Arc<str>>,
    ) -> WorkOutcome {
        let seq = slot.seq();
        let input = self.inner.request.as_ref();
        let range_header = format!("bytes={}-{}", range.start(), range.end());
        let expected_len = match range
            .end()
            .checked_sub(*range.start())
            .and_then(|len| len.checked_add(1))
            .and_then(|len| usize::try_from(len).ok())
        {
            Some(len) if len != 0 => len,
            _ => {
                return self.fail_range(
                    ChunkRef::new(seq, Some(*range.start()..=*range.end())),
                    error::Error::new(
                        error::ErrorKind::RuntimeError,
                        "download range length exceeds platform representation",
                    ),
                )
            }
        };
        let reservation = slot.reservation();

        let recv_latencies = &self.inner.ctx.handle.telemetry.recv_latencies;
        // The deadline guards only the GET response (send → headers ≈ TTFB): the
        // `recv_latencies.guarded(send)` wrapper times the send, then the body is
        // read UNTIMED. A large part on a slow-but-healthy link takes a long time
        // to drain and must not be cancelled as a straggler; a dead mid-body
        // stream is caught by stalled-stream protection. A `GuardError`
        // (deadline timeout OR inner error from either phase) is classified by
        // the retry loop.
        let result = crate::retry::retry(crate::retry::classify_body_retry, |allow_hedge| {
            let rh = range_header.clone();
            let etag = etag.clone();
            let ctx = self.inner.ctx.clone();
            // Every chunk GET must carry the same request fields as discovery
            // (checksum_mode, SSE-C key, version_id, ...). Derive from the input
            // conversion, then pin this chunk's range and the discovered etag.
            let mut builder =
                copy_fields_to_get_object_request(input, ctx.s3_client().get_object());
            builder = builder.set_range(Some(rh.clone()));
            if let Some(etag) = etag.as_ref() {
                builder = builder.if_match(etag.as_ref());
            }
            let req = builder
                .customize()
                .config_override(ctx.handle.download_get_override(input.bucket()));
            async move {
                // Timed (TTFB): obtain response headers. Validate the range here
                // so a mismatch is classified before we commit to the body read.
                let rh_validate = rh.clone();
                let resp = recv_latencies
                    .guarded(allow_hedge, async move {
                        let resp = req.send().await.map_err(crate::error::Error::from)?;
                        validate_content_range(&rh_validate, resp.content_range())?;
                        Ok::<_, crate::error::Error>(resp)
                    })
                    .await?;
                // Untimed: drain the body. Errors here are inner (retryable IO).
                let chunk_meta = ChunkMetadata::from(&resp);
                let bytes = collect_response_body(
                    &ctx.handle.buffer_pool,
                    resp.body,
                    reservation,
                    expected_len,
                    || ctx.is_active(),
                )
                .await
                .map_err(crate::retry::GuardError::Inner)?;
                Ok::<_, crate::retry::GuardError<crate::error::Error>>((chunk_meta, bytes))
            }
        })
        .instrument(tracing::debug_span!(
            target: crate::telemetry::TARGET_TRANSFER,
            "download-part-reissue",
            tid = %self.id()
        ))
        .await;

        let (chunk_meta, bytes) = match result {
            Ok(val) => val,
            Err(e) => {
                return self.fail_range(ChunkRef::new(seq, Some(*range.start()..=*range.end())), e)
            }
        };

        bail_if_terminal!(self);
        let chunk = ChunkOutput {
            seq,
            offset: *range.start(),
            data: bytes,
            metadata: chunk_meta,
        };

        // Edge-triggered disk write: a fill that brings the segment's filled count to
        // the drain batch attempts a drain of the batch-ready run(s) — one positioned
        // write per coalesced run, not every fill. Stream mode's drain is a no-op.
        // `freed` is the occupancy this drain released, accounted under the lock in
        // decrement_in_flight.
        let mut freed = 0u64;
        if slot.fill(chunk) == FillOutcome::DrainReady {
            match self.inner.writer.drain(DrainMode::Batched) {
                Ok(f) => freed = f,
                Err(e) => {
                    // Go terminal before any wake (see fail_range).
                    let guard = self.inner.state.lock().unwrap();
                    return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
                }
            }
        }

        // disk_write reflects bytes committed to the file sink buffer.
        // Actual disk flushes are batched; disk_write may lead physical
        // I/O at any snapshot but converges on transfer completion.
        let bytes_received =
            u64::try_from(expected_len).expect("validated range length originated from u64");
        self.inner.ctx.record_io(&crate::metrics::IoSample {
            network_rx: bytes_received,
            disk_write: if self.inner.writer.has_sink() {
                bytes_received
            } else {
                0
            },
            ..Default::default()
        });

        let reached_terminal = self.decrement_in_flight(freed);
        if reached_terminal {
            return self.finalize_completion();
        }

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            seq,
            offset = *range.start(),
            "chunk downloaded",
        );

        WorkOutcome::Success { data: None }
    }

    /// Fail a range request with an error, tagging the chunk it belongs to.
    fn fail_range(&self, location: ChunkRef, e: Error) -> WorkOutcome {
        // Go terminal before any wake (see the discovery-body error path). Do not
        // decrement_in_flight first: that wakes poll_work, which would observe
        // ranges_in_flight==0 and complete() over this error.
        let guard = self.inner.state.lock().unwrap();
        self.fail(guard, e.with_chunk(location))
    }

    /// Complete one in-flight range: drop `ranges_in_flight`, release `freed` parts of
    /// read-ahead occupancy, and report whether this was the terminal completion (issuance
    /// done and nothing left in flight). Both counters move under the state lock so the
    /// terminal transition is claimed exactly once: the caller that observes `true` owns
    /// completion, and any concurrently-woken `poll_work` sees `Terminal`.
    ///
    /// Releasing the occupancy under the same lock `poll_work` reads the gate and arms
    /// `set_pending` under is what orders this completion's release against the issuer's
    /// park (the mutator protocol `lock -> mutate -> unlock -> try_wake`). `freed` is the
    /// disk drain's freed count (0 if this fill did not hit a drain edge).
    fn decrement_in_flight(&self, freed: u64) -> bool {
        let (terminal, pending) = {
            let mut work = self.inner.state.lock().unwrap();
            match &mut *work {
                DownloadState::Transferring {
                    ranges_in_flight,
                    gate,
                    remaining,
                    ..
                } => {
                    *ranges_in_flight = ranges_in_flight.saturating_sub(1);
                    gate.release(freed);
                    if remaining.is_none() && *ranges_in_flight == 0 {
                        // Terminal: claim the transition under this lock so a
                        // concurrently-woken poll_work cannot also complete.
                        (true, work.enter_terminal())
                    } else {
                        (false, None)
                    }
                }
                _ => (false, None),
            }
        };
        drop(pending);
        // Wake the issuer on every non-terminal completion so a pending poll_work can
        // issue more. A terminal completion is finalized by the caller in `execute`.
        if !terminal {
            self.inner.ctx.try_wake();
        }
        terminal
    }

    /// Finalize a transfer that reached its terminal completion in `execute`: flush the
    /// tail to disk (returning the last carrier charges as `ChunkOutput`s drop), set the
    /// terminal status, and signal waiters. The state is already `Terminal` (claimed under
    /// the lock in `decrement_in_flight`); this runs after the lock is released so disk IO
    /// never happens under it. Symmetric with `fail`, which error paths already call from
    /// `execute`.
    fn finalize_completion(&self) -> WorkOutcome {
        if let Err(e) = self.inner.writer.finalize() {
            // Finalize failed: transition to failed. The state is already Terminal; `fail`
            // calls `enter_terminal` which is idempotent on Terminal (returns None).
            let guard = self.inner.state.lock().unwrap();
            return self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
        }
        self.inner.ctx.set_completed();
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
        WorkOutcome::Success { data: None }
    }

    /// Transition to terminal failed state. Requires holding the work lock.
    fn fail(
        &self,
        mut guard: std::sync::MutexGuard<'_, DownloadState>,
        error: Error,
    ) -> WorkOutcome {
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            "download failed",
        );
        let classification = crate::scheduler::classify_error(&error);
        // Order matters: set status/error before any wakeups
        self.inner.ctx.set_failed(error);
        // Transition to Terminal, taking any memory-blocked claim so cancelling
        // its reservation future happens after releasing the state lock.
        let pending = guard.enter_terminal();
        drop(guard); // release lock before dropping the claim and signaling waiters
        drop(pending);
        // Wake all waiters
        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
        WorkOutcome::Failed { classification }
    }

    /// Transition to terminal success state. Requires holding the work lock.
    fn complete(&self, mut guard: std::sync::MutexGuard<'_, DownloadState>) {
        if let Err(e) = self.inner.writer.finalize() {
            self.fail(guard, error::Error::new(error::ErrorKind::IOError, e));
            return;
        }
        self.inner.ctx.set_completed();
        let pending = guard.enter_terminal();
        drop(guard); // release lock before dropping the claim and signaling waiters
        drop(pending);
        self.inner.writer.notify_consumer();
        self.inner.ctx.signal_terminal();
    }
}

impl Transfer for DownloadTransfer {
    fn ctx(&self) -> &TransferContext {
        DownloadTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        DownloadTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(DownloadTransfer::execute(self, work))
    }

    fn on_terminal(&self) {
        // Release a memory-blocked claim if one is held. External cancellation
        // does not run `fail` or `complete`, so it must explicitly extract the
        // claim and cancel its reservation future after releasing state.
        let pending = {
            let mut state = self.inner.state.lock().unwrap();
            state.enter_terminal()
        };
        drop(pending);

        self.inner.discovery_notify.notify_waiters();
        let _ = self.inner.writer.finalize();
        self.inner.writer.notify_consumer();
    }
}

/// Parse a byte range from either the request format (`bytes=START-END`) or the
/// response format (`bytes START-END/TOTAL` or `bytes START-END/*`).
///
/// Strips a leading `bytes=` or `bytes ` prefix, discards a `/TOTAL` suffix if
/// present, splits on `-`, and parses both halves as `u64`. Returns `Some((start, end))`
/// only when both parse and `start <= end`; returns `None` for any malformed input.
fn parse_byte_range(s: &str) -> Option<(u64, u64)> {
    let s = s
        .strip_prefix("bytes=")
        .or_else(|| s.strip_prefix("bytes "))
        .unwrap_or(s);
    // Strip "/TOTAL" or "/*" suffix (Content-Range format).
    let range_part = s.split('/').next()?;
    let (start_str, end_str) = range_part.split_once('-')?;
    let start: u64 = start_str.trim().parse().ok()?;
    let end: u64 = end_str.trim().parse().ok()?;
    (start <= end).then_some((start, end))
}

/// Validate that the response Content-Range matches the requested range.
///
/// Both the requested range and the response are parsed to numeric `(start, end)` and
/// compared for equality. Returns `Err` when either side is absent or unparseable, or
/// when the parsed ranges differ.
fn validate_content_range(
    requested_range: &str,
    response_content_range: Option<&str>,
) -> Result<(), Error> {
    let requested = parse_byte_range(requested_range);
    let response = response_content_range.and_then(parse_byte_range);

    match (requested, response) {
        (Some(req), Some(resp)) if req == resp => Ok(()),
        _ => Err(error::Error::new(
            error::ErrorKind::RuntimeError,
            format!(
                "content range mismatch: requested {}, response {:?}",
                requested_range, response_content_range
            ),
        )),
    }
}

/// Build the object-integrity result for a completed download.
///
/// `whole_object_in_chunk` must be true only when the entire object arrived in
/// the discovery chunk, the one case where the chunk checksum is the object
/// checksum. Otherwise value members are left `None`.
///
/// Validation is reported `Disabled` when not requested, else
/// `NotValidated{Unavailable}`. `Validated{algorithm}` requires a per-response
/// validation outcome the Rust SDK does not currently expose.
// TODO(vnext): resolve `Validated{algorithm}` from an SDK-reported per-response
// validation outcome once the SDK exposes one, instead of always reporting
// NotValidated when validation is enabled.
fn build_integrity_checks(
    chunk_meta: Option<&ChunkMetadata>,
    whole_object_in_chunk: bool,
    validation_enabled: bool,
) -> crate::types::IntegrityChecks {
    use crate::types::{ChecksumValidation, NotValidatedReason};

    let checksum_validation = if validation_enabled {
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Unavailable,
        }
    } else {
        ChecksumValidation::NotValidated {
            reason: NotValidatedReason::Disabled,
        }
    };

    // Surface checksum values only when they describe the whole object.
    let cm = chunk_meta.filter(|_| whole_object_in_chunk);
    crate::types::IntegrityChecks::new(
        cm.and_then(|m| m.checksum_crc32.clone()),
        cm.and_then(|m| m.checksum_crc32_c.clone()),
        cm.and_then(|m| m.checksum_crc64_nvme.clone()),
        cm.and_then(|m| m.checksum_sha1.clone()),
        cm.and_then(|m| m.checksum_sha256.clone()),
        cm.and_then(|m| m.checksum_type.clone()),
        checksum_validation,
    )
}

#[cfg(all(test, not(s3_tm_loom)))]
mod response_collection_tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    use bytes::{Buf, Bytes};
    use http_body_1x::{Body as HttpBody, Frame, SizeHint};

    use super::*;

    /// Deterministic multi-frame body separating transport frames from carriers.
    struct TestFrameBody {
        frames: std::collections::VecDeque<Bytes>,
        remaining: usize,
    }

    impl TestFrameBody {
        fn new(frames: impl IntoIterator<Item = Bytes>) -> Self {
            let frames: std::collections::VecDeque<_> = frames.into_iter().collect();
            let remaining = frames.iter().map(Bytes::len).sum();
            Self { frames, remaining }
        }

        fn into_byte_stream(self) -> aws_sdk_s3::primitives::ByteStream {
            aws_sdk_s3::primitives::ByteStream::from_body_1_x(self)
        }
    }

    impl HttpBody for TestFrameBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let Some(frame) = self.frames.pop_front() else {
                return Poll::Ready(None);
            };
            self.remaining = self
                .remaining
                .checked_sub(frame.len())
                .expect("test frame accounting underflow");
            Poll::Ready(Some(Ok(Frame::data(frame))))
        }

        fn is_end_stream(&self) -> bool {
            self.frames.is_empty()
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(self.remaining as u64)
        }
    }

    struct CountedFrame {
        bytes: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for CountedFrame {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for CountedFrame {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn test_pool() -> BufferPool {
        crate::memory::BufferPool::builder()
            .memory_budget(crate::types::MemoryBudgetConfig::Limit(128 * 1024))
            .build()
            .expect("response collector test pool")
    }

    fn collect_ready(
        pool: &BufferPool,
        body: aws_sdk_s3::primitives::ByteStream,
        reservation: &Reservation,
        expected_len: usize,
    ) -> Result<SegmentedBytes, Error> {
        collect_ready_with(pool, body, reservation, expected_len, || true)
    }

    fn collect_ready_with(
        pool: &BufferPool,
        body: aws_sdk_s3::primitives::ByteStream,
        reservation: &Reservation,
        expected_len: usize,
        is_active: impl Fn() -> bool,
    ) -> Result<SegmentedBytes, Error> {
        let mut future = std::pin::pin!(collect_response_body(
            pool,
            body,
            reservation,
            expected_len,
            is_active,
        ));
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("in-memory test body unexpectedly yielded Pending"),
        }
    }

    /// Three frames require more than half a carrier each. Reusing writable
    /// tails fits them in a two-carrier reservation.
    #[test]
    fn test_collection_reuses_tail_and_releases_foreign_frames() {
        const FRAME_LEN: usize = 9_000;
        const EXPECTED_LEN: usize = FRAME_LEN * 3;

        let pool = test_pool();
        let reservation = pool
            .try_reserve(EXPECTED_LEN)
            .unwrap()
            .expect("collector reservation");
        let expected: Vec<u8> = (0..EXPECTED_LEN)
            .map(|index| (index.wrapping_mul(31) % 251) as u8)
            .collect();
        let drops = Arc::new(AtomicUsize::new(0));
        let first = Bytes::from_owner(CountedFrame {
            bytes: expected[..FRAME_LEN].to_vec(),
            drops: Arc::clone(&drops),
        });
        let body = TestFrameBody::new([
            Bytes::new(),
            first,
            Bytes::copy_from_slice(&expected[FRAME_LEN..FRAME_LEN * 2]),
            Bytes::copy_from_slice(&expected[FRAME_LEN * 2..]),
        ])
        .into_byte_stream();

        let mut output = collect_ready(&pool, body, &reservation, EXPECTED_LEN)
            .expect("frames fit one reserved stream");

        assert_eq!(output.len(), EXPECTED_LEN);
        assert_eq!(
            drops.load(Ordering::Relaxed),
            1,
            "transport owner must drop after its frame is copied"
        );
        assert_ne!(pool.metrics().charged_capacity_bytes(), 0);
        assert_eq!(output.copy_to_bytes(EXPECTED_LEN).as_ref(), expected);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_short_attempt_restores_reservation_authority_before_retry() {
        const EXPECTED_LEN: usize = 20_000;

        let pool = test_pool();
        let reservation = pool
            .try_reserve(EXPECTED_LEN)
            .unwrap()
            .expect("collector reservation");
        let short = TestFrameBody::new([
            Bytes::from(vec![0x11; 9_000]),
            Bytes::from(vec![0x22; 9_000]),
        ])
        .into_byte_stream();

        let error = collect_ready(&pool, short, &reservation, EXPECTED_LEN)
            .expect_err("short attempt unexpectedly succeeded");
        assert_eq!(error.kind(), &error::ErrorKind::IOError);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);

        let expected: Vec<u8> = (0..EXPECTED_LEN)
            .map(|index| (index.wrapping_mul(17) % 251) as u8)
            .collect();
        let retry = TestFrameBody::new([
            Bytes::copy_from_slice(&expected[..7_000]),
            Bytes::copy_from_slice(&expected[7_000..]),
        ])
        .into_byte_stream();
        let output = collect_ready(&pool, retry, &reservation, EXPECTED_LEN)
            .expect("retry reacquires returned authority");

        assert_eq!(
            output.clone().copy_to_bytes(EXPECTED_LEN).as_ref(),
            expected
        );
        reservation.close_acquisition();
        assert_ne!(
            pool.metrics().charged_capacity_bytes(),
            0,
            "immutable output retains pooled owners after reservation close"
        );
        drop(output);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_collection_rejects_same_carrier_length_overrun() {
        const EXPECTED_LEN: usize = 8;

        let pool = test_pool();
        let reservation = pool
            .try_reserve(EXPECTED_LEN)
            .unwrap()
            .expect("collector reservation");
        let body = TestFrameBody::new([Bytes::from_static(b"123456789")]).into_byte_stream();

        let error = collect_ready(&pool, body, &reservation, EXPECTED_LEN)
            .expect_err("oversized attempt unexpectedly succeeded");

        assert_eq!(error.kind(), &error::ErrorKind::IOError);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_collection_observes_cancellation_after_an_empty_frame() {
        let pool = test_pool();
        let reservation = pool.try_reserve(4).unwrap().expect("collector reservation");
        let body =
            TestFrameBody::new([Bytes::new(), Bytes::from_static(b"data")]).into_byte_stream();

        let error = collect_ready_with(&pool, body, &reservation, 4, || false)
            .expect_err("cancelled attempt unexpectedly succeeded");

        assert_eq!(error.kind(), &error::ErrorKind::OperationCancelled);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }

    #[test]
    fn test_collection_rejects_reservation_envelope_exhaustion() {
        const RESPONSE_LEN: usize = 128 * 1024;

        let pool = test_pool();
        let reservation = pool.try_reserve(1).unwrap().expect("one-carrier envelope");
        let body = TestFrameBody::new([Bytes::from(vec![0; RESPONSE_LEN])]).into_byte_stream();

        let error = collect_ready(&pool, body, &reservation, RESPONSE_LEN)
            .expect_err("acquisition escaped its reservation envelope");

        assert_eq!(error.kind(), &error::ErrorKind::RuntimeError);
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
        drop(
            pool.acquire(&reservation, 1)
                .expect("failed acquisition did not consume direct authority"),
        );
        assert_eq!(pool.metrics().charged_capacity_bytes(), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::download::DownloadInput;
    use crate::scheduler::test_util::{assert_done, assert_pending, assert_ready};
    use crate::transfer::TransferContext;
    use crate::transfer::{IoRequest, WorkOutcome};
    use crate::types::BucketType;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    const MB: u64 = 1024 * 1024;

    use crate::operation::download::chunk_meta::ChunkMetadata;
    use crate::types::{ChecksumValidation, NotValidatedReason};
    use aws_sdk_s3::types::ChecksumType;

    fn chunk_with_crc32(value: &str) -> ChunkMetadata {
        let mut m = ChunkMetadata::default();
        m.checksum_crc32 = Some(value.to_string());
        m.checksum_type = Some(ChecksumType::FullObject);
        m
    }

    #[test]
    fn integrity_disabled_when_validation_not_enabled() {
        // validation_enabled=false models a WhenRequired client with no request
        // override: no validation is attempted, so the verdict is Disabled.
        let ic = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, false);
        assert_eq!(
            *ic.checksum_validation(),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Disabled
            }
        );
    }

    #[test]
    fn integrity_unavailable_when_enabled_pending_sdk_signal() {
        // TODO(vnext): becomes Validated once the SDK reports a validation outcome.
        let ic = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, true);
        assert_eq!(
            *ic.checksum_validation(),
            ChecksumValidation::NotValidated {
                reason: NotValidatedReason::Unavailable
            }
        );
    }

    #[test]
    fn integrity_surfaces_value_only_for_whole_object_chunk() {
        // Whole object in the discovery chunk -> the chunk checksum IS the object's.
        let whole = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), true, false);
        assert_eq!(whole.checksum_crc32(), Some("DUoRhQ=="));
        assert_eq!(whole.checksum_type(), Some(&ChecksumType::FullObject));

        // Multipart (object did not fit in the discovery chunk) -> part-1 value is
        // NOT surfaced as the object value.
        let multipart = build_integrity_checks(Some(&chunk_with_crc32("DUoRhQ==")), false, false);
        assert_eq!(multipart.checksum_crc32(), None);
        assert_eq!(multipart.checksum_type(), None);
    }

    fn create_download(object_size: u64, part_size: u64) -> DownloadTransfer {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        create_download_with_client(
            mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]),
            part_size,
        )
    }

    fn create_download_with_client(client: aws_sdk_s3::Client, part_size: u64) -> DownloadTransfer {
        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_recv_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);

        DownloadTransfer::new(ctx, BucketType::Standard, input, writer)
    }

    /// Execute work using DownloadTransfer directly.
    async fn execute(transfer: &DownloadTransfer, work: &mut IoRequest) -> WorkOutcome {
        transfer.execute(work).await
    }

    /// Run discovery to completion
    async fn skip_discovery(transfer: &DownloadTransfer) {
        let mut work = assert_ready(transfer.poll_work());
        execute(transfer, &mut work).await;
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_initial_poll_returns_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::Discovery));
    }

    // FIXME: crossbeam-epoch is incompatible with miri (https://github.com/crossbeam-rs/crossbeam/issues/1181)
    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_pending_while_discovery_in_flight() {
        let transfer = create_download(24 * MB, 8 * MB);
        let _work = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_generates_ranges_after_discovery() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        assert!(matches!(data, DownloadWork::GetObjectRange { .. }));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_one_with_initial_chunk() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    1,
                    "seq should start at 1 when initial chunk claims seq=0"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_starts_at_zero_without_initial_chunk() {
        let head_obj = mock!(aws_sdk_s3::Client::head_object).then_output(|| {
            aws_sdk_s3::operation::head_object::HeadObjectOutput::builder()
                .content_length(24 * MB as i64)
                .e_tag("test-etag")
                .build()
        });
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
                .content_length(8 * MB as i64)
                .content_range(format!("bytes 0-{}/{}", 8 * MB - 1, 24 * MB))
                .e_tag("test-etag")
                .body(ByteStream::from(vec![0u8; 8 * MB as usize]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[head_obj, get_obj]);

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(8 * MB))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .range("bytes=0-")
            .build()
            .unwrap();

        let (writer, _consumer) = crate::operation::download::body::new_recv_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);

        skip_discovery(&transfer).await;

        let mut work = assert_ready(transfer.poll_work());
        let data = work.data_mut::<DownloadWork>();
        match data {
            DownloadWork::GetObjectRange { slot, .. } => {
                assert_eq!(
                    slot.as_ref().unwrap().seq(),
                    0,
                    "seq should start at 0 when no initial chunk"
                );
            }
            _ => panic!("expected GetObjectRange"),
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_seq_increments_sequentially() {
        let transfer = create_download(32 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut seqs = Vec::new();
        while let PollWork::Ready { io: mut w, .. } = transfer.poll_work() {
            let data = w.data_mut::<DownloadWork>();
            if let DownloadWork::GetObjectRange { slot, .. } = data {
                seqs.push(slot.as_ref().unwrap().seq());
            }
        }

        assert_eq!(seqs.len(), 3, "expected multiple ranges");
        for i in 1..seqs.len() {
            assert_eq!(seqs[i], seqs[i - 1] + 1, "seqs should be sequential");
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_pending_when_range_in_flight() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        // generate range work but don't complete
        let _range = assert_ready(transfer.poll_work());
        // transfer is considered active and shouldn't transition to done until all in-flight work is complete and handle is joined/dropped
        assert_pending(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_done_when_all_complete() {
        let transfer = create_download(12 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        execute(&transfer, &mut range).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_out_of_order_completion() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        let mut range1 = assert_ready(transfer.poll_work()); // seq=1
        let mut range2 = assert_ready(transfer.poll_work()); // seq=2

        // Complete in reverse order
        execute(&transfer, &mut range2).await;
        execute(&transfer, &mut range1).await;

        assert_done(transfer.poll_work());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_failure_transitions_to_failed() {
        let _logs = show_test_logs();
        // Fail seq 1 (first range after discovery)
        let transfer = FailureConfig::new(24 * MB, 8 * MB).fail(1).build();

        skip_discovery(&transfer).await;

        let mut range = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut range).await;

        assert!(matches!(outcome, WorkOutcome::Failed { .. }));
        assert!(transfer.ctx().is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation_transitions_to_cancelled() {
        let transfer = create_download(24 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        transfer.ctx().set_cancelled();
        transfer.ctx().signal_terminal();

        assert!(transfer.ctx().is_cancelled());
        assert_done(transfer.poll_work());
    }

    fn create_download_for_gate(
        object_size: u64,
        part_size: u64,
    ) -> (
        DownloadTransfer,
        crate::operation::download::body::RecvBodyConsumer,
    ) {
        create_download_for_gate_with_capacity(object_size, part_size, None)
    }

    fn create_download_for_gate_with_capacity(
        object_size: u64,
        part_size: u64,
        capacity_bytes: Option<usize>,
    ) -> (
        DownloadTransfer,
        crate::operation::download::body::RecvBodyConsumer,
    ) {
        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]);

        let mut config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size));
        if let Some(capacity_bytes) = capacity_bytes {
            let pool = crate::memory::BufferPool::builder()
                .memory_budget(crate::types::MemoryBudgetConfig::Limit(capacity_bytes))
                .build()
                .expect("test pool");
            config = config.memory(crate::types::MemoryConfig::Explicit(pool));
        }
        let config = config.build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        let (writer, consumer) = crate::operation::download::body::new_recv_body();
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);
        (transfer, consumer)
    }

    /// The read-ahead gate bounds issuance at the current window: with a small window,
    /// issuance proceeds up to `issued − released == window()`, then stalls. Tests the
    /// gate wiring; `read_ahead`'s own tests cover resolving the window value.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_read_ahead_gate_bounds_issuance() {
        let (transfer, _consumer) = create_download_for_gate(128 * MB, 8 * MB);
        skip_discovery(&transfer).await;
        // Force a small window so a few polls exercise the gate; the default is large.
        transfer.read_ahead().force_window(3);
        let w = transfer.read_ahead().window();
        // Discovery claimed seq 0 (issued=1, consumed=0). Drive until the gate stalls.
        let mut issued = 1u64;
        loop {
            match transfer.poll_work() {
                PollWork::Ready { .. } => {
                    issued += 1;
                    assert!(issued <= w, "issuance ran past the window");
                }
                PollWork::Spawned => {}
                PollWork::Pending => break,
                PollWork::Done => panic!("unexpected Done"),
            }
        }
        assert_eq!(issued, w, "issuance should fill the window then stall");
    }

    /// On the stream path, delivering a chunk frees one part; the download layer
    /// releases that occupancy under the state lock (as `Body::next` does via
    /// `release_stream_occupancy`), so `issued − released` drops below the window and
    /// the gate admits another claim.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_consume_reopens_gate() {
        let (transfer, mut consumer) = create_download_for_gate(128 * MB, 8 * MB);
        skip_discovery(&transfer).await;
        transfer.read_ahead().force_window(3);
        // Fill the window.
        while let PollWork::Ready { .. } = transfer.poll_work() {}
        assert_pending(transfer.poll_work());

        // Consume seq 0 (filled by discovery) — the buffer delivers the chunk, then the
        // download layer releases one part of occupancy under the state lock. This is
        // exactly what `Body::next` does; the test drives the two steps directly.
        assert!(
            consumer.try_take_next().is_some(),
            "discovery chunk should deliver"
        );
        transfer.release_stream_occupancy();

        // Gate reopens for exactly one more claim.
        assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
    }

    /// Shared memory admission is the second issuance gate. With the read-ahead
    /// window open, a transfer still returns `Pending` when its memory budget is
    /// exhausted. It retains the claimed slot and resumes when a delivered chunk
    /// returns its carrier charge and capacity is granted to the FIFO head.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_admission_blocks_then_resumes_in_fifo_order() {
        let part_size = 8 * MB;
        let object_size = 3 * part_size;
        let capacity_bytes = 2 * part_size as usize;
        let (transfer, mut consumer) =
            create_download_for_gate_with_capacity(object_size, part_size, Some(capacity_bytes));

        skip_discovery(&transfer).await;
        let pool = transfer.ctx().handle.buffer_pool.clone();
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            0,
            "the completed discovery response has closed acquisition authority"
        );
        assert_eq!(
            pool.metrics().admission_used_bytes(),
            part_size,
            "the delivered discovery payload remains charged"
        );

        // The discovery chunk plus one range exactly fill configured capacity.
        let _w1 = assert_ready(transfer.poll_work());
        assert_eq!(pool.metrics().active_planned_demand_bytes(), part_size);
        assert_eq!(pool.metrics().admission_used_bytes(), 2 * part_size);

        // Part 2 cannot fit and waits in the memory-admission FIFO, holding
        // the claimed slot in `pending` until a chunk frees.
        assert_pending(transfer.poll_work());
        assert_eq!(pool.metrics().queued_reservations(), 1);
        {
            let state = transfer.inner.state.lock().unwrap();
            match &*state {
                DownloadState::Transferring { pending, .. } => {
                    assert!(pending.is_some(), "claim should be waiting for memory");
                }
                _ => panic!("expected Transferring"),
            }
        }

        // Dropping the discovery chunk returns its carrier. Memory admission
        // grants that capacity directly to the FIFO head.
        drop(consumer.try_take_next().expect("discovery chunk is filled"));
        assert_eq!(
            pool.metrics().admission_used_bytes(),
            2 * part_size,
            "freed capacity re-granted to the memory waiter"
        );
        assert_eq!(pool.metrics().queued_reservations(), 0);

        // Poll #3: the waiting claim was granted, so it is ready and no longer pending.
        let _w2 = assert_ready(transfer.poll_work());
        {
            let state = transfer.inner.state.lock().unwrap();
            match &*state {
                DownloadState::Transferring { pending, .. } => {
                    assert!(pending.is_none(), "waiting claim should be consumed");
                }
                _ => panic!("expected Transferring"),
            }
        }
        assert_eq!(pool.metrics().active_planned_demand_bytes(), 2 * part_size);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_memory_reservation_failure_terminates_without_dispatch() {
        let (transfer, _consumer) = create_download_for_gate(3 * 8 * MB, 8 * MB);
        skip_discovery(&transfer).await;
        transfer
            .ctx()
            .handle
            .buffer_pool
            .inject_reservation_failure(ReserveError::MetadataAllocationFailed);

        assert_done(transfer.poll_work());
        assert!(transfer.ctx().is_failed());
        assert_eq!(
            transfer.ctx().error_kind(),
            Some(crate::error::ErrorKind::RuntimeError),
        );
        assert!(matches!(
            &*transfer.inner.state.lock().unwrap(),
            DownloadState::Terminal
        ));
    }

    /// A stream-mode transfer blocked on memory must return `Pending`, never a
    /// `DrainResident` work item. The consumer drives stream release, so the
    /// deadlock relief does not apply and must not spin emitting empty drains.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_stream_memory_wait_does_not_emit_drain_resident() {
        // Stream-mode transfer (create_download_for_gate uses new_recv_body). 3 parts.
        let part_size = 8 * MB;
        let object_size = 3 * part_size;
        let (transfer, _consumer) = create_download_for_gate_with_capacity(
            object_size,
            part_size,
            Some(2 * part_size as usize),
        );

        skip_discovery(&transfer).await;

        // The completed discovery payload remains charged while part 1 retains
        // open acquisition authority. Part 2 therefore waits normally rather
        // than using idle-only admission.
        let _part_1 = assert_ready(transfer.poll_work());

        // Because this is stream mode, the relief path is off: poll_work
        // returns Pending, not Ready(DrainResident).
        match transfer.poll_work() {
            PollWork::Pending => {}
            PollWork::Spawned => panic!("unexpected Spawned"),
            PollWork::Ready { .. } => {
                panic!("stream-mode memory wait must not emit DrainResident")
            }
            PollWork::Done => panic!("unexpected Done"),
        }
    }

    /// Cancelling a transfer while it is waiting for memory must cancel the queued
    /// reservation future and release the held slot. `on_terminal` owns this
    /// cleanup because external cancellation does not run `fail` or `complete`.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_while_memory_blocked_releases_future_and_slot() {
        let part_size = 8 * MB;
        let object_size = 3 * part_size;
        let (transfer, mut consumer) = create_download_for_gate_with_capacity(
            object_size,
            part_size,
            Some(2 * part_size as usize),
        );

        skip_discovery(&transfer).await;
        let pool = transfer.ctx().handle.buffer_pool.clone();

        // Poll 1 issues part 1; poll 2 queues part 2 for memory.
        let _w1 = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
        assert_eq!(
            pool.metrics().queued_reservations(),
            1,
            "part 2's reservation is queued for memory"
        );

        // Cancel and run the terminal hook, exactly as `cancel_descriptor` does.
        assert!(transfer.ctx().set_cancelled());
        transfer.on_terminal();

        // The waiting PendingClaim was dropped: its future left the queue and its
        // held slot released. The queue is empty, so a freed chunk cannot be misgranted
        // to the dead transfer.
        assert_eq!(
            pool.metrics().queued_reservations(),
            0,
            "cancel must dequeue the memory waiter"
        );

        // No planned demand was granted to the cancelled waiter.
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            part_size,
            "only part 1 retains acquisition authority; the waiter was not granted"
        );
        assert_eq!(
            pool.metrics().admission_used_bytes(),
            2 * part_size,
            "the discovery payload and part-1 reservation remain charged"
        );

        // The consumer is woken and sees terminal rather than hanging on seq.
        // (Discovery chunk may still deliver; the point is no deadlock.)
        let _ = consumer.try_take_next();
    }

    /// Discovery awaits memory admission inside `execute`, rather than returning
    /// `Pending` to the scheduler. External cancellation must wake that await,
    /// drop the queued future, and return the worker slot.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_while_discovery_waits_for_memory_admission() {
        let part_size = 8 * MB;
        let (transfer, _consumer) =
            create_download_for_gate_with_capacity(part_size, part_size, Some(part_size as usize));
        let pool = transfer.ctx().handle.buffer_pool.clone();
        let blocker = pool
            .try_reserve(part_size as usize)
            .unwrap()
            .expect("test blocker");

        let mut work = assert_ready(transfer.poll_work());
        let executing = transfer.clone();
        let task = tokio::spawn(async move { execute(&executing, &mut work).await });

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.metrics().queued_reservations() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("discovery reservation should enter the memory FIFO");

        assert!(transfer.ctx().set_cancelled());
        transfer.on_terminal();

        assert!(matches!(task.await.unwrap(), WorkOutcome::Cancelled));
        assert_eq!(
            pool.metrics().queued_reservations(),
            0,
            "terminal notification must cancel the discovery reservation future"
        );
        drop(blocker);
        assert_eq!(pool.metrics().active_planned_demand_bytes(), 0);
    }

    /// The failure path must release a memory-blocked claim. `enter_terminal`
    /// extracts it under download state, then `fail` drops it after unlocking so
    /// cancellation can safely enter memory admission.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fail_while_memory_blocked_releases_future_and_slot() {
        let part_size = 8 * MB;
        let object_size = 3 * part_size;
        let (transfer, mut consumer) = create_download_for_gate_with_capacity(
            object_size,
            part_size,
            Some(2 * part_size as usize),
        );

        skip_discovery(&transfer).await;
        let pool = transfer.ctx().handle.buffer_pool.clone();

        // Poll 1 issues part 1; poll 2 queues part 2 for memory.
        let _w1 = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
        assert_eq!(
            pool.metrics().queued_reservations(),
            1,
            "part 2's reservation is queued for memory"
        );

        // Fail the transfer while the claim waits for memory, exactly as
        // `fail_range` does: take the state lock and run the fail transition.
        let err = error::Error::new(error::ErrorKind::RuntimeError, "injected test failure");
        {
            let guard = transfer.inner.state.lock().unwrap();
            let _ = transfer.fail(guard, err);
        }

        // The waiting PendingClaim was dropped by `enter_terminal`.
        assert_eq!(
            pool.metrics().queued_reservations(),
            0,
            "fail must dequeue the memory waiter"
        );
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            part_size,
            "only part 1 retains acquisition authority; the waiter was not granted"
        );
        assert_eq!(
            pool.metrics().admission_used_bytes(),
            2 * part_size,
            "the discovery payload and part-1 reservation remain charged"
        );

        let _ = consumer.try_take_next();
    }

    /// A single-chunk disk download must flush its tail and return its carrier
    /// charge from `execute`, not from a later `poll_work`.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_terminal_drain_and_carrier_return_happen_in_execute() {
        let part_size = 8 * MB;
        // Single chunk: object_size == part_size. Discovery fetches the entire object in
        // one chunk; no range requests are generated. The terminal completion path runs
        // when execute_read_discovery_body finishes filling that single chunk.
        let object_size = part_size;

        let chunk = vec![0u8; part_size as usize];
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .content_length(part_size as i64)
                .content_range(format!("bytes 0-{}/{}", part_size - 1, object_size))
                .e_tag("test-etag")
                .body(ByteStream::from(chunk.clone()))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]);

        let pool = crate::memory::BufferPool::builder()
            .memory_budget(crate::types::MemoryBudgetConfig::Limit(part_size as usize))
            .build()
            .expect("test pool");
        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .memory(crate::types::MemoryConfig::Explicit(pool.clone()))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        // Disk-mode body so finalize drains to a file and returns carrier charges.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) =
            crate::operation::download::body::new_recv_body_with_sink(file, 0, false);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);

        assert_eq!(pool.metrics().active_planned_demand_bytes(), 0);

        // Drive discovery: poll_work returns discovery work, execute fetches it.
        // The discovery chunk is the complete object, so its execute path fills
        // and terminally drains it without another scheduler poll.
        let mut work = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut work).await;

        // The terminal execute must have finalized and returned the carrier
        // charge, all without any subsequent poll_work.
        assert!(
            matches!(outcome, WorkOutcome::Success { .. }),
            "expected Success, got {:?}",
            outcome
        );
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            0,
            "terminal drain + carrier return must happen in execute, not a later poll_work"
        );
        assert!(
            !transfer.ctx().is_active(),
            "transfer must be completed after terminal execute"
        );

        // Verify data landed on disk.
        let written = std::fs::read(&path).unwrap();
        assert_eq!(
            written.len(),
            part_size as usize,
            "all bytes should be flushed to disk"
        );
    }

    /// Build a disk-mode download transfer on an already-constructed (shared) handle,
    /// so several transfers can contend for one memory budget. Returns the transfer
    /// plus the tempdir/consumer guards retained for the transfer's lifetime.
    #[cfg(test)]
    fn build_disk_transfer_on(
        handle: Arc<crate::client::Handle>,
    ) -> (
        DownloadTransfer,
        crate::operation::download::body::RecvBodyConsumer,
        tempfile::TempDir,
    ) {
        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::File::create(dir.path().join("out")).unwrap();
        let (writer, consumer) =
            crate::operation::download::body::new_recv_body_with_sink(file, 0, false);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);
        (transfer, consumer, dir)
    }

    /// Drive several transfers under one shared memory budget by round-robin
    /// poll+execute, standing in for the scheduler's generate_work loop. Returns once
    /// every transfer is `Done`. A bounded step budget converts a genuine deadlock into
    /// a test failure (all transfers `Pending` with no progress) rather than a hang.
    #[cfg(test)]
    async fn drive_to_completion(transfers: &[&DownloadTransfer]) {
        let mut done = vec![false; transfers.len()];
        // Generous ceiling: each transfer needs O(parts) poll+execute steps, plus drain
        // relief steps. Far above any real count here; exceeding it means no progress.
        let max_steps = 10_000;
        for _ in 0..max_steps {
            if done.iter().all(|d| *d) {
                return;
            }
            let mut progressed = false;
            for (i, t) in transfers.iter().enumerate() {
                if done[i] {
                    continue;
                }
                match t.poll_work() {
                    PollWork::Ready { io: mut work, .. } => {
                        execute(t, &mut work).await;
                        progressed = true;
                    }
                    PollWork::Done => {
                        done[i] = true;
                        progressed = true;
                    }
                    PollWork::Spawned => {
                        progressed = true;
                    }
                    PollWork::Pending => {}
                }
            }
            if !progressed {
                panic!(
                    "no transfer made progress: all Pending with work outstanding \
                     (memory-admission deadlock regressed)"
                );
            }
        }
        panic!("drive_to_completion exceeded {max_steps} steps without completing");
    }

    /// REGRESSION (PR #154 review, landonxjames): the shared-admission deadlock for
    /// concurrent multi-part disk transfers below the drain batch must NOT wedge.
    ///
    /// Before the fix: a disk chunk's charge returned only on a drain (fires at the
    /// batch `min(SEG_SIZE=16, window)` or a full segment) or the terminal finalize. A
    /// multi-part object below a segment (2..15 parts) held ALL parts resident until
    /// terminal, which needs every part issued and therefore memory admission. Spread across
    /// concurrent transfers under tight shared admission, none could reach its part
    /// count, so none finalized, drained, or returned its carrier.
    ///
    /// The fix (`DownloadWork::DrainResident`): a transfer about to
    /// wait for memory while holding a resident run flushes it first, returning
    /// carriers for the FIFO. This test drives two 2-part transfers on a
    /// 2-part memory budget to completion.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_concurrent_multipart_disk_below_drain_batch_completes() {
        let part_size = 8 * MB;
        let object_size = 2 * part_size; // 2 parts, below the 16-part drain batch

        // Range-echoing mock: the content-range must match the requested range or
        // execute_get_range fails validation. (Fixed body works only for single-chunk.)
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
            let start = req
                .range()
                .and_then(|r| r.parse::<crate::http::header::Range>().ok())
                .map(|r| match r.0 {
                    crate::http::header::ByteRange::Inclusive(s, _) => s,
                    crate::http::header::ByteRange::AllFrom(s) => s,
                    _ => 0,
                })
                .unwrap_or(0);
            let end = std::cmp::min(start + part_size, object_size) - 1;
            let chunk = vec![0u8; (end - start + 1) as usize];
            aws_smithy_mocks::MockResponse::Output(
                GetObjectOutput::builder()
                    .content_length((end - start + 1) as i64)
                    .content_range(format!("bytes {}-{}/{}", start, end, object_size))
                    .e_tag("test-etag")
                    .body(ByteStream::from(chunk))
                    .build(),
            )
        });
        let pool = crate::memory::BufferPool::builder()
            .memory_budget(crate::types::MemoryBudgetConfig::Limit(
                (2 * part_size) as usize,
            ))
            .build()
            .expect("test pool");
        let config = crate::Config::builder()
            .client(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]))
            .part_size(crate::types::PartSize::Target(part_size))
            .memory(crate::types::MemoryConfig::Explicit(pool.clone()))
            .build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        let (a, _ca, _da) = build_disk_transfer_on(handle.clone());
        let (b, _cb, _db) = build_disk_transfer_on(handle.clone());

        // Pre-fix this wedges: both retain an undrainable resident seq-0 while
        // queued for part 1. Post-fix each flushes its resident run before waiting,
        // so both complete under the 2-part memory budget.
        drive_to_completion(&[&a, &b]).await;

        assert!(!a.ctx().is_active(), "transfer A completed");
        assert!(!b.ctx().is_active(), "transfer B completed");
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            0,
            "all reservations released after both transfers complete"
        );
        assert_eq!(
            pool.metrics().queued_reservations(),
            0,
            "no memory waiters remain"
        );
    }

    /// REGRESSION (PR #154 review generalization, aajtodd): the admission deadlock is
    /// NOT specific to small objects (`total_parts < batch`). It is the general
    /// "wait while holding undrained resident parts" wedge, reachable with LARGE multipart
    /// objects that are nowhere near terminal.
    ///
    /// Two transfers, each a 20-part object (far above the 16-part drain batch, so the
    /// small-object predicate `total_parts < batch` does NOT apply). Drive each to hold
    /// 3 filled-undrained resident parts (3 < 16, so no non-terminal drain), then let a
    /// tight shared memory budget cap them mid-stream with 17 parts still remaining. Neither
    /// can issue (budget full), drain (below batch), or reach terminal (`remaining` far
    /// from done). Each is waiting on the other to release, and neither can — the exact
    /// wedge, with objects an order of magnitude larger than the drain batch.
    ///
    /// Drives both to completion under the tight memory budget: pre-fix this wedges (caught by
    /// `drive_to_completion`'s no-progress panic), post-fix the `DrainResident` relief
    /// lets both finish. Proves the fix is not the small-object special case.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_concurrent_large_multipart_partial_fills_complete() {
        let part_size = 8 * MB;
        let parts_per_object = 20u64; // WELL above the 16-part drain batch
        let object_size = parts_per_object * part_size;
        let resident_per_transfer = 3u64; // 3 < 16: never drains non-terminally

        // Range-echoing mock: each GET's content-range must match the requested range,
        // or execute_get_range's validate_content_range fails the transfer. (A fixed
        // `then_output` body works only for single-chunk objects.)
        let get_obj = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
            let start = req
                .range()
                .and_then(|r| r.parse::<crate::http::header::Range>().ok())
                .map(|r| match r.0 {
                    crate::http::header::ByteRange::Inclusive(s, _) => s,
                    crate::http::header::ByteRange::AllFrom(s) => s,
                    _ => 0,
                })
                .unwrap_or(0);
            let end = std::cmp::min(start + part_size, object_size) - 1;
            let chunk = vec![0u8; (end - start + 1) as usize];
            aws_smithy_mocks::MockResponse::Output(
                GetObjectOutput::builder()
                    .content_length((end - start + 1) as i64)
                    .content_range(format!("bytes {}-{}/{}", start, end, object_size))
                    .e_tag("test-etag")
                    .body(ByteStream::from(chunk))
                    .build(),
            )
        });
        let capacity_bytes = (2 * resident_per_transfer * part_size) as usize;
        let pool = crate::memory::BufferPool::builder()
            .memory_budget(crate::types::MemoryBudgetConfig::Limit(capacity_bytes))
            .build()
            .expect("test pool");
        let config = crate::Config::builder()
            .client(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]))
            .part_size(crate::types::PartSize::Target(part_size))
            .memory(crate::types::MemoryConfig::Explicit(pool.clone()))
            .build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        let (a, _ca, _da) = build_disk_transfer_on(handle.clone());
        let (b, _cb, _db) = build_disk_transfer_on(handle.clone());

        // Memory budget for exactly the resident parts of both transfers, nothing more.
        // Once both fill to that depth neither can reserve another part without a drain.

        // Drive each transfer to hold `resident_per_transfer` filled-undrained parts:
        // discovery fills seq 0, then (resident-1) range GETs fill seqs 1.. . Each fill's
        // in-execute Batched drain frees nothing (below batch), so all stay resident.
        async fn fill_resident(t: &DownloadTransfer, resident: u64) {
            skip_discovery(t).await; // seq 0 filled + reserved
            for _ in 1..resident {
                let mut w = assert_ready(t.poll_work());
                execute(t, &mut w).await;
            }
        }
        fill_resident(&a, resident_per_transfer).await;
        fill_resident(&b, resident_per_transfer).await;

        assert_eq!(
            pool.metrics().admission_used_bytes(),
            capacity_bytes as u64,
            "both transfers hold charged resident parts; memory budget is exactly full"
        );

        // Both are mid-stream (17 parts each remaining) with the budget full. Pre-fix
        // this wedges — neither can issue, drain (below batch), or reach terminal, and
        // each waits on the other. Post-fix each flushes its resident run before waiting,
        // so both complete under the tight budget. `drive_to_completion` panics on a
        // no-progress wedge instead of hanging.
        drive_to_completion(&[&a, &b]).await;

        assert!(!a.ctx().is_active(), "large-object transfer A completed");
        assert!(!b.ctx().is_active(), "large-object transfer B completed");
        assert_eq!(
            pool.metrics().active_planned_demand_bytes(),
            0,
            "all reservations released after both large multipart transfers complete"
        );
        assert_eq!(
            pool.metrics().queued_reservations(),
            0,
            "no memory waiters remain"
        );
    }

    /// The window resolved at construction follows precedence: a per-request
    /// `read_ahead` override wins; absent one, the client default applies.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_read_ahead_resolution_precedence() {
        use crate::types::ReadAhead;

        let build = |client_mode: ReadAhead, input_override: Option<ReadAhead>| {
            let get_obj = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
                GetObjectOutput::builder()
                    .content_length(8 * MB as i64)
                    .content_range(format!("bytes 0-{}/{}", 8 * MB - 1, 8 * MB))
                    .e_tag("test-etag")
                    .body(ByteStream::from(vec![0u8; 8 * MB as usize]))
                    .build()
            });
            let config = crate::Config::builder()
                .client(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]))
                .part_size(crate::types::PartSize::Target(8 * MB))
                .read_ahead(client_mode)
                .build();
            let handle = crate::client::Handle::test_handle_tokio(config);
            let input = DownloadInput::builder()
                .bucket("test-bucket")
                .key("test-key")
                .set_read_ahead(input_override)
                .build()
                .unwrap();
            let (writer, _consumer) = crate::operation::download::body::new_recv_body();
            let (ctx, _rx) = TransferContext::new(handle);
            DownloadTransfer::new(ctx, BucketType::Standard, input, writer)
        };

        // No request override: the client default resolves.
        let t = build(ReadAhead::Parts(9), None);
        assert_eq!(t.read_ahead().window(), 10, "client default applies");

        // Request override wins over the client default.
        let t = build(ReadAhead::Parts(9), Some(ReadAhead::Parts(3)));
        assert_eq!(t.read_ahead().window(), 4, "request override wins");

        // Client Auto resolves to the fixed default when not overridden.
        let t = build(ReadAhead::Auto, None);
        assert_eq!(
            t.read_ahead().window(),
            super::super::read_ahead::DEFAULT_WINDOW_PARTS
        );
    }

    /// `io_ctl().set_read_ahead` resolves the public knob to a window and applies it
    /// to the running transfer; the gate observes the new value on the next poll.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_io_ctl_set_read_ahead_resizes_window() {
        let (transfer, _consumer) = create_download_for_gate(128 * MB, 8 * MB);
        skip_discovery(&transfer).await;

        // Parts(n) resolves to n + 1 (n speculative parts plus the demand part).
        transfer
            .io_ctl()
            .set_read_ahead(crate::types::ReadAhead::Parts(4));
        assert_eq!(transfer.read_ahead().window(), 5);

        // Parts(0) is demand paging: a window of exactly one outstanding part.
        transfer
            .io_ctl()
            .set_read_ahead(crate::types::ReadAhead::Parts(0));
        assert_eq!(transfer.read_ahead().window(), 1);

        // Auto returns to the default fixed window.
        transfer
            .io_ctl()
            .set_read_ahead(crate::types::ReadAhead::Auto);
        assert_eq!(
            transfer.read_ahead().window(),
            super::super::read_ahead::DEFAULT_WINDOW_PARTS
        );
    }

    use crate::http::header::Range;
    use aws_smithy_mocks::MockResponse;
    use aws_smithy_runtime::test_util::capture_test_logs::show_test_logs;
    use aws_smithy_runtime_api::http::{Response, StatusCode};
    use aws_smithy_types::body::SdkBody;
    use std::collections::HashMap;

    /// Failure behavior for a specific seq
    #[derive(Clone)]
    enum FailureBehavior {
        /// Always fail
        Always,
        /// Fail first N attempts, then succeed
        #[allow(dead_code)] // TODO: re-enable with retry tests
        Times(usize),
    }

    /// Builder for downloads with configurable failure injection
    struct FailureConfig {
        object_size: u64,
        part_size: u64,
        failures: HashMap<u64, FailureBehavior>,
    }

    impl FailureConfig {
        fn new(object_size: u64, part_size: u64) -> Self {
            Self {
                object_size,
                part_size,
                failures: HashMap::new(),
            }
        }

        /// Fail this seq always (retryable 500 error)
        fn fail(mut self, seq: u64) -> Self {
            self.failures.insert(seq, FailureBehavior::Always);
            self
        }

        fn build(self) -> DownloadTransfer {
            let object_size = self.object_size;
            let part_size = self.part_size;
            let failures = Arc::new(self.failures);
            let call_counts: Arc<std::sync::Mutex<HashMap<u64, usize>>> =
                Arc::new(std::sync::Mutex::new(HashMap::new()));

            let get_obj = mock!(aws_sdk_s3::Client::get_object).then_compute_response(move |req| {
                let seq = req
                    .range()
                    .and_then(|r| r.parse::<Range>().ok())
                    .map(|r| match r.0 {
                        crate::http::header::ByteRange::Inclusive(start, _) => start / part_size,
                        crate::http::header::ByteRange::AllFrom(start) => start / part_size,
                        _ => 0,
                    })
                    .unwrap_or(0);

                // Track calls per seq
                let mut counts = call_counts.lock().unwrap();
                let call_num = *counts.entry(seq).and_modify(|c| *c += 1).or_insert(1);
                drop(counts);

                // Check failure config
                let should_fail = match failures.get(&seq) {
                    Some(FailureBehavior::Always) => true,
                    Some(FailureBehavior::Times(n)) => call_num <= *n,
                    None => false,
                };

                if should_fail {
                    MockResponse::Http(Response::new(
                        StatusCode::try_from(500).unwrap(),
                        SdkBody::from("internal error"),
                    ))
                } else {
                    let start = seq * part_size;
                    let end = std::cmp::min(start + part_size, object_size) - 1;
                    let chunk = vec![0u8; (end - start + 1) as usize];

                    MockResponse::Output(
                        GetObjectOutput::builder()
                            .content_length((end - start + 1) as i64)
                            .content_range(format!("bytes {}-{}/{}", start, end, object_size))
                            .e_tag("test-etag")
                            .body(ByteStream::from(chunk))
                            .build(),
                    )
                }
            });

            create_download_with_client(
                mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj], |conf| {
                    conf.retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                }),
                part_size,
            )
        }
    }

    #[test]
    fn test_validate_content_range_success() {
        assert!(validate_content_range("bytes=1024-2047", Some("bytes 1024-2047/4096")).is_ok());
        assert!(validate_content_range("1024-2047", Some("bytes 1024-2047/4096")).is_ok());
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        assert!(validate_content_range("bytes=1024-2047", Some("bytes 2048-3071/4096")).is_err());
    }

    #[test]
    fn test_validate_content_range_missing() {
        assert!(validate_content_range("bytes=1024-2047", None).is_err());
    }

    #[test]
    fn test_validate_content_range_substring_false_positive() {
        // The substring "0-9" appears in "bytes 10-99/100", but the ranges differ.
        // A naive `.contains()` match would wrongly pass this.
        assert!(validate_content_range("bytes=0-9", Some("bytes 10-99/100")).is_err());
    }

    #[test]
    fn test_validate_content_range_exact_match_small_range() {
        assert!(validate_content_range("bytes=0-9", Some("bytes 0-9/100")).is_ok());
    }

    #[test]
    fn test_validate_content_range_unsized_total() {
        // RFC 7233 allows `bytes START-END/*` when the total is unknown.
        assert!(validate_content_range("bytes=0-9", Some("bytes 0-9/*")).is_ok());
    }

    #[test]
    fn test_validate_content_range_large_range_match() {
        assert!(validate_content_range("bytes=100-199", Some("bytes 100-199/500")).is_ok());
    }

    #[test]
    fn test_validate_content_range_response_none() {
        assert!(validate_content_range("bytes=0-9", None).is_err());
    }

    #[test]
    fn test_validate_content_range_garbage_response() {
        assert!(validate_content_range("bytes=0-9", Some("bytes abc")).is_err());
    }

    #[test]
    fn test_parse_byte_range_request_format() {
        assert_eq!(parse_byte_range("bytes=0-9"), Some((0, 9)));
        assert_eq!(parse_byte_range("bytes=100-199"), Some((100, 199)));
    }

    #[test]
    fn test_parse_byte_range_response_format() {
        assert_eq!(parse_byte_range("bytes 0-9/100"), Some((0, 9)));
        assert_eq!(parse_byte_range("bytes 100-199/500"), Some((100, 199)));
        assert_eq!(parse_byte_range("bytes 0-9/*"), Some((0, 9)));
    }

    #[test]
    fn test_parse_byte_range_invalid() {
        assert_eq!(parse_byte_range("bytes abc"), None);
        assert_eq!(parse_byte_range(""), None);
        assert_eq!(parse_byte_range("bytes 9-0"), None); // start > end
    }
}
