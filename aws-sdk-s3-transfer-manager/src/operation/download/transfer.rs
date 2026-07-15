/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Download transfer implementation for scheduler integration.

use std::cmp;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bytes_utils::SegmentedBuf;

use super::input::copy_fields_to_get_object_request;

use crate::error::{self, ChunkRef, Error};
use crate::io::AggregatedBytes;
use crate::operation::download::body::{BodySlot, BodyWriter, ChunkOutput};
use crate::operation::download::chunk_meta::ChunkMetadata;
use crate::operation::download::context::{DownloadState, PendingClaim};
use crate::operation::download::discovery::{discover_obj, ObjectDiscovery};
use crate::operation::download::object_meta::ObjectMetadata;
use crate::operation::download::read_ahead::ReadAhead;
use crate::operation::download::recv_buffer::{DrainMode, FillOutcome};
use crate::operation::download::DownloadInput;
use crate::runtime::memory::{NotifyFn, Reservation, Reserve};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::BucketType;

/// Download-specific work data.
#[derive(Debug)]
pub(crate) enum DownloadWork {
    Discovery,
    GetObjectRange {
        range: std::ops::RangeInclusive<u64>,
        slot: Option<BodySlot>,
        etag: Option<Arc<str>>,
    },
    /// Budget-pressure relief: flush the resident filled run to disk, releasing its
    /// reservations, so a budget-parked transfer does not pin chunks it has no other
    /// path to release. Emitted by [`drain_or_park`](DownloadTransfer::drain_or_park)
    /// and handled in `execute`; carries no data (operates on the writer).
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
    /// - `PollWork::Ready(work)` - work available to execute
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
                PollWork::Ready(IoRequest {
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
                // compose in order — the per-transfer read-ahead window, then the global
                // memory budget — so issuance takes their min. The window is gated first
                // (via `gate.try_issue`) so a window-blocked transfer never holds a slice
                // of the fungible budget it cannot use yet, starving other transfers.
                let slot = if pending.is_some() {
                    // A budget-parked claim takes priority: the gate already admitted and
                    // counted its slot (held in `pending`, not re-gated), waiting only on
                    // the reservation the budget queued.
                    match self.resume_pending_claim(pending) {
                        Some(slot) => slot,
                        // Not granted yet; the queued ticket is the waker. Flush any
                        // resident run first so we do not pin budget while parked.
                        None => return self.drain_or_park(),
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
                    // reserve its backing memory against the budget. A grant issues now;
                    // a queued reservation stashes the claimed slot in `pending` and
                    // parks until the budget wakes us. The gate's `issued` bump stays —
                    // the parked claim legitimately occupies its one window seat — so a
                    // budget-parked transfer holds exactly the slot it will fill.
                    let range_len = chunk_len(range, *part_size);
                    match self.reserve_claim(range_len, pending) {
                        Some(slot) => slot,
                        // Budget-parked; `pending` now holds the claimed slot. Flush any
                        // resident run first so we do not pin budget while parked.
                        None => return self.drain_or_park(),
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

                PollWork::Ready(IoRequest {
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
    /// waker re-readies it — the consumer freeing occupancy (gate), the budget granting
    /// a queued reservation, or a GET completion decrementing the in-flight count.
    fn park(&self) -> PollWork {
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    /// Budget-park decision. A transfer that parks on the budget while holding a
    /// drainable resident run pins those chunks with no path to release them: the run
    /// is below the drain batch (so no fill-triggered drain frees it) and the transfer
    /// cannot reach terminal (its `remaining` part is what is blocked on the budget).
    /// Spread across concurrent disk transfers this deadlocks — none can release, so
    /// none is granted. To avoid it, flush the resident run first (releasing its budget)
    /// by emitting a `DrainResident` work item, and only park when there is nothing to
    /// flush. The drain runs in `execute` (poll_work does no I/O); its completion
    /// re-polls this transfer via `generate_work`, and the freed budget re-grants FIFO.
    ///
    /// Only the budget-park sites call this. A `has_drainable_resident` guard keeps it a
    /// no-op-to-park when the resident prefix is blocked behind an in-flight gap (that
    /// GET's completion carries the drain instead) or in stream mode (the consumer
    /// drives release), so it never spins emitting empty drains.
    fn drain_or_park(&self) -> PollWork {
        if self.inner.writer.has_drainable_resident() {
            PollWork::Ready(IoRequest {
                data: Some(Box::new(DownloadWork::DrainResident)),
            })
        } else {
            self.park()
        }
    }

    /// Resume a budget-parked claim. The gate already admitted and counted the slot
    /// before parking, so it is not re-gated; this only checks whether the budget has
    /// granted the queued reservation. Returns the slot with its reservation attached
    /// once granted, or `None` while still queued (the ticket is the waker).
    fn resume_pending_claim(&self, pending: &mut Option<PendingClaim>) -> Option<BodySlot> {
        let granted = pending.as_mut().unwrap().ticket.take();
        granted.map(|reservation| {
            let mut claim = pending.take().unwrap();
            claim.slot.attach_reservation(reservation);
            claim.slot
        })
    }

    /// Claim a slot and reserve its backing memory against the global budget. The
    /// read-ahead gate has already admitted (and counted) this slot. Returns the slot
    /// with its reservation attached on an immediate grant; on a queued reservation,
    /// stashes the claimed slot in `pending` and returns `None` so the caller parks
    /// until the budget wakes it.
    fn reserve_claim(
        &self,
        range_len: usize,
        pending: &mut Option<PendingClaim>,
    ) -> Option<BodySlot> {
        let mut slot = self.inner.writer.claim();
        // Common case: the budget has room and no one is queued. Grant without building a waker
        // (`try_reserve` grants under exactly the same condition `reserve` returns `Ready`).
        if let Some(reservation) = self.inner.ctx.handle.memory_budget.try_reserve(range_len) {
            slot.attach_reservation(reservation);
            return Some(slot);
        }
        // Budget full or a waiter is queued: build the waker and park on a queued reservation.
        let notify: NotifyFn = {
            let scheduler = self.inner.ctx.handle.scheduler.clone();
            let tid = self.inner.ctx.id;
            Arc::new(move || scheduler.wake(tid))
        };
        match self
            .inner
            .ctx
            .handle
            .memory_budget
            .reserve(range_len, notify)
        {
            Reserve::Ready(reservation) => {
                slot.attach_reservation(reservation);
                Some(slot)
            }
            Reserve::Pending(ticket) => {
                *pending = Some(PendingClaim { slot, ticket });
                None
            }
        }
    }

    /// Reserve budget for a chunk of `len` bytes, awaiting a grant if the budget is
    /// full. Used by the discovery path: discovery has already issued the GET and must
    /// account its chunk's memory, but it runs inside an async `execute` (not the
    /// re-pollable `poll_work`), so it backpressures by awaiting here — holding the
    /// response stream undrained — rather than parking on the scheduler. The head
    /// (seq 0) reserving before any read-ahead range keeps the budget FIFO head-first,
    /// which guarantees forward progress under a tight cap.
    ///
    /// Returns `None` if the transfer goes terminal (cancel/fail) while parked on the
    /// budget: a per-transfer cancel only flips the status flag and wakes waiters on
    /// `discovery_notify`, so the wait is raced against it and rechecks `is_active` on
    /// each wake — otherwise a discovery parked under a tight budget would hang until
    /// the budget granted (only shutdown, not per-transfer cancel, aborts `execute`).
    ///
    /// A budget-parked discovery holds its worker/concurrency slot for the whole
    /// `execute` future (unlike the `poll_work` read-ahead park, which returns `Pending`
    /// and releases the slot), so a pathologically small budget relative to concurrency
    /// can reduce effective worker parallelism; forward progress is still guaranteed by
    /// FIFO plus the idle forced-grant.
    ///
    /// The two wakers have different permit semantics, so the register-before-check
    /// ordering is required for `discovery_notify` but not the budget waker:
    /// `notify_waiters` stores no permit, so a terminal wake is lost unless its
    /// `notified()` future exists before the `is_active` check; `notify_one` stores a
    /// permit for an unregistered waiter, which the next `take` consumes regardless of
    /// ordering.
    async fn reserve_chunk(&self, len: usize) -> Option<Reservation> {
        let notify = Arc::new(tokio::sync::Notify::new());
        let waker = Arc::clone(&notify);
        let notify_fn: NotifyFn = Arc::new(move || waker.notify_one());
        match self.inner.ctx.handle.memory_budget.reserve(len, notify_fn) {
            Reserve::Ready(reservation) => Some(reservation),
            Reserve::Pending(mut ticket) => loop {
                // Register interest on both wakers BEFORE checking state, so a terminal
                // transition that fires `discovery_notify` between the check and the
                // await cannot be lost (tokio `Notify::notify_waiters` stores no permit).
                let budget_wake = notify.notified();
                let terminal_wake = self.inner.discovery_notify.notified();
                if let Some(reservation) = ticket.take() {
                    tracing::debug!(
                        target: crate::telemetry::TARGET_SCHEDULING,
                        tid = %self.inner.ctx.id,
                        len,
                        "discovery chunk reservation granted; resuming",
                    );
                    return Some(reservation);
                }
                if !self.inner.ctx.is_active() {
                    // Terminal while parked: the WaitTicket drops here, cancelling the
                    // queued budget request. Abandon the chunk so `execute` returns.
                    return None;
                }
                tokio::select! {
                    _ = budget_wake => {}
                    _ = terminal_wake => {}
                }
            },
        }
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

    /// Flush the resident filled run to disk, releasing its budget reservations, then
    /// release the freed read-ahead occupancy. Emitted by [`drain_or_park`] when a
    /// transfer would otherwise park on the budget while pinning a resident run.
    ///
    /// Unlike a fill-triggered drain, no GET completed here, so `ranges_in_flight` is
    /// untouched; only occupancy is released (matching the gate side of
    /// [`decrement_in_flight`](Self::decrement_in_flight), under the same state lock so
    /// the release is ordered against the issuer's park). Dropping the drained
    /// `ChunkOutput`s frees their reservations, which the budget re-grants FIFO; the
    /// `on_completion -> generate_work` after this returns re-polls this transfer.
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
            .unwrap_or(0) as u64;
        let total_size =
            chunk_content_len + remaining.as_ref().map_or(0, |r| r.end() - r.start() + 1);
        self.inner.writer.preallocate(total_size);
        self.inner.ctx.set_total_bytes(total_size);

        // If there's an initial chunk, claim seq BEFORE waking to prevent race
        // where poll_work exhausts the window before we can claim our seq.
        // Invariant: initial_chunk.is_some() == chunk_meta.is_some()
        //
        // Reserve the discovery chunk's memory here too: it is already resident (the
        // discovery GET fetched it), so it must be accounted before any read-ahead
        // range issues. Awaiting the reservation head-first keeps the budget FIFO
        // ordered — under a tight budget the head part is admitted before the window
        // fans out — and backpressures by holding this chunk undelivered rather than
        // parking on the scheduler (discovery runs in async `execute`, not poll_work).
        let initial_work = match (initial_chunk, chunk_meta) {
            (Some(stream), Some(meta)) => {
                let mut slot = self.inner.writer.claim();
                match self.reserve_chunk(chunk_content_len as usize).await {
                    Some(reservation) => {
                        slot.attach_reservation(reservation);
                        Some((stream, meta, slot))
                    }
                    // Terminal (cancel/fail by another path) while reserving the discovery
                    // chunk: the transfer is already in its terminal state, so drop the
                    // claimed slot (waking the consumer) and return without transitioning
                    // to Transferring. `execute` reports Cancelled.
                    None => {
                        drop(slot);
                        return WorkOutcome::Cancelled;
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
            Some((stream, chunk_meta, slot)) => {
                self.execute_read_discovery_body(stream, slot, chunk_meta, etag)
                    .await
            }
            None => WorkOutcome::Success { data: None },
        }
    }

    async fn execute_read_discovery_body(
        &self,
        stream: aws_sdk_s3::primitives::ByteStream,
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
        let result = crate::retry::retry(crate::retry::classify_body_retry, || {
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
                            .guarded(async {
                                req.send()
                                    .await
                                    .map(|resp| resp.body)
                                    .map_err(crate::error::Error::from)
                            })
                            .await?
                    }
                };
                // Untimed: drain the body. Errors here are inner (retryable IO).
                Self::read_body_stream(&ctx, body)
                    .await
                    .map_err(crate::retry::GuardError::Inner)
            }
        })
        .await;

        let (segmented, bytes_received) = match result {
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
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
            // The slot carries the reservation; fill() moves it into the chunk.
            reservation: None,
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

    /// Drain a chunk body stream into a buffer, returning the bytes and the count.
    ///
    /// Shared by the range-chunk and discovery-chunk paths. A stream error is
    /// classified by [`error::body_read_error`]: a checksum mismatch becomes
    /// [`ErrorKind::IntegrityError`] (which the retry classifier never re-issues,
    /// to avoid masking corruption), any other stream failure becomes
    /// [`ErrorKind::IOError`] (the body-read class the classifier re-issues). A
    /// cancellation observed mid-read maps to [`ErrorKind::OperationCancelled`].
    async fn read_body_stream(
        ctx: &TransferContext,
        body: aws_sdk_s3::primitives::ByteStream,
    ) -> Result<(SegmentedBuf<bytes::Bytes>, u64), crate::error::Error> {
        let mut segmented = SegmentedBuf::new();
        let mut bytes_received: u64 = 0;
        let mut body_stream = body;

        while let Some(result) = body_stream.next().await {
            let data = result.map_err(|e| crate::error::body_read_error(e, None))?;
            bytes_received += data.len() as u64;
            segmented.push(data);
            if !ctx.is_active() {
                return Err(crate::error::Error::new(
                    crate::error::ErrorKind::OperationCancelled,
                    "transfer cancelled during body read",
                ));
            }
        }

        Ok((segmented, bytes_received))
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

        let recv_latencies = &self.inner.ctx.handle.telemetry.recv_latencies;
        // The deadline guards only the GET response (send → headers ≈ TTFB): the
        // `recv_latencies.guarded(send)` wrapper times the send, then the body is
        // read UNTIMED. A large part on a slow-but-healthy link takes a long time
        // to drain and must not be cancelled as a straggler; a dead mid-body
        // stream is caught by stalled-stream protection. A `GuardError`
        // (deadline timeout OR inner error from either phase) is classified by
        // the retry loop.
        let result = crate::retry::retry(crate::retry::classify_body_retry, || {
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
                    .guarded(async move {
                        let resp = req.send().await.map_err(crate::error::Error::from)?;
                        validate_content_range(&rh_validate, resp.content_range())?;
                        Ok::<_, crate::error::Error>(resp)
                    })
                    .await?;
                // Untimed: drain the body. Errors here are inner (retryable IO).
                let chunk_meta = ChunkMetadata::from(&resp);
                let (segmented, bytes_received) = Self::read_body_stream(&ctx, resp.body)
                    .await
                    .map_err(crate::retry::GuardError::Inner)?;
                Ok::<_, crate::retry::GuardError<crate::error::Error>>((
                    chunk_meta,
                    segmented,
                    bytes_received,
                ))
            }
        })
        .await;

        let (chunk_meta, segmented, bytes_received) = match result {
            Ok(val) => val,
            Err(e) => {
                return self.fail_range(ChunkRef::new(seq, Some(*range.start()..=*range.end())), e)
            }
        };

        bail_if_terminal!(self);
        let chunk = ChunkOutput {
            seq,
            offset: *range.start(),
            data: AggregatedBytes(segmented),
            metadata: chunk_meta,
            // The slot carries the reservation; fill() moves it into the chunk.
            reservation: None,
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
    /// tail to disk (releasing the last reservations as their `ChunkOutput`s drop), set the
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
        // Transition to Terminal, taking any budget-parked claim so its WaitTicket::drop (which
        // locks the budget) runs after we release the state lock, never nested under it.
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
        // Release a budget-parked claim if one is held: an external cancel does not run the
        // `fail`/`complete` transition that would take it, so the queued `WaitTicket` would
        // otherwise linger in the shared budget — and could even be granted a live reservation
        // into a dead transfer's slot, shrinking the budget for others until this transfer's Arc
        // drops. `enter_terminal` takes it under the state lock; drop it after releasing the lock
        // so `WaitTicket::drop` (which takes the budget lock) never nests under the state lock.
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

/// Validate that the response Content-Range matches the requested range.
fn validate_content_range(
    requested_range: &str,
    response_content_range: Option<&str>,
) -> Result<(), Error> {
    let normalized = requested_range
        .strip_prefix("bytes=")
        .unwrap_or(requested_range);

    if response_content_range
        .map(|range| range.contains(normalized))
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(error::Error::new(
            error::ErrorKind::RuntimeError,
            format!(
                "content range mismatch: requested {}, response {:?}",
                requested_range, response_content_range
            ),
        ))
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
        while let PollWork::Ready(mut w) = transfer.poll_work() {
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
                PollWork::Ready(_item) => {
                    issued += 1;
                    assert!(issued <= w, "issuance ran past the window");
                }
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
        while let PollWork::Ready(_item) = transfer.poll_work() {}
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

    /// The memory budget is the second issuance gate, past the read-ahead window: with
    /// the window wide open, a transfer still parks when the budget is exhausted,
    /// holding its claimed slot in `pending`, and resumes when a delivered chunk frees
    /// a reservation the budget re-grants. Distinct from the window gate above (this
    /// leaves the window open and binds on bytes).
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_budget_blocks_then_resumes() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        // Object = 3 parts of one chunk each. Discovery fetches part 0 (seq 0) and
        // reserves its chunk, so in_use == 1 after discovery; remaining covers 1 + 2.
        let part_size = BUDGET_CHUNK_BYTES as u64;
        let object_size = 3 * part_size;
        let (transfer, mut consumer) = create_download_for_gate(object_size, part_size);

        skip_discovery(&transfer).await;
        let budget = transfer.ctx().handle.memory_budget.clone();
        assert_eq!(budget.in_use_chunks(), 1, "discovery chunk is reserved");

        // Tighten to 2 chunks: the discovery chunk plus room for exactly one range. The
        // window stays wide (default), so only the budget can bind here.
        budget.set_limit(2 * BUDGET_CHUNK_BYTES);

        // poll #1: part 1 fits (need 1, free 1) → Ready.
        let _w1 = assert_ready(transfer.poll_work());
        assert_eq!(budget.in_use_chunks(), 2);

        // poll #2: part 2 does not fit (in_use 2, cap 2) → parked on the budget, holding
        // the claimed slot in `pending` until a chunk frees.
        assert_pending(transfer.poll_work());
        {
            let state = transfer.inner.state.lock().unwrap();
            match &*state {
                DownloadState::Transferring { pending, .. } => {
                    assert!(pending.is_some(), "claim should be parked on the budget");
                }
                _ => panic!("expected Transferring"),
            }
        }

        // Consume the discovery chunk (seq 0) and drop it: its reservation releases,
        // freeing a chunk the budget immediately re-grants to the parked part-2 claim.
        drop(consumer.try_take_next().expect("discovery chunk is filled"));
        assert_eq!(
            budget.in_use_chunks(),
            2,
            "freed chunk re-granted to the parked waiter"
        );

        // poll #3: the parked claim was granted → Ready, pending cleared.
        let _w2 = assert_ready(transfer.poll_work());
        {
            let state = transfer.inner.state.lock().unwrap();
            match &*state {
                DownloadState::Transferring { pending, .. } => {
                    assert!(pending.is_none(), "parked claim should be consumed");
                }
                _ => panic!("expected Transferring"),
            }
        }
        assert_eq!(budget.in_use_chunks(), 2);
    }

    /// A stream-mode transfer that budget-parks must return `Pending`, never a
    /// `DrainResident` work item. `drain_or_park` gates on `has_drainable_resident`,
    /// which is false in stream mode (the consumer drives release), so the budget
    /// deadlock relief does not apply and must not spin emitting empty drains.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn stream_budget_park_does_not_emit_drain_resident() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        // Stream-mode transfer (create_download_for_gate uses new_recv_body). 3 parts.
        let part_size = BUDGET_CHUNK_BYTES as u64;
        let object_size = 3 * part_size;
        let (transfer, _consumer) = create_download_for_gate(object_size, part_size);

        skip_discovery(&transfer).await;
        let budget = transfer.ctx().handle.memory_budget.clone();
        // Tighten to the discovery chunk only: the next range reservation cannot fit.
        budget.set_limit(BUDGET_CHUNK_BYTES);

        // The transfer holds a filled seq-0 (resident on the stream surface) and parks
        // on the budget for part 1. Because it is stream mode, the relief path is off:
        // poll_work returns Pending, not Ready(DrainResident).
        match transfer.poll_work() {
            PollWork::Pending => {}
            PollWork::Ready(_) => panic!("stream-mode budget park must not emit DrainResident"),
            PollWork::Done => panic!("unexpected Done"),
        }
    }

    /// Cancelling a transfer while it is budget-parked must cancel the queued budget
    /// wait and release the held slot — otherwise the `WaitTicket` lingers in the
    /// shared budget (and could be granted a live reservation into a dead transfer),
    /// shrinking the budget for every other transfer. `on_terminal` (the external-cancel
    /// hook) is responsible, since cancel does not run the `fail`/`complete` transition.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancel_while_budget_parked_releases_ticket_and_slot() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        let part_size = BUDGET_CHUNK_BYTES as u64;
        let object_size = 3 * part_size;
        let (transfer, mut consumer) = create_download_for_gate(object_size, part_size);

        skip_discovery(&transfer).await;
        let budget = transfer.ctx().handle.memory_budget.clone();
        budget.set_limit(2 * BUDGET_CHUNK_BYTES);

        // poll #1 issues part 1 (in_use 2); poll #2 parks part 2 on the budget.
        let _w1 = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
        assert_eq!(
            budget.stats().waiters,
            1,
            "part 2's reservation is queued on the budget"
        );

        // Cancel and run the terminal hook, exactly as `cancel_descriptor` does.
        assert!(transfer.ctx().set_cancelled());
        transfer.on_terminal();

        // The parked PendingClaim was dropped: its WaitTicket left the queue and its
        // held slot released. The queue is empty, so a freed chunk cannot be misgranted
        // to the dead transfer.
        assert_eq!(
            budget.stats().waiters,
            0,
            "cancel must dequeue the parked budget waiter"
        );

        // The invariant under test is the WAITER release; assert in_use did not grow
        // past what is actually reserved (no phantom grant to the cancelled waiter).
        assert_eq!(
            budget.in_use_chunks(),
            2,
            "exactly the discovery + part-1 reservations remain; none granted to the cancelled waiter"
        );

        // The consumer is woken and sees terminal rather than hanging on seq.
        // (Discovery chunk may still deliver; the point is no deadlock.)
        let _ = consumer.try_take_next();
    }

    /// The failure path must release a budget-parked claim. `fail` reaches `Terminal` through
    /// `enter_terminal`, which extracts the parked `PendingClaim` under the state lock so its
    /// `WaitTicket` (whose drop locks the budget) is dropped only after the lock is released,
    /// rather than nested under it. That lock ordering is not observable from a single-threaded
    /// test; this guards the functional half — that failing while parked dequeues the waiter and
    /// grants it no phantom reservation — for the fail path the cancel test does not cover.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fail_while_budget_parked_releases_ticket_and_slot() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        let part_size = BUDGET_CHUNK_BYTES as u64;
        let object_size = 3 * part_size;
        let (transfer, mut consumer) = create_download_for_gate(object_size, part_size);

        skip_discovery(&transfer).await;
        let budget = transfer.ctx().handle.memory_budget.clone();
        budget.set_limit(2 * BUDGET_CHUNK_BYTES);

        // poll #1 issues part 1 (in_use 2); poll #2 parks part 2 on the budget.
        let _w1 = assert_ready(transfer.poll_work());
        assert_pending(transfer.poll_work());
        assert_eq!(
            budget.stats().waiters,
            1,
            "part 2's reservation is queued on the budget"
        );

        // Fail the transfer while the claim is parked, exactly as `fail_range` does: take the
        // state lock and run the fail transition.
        let err = error::Error::new(error::ErrorKind::RuntimeError, "injected test failure");
        {
            let guard = transfer.inner.state.lock().unwrap();
            let _ = transfer.fail(guard, err);
        }

        // The parked PendingClaim was dropped by `enter_terminal`: its WaitTicket left the queue.
        assert_eq!(
            budget.stats().waiters,
            0,
            "fail must dequeue the parked budget waiter"
        );
        assert_eq!(
            budget.in_use_chunks(),
            2,
            "exactly the discovery + part-1 reservations remain; none granted to the failed waiter"
        );

        let _ = consumer.try_take_next();
    }

    /// A single-chunk disk download must flush its tail and release its budget reservation
    /// from `execute` -- not from a later `poll_work`. Budget reclamation is event-driven
    /// off the disk drain, never gated on a re-poll.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn terminal_drain_and_release_happen_in_execute() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        let part_size = BUDGET_CHUNK_BYTES as u64;
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

        let config = crate::Config::builder()
            .client(client)
            .part_size(crate::types::PartSize::Target(part_size))
            .build();

        let handle = crate::client::Handle::test_handle_tokio(config);
        let budget = handle.memory_budget.clone();

        let input = DownloadInput::builder()
            .bucket("test-bucket")
            .key("test-key")
            .build()
            .unwrap();

        // Disk-mode body so finalize drains to a file, releasing reservations on drop.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out");
        let file = std::fs::File::create(&path).unwrap();
        let (writer, _consumer) =
            crate::operation::download::body::new_recv_body_with_sink(file, 0, false);
        let (ctx, _completion_rx) = TransferContext::new(handle);
        let transfer = DownloadTransfer::new(ctx, BucketType::Standard, input, writer);

        // Budget starts empty.
        assert_eq!(budget.in_use_chunks(), 0);

        // Drive discovery: poll_work returns discovery work, execute fetches it.
        // After discovery, the discovery chunk is reserved (in_use == 1) and remaining
        // is None (single-chunk object), with ranges_in_flight == 1.
        let mut work = assert_ready(transfer.poll_work());
        let outcome = execute(&transfer, &mut work).await;

        // The terminal execute must have finalized (drained to disk) and released the
        // reservation -- all without any subsequent poll_work.
        assert!(
            matches!(outcome, WorkOutcome::Success { .. }),
            "expected Success, got {:?}",
            outcome
        );
        assert_eq!(
            budget.in_use_chunks(),
            0,
            "terminal drain + reservation release must happen in execute, not a later poll_work"
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
    /// so several transfers can contend for one budget. Returns the transfer plus the
    /// tempdir/consumer guards the caller must keep alive for the transfer's lifetime.
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

    /// Drive several transfers on a shared budget to completion by round-robin
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
                    PollWork::Ready(mut work) => {
                        execute(t, &mut work).await;
                        progressed = true;
                    }
                    PollWork::Done => {
                        done[i] = true;
                        progressed = true;
                    }
                    PollWork::Pending => {}
                }
            }
            if !progressed {
                panic!(
                    "no transfer made progress: all Pending with work outstanding \
                     (budget deadlock regressed)"
                );
            }
        }
        panic!("drive_to_completion exceeded {max_steps} steps without completing");
    }

    /// REGRESSION (PR #154 review, landonxjames): the fungible-budget deadlock for
    /// concurrent multi-part disk transfers below the drain batch must NOT wedge.
    ///
    /// Before the fix: a disk chunk's reservation released only on a drain (fires at the
    /// batch `min(SEG_SIZE=16, window)` or a full segment) or the terminal finalize. A
    /// multi-part object below a segment (2..15 parts) held ALL parts resident until
    /// terminal — which needs every part issued, which needs budget. Spread across
    /// concurrent transfers under a tight shared budget, none could reach its part count,
    /// so none finalized/drained/released and the `in_use == 0` forced-grant never fired.
    ///
    /// The fix (`drain_or_park` → `DownloadWork::DrainResident`): a transfer about to
    /// park on the budget while holding a resident run flushes it first, releasing the
    /// chunks FIFO. This test drives two 2-part transfers on a 2-chunk shared budget to
    /// completion; pre-fix it would spin all-`Pending` (caught by `drive_to_completion`'s
    /// no-progress panic), post-fix both finish and the budget fully releases.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn concurrent_multipart_disk_below_drain_batch_completes() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        let part_size = BUDGET_CHUNK_BYTES as u64; // 1 part == 1 chunk
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
        let config = crate::Config::builder()
            .client(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]))
            .part_size(crate::types::PartSize::Target(part_size))
            .build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let budget = handle.memory_budget.clone();
        // Tight shared budget: exactly two chunks (one discovery chunk per transfer).
        budget.set_limit(2 * BUDGET_CHUNK_BYTES);

        let (a, _ca, _da) = build_disk_transfer_on(handle.clone());
        let (b, _cb, _db) = build_disk_transfer_on(handle.clone());

        // Pre-fix this wedges (both park holding an undrainable resident seq-0 while
        // queued for part-1). Post-fix each flushes its resident run before parking, so
        // both complete under the 2-chunk budget.
        drive_to_completion(&[&a, &b]).await;

        assert!(!a.ctx().is_active(), "transfer A completed");
        assert!(!b.ctx().is_active(), "transfer B completed");
        assert_eq!(
            budget.in_use_chunks(),
            0,
            "all reservations released after both transfers complete"
        );
        assert_eq!(budget.stats().waiters, 0, "no budget waiters remain");
    }

    /// REGRESSION (PR #154 review generalization, aajtodd): the budget deadlock is
    /// NOT specific to small objects (`total_parts < batch`). It is the general
    /// "park holding undrained resident parts" wedge, reachable with LARGE multipart
    /// objects that are nowhere near terminal.
    ///
    /// Two transfers, each a 20-part object (far above the 16-part drain batch, so the
    /// small-object predicate `total_parts < batch` does NOT apply). Drive each to hold
    /// 3 filled-undrained resident parts (3 < 16, so no non-terminal drain), then let a
    /// tight shared budget cap them mid-stream with 17 parts still remaining. Neither
    /// can issue (budget full), drain (below batch), or reach terminal (`remaining` far
    /// from done). Each is waiting on the other to release, and neither can — the exact
    /// wedge, with objects an order of magnitude larger than the drain batch.
    ///
    /// Drives both to completion under the tight budget: pre-fix this wedges (caught by
    /// `drive_to_completion`'s no-progress panic), post-fix the `DrainResident` relief
    /// lets both finish. Proves the fix is not the small-object special case.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn concurrent_large_multipart_partial_fills_completes() {
        use crate::runtime::memory::BUDGET_CHUNK_BYTES;

        let part_size = BUDGET_CHUNK_BYTES as u64; // 1 part == 1 chunk
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
        let config = crate::Config::builder()
            .client(mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[get_obj]))
            .part_size(crate::types::PartSize::Target(part_size))
            .build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let budget = handle.memory_budget.clone();

        let (a, _ca, _da) = build_disk_transfer_on(handle.clone());
        let (b, _cb, _db) = build_disk_transfer_on(handle.clone());

        // Budget room for exactly the resident parts of both transfers, nothing more.
        // Once both fill to that depth neither can reserve another part without a drain.
        let cap_chunks = 2 * resident_per_transfer;
        budget.set_limit(cap_chunks as usize * BUDGET_CHUNK_BYTES);

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
            budget.in_use_chunks(),
            cap_chunks,
            "both transfers hold their resident parts; budget is exactly full"
        );

        // Both are mid-stream (17 parts each remaining) with the budget full. Pre-fix
        // this wedges — neither can issue, drain (below batch), or reach terminal, and
        // each waits on the other. Post-fix each flushes its resident run before parking,
        // so both complete under the tight budget. `drive_to_completion` panics on a
        // no-progress wedge instead of hanging.
        drive_to_completion(&[&a, &b]).await;

        assert!(!a.ctx().is_active(), "large-object transfer A completed");
        assert!(!b.ctx().is_active(), "large-object transfer B completed");
        assert_eq!(
            budget.in_use_chunks(),
            0,
            "all reservations released after both large multipart transfers complete"
        );
        assert_eq!(budget.stats().waiters, 0, "no budget waiters remain");
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
}
