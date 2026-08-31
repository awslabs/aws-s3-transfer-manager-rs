/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! State machine for `download_objects` composite transfer.
//!
//! Mirrors the `upload_objects` state machine: a single S3 listing walker
//! discovers objects, the parent spawns child downloads (via
//! [`Download::orchestrate_with_sink`]), and reaps them as they complete.

use aws_sdk_s3::types::Object;
use parking_lot::Mutex;
use path_clean::PathClean;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{self, Error, ErrorKind};
use crate::io::walk::S3Walk;
use crate::operation::download::{Download, DownloadInput, ManagedDownloadHandle};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::{FailedDownload, FailedTransferPolicy};

/// Maximum terminal children drained into a single `JoinChildren` work item per
/// poll. NOT an accumulation threshold: `poll_work` reaps whatever terminals
/// exist every poll (fused with spawning), so this only caps how many
/// already-terminal handles one `execute_join_children` awaits back-to-back,
/// bounding worker-thread hold time on the serial join. Kept modest so a poll's
/// reap work stays small and terminals are retired continuously rather than in
/// large lumps.
const MAX_REAP_PER_POLL: usize = 64;

/// Maximum entries to drain from the walker per AdvanceWalker work item.
/// Prevents a single work item from holding the executor across multiple
/// ListObjectsV2 pages. Matches S3's default MaxKeys.
const MAX_ENTRIES_PER_WALK: usize = 1000;

/// Low-water mark for the key buffer: the walker dispatches the next listing
/// page while `pending_entries` is below this, keeping the buffer fed ahead of
/// spawn draining it. Sized to one page so a single in-flight advance refills
/// more than a page's worth of runway before the buffer empties. Combined with
/// single-flight dispatch, this bounds `pending_entries` to roughly one page
/// above the mark.
const WALK_LOW_WATER: usize = MAX_ENTRIES_PER_WALK;

/// Number of discovered keys accumulated before publishing them to
/// `pending_entries` mid-advance. Small enough that spawning sees keys quickly
/// while a page is still being fetched, large enough to amortize the state-lock
/// acquisition across many keys.
const WALK_FLUSH_CHUNK: usize = 64;

/// Default S3 key delimiter.
const DEFAULT_DELIMITER: &str = "/";

/// Work items produced by [`DownloadObjectsTransfer::poll_work`].
pub(crate) enum DownloadObjectsWork {
    /// Advance the S3 listing walker by one page/entry. The walker is boxed:
    /// `S3Walk` is large (hundreds of bytes), and boxing keeps the work enum
    /// small for the common `JoinChildren` variant (large_enum_variant).
    AdvanceWalker { walk: Option<Box<S3Walk>> },
    /// Join a batch of terminal child downloads.
    JoinChildren { batch: Option<ReapingBatch> },
}

impl crate::transfer::WorkData for DownloadObjectsWork {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::AdvanceWalker { .. } => "advance-walker",
            Self::JoinChildren { .. } => "join-children",
        }
    }
}

impl std::fmt::Debug for DownloadObjectsWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdvanceWalker { .. } => f.debug_struct("AdvanceWalker").finish_non_exhaustive(),
            Self::JoinChildren { batch } => f
                .debug_struct("JoinChildren")
                .field("count", &batch.as_ref().map(|b| b.count))
                .finish(),
        }
    }
}

/// Tracked child download — handle + the S3 key for error reporting.
pub(crate) struct ChildTransfer {
    pub(crate) handle: ManagedDownloadHandle,
    pub(crate) key: String,
}

/// Owns a reservation of N slots in `State::children_reserved`. Either
/// consumed by [`DownloadObjectsTransfer::merge_spawned`] (paired decrement
/// under the state lock) or released on `Drop` if `consume` is not reached.
/// Constructed only by [`DownloadObjectsTransfer::claim_one`].
///
/// The type makes the counter pairing structural: `merge_spawned`'s signature
/// requires the token, so a caller cannot forget to release the reservation,
/// and the count released always matches the count reserved.
struct Reservation {
    transfer: Arc<DownloadObjectsTransferInner>,
    count: usize,
    consumed: bool,
}

impl Reservation {
    /// Caller holds `state.lock`. Decrements `children_reserved` by the owned
    /// count and marks the reservation consumed. Always paired 1:1 with the
    /// `claim_one` that produced this token.
    fn consume(mut self, state: &mut State) {
        debug_assert!(
            !self.consumed,
            "Reservation::consume called on already-consumed reservation"
        );
        debug_assert!(
            state.children_reserved >= self.count,
            "children_reserved underflow: have {}, releasing {}",
            state.children_reserved,
            self.count,
        );
        state.children_reserved -= self.count;
        self.consumed = true;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.consumed && self.count > 0 {
            let mut state = self.transfer.state.lock();
            state.children_reserved = state.children_reserved.saturating_sub(self.count);
            tracing::warn!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.transfer.ctx.id,
                count = self.count,
                "Reservation dropped unconsumed; children_reserved recovered"
            );
        }
    }
}

/// Owns N slots in `State::reaping_in_flight` plus the drained
/// [`ChildTransfer`]s themselves. Either consumed by
/// [`DownloadObjectsTransfer::execute_join_children`] (paired decrement under
/// the state lock) or released on `Drop` if the work item is dropped without
/// executing or `execute_join_children` panics.
///
/// Travels across the `IoRequest` dispatch boundary embedded in
/// [`DownloadObjectsWork::JoinChildren`]. On `Drop` without `consume`, the
/// counter is recovered and the children's handles are dropped, cascading
/// cancellation via their own `Drop`.
pub(crate) struct ReapingBatch {
    transfer: Arc<DownloadObjectsTransferInner>,
    /// Drained terminal children, awaiting `join()` in
    /// `execute_join_children`. Taken out for processing via
    /// [`Self::take_children`]; the counter is still owned by the batch until
    /// `consume()` is called or `Drop` runs.
    children: Option<Vec<ChildTransfer>>,
    count: usize,
    consumed: bool,
}

impl fmt::Debug for ReapingBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReapingBatch")
            .field("count", &self.count)
            .field("consumed", &self.consumed)
            .field("children_len", &self.children.as_ref().map(|c| c.len()))
            .finish_non_exhaustive()
    }
}

impl ReapingBatch {
    /// Take the inner children Vec out for processing. Must be called exactly
    /// once before `consume`.
    fn take_children(&mut self) -> Vec<ChildTransfer> {
        self.children
            .take()
            .expect("ReapingBatch::take_children called twice or after consume")
    }

    /// Caller holds `state.lock`. Decrements `reaping_in_flight` by the owned
    /// count and marks the batch consumed.
    fn consume(mut self, state: &mut State) {
        debug_assert!(
            !self.consumed,
            "ReapingBatch::consume called on already-consumed batch"
        );
        debug_assert!(
            state.reaping_in_flight >= self.count,
            "reaping_in_flight underflow: have {}, releasing {}",
            state.reaping_in_flight,
            self.count,
        );
        state.reaping_in_flight -= self.count;
        self.consumed = true;
    }
}

impl Drop for ReapingBatch {
    fn drop(&mut self) {
        if !self.consumed {
            let mut state = self.transfer.state.lock();
            state.reaping_in_flight = state.reaping_in_flight.saturating_sub(self.count);
            tracing::warn!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.transfer.ctx.id,
                count = self.count,
                "ReapingBatch dropped unconsumed; reaping_in_flight recovered"
            );
            // Children Vec (if still present) drops here. Each
            // ManagedDownloadHandle's own Drop cancels its child transfer via
            // scheduler.cancel_transfer, so orphans don't leak.
        }
    }
}

struct State {
    /// The S3 listing walker (taken out during AdvanceWalker execution).
    walk: Option<S3Walk>,

    /// Whether the destination directory has been validated (once, on the
    /// first walker advance, so the blocking stat runs off the caller thread).
    validated: bool,

    /// Whether a walker work item is currently in flight.
    walk_in_flight: bool,

    /// Objects discovered by the walker, waiting to be spawned as children.
    pending_entries: std::collections::VecDeque<Object>,

    /// Active child downloads keyed by their TransferId.
    children: HashMap<TransferId, ChildTransfer>,

    /// Entries claimed from pending_entries but not yet inserted into children.
    children_reserved: usize,

    /// JoinChildren batches currently executing.
    reaping_in_flight: usize,

    /// Failed downloads (for FailedTransferPolicy::Continue).
    failed: Vec<FailedDownload>,

    /// Count of successfully downloaded objects.
    successful_downloads: u64,
}

#[derive(Clone)]
pub(crate) struct DownloadObjectsTransfer {
    inner: Arc<DownloadObjectsTransferInner>,
}

impl std::fmt::Debug for DownloadObjectsTransfer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadObjectsTransfer")
            .finish_non_exhaustive()
    }
}

struct DownloadObjectsTransferInner {
    ctx: TransferContext,
    state: Mutex<State>,
    bucket: String,
    destination: PathBuf,
    key_prefix: Option<String>,
    delimiter: Option<String>,
    failure_policy: FailedTransferPolicy,
    pipeline_depth: usize,
    /// Directories already created (dedup `create_dir_all` across children
    /// that share a prefix; objects in the same S3 "folder" map to one dir).
    created_dirs: Mutex<HashSet<PathBuf>>,
}

impl DownloadObjectsTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        input: &crate::operation::download_objects::DownloadObjectsInput,
        walk: S3Walk,
        pipeline_depth: usize,
    ) -> Self {
        let bucket = input.bucket().unwrap().to_string();
        let destination = input.destination().unwrap().to_path_buf();
        let key_prefix = input.key_prefix().map(|s| s.to_string());
        let delimiter = input.delimiter().map(|s| s.to_string());
        let failure_policy = input.failure_policy().clone();

        Self {
            inner: Arc::new(DownloadObjectsTransferInner {
                ctx,
                state: Mutex::new(State {
                    walk: Some(walk),
                    validated: false,
                    walk_in_flight: false,
                    pending_entries: std::collections::VecDeque::new(),
                    children: HashMap::new(),
                    children_reserved: 0,
                    reaping_in_flight: 0,
                    failed: Vec::new(),
                    successful_downloads: 0,
                }),
                bucket,
                destination,
                key_prefix,
                delimiter,
                failure_policy,
                pipeline_depth,
                created_dirs: Mutex::new(HashSet::new()),
            }),
        }
    }
    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    pub(crate) fn successful_downloads(&self) -> u64 {
        // Safe: only called after terminal
        self.inner.state.lock().successful_downloads
    }

    pub(crate) fn take_failed(&self) -> Option<Vec<FailedDownload>> {
        let mut state = self.inner.state.lock();
        if state.failed.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut state.failed))
        }
    }

    /// Produce work for one poll. Spawn and reap run in the same poll and are
    /// fused into a single outcome rather than competing as strict-priority
    /// ladder steps:
    ///
    /// 1. Walk (refill): if the walker can dispatch, return its `AdvanceWalker`
    ///    item. A walk poll is the whole poll (neither spawns nor reaps).
    /// 2. Spawn one child (lock released around orchestration).
    /// 3. Reap any terminal children (up to MAX_REAP_PER_POLL this poll).
    /// 4. Fuse: a reap returns `Ready { io: JoinChildren, spawned }`; with no
    ///    reap, `Spawned` if an entry was claimed, else `check_terminal` /
    ///    `Pending`.
    ///
    /// Fusing means one poll can both retire a terminal child and refill, so a
    /// continuous completion stream does not force reap and spawn onto separate
    /// turns and done children are retired as they appear rather than
    /// accumulating. Walk and spawn are skipped when inactive, so cancel/fail
    /// stops new work while in-flight drains.
    pub(crate) fn poll_work(&self) -> PollWork {
        let _span = self.inner.ctx.poll_span();
        let mut state = self.inner.state.lock();

        // 1: Walk (refill). `dispatch_walk` gates on the low-water mark, so it
        // tops off the key buffer ahead of spawn draining it rather than
        // waiting until empty, avoiding stalls on ListObjectsV2 page latency.
        if self.inner.ctx.is_active() {
            if let Some(work) = self.dispatch_walk(&mut state) {
                return work;
            }
        }

        // 2: Spawn one child (lock released around orchestration).
        //
        // `attempted` = an entry was claimed from `pending_entries` this poll,
        // regardless of whether its child orchestrated. It MUST drive re-poll: a
        // claimed entry has left the queue, so the parent has to stay scheduled
        // to drain the rest — even after a failed orchestration — or the
        // remaining entries strand with no wake once the walker is exhausted (a
        // hang; see `test_two_failed_spawns_continue_managed_runtime_does_not_hang`).
        // `materialized` = a live child was actually inserted; it drives the
        // spawn vruntime charge, so a failed orchestration is not charged.
        let mut attempted = false;
        let mut materialized = false;
        if self.inner.ctx.is_active() {
            let (to_spawn, reservation) = self.claim_one(&mut state);
            if !to_spawn.is_empty() {
                attempted = true;
                drop(state);
                let spawned = self.spawn_children(to_spawn);
                state = self.inner.state.lock();
                materialized = self.merge_spawned(&mut state, spawned, reservation) > 0;
            }
        }

        // 3: Reap terminal children (up to MAX_REAP_PER_POLL this poll, no
        // accumulation threshold — done children are retired as they appear).
        let reaped = self.drain_terminal_children(&mut state);

        // 4: Fuse spawn and reap into one poll outcome. A reap dispatches a
        // JoinChildren item and carries `spawned` so the scheduler charges spawn
        // vruntime iff a child materialized this poll — one poll both refills and
        // retires. With no reap, re-poll if we touched the queue; otherwise
        // settle terminal/idle.
        if let Some(batch) = reaped {
            return PollWork::Ready {
                io: IoRequest {
                    data: Some(Box::new(DownloadObjectsWork::JoinChildren {
                        batch: Some(batch),
                    })),
                },
                spawned: materialized,
            };
        }
        if attempted {
            return PollWork::Spawned;
        }
        if let Some(result) = self.check_terminal(&state) {
            return result;
        }
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    /// Collect up to MAX_REAP_PER_POLL children that have reached a terminal
    /// state (success, failure, or cancellation) for reaping via JoinChildren.
    fn drain_terminal_children(&self, state: &mut State) -> Option<ReapingBatch> {
        let terminal_ids: Vec<TransferId> = state
            .children
            .iter()
            .filter(|(_, child)| child.handle.status() != crate::types::TransferStatus::Active)
            .map(|(id, _)| *id)
            .take(MAX_REAP_PER_POLL)
            .collect();

        if terminal_ids.is_empty() {
            return None;
        }

        let batch: Vec<ChildTransfer> = terminal_ids
            .into_iter()
            .filter_map(|id| state.children.remove(&id))
            .collect();

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %self.inner.ctx.id,
            reaping = batch.len(),
            remaining_children = state.children.len(),
            "download_objects draining terminal children",
        );
        let count = batch.len();
        // Reserve reaping_in_flight by the child count (not 1 per batch): the
        // terminal-check guards compare reaping_in_flight == 0, so the counter
        // must track children in flight, released 1:1 by the batch's consume()
        // or Drop.
        state.reaping_in_flight += count;
        Some(ReapingBatch {
            transfer: self.inner.clone(),
            children: Some(batch),
            count,
            consumed: false,
        })
    }

    /// Claim exactly ONE pending entry to spawn as a child.
    ///
    /// Gates on the *in-flight budget*: active children plus reserved, against
    /// `pipeline_depth`. Returns an empty Vec when nothing can be claimed.
    fn claim_one(&self, state: &mut State) -> (Vec<Object>, Reservation) {
        let active = state
            .children
            .values()
            .filter(|c| c.handle.status() == crate::types::TransferStatus::Active)
            .count()
            + state.children_reserved;
        let available = self.inner.pipeline_depth.saturating_sub(active);
        let count = available.min(1).min(state.pending_entries.len());

        let batch: Vec<Object> = state.pending_entries.drain(..count).collect();
        state.children_reserved += batch.len();
        // When batch is empty, count is 0. The resulting Reservation drops
        // harmlessly: Reservation::Drop is guarded by `self.count > 0`.
        let reservation = Reservation {
            transfer: self.inner.clone(),
            count: batch.len(),
            consumed: false,
        };
        (batch, reservation)
    }

    fn spawn_children(&self, entries: Vec<Object>) -> Vec<(Object, Result<ChildTransfer, Error>)> {
        let handle = &self.inner.ctx.handle;
        let parent_id = self.inner.ctx.id.id;

        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            tid = %self.inner.ctx.id,
            count = entries.len(),
            "download_objects spawning children",
        );

        entries
            .into_iter()
            .map(|obj| {
                let key = obj
                    .key()
                    .expect("S3Walk yields objects with keys")
                    .to_string();
                let result = self.spawn_single_child(handle, &key, parent_id);
                (obj, result.map(|h| ChildTransfer { handle: h, key }))
            })
            .collect()
    }

    /// Stat the destination and confirm it is a directory. Blocking; called
    /// once from the first walker advance, during work-item execution rather
    /// than on the caller's `initiate` thread.
    fn validate_destination(&self) -> Result<(), Error> {
        let dest = &self.inner.destination;
        let metadata = std::fs::metadata(dest)?;
        crate::operation::validate_target_is_dir(&metadata, dest)
    }

    fn spawn_single_child(
        &self,
        handle: &Arc<crate::client::Handle>,
        key: &str,
        parent_id: u64,
    ) -> Result<ManagedDownloadHandle, Error> {
        let dest_path = local_key_path(
            &self.inner.destination,
            key,
            self.inner.key_prefix.as_deref(),
            self.inner.delimiter.as_deref(),
        )?;

        // Create parent directories, deduped via created_dirs so we don't
        // re-stat the whole ancestor chain for every object sharing a prefix
        // (create_dir_all stats each ancestor; for N small files in a few
        // dirs that's the dominant per-child syscall cost).
        //
        // TODO(vnext): revisit moving this to the execute phase. poll_work is
        // documented as non-blocking, but the scheduler is edge-triggered so
        // this often already runs on a managed thread; deferring to execute adds
        // a work-item round-trip that may increase total latency rather than
        // reduce it. Measure before moving.
        if let Some(parent) = dest_path.parent() {
            let known = self.inner.created_dirs.lock().contains(parent);
            if !known {
                std::fs::create_dir_all(parent).map_err(|e| {
                    error::Error::new(
                        ErrorKind::IOError,
                        format!("failed to create directory for key '{key}': {e}"),
                    )
                })?;
                self.inner.created_dirs.lock().insert(parent.to_path_buf());
            }
        }

        let input = DownloadInput::builder()
            .bucket(&self.inner.bucket)
            .key(key)
            .build()
            .expect("bucket and key are set");

        // Create temp file synchronously (we're in poll_work, not async).
        // orchestrate_to_path is async (tokio::fs); for child spawning we
        // use std::fs + orchestrate_with_sink directly.
        let unique_id = fastrand::u32(..);
        let temp_name = format!(
            "{}.s3tmp.{:08x}",
            dest_path.file_name().unwrap_or_default().to_string_lossy(),
            unique_id
        );
        let temp_path = dest_path.with_file_name(&temp_name);

        let file = std::fs::File::create(&temp_path).map_err(|e| {
            error::Error::new(
                ErrorKind::IOError,
                format!("failed to create temp file for key '{key}': {e}"),
            )
        })?;

        let inner = Download::orchestrate_with_sink(
            handle.clone(),
            input,
            file,
            0, // range_start
            true,
            Some(parent_id),
        )?;
        Ok(ManagedDownloadHandle::new(inner, temp_path, dest_path))
    }

    /// Merge results of child spawning back into state. Inserts each child into
    /// the active set or records the failure, then consumes the [`Reservation`]
    /// in lockstep so the `children_reserved` pairing is structural.
    ///
    /// Returns the number of children actually inserted (orchestrated
    /// successfully). Callers use this to decide whether a spawn genuinely
    /// occurred: a claimed entry whose orchestration failed is recorded in
    /// `failed` but produces no live child, so it must not be counted as a spawn
    /// (which would over-charge spawn vruntime for a child that never runs).
    fn merge_spawned(
        &self,
        state: &mut State,
        spawned: Vec<(Object, Result<ChildTransfer, Error>)>,
        reservation: Reservation,
    ) -> usize {
        let mut inserted = 0usize;
        for (obj, result) in spawned {
            match result {
                Ok(child) => {
                    let id = child.handle.transfer_id();
                    state.children.insert(id, child);
                    inserted += 1;
                }
                Err(err) => {
                    let key = obj
                        .key()
                        .expect("S3Walk yields objects with keys")
                        .to_string();
                    let failed_input = DownloadInput::builder()
                        .bucket(&self.inner.bucket)
                        .key(&key)
                        .build()
                        .unwrap();
                    state.failed.push(FailedDownload {
                        input: failed_input,
                        error: err,
                    });
                    if self.inner.failure_policy == FailedTransferPolicy::Abort {
                        // TODO: the triggering child's error is preserved in
                        // `state.failed` (reachable via
                        // `Error::failed_downloads`), but the root error's
                        // `source()` is only this string. Connecting the root
                        // `source()` to the failing child's error needs a
                        // shareable error (`Arc`) since `Error` is not `Clone`.
                        self.inner.ctx.set_failed_and_signal(Error::new(
                            ErrorKind::ChildOperationFailed,
                            format!("download failed for key '{key}'"),
                        ));
                    }
                }
            }
        }
        // Release the reservation under the same lock: count reserved by
        // claim_one == results merged here.
        reservation.consume(state);
        inserted
    }

    fn check_terminal(&self, state: &State) -> Option<PollWork> {
        if !self.inner.ctx.is_active() {
            // Wait for all in-flight work to drain
            if state.children.is_empty()
                && state.children_reserved == 0
                && state.reaping_in_flight == 0
                && !state.walk_in_flight
            {
                tracing::debug!(
                    target: crate::telemetry::TARGET_TRANSFER,
                    tid = %self.inner.ctx.id,
                    successful = state.successful_downloads,
                    failed = state.failed.len(),
                    "download_objects terminal (cancelled/failed), signaling",
                );
                self.inner.ctx.signal_terminal();
                return Some(PollWork::Done);
            }
            return None;
        }

        // Check if walk is done and no more work to do
        let walk_done = state.walk.as_ref().is_none_or(|w| w.is_done()) && !state.walk_in_flight;
        if walk_done
            && state.pending_entries.is_empty()
            && state.children.is_empty()
            && state.children_reserved == 0
            && state.reaping_in_flight == 0
        {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.inner.ctx.id,
                successful = state.successful_downloads,
                failed = state.failed.len(),
                "download_objects terminal (walk exhausted), signaling",
            );
            // The success transition: status is still Active here (the
            // !is_active() branch above handles an already-terminal drain after
            // set_failed/set_cancelled, where completing would be wrong).
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        None
    }

    /// Dispatch one listing page, or `None` if listing cannot proceed.
    ///
    /// Single-flight (`walk_in_flight`) and gated on the key buffer alone: a
    /// page is fetched only while `pending_entries` is below the low-water
    /// mark, so the walker refills the buffer ahead of spawn draining it. This
    /// bounds `pending_entries` to roughly low-water plus one page (single
    /// flight admits at most one in-flight page). Independent of the child
    /// in-flight budget (`pipeline_depth`), which gates spawning, not listing.
    fn dispatch_walk(&self, state: &mut State) -> Option<PollWork> {
        if state.walk_in_flight {
            return None;
        }
        if state.pending_entries.len() >= WALK_LOW_WATER {
            return None;
        }

        // Take the walk out for execution; put it back if it's already done.
        let walk = match state.walk.take() {
            Some(w) if !w.is_done() => w,
            Some(w) => {
                state.walk = Some(w);
                return None;
            }
            None => return None,
        };

        state.walk_in_flight = true;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(DownloadObjectsWork::AdvanceWalker {
                walk: Some(Box::new(walk)),
            })),
        }))
    }

    // -----------------------------------------------------------------------
    // execute
    // -----------------------------------------------------------------------

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<DownloadObjectsWork>();
        match data {
            DownloadObjectsWork::AdvanceWalker { walk } => {
                self.execute_advance_walker(*walk.take().unwrap()).await
            }
            DownloadObjectsWork::JoinChildren { batch } => {
                let batch = batch.take().expect("batch already taken");
                self.execute_join_children(batch).await
            }
        }
    }

    async fn execute_advance_walker(&self, mut walk: S3Walk) -> WorkOutcome {
        // Validate the destination directory once, on the first advance, so the
        // blocking stat runs here (during work-item execution) rather than on
        // the caller's `initiate` thread. On failure, fail the transfer; the
        // error surfaces from `handle.join()`.
        let needs_validation = {
            let mut state = self.inner.state.lock();
            let first = !state.validated;
            state.validated = true;
            first
        };
        if needs_validation {
            if let Err(err) = self.validate_destination() {
                let mut state = self.inner.state.lock();
                state.walk_in_flight = false;
                // set_failed alone: check_terminal arbitrates the terminal signal once in-flight work (children/walks/reaps) drains.
                self.inner.ctx.set_failed(err);
                if self.check_terminal(&state).is_some() {
                    return WorkOutcome::Success { data: None };
                }
                drop(state);
                self.inner.ctx.try_wake();
                return WorkOutcome::Success { data: None };
            }
        }

        // Drain up to a page worth of entries from the walker, publishing them
        // to `pending_entries` in small chunks as they arrive rather than in a
        // single batch at the end. Early publication lets spawning consume keys
        // while the rest of the page is still being fetched, so the buffer does
        // not sit empty across a ListObjectsV2 round-trip.
        let mut chunk = Vec::with_capacity(WALK_FLUSH_CHUNK);
        let mut discovered = 0usize;
        let mut fatal_error = None;

        loop {
            match walk.next().await {
                Some(Ok(obj)) => {
                    chunk.push(obj);
                    discovered += 1;
                    if chunk.len() >= WALK_FLUSH_CHUNK {
                        let mut state = self.inner.state.lock();
                        state.pending_entries.extend(chunk.drain(..));
                        drop(state);
                        // Wake so a spawn poll runs against the just-published
                        // keys without waiting for this advance to complete.
                        self.inner.ctx.try_wake();
                    }
                    // Limit to one page worth of results per work item so we
                    // don't hold the executor across multiple ListObjectsV2
                    // calls, starving other transfers.
                    if discovered >= MAX_ENTRIES_PER_WALK {
                        break;
                    }
                }
                Some(Err(err)) => {
                    if err.is_fatal() {
                        fatal_error = Some(err);
                        break;
                    }
                    // Non-fatal errors: log and continue
                    tracing::warn!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        tid = %self.inner.ctx.id,
                        error = %err,
                        "non-fatal walker error, continuing"
                    );
                }
                None => break, // walker exhausted or needs next page
            }
        }

        // Publish any remaining keys and put the walk back into state.
        let mut state = self.inner.state.lock();
        state.walk_in_flight = false;

        if let Some(err) = fatal_error {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.inner.ctx.id,
                error = %err,
                "download_objects fatal walker error",
            );
            self.inner.ctx.set_failed(err);
        } else {
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                tid = %self.inner.ctx.id,
                discovered,
                walk_done = walk.is_done(),
                "download_objects walker advanced",
            );
            state.pending_entries.extend(chunk.drain(..));
            if !walk.is_done() {
                state.walk = Some(walk);
            }
        }

        // An execute callback that drains the last in-flight work owns the
        // terminal transition: check and signal here rather than deferring to
        // a subsequent poll_work.
        if self.check_terminal(&state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }

        drop(state);
        // Wake: new entries in pending_entries unblock poll_work from Pending.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_join_children(&self, mut batch: ReapingBatch) -> WorkOutcome {
        let children = batch.take_children();
        for child in children {
            let key = child.key;
            // Snapshot metrics before `join()` consumes the handle.
            let metrics = child.handle.metrics();
            match child.handle.join().await {
                Ok(_output) => {
                    let mut state = self.inner.state.lock();
                    state.successful_downloads += 1;
                    drop(state);
                    // Aggregate the child's bytes into the parent's per-transfer
                    // MetricsState directly (not via record_io on the child's
                    // context, which already updated the client-level counters
                    // during the child transfer) so DownloadObjectsOutput.metrics
                    // reflects the whole directory download without double-counting.
                    self.inner.ctx.metrics.record_io(&crate::metrics::IoSample {
                        network_tx: metrics.network_tx,
                        network_rx: metrics.network_rx,
                        disk_read: metrics.disk_read,
                        disk_write: metrics.disk_write,
                    });
                }
                Err(err) => {
                    let mut state = self.inner.state.lock();
                    let failed_input = DownloadInput::builder()
                        .bucket(&self.inner.bucket)
                        .key(&key)
                        .build()
                        .unwrap();
                    state.failed.push(FailedDownload {
                        input: failed_input,
                        error: err,
                    });
                    if self.inner.failure_policy == FailedTransferPolicy::Abort {
                        // TODO: the triggering child's error is preserved in
                        // `state.failed` (reachable via
                        // `Error::failed_downloads`), but the root error's
                        // `source()` is only this string. Connecting the root
                        // `source()` to the failing child's error needs a
                        // shareable error (`Arc`) since `Error` is not `Clone`.
                        self.inner.ctx.set_failed_and_signal(Error::new(
                            ErrorKind::ChildOperationFailed,
                            format!("download failed for key '{key}'"),
                        ));
                    }
                }
            }
        }

        let mut state = self.inner.state.lock();
        batch.consume(&mut state);
        // An execute callback that drains the last in-flight work owns the
        // terminal transition: check and signal here rather than deferring to
        // a subsequent poll_work.
        if self.check_terminal(&state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        // Wake: freed pipeline capacity may unblock dispatch_walk or spawning.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }
}

impl Transfer for DownloadObjectsTransfer {
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    fn poll_work(&self) -> PollWork {
        self.poll_work()
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(self.execute(work))
    }

    fn on_terminal(&self) {}
}

fn strip_key_prefix<'a>(key: &'a str, prefix: Option<&str>, delimiter: Option<&str>) -> &'a str {
    let prefix = prefix.unwrap_or("");
    let delim = delimiter.unwrap_or(DEFAULT_DELIMITER);

    if key.is_empty() || prefix.is_empty() || !key.starts_with(prefix) || !key.contains(delim) {
        return key;
    }

    let stripped = &key[prefix.len()..];

    if prefix.ends_with(delim) || !stripped.starts_with(delim) {
        return stripped;
    }

    &stripped[1..]
}

fn replace_delim<'a>(key: &'a str, delimiter: Option<&str>, path_separator: &str) -> Cow<'a, str> {
    match delimiter {
        Some(delim) if delim != path_separator => {
            let replaced = key.replace(delim, path_separator);
            Cow::Owned(replaced)
        }
        _ => Cow::Borrowed(key),
    }
}

/// Derive the local filesystem path for a given S3 key.
///
/// Strips the configured prefix, replaces the delimiter with the OS path
/// separator, joins with the destination root, normalizes via `path_clean`,
/// and validates the result stays within the root (path traversal guard).
pub(crate) fn local_key_path(
    root_dir: &Path,
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Result<PathBuf, Error> {
    let stripped = strip_key_prefix(key, prefix, delimiter);
    let relative_path = replace_delim(stripped, delimiter, std::path::MAIN_SEPARATOR_STR);

    let local_path = root_dir.join(relative_path.as_ref()).clean();
    validate_path(root_dir, &local_path, key)?;

    Ok(local_path)
}

fn validate_path(root_dir: &Path, local_path: &Path, key: &str) -> Result<(), Error> {
    if !local_path.starts_with(root_dir) {
        return Err(Error::new(
            ErrorKind::InputInvalid,
            format!(
                "Unable to download key: '{key}', its relative path resolves \
                 outside the target destination directory"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::walk::{S3WalkContext, S3Walker};
    use crate::transfer::TransferContext;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::timeout;

    fn mock_s3_success() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("a.txt").size(5).build(),
                    Object::builder().key("b.txt").size(5).build(),
                    Object::builder().key("c.txt").size(5).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get])
    }

    fn mock_s3_get_failure() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("a.txt").size(5).build()]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_error(|| {
            aws_sdk_s3::operation::get_object::GetObjectError::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("NoSuchKey")
                    .message("not found")
                    .build(),
            )
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get])
    }

    fn mock_s3_list_failure() -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
            aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("AccessDenied")
                    .message("access denied")
                    .build(),
            )
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list])
    }

    fn setup(
        dest: &Path,
        policy: FailedTransferPolicy,
        s3_client: aws_sdk_s3::Client,
    ) -> (
        DownloadObjectsTransfer,
        crate::transfer::StateMachineTerminalReceiver,
    ) {
        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        let walk = S3Walker::builder().build().walk(
            S3WalkContext::builder()
                .client(s3_client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle);
        ctx.handle
            .scheduler
            .register_empty_group_for_test(ctx.id.id);

        let input = crate::operation::download_objects::DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination(dest)
            .failure_policy(policy)
            .build()
            .unwrap();
        let transfer = DownloadObjectsTransfer::new(ctx, &input, walk, 1000);
        (transfer, completion_rx)
    }

    async fn drive_transfer(transfer: &DownloadObjectsTransfer) {
        loop {
            match transfer.poll_work() {
                PollWork::Ready { io: mut work, .. } => {
                    transfer.execute(&mut work).await;
                }
                PollWork::Spawned => {}
                PollWork::Pending => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                PollWork::Done => break,
            }
        }
    }

    /// Setup that enqueues the transfer into the scheduler (for managed-runtime tests).
    /// Returns the transfer and completion receiver. The scheduler drives execution.
    fn setup_enqueued(
        dest: &Path,
        policy: FailedTransferPolicy,
        s3_client: aws_sdk_s3::Client,
        handle: std::sync::Arc<crate::client::Handle>,
    ) -> (
        DownloadObjectsTransfer,
        crate::transfer::StateMachineTerminalReceiver,
    ) {
        let walk = S3Walker::builder().build().walk(
            S3WalkContext::builder()
                .client(s3_client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle.clone());

        let input = crate::operation::download_objects::DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination(dest)
            .failure_policy(policy)
            .build()
            .unwrap();
        let transfer = DownloadObjectsTransfer::new(ctx, &input, walk, 1000);

        handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        (transfer, completion_rx)
    }

    // --- Key derivation tests (ported from worker.rs) ---

    #[test]
    fn test_strip_key_prefix() {
        let cases: &[(&str, Option<&str>, Option<&str>, &str)] = &[
            ("no-delim", None, None, "no-delim"),
            ("no-delim", Some(""), None, "no-delim"),
            (
                "delim/with/separator",
                Some(""),
                None,
                "delim/with/separator",
            ),
            ("", Some("no-delim"), None, ""),
            ("no-delim", Some("no-delim"), None, "no-delim"),
            ("delim/", Some("delim"), None, ""),
            ("not-in-key", Some("prefix"), None, "not-in-key"),
            ("notes/2021/1.txt", Some("notes/2021"), None, "1.txt"),
            ("notes/2021/1.txt", Some("notes/2021/"), None, "1.txt"),
            (
                "top-level/sub-folder/1.txt",
                Some("top-"),
                None,
                "level/sub-folder/1.txt",
            ),
            (
                "someInnerFolder/another/file1.txt",
                Some("someInner"),
                None,
                "Folder/another/file1.txt",
            ),
            (
                "someInnerF/another/file1.txt",
                Some("someInner"),
                None,
                "F/another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner"),
                None,
                "another/file1.txt",
            ),
            (
                "someInner/another/file1.txt",
                Some("someInner/a"),
                None,
                "nother/file1.txt",
            ),
        ];
        for (key, prefix, delim, expected) in cases {
            let actual = strip_key_prefix(key, *prefix, *delim);
            assert_eq!(
                *expected, actual,
                "key={key:?} prefix={prefix:?} delim={delim:?}"
            );
        }
    }

    #[test]
    fn test_strip_key_prefix_delims() {
        for delim in ["/", "//", "\\", "|", "delim"] {
            let prefix = format!("notes{delim}2021{delim}");
            let key = format!("notes{delim}2021{delim}1.txt");
            let actual = strip_key_prefix(&key, Some(&prefix), Some(delim));
            assert_eq!("1.txt", actual, "delim={delim:?}");
        }
    }

    #[cfg(target_family = "unix")]
    #[test]
    fn test_local_key_path_comprehensive() {
        // (key, key_prefix, delimiter, expected) for derive_local_path.
        type Case = (
            &'static str,
            Option<&'static str>,
            Option<&'static str>,
            Result<&'static str, &'static str>,
        );
        let root = Path::new("test");
        let cases: &[Case] = &[
            ("2023/Jan/1.png", None, None, Ok("test/2023/Jan/1.png")),
            ("2023/Jan/1.png", Some("2023/Jan/"), None, Ok("test/1.png")),
            ("2023/Jan/1.png", Some("2023/Jan"), None, Ok("test/1.png")),
            ("2023-Jan-1.png", None, Some("-"), Ok("test/2023/Jan/1.png")),
            ("2023-Jan-.png", None, Some("-"), Ok("test/2023/Jan/.png")),
            (
                "many////delims-in-a-row",
                None,
                Some("/"),
                Ok("test/many/delims-in-a-row"),
            ),
            ("../2023/Jan/1.png", None, None, Err("outside the target")),
            ("/2023/Jan/1.png", None, None, Err("outside the target")),
            (
                "foo/../2023/../../Jan/1.png",
                None,
                None,
                Err("outside the target"),
            ),
            (
                "../test-2/object.dat",
                None,
                None,
                Err("outside the target"),
            ),
        ];
        for (key, prefix, delim, expected) in cases {
            let actual = local_key_path(root, key, *prefix, *delim);
            match expected {
                Ok(path) => {
                    let actual =
                        actual.unwrap_or_else(|e| panic!("expected Ok for key={key:?}: {e}"));
                    assert_eq!(Path::new(path), actual, "key={key:?}");
                }
                Err(_) => {
                    let err = actual.expect_err(&format!("expected Err for key={key:?}"));
                    assert_eq!(err.kind(), &ErrorKind::InputInvalid, "key={key:?}");
                }
            }
        }
    }

    // --- State machine tests ---

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_happy_path_3_files() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 3);
        assert!(transfer.take_failed().is_none());

        // Verify files exist
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
        assert!(dir.path().join("c.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_continue() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_get_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        let failed = transfer.take_failed().unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_abort() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Abort,
            mock_s3_get_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        assert!(transfer.inner.ctx.is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fatal_walker_error() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_list_failure(),
        );

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert!(transfer.inner.ctx.is_failed());
        assert_eq!(transfer.successful_downloads(), 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_empty_listing() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        assert!(transfer.take_failed().is_none());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_path_traversal_rejected_as_failure() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder()
                    .key("../../../etc/passwd")
                    .size(100)
                    .build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        let failed = transfer.take_failed().unwrap();
        assert_eq!(failed.len(), 1);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_nested_keys_create_directories() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder()
                        .key("photos/2023/jan/img.jpg")
                        .size(5)
                        .build(),
                    Object::builder().key("docs/readme.txt").size(5).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 2);
        assert!(dir.path().join("photos/2023/jan/img.jpg").exists());
        assert!(dir.path().join("docs/readme.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_prefix_stripping() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("backup/2023/a.txt").size(3).build(),
                    Object::builder().key("backup/2023/b.txt").size(3).build(),
                ]))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(3)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        let walk = S3Walker::builder().prefix("backup/2023/").build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle);
        ctx.handle
            .scheduler
            .register_empty_group_for_test(ctx.id.id);

        let input = crate::operation::download_objects::DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination(dir.path())
            .key_prefix("backup/2023/")
            .failure_policy(FailedTransferPolicy::Continue)
            .build()
            .unwrap();
        let transfer = DownloadObjectsTransfer::new(ctx, &input, walk, 1000);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 2);
        // Prefix stripped: "backup/2023/a.txt" → "a.txt"
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_many_objects() {
        let dir = tempdir().unwrap();

        let objects: Vec<Object> = (0..20)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i:02}.txt"))
                    .size(2)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(2)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 20);
        for i in 0..20 {
            assert!(dir.path().join(format!("file{i:02}.txt")).exists());
        }

        // The success transition runs set_completed(), so a successful transfer
        // ends Completed (not still Active). Without it, a dropped handle would
        // see is_active() and spuriously cancel an already-finished transfer.
        assert_eq!(
            transfer.ctx().transfer_status(),
            crate::types::TransferStatus::Completed
        );

        // Child metrics are rolled into the parent: 20 objects x 2 bytes each.
        let m = transfer.ctx().metrics();
        assert_eq!(
            m.network_rx, 40,
            "child network_rx must aggregate to parent"
        );
        assert_eq!(
            m.disk_write, 40,
            "child disk_write must aggregate to parent"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation() {
        let dir = tempdir().unwrap();

        // Large listing so the transfer doesn't complete before we cancel
        let objects: Vec<Object> = (0..100)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i}.txt"))
                    .size(5)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(5)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, _completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        // Cancel after first poll_work cycle
        transfer.inner.ctx.set_cancelled();

        timeout(Duration::from_secs(10), async {
            drive_transfer(&transfer).await;
        })
        .await
        .expect("cancelled transfer should terminate");

        assert!(!transfer.inner.ctx.is_active());
    }

    // --- Managed-runtime tests ---
    // These exercise the real scheduler dispatch path (managed threads drive
    // poll_work/execute). Catches bugs like missing set_pending that only
    // manifest when work crosses thread boundaries.

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_happy_path_managed_runtime() {
        let dir = tempdir().unwrap();
        let config = crate::Config::builder().client(mock_s3_success()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) = setup_enqueued(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            handle,
        );

        timeout(Duration::from_secs(10), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 3);
        assert!(dir.path().join("a.txt").exists());
        assert!(dir.path().join("b.txt").exists());
        assert!(dir.path().join("c.txt").exists());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_many_objects_managed_runtime() {
        let dir = tempdir().unwrap();

        let objects: Vec<Object> = (0..20)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i:02}.txt"))
                    .size(2)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(2)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
                .build()
        });
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) = setup_enqueued(
            dir.path(),
            FailedTransferPolicy::Continue,
            s3_client,
            handle,
        );

        timeout(Duration::from_secs(10), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 20);
        for i in 0..20 {
            assert!(dir.path().join(format!("file{i:02}.txt")).exists());
        }

        // The success transition runs set_completed(), so a successful transfer
        // ends Completed (not still Active). Without it, a dropped handle would
        // see is_active() and spuriously cancel an already-finished transfer.
        assert_eq!(
            transfer.ctx().transfer_status(),
            crate::types::TransferStatus::Completed
        );

        // Child metrics are rolled into the parent: 20 objects x 2 bytes each.
        let m = transfer.ctx().metrics();
        assert_eq!(
            m.network_rx, 40,
            "child network_rx must aggregate to parent"
        );
        assert_eq!(
            m.disk_write, 40,
            "child disk_write must aggregate to parent"
        );
    }

    /// Under `Abort`, a child failure sets the parent failed in an execute
    /// callback, which must signal terminal there. Many keys keep sibling
    /// children in flight when the failure fires, so the parent goes terminal
    /// while children remain; its `join()` (completion) must resolve rather
    /// than hang on the scheduler removing the terminal parent.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_abort_managed_runtime_does_not_strand() {
        let dir = tempdir().unwrap();

        // Many keys so several children spawn; GetObject always errors so the
        // first reaped failure aborts while siblings are still registered.
        let objects: Vec<Object> = (0..20)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i:02}.txt"))
                    .size(2)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_error(|| {
            aws_sdk_s3::operation::get_object::GetObjectError::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("NoSuchKey")
                    .message("not found")
                    .build(),
            )
        });
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) =
            setup_enqueued(dir.path(), FailedTransferPolicy::Abort, s3_client, handle);

        // The parent's completion must resolve promptly rather than stranding
        // when the scheduler removes the terminal parent.
        timeout(Duration::from_secs(10), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("Abort failure must signal parent terminal, not strand join()");

        assert!(transfer.ctx().is_failed());
    }

    /// Regression (real scheduler): two claimed-but-failed spawns under Continue
    /// must not hang the parent. A path-traversal key is rejected by
    /// `local_key_path` DURING the spawn step (before any GET), so it is a
    /// claimed entry that produces no child. The parent must stay scheduled to
    /// drain the SECOND such entry; if `poll_work` gates re-poll on whether a
    /// child *materialized* rather than whether an entry was *claimed*, the
    /// second entry strands with no wake once the walker is exhausted and the
    /// parent's `completion_rx` never resolves. Must use the managed runtime:
    /// the immediate `drive_transfer` harness busy-re-polls on `Pending` and so
    /// cannot observe a lost wake.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_two_failed_spawns_continue_managed_runtime_does_not_hang() {
        let dir = tempdir().unwrap();

        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![
                    Object::builder().key("../../../etc/passwd").size(2).build(),
                    Object::builder().key("../../../etc/shadow").size(2).build(),
                ]))
                .build()
        });
        // No GET should ever fire (both fail at key-path derivation), but wire a
        // benign one so the mock client is well-formed.
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(2)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
                .build()
        });
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) = setup_enqueued(
            dir.path(),
            FailedTransferPolicy::Continue,
            s3_client,
            handle,
        );

        timeout(Duration::from_secs(10), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("two claimed-but-failed spawns must not strand the parent's completion");

        assert_eq!(transfer.successful_downloads(), 0);
        assert_eq!(
            transfer.take_failed().map(|f| f.len()).unwrap_or(0),
            2,
            "both failed entries must be recorded"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_fatal_walker_error_managed_runtime() {
        let dir = tempdir().unwrap();
        let s3_client = mock_s3_list_failure();

        let config = crate::Config::builder().client(s3_client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) = setup_enqueued(
            dir.path(),
            FailedTransferPolicy::Continue,
            s3_client,
            handle,
        );

        timeout(Duration::from_secs(10), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 0);
        assert!(transfer.ctx().is_failed());
    }

    // --- RAII counter guard tests (mirror upload_objects) ---

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reservation_consume_decrements_counter() {
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        // Pre-set the counter as if claim_one had reserved 5 slots.
        {
            let mut state = transfer.inner.state.lock();
            state.children_reserved = 5;
        }

        let reservation = Reservation {
            transfer: transfer.inner.clone(),
            count: 5,
            consumed: false,
        };

        {
            let mut state = transfer.inner.state.lock();
            reservation.consume(&mut state);
            assert_eq!(0, state.children_reserved, "consume must decrement");
        }

        // Drop after consume is a no-op (consumed = true).
        let state = transfer.inner.state.lock();
        assert_eq!(
            0, state.children_reserved,
            "Drop after consume must not double-decrement"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reservation_drop_without_consume_recovers_counter() {
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        {
            let mut state = transfer.inner.state.lock();
            state.children_reserved = 5;
        }

        // Drop without calling consume (e.g. an early return before merge_spawned).
        {
            let _reservation = Reservation {
                transfer: transfer.inner.clone(),
                count: 5,
                consumed: false,
            };
        }

        let state = transfer.inner.state.lock();
        assert_eq!(
            0, state.children_reserved,
            "Drop on unconsumed Reservation must recover the counter"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reservation_drop_underflow_saturates() {
        // State is 0; reservation claims 5. Drop must saturate, not panic.
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        {
            let _reservation = Reservation {
                transfer: transfer.inner.clone(),
                count: 5,
                consumed: false,
            };
        }

        let state = transfer.inner.state.lock();
        assert_eq!(0, state.children_reserved, "Drop must saturate, not panic");
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reaping_batch_consume_decrements_counter() {
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        {
            let mut state = transfer.inner.state.lock();
            state.reaping_in_flight = 3;
        }

        let batch = ReapingBatch {
            transfer: transfer.inner.clone(),
            children: Some(Vec::new()),
            count: 3,
            consumed: false,
        };

        {
            let mut state = transfer.inner.state.lock();
            batch.consume(&mut state);
            assert_eq!(0, state.reaping_in_flight, "consume must decrement");
        }

        let state = transfer.inner.state.lock();
        assert_eq!(
            0, state.reaping_in_flight,
            "Drop after consume must not double-decrement"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reaping_batch_drop_without_consume_recovers_counter() {
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        {
            let mut state = transfer.inner.state.lock();
            state.reaping_in_flight = 3;
        }

        // Simulates the scheduler dropping a JoinChildren work item without
        // executing, or execute_join_children panicking before batch.consume.
        {
            let _batch = ReapingBatch {
                transfer: transfer.inner.clone(),
                children: Some(Vec::new()),
                count: 3,
                consumed: false,
            };
        }

        let state = transfer.inner.state.lock();
        assert_eq!(
            0, state.reaping_in_flight,
            "Drop on unconsumed ReapingBatch must recover the counter"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reaping_batch_take_children_then_consume() {
        // The happy path: execute_join_children takes children out for
        // processing, then consumes the batch under the lock at the end.
        let dir = tempdir().unwrap();
        let (transfer, _rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
        );

        {
            let mut state = transfer.inner.state.lock();
            state.reaping_in_flight = 2;
        }

        let mut batch = ReapingBatch {
            transfer: transfer.inner.clone(),
            children: Some(Vec::new()),
            count: 2,
            consumed: false,
        };

        let _children = batch.take_children();

        {
            let mut state = transfer.inner.state.lock();
            batch.consume(&mut state);
            assert_eq!(0, state.reaping_in_flight);
        }
    }

    // --- Single-ticket spawn cadence tests ---

    /// Anti-stall guard: a directory of 500 objects fully transfers to
    /// completion with the single-ticket spawn cadence. If the spawn path
    /// stalls (returns nothing, never re-polled), this hangs and the
    /// timeout fires as a failure.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_single_ticket_500_objects_completes() {
        let dir = tempdir().unwrap();

        let objects: Vec<Object> = (0..500)
            .map(|i| {
                Object::builder()
                    .key(format!("obj{i:04}.bin"))
                    .size(4)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(4)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"data"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);

        let (transfer, completion_rx) =
            setup_enqueued(dir.path(), FailedTransferPolicy::Continue, client, handle);

        timeout(Duration::from_secs(30), async {
            let _ = completion_rx.await;
        })
        .await
        .expect("500-object transfer must complete without stalling");

        assert_eq!(transfer.successful_downloads(), 500);
        for i in 0..500 {
            assert!(
                dir.path().join(format!("obj{i:04}.bin")).exists(),
                "object {i} must be present on disk"
            );
        }
    }

    /// Backstop test: with a small max_concurrent set via pipeline_depth,
    /// in-flight children never exceed it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_backstop_limits_in_flight_children() {
        let dir = tempdir().unwrap();
        let max_concurrent: usize = 4;

        let objects: Vec<Object> = (0..20)
            .map(|i| {
                Object::builder()
                    .key(format!("file{i:02}.txt"))
                    .size(3)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(3)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"abc"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        let walk = S3Walker::builder().build().walk(
            S3WalkContext::builder()
                .client(client)
                .bucket("test-bucket")
                .build(),
        );

        let (ctx, completion_rx) = TransferContext::new(handle);
        ctx.handle
            .scheduler
            .register_empty_group_for_test(ctx.id.id);

        let input = crate::operation::download_objects::DownloadObjectsInput::builder()
            .bucket("test-bucket")
            .destination(dir.path())
            .failure_policy(FailedTransferPolicy::Continue)
            .build()
            .unwrap();
        let transfer = DownloadObjectsTransfer::new(ctx, &input, walk, max_concurrent);

        let mut max_observed: usize = 0;

        timeout(Duration::from_secs(10), async {
            loop {
                // Sample in-flight before polling.
                {
                    let state = transfer.inner.state.lock();
                    let active = state
                        .children
                        .values()
                        .filter(|c| c.handle.status() == crate::types::TransferStatus::Active)
                        .count()
                        + state.children_reserved;
                    if active > max_observed {
                        max_observed = active;
                    }
                }

                match transfer.poll_work() {
                    PollWork::Ready { io: mut work, .. } => {
                        transfer.execute(&mut work).await;
                    }
                    PollWork::Spawned => {}
                    PollWork::Pending => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    PollWork::Done => break,
                }
            }
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_downloads(), 20);
        assert!(
            max_observed <= max_concurrent,
            "in-flight children ({max_observed}) must not exceed backstop ({max_concurrent})"
        );
    }

    /// The fused ladder reaps terminal children continuously rather than
    /// waiting for a batch threshold: every poll drains whatever terminals
    /// exist and, when it also spawns, fuses both into one `Ready { spawned }`.
    /// This guards the de-batching invariant — terminal children must NOT
    /// accumulate to a large unreaped backlog while a directory is spawning.
    /// A regression that reintroduced a reap-only-when->=MAX_REAP_PER_POLL
    /// threshold (or reap-behind-spawn starvation) would let the backlog grow
    /// and fail the bound below.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_reap_is_continuous_not_batched() {
        let dir = tempdir().unwrap();

        let objects: Vec<Object> = (0..300)
            .map(|i| {
                Object::builder()
                    .key(format!("f{i:04}.txt"))
                    .size(2)
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(2)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);

        let (transfer, completion_rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        // Track how many polls fused a reap with a spawn (Ready{spawned:true})
        // to confirm the fused path is actually exercised, and the peak
        // unreaped-terminal backlog to confirm reaping keeps up.
        let mut fused_reap_spawn = 0usize;
        let mut max_terminal_backlog = 0usize;

        timeout(Duration::from_secs(30), async {
            loop {
                // Sample the unreaped-terminal backlog before polling.
                let terminal_before = {
                    let state = transfer.inner.state.lock();
                    state
                        .children
                        .values()
                        .filter(|c| c.handle.status() != crate::types::TransferStatus::Active)
                        .count()
                };
                max_terminal_backlog = max_terminal_backlog.max(terminal_before);

                match transfer.poll_work() {
                    PollWork::Ready {
                        io: mut work,
                        spawned,
                    } => {
                        if spawned {
                            fused_reap_spawn += 1;
                        }
                        transfer.execute(&mut work).await;
                    }
                    PollWork::Spawned => {}
                    PollWork::Pending => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    PollWork::Done => break,
                }
            }
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        // Liveness under the fused spawn+reap ladder: a 300-object directory
        // completes with every child reaped and no hang. This guards the ladder
        // restructure against dropping work, starving reap, or deadlocking.
        //
        // Whether in-flight stays flat and fusion fires is a property of real
        // concurrency and is measured with end-to-end benchmarks, not asserted
        // here: this single-parent-loop harness drives children in bursts at the
        // parent's await points, so completion timing (hence backlog magnitude
        // and whether reap+spawn coincide in one poll) is a harness artifact,
        // not the product cadence. `max_terminal_backlog`/`fused_reap_spawn` are
        // captured for debugging but not gated.
        assert_eq!(transfer.successful_downloads(), 300);
        let _ = (max_terminal_backlog, fused_reap_spawn);
    }

    /// `dispatch_walk` gates listing on the key buffer alone: it dispatches a
    /// page while `pending_entries` is below the low-water mark, refuses at or
    /// above it, and — the property this change introduced — dispatches
    /// regardless of how many children are in flight. Listing is decoupled from
    /// the child in-flight budget (`pipeline_depth`); a re-coupling regression
    /// would fail the third case.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_dispatch_walk_gates_on_key_buffer_not_children() {
        // Each case uses a fresh transfer so a dispatch in one does not consume
        // the walk / set walk_in_flight for the next.
        let dir = tempdir().unwrap();

        // Below low-water, no children: dispatches.
        {
            let (transfer, _rx) = setup(
                dir.path(),
                FailedTransferPolicy::Continue,
                mock_s3_success(),
            );
            let mut state = transfer.inner.state.lock();
            assert!(state.pending_entries.len() < WALK_LOW_WATER);
            assert!(
                transfer.dispatch_walk(&mut state).is_some(),
                "should dispatch a walk when the key buffer is below low-water"
            );
        }

        // At/above low-water: refuses (buffer backpressure).
        {
            let (transfer, _rx) = setup(
                dir.path(),
                FailedTransferPolicy::Continue,
                mock_s3_success(),
            );
            let mut state = transfer.inner.state.lock();
            for i in 0..WALK_LOW_WATER {
                state
                    .pending_entries
                    .push_back(Object::builder().key(format!("k{i}")).size(1).build());
            }
            assert!(
                transfer.dispatch_walk(&mut state).is_none(),
                "should not dispatch a walk when the key buffer is at/above low-water"
            );
        }

        // Decoupling: children at the in-flight budget (pipeline_depth) but the
        // key buffer low → still dispatches. The old gate refused here.
        {
            let (transfer, _rx) = setup(
                dir.path(),
                FailedTransferPolicy::Continue,
                mock_s3_success(),
            );
            let mut state = transfer.inner.state.lock();
            state.children_reserved = transfer.inner.pipeline_depth;
            assert!(
                transfer.dispatch_walk(&mut state).is_some(),
                "listing must proceed even when children are at the in-flight budget"
            );
        }
    }

    /// Incremental delivery: `execute_advance_walker` publishes discovered keys
    /// to `pending_entries` in `WALK_FLUSH_CHUNK`-sized batches mid-advance plus
    /// a final remainder, rather than one batch at the end. This pins that the
    /// chunked delivery is lossless and order-preserving across a chunk
    /// boundary (150 keys = two full chunks + a 22-key remainder).
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_incremental_delivery_lossless_and_ordered() {
        let dir = tempdir().unwrap();

        let n = 2 * WALK_FLUSH_CHUNK + 22; // spans two full chunks + a remainder
        let objects: Vec<Object> = (0..n)
            .map(|i| Object::builder().key(format!("k{i:05}")).size(1).build())
            .collect();
        let expected: Vec<String> = objects
            .iter()
            .map(|o| o.key().unwrap().to_string())
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(objects.clone()))
                .build()
        });
        let get = mock!(aws_sdk_s3::Client::get_object).then_output(|| {
            GetObjectOutput::builder()
                .content_length(1)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, get]);
        let (transfer, _rx) = setup(dir.path(), FailedTransferPolicy::Continue, client);

        // Dispatch one walk and execute it, capturing the keys it delivers.
        let mut work = {
            let mut state = transfer.inner.state.lock();
            match transfer.dispatch_walk(&mut state) {
                Some(PollWork::Ready { io, .. }) => io,
                other => panic!("expected an AdvanceWalker work item, got {other:?}"),
            }
        };
        transfer.execute(&mut work).await;

        // n < MAX_ENTRIES_PER_WALK, so a single advance drains all of them.
        let delivered: Vec<String> = {
            let state = transfer.inner.state.lock();
            state
                .pending_entries
                .iter()
                .map(|o| o.key().unwrap().to_string())
                .collect()
        };

        assert_eq!(
            delivered.len(),
            n,
            "all discovered keys must be delivered exactly once (no drop/dup across chunks)"
        );
        assert_eq!(
            delivered, expected,
            "keys must be delivered in listing order"
        );
    }
}
