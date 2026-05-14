/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! State machine for plural upload (`upload_objects`).

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::{PathBuf, MAIN_SEPARATOR, MAIN_SEPARATOR_STR};
use std::pin::Pin;
use std::sync::Arc;

use crate::error;
use crate::io::walk::{DirEntry, FsWalk};
use crate::io::InputStream;
use crate::operation::upload::{Upload, UploadHandle, UploadInput};
use crate::operation::DEFAULT_DELIMITER;
use crate::runtime::sync::Mutex;
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, TransferId, WorkOutcome};
use crate::types::{FailedTransferPolicy, FailedUpload};

/// Maximum number of walkers active at once (live in `State::walks` plus
/// in-flight in an `AdvanceWalker` work item). Bounds the fan-out of
/// subtree claiming so a deep tree doesn't spawn an unbounded number of
/// parallel walks. Tuned empirically against typical filesystem layouts;
/// revisit if walker throughput becomes a bottleneck on very wide trees.
const MAX_PARALLEL_WALKS: usize = 16;

/// Maximum number of entries drained from a walker per `execute_advance_walker`
/// pass before yielding back to the scheduler. Bounds the time spent
/// enumerating under a single scheduler slot and limits the burst size of
/// entries added to `State::pending_entries` in one pass.
const WALK_BATCH_SIZE: usize = 64;

/// Maximum number of children spawned per `poll_work` pass before yielding
/// back to the scheduler. Bounds the time spent in a single composite poll
/// (each spawn calls `Upload::orchestrate_child` and `enqueue_transfer`)
/// independently of `max_concurrent_uploads`. The scheduler's hierarchical
/// fair-share keeps the spawn rate matched to consumption; this constant
/// primarily caps per-poll latency, not steady-state concurrency.
const SPAWN_BATCH_SIZE: usize = 32;

/// Work data variants for the UploadObjectsTransfer state machine.
#[derive(Debug)]
pub(crate) enum UploadObjectsWork {
    /// Advance a walker and drain a batch of entries into `pending_entries`.
    AdvanceWalker { walk_id: u64, walk: Option<FsWalk> },
    /// Await `join()` on every child that has reached a terminal status.
    ///
    /// Each `poll_work` pass drains all currently-terminal children into a
    /// single work item; `execute_join_children` processes them back-to-back
    /// under one scheduler dispatch. Running every child through `join()`
    /// (rather than dropping the handle inline) ensures each handle is
    /// consumed the same way and the child's real `Ok(UploadOutput)` /
    /// `Err(Error)` is surfaced into `successful_uploads` /
    /// `FailedUpload.error`.
    JoinChildren {
        children: Option<Vec<ChildTransfer>>,
    },
}

pub(crate) struct ChildTransfer {
    source_path: PathBuf,
    key: String,
    handle: UploadHandle,
}

/// An entry claimed from `pending_entries` during phase 1 of spawning,
/// ready to be handed to `Upload::orchestrate_child` without holding the
/// state lock.
struct ClaimedEntry {
    source_path: PathBuf,
    key: String,
    input: UploadInput,
}

/// Outcome of phase 1 (`claim_spawn_batch`).
enum SpawnDecision {
    /// Abort policy tripped on a pre-orchestration failure. Caller exits
    /// `poll_work` immediately with `PollWork::Done`.
    Abort,
    /// A (possibly empty) batch of entries ready to be orchestrated outside
    /// the state lock.
    Batch(Vec<ClaimedEntry>),
}

/// Result of one `Upload::orchestrate_child` call. Named for readability in
/// the phase 3 signature.
type OrchestrateOutcome = Result<UploadHandle, crate::error::Error>;

impl fmt::Debug for ChildTransfer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildTransfer")
            .field("source_path", &self.source_path)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

struct State {
    walks: HashMap<u64, FsWalk>,
    next_walk_id: u64,
    in_flight_walks: usize,
    pending_entries: VecDeque<DirEntry>,
    children: HashMap<TransferId, ChildTransfer>,
    /// Entries that have been claimed from `pending_entries` by a `poll_work`
    /// frame that has released the state lock to run `orchestrate_child`, but
    /// have not yet been inserted into `children` or `failed`. Concurrent
    /// `poll_work` calls must not overshoot `max_concurrent_uploads` or signal
    /// termination while these are in flight.
    children_reserved: usize,
    /// Children that have been drained from `children` into a `JoinChildren`
    /// work item but whose `execute_join_children` has not yet finished
    /// updating counters / failed list. `check_terminal` must wait for these
    /// to drop to zero before signalling completion - otherwise the parent
    /// can terminate with stale `successful_uploads == 0` while results are
    /// still being tallied.
    reaping_in_flight: usize,
    failed: Vec<FailedUpload>,
    successful_uploads: u64,
}

/// Parent state machine for plural upload (`upload_objects`).
#[derive(Clone)]
pub(crate) struct UploadObjectsTransfer {
    inner: Arc<UploadObjectsTransferInner>,
}

struct UploadObjectsTransferInner {
    ctx: TransferContext,
    request: Arc<super::UploadObjectsInput>,
    state: Mutex<State>,
}

impl UploadObjectsTransfer {
    pub(crate) fn new(
        ctx: TransferContext,
        request: super::UploadObjectsInput,
        walker: FsWalk,
    ) -> Self {
        let mut walks = HashMap::new();
        walks.insert(0, walker);
        let inner = Arc::new(UploadObjectsTransferInner {
            ctx,
            request: Arc::new(request),
            state: Mutex::new(State {
                walks,
                next_walk_id: 1,
                in_flight_walks: 0,
                pending_entries: VecDeque::new(),
                children: HashMap::new(),
                children_reserved: 0,
                reaping_in_flight: 0,
                failed: Vec::new(),
                successful_uploads: 0,
            }),
        });
        Self { inner }
    }

    pub(crate) fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    pub(crate) fn successful_uploads(&self) -> u64 {
        self.inner.state.lock().successful_uploads
    }

    pub(crate) fn take_failed(&self) -> Vec<FailedUpload> {
        std::mem::take(&mut self.inner.state.lock().failed)
    }

    fn max_concurrent_uploads(&self) -> usize {
        self.inner.request.max_concurrent_uploads()
    }

    fn failure_policy(&self) -> &FailedTransferPolicy {
        self.inner.request.failure_policy()
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let active = self.inner.ctx.is_active();

        // Phase 1: under state lock, drain terminal children, handle
        // cancellation, claim a batch of entries to spawn, and decide
        // whether the remaining steps need to run at all.
        let claim: SpawnDecision = {
            let mut state = self.inner.state.lock();

            // 1a. Drain any children that have reached a terminal status
            //     into a single JoinChildren work item. Running them as a
            //     batch avoids N scheduler round-trips when many children
            //     finish in the same window. Runs regardless of `active`
            //     so partial results at cancellation time make the summary.
            let terminal = Self::drain_terminal_children(&mut state);
            if !terminal.is_empty() {
                return PollWork::Ready(IoRequest {
                    data: Some(Box::new(UploadObjectsWork::JoinChildren {
                        children: Some(terminal),
                    })),
                });
            }

            // 1b. If we are inactive (cancelled or failed), signal terminal
            //     and exit. Children are cancelled via
            //     `scheduler.cancel_transfer(parent_id)` from the handle's
            //     abort/drop/join paths, which cascade outside the state lock.
            if !active {
                self.inner.ctx.signal_terminal();
                return PollWork::Done;
            }

            // 1c. Claim a batch of entries to orchestrate as children. This
            //     does all the side-effect-free work under the lock (key
            //     derivation, stream creation, `UploadInput` build). The
            //     heavyweight step - `Upload::orchestrate_child`, which
            //     recurses into the scheduler - runs in phase 2 with the
            //     lock released.
            self.claim_spawn_batch(&mut state)
        };

        let claimed = match claim {
            SpawnDecision::Abort => return PollWork::Done,
            SpawnDecision::Batch(items) => items,
        };

        // Phase 2: orchestrate claimed children without holding the state
        // lock. `Upload::orchestrate_child` calls `scheduler.enqueue_transfer`
        // which acquires scheduler-level locks; doing that while holding
        // this transfer's state lock would serialise all concurrent
        // `poll_work` callers (child wakes, drain cycles) behind a single
        // producer.
        let outcomes: Vec<(OrchestrateOutcome, PathBuf, String)> = claimed
            .into_iter()
            .map(|claimed| {
                let ClaimedEntry {
                    source_path,
                    key,
                    input,
                } = claimed;
                let outcome = Upload::orchestrate_child(
                    self.inner.ctx.handle.clone(),
                    input,
                    self.inner.ctx.id.id,
                );
                (outcome, source_path, key)
            })
            .collect();

        // Phase 3: under state lock again, merge orchestration results,
        // release the reservation, and proceed to subtree claiming /
        // terminal check / dispatch. An abort during merge returns Done.
        let mut state = self.inner.state.lock();
        if let Some(done) = self.merge_spawned(&mut state, outcomes) {
            return done;
        }

        // Opportunistic subtree claiming.
        self.claim_subtrees(&mut state);

        // Terminal check (all walks drained, all children done, no reaps in
        // flight, no reservations outstanding).
        if let Some(out) = self.check_terminal(&mut state) {
            return out;
        }

        // Decide dispatch.
        self.dispatch_walk(&mut state)
    }

    /// Record the failure cause and signal terminal. Children are cancelled
    /// when the handle's cancel path runs `scheduler.cancel_transfer(parent_id)`.
    fn abort(&self, _state: &mut State, cause: impl Into<String>) -> PollWork {
        let cause = cause.into();
        tracing::debug!(
            target: crate::telemetry::TARGET_TRANSFER,
            transfer_id = ?self.inner.ctx.id,
            "upload_objects aborting: {cause}"
        );
        self.inner.ctx.set_failed(crate::error::Error::new(
            crate::error::ErrorKind::ChildOperationFailed,
            format!("upload_objects aborted: {cause}"),
        ));
        self.inner.ctx.signal_terminal();
        PollWork::Done
    }

    /// Remove every child that has reached a terminal status and return
    /// them. Increments `reaping_in_flight` by the number returned so that
    /// `check_terminal` does not signal completion while the resulting
    /// `execute_join_children` is still updating counters.
    fn drain_terminal_children(state: &mut State) -> Vec<ChildTransfer> {
        let terminal_ids: Vec<TransferId> = state
            .children
            .iter()
            .filter(|(_, c)| c.handle.status().is_terminal())
            .map(|(id, _)| *id)
            .collect();
        let drained: Vec<ChildTransfer> = terminal_ids
            .into_iter()
            .map(|id| state.children.remove(&id).expect("id from current map"))
            .collect();
        state.reaping_in_flight += drained.len();
        drained
    }

    /// Phase 1 of child spawning: under state lock, consume entries from
    /// `pending_entries` up to pipeline capacity, do the side-effect-free
    /// preparation (key derivation, `InputStream` creation, `UploadInput`
    /// build), and reserve slots via `children_reserved`.
    ///
    /// Returns [`SpawnDecision::Abort`] if a pre-orchestration failure was
    /// observed under Abort policy; the caller exits `poll_work` with
    /// [`PollWork::Done`] without orchestrating anything. Otherwise returns
    /// [`SpawnDecision::Batch`] with the prepared entries.
    fn claim_spawn_batch(&self, state: &mut State) -> SpawnDecision {
        let max_concurrent_uploads = self.max_concurrent_uploads();
        let bucket = self
            .inner
            .request
            .bucket()
            .expect("bucket validated by UploadObjectsInputBuilder::build")
            .to_string();
        let key_prefix = self.inner.request.key_prefix().map(|s| s.to_string());
        let delimiter = self.inner.request.delimiter().map(|s| s.to_string());

        let mut batch: Vec<ClaimedEntry> = Vec::new();
        while state.children.len() + state.children_reserved + batch.len() < max_concurrent_uploads
            && batch.len() < SPAWN_BATCH_SIZE
        {
            let entry = match state.pending_entries.pop_front() {
                Some(e) => e,
                None => break,
            };

            let relative = entry.relative_path().to_string_lossy().to_string();
            let key =
                match derive_object_key(&relative, key_prefix.as_deref(), delimiter.as_deref()) {
                    Ok(k) => k.into_owned(),
                    Err(e) => {
                        state.failed.push(FailedUpload {
                            input: None,
                            error: e,
                            source_path: Some(entry.path().to_path_buf()),
                        });
                        if *self.failure_policy() == FailedTransferPolicy::Abort {
                            self.abort(state, "key derivation failure");
                            return SpawnDecision::Abort;
                        }
                        continue;
                    }
                };

            let stream = match InputStream::read_from()
                .path(entry.path())
                .metadata(entry.metadata().clone())
                .build()
            {
                Ok(s) => s,
                Err(e) => {
                    state.failed.push(FailedUpload {
                        input: None,
                        error: e.into(),
                        source_path: Some(entry.path().to_path_buf()),
                    });
                    if *self.failure_policy() == FailedTransferPolicy::Abort {
                        self.abort(state, "stream creation failure");
                        return SpawnDecision::Abort;
                    }
                    continue;
                }
            };

            let input = UploadInput::builder()
                .bucket(bucket.clone())
                .key(key.clone())
                .body(stream)
                .build()
                .unwrap();

            batch.push(ClaimedEntry {
                source_path: entry.path().to_path_buf(),
                key,
                input,
            });
        }
        state.children_reserved += batch.len();
        SpawnDecision::Batch(batch)
    }

    /// Phase 3 of child spawning: merge orchestration results into state.
    /// Inserts successful handles into `children`, records failures in
    /// `failed`, and releases the reservation count in lockstep.
    ///
    /// Under [`FailedTransferPolicy::Abort`], the first orchestration
    /// failure aborts the transfer and returns [`PollWork::Done`]. The
    /// reservation is still released so `check_terminal` can see a
    /// consistent state.
    fn merge_spawned(
        &self,
        state: &mut State,
        outcomes: Vec<(OrchestrateOutcome, PathBuf, String)>,
    ) -> Option<PollWork> {
        let batch_size = outcomes.len();
        let mut aborted_in_batch = false;
        let mut abort_result = None;
        for (outcome, source_path, key) in outcomes {
            match outcome {
                Ok(handle) => {
                    let child_id = handle.id();
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        transfer_id = ?self.inner.ctx.id,
                        child_id = ?child_id,
                        key = %key,
                        "spawned child upload"
                    );
                    state.children.insert(
                        child_id,
                        ChildTransfer {
                            source_path,
                            key,
                            handle,
                        },
                    );
                }
                Err(e) => {
                    state.failed.push(FailedUpload {
                        input: None,
                        error: e,
                        source_path: Some(source_path),
                    });
                    if !aborted_in_batch && *self.failure_policy() == FailedTransferPolicy::Abort {
                        abort_result = Some(self.abort(state, "orchestration failure"));
                        aborted_in_batch = true;
                    }
                }
            }
        }
        state.children_reserved -= batch_size;
        abort_result
    }

    fn claim_subtrees(&self, state: &mut State) {
        loop {
            if state.walks.len() + state.in_flight_walks >= MAX_PARALLEL_WALKS {
                break;
            }
            let mut claimed_any = false;
            let walk_ids: Vec<u64> = state.walks.keys().copied().collect();
            for wid in walk_ids {
                if state.walks.len() + state.in_flight_walks >= MAX_PARALLEL_WALKS {
                    break;
                }
                if let Some(sub) = state.walks.get_mut(&wid).unwrap().try_claim_subtree() {
                    let new_id = state.next_walk_id;
                    state.next_walk_id += 1;
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        transfer_id = ?self.inner.ctx.id,
                        parent_walk_id = wid,
                        new_walk_id = new_id,
                        "claimed subtree"
                    );
                    state.walks.insert(new_id, sub);
                    claimed_any = true;
                }
            }
            if !claimed_any {
                break;
            }
        }
    }

    fn check_terminal(&self, state: &mut State) -> Option<PollWork> {
        if state.walks.is_empty()
            && state.in_flight_walks == 0
            && state.pending_entries.is_empty()
            && state.children.is_empty()
            && state.children_reserved == 0
            && state.reaping_in_flight == 0
        {
            let m = self.inner.ctx.metrics();
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                successful = state.successful_uploads,
                failed = state.failed.len(),
                network_tx = m.network_tx,
                disk_read = m.disk_read,
                "upload_objects complete"
            );
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
    }

    fn dispatch_walk(&self, state: &mut State) -> PollWork {
        let max_concurrent_uploads = self.max_concurrent_uploads();
        if state.children.len() + state.children_reserved >= max_concurrent_uploads {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                children = state.children.len(),
                children_reserved = state.children_reserved,
                max_concurrent_uploads,
                "dispatch_walk.pending.cap_full"
            );
            self.inner.ctx.set_pending();
            return PollWork::Pending;
        }
        if state.walks.is_empty() && state.in_flight_walks > 0 {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                in_flight_walks = state.in_flight_walks,
                "dispatch_walk.pending.walks_in_flight"
            );
            self.inner.ctx.set_pending();
            return PollWork::Pending;
        }
        if state.in_flight_walks >= MAX_PARALLEL_WALKS {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                in_flight_walks = state.in_flight_walks,
                MAX_PARALLEL_WALKS,
                "dispatch_walk.pending.walks_saturated"
            );
            self.inner.ctx.set_pending();
            return PollWork::Pending;
        }

        if let Some(&walk_id) = state.walks.keys().next() {
            let walk = state.walks.remove(&walk_id).unwrap();
            state.in_flight_walks += 1;
            tracing::trace!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                walk_id,
                "dispatching advance_walker"
            );
            PollWork::Ready(IoRequest {
                data: Some(Box::new(UploadObjectsWork::AdvanceWalker {
                    walk_id,
                    walk: Some(walk),
                })),
            })
        } else {
            tracing::debug!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                children = state.children.len(),
                children_reserved = state.children_reserved,
                in_flight_walks = state.in_flight_walks,
                "dispatch_walk.pending.no_walks_no_work"
            );
            self.inner.ctx.set_pending();
            PollWork::Pending
        }
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let data = work.data_mut::<UploadObjectsWork>();
        match data {
            UploadObjectsWork::AdvanceWalker { walk_id, walk } => {
                let walk_id = *walk_id;
                let walk = walk.take().expect("walk already taken");
                self.execute_advance_walker(walk_id, walk).await
            }
            UploadObjectsWork::JoinChildren { children } => {
                let children = children.take().expect("children already taken");
                self.execute_join_children(children).await
            }
        }
    }

    async fn execute_advance_walker(&self, walk_id: u64, mut walk: FsWalk) -> WorkOutcome {
        let ctx = &self.inner.ctx;
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            transfer_id = ?self.inner.ctx.id,
            walk_id,
            ready_files = walk.ready_files_len(),
            pending_dirs = walk.pending_dirs_len(),
            "advance_walker start"
        );

        // 1. Check active
        if !ctx.is_active() {
            let mut state = self.inner.state.lock();
            state.in_flight_walks -= 1;
            if !walk.is_exhausted() {
                state.walks.insert(walk_id, walk);
            }
            return WorkOutcome::Cancelled;
        }

        // 2. Advance walk up to 64 entries
        let mut entries = Vec::new();
        let mut walk_errors = Vec::new();
        let mut fatal_error = None;

        for _ in 0..WALK_BATCH_SIZE {
            match walk.next().await {
                Some(Ok(entry)) => entries.push(entry),
                Some(Err(e)) => {
                    if e.is_fatal() {
                        fatal_error = Some(e);
                        break;
                    }
                    walk_errors.push(e);
                }
                None => break,
            }
        }

        // 3. Lock state and process results
        let mut state = self.inner.state.lock();
        state.in_flight_walks -= 1;

        if let Some(fatal) = fatal_error {
            tracing::error!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                walk_id,
                error = %fatal,
                "fatal walker error, failing upload_objects"
            );
            ctx.set_failed(crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                fatal.to_string(),
            ));
            ctx.signal_terminal();
            return WorkOutcome::Failed {
                classification: None,
            };
        }

        let n_entries = entries.len();
        state.pending_entries.extend(entries);

        for we in walk_errors {
            tracing::warn!(
                target: crate::telemetry::TARGET_TRANSFER,
                transfer_id = ?self.inner.ctx.id,
                walk_id,
                path = ?we.path(),
                error = %we,
                "non-fatal walker error recorded"
            );
            state.failed.push(FailedUpload {
                input: None,
                error: crate::error::Error::new(crate::error::ErrorKind::IOError, we.to_string()),
                source_path: we.path().map(|p| p.to_path_buf()),
            });
            if *self.failure_policy() == FailedTransferPolicy::Abort {
                self.abort(&mut state, "walker error");
                return WorkOutcome::Failed {
                    classification: None,
                };
            }
        }

        let exhausted_now = walk.is_exhausted();
        let ready_files_remaining = walk.ready_files_len();
        let pending_dirs_remaining = walk.pending_dirs_len();

        if !exhausted_now {
            state.walks.insert(walk_id, walk);
        }
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            transfer_id = ?self.inner.ctx.id,
            walk_id,
            yielded = n_entries,
            ready_files = ready_files_remaining,
            pending_dirs = pending_dirs_remaining,
            exhausted = exhausted_now,
            "advance_walker end"
        );

        drop(state);
        ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    /// Consume every child handle via `join()` to capture each one's final
    /// `Ok(UploadOutput)` / `Err(Error)`. The parent's `MetricsState` is
    /// updated with the child's cumulative IO on success, and the child's
    /// actual error is stored in `FailedUpload.error` on failure or
    /// cancellation. Under `Abort` policy with an active parent, the first
    /// failure in the batch threads its cause text into `abort()`; later
    /// children in the batch are recorded but do not re-trigger abort.
    async fn execute_join_children(&self, children: Vec<ChildTransfer>) -> WorkOutcome {
        let n_children = children.len();
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            transfer_id = ?self.inner.ctx.id,
            n_children,
            "join_children start"
        );
        // Phase 1: await every child's join concurrently. Each join on an
        // already-terminal handle resolves immediately, but join_all keeps
        // the pattern open for handles whose completion signal has not yet
        // fully drained.
        let futures = children.into_iter().map(|child| {
            // Snapshot metrics before `join()` consumes the handle.
            let metrics = child.handle.metrics();
            let source_path = child.source_path;
            let key = child.key;
            async move {
                let result = child.handle.join().await;
                (result, metrics, source_path, key)
            }
        });
        let results = futures_util::future::join_all(futures).await;

        // Phase 2: update state once with all outcomes.
        let mut state = self.inner.state.lock();
        let reaped = results.len();
        let mut aborted_in_batch = false;

        for (result, metrics, source_path, key) in results {
            match result {
                Ok(_output) => {
                    state.successful_uploads += 1;
                    // Record directly into the parent's `MetricsState` via
                    // the field (rather than `TransferContext::record_io`)
                    // so the child's bytes are aggregated into the parent's
                    // per-transfer metrics without double-counting them in
                    // the client-level telemetry counters, which the child's
                    // own context already updated during its transfer.
                    self.inner.ctx.metrics.record_io(&crate::metrics::IoSample {
                        network_tx: metrics.network_tx,
                        network_rx: metrics.network_rx,
                        disk_read: metrics.disk_read,
                        disk_write: metrics.disk_write,
                    });
                    tracing::trace!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        transfer_id = ?self.inner.ctx.id,
                        key = %key,
                        "child upload completed"
                    );
                }
                Err(e) => {
                    // Re-check `active` inside the loop: an earlier iteration
                    // may have aborted the transfer, at which point later
                    // children should be recorded but must not re-trigger
                    // the abort cascade.
                    let active = self.inner.ctx.is_active();
                    let should_abort = !aborted_in_batch
                        && active
                        && *self.failure_policy() == FailedTransferPolicy::Abort;
                    let abort_cause =
                        should_abort.then(|| format!("child upload failed ({key}): {e}"));
                    tracing::warn!(
                        target: crate::telemetry::TARGET_TRANSFER,
                        transfer_id = ?self.inner.ctx.id,
                        key = %key,
                        error = %e,
                        "child upload failed"
                    );
                    state.failed.push(FailedUpload {
                        input: None,
                        error: e,
                        source_path: Some(source_path),
                    });
                    if let Some(cause) = abort_cause {
                        self.abort(&mut state, cause);
                        aborted_in_batch = true;
                    }
                }
            }
        }

        // Release the reap counts now that all results have been tallied
        // into the state. This must happen before dropping the lock so that
        // a concurrent `check_terminal` does not see
        // `children.is_empty() && reaping_in_flight == 0` while results are
        // still being applied.
        state.reaping_in_flight -= reaped;

        drop(state);
        self.inner.ctx.try_wake();
        tracing::trace!(
            target: crate::telemetry::TARGET_TRANSFER,
            transfer_id = ?self.inner.ctx.id,
            reaped,
            "join_children end"
        );
        WorkOutcome::Success { data: None }
    }
}

impl fmt::Debug for UploadObjectsTransfer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadObjectsTransfer")
            .field("id", &self.inner.ctx.id)
            .finish_non_exhaustive()
    }
}

impl Transfer for UploadObjectsTransfer {
    fn ctx(&self) -> &TransferContext {
        UploadObjectsTransfer::ctx(self)
    }

    fn poll_work(&self) -> PollWork {
        UploadObjectsTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(UploadObjectsTransfer::execute(self, work))
    }

    fn on_terminal(&self) {}
}

/// Derive the S3 object key for a file at `relative_filename` inside the walk root.
///
/// The key is formed by optionally prepending a prefix and substituting the
/// path separator with a custom delimiter if one is configured. When the
/// custom delimiter appears inside `relative_filename`, derivation fails with
/// an invalid-input error.
pub(crate) fn derive_object_key<'a>(
    relative_filename: &'a str,
    object_key_prefix: Option<&str>,
    object_key_delimiter: Option<&str>,
) -> Result<Cow<'a, str>, error::Error> {
    if let Some(delim) = object_key_delimiter {
        if delim != DEFAULT_DELIMITER && relative_filename.contains(delim) {
            return Err(error::invalid_input(format!(
                "a custom delimiter `{delim}` should not appear in `{relative_filename}`"
            )));
        }
    }

    let delim = object_key_delimiter.unwrap_or(DEFAULT_DELIMITER);

    let relative_filename = if delim == MAIN_SEPARATOR_STR {
        Cow::Borrowed(relative_filename)
    } else {
        Cow::Owned(relative_filename.replace(MAIN_SEPARATOR, delim))
    };

    let object_key = if let Some(prefix) = object_key_prefix {
        if prefix.ends_with(delim) {
            Cow::Owned(format!("{prefix}{relative_filename}"))
        } else {
            Cow::Owned(format!("{prefix}{delim}{relative_filename}"))
        }
    } else {
        relative_filename
    };

    Ok(object_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer::TransferContext;
    use crate::types::FailedTransferPolicy;
    use crate::DEFAULT_CONCURRENCY;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use std::fs;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::time::timeout;

    use crate::io::walk::{FsWalkContext, FsWalker};

    #[cfg(target_family = "unix")]
    #[test]
    fn test_derive_object_key() {
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", None, None).unwrap()
        );
        assert_eq!(
            "foobar/2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar"), None).unwrap()
        );
        assert_eq!(
            "foobar/2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar/"), None).unwrap()
        );
        assert_eq!(
            "2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", None, Some("-")).unwrap()
        );
        assert_eq!(
            "foobar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar"), Some("-")).unwrap()
        );
        assert_eq!(
            "foobar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar-"), Some("-")).unwrap()
        );
        assert_eq!(
            "foobar--2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foobar--"), Some("-")).unwrap()
        );
        assert_eq!(
            "2023/MYLONGDELIMJan/MYLONGDELIM1.png",
            derive_object_key("2023/Jan/1.png", None, Some("/MYLONGDELIM")).unwrap()
        );
        {
            use std::error::Error as _;
            let err = derive_object_key("2023/Jan-1.png", None, Some("-"))
                .err()
                .unwrap();
            assert_eq!(
                "a custom delimiter `-` should not appear in `2023/Jan-1.png`",
                format!("{}", err.source().unwrap())
            );
        }

        // Should not replace the path separator in prefix with a custom delimiter
        assert_eq!(
            "foo/bar-2023-Jan-1.png",
            derive_object_key("2023/Jan/1.png", Some("foo/bar"), Some("-")).unwrap()
        );

        // Should not fail if the user specifies the default delimiter as a custom delimiter
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023/Jan/1.png", None, Some(DEFAULT_DELIMITER)).unwrap()
        );
    }

    #[cfg(target_family = "windows")]
    #[test]
    fn test_derive_object_key() {
        assert_eq!(
            "2023/Jan/1.png",
            derive_object_key("2023\\Jan\\1.png", None, None).unwrap()
        );
    }

    fn mock_s3_success() -> aws_sdk_s3::Client {
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put])
    }

    fn mock_s3_failure() -> aws_sdk_s3::Client {
        let put = mock!(aws_sdk_s3::Client::put_object).then_error(|| {
            aws_sdk_s3::operation::put_object::PutObjectError::generic(
                aws_sdk_s3::error::ErrorMetadata::builder()
                    .code("InternalError")
                    .message("simulated failure")
                    .build(),
            )
        });
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put])
    }

    fn setup(
        source: &std::path::Path,
        policy: FailedTransferPolicy,
        s3_client: aws_sdk_s3::Client,
        recursive: bool,
    ) -> (
        UploadObjectsTransfer,
        crate::transfer::StateMachineTerminalReceiver,
    ) {
        let config = crate::Config::builder().client(s3_client).build();
        let handle = crate::client::Handle::new_for_test(config, DEFAULT_CONCURRENCY);

        let input = super::super::UploadObjectsInputBuilder::default()
            .bucket("test-bucket")
            .source(source)
            .failure_policy(policy)
            .build()
            .unwrap();

        let walker = FsWalker::builder()
            .recursive(recursive)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(source).build());

        let (ctx, completion_rx) = TransferContext::new(handle);
        let transfer = UploadObjectsTransfer::new(ctx, input, walker);
        // Register the parent group so children spawned via the scheduler
        // can find it. The composite is driven directly via `drive_transfer`
        // rather than enqueued, so we set up only the group entry, not the
        // descriptor in `transfers`.
        transfer
            .inner
            .ctx
            .handle
            .scheduler
            .register_empty_group_for_test(transfer.inner.ctx.id.id);
        (transfer, completion_rx)
    }

    /// Drive the transfer to completion by repeatedly polling and executing.
    async fn drive_transfer(transfer: &UploadObjectsTransfer) {
        loop {
            match transfer.poll_work() {
                PollWork::Ready(mut work) => {
                    transfer.execute(&mut work).await;
                }
                PollWork::Pending => {
                    // Give children time to complete
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                PollWork::Done => break,
            }
        }
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_happy_path_3_files_succeed() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world").unwrap();
        fs::write(dir.path().join("c.txt"), "test!").unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_uploads(), 3);
        assert!(transfer.take_failed().is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_nested_recursive() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "top").unwrap();
        fs::create_dir(dir.path().join("sub1")).unwrap();
        fs::write(dir.path().join("sub1/a.txt"), "a").unwrap();
        fs::create_dir(dir.path().join("sub2")).unwrap();
        fs::write(dir.path().join("sub2/b.txt"), "b").unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            true,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_uploads(), 3);
        assert!(transfer.take_failed().is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_keys_derived_correctly_with_prefix() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("top.txt"), "x").unwrap();
        fs::create_dir(dir.path().join("sub")).unwrap();
        fs::write(dir.path().join("sub/nested.txt"), "y").unwrap();

        // Capture all keys sent to S3 via the mock.
        let captured: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_cl = captured.clone();
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |req| {
                if let Some(k) = req.key() {
                    captured_cl.lock().unwrap().push(k.to_string());
                }
                true
            })
            .then_output(|| PutObjectOutput::builder().e_tag("test-etag").build());
        let s3_client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[put]);

        let config = crate::Config::builder().client(s3_client).build();
        let handle = crate::client::Handle::new_for_test(config, DEFAULT_CONCURRENCY);

        let input = super::super::UploadObjectsInputBuilder::default()
            .bucket("test-bucket")
            .source(dir.path())
            .failure_policy(FailedTransferPolicy::Continue)
            .key_prefix("photos/2024")
            .build()
            .unwrap();

        let walker = FsWalker::builder()
            .recursive(true)
            .follow_symlinks(true)
            .build()
            .walk(FsWalkContext::builder().root(dir.path()).build());

        let (ctx, completion_rx) = TransferContext::new(handle);
        let transfer = UploadObjectsTransfer::new(ctx, input, walker);
        transfer
            .inner
            .ctx
            .handle
            .scheduler
            .register_empty_group_for_test(transfer.inner.ctx.id.id);

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_uploads(), 2);

        // Verify the captured keys are derived with the prefix + default delimiter ("/")
        let mut keys = captured.lock().unwrap().clone();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "photos/2024/sub/nested.txt".to_string(),
                "photos/2024/top.txt".to_string(),
            ]
        );
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_per_entry_walker_error_continue() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("good.txt"), "ok").unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", dir.path().join("broken")).unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_uploads(), 1);
        let failed = transfer.take_failed();
        assert!(!failed.is_empty());
    }

    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_per_entry_walker_error_abort() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("good.txt"), "ok").unwrap();
        std::os::unix::fs::symlink("/nonexistent/target/xyz", dir.path().join("broken")).unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Abort,
            mock_s3_success(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert!(transfer.ctx().is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_continue() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world").unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_failure(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        let failed = transfer.take_failed();
        assert!(!failed.is_empty());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_child_failure_abort() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), "hello").unwrap();
        fs::write(dir.path().join("b.txt"), "world").unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Abort,
            mock_s3_failure(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert!(transfer.ctx().is_failed());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_cancellation() {
        let dir = tempdir().unwrap();
        for i in 0..50 {
            fs::write(dir.path().join(format!("file_{i}.txt")), "data").unwrap();
        }

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            false,
        );

        // Cancel immediately before driving
        transfer.ctx().set_cancelled();
        transfer.ctx().try_wake();

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should reach terminal state within timeout");

        // Transfer should be in a terminal state (cancelled)
        assert!(!transfer.ctx().is_active());
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn test_empty_directory() {
        let dir = tempdir().unwrap();

        let (transfer, completion_rx) = setup(
            dir.path(),
            FailedTransferPolicy::Continue,
            mock_s3_success(),
            false,
        );

        timeout(Duration::from_secs(5), async {
            drive_transfer(&transfer).await;
            let _ = completion_rx.await;
        })
        .await
        .expect("transfer should complete within timeout");

        assert_eq!(transfer.successful_uploads(), 0);
        assert!(transfer.take_failed().is_empty());
    }
}
