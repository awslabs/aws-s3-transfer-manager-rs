/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! THROWAWAY SPIKE — sync as a composite transfer, to find out whether it fits.
//!
//! The point of this file is the shape, not the behaviour. The earlier driver awaited both
//! walks and every upload inline in one task, which answered a throughput question and
//! said nothing about whether sync can live under the scheduler. Here nothing is awaited
//! outside `execute`, both sides advance as dispatched work, and per-key transfers are
//! children of this transfer rather than independent peers.
//!
//! The two sides are `source` and `destination` — roles, not places, since a source is a
//! filesystem walk in one direction and a listing in another. Comparison is identical
//! whichever way bytes move, so the direction-specific parts are passed in: how one entry
//! becomes a child transfer, and what "different" means. Only the upload direction is
//! wired up; S3-to-S3 would also need a single-object copy primitive, which the crate does
//! not have.
//!
//! Absent on purpose: deletes, failure policy, per-key reporting, filters.
//!
//! The question it answers: can `poll_work` express "feed whichever side is behind,
//! compare what arrived, spawn a child, retire finished children" without awaiting,
//! without unbounded buffering, and without starving any of those steps.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use crate::client::Handle;
use crate::error::Error;
use crate::io::key_stream::{Entry, EntryMeta, KeyStream};
use crate::operation::download::ManagedDownloadHandle;
use crate::operation::upload::UploadHandle;
use crate::runtime::sync::Mutex;
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};

// Entries drained per dispatched work item, per side. A filesystem walk matches
// `upload_objects` at 64, since each entry costs a blocking stat. A listing takes a whole
// page: the request already produced up to 1000 keys, and splitting them wastes the round
// trip.
const SOURCE_BATCH: usize = 64;
const DEST_BATCH: usize = 1000;

// Refill a side once its queue falls this low, so comparison still has something to work
// on while the next batch is fetched.
const LOW_WATER: usize = 16;

/// A child transfer, whichever direction it moves bytes in.
///
/// The directions use unrelated handle types returning different outputs, but sync only
/// needs to know whether a child has finished and whether it worked.
#[derive(Debug)]
pub(crate) enum ChildHandle {
    Upload(UploadHandle),
    Download(ManagedDownloadHandle),
}

impl ChildHandle {
    fn status(&self) -> crate::types::TransferStatus {
        match self {
            Self::Upload(h) => h.status(),
            Self::Download(h) => h.status(),
        }
    }

    async fn join(self) -> Result<(), Error> {
        match self {
            Self::Upload(h) => h.join().await.map(|_| ()),
            Self::Download(h) => h.join().await.map(|_| ()),
        }
    }
}

/// Starts moving one source entry, as a child of the given parent.
type SpawnFn<T> =
    Box<dyn Fn(&Arc<Handle>, u64, &Entry<T>) -> Result<ChildHandle, Error> + Send + Sync>;

/// Whether a key on both sides needs moving. Direction-specific: uploading leaves a newer
/// destination alone, downloading leaves a newer local file alone.
type DiffersFn = Box<dyn Fn(&EntryMeta, &EntryMeta) -> bool + Send + Sync>;

pub(crate) enum SyncWork<S, D> {
    AdvanceSource { walk: Option<Box<S>> },
    AdvanceDest { walk: Option<Box<D>> },
    JoinChildren { handles: Option<Vec<ChildHandle>> },
}

// Hand-written because neither walk is `Debug` — the same reason `DownloadObjectsWork`
// writes its own.
impl<S, D> std::fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdvanceSource { .. } => f.debug_struct("AdvanceSource").finish_non_exhaustive(),
            Self::AdvanceDest { .. } => f.debug_struct("AdvanceDest").finish_non_exhaustive(),
            Self::JoinChildren { .. } => f.debug_struct("JoinChildren").finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Default)]
struct Counters {
    sent: usize,
    destination_only: usize,
    peak_children: usize,
}

struct State<S: KeyStream, D: KeyStream> {
    // Taken out while that side's advance item is in flight, so only one advance per side
    // can be running at a time.
    source: Option<Box<S>>,
    dest: Option<Box<D>>,
    source_queue: VecDeque<Entry<S::Source>>,
    dest_queue: VecDeque<Entry<D::Source>>,
    source_done: bool,
    dest_done: bool,
    source_in_flight: bool,
    dest_in_flight: bool,
    children: Vec<ChildHandle>,
    // Work items dispatched but not yet finished. Draining terminal children moves them out
    // of `children` and into a join item, so without this the transfer can look idle while
    // that item is still counting.
    dispatched: usize,
    counters: Counters,
}

impl<S: KeyStream, D: KeyStream> State<S, D> {
    fn source_needs_refill(&self) -> bool {
        !self.source_done && !self.source_in_flight && self.source_queue.len() <= LOW_WATER
    }

    fn dest_needs_refill(&self) -> bool {
        !self.dest_done && !self.dest_in_flight && self.dest_queue.len() <= LOW_WATER
    }

    fn all_enumerated(&self) -> bool {
        self.source_done
            && self.dest_done
            && self.source_queue.is_empty()
            && self.dest_queue.is_empty()
    }
}

struct Inner<S: KeyStream, D: KeyStream> {
    ctx: TransferContext,
    spawn: SpawnFn<S::Source>,
    differs: DiffersFn,
    max_children: usize,
    state: Mutex<State<S, D>>,
}

impl<S: KeyStream, D: KeyStream> std::fmt::Debug for Inner<S, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncSpike")
            .field("id", &self.ctx.id)
            .finish()
    }
}

pub(crate) struct SyncTransfer<S: KeyStream, D: KeyStream> {
    inner: Arc<Inner<S, D>>,
}

// Derived would demand `Debug` of the stream types, which the walks do not implement.
impl<S: KeyStream, D: KeyStream> std::fmt::Debug for SyncTransfer<S, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

// Derived `Clone` would demand it of the stream types; only the `Arc` is cloned.
impl<S: KeyStream, D: KeyStream> Clone for SyncTransfer<S, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, D> SyncTransfer<S, D>
where
    S: KeyStream + Send + 'static,
    D: KeyStream + Send + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    pub(crate) fn new(
        ctx: TransferContext,
        source: S,
        dest: D,
        spawn: SpawnFn<S::Source>,
        differs: DiffersFn,
        max_children: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                ctx,
                spawn,
                differs,
                max_children,
                state: Mutex::new(State {
                    source: Some(Box::new(source)),
                    dest: Some(Box::new(dest)),
                    source_queue: VecDeque::new(),
                    dest_queue: VecDeque::new(),
                    source_done: false,
                    dest_done: false,
                    source_in_flight: false,
                    dest_in_flight: false,
                    children: Vec::new(),
                    dispatched: 0,
                    counters: Counters::default(),
                }),
            }),
        }
    }

    pub(crate) fn id(&self) -> crate::transfer::TransferId {
        self.inner.ctx.id
    }

    pub(crate) fn counts(&self) -> (usize, usize, usize) {
        let state = self.inner.state.lock();
        (
            state.counters.sent,
            state.counters.destination_only,
            state.counters.peak_children,
        )
    }

    // One comparison step against the queue heads, consuming what it decided about.
    // `Ok(None)` means it cannot decide yet — either nothing has arrived, or a side is
    // empty but unfinished, and guessing there is how keys get wrongly deleted.
    fn decide(&self, state: &mut State<S, D>) -> Option<Entry<S::Source>> {
        loop {
            let ord = match (state.source_queue.front(), state.dest_queue.front()) {
                (Some(_), None) if state.dest_done => std::cmp::Ordering::Less,
                (None, Some(_)) if state.source_done => std::cmp::Ordering::Greater,
                (Some(s), Some(d)) => s.key.cmp(&d.key),
                _ => return None,
            };

            match ord {
                std::cmp::Ordering::Less => {
                    return state.source_queue.pop_front();
                }
                std::cmp::Ordering::Greater => {
                    state.dest_queue.pop_front();
                    state.counters.destination_only += 1;
                }
                std::cmp::Ordering::Equal => {
                    let s = state.source_queue.pop_front().expect("checked above");
                    let d = state.dest_queue.pop_front().expect("checked above");
                    if (self.inner.differs)(&s.meta, &d.meta) {
                        return Some(s);
                    }
                }
            }
        }
    }

    fn drain_terminal_children(state: &mut State<S, D>) -> Option<Vec<ChildHandle>> {
        if !state.children.iter().any(|c| c.status().is_terminal()) {
            return None;
        }
        let (terminal, active): (Vec<_>, Vec<_>) = std::mem::take(&mut state.children)
            .into_iter()
            .partition(|c| c.status().is_terminal());
        state.children = active;
        Some(terminal)
    }
}

impl<S, D> Transfer for SyncTransfer<S, D>
where
    S: KeyStream + Send + Sync + 'static,
    D: KeyStream + Send + Sync + 'static,
    S::Source: Send + Sync + 'static,
    D::Source: Send + Sync + 'static,
{
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    fn poll_work(&self) -> PollWork {
        let mut state = self.inner.state.lock();

        if !self.inner.ctx.is_active() {
            return PollWork::Done;
        }

        // 1. Feed whichever side is short. Nothing is read here: the walk travels to
        //    `execute`, the only place allowed to block.
        if state.source_needs_refill() {
            if let Some(walk) = state.source.take() {
                state.source_in_flight = true;
                state.dispatched += 1;
                return PollWork::ready(IoRequest {
                    data: Some(Box::new(SyncWork::<S, D>::AdvanceSource {
                        walk: Some(walk),
                    })),
                });
            }
        }
        if state.dest_needs_refill() {
            if let Some(walk) = state.dest.take() {
                state.dest_in_flight = true;
                state.dispatched += 1;
                return PollWork::ready(IoRequest {
                    data: Some(Box::new(SyncWork::<S, D>::AdvanceDest { walk: Some(walk) })),
                });
            }
        }

        // 2. Compare what has arrived, spawning at most one child before yielding, which is
        //    the scheduler's single-ticket spawn contract.
        if state.children.len() < self.inner.max_children {
            if let Some(entry) = self.decide(&mut state) {
                match (self.inner.spawn)(&self.inner.ctx.handle, self.inner.ctx.id.id, &entry) {
                    Ok(child) => {
                        state.children.push(child);
                        state.counters.peak_children =
                            state.counters.peak_children.max(state.children.len());
                        return PollWork::Spawned;
                    }
                    Err(e) => {
                        self.inner.ctx.set_failed_and_signal(e);
                        return PollWork::Done;
                    }
                }
            }
        }

        // 3. Retire finished children.
        if let Some(handles) = Self::drain_terminal_children(&mut state) {
            state.dispatched += 1;
            return PollWork::ready(IoRequest {
                data: Some(Box::new(SyncWork::<S, D>::JoinChildren {
                    handles: Some(handles),
                })),
            });
        }

        // 4. Finished when both sides are exhausted and nothing is outstanding.
        if state.all_enumerated() && state.children.is_empty() && state.dispatched == 0 {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return PollWork::Done;
        }

        // Otherwise something in flight will wake us.
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(async move {
            match work.data_mut::<SyncWork<S, D>>() {
                SyncWork::AdvanceSource { walk } => {
                    let mut walk = walk.take().expect("walk taken twice");
                    let mut entries = Vec::new();
                    let mut done = false;
                    for _ in 0..SOURCE_BATCH {
                        match walk.next_entry().await {
                            Some(Ok(entry)) => entries.push(entry),
                            Some(Err(err)) => {
                                self.inner.ctx.set_failed_and_signal(err);
                                return WorkOutcome::Failed {
                                    classification: None,
                                };
                            }
                            None => {
                                done = true;
                                break;
                            }
                        }
                    }
                    {
                        let mut state = self.inner.state.lock();
                        state.source_queue.extend(entries);
                        state.source_done = done;
                        state.source_in_flight = false;
                        state.source = Some(walk);
                        state.dispatched -= 1;
                    }
                    self.inner.ctx.try_wake();
                    WorkOutcome::Success { data: None }
                }
                SyncWork::AdvanceDest { walk } => {
                    let mut walk = walk.take().expect("walk taken twice");
                    let mut entries = Vec::new();
                    let mut done = false;
                    for _ in 0..DEST_BATCH {
                        match walk.next_entry().await {
                            Some(Ok(entry)) => entries.push(entry),
                            Some(Err(err)) => {
                                self.inner.ctx.set_failed_and_signal(err);
                                return WorkOutcome::Failed {
                                    classification: None,
                                };
                            }
                            None => {
                                done = true;
                                break;
                            }
                        }
                    }
                    {
                        let mut state = self.inner.state.lock();
                        state.dest_queue.extend(entries);
                        state.dest_done = done;
                        state.dest_in_flight = false;
                        state.dest = Some(walk);
                        state.dispatched -= 1;
                    }
                    self.inner.ctx.try_wake();
                    WorkOutcome::Success { data: None }
                }
                SyncWork::JoinChildren { handles } => {
                    let handles = handles.take().expect("handles taken twice");
                    let mut joined = 0;
                    for handle in handles {
                        match handle.join().await {
                            Ok(()) => joined += 1,
                            Err(e) => {
                                self.inner.ctx.set_failed_and_signal(e);
                                return WorkOutcome::Failed {
                                    classification: None,
                                };
                            }
                        }
                    }
                    {
                        let mut state = self.inner.state.lock();
                        state.counters.sent += joined;
                        state.dispatched -= 1;
                    }
                    self.inner.ctx.try_wake();
                    WorkOutcome::Success { data: None }
                }
            }
        })
    }
}

/// Start a local-to-S3 sync as a scheduled transfer, without waiting for it.
///
/// Separate from waiting so a caller can cancel a run in progress — which is also the
/// shape a real operation would take, where initiating and joining are distinct.
#[allow(clippy::type_complexity)]
pub(crate) fn start_sync_up(
    tm: &crate::Client,
    root: &std::path::Path,
    bucket: &str,
    prefix: &str,
    max_children: usize,
) -> (
    SyncTransfer<crate::io::walk::FsWalk, crate::io::walk::S3Walk>,
    crate::transfer::StateMachineTerminalReceiver,
) {
    use crate::io::walk::{FsWalkContext, FsWalker, S3WalkContext, S3Walker};
    use crate::operation::upload::{Upload, UploadInput};

    let handle = tm.handle.clone();
    let source = FsWalker::builder()
        .recursive(true)
        .key_order(true)
        .build()
        .walk(FsWalkContext::builder().root(root).build());
    let dest = S3Walker::builder()
        .prefix(prefix)
        .filter(crate::io::walk::exclude_s3_folder_markers)
        .build()
        .walk(
            S3WalkContext::builder()
                .client(handle.s3_client.clone())
                .bucket(bucket)
                .build(),
        );

    let bucket_owned = bucket.to_string();
    let prefix_owned = prefix.to_string();
    let spawn: SpawnFn<crate::io::walk::DirEntry> = Box::new(move |handle, parent_id, entry| {
        // The entry's own path, not one rebuilt from the key, which would fail to open a
        // name that is not valid UTF-8.
        let stream = crate::io::InputStream::from_path(entry.source.path())?;
        let key = if prefix_owned.is_empty() {
            entry.key.clone()
        } else {
            format!("{}/{}", prefix_owned.trim_end_matches('/'), entry.key)
        };
        let input = UploadInput::builder()
            .bucket(bucket_owned.clone())
            .key(key)
            .body(stream)
            .build()?;
        Ok(ChildHandle::Upload(Upload::orchestrate_child(
            handle.clone(),
            input,
            parent_id,
        )?))
    });

    // Uploading: send when the sizes differ or the local file is newer.
    let differs: DiffersFn = Box::new(|source: &EntryMeta, dest: &EntryMeta| {
        source.size != dest.size || source.last_modified_secs > dest.last_modified_secs
    });

    let (ctx, completion_rx) = TransferContext::new(handle.clone());
    let transfer = SyncTransfer::new(ctx, source, dest, spawn, differs, max_children);
    handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    (transfer, completion_rx)
}

/// Run a local-to-S3 sync as a scheduled transfer and wait for it. Returns files sent,
/// destination-only keys seen, and the most children ever in flight.
pub(crate) async fn sync_up_scheduled(
    tm: &crate::Client,
    root: &std::path::Path,
    bucket: &str,
    prefix: &str,
    max_children: usize,
) -> Result<(usize, usize, usize), Error> {
    let (transfer, completion_rx) = start_sync_up(tm, root, bucket, prefix, max_children);
    // The sender drops only when the transfer signals terminal, so a receive error means
    // the scheduler dropped it — treated the same as any other terminal.
    let _ = completion_rx.await;
    if let Some(err) = transfer.inner.ctx.take_error() {
        return Err(err);
    }
    Ok(transfer.counts())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_types::DateTime;
    use std::sync::Mutex as StdMutex;
    use tempfile::tempdir;

    fn client_with(objects: Vec<Object>, seen: Arc<StdMutex<Vec<String>>>) -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            let mut out = ListObjectsV2Output::builder();
            for obj in &objects {
                out = out.contents(obj.clone());
            }
            out.build()
        });
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |req| {
                seen.lock().unwrap().push(req.key().unwrap().to_string());
                true
            })
            .then_output(|| PutObjectOutput::builder().build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, put])
    }

    // Every scheduled run goes through this. A missed wake parks the transfer forever, and
    // a test that hangs reports nothing at all — so the bound is part of the harness, not
    // an afterthought.
    async fn run(
        tm: &crate::Client,
        root: &std::path::Path,
        bucket: &str,
        prefix: &str,
        max_children: usize,
    ) -> (usize, usize, usize) {
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            sync_up_scheduled(tm, root, bucket, prefix, max_children),
        )
        .await
        .expect("sync never finished: a Pending with no matching wake")
        .expect("sync failed")
    }

    // The whole point: driven entirely by the scheduler, with both walks advanced as
    // dispatched work rather than awaited inline.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn scheduled_sync_sends_what_is_missing() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("inner")).unwrap();
        std::fs::write(dir.path().join("inner/new.txt"), "xx").unwrap();
        std::fs::write(dir.path().join("keep.txt"), "yyy").unwrap();

        let local_secs = std::fs::metadata(dir.path().join("keep.txt"))
            .unwrap()
            .modified()
            .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)
            .unwrap();
        let already_there = Object::builder()
            .key("data/keep.txt")
            .size(3)
            .last_modified(DateTime::from_secs(local_secs + 10))
            .build();

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let s3 = client_with(vec![already_there], seen.clone());
        let tm = crate::Client::new(crate::Config::builder().client(s3).build());

        let (sent, destination_only, peak) = run(&tm, dir.path(), "bucket", "data", 8).await;

        assert_eq!(sent, 1, "only the missing file should be sent");
        assert_eq!(destination_only, 0);
        assert!(peak >= 1);
        assert_eq!(*seen.lock().unwrap(), vec!["data/inner/new.txt"]);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn scheduled_sync_reports_destination_only_keys() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), "x").unwrap();

        let gone = Object::builder()
            .key("data/zz-gone.txt")
            .size(1)
            .last_modified(DateTime::from_secs(1))
            .build();

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let s3 = client_with(vec![gone], seen.clone());
        let tm = crate::Client::new(crate::Config::builder().client(s3).build());

        let (sent, destination_only, _) = sync_up_scheduled(&tm, dir.path(), "bucket", "data", 8)
            .await
            .unwrap();

        assert_eq!(sent, 1);
        assert_eq!(destination_only, 1, "the key only at the destination");
    }

    // --- cancellation ---
    //
    // Cancelling is triggered from inside the mock, on the fifth upload, so it always
    // lands with children genuinely in flight. Starting it and then cancelling from the
    // test would race the run to completion, and a test that sometimes cancels an
    // already-finished sync would pass while proving nothing.

    // Set after the transfer starts; the mock reads it and cancels once enough uploads have
    // gone through.
    type CancelAt = Arc<StdMutex<Option<(Arc<Handle>, crate::transfer::TransferId)>>>;

    fn client_cancelling_on_fifth_put(
        seen: Arc<StdMutex<Vec<String>>>,
        cancel: CancelAt,
    ) -> aws_sdk_s3::Client {
        let list = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |req| {
                let mut seen = seen.lock().unwrap();
                seen.push(req.key().unwrap().to_string());
                if seen.len() == 5 {
                    if let Some((handle, id)) = cancel.lock().unwrap().as_ref() {
                        handle.scheduler.cancel_transfer(*id);
                    }
                }
                true
            })
            .then_output(|| PutObjectOutput::builder().build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, put])
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn cancelling_stops_the_run() {
        let dir = tempdir().unwrap();
        for i in 0..200 {
            std::fs::write(dir.path().join(format!("f{i:03}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let cancel: CancelAt = Arc::default();
        let s3 = client_cancelling_on_fifth_put(seen.clone(), cancel.clone());
        let tm = crate::Client::new(crate::Config::builder().client(s3).build());

        let (transfer, completion_rx) = start_sync_up(&tm, dir.path(), "bucket", "", 4);
        *cancel.lock().unwrap() = Some((tm.handle.clone(), transfer.id()));

        // Must resolve, not hang: a cancelled transfer still owes its waiter an answer.
        tokio::time::timeout(std::time::Duration::from_secs(20), completion_rx)
            .await
            .expect("cancelled sync never resolved")
            .ok();

        let issued = seen.lock().unwrap().len();
        assert!(
            issued < 200,
            "cancel did not stop the run: {issued} of 200 uploaded"
        );
        assert!(
            transfer.ctx().is_cancelled() || !transfer.ctx().is_active(),
            "transfer should not still be active after cancellation"
        );
        println!("cancelled after {issued} uploads of 200");
    }

    // Control for the test above: without the cancel, the same setup sends everything. So
    // "few uploads happened" cannot pass for some unrelated reason.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn without_cancelling_the_same_run_sends_everything() {
        let dir = tempdir().unwrap();
        for i in 0..200 {
            std::fs::write(dir.path().join(format!("f{i:03}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        // Cell left empty, so the mock never cancels.
        let cancel: CancelAt = Arc::default();
        let s3 = client_cancelling_on_fifth_put(seen.clone(), cancel);
        let tm = crate::Client::new(crate::Config::builder().client(s3).build());

        let (sent, _, _) = run(&tm, dir.path(), "bucket", "", 4).await;
        assert_eq!(sent, 200);
        assert_eq!(seen.lock().unwrap().len(), 200);
    }

    // --- starvation ---
    //
    // Every `PollWork::Pending` owes a future wake, and sync parks in exactly one place:
    // waiting on children or on an advance it dispatched. Advances wake themselves when
    // they finish; children are woken for us, because `signal_terminal` on a child wakes
    // its parent — which only works because they were created through
    // `orchestrate_child` with the parent id rather than as top-level transfers.
    //
    // A missed wake shows up as a hang, so every test here is wrapped in a timeout: the
    // failure mode is "never finishes", and an assertion that never runs proves nothing.

    fn client_with_concurrency(
        objects: Vec<Object>,
        seen: Arc<StdMutex<Vec<String>>>,
        concurrency: usize,
    ) -> crate::Client {
        let s3 = client_with(objects, seen);
        crate::Client::new(
            crate::Config::builder()
                .client(s3)
                .concurrency(crate::types::ConcurrencyMode::Explicit(concurrency))
                .build(),
        )
    }

    // One dispatch slot for everything: both walks, every child, and every join have to
    // take turns through the same eye of a needle.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn finishes_with_a_single_dispatch_slot() {
        let dir = tempdir().unwrap();
        for i in 0..60 {
            std::fs::write(dir.path().join(format!("f{i:02}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let tm = client_with_concurrency(vec![], seen.clone(), 1);

        let (sent, _, peak) = run(&tm, dir.path(), "bucket", "", 4).await;
        assert_eq!(sent, 60);
        assert!(peak <= 4);
        assert_eq!(seen.lock().unwrap().len(), 60);
    }

    // A child cap of one means the transfer parks on a single child over and over, so
    // every one of those parks depends on the parent being woken when that child ends.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn finishes_with_one_child_at_a_time() {
        let dir = tempdir().unwrap();
        for i in 0..20 {
            std::fs::write(dir.path().join(format!("f{i:02}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let tm = client_with_concurrency(vec![], seen.clone(), 2);

        let (sent, _, peak) = run(&tm, dir.path(), "bucket", "", 1).await;
        assert_eq!(sent, 20);
        assert_eq!(peak, 1);
    }

    // Other transfers competing for the same slot. Sync must not be starved indefinitely,
    // and must not starve them either.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn finishes_while_other_transfers_compete() {
        let dir = tempdir().unwrap();
        for i in 0..30 {
            std::fs::write(dir.path().join(format!("f{i:02}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let tm = client_with_concurrency(vec![], seen.clone(), 1);

        // Outside the synced directory, or the sync would pick it up as another file.
        let elsewhere = tempdir().unwrap();
        let other = elsewhere.path().join("other.bin");
        std::fs::write(&other, vec![1u8; 4096]).unwrap();

        let competing = {
            let tm = tm.clone();
            let other = other.clone();
            tokio::spawn(async move {
                for i in 0..20 {
                    let stream = crate::io::InputStream::from_path(&other).unwrap();
                    tm.upload()
                        .bucket("bucket")
                        .key(format!("unrelated/{i}"))
                        .body(stream)
                        .initiate()
                        .unwrap()
                        .join()
                        .await
                        .unwrap();
                }
            })
        };

        let (sent, _, _) = run(&tm, dir.path(), "bucket", "", 4).await;
        assert_eq!(sent, 30);

        tokio::time::timeout(std::time::Duration::from_secs(20), competing)
            .await
            .expect("competing transfers starved by sync")
            .unwrap();
    }

    // More files than child slots, so the cap has to hold and the run still finishes.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn scheduled_sync_respects_the_child_cap() {
        let dir = tempdir().unwrap();
        for i in 0..40 {
            std::fs::write(dir.path().join(format!("f{i:02}.txt")), "x").unwrap();
        }

        let seen: Arc<StdMutex<Vec<String>>> = Arc::default();
        let s3 = client_with(vec![], seen.clone());
        let tm = crate::Client::new(crate::Config::builder().client(s3).build());

        let (sent, _, peak) = run(&tm, dir.path(), "bucket", "", 4).await;

        assert_eq!(sent, 40);
        assert!(peak <= 4, "child cap exceeded: {peak}");
        // Every file was in fact uploaded even with the completion bug present; what was
        // wrong was the count and the early finish. Both are asserted so a regression in
        // either shows up.
        assert_eq!(seen.lock().unwrap().len(), 40);
    }
}
