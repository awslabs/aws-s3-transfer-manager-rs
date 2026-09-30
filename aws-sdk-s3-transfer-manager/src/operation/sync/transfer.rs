/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::io::key::stream::{KeyStream, StreamError};
use crate::operation::sync::compare::{Compare, Decision, Verdict};
use crate::operation::sync::walk::{Pairing, Walk};
use crate::transfer::{IoRequest, PollWork, Transfer, TransferContext, WorkOutcome};

// Pairings taken per work item. The merge pulls from whichever side answers the next key, so a
// batch spans an unknown number of entries on either side. The bound counts pairings for that
// reason, and no listing page can size it. The bound caps how long a single item occupies an
// executor slot, and one slot is a share of what the whole client has.
const MERGE_BATCH: usize = 64;

// How many failures a run keeps. The walk reports most failures per entry and carries on, so a
// tree where every entry fails would otherwise put the number of entries into peak memory. Past
// the cap the run counts a failure and holds none of it. A result reports the count, and the
// reporting layer names each key.
//
// The sample holds the first failures seen, not a representative spread. Where failures cluster in
// one early subtree, the whole sample comes from there. A reader taking the sample as typical of
// the run would be wrong.
const FAILURES_KEPT: usize = 64;

// Turning a decision into a child. The other thing that differs by direction, and unlike the
// comparison it cannot be a static: building a child needs the client, the bucket, and the roots.
pub(crate) trait SpawnChild<S>: Send + Sync {
    // Enqueue a child for this key and hand back a way to ask after it. The key is the relative
    // one both sides agree on; turning it into an address is this implementation's business.
    fn spawn(&self, key: &str, source: &S, parent: u64)
        -> Result<ChildHandle, crate::error::Error>;
}

// Sync asks a child two things: whether it finished, and what it moved. An upload handle and a
// download handle have different types and return different outputs, and both carry the part sync
// needs in the same field.
pub(crate) struct ChildHandle {
    id: crate::transfer::TransferId,
    inner: ChildInner,
}

enum ChildInner {
    Upload(crate::operation::upload::UploadHandle),
    #[cfg(test)]
    Controlled {
        ended: Arc<std::sync::atomic::AtomicBool>,
        moved: u64,
        failed: bool,
    },
}

impl fmt::Debug for ChildHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildHandle").field("id", &self.id).finish()
    }
}

impl ChildHandle {
    pub(crate) fn id(&self) -> crate::transfer::TransferId {
        self.id
    }

    // Only children that have reached an end go to a reap. Joining one still running would hold a
    // work item open for the length of the transfer.
    pub(crate) fn is_finished(&self) -> bool {
        match &self.inner {
            ChildInner::Upload(handle) => handle.status().is_terminal(),
            #[cfg(test)]
            ChildInner::Controlled { ended, .. } => ended.load(std::sync::atomic::Ordering::SeqCst),
        }
    }

    // What the child moved, and whether it got there. Consuming, because joining is the only way to
    // learn either.
    pub(crate) async fn join(self) -> Result<u64, crate::error::Error> {
        match self.inner {
            ChildInner::Upload(handle) => handle.join().await.map(|out| out.metrics.network_tx),
            #[cfg(test)]
            ChildInner::Controlled { moved, failed, .. } => {
                if failed {
                    Err(crate::error::Error::new(
                        crate::error::ErrorKind::IOError,
                        "a child that ended badly",
                    ))
                } else {
                    Ok(moved)
                }
            }
        }
    }
}

// Sends a local file to a bucket. `SpawnDownload` below is the other half, and the trait above
// holds for both. Two things differ by direction: which client call carries the bytes, and what the
// destination needs in place before they land.
pub(crate) struct SpawnUpload {
    handle: Arc<crate::client::Handle>,
    bucket: String,
    // The place in the bucket the run is against, already ending in its delimiter. Both sides
    // compare on keys relative to their own root. The listing strips the prefix off, so naming an
    // object means putting the prefix back. Without it a prefixed run writes to the bucket root.
    root: String,
}

impl SpawnUpload {
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        bucket: impl Into<String>,
        prefix: Option<&str>,
    ) -> Self {
        Self {
            handle,
            bucket: bucket.into(),
            root: crate::io::key::stream::root_prefix(prefix).into_owned(),
        }
    }

    // The object a relative key names under this run's root. Used by `spawn` and asserted on
    // directly, so a test cannot be checking a different rule from the one that runs.
    pub(crate) fn object_key(&self, key: &str) -> String {
        format!("{}{}", self.root, key)
    }
}

impl SpawnChild<crate::io::walk::FsEntry> for SpawnUpload {
    fn spawn(
        &self,
        key: &str,
        source: &crate::io::walk::FsEntry,
        parent: u64,
    ) -> Result<ChildHandle, crate::error::Error> {
        // Hand the builder the metadata the walk already read. Without it the builder stats the path
        // again — a blocking syscall inside a poll, once per key, for a size the comparison has
        // already decided from. A second read can also disagree with the first.
        let mut body = crate::io::InputStream::read_from().path(source.path());
        if let Some(metadata) = source.metadata() {
            body = body.metadata(metadata.clone());
        }
        let stream = body.build()?;
        let input = crate::operation::upload::UploadInput::builder()
            .bucket(self.bucket.clone())
            .key(self.object_key(key))
            .body(stream)
            .build()
            .expect("bucket, key and body are all set");
        let handle = crate::operation::upload::Upload::orchestrate_child(
            self.handle.clone(),
            input,
            parent,
        )?;
        Ok(ChildHandle {
            id: handle.id(),
            inner: ChildInner::Upload(handle),
        })
    }
}

// The merge, moved out of `State` for as long as one work item holds it. `next` needs
// `&mut`, and holding the state lock across it would block every poll on this transfer.
pub(crate) enum SyncWork<S: KeyStream, D: KeyStream> {
    AdvanceMerge { walk: Option<Box<Walk<S, D>>> },
    // Children that have reached an end. Joining one waits, so collecting them is a work item like
    // any other, and they leave `State::children` as the item takes them.
    ReapChildren { children: Vec<ChildHandle> },
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncWork::AdvanceMerge { .. } => f.write_str("AdvanceMerge"),
            SyncWork::ReapChildren { children } => {
                write!(f, "ReapChildren({})", children.len())
            }
        }
    }
}

// A key the comparison qualified for a transfer, with the pairing behind the decision. Spawning
// needs both: the key names the destination, and a child reads its bytes from the source entry.
type Qualified<S, D> = (Pairing<S, D>, Decision);

// What a run decided, by kind. Every pairing yields exactly one decision, so the three counts sum
// to `paired`. Grouped so a count added later reaches every site at once.
#[derive(Debug, Default)]
struct Decided {
    transfers: u64,
    deletes: u64,
    skips: u64,
}

impl std::ops::AddAssign for Decided {
    fn add_assign(&mut self, batch: Self) {
        self.transfers += batch.transfers;
        self.deletes += batch.deletes;
        self.skips += batch.skips;
    }
}

struct State<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, so holding the
    // state lock across it would block every poll on this transfer. The merge moves into the item
    // for as long as the item runs.
    walk: Option<Walk<S, D>>,
    merge_in_flight: bool,
    paired: u64,
    decided: Decided,
    // Transfers waiting for a child slot. The comparison decides a key as soon as the merge pairs
    // it. The decision then waits here for room, so the number of children already running never
    // holds the merge back.
    waiting: VecDeque<Qualified<S::Source, D::Source>>,
    // Children enqueued and not yet reaped.
    children: std::collections::HashMap<crate::transfer::TransferId, ChildHandle>,
    // Children handed to a reap and not yet joined. They have left `children`, so without this
    // count a poll would see no children and call the run over while their outcomes were still
    // coming back.
    reap_in_flight: usize,
    bytes_moved: u64,
    // Transfers that arrived, one per child the reap joins. `decided.transfers` counts the other
    // end, when the comparison picks a key out. A caller reconciling the run against the
    // destination reads this one. Deriving arrivals from the decision count means subtracting every
    // population in between and trusting them not to overlap.
    transferred: u64,
    // A child the run could not enqueue, or one that ended badly. The run counts each failure and
    // keeps none of them, because naming the key that failed belongs to the reporting layer.
    transfer_failures: u64,
    // Set when a comparison answers something the run cannot act on.
    plan_incomplete: bool,
    // What the walk knew when a work item last handed it back. A work item holds the walk for as
    // long as it runs, so the run copies the answer out as each item returns.
    //
    // The run adds to this flag and never overwrites it, so a hole stays reported whatever the
    // walk says later. Overwriting would hold only if nothing ever cleared the walk's own flag,
    // and another module could.
    walk_plan_incomplete: bool,
    // `FAILURES_KEPT` caps the list, and `failures_dropped` counts the rest.
    failures: Vec<StreamError>,
    failures_dropped: u64,
}

pub(crate) struct SyncTransfer<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    inner: Arc<Inner<S, D>>,
}

impl<S: KeyStream, D: KeyStream> Clone for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncTransfer<S, D>
where
    S::Source: 'static,
    D::Source: 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTransfer").finish_non_exhaustive()
    }
}

struct Inner<S: KeyStream, D: KeyStream>
where
    S::Source: 'static,
    D::Source: 'static,
{
    ctx: TransferContext,
    state: Mutex<State<S, D>>,
    // Which comparison to ask. The caller passes it in, because an upload and a download
    // disagree about which side being newer wins.
    comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
    // The other one. Unlike the comparison this holds state, because building a child needs the
    // client and the bucket.
    spawner: Arc<dyn SpawnChild<S::Source>>,
    // How many children may be live at once. One slot is a share of what the whole client has, so
    // whoever starts a run sets it.
    max_children: usize,
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
        walk: Walk<S, D>,
        comparison: &'static (dyn Compare<S::Source, D::Source> + Send + Sync),
        spawner: Arc<dyn SpawnChild<S::Source>>,
        max_children: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                ctx,
                comparison,
                spawner,
                max_children: max_children.max(1),
                state: Mutex::new(State {
                    walk: Some(walk),
                    merge_in_flight: false,
                    paired: 0,
                    decided: Decided::default(),
                    waiting: VecDeque::new(),
                    children: std::collections::HashMap::new(),
                    reap_in_flight: 0,
                    bytes_moved: 0,
                    transferred: 0,
                    transfer_failures: 0,
                    plan_incomplete: false,
                    walk_plan_incomplete: false,
                    failures: Vec::new(),
                    failures_dropped: 0,
                }),
            }),
        }
    }

    // Whether the comparison reached every key. Two things leave holes, and they are independent.
    // First, a stream the walk could not finish reading. Second, a comparison the run could not act
    // on. Either one alone means the plan has holes.
    //
    // The run reads both flags from its own state and never asks the walk. An absent walk answers
    // nothing, so asking the question would let the same run report its plan as complete at one
    // moment and incomplete at another.
    pub(crate) fn is_plan_complete(&self) -> bool {
        let state = self.inner.state.lock();
        !state.plan_incomplete && !state.walk_plan_incomplete
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let active = self.inner.ctx.is_active();
        let mut state = self.inner.state.lock();

        if active {
            if let Some(work) = self.dispatch_merge(&mut state) {
                return work;
            }
            if self.spawn_one(&mut state) {
                return PollWork::Spawned;
            }
        }

        // Reaping runs whether or not the transfer is still active: a child that has ended has an
        // outcome owed to the run, and cancelling does not make it go away.
        if let Some(work) = self.dispatch_reap(&mut state) {
            return work;
        }

        if let Some(done) = self.check_terminal(&mut state) {
            return done;
        }

        // Mark the transfer as waiting before returning. `try_wake` only signals a transfer
        // already marked, so without the mark a finishing work item signals nothing. The transfer
        // would then sit outside the ready set with nothing to put it back.
        self.inner.ctx.set_pending();
        PollWork::Pending
    }

    // Signal telling the caller whether the run is over. Answering `Done` with `merge_in_flight`
    // still set would report a finished run while the scheduler holds a work item, and the keys in
    // that item would go unreported.
    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if !self.inner.ctx.is_active() {
            // Everything dispatched is still owed an answer, however the run ended.
            if state.merge_in_flight || state.reap_in_flight > 0 || !state.children.is_empty() {
                return None;
            }
            // Cancelled or failed: the run already recorded the outcome, so the only thing left
            // is to signal the caller.
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        if !state.merge_in_flight
            && state.waiting.is_empty()
            && state.children.is_empty()
            && state.reap_in_flight == 0
            && state.walk.as_ref().is_some_and(Walk::is_done)
        {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
    }

    // Turn one waiting decision into a child, if a slot is free. `true` means the run enqueued a
    // child. The scheduler charges for the spawn without taking a dispatch ticket, because the
    // child takes its own ticket once the scheduler polls it.
    fn spawn_one(&self, state: &mut State<S, D>) -> bool {
        // Only live children count. One sitting in a reap has already ended and holds no network or
        // disk concurrency. Counting it would idle a share of the budget for the length of every
        // reap.
        if state.children.len() >= self.inner.max_children {
            return false;
        }

        // Keep going past anything that cannot become a child, so one key nothing can act on does
        // not strand the keys behind it. Answering the poll with no child enqueued leaves the run
        // waiting, with keys still buffered and no work item left to signal.
        while let Some((pairing, _)) = state.waiting.pop_front() {
            let Some(entry) = pairing.source().entry() else {
                // A transfer is only decided for a source that is present, so reaching here means a
                // comparison answered something it had no grounds for.
                state.transfer_failures += 1;
                continue;
            };
            match self
                .inner
                .spawner
                .spawn(pairing.key(), &entry.source, self.inner.ctx.id.id)
            {
                Ok(child) => {
                    state.children.insert(child.id(), child);
                    return true;
                }
                Err(_) => {
                    state.transfer_failures += 1;
                    continue;
                }
            }
        }
        false
    }

    // Collect children that have ended into a work item. They leave `children` here, so
    // `reap_in_flight` stands in for them until their outcomes are back.
    fn dispatch_reap(&self, state: &mut State<S, D>) -> Option<PollWork> {
        let finished: Vec<crate::transfer::TransferId> = state
            .children
            .iter()
            .filter(|(_, child)| child.is_finished())
            .map(|(id, _)| *id)
            .collect();
        if finished.is_empty() {
            return None;
        }
        let children: Vec<ChildHandle> = finished
            .into_iter()
            .map(|id| state.children.remove(&id).expect("id came from this map"))
            .collect();
        state.reap_in_flight += children.len();
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::<S, D>::ReapChildren { children })),
        }))
    }

    // Hand the merge to a work item. `None` leaves the poll to choose between `Done` and
    // `Pending`.
    fn dispatch_merge(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if state.merge_in_flight {
            return None;
        }

        let walk = match state.walk.take() {
            Some(walk) if !walk.is_done() => walk,
            // Exhausted. Put it back so the completion test can see it.
            Some(walk) => {
                state.walk = Some(walk);
                return None;
            }
            None => return None,
        };

        state.merge_in_flight = true;
        Some(PollWork::ready(IoRequest {
            data: Some(Box::new(SyncWork::AdvanceMerge {
                walk: Some(Box::new(walk)),
            })),
        }))
    }

    pub(crate) async fn execute(&self, work: &mut IoRequest) -> WorkOutcome {
        let work: &mut SyncWork<S, D> = work.data_mut();
        match work {
            SyncWork::AdvanceMerge { walk } => {
                let walk = walk.take().expect("the merge was already taken");
                self.execute_advance_merge(*walk).await
            }
            SyncWork::ReapChildren { children } => {
                let children = std::mem::take(children);
                self.execute_reap(children).await
            }
        }
    }

    async fn execute_reap(&self, children: Vec<ChildHandle>) -> WorkOutcome {
        let count = children.len();
        let mut moved = 0u64;
        let mut arrived = 0u64;
        let mut failed = 0u64;
        for child in children {
            match child.join().await {
                Ok(bytes) => {
                    arrived += 1;
                    moved += bytes;
                }
                Err(_) => failed += 1,
            }
        }

        let mut state = self.inner.state.lock();
        state.reap_in_flight -= count;
        state.bytes_moved += moved;
        state.transferred += arrived;
        state.transfer_failures += failed;
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }

    async fn execute_advance_merge(&self, mut walk: Walk<S, D>) -> WorkOutcome {
        if !self.inner.ctx.is_active() {
            let mut state = self.inner.state.lock();
            state.walk = Some(walk);
            state.merge_in_flight = false;
            return WorkOutcome::Cancelled;
        }

        let mut paired = 0u64;
        let mut decided = Decided::default();
        let mut deferred = false;
        let mut failures = Vec::new();
        let mut batch = VecDeque::new();

        for _ in 0..MERGE_BATCH {
            match walk.next().await {
                Some(Ok(pairing)) => {
                    paired += 1;
                    // TODO(sync): Map this comparison `Decision` into the per-entry
                    // transfer event record before dispatching its work.
                    match self.inner.comparison.compare(&pairing) {
                        Verdict::Decided(decision @ Decision::Transfer(_)) => {
                            decided.transfers += 1;
                            batch.push_back((pairing, decision));
                        }
                        // Counted and let go: nothing here issues a delete, so buffering one
                        // would hold the run open for work that has no home yet.
                        Verdict::Decided(Decision::Delete(_)) => decided.deletes += 1,
                        // A skip needs nothing done to it, so it never waits for a slot.
                        Verdict::Decided(Decision::Skip(_)) => decided.skips += 1,
                        // Nothing shipped here defers, so one arriving is a defect in whatever
                        // comparison produced it. The run skips the key and records the plan as
                        // incomplete. Sending the key would waste bandwidth, and dropping it
                        // silently would leave a file nobody hears about. The run keeps a count and
                        // a flag, and names no key.
                        Verdict::Deferred(_) => {
                            decided.skips += 1;
                            deferred = true;
                        }
                    }
                }
                Some(Err(err)) => failures.push(err),
                None => break,
            }
        }

        let mut state = self.inner.state.lock();
        state.walk_plan_incomplete |= !walk.is_plan_complete();
        state.walk = Some(walk);
        state.merge_in_flight = false;
        state.paired += paired;
        state.decided += decided;
        state.plan_incomplete |= deferred;
        state.waiting.append(&mut batch);
        for failure in failures {
            if state.failures.len() < FAILURES_KEPT {
                state.failures.push(failure);
            } else {
                state.failures_dropped += 1;
            }
        }

        // Draining the last batch makes this work item end the run, so it tells the caller here
        // and nothing has to poll again.
        if self.check_terminal(&mut state).is_some() {
            drop(state);
            return WorkOutcome::Success { data: None };
        }
        drop(state);

        // Without the signal the run waits forever. The poll handing this item over answered
        // `Pending`, the transfer left the scheduler's ready set, and nothing else puts it back.
        self.inner.ctx.try_wake();
        WorkOutcome::Success { data: None }
    }
}

impl<S, D> Transfer for SyncTransfer<S, D>
where
    S: KeyStream + Send + Sync + fmt::Debug + 'static,
    D: KeyStream + Send + Sync + fmt::Debug + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    fn ctx(&self) -> &TransferContext {
        &self.inner.ctx
    }

    fn poll_work(&self) -> PollWork {
        SyncTransfer::poll_work(self)
    }

    fn execute<'a>(
        &'a self,
        work: &'a mut IoRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = WorkOutcome> + Send + 'a>> {
        Box::pin(SyncTransfer::execute(self, work))
    }
}

// Upload and download each implement `Transfer`.
// A trait bound that excludes either direction fails to compile here.
const _: fn() = || {
    fn implements_transfer<T: Transfer>() {}
    implements_transfer::<SyncTransfer<crate::io::walk::FsWalk, crate::io::walk::S3Walk>>();
    implements_transfer::<
        SyncTransfer<crate::io::walk::S3Walk, crate::operation::sync::walk::LocalDestination>,
    >();
};

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};

    use crate::io::walk::{FsWalk, S3Walk};
    use crate::operation::sync::modes::Mode;
    use crate::operation::sync::walk::{LocalAndBucket, Walker};

    use super::*;

    fn a_bucket_holding(keys: &[&str]) -> aws_sdk_s3::Client {
        let contents: Vec<Object> = keys
            .iter()
            .map(|k| {
                Object::builder()
                    .key(*k)
                    .size(0)
                    .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                    .build()
            })
            .collect();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        // A spawned child puts an object, so the bucket has to accept writes as well as answer a
        // listing.
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| aws_sdk_s3::operation::put_object::PutObjectOutput::builder().build());
        mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &put])
    }

    fn a_local_tree(root: &Path, keys: &[&str]) {
        for key in keys {
            let path = root.join(key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("a parent directory");
            }
            std::fs::write(&path, b"").expect("a file");
        }
    }

    fn uploading(
        local: &Path,
        bucket_keys: &[&str],
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(bucket_keys);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);

        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        (
            SyncTransfer::new(
                ctx.clone(),
                walk,
                Mode::default().uploading(),
                Arc::new(SpawnEnded::new(0, false)),
                2,
            ),
            ctx,
        )
    }

    // Drive the loop the way the scheduler does, under a deadline. A lost signal shows up as a
    // hang and not a failure, so without the deadline the test would never report.
    async fn drive(transfer: &SyncTransfer<FsWalk, S3Walk>) -> u64 {
        let run = async {
            loop {
                match transfer.poll_work() {
                    PollWork::Ready { io: mut work, .. } => {
                        transfer.execute(&mut work).await;
                    }
                    PollWork::Done => break,
                    PollWork::Pending => {
                        panic!("nothing else can make progress, so this would park forever")
                    }
                    PollWork::Spawned => {}
                }
            }
            transfer.inner.state.lock().paired
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the run parked: a poll answered Pending with no wake to follow")
    }

    struct SpawnEnded {
        moved: u64,
        fails: bool,
        asked: std::sync::atomic::AtomicUsize,
        // Shared by every child it hands out, so a test can hold them all open and then release
        // them together.
        ended: Arc<std::sync::atomic::AtomicBool>,
    }

    impl SpawnEnded {
        fn new(moved: u64, fails: bool) -> Self {
            Self {
                moved,
                fails,
                asked: std::sync::atomic::AtomicUsize::new(0),
                ended: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            }
        }

        fn holding_children_open() -> Self {
            let spawner = Self::new(0, false);
            spawner
                .ended
                .store(false, std::sync::atomic::Ordering::SeqCst);
            spawner
        }

        fn asked_count(&self) -> usize {
            self.asked.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn release(&self) {
            self.ended.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl SpawnChild<crate::io::walk::FsEntry> for SpawnEnded {
        fn spawn(
            &self,
            _key: &str,
            _source: &crate::io::walk::FsEntry,
            _parent: u64,
        ) -> Result<ChildHandle, crate::error::Error> {
            let n = self.asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ChildHandle {
                id: crate::transfer::TransferId {
                    id: 900_000 + n as u64,
                    parent: None,
                },
                inner: ChildInner::Controlled {
                    ended: self.ended.clone(),
                    moved: self.moved,
                    failed: self.fails,
                },
            })
        }
    }

    struct AlwaysDefers;

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysDefers {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden, so no arm reaches a described pair")
        }

        fn compare(
            &self,
            _pairing: &crate::operation::sync::walk::Pairing<
                crate::io::walk::FsEntry,
                aws_sdk_s3::types::Object,
            >,
        ) -> Verdict {
            Verdict::Deferred(crate::operation::sync::compare::Deferred {})
        }
    }

    // Every pairing produces exactly one decision, so the three counts account for every key the
    // merge paired. A key counted twice or not at all would show here.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn every_pairing_yields_exactly_one_decision() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);

        let paired = drive(&transfer).await;
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers + state.decided.deletes + state.decided.skips,
            paired,
            "the decisions do not account for every key that was paired"
        );
        assert_eq!(state.decided.deletes, 1, "d.txt is on the bucket alone");
    }

    // A verdict the run cannot act on carries two obligations. First, the run skips the key.
    // Second, the run records the plan as incomplete. Counting the key while calling the plan
    // complete would report a complete plan missing a key.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_deferred_verdict_is_skipped_and_shortens_the_plan() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            &AlwaysDefers,
            Arc::new(SpawnEnded::new(0, false)),
            2,
        );

        let paired = drive(&transfer).await;
        assert_eq!(paired, 2);
        let state = transfer.inner.state.lock();
        assert_eq!(state.decided.skips, 2, "a deferred key was not skipped");
        assert_eq!(state.decided.transfers, 0);
        assert!(
            state.plan_incomplete,
            "the plan is short two keys and does not say so"
        );
    }

    // The two flags are independent, and either alone leaves the plan short.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_plan_is_whole_only_when_neither_flag_is_set() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (transfer, _ctx) = uploading(dir.path(), &[]);

        for (mine, walks, whole) in [
            (false, false, true),
            (true, false, false),
            (false, true, false),
            (true, true, false),
        ] {
            {
                let mut state = transfer.inner.state.lock();
                state.plan_incomplete = mine;
                state.walk_plan_incomplete = walks;
            }
            assert_eq!(
                transfer.is_plan_complete(),
                whole,
                "a deferred verdict of {mine} and an unread stream of {walks} answered wrongly"
            );
        }
    }

    // The answer is about the plan, not about what is happening right now, so taking the merge away
    // for a work item must not change it.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn taking_the_merge_away_does_not_change_the_answer() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, _ctx) = uploading(dir.path(), &[]);

        let before = transfer.is_plan_complete();
        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        assert!(
            transfer.inner.state.lock().walk.is_none(),
            "the merge is still in state, so nothing was taken away"
        );
        assert_eq!(
            transfer.is_plan_complete(),
            before,
            "the answer changed while a work item held the merge"
        );

        transfer.execute(&mut work).await;
        assert!(
            transfer.is_plan_complete(),
            "a clean run reported a short plan"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_over_two_empty_sides_is_over_at_once() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let (transfer, _ctx) = uploading(dir.path(), &[]);
        assert_eq!(drive(&transfer).await, 0);
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn every_key_either_side_holds_is_paired_once() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
        // `b.txt` is on both sides, so the union is four keys, not five.
        let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);
        assert_eq!(drive(&transfer).await, 4);
        assert!(
            transfer.inner.state.lock().failures.is_empty(),
            "a well-formed listing produced a failure"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_key_the_listing_described_badly_is_kept_as_a_failure() {
        let dir = tempfile::tempdir().expect("a temp dir");
        // No last-modified. The walk calls such a listing malformed.
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
            ListObjectsV2Output::builder()
                .set_contents(Some(vec![Object::builder().key("d.txt").size(0).build()]))
                .build()
        });
        let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            2,
        );

        assert_eq!(
            drive(&transfer).await,
            0,
            "no key was described well enough to pair"
        );
        assert_eq!(
            transfer.inner.state.lock().failures.len(),
            1,
            "the run ended without keeping what it could not account for"
        );
        // The walk sets its own flag for this, so the plan is short by the key nobody could
        // describe. This is the half the four-case table cannot reach: that one sets both flags by
        // hand, so it proves how they combine and not where either value came from.
        assert!(
            !transfer.is_plan_complete(),
            "a key the listing described badly left the plan looking whole"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_tree_larger_than_one_batch_takes_several_work_items() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..MERGE_BATCH * 3)
            .map(|n| format!("f{n:04}.txt"))
            .collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let (transfer, _ctx) = uploading(dir.path(), &[]);
        let mut items = 0;
        loop {
            match transfer.poll_work() {
                PollWork::Ready { io: mut work, .. } => {
                    items += 1;
                    transfer.execute(&mut work).await;
                }
                PollWork::Spawned => {}
                PollWork::Done => break,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(transfer.inner.state.lock().paired, keys.len() as u64);
        assert!(
            items > 1,
            "a tree of {} keys came back in one work item, so the batch bound did nothing",
            keys.len()
        );
    }

    // The only test leaving the re-polling to the scheduler. Every other one calls `poll_work` in
    // a loop and never needs a signal to come back. So this is the one test failing if a work item
    // finishes without signalling. The tree has to be larger than one batch: a run finishing
    // inside a single work item signals from `execute` and never waits.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_scheduler_gets_the_run_to_the_end_on_its_own() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..MERGE_BATCH * 3)
            .map(|n| format!("f{n:04}.txt"))
            .collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        // Managed threads, so dispatched work actually runs and the signal brings the transfer back
        // for the poll that ends it.
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            2,
        );

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run parked: a work item finished without waking the transfer")
            .expect("the terminal signal was dropped");

        assert_eq!(transfer.inner.state.lock().paired, keys.len() as u64);
        assert!(
            keys.len() > MERGE_BATCH,
            "the tree fits in one work item, so the run never parked and the wake went untested"
        );
    }

    // A run holding a delete batch the scheduler has not answered must not report itself over.
    // Emptying the merge by hand keeps the transfer active, so the assertion lands on the
    // completion test itself. Emptying it through `execute` would mark the transfer completed, the
    // poll would answer through the inactive path, and the test would prove nothing.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_batch_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut walk = transfer.inner.state.lock().walk.take().expect("the merge");
        while walk.next().await.is_some() {}
        assert!(walk.is_done(), "the merge did not reach the end");
        {
            let mut state = transfer.inner.state.lock();
            state.walk = Some(walk);
            state.merge_in_flight = true;
        }
        assert!(ctx.is_active(), "the transfer ended before the assertion");

        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a finished merge with a batch still out reported the run over"
        );

        transfer.inner.state.lock().merge_in_flight = false;
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "with nothing outstanding the run is not over"
        );
    }

    // A run finishing inside a work item tells its caller there. Nothing else will. The scheduler
    // tells a caller when it cancels a transfer, and when a work item fails, and its `Done` branch
    // drops a completed transfer silently. So the last item to empty the merge has to do it, and
    // this test asserts that without letting another poll run.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn the_item_that_drains_the_merge_answers_the_waiter() {
        // Nothing on either side, so the merge advance is the only work item and the one ending the
        // run. A key to transfer would make the last item a reap, and the claim would be about the
        // reap path. No further poll runs here.
        let dir = tempfile::tempdir().expect("a temp dir");

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnEnded::new(0, false)),
            2,
        );

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        transfer.execute(&mut work).await;

        // No further poll. The caller has to already have its answer, and the deadline turns a
        // missing signal into a failure the test can see.
        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run drained inside a work item and left its waiter unanswered")
            .expect("the terminal signal was dropped");
    }

    // A run whose every entry fails must not grow with the tree. The walk reports most failures
    // per entry and carries on, so what stops peak memory tracking the entry count is the cap.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_where_everything_fails_keeps_a_bounded_sample() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("a temp dir");
        // Each unreadable directory costs one failure the walk reports and carries on from.
        let wanted = FAILURES_KEPT + 40;
        let mut locked = Vec::new();
        for n in 0..wanted {
            let sub = dir.path().join(format!("d{n:04}"));
            std::fs::create_dir(&sub).expect("a directory");
            std::fs::write(sub.join("inner.txt"), b"").expect("a file inside it");
            std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o000)).expect("chmod");
            locked.push(sub);
        }
        let usable = locked.iter().all(|p| std::fs::read_dir(p).is_err());

        let (transfer, _ctx) = uploading(dir.path(), &[]);
        drive(&transfer).await;

        for sub in &locked {
            std::fs::set_permissions(sub, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        assert!(
            usable,
            "the fixture did not produce unreadable directories, so nothing was tested"
        );

        let state = transfer.inner.state.lock();
        assert!(
            state.failures.len() <= FAILURES_KEPT,
            "the run kept {} failures, so peak memory follows the tree",
            state.failures.len()
        );
        assert!(
            state.failures_dropped > 0,
            "nothing was dropped, so the cap was never reached and this proves nothing"
        );
    }

    fn spawn_until_something_else(transfer: &SyncTransfer<FsWalk, S3Walk>) -> PollWork {
        loop {
            match transfer.poll_work() {
                PollWork::Spawned => continue,
                other => return other,
            }
        }
    }

    // Builds a transfer whose children a test controls, so spawning and reaping can be driven a step
    // at a time.
    // A run reports the transfers that arrived, not the ones it decided to send.
    //
    // The two counts come from opposite ends of the run. The comparison picks a key out and
    // `decided.transfers` moves; the reap joins a child and `transferred` moves. A caller
    // reconciling the run against the destination reads the second. Deriving it from the first
    // means subtracting whatever intervened, and a key that never got a child survives that
    // subtraction to read as an arrival.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_run_reports_the_transfers_that_arrived() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        // Every child this spawner hands out has already ended badly. Both keys get a decision
        // and neither arrives.
        let spawner = Arc::new(SpawnEnded::new(7, true));
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers, 2,
            "the run decided {} transfers for two keys",
            state.decided.transfers
        );
        assert_eq!(
            state.transferred, 0,
            "both children ended badly and the run reports {} arrivals",
            state.transferred
        );
    }

    fn uploading_with(
        local: &Path,
        spawner: Arc<SpawnEnded>,
        cap: usize,
    ) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer =
            SyncTransfer::new(ctx.clone(), walk, Mode::default().uploading(), spawner, cap);
        (transfer, ctx)
    }

    // Each term below holds the run open on its own.

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_decision_still_waiting_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        // A cap of zero clamps to one, so fill the one slot and leave a second decision waiting.
        a_local_tree(dir.path(), &["b.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 1);

        // Drain the merge, then spawn until the slot is full.
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let parked = spawn_until_something_else(&transfer);

        let state = transfer.inner.state.lock();
        assert!(
            !state.waiting.is_empty(),
            "nothing is waiting, so this proves nothing about the buffer"
        );
        drop(state);
        assert!(
            matches!(parked, PollWork::Pending),
            "a decision still waiting for a slot did not hold the run open"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_live_child_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let parked = spawn_until_something_else(&transfer);

        assert_eq!(spawner.asked_count(), 1, "no child was spawned");
        assert!(
            matches!(parked, PollWork::Pending),
            "a child that has not ended did not hold the run open"
        );

        // Release it and the run finishes through a reap.
        spawner.release();
        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_reap_still_out_holds_the_run_open() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, _ctx) = uploading_with(dir.path(), spawner, 4);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        // Spawning ends with the poll that produces the reap, so this is that work item.
        let mut reap = match spawn_until_something_else(&transfer) {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a reap, got {other:?}"),
        };
        {
            let state = transfer.inner.state.lock();
            assert!(state.children.is_empty(), "the child is still in the map");
            assert_eq!(state.reap_in_flight, 1, "the reap is not accounted for");
        }
        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a reap still out did not hold the run open, so its child's outcome would be lost"
        );

        transfer.execute(&mut reap).await;
        assert!(matches!(transfer.poll_work(), PollWork::Done));
    }

    // The cap is the caller's share of the client, so it bounds live children and not the number
    // the run has asked for.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn no_more_children_are_live_than_the_cap_allows() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let keys: Vec<String> = (0..12).map(|n| format!("f{n:02}.txt")).collect();
        let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        a_local_tree(dir.path(), &refs);

        let spawner = Arc::new(SpawnEnded::holding_children_open());
        let cap = 3;
        let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), cap);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }
        let mut seen = 0usize;
        let parked = loop {
            match transfer.poll_work() {
                PollWork::Spawned => {
                    let live = transfer.inner.state.lock().children.len();
                    seen = seen.max(live);
                    assert!(
                        live <= cap,
                        "{live} children were live against a cap of {cap}"
                    );
                }
                other => break other,
            }
        };
        assert_eq!(seen, cap, "the cap was never reached, so it was not tested");
        assert!(
            matches!(parked, PollWork::Pending),
            "with every slot full and nine keys still waiting, the poll had nothing else to answer"
        );
        assert_eq!(
            spawner.asked_count(),
            cap,
            "more children were built than the cap allows"
        );
    }

    struct AlwaysTransfers;

    impl Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> for AlwaysTransfers {
        fn compare_described(
            &self,
            _source: crate::operation::sync::compare::Described<'_, crate::io::walk::FsEntry>,
            _destination: crate::operation::sync::compare::Described<'_, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            unreachable!("compare is overridden")
        }

        fn compare(
            &self,
            _pairing: &Pairing<crate::io::walk::FsEntry, aws_sdk_s3::types::Object>,
        ) -> Verdict {
            Verdict::decided(Decision::transfer(
                crate::operation::sync::compare::TransferReason::Missing,
            ))
        }
    }

    // A key with nothing to send cannot become a child, and the run has to carry on past it.
    // Answering the poll with nothing enqueued leaves the run waiting, with the rest of the buffer
    // still in it and no work item left to signal. The run hangs and reports nothing.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_transfer_decided_on_an_absent_source_does_not_strand_the_rest() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["b.txt"]);
        // `a.txt` is on the bucket alone, so its pairing has no source — and this comparison still
        // calls for a transfer.
        let client = a_bucket_holding(&["a.txt"]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let transfer = SyncTransfer::new(ctx, walk, &AlwaysTransfers, spawner.clone(), 4);

        // `drive` panics on `Pending`, and a stranded buffer produces a `Pending`.
        let paired = drive(&transfer).await;
        assert_eq!(paired, 2);
        let state = transfer.inner.state.lock();
        assert_eq!(
            state.transfer_failures, 1,
            "the key with no source was not accounted for"
        );
        assert_eq!(
            spawner.asked_count(),
            1,
            "the key behind it was never spawned"
        );
    }

    // The builder stats the path whenever metadata is missing, so a file gone away between the walk
    // and the spawn separates the two cases. With the walk's metadata the body still builds.
    // Without it the stat fails. The poll therefore reads no size the comparison has already
    // decided from.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn spawning_uses_the_size_the_walk_read() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("a.txt");
        std::fs::write(&path, b"0123456789").expect("a file");

        let mut walked = crate::io::walk::FsWalker::builder().build().walk(
            crate::io::walk::FsWalkContext::builder()
                .root(dir.path())
                .build(),
        );
        // One file in the tree, so the first entry is it.
        let entry = match walked.next().await {
            Some(Ok(entry)) => entry,
            Some(Err(err)) => panic!("the walk failed: {err}"),
            None => panic!("the walk produced nothing"),
        };
        assert!(
            entry.metadata().is_some(),
            "the walk read no metadata, so this test cannot tell the two paths apart"
        );

        // Remove the file. A spawn that stats again fails here; one that uses what the walk read
        // does not.
        std::fs::remove_file(&path).expect("remove");

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let spawner = SpawnUpload::new(handle, "amzn-s3-demo-bucket", None);
        assert!(
            spawner.spawn("a.txt", &entry, 1).is_ok(),
            "the spawn stat'd the path again instead of using the size the walk read"
        );
    }

    // Both sides compare on keys relative to their own root. The listing strips the run's prefix
    // off, so naming an object means putting the prefix back. A mocked `PutObject` accepts any key,
    // so a test watching only for the call cannot tell writing under the prefix from writing to the
    // bucket root.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn an_uploaded_key_is_named_under_the_runs_prefix() {
        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::write(dir.path().join("a.txt"), b"x").expect("a file");

        let mut walked = crate::io::walk::FsWalker::builder().build().walk(
            crate::io::walk::FsWalkContext::builder()
                .root(dir.path())
                .build(),
        );
        let entry = match walked.next().await {
            Some(Ok(entry)) => entry,
            other => panic!("expected one file, got {other:?}"),
        };

        let config = crate::Config::builder()
            .client(a_bucket_holding(&[]))
            .build();
        let handle = crate::client::Handle::test_handle_tokio(config);

        for (prefix, expected) in [
            (None, "a.txt"),
            (Some("data"), "data/a.txt"),
            (Some("data/"), "data/a.txt"),
            (Some(""), "a.txt"),
        ] {
            let spawner = SpawnUpload::new(handle.clone(), "amzn-s3-demo-bucket", prefix);
            let key = spawner.object_key("a.txt");
            assert_eq!(
                key, expected,
                "a run under prefix {prefix:?} would have written to {key}"
            );
            // The whole path still has to build, so this covers more than the key.
            assert!(spawner.spawn("a.txt", &entry, 1).is_ok());
        }
    }

    // The only test using the real spawner. Every other one hands back a controlled child, so no
    // other test shows an object reaching the bucket. Managed threads, because a real child is a
    // scheduled transfer and the tokio test handle never runs dispatched work.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_qualified_key_reaches_the_bucket() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, completion_rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(dir.path())
                    .client(client)
                    .bucket("amzn-s3-demo-bucket")
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let spawner = Arc::new(SpawnUpload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            None,
        ));
        let transfer =
            SyncTransfer::new(ctx.clone(), walk, Mode::default().uploading(), spawner, 4);

        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));

        tokio::time::timeout(Duration::from_secs(20), completion_rx)
            .await
            .expect("the run never finished, so a child was spawned and never reaped")
            .expect("the terminal signal was dropped");

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.decided.transfers, 2,
            "both keys should have been sent"
        );
        assert_eq!(
            state.transfer_failures, 0,
            "a child failed, so the upload path is not working"
        );
        assert!(
            state.children.is_empty() && state.reap_in_flight == 0,
            "the run ended with children unaccounted for"
        );
    }

    // Cancelling does not make a dispatched work item go away. Reporting the run over while one is
    // out would have the scheduler retire the transfer under an item still holding the merge.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_waits_for_the_batch_it_dispatched() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        ctx.set_cancelled();

        assert!(
            matches!(transfer.poll_work(), PollWork::Pending),
            "a cancelled run reported itself over with a batch still out"
        );

        transfer.execute(&mut work).await;
        assert!(
            matches!(transfer.poll_work(), PollWork::Done),
            "the batch came back and the cancelled run still did not end"
        );
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_cancelled_run_hands_the_merge_back() {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, ctx) = uploading(dir.path(), &[]);

        let mut work = match transfer.poll_work() {
            PollWork::Ready { io, .. } => io,
            other => panic!("expected a work item, got {other:?}"),
        };
        ctx.set_cancelled();

        assert!(matches!(
            transfer.execute(&mut work).await,
            WorkOutcome::Cancelled
        ));
        let state = transfer.inner.state.lock();
        assert!(
            state.walk.is_some() && !state.merge_in_flight,
            "a cancelled work item left the merge where no poll can reach it"
        );
    }
}
