/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use parking_lot::Mutex;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use crate::io::key::stream::{KeyStream, StreamError};
use crate::operation::sync::walk::Walk;
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

// The merge, moved out of `State` for as long as one work item holds it. `next` needs
// `&mut`, and holding the state lock across it would block every poll on this transfer.
pub(crate) enum SyncWork<S: KeyStream, D: KeyStream> {
    AdvanceMerge { walk: Option<Box<Walk<S, D>>> },
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncWork<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncWork::AdvanceMerge { .. } => f.write_str("AdvanceMerge"),
        }
    }
}

struct State<S: KeyStream, D: KeyStream> {
    // `None` only while a work item holds the merge. `Walk::next` needs `&mut`, and holding the
    // state lock across it would block every poll on this transfer, so the merge moves out of the
    // state and into the item for as long as the item runs.
    walk: Option<Walk<S, D>>,
    merge_in_flight: bool,
    paired: u64,
    // Capped by `FAILURES_KEPT`; anything past that is counted in `failures_dropped`.
    failures: Vec<StreamError>,
    failures_dropped: u64,
}

pub(crate) struct SyncTransfer<S: KeyStream, D: KeyStream> {
    inner: Arc<Inner<S, D>>,
}

impl<S: KeyStream, D: KeyStream> Clone for SyncTransfer<S, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: KeyStream, D: KeyStream> fmt::Debug for SyncTransfer<S, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncTransfer").finish_non_exhaustive()
    }
}

struct Inner<S: KeyStream, D: KeyStream> {
    ctx: TransferContext,
    state: Mutex<State<S, D>>,
}

impl<S, D> SyncTransfer<S, D>
where
    S: KeyStream + Send + 'static,
    D: KeyStream + Send + 'static,
    S::Source: Send,
    D::Source: Send,
{
    pub(crate) fn new(ctx: TransferContext, walk: Walk<S, D>) -> Self {
        Self {
            inner: Arc::new(Inner {
                ctx,
                state: Mutex::new(State {
                    walk: Some(walk),
                    merge_in_flight: false,
                    paired: 0,
                    failures: Vec::new(),
                    failures_dropped: 0,
                }),
            }),
        }
    }

    pub(crate) fn poll_work(&self) -> PollWork {
        let active = self.inner.ctx.is_active();
        let mut state = self.inner.state.lock();

        if active {
            if let Some(work) = self.dispatch_merge(&mut state) {
                return work;
            }
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

    // Signal telling the caller whether the run is over. Answering `Done` while
    // `merge_in_flight` is set would report a finished run while the scheduler still holds a work
    // item, and the keys in that item would go unreported.
    fn check_terminal(&self, state: &mut State<S, D>) -> Option<PollWork> {
        if !self.inner.ctx.is_active() {
            if state.merge_in_flight {
                return None;
            }
            // Cancelled or failed: the run already recorded the outcome, so the only thing left
            // is to signal the caller.
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }

        if !state.merge_in_flight && state.walk.as_ref().is_some_and(Walk::is_done) {
            self.inner.ctx.set_completed();
            self.inner.ctx.signal_terminal();
            return Some(PollWork::Done);
        }
        None
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
        }
    }

    async fn execute_advance_merge(&self, mut walk: Walk<S, D>) -> WorkOutcome {
        if !self.inner.ctx.is_active() {
            let mut state = self.inner.state.lock();
            state.walk = Some(walk);
            state.merge_in_flight = false;
            return WorkOutcome::Cancelled;
        }

        let mut paired = 0u64;
        let mut failures = Vec::new();

        for _ in 0..MERGE_BATCH {
            match walk.next().await {
                Some(Ok(_pairing)) => paired += 1,
                Some(Err(err)) => failures.push(err),
                None => break,
            }
        }

        let mut state = self.inner.state.lock();
        state.walk = Some(walk);
        state.merge_in_flight = false;
        state.paired += paired;
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
    S::Source: Send,
    D::Source: Send,
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
    use crate::operation::sync::walk::{LocalAndBucket, Walker};

    use super::*;

    // A bucket that answers one page and nothing more. Every object carries a last-modified.
    // Without one, the walk reports the listing malformed and produces no key.
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
        let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .set_contents(Some(contents.clone()))
                .build()
        });
        mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&rule])
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

    // An upload-direction transfer over a real local tree and a mocked bucket. The scheduler
    // holds it, so a poll answering `Pending` is set aside the way a real run sets it aside.
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
        (SyncTransfer::new(ctx.clone(), walk), ctx)
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
                    PollWork::Spawned => unreachable!("nothing spawns children yet"),
                }
            }
            transfer.inner.state.lock().paired
        };
        tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("the run parked: a poll answered Pending with no wake to follow")
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
        let transfer = SyncTransfer::new(ctx, walk);

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
        // Managed threads, so dispatched work actually runs and the wake is what brings the
        // transfer back. The tokio test handle never drains dispatched work.
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
        let transfer = SyncTransfer::new(ctx.clone(), walk);

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
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);

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
        let transfer = SyncTransfer::new(ctx, walk);

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
