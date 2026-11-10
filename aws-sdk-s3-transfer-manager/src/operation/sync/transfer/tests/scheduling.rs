/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run schedules its children, batches and reaps, and when the merge
// pauses.

use super::*;

// The merge decides far faster than the children send. With every child busy, the merge stops
// once the queue holds `MERGE_LOW_WATER` transfers, and it resumes as children finish.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_with_every_child_busy_stops_deciding_once_its_queue_is_full() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let names: Vec<String> = (0..3000).map(|i| format!("{i:04}.txt")).collect();
    a_local_tree(
        dir.path(),
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with_policy(
        dir.path(),
        spawner.clone(),
        4,
        FailedTransferPolicy::Continue,
    );

    loop {
        match transfer.poll_work() {
            PollWork::Ready { io: mut work, .. } => {
                transfer.execute(&mut work).await;
            }
            PollWork::Spawned => {}
            _ => break,
        }
    }
    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.paired < 3000,
        "the merge decided every key while its children were all busy"
    );
    assert!(
        snapshot.transfers_waiting <= MERGE_LOW_WATER + MERGE_BATCH,
        "{} transfers waited for 4 children",
        snapshot.transfers_waiting
    );

    spawner.release(&ctx);
    drive(&transfer).await;
    assert_eq!(transfer.inner.state.lock().snapshot().arrived, 3000);
}

// A bucket answers a delete batch slowly while the merge keeps deciding. The merge stops once
// `DELETES_IN_FLIGHT_LOW_WATER` keys are in flight.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_stops_deciding_while_two_delete_batches_are_in_flight() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let names: Vec<String> = (0..5000).map(|i| format!("{i:04}.txt")).collect();
    let (deleter, pause) = RecordDeletes::pausing(DELETE_BATCH);
    let deleter = Arc::new(deleter);
    let (transfer, _ctx) = deleting_with_policy(
        dir.path(),
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    let mut in_flight = Vec::new();
    loop {
        let before = transfer.inner.state.lock().snapshot().deletes_in_flight;
        match transfer.poll_work() {
            PollWork::Ready { io: mut work, .. } => {
                if transfer.inner.state.lock().snapshot().deletes_in_flight > before {
                    in_flight.push(work);
                } else {
                    transfer.execute(&mut work).await;
                }
            }
            PollWork::Spawned => {}
            _ => break,
        }
    }
    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.paired < 5000,
        "the merge decided every key while its delete batches went unanswered"
    );
    assert!(
        snapshot.deletes_in_flight <= DELETES_IN_FLIGHT_LOW_WATER,
        "{} delete keys were in flight",
        snapshot.deletes_in_flight
    );

    for mut work in in_flight {
        pause.notify_one();
        transfer.execute(&mut work).await;
    }
    let finish = async {
        loop {
            match transfer.poll_work() {
                PollWork::Ready { io: mut work, .. } => {
                    pause.notify_one();
                    transfer.execute(&mut work).await;
                }
                PollWork::Done => break,
                PollWork::Spawned => {}
                PollWork::Pending => panic!("the run parked with nothing left to wake it"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(20), finish)
        .await
        .expect("the run did not finish");
    assert_eq!(
        deleter.keys_sent(),
        5000,
        "the run did not remove every key"
    );
}

// A bucket takes up to `DELETE_BATCH` keys per request, so the last partial batch goes out only
// after the merge finishes. A merge that paused for waiting deletes would hang.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_partial_delete_batch_still_goes_out_while_the_merge_can_pause() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let names: Vec<String> = (0..2500).map(|i| format!("{i:04}.txt")).collect();
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, _ctx) = deleting_with_policy(
        dir.path(),
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    drive(&transfer).await;

    let sizes: Vec<usize> = deleter.batches().iter().map(Vec::len).collect();
    assert_eq!(
        sizes,
        [1000, 1000, 500],
        "the run did not send its last partial batch"
    );
}

// Every download here waits behind the removal of the file `a`. A local tree removes one file per
// request, so that removal goes out at once, and the held downloads drain while the merge pauses.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn downloads_held_behind_a_removal_drain_while_the_merge_pauses() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::write(root.join("a"), "a file in the way").expect("the file is written");
    let keys: Vec<String> = (0..2000).map(|i| format!("a/{i:04}")).collect();
    let (client, _gets) =
        a_bucket_recording_gets(&keys.iter().map(String::as_str).collect::<Vec<_>>(), 1);

    let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::On);
    run_managed(&t, &ctx, rx).await;

    let landed = std::fs::read_dir(root.join("a"))
        .expect("`a` is a directory after the run")
        .count();
    assert_eq!(landed, 2000, "the run did not land every held download");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn bytes_a_dropped_child_moved_are_counted() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open_having_moved(512));
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = spawn_until_something_else(&transfer);
    let held = transfer.inner.state.lock().snapshot().children_running;
    assert!(held > 0, "no child is held, so this proves nothing");
    assert_eq!(
        transfer.inner.state.lock().snapshot().bytes,
        0,
        "a child was already accounted for, so this would count it twice"
    );

    ctx.set_cancelled();
    transfer.on_terminal();

    assert_eq!(
        transfer.inner.state.lock().snapshot().bytes,
        512 * held as u64,
        "a run forgot what its dropped children had moved"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn children_dropped_on_notice_leave_their_outcome_unknown() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = spawn_until_something_else(&transfer);
    let held = transfer.inner.state.lock().snapshot().children_running;
    assert!(held > 0, "no child is held, so this proves nothing");

    ctx.set_cancelled();
    transfer.on_terminal();

    assert!(
        transfer.inner.state.lock().snapshot().children_running == 0,
        "the notice left the children where the outstanding question could still see them"
    );
    assert_eq!(
        transfer.inner.state.lock().snapshot().outcomes_unknown,
        held as u64,
        "a run forgot that it never learned how {held} transfer(s) went"
    );
    assert_ne!(
        transfer.outcome(),
        RunOutcome::Clean,
        "a run that cannot say how {held} transfer(s) went called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn keys_qualified_and_never_started_leave_the_plan_short() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
        if transfer.inner.state.lock().snapshot().transfers_waiting > 0 {
            break;
        }
    }
    let queued = transfer.inner.state.lock().snapshot().transfers_waiting;
    assert!(queued > 0, "no key is queued, so this proves nothing");
    assert!(
        transfer.inner.state.lock().snapshot().children_running == 0,
        "a child is alive, so the children path could set the flag instead"
    );

    ctx.set_cancelled();
    let _ = transfer.poll_work();

    assert!(
        !transfer.is_plan_complete(),
        "a run that dropped {queued} qualified key(s) reported a complete plan"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_dropped_reap_leaves_its_outcomes_unknown() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::new(0, true));
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let PollWork::Ready { io: reap, .. } = spawn_until_something_else(&transfer) else {
        panic!("the finished children were not handed to a reap");
    };
    let held = transfer.inner.state.lock().snapshot().children_reaping;
    assert!(held > 0, "no reap was dispatched, so this proves nothing");
    drop(reap);

    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.children_reaping, 0,
        "a dropped reap kept its count, so the run would never finish"
    );
    assert_eq!(
        snapshot.outcomes_unknown, held as u64,
        "a dropped reap lost {held} child outcome(s) without counting them"
    );
    assert_ne!(
        transfer.outcome(),
        RunOutcome::Clean,
        "a run that lost {held} child outcome(s) called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_finished_child_frees_its_slot_before_its_reap() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 1);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = spawn_until_something_else(&transfer);
    assert_eq!(
        spawner.asked_count(),
        1,
        "a cap of one started two children"
    );

    spawner.release(&ctx);
    assert!(
        matches!(transfer.poll_work(), PollWork::Spawned),
        "the next poll did not start the second child"
    );
    assert_eq!(
        spawner.asked_count(),
        2,
        "the finished child kept its slot until its reap"
    );
    assert_eq!(transfer.inner.state.lock().snapshot().children_reaping, 0);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_reap_takes_at_most_one_batch() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let keys: Vec<String> = (0..100).map(|n| format!("k{n:03}.txt")).collect();
    a_local_tree(
        dir.path(),
        &keys.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let spawner = Arc::new(SpawnEnded::new(0, false));
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 100);

    let reaped = loop {
        match transfer.poll_work() {
            PollWork::Ready { io: mut work, .. } => {
                let reaping = transfer.inner.state.lock().snapshot().children_reaping;
                if reaping > 0 {
                    break reaping;
                }
                transfer.execute(&mut work).await;
            }
            PollWork::Spawned => {}
            other => panic!("the run reached {other:?} before any reap"),
        }
    };
    assert_eq!(
        spawner.asked_count(),
        100,
        "not every child had finished first"
    );
    assert_eq!(
        reaped,
        crate::transfer::composite::MAX_REAP_PER_POLL,
        "one reap took {reaped} of 100 finished children"
    );
}

// This spawner records whether the run's state lock was free when it ran.
struct SpawnProbingTheLock {
    inner: SpawnEnded,
    run: std::sync::OnceLock<std::sync::Weak<Inner<FsWalk, S3Walk>>>,
    lock_was_free: std::sync::atomic::AtomicBool,
}

impl SpawnChild<crate::io::walk::FsEntry> for SpawnProbingTheLock {
    fn spawn(
        &self,
        key: &str,
        source: &crate::io::walk::FsEntry,
        parent: u64,
    ) -> Result<SyncChild, crate::error::Error> {
        let free = self
            .run
            .get()
            .and_then(std::sync::Weak::upgrade)
            .is_some_and(|run| run.state.try_lock().is_some());
        self.lock_was_free
            .store(free, std::sync::atomic::Ordering::SeqCst);
        self.inner.spawn(key, source, parent)
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_child_starts_without_the_run_state_lock() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let spawner = Arc::new(SpawnProbingTheLock {
        inner: SpawnEnded::new(0, false),
        run: std::sync::OnceLock::new(),
        lock_was_free: std::sync::atomic::AtomicBool::new(false),
    });
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);
    spawner
        .run
        .set(Arc::downgrade(&transfer.inner))
        .unwrap_or_else(|_| panic!("the probe was set twice"));

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = spawn_until_something_else(&transfer);

    assert_eq!(
        spawner.inner.asked_count(),
        1,
        "the run never spawned a child"
    );
    assert!(
        spawner
            .lock_was_free
            .load(std::sync::atomic::Ordering::SeqCst),
        "the run held its state lock while it spawned a child"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_abandoned_batch_leaves_the_plan_short() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    let mut held = None;
    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        if transfer.inner.state.lock().snapshot().deletes_in_flight > 0 {
            held = Some(work);
            break;
        }
        transfer.execute(&mut work).await;
    }
    let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");
    assert!(
        transfer.inner.state.lock().snapshot().deletes_waiting == 0,
        "the keys are still in the buffer, so the accounting sites would see them"
    );

    ctx.set_cancelled();
    transfer.execute(&mut batch).await;

    assert!(
        !transfer.is_plan_complete(),
        "a run that let go of decided removals reported a complete plan"
    );
    assert_ne!(
        transfer.outcome(),
        RunOutcome::Clean,
        "a run that let go of decided removals called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_abandoned_batch_gives_back_every_slot_it_took() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt", "gone-c.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    let mut held = None;
    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        if transfer.inner.state.lock().snapshot().deletes_in_flight > 0 {
            held = Some(work);
            break;
        }
        transfer.execute(&mut work).await;
    }
    let mut batch = held.expect("no delete batch was dispatched, so this proves nothing");
    let took = transfer.inner.state.lock().snapshot().deletes_in_flight;
    assert!(took > 1, "a batch of one slot cannot show the leak");

    ctx.set_cancelled();
    transfer.execute(&mut batch).await;

    assert_eq!(
        transfer.inner.state.lock().snapshot().deletes_in_flight,
        0,
        "a batch of {took} keys gave back fewer slots than it took, so the run can never end"
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
    assert_eq!(
        transfer.inner.state.lock().snapshot().paired,
        keys.len() as u64
    );
    assert!(
        items > 1,
        "a tree of {} keys came back in one work item, so the batch bound did nothing",
        keys.len()
    );
}

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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));

    tokio::time::timeout(Duration::from_secs(20), completion_rx)
        .await
        .expect("the run parked: a work item finished without waking the transfer")
        .expect("the terminal signal was dropped");

    assert_eq!(
        transfer.inner.state.lock().snapshot().paired,
        keys.len() as u64
    );
    assert!(
        keys.len() > MERGE_BATCH,
        "the tree fits in one work item, so the run never parked and the wake went untested"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_batch_still_out_holds_the_run_open() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let (transfer, ctx) = uploading(dir.path(), &[]);

    let mut walk = transfer
        .inner
        .state
        .lock()
        .merge
        .take_walk_to_advance()
        .expect("the merge");
    while walk.next().await.is_some() {}
    assert_eq!(
        walk.progress(),
        Progress::Accounted,
        "the merge did not reach the end"
    );
    {
        let mut state = transfer.inner.state.lock();
        state.merge.hold_in_flight(walk);
    }
    assert!(ctx.is_active(), "the transfer ended before the assertion");

    assert!(
        matches!(transfer.poll_work(), PollWork::Pending),
        "a finished merge with a batch still out reported the run over"
    );

    transfer.inner.state.lock().merge.release();
    assert!(
        matches!(transfer.poll_work(), PollWork::Done),
        "with nothing outstanding the run is not over"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_item_that_drains_the_merge_answers_the_waiter() {
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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    let mut work = match transfer.poll_work() {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected a work item, got {other:?}"),
    };
    transfer.execute(&mut work).await;

    tokio::time::timeout(Duration::from_secs(20), completion_rx)
        .await
        .expect("the run drained inside a work item and left its waiter unanswered")
        .expect("the terminal signal was dropped");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_decision_still_waiting_holds_the_run_open() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    a_local_tree(dir.path(), &["b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 1);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let parked = spawn_until_something_else(&transfer);

    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.transfers_waiting > 0,
        "nothing is waiting, so this proves nothing about the buffer"
    );
    assert!(
        matches!(parked, PollWork::Pending),
        "a decision still waiting for a slot did not hold the run open"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_decision_a_stopped_run_gives_back_keeps_the_decision_it_was_made_with() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
    let (transfer, _ctx) =
        uploading_with_policy(dir.path(), spawner.clone(), 4, FailedTransferPolicy::Abort);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    // The test keeps each returned work item, so the cancel batch stays out and the run stays
    // stopped while the test reads it.
    let _held: Vec<PollWork> = (0..3).map(|_| transfer.poll_work()).collect();

    let state = transfer.inner.state.lock();
    let snapshot = state.snapshot();
    assert!(
        snapshot.stopped,
        "the run did not stop, so nothing was given back"
    );
    assert!(
        snapshot.transfers_waiting > 0,
        "nothing was given back, so this proves nothing about the decision"
    );
    for (pairing, decision) in state.waiting_transfers() {
        assert!(
            matches!(decision, Decision::Transfer(_)),
            "the key {:?} was qualified for transfer and came back as {decision:?}",
            pairing.key(),
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_live_child_holds_the_run_open() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let parked = spawn_until_something_else(&transfer);

    assert_eq!(spawner.asked_count(), 1, "no child was spawned");
    assert!(
        matches!(parked, PollWork::Pending),
        "a child that has not ended did not hold the run open"
    );

    spawner.release(&ctx);
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
    let mut reap = match spawn_until_something_else(&transfer) {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected a reap, got {other:?}"),
    };
    {
        let snapshot = transfer.inner.state.lock().snapshot();
        assert!(
            snapshot.children_running == 0,
            "the child is still in the map"
        );
        assert_eq!(
            snapshot.children_reaping, 1,
            "the reap is not accounted for"
        );
    }
    assert!(
        matches!(transfer.poll_work(), PollWork::Pending),
        "a reap still out did not hold the run open, so its child's outcome would be lost"
    );

    transfer.execute(&mut reap).await;
    assert!(matches!(transfer.poll_work(), PollWork::Done));
}

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
                let live = transfer.inner.state.lock().snapshot().children_running;
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

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_upload_never_waits_for_a_bucket_removal() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a/b.txt"]);
    let deleter = Arc::new(RecordDeletes::new(1));
    let (transfer, _ctx) = deleting(dir.path(), &["a"], deleter.clone());

    loop {
        let next = transfer.poll_work();
        // The test checks before the work item runs, while the deleter has not answered for `a`.
        assert_eq!(
            transfer.inner.state.lock().snapshot().transfers_held,
            0,
            "an upload waited for a bucket removal"
        );
        match next {
            PollWork::Ready { io: mut work, .. } => {
                transfer.execute(&mut work).await;
            }
            PollWork::Spawned => {}
            _ => break,
        }
    }
    assert_eq!(
        deleter.batches(),
        vec![vec!["a".to_string()]],
        "the run named the wrong key for removal"
    );
}
