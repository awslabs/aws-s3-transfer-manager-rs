/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run stops when a caller cancels it or a failure aborts it.

use super::*;

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_cancelled_run_that_dropped_work_does_not_report_clean() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    let mut buffered = 0;
    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
        buffered = transfer.inner.state.lock().snapshot().deletes_waiting;
        if buffered > 0 {
            break;
        }
    }
    assert!(
        buffered > 0,
        "nothing was buffered, so this run had no decided work to drop"
    );

    ctx.set_cancelled();
    while !matches!(transfer.poll_work(), PollWork::Done | PollWork::Pending) {}

    assert_ne!(
        transfer.outcome(),
        RunOutcome::Clean,
        "a run that dropped {buffered} decided removal(s) called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_still_holding_children_does_not_report_clean() {
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

    assert_ne!(
        transfer.outcome(),
        RunOutcome::Clean,
        "a run holding {held} unjoined child(ren) called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_does_not_send_a_batch_that_was_already_in_flight() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Abort,
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

    {
        let mut state = transfer.inner.state.lock();
        state.stop(crate::error::Error::new(
            crate::error::ErrorKind::IOError,
            "a child failed while the batch was away",
        ));
    }
    assert!(
        ctx.is_active(),
        "the status already moved, so the window this test is about is not open"
    );
    assert!(
        transfer.inner.state.lock().snapshot().stopped,
        "the run did not decide to stop, so this proves nothing"
    );

    transfer.execute(&mut batch).await;

    assert_eq!(
        deleter.keys_sent(),
        0,
        "an aborting run removed keys it had decided to abandon"
    );
}

// A destination asks the run's stop check before each retry. A run that stops while a batch is in
// flight answers `true`, so the destination stops retrying.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_delete_batch_learns_that_the_run_stopped_while_in_flight() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let (deleter, pause) = RecordDeletes::pausing(DELETE_BATCH);
    let deleter = Arc::new(deleter);
    let (transfer, _ctx) = deleting_with_policy(
        dir.path(),
        &["gone.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Abort,
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

    let stop_while_in_flight = async {
        transfer.inner.state.lock().stop(crate::error::Error::new(
            crate::error::ErrorKind::IOError,
            "a child failed while the batch was in flight",
        ));
        pause.notify_one();
    };
    tokio::join!(transfer.execute(&mut batch), stop_while_in_flight);

    assert_eq!(
        deleter.stops_seen(),
        [true],
        "the destination did not learn that the run stopped"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_stopped_run_reports_fewer_arrivals_than_it_decided() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
    let (transfer, ctx) =
        uploading_with_policy(dir.path(), spawner.clone(), 4, FailedTransferPolicy::Abort);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        let _ = transfer.execute(&mut work).await;
    }
    let _ = transfer.poll_work();
    assert!(
        transfer.inner.state.lock().snapshot().transfers_waiting > 0,
        "nothing was left buffered, so this proves nothing about the counts"
    );

    drive(&transfer).await;
    assert!(ctx.is_failed(), "the aborting run did not fail");

    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.transfers_waiting == 0,
        "the teardown left keys buffered"
    );
    assert_eq!(
        snapshot.decided.transfers, 3,
        "the run decided {} transfers for three keys",
        snapshot.decided.transfers
    );
    assert_eq!(
        (
            snapshot.arrived,
            snapshot.cancelled,
            spawner.cancelled_count()
        ),
        (0, 1, 1),
        "the run did not cancel the child it held open"
    );
    assert!(
        snapshot.plan_incomplete,
        "a run that abandoned a decided transfer called its plan whole"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_cancelled_run_starts_no_further_child() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        let _ = transfer.execute(&mut work).await;
    }
    assert!(
        transfer.inner.state.lock().snapshot().transfers_waiting > 0,
        "nothing is waiting, so no spawn would have been attempted either way"
    );

    let asked_before = spawner.asked_count();
    ctx.set_cancelled();
    let claimed = {
        let mut state = transfer.inner.state.lock();
        transfer.claim_one(&mut state).is_some()
    };

    assert!(!claimed, "a cancelled run claimed another child");
    assert_eq!(
        spawner.asked_count(),
        asked_before,
        "a cancelled run asked the spawner for one more key"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_does_not_start_the_keys_after_the_one_that_failed() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
    let (transfer, ctx) =
        uploading_with_policy(dir.path(), spawner.clone(), 4, FailedTransferPolicy::Abort);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = transfer.poll_work();
    let _ = transfer.poll_work();

    assert!(
        transfer.inner.state.lock().snapshot().stopped,
        "the run did not decide to stop, so the guard this test is about was never reached"
    );
    assert_eq!(
        spawner.asked_count(),
        2,
        "an aborting run asked for a key queued behind the one that ended it"
    );
    assert!(
        !ctx.is_failed(),
        "the status flipped before a pass could carry the run to its end"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_cancelled_run_does_not_send_a_batch_already_handed_over() {
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

    ctx.set_cancelled();
    transfer.execute(&mut batch).await;

    assert_eq!(
        deleter.keys_sent(),
        0,
        "a cancelled run sent a batch it had already handed over"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_cancelled_run_does_not_start_the_keys_it_had_buffered() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let buffered = transfer.inner.state.lock().snapshot().transfers_waiting;
    assert!(
        buffered > 0,
        "no key is waiting, so the guard this test is about holds nothing"
    );
    let started_before = spawner.asked_count();

    ctx.set_cancelled();
    let _ = transfer.poll_work();

    assert_eq!(
        spawner.asked_count(),
        started_before,
        "a cancelled run started {buffered} key(s) it had already qualified"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_cancelled_run_keeps_the_children_it_already_sent() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_children_open());
    let (transfer, ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let spawned = spawn_until_something_else(&transfer);
    assert!(
        matches!(spawned, PollWork::Pending),
        "the run did not park with children live, so this proves nothing"
    );
    let live = transfer.inner.state.lock().snapshot().children_running;
    assert!(live > 0, "no child is live, so there is nothing to keep");

    ctx.set_cancelled();

    assert!(
        matches!(transfer.poll_work(), PollWork::Pending),
        "a cancelled run called itself over with {live} children still away"
    );
    assert_eq!(
        spawner.asked_count(),
        live,
        "a cancelled run asked for another child"
    );

    spawner.release(&ctx);
    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    assert!(
        matches!(transfer.poll_work(), PollWork::Done),
        "the children ended and the cancelled run still did not finish"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn cancelling_before_a_failure_still_reports_cancelled() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
    let (transfer, ctx) =
        uploading_with_policy(dir.path(), spawner.clone(), 4, FailedTransferPolicy::Abort);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        let _ = transfer.execute(&mut work).await;
    }
    let _ = transfer.poll_work();
    let _ = transfer.poll_work();

    assert!(
        transfer.inner.state.lock().snapshot().stopped,
        "nothing failed, so the cancellation has no failure to beat"
    );
    assert!(
        !ctx.is_failed(),
        "the failure reached the status already, leaving no window to cancel in"
    );

    ctx.set_cancelled();
    spawner.release(&ctx);
    drive(&transfer).await;

    assert!(ctx.is_cancelled(), "the cancellation was lost");
    assert!(
        !ctx.is_failed(),
        "a cancelled run reported the failure it was carrying instead"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread")]
async fn an_aborting_run_answers_its_waiter_without_another_poll() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));

    let client = a_bucket_holding(&[]);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
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
        spawner.clone(),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Abort,
        },
    );

    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    spawner.release(&ctx);
    let answered = tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .expect("an aborting run never answered its waiter");
    assert!(
        answered.is_ok(),
        "the run was removed without signalling, so its waiter got nothing"
    );
    assert!(ctx.is_failed(), "the run did not report itself failed");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_does_not_send_the_deletes_it_buffered() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let deleter = Arc::new(RecordDeletes::new(1));
    let client = a_bucket_holding(&["zz-gone.txt"]);
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
        Arc::new(SpawnEnded::refusing_to_spawn()),
        deleter.clone(),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Abort,
        },
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

    assert!(ctx.is_failed(), "the refused spawn did not abort the run");
    assert_eq!(
        deleter.keys_sent(),
        0,
        "an aborting run sent {} keys it was supposed to drop",
        deleter.keys_sent()
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_that_stops_before_its_merge_finishes_reports_a_partial_plan() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let names: Vec<String> = (0..MERGE_BATCH * 2)
        .map(|i| format!("{i:04}.txt"))
        .collect();
    a_local_tree(
        dir.path(),
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    // Every child of the first batch stays open, and the last spawn of that batch fails. The run
    // then stops with nothing waiting, while the test still holds the second merge batch.
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(MERGE_BATCH - 1));
    let (transfer, ctx) = uploading_with_policy(
        dir.path(),
        spawner,
        MERGE_BATCH,
        FailedTransferPolicy::Abort,
    );

    let mut first = match transfer.poll_work() {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected the first merge batch, got {other:?}"),
    };
    transfer.execute(&mut first).await;
    let mut second = match transfer.poll_work() {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected the second merge batch, got {other:?}"),
    };
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
        snapshot.stopped && snapshot.transfers_waiting == 0 && snapshot.deletes_waiting == 0,
        "the run must stop with nothing waiting, or this proves nothing: {snapshot:?}"
    );
    transfer.execute(&mut second).await;
    drive(&transfer).await;

    assert!(ctx.is_failed(), "the aborting run did not fail");
    assert!(
        !transfer.is_plan_complete(),
        "a run that stopped before its merge reached every key reported a whole plan"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_cancels_its_running_children_and_waits_for_them() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    // The child for `a.txt` stays open. The spawn for `b.txt` fails, and the failure aborts the
    // run.
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
    let (transfer, ctx) =
        uploading_with_policy(dir.path(), spawner.clone(), 4, FailedTransferPolicy::Abort);

    drive(&transfer).await;

    assert_eq!(
        spawner.cancelled_count(),
        1,
        "the run left its open child running"
    );
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        (
            snapshot.cancelled,
            snapshot.children_running,
            snapshot.children_reaping
        ),
        (1, 0, 0),
        "the run failed before its cancelled child settled"
    );
    let err = ctx.take_error().expect("an aborting run attached no error");
    assert_eq!(
        *err.kind(),
        crate::error::ErrorKind::ObjectNotDiscoverable,
        "the run error is not the failure that stopped the run"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_continuing_run_lets_its_children_finish() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "c.txt"]);
    let spawner = Arc::new(SpawnEnded::holding_open_but_refusing_at(1));
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
    assert_eq!(
        transfer.inner.state.lock().snapshot().children_running,
        2,
        "the run started the two children it could build"
    );
    assert_eq!(
        spawner.cancelled_count(),
        0,
        "a failure cancelled a child under Continue"
    );

    spawner.release(&ctx);
    drive(&transfer).await;

    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        (
            snapshot.arrived,
            snapshot.cancelled,
            snapshot.transfer_failures
        ),
        (2, 0, 1),
        "a continuing run did not let both open children arrive"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn cancelling_lets_the_pending_batch_go() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

    let mut work = match transfer.poll_work() {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected the merge, got {other:?}"),
    };
    transfer.execute(&mut work).await;
    assert_eq!(
        transfer.inner.state.lock().snapshot().deletes_waiting,
        2,
        "the keys were not waiting, so this proves nothing"
    );

    ctx.set_cancelled();
    assert!(matches!(transfer.poll_work(), PollWork::Done));
    assert_eq!(
        deleter.keys_sent(),
        0,
        "a cancelled run still issued deletes judged against an incomplete source"
    );
    assert!(transfer.inner.state.lock().snapshot().deletes_waiting == 0);
}

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
async fn an_aborting_run_hands_the_merge_back() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::new(0, false));
    let (transfer, ctx) =
        uploading_with_policy(dir.path(), spawner, 4, FailedTransferPolicy::Abort);

    let mut work = match transfer.poll_work() {
        PollWork::Ready { io, .. } => io,
        other => panic!("expected a work item, got {other:?}"),
    };

    {
        let mut state = transfer.inner.state.lock();
        state.stop(crate::error::Error::new(
            crate::error::ErrorKind::IOError,
            "a child failed",
        ));
    }
    assert!(
        ctx.is_active(),
        "the status already moved, so the window this test is about is not open"
    );

    assert!(matches!(
        transfer.execute(&mut work).await,
        WorkOutcome::Cancelled
    ));
    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.merge_present && !snapshot.merge_in_flight,
        "an aborting work item left the merge where no poll can reach it"
    );
    assert_eq!(
        snapshot.decided.transfers, 0,
        "an aborting run paired and qualified keys after deciding to stop"
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
    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.merge_present && !snapshot.merge_in_flight,
        "a cancelled work item left the merge where no poll can reach it"
    );
}
