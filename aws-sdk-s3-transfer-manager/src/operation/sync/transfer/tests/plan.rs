/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run decides each key and counts its plan.

use super::*;

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_archived_object_and_a_restoring_one_are_counted_apart() {
    use crate::io::key::stream::Obstruction;

    for comparison in [
        &AlwaysObstructed(Obstruction::Archived),
        &AlwaysObstructed(Obstruction::BeingRestored),
    ] {
        let why = comparison.0;
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, _ctx) = uploading_comparing_with(dir.path(), comparison);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }

        let counts = transfer.inner.state.lock().snapshot().decided.obstructed;
        let expected = match why {
            Obstruction::Archived => Obstructed {
                archived: 1,
                ..Obstructed::default()
            },
            Obstruction::BeingRestored => Obstructed {
                restoring: 1,
                ..Obstructed::default()
            },
            Obstruction::NothingToRead
            | Obstruction::UnfollowedLink
            | Obstruction::ExcludedLink
            | Obstruction::LinkToDirectory => {
                unreachable!("not under test here")
            }
        };
        assert_eq!(counts, expected, "{why:?} was counted under another reason");
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_key_whose_absence_went_unread_is_counted_apart_from_an_unchanged_one() {
    use crate::io::key::stream::KeysLost;

    for comparison in [
        &AlwaysUnknown(KeysLost::OneKey),
        &AlwaysUnknown(KeysLost::UnknownRange),
    ] {
        let lost = comparison.0;
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt"]);
        let (transfer, _ctx) = uploading_comparing_with(dir.path(), comparison);

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }

        let snapshot = transfer.inner.state.lock().snapshot();
        assert_eq!(
            snapshot.decided.skipped.total(),
            1,
            "the key was not skipped, so this proves nothing about the reason"
        );
        assert_eq!(
            snapshot.decided.skipped.unread, 1,
            "a key the run could not compare, lost as {lost:?}, is counted as one it compared"
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn every_pairing_yields_exactly_one_decision() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt", "nested/c.txt"]);
    let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);

    let paired = drive(&transfer).await;
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.decided.transfers + snapshot.decided.deletes + snapshot.decided.skipped.total(),
        paired,
        "the decisions do not account for every key that was paired"
    );
    assert_eq!(snapshot.decided.deletes, 1, "d.txt is on the bucket alone");
}

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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    let paired = drive(&transfer).await;
    assert_eq!(paired, 2);
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.decided.skipped.total(),
        2,
        "a deferred key was not skipped"
    );
    assert_eq!(snapshot.decided.transfers, 0);
    assert!(
        snapshot.plan_incomplete,
        "the plan is short two keys and does not say so"
    );
}

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
            state.set_plan_flags(mine, walks);
        }
        assert_eq!(
            transfer.is_plan_complete(),
            whole,
            "a deferred verdict of {mine} and an unread stream of {walks} answered wrongly"
        );
    }
}

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
        !transfer.inner.state.lock().snapshot().merge_present,
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
    let (transfer, _ctx) = uploading(dir.path(), &["b.txt", "d.txt"]);
    assert_eq!(drive(&transfer).await, 4);
    assert!(
        transfer.inner.state.lock().snapshot().walk_failures_kept == 0,
        "a well-formed listing produced a failure"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_with_nothing_wrong_reports_clean() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, _ctx) = deleting(dir.path(), &["gone.txt"], deleter);

    drive(&transfer).await;

    assert_eq!(transfer.outcome(), RunOutcome::Clean);
}

#[test]
fn the_defaults_are_continue_and_no_deleting() {
    assert_eq!(
        RunSettings::default().failure_policy,
        FailedTransferPolicy::Continue
    );
    assert_eq!(DeleteMode::default(), DeleteMode::Off);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_reports_the_transfers_that_arrived() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let spawner = Arc::new(SpawnEnded::new(7, true));
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    drive(&transfer).await;

    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.decided.transfers, 2,
        "the run decided {} transfers for two keys",
        snapshot.decided.transfers
    );
    assert_eq!(
        snapshot.arrived, 0,
        "both children ended badly and the run reports {} arrivals",
        snapshot.arrived
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_transfer_decided_on_an_absent_source_does_not_strand_the_rest() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["b.txt"]);
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
    let transfer = SyncTransfer::new(
        ctx,
        walk,
        &AlwaysTransfers,
        spawner.clone(),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    let paired = drive(&transfer).await;
    assert_eq!(paired, 2);
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.transfer_failures, 1,
        "the key with no source was not accounted for"
    );
    assert_eq!(
        spawner.asked_count(),
        1,
        "the key behind it was never spawned"
    );
}

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
    let entry = match walked.next().await {
        Some(Ok(entry)) => entry,
        Some(Err(err)) => panic!("the walk failed: {err}"),
        None => panic!("the walk produced nothing"),
    };
    assert!(
        entry.metadata().is_some(),
        "the walk read no metadata, so this test cannot tell the two paths apart"
    );

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

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_uploaded_key_is_named_under_the_runs_prefix() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "nested/c.txt"]);

    let (client, puts) = a_bucket_recording_puts(&[]);
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
                .prefix("data")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnUpload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            Some("data"),
        )),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(20), completion_rx)
        .await
        .expect("the run did not finish")
        .expect("the terminal signal was dropped");

    let mut written = puts.lock().clone();
    written.sort();
    assert_eq!(
        written,
        vec!["data/a.txt".to_string(), "data/nested/c.txt".to_string()],
        "the writes did not land under the run's prefix"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_qualified_key_reaches_the_bucket() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);

    let (client, put_keys) = a_bucket_recording_puts(&[]);
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        spawner,
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));

    tokio::time::timeout(Duration::from_secs(20), completion_rx)
        .await
        .expect("the run never finished, so a child was spawned and never reaped")
        .expect("the terminal signal was dropped");

    let mut sent = put_keys.lock().clone();
    sent.sort();
    assert_eq!(
        sent,
        vec!["a.txt".to_string(), "b.txt".to_string()],
        "the bucket did not receive both keys"
    );

    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.decided.transfers, 2,
        "both keys should have been sent"
    );
    assert_eq!(
        snapshot.transfer_failures, 0,
        "a child failed, so the upload path is not working"
    );
    assert!(
        snapshot.children_running == 0 && snapshot.children_reaping == 0,
        "the run ended with children unaccounted for"
    );
}
