/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run records failures and which failures end it.

use super::*;

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_that_only_met_an_obstruction_does_not_report_clean() {
    let dir = tempfile::tempdir().expect("a temp dir");
    nix::unistd::mkfifo(&dir.path().join("pipe"), nix::sys::stat::Mode::S_IRWXU)
        .expect("a named pipe");

    let spawner = Arc::new(SpawnEnded::new(0, false));
    let (transfer, _ctx) = uploading_with(dir.path(), spawner.clone(), 4);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    assert_eq!(
        spawner.asked_count(),
        0,
        "something was sent for a name that holds nothing readable"
    );
    assert_eq!(
        transfer.outcome(),
        RunOutcome::Warned,
        "a run whose only event was an obstruction called itself clean"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_attaches_the_category_the_failure_had() {
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
    use aws_smithy_types::error::metadata::ErrorMetadata;

    let refused = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
        ListObjectsV2Error::generic(ErrorMetadata::builder().code("AccessDenied").build())
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&refused]);

    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::Off,
            failure_policy: FailedTransferPolicy::Abort,
        },
    );

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }
    let _ = transfer.poll_work();

    let err = ctx.take_error().expect("an aborting run attached no error");
    assert_eq!(
        *err.kind(),
        crate::error::ErrorKind::ServiceError,
        "a service refusal was handed to a caller under another category"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_listing_that_failed_does_not_release_the_part_batch() {
    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
    use aws_smithy_types::error::metadata::ErrorMetadata;

    let page = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
        ListObjectsV2Output::builder()
            .set_contents(Some(vec![Object::builder()
                .key("a.txt")
                .size(0)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                .build()]))
            .is_truncated(true)
            .next_continuation_token("more")
            .build()
    });
    let refused = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
        ListObjectsV2Error::generic(ErrorMetadata::builder().code("InternalError").build())
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&page, &refused]);

    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
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
        deleter.clone(),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        transfer.execute(&mut work).await;
    }

    assert_eq!(
        deleter.keys_sent(),
        0,
        "a run whose listing failed part way sent the removals it had buffered"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_refused_key_reaches_a_caller_as_a_service_refusal() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Abort,
    );

    drive(&transfer).await;

    let err = ctx
        .take_error()
        .expect("an aborting run attached no error for a refused key");
    assert_eq!(
        *err.kind(),
        crate::error::ErrorKind::ServiceError,
        "a key the service refused reached a caller under another category"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_source_root_nobody_can_list_fails_the_run() {
    for policy in [FailedTransferPolicy::Continue, FailedTransferPolicy::Abort] {
        let dir = tempfile::tempdir().expect("a temp dir");
        let absent = dir.path().join("not-there");
        let spawner = Arc::new(SpawnEnded::new(0, false));
        let (transfer, ctx) = uploading_with_policy(&absent, spawner, 4, policy.clone());

        while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
            transfer.execute(&mut work).await;
        }

        assert!(
            ctx.is_failed(),
            "under {policy:?} a run that never read its source carried on to a status a \
             caller reads as finished"
        );
        assert_eq!(
            transfer.outcome(),
            RunOutcome::Failed,
            "under {policy:?} a run that never read its source reported otherwise"
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_failure_ends_an_aborting_run_and_lets_its_batch_go() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::refusing(1));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Abort,
    );

    drive(&transfer).await;

    assert_eq!(
        transfer.outcome(),
        RunOutcome::Failed,
        "a refused delete did not make the run report a failure"
    );
    assert!(
        ctx.is_failed(),
        "a refused delete ended an aborting run without recording it as failed"
    );
    assert!(
        transfer.inner.state.lock().snapshot().deletes_waiting == 0,
        "an ended run kept keys judged against a stream it stopped reading"
    );
    assert_eq!(
        deleter.keys_sent(),
        1,
        "an ended run sent a key judged against a stream it stopped reading"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_same_failure_leaves_a_continuing_run_going() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
    let (transfer, ctx) = deleting_with_policy(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::On,
        FailedTransferPolicy::Continue,
    );

    drive(&transfer).await;

    assert_eq!(transfer.outcome(), RunOutcome::Failed);
    assert_eq!(
        deleter.keys_sent(),
        2,
        "a continuing run stopped before asking about every key"
    );
    assert!(
        !ctx.is_active(),
        "the run should have completed rather than stayed open"
    );
    assert!(
        !ctx.is_failed(),
        "a continuing run reported itself as failed"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_badly_described_key_ends_an_aborting_run() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let rule = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(|| {
        ListObjectsV2Output::builder()
            .set_contents(Some(vec![Object::builder().key("d.txt").size(0).build()]))
            .build()
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&rule]);
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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Abort,
        },
    );

    drive(&transfer).await;

    assert_eq!(transfer.outcome(), RunOutcome::Failed);
    assert!(
        ctx.is_failed(),
        "a failure on the walk's own channel did not end an aborting run"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_child_that_failed_ends_an_aborting_run() {
    for (policy, expect_failed) in [
        (FailedTransferPolicy::Continue, false),
        (FailedTransferPolicy::Abort, true),
    ] {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let (transfer, ctx) = with_spawner(dir.path(), SpawnEnded::new(0, true), policy.clone());

        drive(&transfer).await;

        assert!(
            transfer.inner.state.lock().snapshot().transfer_failures > 0,
            "under {policy:?} the failed child was not counted"
        );
        assert_eq!(
            ctx.is_failed(),
            expect_failed,
            "under {policy:?} the run's status does not match the policy"
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_failed_child_is_recorded_under_its_own_key() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt", "b.txt"]);
    let (transfer, _ctx) = with_spawner(
        dir.path(),
        SpawnEnded::new(0, true),
        FailedTransferPolicy::Continue,
    );

    drive(&transfer).await;

    let state = transfer.inner.state.lock();
    let mut keys: Vec<&str> = state
        .transfer_failure_sample()
        .iter()
        .map(|failure| failure.key.as_str())
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["a.txt", "b.txt"], "a child failure lost its key");
    assert!(
        state
            .transfer_failure_sample()
            .iter()
            .all(|failure| *failure.kind() == crate::error::ErrorKind::IOError),
        "a child failure lost its own error: {:?}",
        state.transfer_failure_sample()
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_failure_that_aborts_a_run_is_counted_once() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let (transfer, ctx) = with_spawner(
        dir.path(),
        SpawnEnded::new(0, true),
        FailedTransferPolicy::Abort,
    );

    drive(&transfer).await;

    let err = ctx.take_error().expect("an aborting run attached no error");
    assert!(
        aws_smithy_types::error::display::DisplayErrorContext(&err)
            .to_string()
            .contains("a child that ended badly"),
        "the run error is not the child's own error: {err:?}"
    );
    let state = transfer.inner.state.lock();
    assert_eq!(
        state.snapshot().transfer_failures,
        1,
        "the failure that ended the run was not counted"
    );
    assert!(
        state.transfer_failure_sample().is_empty(),
        "the run error and the sample both hold the same failure"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_aborting_run_keeps_the_failure_the_site_had() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let (transfer, ctx) = with_spawner(
        dir.path(),
        SpawnEnded::refusing_to_spawn(),
        FailedTransferPolicy::Abort,
    );

    drive(&transfer).await;

    let err = ctx.take_error().expect("an aborting run attached no error");
    assert_eq!(
        *err.kind(),
        crate::error::ErrorKind::ObjectNotDiscoverable,
        "the run replaced the site's failure with a category of its own"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_child_that_could_not_be_built_ends_an_aborting_run() {
    for (policy, expect_failed) in [
        (FailedTransferPolicy::Continue, false),
        (FailedTransferPolicy::Abort, true),
    ] {
        let dir = tempfile::tempdir().expect("a temp dir");
        a_local_tree(dir.path(), &["a.txt", "b.txt"]);
        let (transfer, ctx) =
            with_spawner(dir.path(), SpawnEnded::refusing_to_spawn(), policy.clone());

        drive(&transfer).await;

        assert!(
            transfer.inner.state.lock().snapshot().transfer_failures > 0,
            "under {policy:?} the refused spawn was not counted"
        );
        assert_eq!(
            ctx.is_failed(),
            expect_failed,
            "under {policy:?} the run's status does not match the policy"
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_key_the_listing_described_badly_is_kept_as_a_failure() {
    let dir = tempfile::tempdir().expect("a temp dir");
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
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 2,
            delete_mode: DeleteMode::On,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );

    assert_eq!(
        drive(&transfer).await,
        0,
        "no key was described well enough to pair"
    );
    assert_eq!(
        transfer.inner.state.lock().snapshot().walk_failures_kept,
        1,
        "the run ended without keeping what it could not account for"
    );
    assert!(
        !transfer.is_plan_complete(),
        "a key the listing described badly left the plan looking whole"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_where_everything_fails_keeps_a_bounded_sample() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temp dir");
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

    let snapshot = transfer.inner.state.lock().snapshot();
    assert!(
        snapshot.walk_failures_kept <= FAILURES_KEPT,
        "the run kept {} failures, so peak memory follows the tree",
        snapshot.walk_failures_kept
    );
    assert!(
        snapshot.walk_failures > snapshot.walk_failures_kept as u64,
        "the total did not outrun the sample, so the cap was never reached"
    );
}

#[tokio::test]
async fn a_refused_listing_keeps_its_service_code_on_the_run_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    a_local_tree(dir.path(), &["a.txt"]);
    let denied = mock!(aws_sdk_s3::Client::list_objects_v2).then_error(|| {
        aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .message("denied")
                .build(),
        )
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&denied]);
    let (t, ctx) = upload_run(dir.path(), client, Walker::builder().build());
    drive(&t).await;
    let err = ctx.take_error().expect("the run failed");
    assert_eq!(
        err.code(),
        Some("AccessDenied"),
        "the run error lost the service code"
    );
}
