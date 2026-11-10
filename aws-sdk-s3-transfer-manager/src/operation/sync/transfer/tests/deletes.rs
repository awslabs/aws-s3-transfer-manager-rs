/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run deletes keys from the bucket in batches.

use super::*;

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn destination_only_keys_are_deleted_in_batches() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let keys: Vec<String> = (0..7).map(|n| format!("gone{n}.txt")).collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let deleter = Arc::new(RecordDeletes::new(3));
    let (transfer, _ctx) = deleting(dir.path(), &refs, deleter.clone());

    drive(&transfer).await;

    let batches = deleter.batches();
    assert_eq!(
        deleter.keys_sent(),
        7,
        "not every key reached the delete path"
    );
    assert!(
        batches.len() > 1,
        "seven keys went out in {} request(s), so nothing was batched",
        batches.len()
    );
    assert!(
        batches.iter().all(|b| b.len() <= 3),
        "a batch exceeded the limit: {batches:?}"
    );
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(snapshot.removed, 7, "each key's outcome was not counted");
    assert_eq!(
        snapshot.decided.deletable, 7,
        "a run allowed to delete does not report what the comparison marked"
    );
    assert_eq!(snapshot.delete_failures, 0);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_refused_delete_is_counted_against_its_own_key() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::refusing(DELETE_BATCH));
    let (transfer, _ctx) = deleting(dir.path(), &["a.txt", "b.txt"], deleter.clone());

    drive(&transfer).await;

    let state = transfer.inner.state.lock();
    assert_eq!(
        state.snapshot().delete_failures,
        2,
        "a refusal was not attributed per key"
    );
    assert_eq!(
        state.snapshot().removed,
        0,
        "a refused key was counted as removed"
    );
    let failures = state.delete_failure_sample();
    assert!(
        failures.iter().any(|failure| failure.key == "a.txt")
            && failures.iter().any(|failure| failure.key == "b.txt"),
        "the refusal did not reach the record naming its key: {failures:?}"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_run_not_asked_to_delete_leaves_the_key_and_still_counts_it() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, _ctx) = deleting_with(
        dir.path(),
        &["gone-a.txt", "gone-b.txt"],
        deleter.clone(),
        DeleteMode::Off,
    );

    drive(&transfer).await;

    assert_eq!(
        deleter.keys_sent(),
        0,
        "a run that was not asked to delete issued {} keys anyway",
        deleter.keys_sent()
    );
    let snapshot = transfer.inner.state.lock().snapshot();
    assert_eq!(snapshot.removed, 0, "a key was reported as removed");
    assert_eq!(snapshot.decided.deletes, 0, "a delete was decided");
    assert_eq!(
        snapshot.decided.skipped.total(),
        2,
        "the two keys left alone were not accounted for"
    );
    assert_eq!(
        snapshot.decided.deletable, 2,
        "the run cannot say how many objects turning deletion on would remove"
    );
    assert!(snapshot.deletes_waiting == 0);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_refused_delete_request_gives_each_key_its_service_code() {
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;

    let denied = mock!(aws_sdk_s3::Client::delete_objects).then_error(|| {
        DeleteObjectsError::generic(
            aws_sdk_s3::error::ErrorMetadata::builder()
                .code("AccessDenied")
                .message("denied")
                .build(),
        )
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&denied]);
    let deleter = DeleteFromBucket::new(client, "amzn-s3-demo-bucket", None);

    let outcomes = DeleteKeys::delete(
        &deleter,
        ["a.txt", "b.txt"].map(String::from).to_vec(),
        still_running(),
    )
    .await;

    for (outcome, key) in outcomes.iter().zip(["a.txt", "b.txt"]) {
        let failure = outcome
            .as_ref()
            .expect_err("a refused request removed a key");
        assert_eq!(failure.key, key);
        assert_eq!(
            failure.error.code(),
            Some("AccessDenied"),
            "a key lost the code of the request that failed for it: {failure:?}"
        );
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_deleter_reports_each_key_from_the_response_it_got() {
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
    use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

    let quiet_flags = Arc::new(Mutex::new(Vec::<Option<bool>>::new()));
    let quiet_recorder = quiet_flags.clone();
    let answered = mock!(aws_sdk_s3::Client::delete_objects)
        .match_requests(move |req| {
            quiet_recorder
                .lock()
                .push(req.delete().and_then(|d| d.quiet()));
            true
        })
        .then_output(|| {
            DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![DeletedObject::builder()
                    .key("data/gone.txt")
                    .build()]))
                .set_errors(Some(vec![
                    S3Error::builder()
                        .key("data/held.txt")
                        .code("AccessDenied")
                        .message("denied")
                        .build(),
                    S3Error::builder()
                        .key("data/terse.txt")
                        .code("InvalidArgument")
                        .build(),
                ]))
                .build()
        });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&answered]);

    let deleter: Arc<dyn DeleteKeys> = Arc::new(DeleteFromBucket::new(
        client,
        "amzn-s3-demo-bucket",
        Some("data"),
    ));
    let outcomes = deleter
        .delete(
            ["gone.txt", "held.txt", "silent.txt", "terse.txt"]
                .map(String::from)
                .to_vec(),
            still_running(),
        )
        .await;

    assert_eq!(
        outcomes.len(),
        4,
        "the outcomes do not account for every key sent"
    );
    assert!(matches!(&outcomes[0], Ok(key) if key == "gone.txt"));
    let held = outcomes[1]
        .as_ref()
        .expect_err("a refusal was read as success");
    assert!(
        held.why().starts_with("held.txt:") && held.why().contains("denied"),
        "a refusal did not name its key and reason: {held:?}"
    );
    assert_eq!(
        (held.error.code(), held.error.operation_name()),
        (Some("AccessDenied"), Some("DeleteObjects")),
        "a refusal lost the service code S3 gave for its key"
    );
    let silent = outcomes[2]
        .as_ref()
        .expect_err("a key the response skipped was read as success");
    assert!(
        silent.why().starts_with("silent.txt:"),
        "an unmentioned key was not named: {silent:?}"
    );
    let terse = outcomes[3]
        .as_ref()
        .expect_err("a refusal was read as success");
    assert!(
        terse.why().starts_with("terse.txt:") && terse.why().contains("InvalidArgument"),
        "a refusal carrying only a code reported no reason: {terse:?}"
    );
    assert!(
        quiet_flags.lock().iter().all(|q| *q == Some(false)),
        "the request left the response's verbosity to chance: {:?}",
        quiet_flags.lock()
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test(start_paused = true)]
async fn a_key_refused_for_load_is_asked_about_again() {
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
    use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

    let asked = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let recorder = asked.clone();
    let quiet_flags = Arc::new(Mutex::new(Vec::<Option<bool>>::new()));
    let quiet_recorder = quiet_flags.clone();
    let first = mock!(aws_sdk_s3::Client::delete_objects)
        .match_requests(move |req| {
            let names = req
                .delete()
                .map(|d| {
                    d.objects()
                        .iter()
                        .map(|o| o.key().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            recorder.lock().push(names);
            quiet_recorder
                .lock()
                .push(req.delete().and_then(|d| d.quiet()));
            true
        })
        .sequence()
        .output(|| {
            DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![DeletedObject::builder().key("gone.txt").build()]))
                .set_errors(Some(vec![S3Error::builder()
                    .key("busy.txt")
                    .code("SlowDown")
                    .message("slow down")
                    .build()]))
                .build()
        })
        .output(|| {
            DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![DeletedObject::builder().key("busy.txt").build()]))
                .build()
        })
        .build();
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&first]);

    let deleter: Arc<dyn DeleteKeys> =
        Arc::new(DeleteFromBucket::new(client, "amzn-s3-demo-bucket", None));
    let started = tokio::time::Instant::now();
    let outcomes = deleter
        .delete(
            ["gone.txt", "busy.txt"].map(String::from).to_vec(),
            still_running(),
        )
        .await;
    let waited = started.elapsed();

    assert!(
        matches!(outcomes.as_slice(), [Ok(a), Ok(b)] if a == "gone.txt" && b == "busy.txt"),
        "a key refused for load was not asked about again: {outcomes:?}"
    );
    let batches = asked.lock().clone();
    assert_eq!(
        batches.len(),
        2,
        "the refusal did not cost a second request"
    );
    assert_eq!(
        batches[1],
        vec!["busy.txt".to_string()],
        "the second request named keys the first had already removed"
    );
    assert!(
        waited > Duration::ZERO,
        "the refused key was asked about again with no wait"
    );
    assert!(
        quiet_flags.lock().iter().all(|q| *q == Some(false)),
        "a request left the response's verbosity to chance: {:?}",
        quiet_flags.lock()
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test(start_paused = true)]
async fn a_run_that_stops_while_a_key_waits_sends_no_further_attempt() {
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
    use aws_sdk_s3::types::{DeletedObject, Error as S3Error};

    let asked = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
    let recorder = asked.clone();
    let rule = mock!(aws_sdk_s3::Client::delete_objects)
        .match_requests(move |req| {
            let names = req
                .delete()
                .map(|d| {
                    d.objects()
                        .iter()
                        .map(|o| o.key().to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            recorder.lock().push(names);
            true
        })
        .then_output(|| {
            DeleteObjectsOutput::builder()
                .set_deleted(Some(vec![DeletedObject::builder().key("gone.txt").build()]))
                .set_errors(Some(vec![S3Error::builder()
                    .key("busy.txt")
                    .code("SlowDown")
                    .message("slow down")
                    .build()]))
                .build()
        });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&rule]);
    let deleter: Arc<dyn DeleteKeys> =
        Arc::new(DeleteFromBucket::new(client, "amzn-s3-demo-bucket", None));

    let stopped = || true;
    let outcomes = deleter
        .delete(
            ["gone.txt", "busy.txt"].map(String::from).to_vec(),
            &stopped,
        )
        .await;

    let batches = asked.lock().clone();
    assert_eq!(
        batches.len(),
        1,
        "a stopped run asked again for a throttled key: {batches:?}"
    );
    assert!(
        matches!(&outcomes[0], Ok(key) if key == "gone.txt"),
        "the key the response removed was not reported as removed"
    );
    let busy = outcomes[1]
        .as_ref()
        .expect_err("a key never answered for was reported as removed");
    assert!(
        busy.why().contains("SlowDown"),
        "the key lost the reason the service gave for refusing it: {busy:?}"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_delete_still_out_holds_the_run_open() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = Arc::new(RecordDeletes::new(DELETE_BATCH));
    let (transfer, _ctx) = deleting(dir.path(), &["a.txt"], deleter);

    while let PollWork::Ready { io: mut work, .. } = transfer.poll_work() {
        let is_delete = matches!(
            work.data.as_ref().map(|d| format!("{d:?}")),
            Some(ref s) if s.starts_with("DeleteKeys")
        );
        if is_delete {
            {
                let snapshot = transfer.inner.state.lock().snapshot();
                assert!(
                    snapshot.deletes_waiting == 0,
                    "the keys did not leave the buffer when the batch was built"
                );
                assert_eq!(snapshot.deletes_in_flight, 1);
            }
            assert!(
                matches!(transfer.poll_work(), PollWork::Pending),
                "a delete still out did not hold the run open"
            );
            transfer.execute(&mut work).await;
            break;
        }
        transfer.execute(&mut work).await;
    }
    assert!(matches!(transfer.poll_work(), PollWork::Done));
}
