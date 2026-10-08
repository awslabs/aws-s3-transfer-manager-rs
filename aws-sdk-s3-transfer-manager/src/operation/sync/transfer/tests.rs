/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::time::Duration;

use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::types::Object;
use aws_smithy_mocks::{mock, mock_client, RuleMode};

use crate::io::walk::{FsWalk, S3Walk};
use crate::operation::sync::modes::Mode;
use crate::operation::sync::walk::{LocalAndBucket, Walker};

use super::child::*;
use super::delete::*;
use super::state::*;
use super::test_util::*;
use super::*;

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
            Arc::new(RecordDeletes::new(DELETE_BATCH)),
            RunSettings {
                max_children: 2,
                delete_mode: DeleteMode::On,
                failure_policy: FailedTransferPolicy::Continue,
            },
        ),
        ctx,
    )
}

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
        transfer.inner.state.lock().snapshot().paired
    };
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the run parked: a poll answered Pending with no wake to follow")
}

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
            Obstruction::NothingToRead => unreachable!("not under test here"),
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

fn downloading_into(
    local: &Path,
    client: aws_sdk_s3::Client,
    delete_mode: DeleteMode,
) -> (
    SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    TransferContext,
    crate::transfer::StateMachineTerminalReceiver,
) {
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .downloading(
            LocalAndBucket::builder()
                .local_root(local)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().downloading(),
        Arc::new(SpawnDownload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            None,
            local,
        )),
        Arc::new(DeleteFromLocalTree::new(local)),
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx, rx)
}

async fn run_managed(
    transfer: &SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
    ctx: &TransferContext,
    rx: crate::transfer::StateMachineTerminalReceiver,
) {
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    tokio::time::timeout(Duration::from_secs(20), rx)
        .await
        .expect("the run did not finish inside twenty seconds")
        .expect("the terminal signal was dropped");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_lands_under_its_directories_with_the_objects_time() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let when = 1_600_000_000i64;
    let client = a_bucket_to_download(&["a.txt", "deep/down/c.txt"], when);
    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    for key in ["a.txt", "deep/down/c.txt"] {
        let path = dir.path().join(key);
        let meta = std::fs::metadata(&path)
            .unwrap_or_else(|e| panic!("{key} did not arrive at its final name: {e}"));
        let stamped = meta
            .modified()
            .expect("a modified time")
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("a time after the epoch")
            .as_secs();
        assert_eq!(
            stamped, when as u64,
            "{key} kept the time it was written rather than the object's"
        );
    }
    let strays: Vec<_> = walkdir_s3tmp(dir.path());
    assert!(
        strays.is_empty(),
        "temporary files were left behind: {strays:?}"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_time_that_cannot_be_applied_does_not_cost_the_download() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let client = a_bucket_to_download(&["a.txt"], i64::MAX);
    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    let path = dir.path().join("a.txt");
    assert!(
        path.exists(),
        "a stamp that could not be applied took the downloaded file with it"
    );
    assert_eq!(
        std::fs::read(&path).expect("the file reads"),
        b"hello",
        "the file arrived without its contents"
    );
    assert!(
        !ctx.is_failed(),
        "a stamp that could not be applied failed the run"
    );
    let stamped = std::fs::metadata(&path)
        .expect("the file has metadata")
        .modified()
        .expect("a modified time")
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("a time after the epoch")
        .as_secs();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_secs();
    assert!(
        stamped > now + 86_400 * 365,
        "the file is dated {stamped}, within a year of the {now} it was written at, \
         so the object's time never reached it"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_downloaded_file_carries_the_time_the_object_had() {
    const AT: i64 = 1_600_000_000;

    let dir = tempfile::tempdir().expect("a temp dir");
    let client = a_bucket_to_download(&["a.txt"], AT);
    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    let stamped = std::fs::metadata(dir.path().join("a.txt"))
        .expect("the file has metadata")
        .modified()
        .expect("a modified time")
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("a time after the epoch")
        .as_secs();
    assert_eq!(
        stamped, AT as u64,
        "the file holds its own write time, so the object's was never applied"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_local_delete_removes_the_file_and_leaves_its_directory() {
    let dir = tempfile::tempdir().expect("a temp dir");
    a_local_tree(dir.path(), &["keep/gone.txt"]);
    let client = a_bucket_to_download(&[], 1_600_000_000);
    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    assert!(
        !dir.path().join("keep/gone.txt").exists(),
        "the file the source does not have is still there"
    );
    assert!(
        dir.path().join("keep").is_dir(),
        "the directory went with the file"
    );
    assert_eq!(transfer.inner.state.lock().snapshot().removed, 1);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_object_whose_key_names_a_place_is_reported_not_written() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let client = a_bucket_to_download(&["photos/2019/"], 1_600_000_000);
    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    assert!(
        !dir.path().join("photos/2019").exists(),
        "a key naming a place was written as a file"
    );
    assert_eq!(
        transfer.inner.state.lock().snapshot().transfer_failures,
        1,
        "the key was not accounted for"
    );
    assert!(!ctx.is_failed(), "a continuing run reported itself failed");
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_key_naming_somewhere_above_the_root_is_refused() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().join("inside");
    std::fs::create_dir(&root).expect("the root");
    let outside = dir.path().join("outside.txt");
    std::fs::write(&outside, b"not sync's").expect("a file");

    let spawner = SpawnDownload::new(
        crate::client::Handle::test_handle_tokio(
            crate::Config::builder()
                .client(a_bucket_holding(&[]))
                .build(),
        ),
        "amzn-s3-demo-bucket",
        None,
        &root,
    );
    assert!(
        spawner.file_path("../outside.txt").is_err(),
        "a download would have written above its destination"
    );

    let deleter = DeleteFromLocalTree::new(&root);
    assert!(
        deleter.file_path("../outside.txt").is_err(),
        "a delete would have reached above its destination"
    );
    let outcomes = deleter.delete(vec!["../outside.txt".to_string()]).await;
    assert!(
        outcomes[0].is_err(),
        "the key was accepted rather than refused"
    );
    assert!(
        outside.exists(),
        "a file outside the destination was removed"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_refusal_carries_the_category_its_destination_answers_for() {
    use crate::error::ErrorKind;

    let dir = tempfile::tempdir().expect("a temp dir");
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).expect("a directory to stand in for an unremovable file");

    let deleter = DeleteFromLocalTree::new(dir.path());
    let outcomes = deleter.delete(vec!["locked".to_string()]).await;
    let refusal = outcomes[0]
        .as_ref()
        .expect_err("the disk accepted a directory as a file");
    assert_eq!(
        refusal.kind(),
        &ErrorKind::IOError,
        "a local delete refused by the disk reports {:?}",
        refusal.kind()
    );

    let outcomes = deleter.delete(vec!["../outside.txt".to_string()]).await;
    let refusal = outcomes[0]
        .as_ref()
        .expect_err("a key reaching outside the destination was accepted");
    assert_eq!(
        refusal.kind(),
        &ErrorKind::InputInvalid,
        "a key naming no file this run would remove reports {:?}",
        refusal.kind()
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn deleting_a_file_that_is_already_gone_is_not_a_failure() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let deleter = DeleteFromLocalTree::new(dir.path());

    let outcomes = deleter.delete(vec!["never-existed.txt".to_string()]).await;

    assert_eq!(outcomes, vec![Ok("never-existed.txt".to_string())]);
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_destination_that_does_not_exist_yet_is_not_a_failure() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().join("not-there-yet");
    let client = a_bucket_to_download(&["a.txt"], 1_600_000_000);
    let (transfer, ctx, rx) = downloading_into(&root, client, DeleteMode::On);

    run_managed(&transfer, &ctx, rx).await;

    assert!(
        !ctx.is_failed(),
        "a destination that did not exist yet failed the run"
    );
    assert!(
        root.join("a.txt").exists(),
        "the key did not arrive under a root that had to be made"
    );
    assert_eq!(transfer.outcome(), RunOutcome::Clean);
}

fn walkdir_s3tmp(root: &Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.to_string_lossy().contains(".s3tmp.") {
                found.push(path);
            }
        }
    }
    found
}

#[cfg_attr(miri, ignore)]
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_run_lets_go_of_the_files_its_children_opened() {
    let dir = tempfile::tempdir().expect("a temp dir");

    let pending: Arc<Mutex<Option<TransferContext>>> = Arc::new(Mutex::new(None));
    let fetched = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cancel_on_first = pending.clone();
    let counter = fetched.clone();

    let contents: Vec<Object> = ["a.txt", "b.txt", "c.txt"]
        .iter()
        .map(|k| {
            Object::builder()
                .key(*k)
                .size(5)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
                .build()
        })
        .collect();
    let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
        ListObjectsV2Output::builder()
            .set_contents(Some(contents.clone()))
            .build()
    });
    let get = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
        if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            if let Some(ctx) = cancel_on_first.lock().as_ref() {
                ctx.handle.scheduler.cancel_transfer(ctx.id);
            }
        }
        aws_sdk_s3::operation::get_object::GetObjectOutput::builder()
            .content_length(5)
            .last_modified(aws_smithy_types::DateTime::from_secs(1_600_000_000))
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
            .build()
    });
    let client = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[&list, &get]);

    let (transfer, ctx, rx) = downloading_into(dir.path(), client, DeleteMode::On);
    *pending.lock() = Some(ctx.clone());
    ctx.handle
        .scheduler
        .enqueue_transfer(Box::new(transfer.clone()));
    let _ = tokio::time::timeout(Duration::from_secs(20), rx).await;

    assert!(
        fetched.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "nothing was fetched, so the cancellation landed before any child opened a file"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !walkdir_s3tmp(dir.path()).is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let strays = walkdir_s3tmp(dir.path());
    assert!(
        strays.is_empty(),
        "a cancelled run is still holding the files its children opened: {strays:?}"
    );
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_asks_for_the_key_under_its_prefix() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let (client, asked) =
        a_bucket_recording_gets(&["backup/a.txt", "backup/nested/b.txt"], 1_600_000_000);
    let config = crate::Config::builder().client(client.clone()).build();
    let handle = crate::client::Handle::test_handle_managed(config);
    let (ctx, rx) = TransferContext::new(handle);
    let walk = Walker::builder()
        .build()
        .downloading(
            LocalAndBucket::builder()
                .local_root(dir.path())
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .prefix("backup/")
                .build(),
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().downloading(),
        Arc::new(SpawnDownload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            Some("backup/"),
            dir.path(),
        )),
        Arc::new(DeleteFromLocalTree::new(dir.path())),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::Off,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    run_managed(&transfer, &ctx, rx).await;

    let mut asked = asked.lock().clone();
    asked.sort();
    assert_eq!(
        asked,
        vec![
            "backup/a.txt".to_string(),
            "backup/nested/b.txt".to_string()
        ],
        "a download asked for keys without the prefix it was listing under"
    );
}

#[test]
fn neither_local_site_acts_on_a_key_naming_a_place() {
    let root = Path::new("/tmp/root");
    for key in ["photos/2019/", "a/"] {
        assert!(
            local_path_for_key(root, key).is_err(),
            "a local site would have acted on {key:?}"
        );
    }
    assert_eq!(
        local_path_for_key(root, "photos/2019").expect("a file"),
        Path::new("/tmp/root/photos/2019")
    );
}

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
        transfer.stop_if_aborting(
            &mut state,
            crate::error::Error::new(
                crate::error::ErrorKind::IOError,
                "a child failed while the batch was away",
            ),
        );
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

#[test]
fn the_local_deleter_refuses_a_key_its_derivation_would_rewrite() {
    let deleter = DeleteFromLocalTree::new("/tmp/root");
    for key in ["a//b", "a/./b", "a/../b", "./a"] {
        assert!(
            deleter.file_path(key).is_err(),
            "the deleter accepted {key:?}, whose path names a different key's file"
        );
    }
    assert_eq!(
        deleter.file_path("a/b").expect("a key from a real path"),
        std::path::Path::new("/tmp/root/a/b")
    );
}

#[test]
fn the_local_deleter_accepts_its_keys_under_a_root_written_the_long_way_round() {
    for root in [".", "./", "/tmp/other/../root", "/tmp/./root"] {
        let deleter = DeleteFromLocalTree::new(root);
        let got = deleter.file_path("a/b");
        assert!(
            got.is_ok(),
            "root {root:?} refused a key inside it: {}",
            got.unwrap_err()
        );
    }
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
    let _ = transfer.poll_work();
    assert!(
        transfer.inner.state.lock().snapshot().transfers_waiting > 0,
        "nothing was left buffered, so this proves nothing about the counts"
    );

    spawner.release(&ctx);
    drive(&transfer).await;

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
        snapshot.arrived, 1,
        "one key arrived and the run reports {}",
        snapshot.arrived
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

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_loop_warns_under_either_policy() {
    for policy in [FailedTransferPolicy::Continue, FailedTransferPolicy::Abort] {
        let dir = tempfile::tempdir().expect("a temp dir");
        std::fs::write(dir.path().join("a.txt"), b"x").expect("a file");
        let inner = dir.path().join("down");
        std::fs::create_dir(&inner).expect("a directory");
        std::os::unix::fs::symlink(dir.path(), inner.join("up")).expect("a symlink");

        let client = a_bucket_holding(&[]);
        let config = crate::Config::builder().client(client.clone()).build();
        let handle = crate::client::Handle::test_handle_tokio(config);
        let (ctx, _rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .follow_symlinks(true)
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
                failure_policy: policy.clone(),
            },
        );

        drive(&transfer).await;

        let state = transfer.inner.state.lock();
        assert_eq!(
            state.snapshot().warnings_kept,
            1,
            "under {policy:?} the loop was not kept where a caller can read it"
        );
        assert!(
            state.walk_failure_sample().is_empty(),
            "a loop nothing could transfer was filed as a failure: {:?}",
            state.walk_failure_sample()
        );
        drop(state);
        assert_eq!(
            transfer.outcome(),
            RunOutcome::Warned,
            "under {policy:?} the run did not report a warning"
        );
        assert!(
            !ctx.is_failed(),
            "an aborting run failed over something it was never going to send"
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

fn spawn_until_something_else(transfer: &SyncTransfer<FsWalk, S3Walk>) -> PollWork {
    loop {
        match transfer.poll_work() {
            PollWork::Spawned => continue,
            other => return other,
        }
    }
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

fn uploading_with(
    local: &Path,
    spawner: Arc<dyn SpawnChild<crate::io::walk::FsEntry>>,
    cap: usize,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    uploading_with_policy(local, spawner, cap, FailedTransferPolicy::Continue)
}

fn uploading_with_policy(
    local: &Path,
    spawner: Arc<dyn SpawnChild<crate::io::walk::FsEntry>>,
    cap: usize,
    failure_policy: FailedTransferPolicy,
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        spawner,
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: cap,
            delete_mode: DeleteMode::On,
            failure_policy,
        },
    );
    (transfer, ctx)
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
    let _ = transfer.poll_work();
    let _ = transfer.poll_work();
    let _ = transfer.poll_work();

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

fn deleting(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext) {
    deleting_with(local, bucket_keys, deleter, DeleteMode::On)
}

fn with_spawner(
    local: &Path,
    spawner: SpawnEnded,
    failure_policy: FailedTransferPolicy,
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(spawner),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::On,
            failure_policy,
        },
    );
    (transfer, ctx)
}

fn deleting_with_policy(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
    delete_mode: DeleteMode,
    failure_policy: FailedTransferPolicy,
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnEnded::new(0, false)),
        deleter,
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy,
        },
    );
    (transfer, ctx)
}

fn uploading_comparing_with<C>(
    local: &Path,
    comparison: &'static C,
) -> (SyncTransfer<FsWalk, S3Walk>, TransferContext)
where
    C: Compare<crate::io::walk::FsEntry, aws_sdk_s3::types::Object> + Send + Sync + 'static,
{
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        comparison,
        Arc::new(SpawnEnded::new(0, false)),
        Arc::new(RecordDeletes::new(DELETE_BATCH)),
        RunSettings {
            max_children: 4,
            delete_mode: DeleteMode::Off,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx)
}

fn deleting_with(
    local: &Path,
    bucket_keys: &[&str],
    deleter: Arc<RecordDeletes>,
    delete_mode: DeleteMode,
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
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        Mode::default().uploading(),
        Arc::new(SpawnEnded::new(0, false)),
        deleter,
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx)
}

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
    assert_eq!(snapshot.refusals, 0);
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
        state.snapshot().refusals,
        2,
        "a refusal was not attributed per key"
    );
    assert_eq!(
        state.snapshot().removed,
        0,
        "a refused key was counted as removed"
    );
    let refusals = state.delete_refusals();
    assert!(
        refusals.iter().any(|why| why.starts_with("a.txt:"))
            && refusals.iter().any(|why| why.starts_with("b.txt:")),
        "the refusal did not reach the record naming its key: {refusals:?}"
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
    assert_eq!(outcomes[0], Ok("gone.txt".to_string()));
    let held = outcomes[1]
        .as_ref()
        .expect_err("a refusal was read as success");
    assert!(
        held.why().starts_with("held.txt:") && held.why().contains("denied"),
        "a refusal did not name its key and reason: {held:?}"
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

    assert_eq!(
        outcomes,
        vec![Ok("gone.txt".to_string()), Ok("busy.txt".to_string())],
        "a key refused for load was not asked about again"
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
    assert_eq!(
        outcomes[0],
        Ok("gone.txt".to_string()),
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
        transfer.stop_if_aborting(
            &mut state,
            crate::error::Error::new(crate::error::ErrorKind::IOError, "a child failed"),
        );
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

// ======================================================================
// Real-bucket runs. `#[ignore]`d, so the ordinary suite never reaches S3:
//
//   S3_TEST_BUCKET_NAME_RS=<bucket> AWS_REGION=<region> \
//   SSL_CERT_FILE=/etc/ssl/cert.pem \
//     cargo test -p aws-sdk-s3-transfer-manager --lib real_bucket \
//     -- --ignored --test-threads=1 --nocapture
//
// rustls may not read the platform root certificates. Set `SSL_CERT_FILE` before running ignored
// real-bucket tests.
//
// TODO(sync): Ignored real-bucket tests call crate-private sync APIs. Move customer-facing cases to
// `examples/` after the public API can start a sync run.
// ======================================================================
mod real_bucket {
    use super::*;

    const BUCKET_VAR: &str = "S3_TEST_BUCKET_NAME_RS";

    fn named_bucket(var: &str) -> String {
        std::env::var(var).unwrap_or_else(|_| {
            panic!("set {var} to a bucket these tests may write to and delete from")
        })
    }

    fn regular_bucket() -> String {
        named_bucket(BUCKET_VAR)
    }

    async fn real_client() -> aws_sdk_s3::Client {
        let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_s3::Client::new(&sdk)
    }

    fn a_run_prefix(what: &str) -> String {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a clock after the epoch")
            .as_nanos();
        format!("sync-e2e/{what}-{stamp}/")
    }

    async fn bucket_keys(c: &aws_sdk_s3::Client, prefix: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut req = c.list_objects_v2().bucket(regular_bucket()).prefix(prefix);
            if let Some(t) = token {
                req = req.continuation_token(t);
            }
            let page = req.send().await.expect("the listing answers");
            for o in page.contents() {
                if let Some(k) = o.key() {
                    out.push(k.strip_prefix(prefix).unwrap_or(k).to_string());
                }
            }
            token = page.next_continuation_token().map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        out.sort();
        out
    }

    fn tree_files(root: &Path) -> Vec<String> {
        fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, base, out);
                } else {
                    let rel = p.strip_prefix(base).expect("a path under the root");
                    out.push(
                        rel.to_string_lossy()
                            .replace(std::path::MAIN_SEPARATOR, "/"),
                    );
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out.sort();
        out
    }

    fn a_tree_with_contents(root: &Path, files: &[(&str, &str)]) {
        for (rel, body) in files {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().expect("a parent")).expect("directories are made");
            std::fs::write(&p, body).expect("the file is written");
        }
    }

    async fn remove_prefix(c: &aws_sdk_s3::Client, prefix: &str) {
        let keys = bucket_keys(c, prefix).await;
        for chunk in keys.chunks(1000) {
            let ids: Vec<_> = chunk
                .iter()
                .filter_map(|k| {
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key(format!("{prefix}{k}"))
                        .build()
                        .ok()
                })
                .collect();
            if ids.is_empty() {
                continue;
            }
            let del = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(ids))
                .build()
                .expect("a delete request");
            let _ = c
                .delete_objects()
                .bucket(regular_bucket())
                .delete(del)
                .send()
                .await;
        }
    }

    #[derive(Debug)]
    struct RealRun {
        moved: u64,
        transfers: u64,
        transferred: u64,
        deleted: u64,
        failures: u64,
        skipped: u64,
    }

    fn counters<S: KeyStream, D: KeyStream>(t: &SyncTransfer<S, D>) -> RealRun {
        let snapshot = t.inner.state.lock().snapshot();
        RealRun {
            moved: snapshot.bytes,
            transfers: snapshot.decided.transfers,
            transferred: snapshot.arrived,
            deleted: snapshot.removed,
            failures: snapshot.transfer_failures + snapshot.walk_failures,
            skipped: snapshot.decided.skipped.total(),
        }
    }

    async fn real_upload(
        local: &Path,
        prefix: &str,
        c: aws_sdk_s3::Client,
        delete_mode: DeleteMode,
    ) -> RealRun {
        let config = crate::Config::builder().client(c.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .uploading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(c.clone())
                    .bucket(regular_bucket())
                    .prefix(prefix)
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().uploading(),
            Arc::new(SpawnUpload::new(
                ctx.handle.clone(),
                regular_bucket(),
                Some(prefix),
            )),
            Arc::new(DeleteFromBucket::new(c, regular_bucket(), Some(prefix))),
            RunSettings {
                max_children: 8,
                delete_mode,
                failure_policy: FailedTransferPolicy::Continue,
            },
        );
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(300), rx)
            .await
            .expect("the upload did not finish inside five minutes")
            .expect("the terminal signal was dropped");
        counters(&transfer)
    }

    async fn real_download(
        local: &Path,
        prefix: &str,
        c: aws_sdk_s3::Client,
        delete_mode: DeleteMode,
    ) -> RealRun {
        let config = crate::Config::builder().client(c.clone()).build();
        let handle = crate::client::Handle::test_handle_managed(config);
        let (ctx, rx) = TransferContext::new(handle);
        let walk = Walker::builder()
            .build()
            .downloading(
                LocalAndBucket::builder()
                    .local_root(local)
                    .client(c)
                    .bucket(regular_bucket())
                    .prefix(prefix)
                    .build(),
            )
            .expect("an ordinary bucket builds");
        let transfer = SyncTransfer::new(
            ctx.clone(),
            walk,
            Mode::default().downloading(),
            Arc::new(SpawnDownload::new(
                ctx.handle.clone(),
                regular_bucket(),
                Some(prefix),
                local,
            )),
            Arc::new(DeleteFromLocalTree::new(local)),
            RunSettings {
                max_children: 8,
                delete_mode,
                failure_policy: FailedTransferPolicy::Continue,
            },
        );
        ctx.handle
            .scheduler
            .enqueue_transfer(Box::new(transfer.clone()));
        tokio::time::timeout(Duration::from_secs(300), rx)
            .await
            .expect("the download did not finish inside five minutes")
            .expect("the terminal signal was dropped");
        counters(&transfer)
    }

    const REAL_TREE: &[(&str, &str)] = &[
        ("a.txt", "alpha"),
        ("b.txt", "bravo"),
        ("nested/c.txt", "charlie"),
        ("nested/deep/d.txt", "delta"),
        ("empty.txt", ""),
    ];

    fn real_tree_keys() -> Vec<String> {
        let mut v: Vec<String> = REAL_TREE.iter().map(|(k, _)| k.to_string()).collect();
        v.sort();
        v
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_upload_puts_every_key_under_its_prefix() {
        let c = real_client().await;
        let prefix = a_run_prefix("up-prefix");
        let dir = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(dir.path(), REAL_TREE);

        let run = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload: {run:?}");

        assert_eq!(
            bucket_keys(&c, &prefix).await,
            real_tree_keys(),
            "the bucket does not hold the tree under {prefix}"
        );
        assert_eq!(run.failures, 0, "a key failed");
        let stray = bucket_keys(&c, "a.txt").await;
        assert!(
            stray.is_empty(),
            "a key landed at the bucket root: {stray:?}"
        );

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_download_strips_the_prefix_from_every_key() {
        let c = real_client().await;
        let prefix = a_run_prefix("down-prefix");
        let src = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(src.path(), REAL_TREE);
        real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;

        let dst = tempfile::tempdir().expect("a temp dir");
        let run = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download: {run:?}");

        assert_eq!(
            tree_files(dst.path()),
            real_tree_keys(),
            "the local tree does not mirror the keys under {prefix}"
        );
        assert_eq!(run.failures, 0, "a key failed");
        assert!(
            !dst.path().join("sync-e2e").exists(),
            "the prefix reached the local tree as a directory"
        );
        for (rel, body) in REAL_TREE {
            let got = std::fs::read_to_string(dst.path().join(rel)).expect("the file reads");
            assert_eq!(&got, body, "{rel} arrived with the wrong contents");
        }

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_second_run_moves_nothing() {
        let c = real_client().await;
        let prefix = a_run_prefix("idempotent");
        let dir = tempfile::tempdir().expect("a temp dir");
        a_tree_with_contents(dir.path(), REAL_TREE);

        let up1 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload 1: {up1:?}");
        let up2 = real_upload(dir.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload 2: {up2:?}");
        assert_eq!(
            up2.transfers,
            0,
            "the second upload re-sent {} of {} keys",
            up2.transfers,
            REAL_TREE.len()
        );

        let dst = tempfile::tempdir().expect("a temp dir");
        let down1 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 1: {down1:?}");
        let down2 = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 2: {down2:?}");
        assert_eq!(
            down2.transfers,
            0,
            "the second download re-fetched {} of {} keys",
            down2.transfers,
            REAL_TREE.len()
        );

        remove_prefix(&c, &prefix).await;
    }

    #[ignore = "will be moved to examples"]
    #[tokio::test]
    async fn real_bucket_moves_an_object_past_the_multipart_threshold() {
        let c = real_client().await;
        let prefix = a_run_prefix("multipart");
        let src = tempfile::tempdir().expect("a temp dir");
        let body: Vec<u8> = (0..20 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(src.path().join("big.dat"), &body).expect("the file is written");

        let up = real_upload(src.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  upload: {up:?}");
        assert_eq!(up.failures, 0, "the multipart upload failed");
        assert_eq!(
            up.moved,
            body.len() as u64,
            "the run moved {} bytes of {}",
            up.moved,
            body.len()
        );

        let dst = tempfile::tempdir().expect("a temp dir");
        let down = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download: {down:?}");
        assert_eq!(down.failures, 0, "the multipart download failed");
        let back = std::fs::read(dst.path().join("big.dat")).expect("the copy reads");
        assert_eq!(
            back.len(),
            body.len(),
            "the copy is {} bytes against {}",
            back.len(),
            body.len()
        );
        assert!(back == body, "the bytes came back in a different order");

        let again = real_download(dst.path(), &prefix, c.clone(), DeleteMode::Off).await;
        println!("  download 2: {again:?}");
        assert_eq!(
            again.transfers, 0,
            "the second download fetched the object again"
        );

        remove_prefix(&c, &prefix).await;
    }
}
