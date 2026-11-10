/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests check how a run writes, stamps and deletes files in the local tree, and which
// keys it refuses there.

use super::*;

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

    assert!(
        matches!(outcomes.as_slice(), [Ok(key)] if key == "never-existed.txt"),
        "a file already gone was not reported as removed: {outcomes:?}"
    );
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
            DeleteMode::Off,
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
fn neither_local_site_acts_on_a_key_whose_path_names_another_key() {
    let root = Path::new("/tmp/root");
    for key in ["a//b", "a/./b", "a/../b", "./a"] {
        assert!(
            crate::io::key::local_path_strict(root, key).is_err(),
            "a local site accepted {key:?}, whose path names a different key's file"
        );
    }
}

// The bucket's `a//b` and the local `a/b` are different keys, so a download run in delete mode
// removes `a/b`. Path cleaning turns `a//b` into `a/b`, so a download of `a//b` would write the
// file the same run removes.
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_refuses_a_key_whose_path_names_another_key() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    a_local_tree(&root, &["a/b"]);
    let (client, gets) = a_bucket_recording_gets(&["a//b"], 1_600_000_000);

    let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::On);
    run_managed(&t, &ctx, rx).await;

    assert!(
        gets.lock().is_empty(),
        "the run downloaded `a//b` onto `a/b`"
    );
    let state = t.inner.state.lock();
    let refused: Vec<&str> = state
        .transfer_failure_sample()
        .iter()
        .map(|failure| failure.key.as_str())
        .collect();
    assert_eq!(refused, ["a//b"], "the run did not refuse `a//b`");
}

#[test]
fn neither_local_site_acts_on_a_key_naming_a_place() {
    let root = Path::new("/tmp/root");
    for key in ["photos/2019/", "a/"] {
        assert!(
            crate::io::key::local_path_strict(root, key).is_err(),
            "a local site would have acted on {key:?}"
        );
    }
    assert_eq!(
        crate::io::key::local_path_strict(root, "photos/2019").expect("a file"),
        Path::new("/tmp/root/photos/2019")
    );
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
async fn a_file_standing_where_a_directory_is_needed_does_not_fail_the_download() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::write(root.join("a"), b"old").expect("a file named a");
    let (client, _gets) = a_bucket_recording_gets(&["a/b"], 1_600_000_000);
    let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::On);
    run_managed(&t, &ctx, rx).await;
    assert!(
        root.join("a/b").exists(),
        "the run removed `a` but never wrote `a/b`"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn each_removal_on_a_path_runs_before_the_downloads_under_it() {
    let outside_photos = tempfile::tempdir().expect("outside");
    let outside_docs = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside_photos.path(), root.join("photos")).expect("symlink");
    std::os::unix::fs::symlink(outside_docs.path(), root.join("docs")).expect("symlink");
    std::fs::write(root.join("a"), b"old").expect("a file named a");
    let keys = [
        "a/b",
        "docs/x.txt",
        "docs/y/z.txt",
        "photos/1.jpg",
        "photos/2.jpg",
        "q.txt",
    ];
    // Run 1 removes the links and holds the keys under them. Run 2 downloads into real directories.
    for _ in 0..2 {
        let client = a_bucket_to_download(&keys, 1_600_000_000);
        let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::On);
        run_managed(&t, &ctx, rx).await;
        let snapshot = t.inner.state.lock().snapshot();
        assert_eq!(snapshot.transfer_failures, 0);
        assert_eq!(snapshot.transfers_held, 0);
    }

    for key in keys {
        let path = root.join(key);
        assert_eq!(
            std::fs::read(&path).unwrap_or_default(),
            b"hello",
            "{key} did not land inside the root"
        );
    }
    for name in ["photos", "docs", "a"] {
        let meta = std::fs::symlink_metadata(root.join(name)).expect("the name exists");
        assert!(
            meta.is_dir(),
            "{name} is not a real directory after the run"
        );
    }
    for outside in [&outside_photos, &outside_docs] {
        let escaped: Vec<_> = std::fs::read_dir(outside.path()).expect("reads").collect();
        assert!(escaped.is_empty(), "the run wrote outside the root");
    }
}

#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn downloads_behind_a_refused_removal_fail_without_writing() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::write(root.join("a"), b"old").expect("a file named a");
    let client = a_bucket_to_download(&["a/b", "a/c", "q.txt"], 1_600_000_000);
    let deleter = Arc::new(RecordDeletes::refusing_in_a_tree(1));
    let (t, ctx, rx) = downloading_into_with(
        &root,
        client,
        DeleteMode::On,
        deleter,
        Walker::builder().build(),
    );

    run_managed(&t, &ctx, rx).await;

    assert_eq!(
        std::fs::read(root.join("a")).expect("a is still a file"),
        b"old"
    );
    assert!(
        root.join("q.txt").exists(),
        "an unblocked download did not run"
    );
    let state = t.inner.state.lock();
    assert_eq!(
        state.snapshot().transfer_failures,
        2,
        "each download behind the refused removal should fail"
    );
    assert_eq!(state.snapshot().transfers_held, 0);
    let reasons = state.transfer_failure_sample();
    assert!(
        reasons
            .iter()
            .all(|failure| failure.why().contains("`a` stands on its path")),
        "a failure did not name the removal that blocked it: {reasons:?}"
    );
}
