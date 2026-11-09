/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// These tests run sync over trees that hold symlinks.

use super::*;

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

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_dangling_directory_link_sends_no_delete_for_its_subtree() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::write(root.join("a.txt"), b"x").expect("a.txt");
    std::os::unix::fs::symlink(root.join("unmounted-volume"), root.join("photos"))
        .expect("dangling symlink");
    let (client, deleted) =
        listing_and_recording_deletes(&["a.txt", "photos", "photos/2019/x.jpg", "photos/y.jpg"], 1);
    let (t, _ctx) = upload_run(
        &root,
        client,
        Walker::builder().follow_symlinks(true).build(),
    );
    drive(&t).await;
    let sent = deleted.lock().clone();
    assert!(
        !sent
            .iter()
            .any(|k| k == "photos" || k.starts_with("photos/")),
        "DeleteObjects was sent for objects at or under a dangling directory link: {sent:?}"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn default_settings_send_no_delete_for_objects_under_a_directory_link() {
    let real = tempfile::tempdir().expect("real photos dir");
    std::fs::write(real.path().join("y.jpg"), b"x").expect("y.jpg");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(real.path(), root.join("photos")).expect("symlink");
    let (client, deleted) = listing_and_recording_deletes(&["photos", "photos/y.jpg"], 1);
    let (t, _ctx) = upload_run(&root, client, Walker::builder().build());
    drive(&t).await;
    let sent = deleted.lock().clone();
    assert!(
        sent.is_empty(),
        "default settings deleted an object at or under a directory link: {sent:?}"
    );
}

// A rule on a name matches only that name. `exclude("photos")` hides the link `photos` and the
// object `photos`, and it leaves `photos/y.jpg` in the listing.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_excluded_directory_link_still_holds_back_the_objects_under_its_name() {
    use crate::io::key::filter::{KeyFilter, Rule};

    let real = tempfile::tempdir().expect("real photos dir");
    std::fs::write(real.path().join("y.jpg"), b"x").expect("y.jpg");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(real.path(), root.join("photos")).expect("symlink");
    let (client, deleted) = listing_and_recording_deletes(&["photos/y.jpg"], 1);
    let walker = Walker::builder()
        .filter(Arc::new(KeyFilter::new(vec![Rule::exclude("photos")])))
        .build();
    let (t, _ctx) = upload_run(&root, client, walker);
    drive(&t).await;
    let sent = deleted.lock().clone();
    assert!(
        sent.is_empty(),
        "the run deleted an object under an excluded link: {sent:?}"
    );
    assert!(
        !t.is_plan_complete(),
        "a run that held back keys under a link reported a whole plan"
    );
}

// Two links the filter excludes. Skipping past the second must not release the hold of the first.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_second_excluded_link_keeps_the_keys_under_the_first() {
    use crate::io::key::filter::{KeyFilter, Rule};

    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("real")).expect("real");
    std::os::unix::fs::symlink(root.join("real"), root.join("photos")).expect("photos");
    std::os::unix::fs::symlink(root.join("real"), root.join("videos")).expect("videos");
    let (client, deleted) = listing_and_recording_deletes(&["photos/y.jpg"], 1);
    let walker = Walker::builder()
        .filter(Arc::new(KeyFilter::new(vec![
            Rule::exclude("photos"),
            Rule::exclude("videos"),
        ])))
        .build();
    let (t, _ctx) = upload_run(&root, client, walker);
    drive(&t).await;
    let sent = deleted.lock().clone();
    assert!(
        sent.is_empty(),
        "DeleteObjects was sent for a key under an excluded link: {sent:?}"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_excluded_dangling_link_still_holds_back_the_objects_under_its_name() {
    use crate::io::key::filter::{KeyFilter, Rule};

    let gone = tempfile::tempdir().expect("a target to remove");
    let target = gone.path().join("unmounted");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(&target, root.join("photos")).expect("symlink");
    let (client, deleted) = listing_and_recording_deletes(&["photos/y.jpg"], 1);
    let walker = Walker::builder()
        .follow_symlinks(true)
        .filter(Arc::new(KeyFilter::new(vec![Rule::exclude("photos")])))
        .build();
    let (t, _ctx) = upload_run(&root, client, walker);
    drive(&t).await;
    let sent = deleted.lock().clone();
    assert!(
        sent.is_empty(),
        "the run deleted an object under an excluded dangling link: {sent:?}"
    );
    assert!(
        !t.is_plan_complete(),
        "a run that held back keys under a link reported a whole plan"
    );
}

// `download_objects` renames its temporary file onto the destination name. The rename replaces a
// link at that name and leaves the link's target alone. Sync does the same.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_replaces_a_link_at_its_own_name_as_download_objects_does() {
    let outside = tempfile::tempdir().expect("outside");
    let target = outside.path().join("target.jpg");
    std::fs::write(&target, "original").expect("the target is written");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(&target, root.join("a.jpg")).expect("symlink");
    let (client, gets) = a_bucket_recording_gets(&["a.jpg"], 1_600_000_000);

    let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::Off);
    run_managed(&t, &ctx, rx).await;

    assert_eq!(
        *gets.lock(),
        ["a.jpg"],
        "the run skipped the key a link stood on"
    );
    let landed = std::fs::symlink_metadata(root.join("a.jpg")).expect("the name exists");
    assert!(
        landed.is_file(),
        "the link still stands where the download should land"
    );
    assert_eq!(
        std::fs::read_to_string(root.join("a.jpg")).expect("the file reads"),
        "hello"
    );
    assert_eq!(
        std::fs::read_to_string(&target).expect("the target reads"),
        "original",
        "the download wrote through the link into its target"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_link_whose_lstat_matches_the_object_is_not_read_as_unchanged() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    // The link's `lstat` and the object both report five bytes. The object is newer.
    std::os::unix::fs::symlink("datum", root.join("a.jpg")).expect("symlink");
    let (client, gets) = a_bucket_recording_gets(&["a.jpg"], 4_000_000_000);

    let (t, ctx, rx) = downloading_into(&root, client, DeleteMode::Off);
    run_managed(&t, &ctx, rx).await;

    assert_eq!(
        *gets.lock(),
        ["a.jpg"],
        "the run read the link's own stat and left the key alone"
    );
    assert_eq!(
        t.inner.state.lock().snapshot().decided.skipped.unchanged,
        0,
        "the link's stat matched the object, so the key was counted as unchanged"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_delete_through_a_destination_link_does_not_remove_the_file_it_aliases() {
    let when = 1_600_000_000i64;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("real")).expect("real dir");
    std::fs::write(root.join("real/x.txt"), b"hello").expect("x.txt");
    // the object's time, so the paired key compares unchanged and nothing rewrites it
    std::fs::File::options()
        .write(true)
        .open(root.join("real/x.txt"))
        .expect("x.txt opens")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(when as u64)),
        )
        .expect("the object's time applies");
    std::os::unix::fs::symlink(root.join("real"), root.join("link")).expect("symlink");

    let (t, ctx, rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&["real/x.txt"], when),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    run_managed(&t, &ctx, rx).await;

    assert_eq!(
        std::fs::read(root.join("real/x.txt")).unwrap_or_default(),
        b"hello",
        "the run removed the file it had decided to keep"
    );
}

// A followed link makes two keys name one local file. Each row builds such a tree and names the
// paths the run must leave resolving, and the paths it must still remove.
#[cfg(unix)]
fn stamp_hello(path: &Path, when: i64) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parent dir");
    }
    std::fs::write(path, b"hello").expect("file");
    std::fs::File::options()
        .write(true)
        .open(path)
        .expect("file opens")
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(when as u64)),
        )
        .expect("the object's time applies");
}

// A link in a download's destination that reaches nothing, with following on. It is no
// failure. It holds the keys below its name, because it may stand for a folder on a drive that is
// away. Its own name stays free, so a download there replaces the link.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_destination_link_that_reaches_nothing_holds_only_the_keys_below_it() {
    use std::os::unix::fs::symlink;
    type Case = (
        &'static str,
        fn(&Path),
        &'static [&'static str],
        &'static str,
        bool,
        RunOutcome,
    );
    // Each case: the tree, the bucket, the name to look at, whether a link stays there, and the
    // outcome.
    let cases: &[Case] = &[
        (
            "dangling photos, bucket has photos/a.jpg",
            |root| symlink(root.join("unmounted"), root.join("photos")).unwrap(),
            &["photos/a.jpg"],
            "photos",
            true,
            RunOutcome::Warned,
        ),
        (
            "dangling photos, bucket has photos",
            |root| symlink(root.join("unmounted"), root.join("photos")).unwrap(),
            &["photos"],
            "photos",
            false,
            RunOutcome::Clean,
        ),
        (
            "dangling photos, bucket empty",
            |root| symlink(root.join("unmounted"), root.join("photos")).unwrap(),
            &[],
            "photos",
            true,
            RunOutcome::Clean,
        ),
        (
            "endless chain a -> b -> a, bucket has a",
            |root| {
                symlink(root.join("b"), root.join("a")).unwrap();
                symlink(root.join("a"), root.join("b")).unwrap();
            },
            &["a"],
            "a",
            false,
            RunOutcome::Clean,
        ),
    ];
    let mut failures = Vec::new();
    for (name, build, bucket, at, link_stays, outcome) in cases {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().canonicalize().expect("canonical root");
        build(&root);
        let (t, ctx, rx) = downloading_into_walking(
            &root,
            a_bucket_to_download(bucket, 1_600_000_000),
            DeleteMode::On,
            Walker::builder().follow_symlinks(true).build(),
        );
        run_managed(&t, &ctx, rx).await;
        let is_link = std::fs::symlink_metadata(root.join(at))
            .is_ok_and(|meta| meta.file_type().is_symlink());
        if is_link != *link_stays {
            failures.push(format!(
                "{name}: a link at {at} is {is_link}, want {link_stays}"
            ));
        }
        if t.outcome() != *outcome {
            failures.push(format!("{name}: {:?}, want {outcome:?}", t.outcome()));
        }
        if t.inner.state.lock().snapshot().walk_failures != 0 {
            failures.push(format!("{name}: the link counted as a walk failure"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// A download with following on into `root`, comparing with `mode`.
fn downloading_in_mode(
    root: &Path,
    client: aws_sdk_s3::Client,
    mode: Mode,
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
        .follow_symlinks(true)
        .build()
        .downloading(
            LocalAndBucket::builder()
                .local_root(root)
                .client(client)
                .bucket("amzn-s3-demo-bucket")
                .build(),
            delete_mode,
        )
        .expect("an ordinary bucket builds");
    let transfer = SyncTransfer::new(
        ctx.clone(),
        walk,
        mode.downloading(),
        Arc::new(SpawnDownload::new(
            ctx.handle.clone(),
            "amzn-s3-demo-bucket",
            None,
            root,
        )),
        Arc::new(DeleteFromLocalTree::new(root)),
        RunSettings {
            max_children: 4,
            delete_mode,
            failure_policy: FailedTransferPolicy::Continue,
        },
    );
    (transfer, ctx, rx)
}

// Execute the run's first work item, the merge. The merge then has decided every key of a small
// tree, and nothing has acted on them yet.
async fn decide_without_acting<S, D>(t: &SyncTransfer<S, D>)
where
    S: crate::io::key::stream::KeyStream + Send + 'static,
    D: crate::io::key::stream::KeyStream + Send + 'static,
    S::Source: Send + 'static,
    D::Source: Send + 'static,
{
    let PollWork::Ready { io: mut work, .. } = t.poll_work() else {
        panic!("the first work a run hands out is its merge");
    };
    t.execute(&mut work).await;
}

// Swap the directory `dir` for a link to `target`, as another program might during a run.
#[cfg(unix)]
fn swap_for_a_link(dir: &Path, target: &Path) {
    std::fs::remove_dir_all(dir).expect("the directory goes");
    std::os::unix::fs::symlink(target, dir).expect("a link in its place");
}

// A link that appears after the walk read its directory slips past every hold. With
// following off, the download checks the path just before it writes, and it skips the key. With
// following on, a write through a link above the file is allowed on purpose.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_checks_its_path_for_a_link_just_before_it_writes() {
    for (follow, writes_through) in [(false, false), (true, true)] {
        let outside = tempfile::tempdir().expect("outside");
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().canonicalize().expect("canonical root");
        std::fs::create_dir(root.join("d")).expect("d");
        let (t, ctx, rx) = downloading_into_walking(
            &root,
            a_bucket_to_download(&["d/a.txt"], 1_600_000_000),
            DeleteMode::Off,
            Walker::builder().follow_symlinks(follow).build(),
        );
        decide_without_acting(&t).await;
        swap_for_a_link(&root.join("d"), outside.path());
        run_managed(&t, &ctx, rx).await;
        let wrote = outside.path().join("a.txt").exists();
        assert_eq!(wrote, writes_through, "following {follow}");
        assert_eq!(t.is_plan_complete(), follow, "following {follow}");
    }
}

// A local delete checks its path for a link in either follow setting. A link that appeared
// after the walk read its directory would make the delete remove a file outside the root.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_local_delete_checks_its_path_for_a_link_just_before_it_acts() {
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("x.txt"), b"keep me").expect("x.txt outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("d")).expect("d");
    std::fs::write(root.join("d/x.txt"), b"x").expect("d/x.txt");
    let (t, ctx, rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&[], 1_600_000_000),
        DeleteMode::On,
        Walker::builder().build(),
    );
    decide_without_acting(&t).await;
    swap_for_a_link(&root.join("d"), outside.path());
    run_managed(&t, &ctx, rx).await;
    assert!(
        outside.path().join("x.txt").exists(),
        "the delete went through a link to a file outside the root"
    );
    assert!(!t.is_plan_complete());
}

// With following off, an upload checks the path just before it reads.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_upload_checks_its_path_for_a_link_just_before_it_reads() {
    let outside = tempfile::tempdir().expect("outside");
    std::fs::write(outside.path().join("a.txt"), b"not yours").expect("a.txt outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("d")).expect("d");
    std::fs::write(root.join("d/a.txt"), b"x").expect("d/a.txt");
    let (client, _deleted) = listing_and_recording_deletes(&[], 1);
    let (t, _ctx) = upload_run(&root, client, Walker::builder().build());
    decide_without_acting(&t).await;
    swap_for_a_link(&root.join("d"), outside.path());
    drive(&t).await;
    let snapshot = t.inner.state.lock().snapshot();
    assert_eq!(
        snapshot.decided.transfers, 0,
        "the upload read through the link"
    );
    assert_eq!(snapshot.decided.skipped.unread, 1);
    assert!(!t.is_plan_complete());
}

// On a disk that ignores case, the key `photos/a.jpg` reaches the link `Photos`. The
// merge compares names byte for byte, so its hold on `Photos` misses the key. The check asks the
// disk, and the disk finds the link. On a disk that respects case, the download lands in a new
// `photos/`, and nothing reaches the link either.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_never_writes_through_a_link_that_matches_only_by_case() {
    let outside = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside.path(), root.join("Photos")).expect("symlink");
    let (t, ctx, rx) = downloading_into(
        &root,
        a_bucket_to_download(&["photos/a.jpg"], 1_600_000_000),
        DeleteMode::Off,
    );
    run_managed(&t, &ctx, rx).await;
    let escaped: Vec<_> = std::fs::read_dir(outside.path()).expect("reads").collect();
    assert!(
        escaped.is_empty(),
        "a download wrote through a link it matched by case"
    );
}

// The run starts at the root the caller named, even when the root is a link. The
// follow setting covers the entries inside the root.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_upload_starts_at_a_root_that_is_a_link() {
    for follow in [false, true] {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().canonicalize().expect("canonical dir");
        std::fs::create_dir(data.join("dest")).expect("dest");
        std::fs::write(data.join("dest/a.txt"), b"x").expect("a.txt");
        std::os::unix::fs::symlink(data.join("dest"), data.join("current")).expect("symlink");
        let (client, _deleted) = listing_and_recording_deletes(&[], 1);
        let (t, _ctx) = upload_run(
            &data.join("current"),
            client,
            Walker::builder().follow_symlinks(follow).build(),
        );
        drive(&t).await;
        let snapshot = t.inner.state.lock().snapshot();
        assert_eq!(
            snapshot.walk_failures, 0,
            "following {follow}: the root failed"
        );
        assert_eq!(snapshot.decided.transfers, 1, "following {follow}");
    }
}

// A link to a directory that holds the root is a loop. Following it would send the
// files beside the root, and the root's own files a second time.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_upload_never_climbs_above_the_root_through_a_link() {
    let dir = tempfile::tempdir().expect("temp dir");
    let data = dir.path().canonicalize().expect("canonical dir");
    std::fs::create_dir_all(data.join("sibling")).expect("sibling");
    std::fs::write(data.join("sibling/s.txt"), b"x").expect("s.txt");
    std::fs::create_dir(data.join("dest")).expect("dest");
    std::fs::write(data.join("dest/a.txt"), b"x").expect("a.txt");
    std::os::unix::fs::symlink("..", data.join("dest/up")).expect("up -> ..");
    let (client, _deleted) = listing_and_recording_deletes(&[], 1);
    let (t, _ctx) = upload_run(
        &data.join("dest"),
        client,
        Walker::builder().follow_symlinks(true).build(),
    );
    drive(&t).await;
    assert_eq!(
        t.inner.state.lock().snapshot().decided.transfers,
        1,
        "the run sent files from above the root"
    );
}

// A root that is a link to nothing exists, and it is no directory. A download into it ends
// before any work, and it never reads the root as a directory nobody has created yet.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_into_a_root_that_links_to_nothing_ends_before_any_work() {
    let dir = tempfile::tempdir().expect("temp dir");
    let base = dir.path().canonicalize().expect("canonical dir");
    let root = base.join("current");
    std::os::unix::fs::symlink(base.join("gone"), &root).expect("a dangling root");
    let (t, ctx, rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&["a.txt"], 1_600_000_000),
        DeleteMode::Off,
        Walker::builder().follow_symlinks(true).build(),
    );
    run_managed(&t, &ctx, rx).await;
    assert_eq!(t.inner.state.lock().snapshot().decided.transfers, 0);
    assert_eq!(t.outcome(), RunOutcome::Failed);
}

// A followed link to a directory sits at a key's own name. The walk descends into it,
// and it still reports the name as taken. No-overwrite keeps the link, and the default modes replace
// it.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn no_overwrite_keeps_a_link_to_a_directory_at_a_keys_name() {
    for (mode, link_stays) in [(Mode::NoOverwrite, true), (Mode::SizeAndTime, false)] {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().canonicalize().expect("canonical root");
        std::fs::create_dir(root.join("real")).expect("real");
        std::os::unix::fs::symlink(root.join("real"), root.join("y.txt")).expect("symlink");
        let (t, ctx, rx) = downloading_in_mode(
            &root,
            a_bucket_to_download(&["y.txt"], 1_600_000_000),
            mode,
            DeleteMode::Off,
        );
        run_managed(&t, &ctx, rx).await;
        let is_link = std::fs::symlink_metadata(root.join("y.txt"))
            .expect("y.txt exists")
            .file_type()
            .is_symlink();
        assert_eq!(is_link, link_stays, "{mode:?}");
        assert!(
            root.join("real").is_dir(),
            "{mode:?} touched the link's target"
        );
    }
}

// The link's own name pairs only with the same source key. Here the bucket has every file through
// the link and no key `y.txt`, so delete mode leaves the link, and the run is clean.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn delete_mode_keeps_a_followed_link_to_a_directory() {
    const WHEN: i64 = 1_600_000_000;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    stamp_hello(&root.join("real/a"), WHEN);
    std::os::unix::fs::symlink(root.join("real"), root.join("y.txt")).expect("symlink");
    let (t, ctx, rx) = downloading_in_mode(
        &root,
        a_bucket_to_download(&["real/a", "y.txt/a"], WHEN),
        Mode::SizeAndTime,
        DeleteMode::On,
    );
    run_managed(&t, &ctx, rx).await;
    assert!(
        std::fs::symlink_metadata(root.join("y.txt"))
            .expect("y.txt exists")
            .file_type()
            .is_symlink(),
        "delete mode removed the link"
    );
    assert_eq!(t.outcome(), RunOutcome::Clean);
}

// On an upload, a link the walk followed into a directory has nothing to send at its own name. An
// object at that name is destination-only, as for a real directory, and delete mode
// removes it.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn an_upload_deletes_the_object_at_a_followed_directory_links_name() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("real")).expect("real");
    std::os::unix::fs::symlink(root.join("real"), root.join("y.txt")).expect("symlink");
    let (client, deleted) = listing_and_recording_deletes(&["y.txt"], 1);
    let (t, _ctx) = upload_run(
        &root,
        client,
        Walker::builder().follow_symlinks(true).build(),
    );
    drive(&t).await;
    assert_eq!(deleted.lock().clone(), vec!["y.txt".to_string()]);
}

// A loop reaches a directory, so its own name counts like a link to a directory. A download of
// that name replaces the link, and the keys below it stay held.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_replaces_a_destination_loop_at_its_own_name() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(&root, root.join("up")).expect("a loop");
    let (t, ctx, rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&["up"], 1_600_000_000),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    run_managed(&t, &ctx, rx).await;
    let meta = std::fs::symlink_metadata(root.join("up")).expect("up exists");
    assert!(meta.is_file(), "the download did not replace the loop");
}

// Two aliases with the link moved after its target. The merge decides on the target before the
// walk reads the link's directory, so only the link scan can keep the file.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_link_that_sorts_after_its_target_keeps_the_file_another_key_names() {
    use std::os::unix::fs::symlink;
    const WHEN: i64 = 1_600_000_000;
    type Case = (
        &'static str,
        &'static [&'static str],
        fn(&Path),
        &'static str,
    );
    let cases: &[Case] = &[
        (
            "z/link -> ../real, bucket keeps z/link/x.txt",
            &["z/link/x.txt"],
            |root| {
                stamp_hello(&root.join("real/x.txt"), WHEN);
                std::fs::create_dir(root.join("z")).unwrap();
                symlink("../real", root.join("z/link")).unwrap();
            },
            "real/x.txt",
        ),
        (
            "z/y.txt -> ../a.txt, bucket keeps z/y.txt",
            &["z/y.txt"],
            |root| {
                stamp_hello(&root.join("a.txt"), WHEN);
                std::fs::create_dir(root.join("z")).unwrap();
                symlink("../a.txt", root.join("z/y.txt")).unwrap();
            },
            "a.txt",
        ),
    ];
    let mut failures = Vec::new();
    for (name, bucket, build, kept) in cases {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().canonicalize().expect("canonical root");
        build(&root);
        let (t, ctx, rx) = downloading_into_walking(
            &root,
            a_bucket_to_download(bucket, WHEN),
            DeleteMode::On,
            Walker::builder().follow_symlinks(true).build(),
        );
        run_managed(&t, &ctx, rx).await;
        if std::fs::metadata(root.join(kept)).is_err() {
            failures.push(format!("{name}: {kept} no longer resolves"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// Without the link scan, the runs switched between deleting `real/x.txt` and writing it back
// through `z/link`. With it, every run decides the same thing.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn every_run_over_a_link_that_sorts_after_its_target_decides_the_same() {
    const WHEN: i64 = 1_600_000_000;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    stamp_hello(&root.join("real/x.txt"), WHEN);
    std::fs::create_dir(root.join("z")).expect("z");
    std::os::unix::fs::symlink("../real", root.join("z/link")).expect("symlink");
    for run in 1..=3 {
        let (t, ctx, rx) = downloading_into_walking(
            &root,
            a_bucket_to_download(&["z/link/x.txt"], WHEN),
            DeleteMode::On,
            Walker::builder().follow_symlinks(true).build(),
        );
        run_managed(&t, &ctx, rx).await;
        assert!(
            root.join("real/x.txt").exists(),
            "run {run} removed the file z/link/x.txt names"
        );
        assert_eq!(
            t.outcome(),
            RunOutcome::Warned,
            "run {run} kept a stale key and reported no warning"
        );
    }
}

// Run every work item but the link scan, until the run has nothing else to do. Return the scan's
// work item, so a test can let the merge decide deletes before the scan ends.
async fn drive_holding_the_scan(
    t: &SyncTransfer<S3Walk, crate::operation::sync::walk::LocalDestination>,
) -> IoRequest {
    let mut scan = None;
    loop {
        match t.poll_work() {
            PollWork::Ready { io: mut work, .. } => {
                let is_scan = matches!(
                    work.data_mut::<SyncWork<S3Walk, crate::operation::sync::walk::LocalDestination>>(),
                    SyncWork::ScanLinks { .. }
                );
                if is_scan && scan.is_none() {
                    scan = Some(work);
                } else {
                    t.execute(&mut work).await;
                }
            }
            PollWork::Pending => break,
            PollWork::Done => panic!("the run finished while its link scan never ran"),
            PollWork::Spawned => {}
        }
    }
    scan.expect("the run dispatched its link scan")
}

// Case F with the scan held back. The merge decides to delete `real/x.txt` before the scan reads
// `z/`. The delete waits, and once the scan names `real`, it stays.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_delete_the_merge_decides_before_the_scan_ends_waits_for_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("real")).expect("real");
    std::fs::write(root.join("real/x.txt"), b"x").expect("x.txt");
    std::fs::create_dir(root.join("z")).expect("z");
    std::os::unix::fs::symlink("../real", root.join("z/link")).expect("symlink");
    let (t, _ctx, _rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&[], 1_600_000_000),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    let mut scan = drive_holding_the_scan(&t).await;
    assert!(
        root.join("real/x.txt").exists(),
        "a delete went out before the scan ended"
    );
    t.execute(&mut scan).await;
    drive(&t).await;
    assert!(
        root.join("real/x.txt").exists(),
        "the run removed the file z/link/x.txt names"
    );
    assert_eq!(t.outcome(), RunOutcome::Warned);
}

// Local deletes wait for the link scan. While the scan has not run, the merge decides deletes until
// 1,024 wait, then pauses. Once the scan ends, every delete goes out.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_merge_pauses_on_deletes_that_wait_for_the_link_scan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    for n in 0..1100 {
        std::fs::write(root.join(format!("f{n:04}")), b"x").expect("a local-only file");
    }
    let (t, _ctx, _rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&[], 1_600_000_000),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    let mut scan = drive_holding_the_scan(&t).await;
    let waiting = t.inner.state.lock().snapshot();
    assert!(
        (1024..1024 + MERGE_BATCH).contains(&waiting.deletes_waiting),
        "the merge did not pause at 1,024 waiting deletes: {}",
        waiting.deletes_waiting
    );
    assert_eq!(
        waiting.removed, 0,
        "a delete went out before the scan ended"
    );

    t.execute(&mut scan).await;
    drive(&t).await;
    let left = std::fs::read_dir(&root).expect("root reads").count();
    assert_eq!(left, 0, "{left} local-only files survived the run");
    assert!(t.is_plan_complete());
}

// A link someone creates after the scan read its directory slips past the scan. The walk still
// finds it, and the run reports that its plan may have removed a file the link names. Every key
// compares unchanged, so the late link is the only reason the plan is not whole.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_link_created_after_the_scan_read_its_directory_marks_the_plan_incomplete() {
    const WHEN: i64 = 1_600_000_000;
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    stamp_hello(&root.join("real/x.txt"), WHEN);
    std::fs::create_dir(root.join("z")).expect("z");
    let (t, _ctx, _rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&["real/x.txt", "z/link/x.txt"], WHEN),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    while !t
        .inner
        .state
        .lock()
        .link_scan
        .as_ref()
        .is_some_and(|scan| scan.is_done())
    {
        let PollWork::Ready { io: mut work, .. } = t.poll_work() else {
            panic!("the first work a run hands out is its link scan");
        };
        t.execute(&mut work).await;
    }
    std::os::unix::fs::symlink("../real", root.join("z/link")).expect("a link made mid-run");
    drive(&t).await;
    assert_eq!(
        t.inner.state.lock().snapshot().decided.transfers,
        0,
        "every key should compare unchanged"
    );
    assert!(
        !t.is_plan_complete(),
        "a run that followed a link its scan never saw reported a whole plan"
    );
}

// More link names than the bound. The run cannot tell which deletes are safe, so it keeps every one
// of them. The walk has a bound of its own for the links it follows while the scan reads. Either
// bound keeps the file here. `the_link_scan_gives_up_past_its_bound` tests the scan's bound alone.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_tree_past_the_link_bound_keeps_every_local_delete() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::fs::create_dir(root.join("real")).expect("real");
    // Each directory link names itself and its target. 1,100 links to one target name 1,101 keys,
    // past the bound of 1,024.
    for n in 0..1100 {
        std::os::unix::fs::symlink(root.join("real"), root.join(format!("l{n:04}")))
            .expect("a link");
    }
    std::fs::write(root.join("old.txt"), b"x").expect("a local-only file");
    let (t, _ctx, _rx) = downloading_into_walking(
        &root,
        a_bucket_to_download(&[], 1_600_000_000),
        DeleteMode::On,
        Walker::builder().follow_symlinks(true).build(),
    );
    drive(&t).await;
    assert!(
        root.join("old.txt").exists(),
        "a run past the link bound removed a local file"
    );
    assert_eq!(t.outcome(), RunOutcome::Warned);
    assert!(
        t.inner.state.lock().snapshot().past_link_bound,
        "the run kept every delete and did not record why"
    );
}

// The names a followed link reaches exist only to stop deletes. With delete mode off, the run names
// none, so the destination-only key `link/x.txt` stays destination-only and the plan is whole. With
// delete mode on, the same key reads as unknown.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_names_link_targets_only_with_delete_mode_on() {
    for (delete_mode, whole) in [(DeleteMode::Off, true), (DeleteMode::On, false)] {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().canonicalize().expect("canonical root");
        std::fs::create_dir(root.join("real")).expect("real");
        std::fs::write(root.join("real/x.txt"), b"hello").expect("x.txt");
        std::os::unix::fs::symlink(root.join("real"), root.join("link")).expect("symlink");
        let (t, ctx, rx) = downloading_into_walking(
            &root,
            a_bucket_to_download(&["real/x.txt"], 1_600_000_000),
            delete_mode,
            Walker::builder().follow_symlinks(true).build(),
        );
        run_managed(&t, &ctx, rx).await;
        assert_eq!(
            t.is_plan_complete(),
            whole,
            "delete mode {delete_mode:?}: the plan's completeness was wrong"
        );
        assert!(
            root.join("real/x.txt").exists(),
            "delete mode {delete_mode:?}: the run removed the file a kept key names"
        );
    }
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_destination_alias_keeps_the_file_another_key_names() {
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    const WHEN: i64 = 1_600_000_000;

    // Write the bucket's bytes at the object's time, so a paired key compares unchanged.
    fn stamped(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("parent dir");
        }
        std::fs::write(path, b"hello").expect("file");
        std::fs::File::options()
            .write(true)
            .open(path)
            .expect("file opens")
            .set_times(
                std::fs::FileTimes::new().set_modified(
                    std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(WHEN as u64),
                ),
            )
            .expect("the object's time applies");
    }

    struct Tree {
        root: PathBuf,
        kept: Vec<PathBuf>,
        removed: Vec<PathBuf>,
    }

    // Spell the root through a link, so walked paths and canonical paths differ.
    fn spelled_root(scratch: &Path) -> PathBuf {
        std::fs::create_dir_all(scratch.join("actual/dest")).expect("actual dest");
        symlink(scratch.join("actual"), scratch.join("via")).expect("via");
        scratch.join("via/dest")
    }

    fn kept(root: PathBuf, kept: &[&str]) -> Tree {
        Tree {
            kept: kept.iter().map(|k| root.join(k)).collect(),
            removed: Vec::new(),
            root,
        }
    }

    type Case = (&'static str, &'static [&'static str], fn(&Path) -> Tree);
    let cases: &[Case] = &[
        // The target's keys pair before the walk reaches `zlink`, so only the read of the root can
        // hold the target. The link names its target by canonical path, and the walk spells the
        // root through `via`. Only the canonical root maps that target to a key.
        (
            "zlink -> real, bucket keeps zlink/x.txt",
            &["zlink/x.txt"],
            |s: &Path| {
                let root = spelled_root(s);
                stamped(&root.join("real/x.txt"));
                symlink(s.join("actual/dest/real"), root.join("zlink")).expect("zlink");
                kept(root, &["real/x.txt"])
            },
        ),
        // The link's own name maps under the root as spelled. The walk reports the dangling link
        // ahead of `link/x.txt`, in the same call that records `link`.
        (
            "link -> real beside a dangling link, bucket keeps real/x.txt",
            &["real/x.txt"],
            |s: &Path| {
                let root = spelled_root(s);
                stamped(&root.join("real/x.txt"));
                symlink("real", root.join("link")).expect("link");
                symlink("missing", root.join("broken")).expect("broken");
                kept(root, &["real/x.txt"])
            },
        ),
        (
            "y.txt -> z.txt -> x.txt, bucket keeps y.txt",
            &["y.txt"],
            |s: &Path| {
                let root = s.join("dest");
                stamped(&root.join("x.txt"));
                symlink("x.txt", root.join("z.txt")).expect("z.txt");
                symlink("z.txt", root.join("y.txt")).expect("y.txt");
                kept(root, &["y.txt"])
            },
        ),
        // An outside target maps to no name, so a delete elsewhere in the tree still goes out.
        (
            "two links to one outside directory, bucket keeps l1/x.txt",
            &["l1/x.txt"],
            |s: &Path| {
                stamped(&s.join("outside/d/x.txt"));
                let root = s.join("dest");
                stamped(&root.join("stale.txt"));
                symlink(s.join("outside/d"), root.join("l1")).expect("l1");
                symlink(s.join("outside/d"), root.join("l2")).expect("l2");
                Tree {
                    kept: vec![s.join("outside/d/x.txt")],
                    removed: vec![root.join("stale.txt")],
                    root,
                }
            },
        ),
        // A file link's own name stays deletable: removing it unlinks the link and keeps `x.txt`.
        (
            "y.txt -> x.txt, bucket keeps x.txt",
            &["x.txt"],
            |s: &Path| {
                let root = s.join("dest");
                stamped(&root.join("x.txt"));
                symlink("x.txt", root.join("y.txt")).expect("y.txt");
                Tree {
                    kept: vec![root.join("x.txt")],
                    removed: vec![root.join("y.txt")],
                    root,
                }
            },
        ),
    ];

    let mut failures = Vec::new();
    for (name, bucket, build) in cases {
        let scratch = tempfile::tempdir().expect("temp dir");
        let base = scratch.path().canonicalize().expect("canonical scratch");
        let tree = build(&base);
        let (t, ctx, rx) = downloading_into_walking(
            &tree.root,
            a_bucket_to_download(bucket, WHEN),
            DeleteMode::On,
            Walker::builder().follow_symlinks(true).build(),
        );
        run_managed(&t, &ctx, rx).await;
        for path in &tree.kept {
            if std::fs::metadata(path).is_err() {
                failures.push(format!("{name}: {} no longer resolves", path.display()));
            }
        }
        for path in &tree.removed {
            if std::fs::symlink_metadata(path).is_ok() {
                failures.push(format!("{name}: {} was not removed", path.display()));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// A run that downloads `keys` objects under the destination link `photos`. The link points outside
// the root. `refuse` makes the tree refuse the link's removal. The run returns the names that landed
// outside, whether the plan came out whole, and whether the link still stands.
#[cfg(unix)]
async fn through_a_destination_link(
    delete_mode: DeleteMode,
    keys: usize,
    refuse: bool,
) -> (Vec<String>, bool, bool) {
    let outside = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside.path(), root.join("photos")).expect("symlink");
    let names: Vec<String> = (0..keys).map(|n| format!("photos/k{n:03}.jpg")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let deleter: Arc<dyn DeleteKeys> = if refuse {
        Arc::new(RecordDeletes::refusing_in_a_tree(1))
    } else {
        Arc::new(DeleteFromLocalTree::new(&root))
    };
    let (t, ctx, rx) = downloading_into_with(
        &root,
        a_bucket_to_download(&refs, 1_600_000_000),
        delete_mode,
        deleter,
        Walker::builder().build(),
    );
    run_managed(&t, &ctx, rx).await;
    let escaped = std::fs::read_dir(outside.path())
        .expect("outside reads")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    let link_kept = std::fs::symlink_metadata(root.join("photos"))
        .is_ok_and(|meta| meta.file_type().is_symlink());
    (escaped, t.is_plan_complete(), link_kept)
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_writes_nothing_outside_the_root_through_a_link_it_deletes() {
    let (escaped, _, link_kept) = through_a_destination_link(DeleteMode::On, 1, false).await;
    assert!(
        escaped.is_empty(),
        "delete mode on: wrote outside the root: {escaped:?}"
    );
    assert!(!link_kept, "delete mode on kept a destination-only link");
}

// 300 keys are more than one merge batch, so the merge decides some of them after the run kept the
// link. None of them may write through it.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn no_download_writes_through_a_link_the_run_keeps() {
    let (escaped, _, link_kept) = through_a_destination_link(DeleteMode::Off, 300, false).await;
    assert!(
        escaped.is_empty(),
        "downloads wrote through a link the run kept: {} of them",
        escaped.len()
    );
    assert!(link_kept, "delete mode off removed the link");
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn no_download_writes_through_a_link_the_run_could_not_remove() {
    let (escaped, complete, _) = through_a_destination_link(DeleteMode::On, 300, true).await;
    assert!(
        escaped.is_empty(),
        "downloads wrote through a link the run could not remove: {} of them",
        escaped.len()
    );
    assert!(
        !complete,
        "a run that held back keys under a link reported a whole plan"
    );
}
