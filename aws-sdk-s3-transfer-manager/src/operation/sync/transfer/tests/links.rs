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

#[cfg(unix)]
async fn through_a_destination_link(delete_mode: DeleteMode) -> Vec<String> {
    let outside = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside.path(), root.join("photos")).expect("symlink");
    let (client, _gets) = a_bucket_recording_gets(&["photos/a.jpg"], 1_600_000_000);
    let (t, ctx, rx) = downloading_into(&root, client, delete_mode);
    run_managed(&t, &ctx, rx).await;
    std::fs::read_dir(outside.path())
        .expect("outside reads")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect()
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_writes_nothing_outside_the_root_through_a_link_it_deletes() {
    let escaped = through_a_destination_link(DeleteMode::On).await;
    assert!(
        escaped.is_empty(),
        "delete mode on: wrote outside the root: {escaped:?}"
    );
}

#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_download_writes_nothing_through_a_destination_link_it_keeps() {
    let escaped = through_a_destination_link(DeleteMode::Off).await;
    assert!(
        escaped.is_empty(),
        "delete mode off: wrote outside the root: {escaped:?}"
    );
}

// More keys than one merge batch, so the merge decides some of them after the tree refused to remove
// the link. None of them may write through it.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn no_download_writes_through_a_link_the_run_could_not_remove() {
    let outside = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside.path(), root.join("photos")).expect("symlink");
    let names: Vec<String> = (0..300).map(|n| format!("photos/k{n:03}.jpg")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let (t, ctx, rx) = downloading_into_with(
        &root,
        a_bucket_to_download(&refs, 1_600_000_000),
        DeleteMode::On,
        Arc::new(RecordDeletes::refusing_in_a_tree(1)),
        Walker::builder().build(),
    );
    run_managed(&t, &ctx, rx).await;

    let written_outside = std::fs::read_dir(outside.path())
        .expect("outside reads")
        .count();
    assert_eq!(
        written_outside, 0,
        "downloads wrote through a link the run could not remove"
    );
    assert!(
        !t.is_plan_complete(),
        "a run that held back keys under a link reported a whole plan"
    );
}

// The same 300 keys with delete mode off. The run keeps the link, so every key stays held.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn no_download_writes_through_a_link_the_run_keeps() {
    let outside = tempfile::tempdir().expect("outside");
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().canonicalize().expect("canonical root");
    std::os::unix::fs::symlink(outside.path(), root.join("photos")).expect("symlink");
    let names: Vec<String> = (0..300).map(|n| format!("photos/k{n:03}.jpg")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let (t, ctx, rx) = downloading_into(
        &root,
        a_bucket_to_download(&refs, 1_600_000_000),
        DeleteMode::Off,
    );
    run_managed(&t, &ctx, rx).await;

    let written_outside = std::fs::read_dir(outside.path())
        .expect("outside reads")
        .count();
    assert_eq!(
        written_outside, 0,
        "downloads wrote through a link the run kept"
    );
    assert!(
        std::fs::symlink_metadata(root.join("photos"))
            .expect("photos exists")
            .file_type()
            .is_symlink(),
        "delete mode off removed the link"
    );
}
