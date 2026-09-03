/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! THROWAWAY SPIKE — not a design, not a review target.
//!
//! Proves the spine end to end for one direction: local files up to an S3 prefix, no
//! deletes, one transfer at a time. Everything that makes sync hard is missing on
//! purpose — concurrency, delete authority, failure policy, the download direction,
//! cancellation, per-key reporting.
//!
//! The most informative assertion available here is that a second run transfers
//! nothing. Uploading is the only direction where that holds without extra machinery:
//! once an object is written, its last-modified is newer than the local file's, so the
//! "destination is newer, leave it" rule makes the re-run a no-op. The download
//! direction cannot say the same until local timestamps are written back.

pub(crate) mod transfer;

use std::cmp::Ordering;
use std::path::Path;

use crate::io::key_stream::{Entry, EntryMeta, KeyStream};
use crate::io::walk::{FsWalkContext, FsWalker, S3WalkContext, S3Walker, WalkError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    // Only on the source, or on both and different.
    Transfer,
    // Only at the destination. Counted but not executed here.
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Decision {
    pub(crate) key: String,
    pub(crate) action: Action,
}

// Transfer when the sizes differ, or when the source is newer. Comparing whole seconds
// is what keeps a matching pair matching from one run to the next.
fn differs(src: &EntryMeta, dst: &EntryMeta) -> bool {
    src.size != dst.size || src.last_modified_secs > dst.last_modified_secs
}

/// Walks both sides once in key order, yielding each decision as it is reached.
///
/// Holds one entry per side and nothing else, so neither listing is materialized and
/// memory does not grow with the number of keys. Draining this and printing is a dry
/// run; draining it and acting is a sync.
pub(crate) struct Compare<S: KeyStream, D: KeyStream> {
    src: S,
    dst: D,
    left: Option<Entry<S::Source>>,
    right: Option<Entry<D::Source>>,
    primed: bool,
    done: bool,
}

impl<S: KeyStream, D: KeyStream> Compare<S, D> {
    pub(crate) fn new(src: S, dst: D) -> Self {
        Self {
            src,
            dst,
            left: None,
            right: None,
            primed: false,
            done: false,
        }
    }

    async fn pull_left(&mut self) -> Option<WalkError> {
        match self.src.next_entry().await {
            Some(Ok(e)) => {
                self.left = Some(e);
                None
            }
            Some(Err(e)) => Some(e),
            None => {
                self.left = None;
                None
            }
        }
    }

    async fn pull_right(&mut self) -> Option<WalkError> {
        match self.dst.next_entry().await {
            Some(Ok(e)) => {
                self.right = Some(e);
                None
            }
            Some(Err(e)) => Some(e),
            None => {
                self.right = None;
                None
            }
        }
    }

    // Advance both sides, reporting the first failure.
    async fn pull_both(&mut self) -> Option<WalkError> {
        let left = self.pull_left().await;
        let right = self.pull_right().await;
        left.or(right)
    }

    pub(crate) async fn next_decision(&mut self) -> Option<Result<Decision, WalkError>> {
        if self.done {
            return None;
        }
        if !self.primed {
            self.primed = true;
            if let Some(err) = self.pull_both().await {
                self.done = true;
                return Some(Err(err));
            }
        }

        loop {
            let ord = match (self.left.as_ref(), self.right.as_ref()) {
                (None, None) => {
                    self.done = true;
                    return None;
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(l), Some(r)) => l.key.cmp(&r.key),
            };

            match ord {
                // Source only.
                Ordering::Less => {
                    let key = self.left.take().expect("checked above").key;
                    if let Some(err) = self.pull_left().await {
                        self.done = true;
                        return Some(Err(err));
                    }
                    return Some(Ok(Decision {
                        key,
                        action: Action::Transfer,
                    }));
                }
                // Destination only.
                Ordering::Greater => {
                    let key = self.right.take().expect("checked above").key;
                    if let Some(err) = self.pull_right().await {
                        self.done = true;
                        return Some(Err(err));
                    }
                    return Some(Ok(Decision {
                        key,
                        action: Action::Delete,
                    }));
                }
                // On both sides: advance both, and only report if they differ.
                Ordering::Equal => {
                    let l = self.left.take().expect("checked above");
                    let r = self.right.take().expect("checked above");
                    let changed = differs(&l.meta, &r.meta);
                    if let Some(err) = self.pull_both().await {
                        self.done = true;
                        return Some(Err(err));
                    }
                    if changed {
                        return Some(Ok(Decision {
                            key: l.key,
                            action: Action::Transfer,
                        }));
                    }
                }
            }
        }
    }
}

/// Upload each local file the comparison asks for, as it asks for it. Returns the
/// number sent and the number of destination-only keys left alone.
pub(crate) async fn sync_up(
    tm: &crate::Client,
    s3: &aws_sdk_s3::Client,
    root: &Path,
    bucket: &str,
    prefix: &str,
) -> Result<(usize, usize), crate::error::Error> {
    let local = FsWalker::builder()
        .recursive(true)
        .key_order(true)
        .build()
        .walk(FsWalkContext::builder().root(root).build());

    // `builder().build()` applies no filter, unlike `S3Walker::default()`, so the
    // folder-marker exclusion has to be named here.
    let remote = S3Walker::builder()
        .prefix(prefix)
        .filter(crate::io::walk::exclude_s3_folder_markers)
        .build()
        .walk(
            S3WalkContext::builder()
                .client(s3.clone())
                .bucket(bucket)
                .build(),
        );

    let mut compare = Compare::new(local, remote);
    let mut sent = 0;
    let mut destination_only = 0;

    while let Some(decision) = compare.next_decision().await {
        let decision = decision?;
        match decision.action {
            Action::Delete => destination_only += 1,
            Action::Transfer => {
                let path = root.join(&decision.key);
                let key = if prefix.is_empty() {
                    decision.key.clone()
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), decision.key)
                };
                let stream = crate::io::InputStream::from_path(&path)?;
                tm.upload()
                    .bucket(bucket)
                    .key(key)
                    .body(stream)
                    .initiate()?
                    .join()
                    .await?;
                sent += 1;
            }
        }
    }
    Ok((sent, destination_only))
}

/// What a run did, and how much of it happened at once.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Summary {
    pub(crate) sent: usize,
    pub(crate) destination_only: usize,
    // The most transfers ever in flight together. Above one, the comparison kept
    // advancing while bytes were moving; at one, everything took turns.
    pub(crate) peak_in_flight: usize,
}

/// Keep up to `max_in_flight` transfers moving while the comparison continues.
///
/// The difference from `sync_up` is only where the waiting happens. `initiate` starts a
/// transfer, `join` waits for it, and doing both on the spot means nothing is read or
/// listed while bytes are on the wire. Here a started transfer is parked, the loop goes
/// back for the next decision, and a slot is only waited for when all of them are busy.
pub(crate) async fn sync_up_concurrent(
    tm: &crate::Client,
    s3: &aws_sdk_s3::Client,
    root: &Path,
    bucket: &str,
    prefix: &str,
    max_in_flight: usize,
) -> Result<Summary, crate::error::Error> {
    use futures_util::stream::{FuturesUnordered, StreamExt};

    let local = FsWalker::builder()
        .recursive(true)
        .key_order(true)
        .build()
        .walk(FsWalkContext::builder().root(root).build());
    let remote = S3Walker::builder()
        .prefix(prefix)
        .filter(crate::io::walk::exclude_s3_folder_markers)
        .build()
        .walk(
            S3WalkContext::builder()
                .client(s3.clone())
                .bucket(bucket)
                .build(),
        );

    let mut compare = Compare::new(local, remote);
    let mut in_flight = FuturesUnordered::new();
    let mut summary = Summary::default();

    while let Some(decision) = compare.next_decision().await {
        let decision = decision?;
        match decision.action {
            Action::Delete => summary.destination_only += 1,
            Action::Transfer => {
                let path = root.join(&decision.key);
                let key = if prefix.is_empty() {
                    decision.key.clone()
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), decision.key)
                };
                let stream = crate::io::InputStream::from_path(&path)?;
                let handle = tm
                    .upload()
                    .bucket(bucket)
                    .key(key)
                    .body(stream)
                    .initiate()?;
                in_flight.push(handle.join());
                summary.peak_in_flight = summary.peak_in_flight.max(in_flight.len());

                // Only wait once every slot is taken, so the comparison runs ahead of
                // the wire rather than behind it.
                if in_flight.len() >= max_in_flight {
                    if let Some(result) = in_flight.next().await {
                        result?;
                        summary.sent += 1;
                    }
                }
            }
        }
    }

    while let Some(result) = in_flight.next().await {
        result?;
        summary.sent += 1;
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A stream over entries already in memory, so comparison tests need no filesystem
    // and no mock client.
    struct Fixed {
        entries: std::collections::VecDeque<Entry<()>>,
    }

    impl Fixed {
        fn new(entries: impl IntoIterator<Item = (&'static str, u64, i64)>) -> Self {
            Self {
                entries: entries
                    .into_iter()
                    .map(|(key, size, last_modified_secs)| Entry {
                        key: key.to_string(),
                        meta: EntryMeta {
                            size,
                            last_modified_secs,
                        },
                        source: (),
                    })
                    .collect(),
            }
        }
    }

    impl KeyStream for Fixed {
        type Source = ();

        async fn next_entry(&mut self) -> Option<Result<Entry<()>, WalkError>> {
            self.entries.pop_front().map(Ok)
        }
    }

    // Collecting in a test is bounded by the fixture; the comparison itself holds two
    // entries however long the sides are.
    async fn decisions(src: Fixed, dst: Fixed) -> Vec<Decision> {
        let mut compare = Compare::new(src, dst);
        let mut out = Vec::new();
        while let Some(d) = compare.next_decision().await {
            out.push(d.expect("comparison failed"));
        }
        out
    }

    fn of(decisions: &[Decision], action: Action) -> Vec<&str> {
        decisions
            .iter()
            .filter(|d| d.action == action)
            .map(|d| d.key.as_str())
            .collect()
    }

    #[tokio::test]
    async fn identical_sides_decide_nothing() {
        let d = decisions(
            Fixed::new([("a.txt", 1, 100), ("b/c.txt", 2, 100)]),
            Fixed::new([("a.txt", 1, 100), ("b/c.txt", 2, 100)]),
        )
        .await;
        assert!(d.is_empty(), "{d:?}");
    }

    #[tokio::test]
    async fn new_changed_and_destination_only_keys() {
        let d = decisions(
            Fixed::new([
                ("new.txt", 1, 100),
                ("same.txt", 1, 100),
                ("size.txt", 9, 100),
                ("time.txt", 1, 200),
            ]),
            Fixed::new([
                ("gone.txt", 1, 100),
                ("same.txt", 1, 100),
                ("size.txt", 1, 100),
                ("time.txt", 1, 100),
            ]),
        )
        .await;

        assert_eq!(
            of(&d, Action::Transfer),
            vec!["new.txt", "size.txt", "time.txt"]
        );
        assert_eq!(of(&d, Action::Delete), vec!["gone.txt"]);
    }

    // The state right after an upload. It must not cause another one.
    #[tokio::test]
    async fn a_newer_destination_is_left_alone() {
        let d = decisions(
            Fixed::new([("a.txt", 1, 100)]),
            Fixed::new([("a.txt", 1, 500)]),
        )
        .await;
        assert!(d.is_empty(), "{d:?}");
    }

    // `a.txt` sorts before `a/c`, and identical sides must pair up regardless.
    #[tokio::test]
    async fn keys_around_a_folder_boundary_pair_up() {
        let d = decisions(
            Fixed::new([("a.txt", 1, 100), ("a/c", 1, 100)]),
            Fixed::new([("a.txt", 1, 100), ("a/c", 1, 100)]),
        )
        .await;
        assert!(d.is_empty(), "{d:?}");
    }

    // Overlap is a structural property, so assert it structurally rather than by
    // timing: with several files to send and room for four at once, more than one must
    // have been in flight together.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn transfers_overlap_the_comparison() {
        let dir = tempdir().unwrap();
        for i in 0..8 {
            std::fs::write(dir.path().join(format!("f{i}.txt")), "x").unwrap();
        }

        let list = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().build());
        let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, put]);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let summary = sync_up_concurrent(&tm, &s3, dir.path(), "bucket", "data", 4)
            .await
            .unwrap();

        assert_eq!(summary.sent, 8);
        assert!(
            summary.peak_in_flight > 1,
            "expected overlapping transfers, peaked at {}",
            summary.peak_in_flight
        );
    }

    // One at a time is the old behavior, and it must still be expressible.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn a_limit_of_one_never_overlaps() {
        let dir = tempdir().unwrap();
        for i in 0..4 {
            std::fs::write(dir.path().join(format!("f{i}.txt")), "x").unwrap();
        }

        let list = mock!(aws_sdk_s3::Client::list_objects_v2)
            .then_output(|| ListObjectsV2Output::builder().build());
        let put = mock!(aws_sdk_s3::Client::put_object)
            .then_output(|| PutObjectOutput::builder().build());
        let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, put]);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let summary = sync_up_concurrent(&tm, &s3, dir.path(), "bucket", "data", 1)
            .await
            .unwrap();

        assert_eq!(summary.sent, 4);
        assert_eq!(summary.peak_in_flight, 1);
    }

    // --- the wiring, against a mocked service ---
    //
    // The comparison tests above never touch a key's prefix or its path on disk, and
    // that mapping is where a silent bug would live: derive the key one way for
    // comparison and another for the upload, and every run re-sends everything while
    // looking like it worked.

    use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::types::Object;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use aws_smithy_types::DateTime;
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    fn object(key: &str, size: i64, secs: i64) -> Object {
        Object::builder()
            .key(key)
            .size(size)
            .last_modified(DateTime::from_secs(secs))
            .build()
    }

    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn uploads_the_keys_the_comparison_asks_for() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("inner")).unwrap();
        std::fs::write(dir.path().join("inner/new.txt"), "xx").unwrap();
        std::fs::write(dir.path().join("same.txt"), "yyy").unwrap();

        // `same.txt` already there, matching in size and newer, so it is left alone.
        let local_secs = std::fs::metadata(dir.path().join("same.txt"))
            .unwrap()
            .modified()
            .map(|t| t.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64)
            .unwrap();
        let list = mock!(aws_sdk_s3::Client::list_objects_v2).then_output(move || {
            ListObjectsV2Output::builder()
                .contents(object("data/same.txt", 3, local_secs + 10))
                .build()
        });

        let uploaded: Arc<Mutex<Vec<String>>> = Arc::default();
        let seen = uploaded.clone();
        let put = mock!(aws_sdk_s3::Client::put_object)
            .match_requests(move |req| {
                seen.lock().unwrap().push(req.key().unwrap().to_string());
                true
            })
            .then_output(|| PutObjectOutput::builder().build());

        let s3 = mock_client!(aws_sdk_s3, RuleMode::MatchAny, &[list, put]);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let (sent, destination_only) = sync_up(&tm, &s3, dir.path(), "bucket", "data")
            .await
            .unwrap();

        assert_eq!(sent, 1, "only the new file should be sent");
        assert_eq!(destination_only, 0);
        // The prefix is joined back on, and the key names the file's real location.
        assert_eq!(*uploaded.lock().unwrap(), vec!["data/inner/new.txt"]);
    }

    #[tokio::test]
    async fn one_empty_side_decides_every_key() {
        let d = decisions(Fixed::new([("a", 1, 100), ("b", 1, 100)]), Fixed::new([])).await;
        assert_eq!(of(&d, Action::Transfer), vec!["a", "b"]);

        let d = decisions(Fixed::new([]), Fixed::new([("a", 1, 100), ("b", 1, 100)])).await;
        assert_eq!(of(&d, Action::Delete), vec!["a", "b"]);
    }
}

// Against a real bucket. Run with:
//
//   S3_TEST_BUCKET_NAME_RS=<bucket> AWS_PROFILE=<profile> AWS_REGION=us-west-2 \
//     RUSTFLAGS="--cfg e2e_test" cargo test -p aws-sdk-s3-transfer-manager \
//     --lib sync_spike::real -- --nocapture
//
// Writes under a unique prefix per run and deletes nothing.
#[cfg(all(test, e2e_test))]
mod real {
    use super::*;

    // The scheduler-driven version, against the real service. Same assertions as the
    // inline one, so a difference in outcome means the work-item shape changed behaviour.
    #[tokio::test]
    async fn scheduled_sync_matches_the_inline_one() {
        let bucket = std::env::var("S3_TEST_BUCKET_NAME_RS")
            .expect("set S3_TEST_BUCKET_NAME_RS to a bucket you can write to");
        let sdk_config = aws_config::from_env().load().await;
        let s3 = aws_sdk_s3::Client::new(&sdk_config);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("a")).unwrap();
        std::fs::write(dir.path().join("a.txt"), "one").unwrap();
        std::fs::write(dir.path().join("a/inner.txt"), "two").unwrap();
        std::fs::write(dir.path().join("with space.txt"), "three").unwrap();
        std::fs::write(dir.path().join("empty.txt"), "").unwrap();

        let prefix = format!(
            "sync-spike/{}-scheduled",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let started = std::time::Instant::now();
        let (sent, extra, peak) =
            super::transfer::sync_up_scheduled(&tm, dir.path(), &bucket, &prefix, 32)
                .await
                .expect("first run");
        println!("scheduled first run: sent {sent}, destination-only {extra}, peak children {peak} in {:?}", started.elapsed());
        assert_eq!(sent, 4);
        assert_eq!(extra, 0);

        let (sent_again, extra_again, _) =
            super::transfer::sync_up_scheduled(&tm, dir.path(), &bucket, &prefix, 32)
                .await
                .expect("second run");
        println!("scheduled second run: sent {sent_again}, destination-only {extra_again}");
        assert_eq!(sent_again, 0, "a re-run must transfer nothing");
        assert_eq!(extra_again, 0);
    }

    // Many small files are the case where taking turns hurts: each one costs a round
    // trip, and a single large file would hide it behind multipart concurrency.
    #[tokio::test]
    async fn overlapping_transfers_beat_taking_turns() {
        let bucket = std::env::var("S3_TEST_BUCKET_NAME_RS")
            .expect("set S3_TEST_BUCKET_NAME_RS to a bucket you can write to");
        let count: usize = std::env::var("SYNC_SPIKE_FILES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);

        let sdk_config = aws_config::from_env().load().await;
        let s3 = aws_sdk_s3::Client::new(&sdk_config);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let dir = tempfile::tempdir().unwrap();
        for i in 0..count {
            std::fs::write(dir.path().join(format!("f{i:04}.txt")), b"small").unwrap();
        }

        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let one_at_a_time = format!("sync-spike/{stamp}-serial");
        let started = std::time::Instant::now();
        let serial = sync_up_concurrent(&tm, &s3, dir.path(), &bucket, &one_at_a_time, 1)
            .await
            .expect("serial run");
        let serial_time = started.elapsed();

        let overlapping = format!("sync-spike/{stamp}-concurrent");
        let started = std::time::Instant::now();
        let concurrent = sync_up_concurrent(&tm, &s3, dir.path(), &bucket, &overlapping, 64)
            .await
            .expect("concurrent run");
        let concurrent_time = started.elapsed();

        println!(
            "{count} files\n  one at a time: {serial_time:?} (peak in flight {})\n  overlapping:   {concurrent_time:?} (peak in flight {})",
            serial.peak_in_flight, concurrent.peak_in_flight
        );

        assert_eq!(serial.sent, count);
        assert_eq!(concurrent.sent, count);
        assert_eq!(serial.peak_in_flight, 1);
        assert!(concurrent.peak_in_flight > 1);
        assert!(
            concurrent_time < serial_time,
            "overlapping transfers should finish sooner: {concurrent_time:?} vs {serial_time:?}"
        );
    }

    #[tokio::test]
    async fn a_second_run_transfers_nothing() {
        let bucket = std::env::var("S3_TEST_BUCKET_NAME_RS")
            .expect("set S3_TEST_BUCKET_NAME_RS to a bucket you can write to");
        let prefix = format!(
            "sync-spike/{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );

        let sdk_config = aws_config::from_env().load().await;
        let s3 = aws_sdk_s3::Client::new(&sdk_config);
        let tm = crate::Client::new(crate::Config::builder().client(s3.clone()).build());

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("nested/deeper")).unwrap();
        std::fs::write(dir.path().join("a.txt"), "one").unwrap();
        std::fs::write(dir.path().join("nested/b.txt"), "two").unwrap();
        std::fs::write(dir.path().join("nested/deeper/c.bin"), vec![7u8; 1024]).unwrap();
        // Keys chosen to disagree if either side orders differently than we assume:
        // `a.txt` must sort before `a/`, and spaces and `+` must survive the round trip.
        std::fs::create_dir_all(dir.path().join("a")).unwrap();
        std::fs::write(dir.path().join("a/inner.txt"), "three").unwrap();
        std::fs::write(dir.path().join("with space.txt"), "four").unwrap();
        std::fs::write(dir.path().join("with+plus.txt"), "five").unwrap();
        std::fs::write(dir.path().join("empty.txt"), "").unwrap();

        let (sent, extra) = sync_up(&tm, &s3, dir.path(), &bucket, &prefix)
            .await
            .expect("first run");
        println!("first run: sent {sent}, destination-only {extra}");
        assert_eq!(sent, 7, "every local file should be sent the first time");
        assert_eq!(extra, 0);

        let (sent_again, extra_again) = sync_up(&tm, &s3, dir.path(), &bucket, &prefix)
            .await
            .expect("second run");
        println!("second run: sent {sent_again}, destination-only {extra_again}");
        assert_eq!(
            sent_again, 0,
            "a re-run with nothing changed must transfer nothing"
        );
        assert_eq!(extra_again, 0, "no key should look destination-only");

        // Touching one file must send exactly that one.
        std::fs::write(dir.path().join("a.txt"), "one changed").unwrap();
        let (sent_third, _) = sync_up(&tm, &s3, dir.path(), &bucket, &prefix)
            .await
            .expect("third run");
        assert_eq!(sent_third, 1, "only the changed file should be sent");
    }
}
