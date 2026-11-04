/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;
use std::sync::Arc;

use aws_sdk_s3::types::Object;

use crate::io::key::filter::{KeyFilter, Rule};
use crate::io::key::stream::{EntryMeta, Obstruction};
use crate::io::walk::{FsWalkContext, FsWalker, SortOrder, WalkError, WalkErrorKind};

use super::*;

// A stream that hands back a written-down sequence, counting what was read so a test
// can show the merge does not read ahead.
//
// `stops_on_failure` models a walk that a failure ended: such a walk reports itself done
// from that moment, where one that survived its failure does not.
struct Scripted {
    items: VecDeque<Result<Entry<()>, StreamError>>,
    reads: usize,
    stops_on_failure: bool,
    stopped: bool,
}

impl Scripted {
    fn of(keys: &[&str]) -> Self {
        Self::build(keys.iter().map(|k| Ok(entry(k))).collect(), false)
    }

    fn from(items: Vec<Result<Entry<()>, StreamError>>) -> Self {
        Self::build(items, false)
    }

    // A side whose failure ends it, the way a walk behaves when it cannot carry on.
    fn that_stops(items: Vec<Result<Entry<()>, StreamError>>) -> Self {
        Self::build(items, true)
    }

    fn build(items: Vec<Result<Entry<()>, StreamError>>, stops_on_failure: bool) -> Self {
        Self {
            items: items.into(),
            reads: 0,
            stops_on_failure,
            stopped: false,
        }
    }
}

impl KeyStream for Scripted {
    type Source = ();

    async fn next_entry(&mut self) -> Option<Result<Entry<()>, StreamError>> {
        self.reads += 1;
        let item = self.items.pop_front();
        match &item {
            // Reaching the end stops a walk, as does a failure it could not survive.
            None => self.stopped = true,
            Some(Err(_)) if self.stops_on_failure => self.stopped = true,
            _ => {}
        }
        item
    }

    fn is_done(&self) -> bool {
        self.stopped
    }
}

fn entry(key: &str) -> Entry<()> {
    Entry {
        key: key.to_string(),
        meta: EntryMeta {
            size: Some(1),
            last_modified_secs: Some(0),
            obstruction: None,
        },
        source: (),
    }
}

// One key lost, and the side carries on.
fn a_failure() -> StreamError {
    StreamError::MalformedListing {
        key: Some("dropped.txt".to_string()),
        what: "size",
    }
}

// A failure that ends the side it came from.
fn a_fatal_failure() -> StreamError {
    StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/gone")),
        WalkErrorKind::SourceUnreadable,
        Box::from("the root went away"),
    ))
}

// Where a key was found, written short so a whole plan fits in one assertion.
//
// The two unknowns are kept apart, because a consumer acts on them differently: one names a
// key it can hold back, the other says absence cannot be read from position here at all. A
// single `Unknown` would let a change swap one for the other with every plan test still
// passing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum At {
    Here,
    Gone,
    UnknownKey,
    UnknownRange,
}

// A file that went away holds nothing back. Worth pinning because the answer it must not give
// is the one the arms below it would reach: a failure that names no lost key falls through to
// the whole side's account staying open, which would stop every later key from being decided
// over a file that is simply gone.
#[test]
fn a_file_that_went_away_holds_nothing_back() {
    let gone = StreamError::Walk(WalkError::new(
        Some(std::path::PathBuf::from("/root/a.txt")),
        WalkErrorKind::Vanished,
        Box::from("not there any more"),
    ));
    assert!(matches!(
        cost_of(&gone, Some(Path::new("/root"))),
        Cost::Nothing
    ));
    // With no root to name the key against, the answer is the same: there is no key to hold.
    assert!(matches!(cost_of(&gone, None), Cost::Nothing));
    // A file that is there and could not be read still holds its own key.
    let denied = StreamError::Walk(WalkError::new(
        Some(std::path::PathBuf::from("/root/b.txt")),
        WalkErrorKind::PermissionDenied,
        Box::from("denied"),
    ));
    match cost_of(&denied, Some(Path::new("/root"))) {
        Cost::Key(key) => assert_eq!(key, "b.txt"),
        other => panic!("a file that is there should hold its own key, got {other:?}"),
    }
}

fn at<T>(side: &SideState<T>) -> At {
    match side {
        SideState::Present(_) => At::Here,
        SideState::Absent => At::Gone,
        SideState::Unknown(KeysLost::OneKey) => At::UnknownKey,
        SideState::Unknown(KeysLost::UnknownRange) => At::UnknownRange,
        // A failure that cost this side nothing leaves the position readable, which is the
        // same thing a consumer learns from the side simply not having a key there.
        SideState::Unknown(KeysLost::Nothing) => At::Gone,
    }
}

async fn pair_up(src: &[&str], dst: &[&str]) -> Vec<(String, At, At)> {
    let mut walk = Walk::new(Scripted::of(src), Scripted::of(dst));
    let mut plan = Vec::new();
    while let Some(next) = walk.next().await {
        let pairing = next.expect("scripted sides do not fail");
        plan.push((
            pairing.key().to_string(),
            at(pairing.source()),
            at(pairing.destination()),
        ));
    }
    assert_eq!(
        walk.progress(),
        Progress::Accounted,
        "the merge ends only when both sides are done"
    );
    plan
}

fn plan(rows: &[(&str, At, At)]) -> Vec<(String, At, At)> {
    rows.iter()
        .map(|(k, s, d)| (k.to_string(), *s, *d))
        .collect()
}

#[tokio::test]
async fn identical_sides_pair_every_key_from_both() {
    assert_eq!(
        pair_up(&["a.txt", "b.txt"], &["a.txt", "b.txt"]).await,
        plan(&[("a.txt", At::Here, At::Here), ("b.txt", At::Here, At::Here)])
    );
}

#[tokio::test]
async fn a_key_only_the_source_has_is_absent_from_the_destination() {
    assert_eq!(
        pair_up(&["a.txt", "b.txt"], &["b.txt"]).await,
        plan(&[("a.txt", At::Here, At::Gone), ("b.txt", At::Here, At::Here)])
    );
}

#[tokio::test]
async fn a_key_only_the_destination_has_is_absent_from_the_source() {
    assert_eq!(
        pair_up(&["b.txt"], &["a.txt", "b.txt"]).await,
        plan(&[("a.txt", At::Gone, At::Here), ("b.txt", At::Here, At::Here)])
    );
}

#[tokio::test]
async fn keys_past_the_end_of_the_source_still_pair() {
    assert_eq!(
        pair_up(&["a.txt"], &["a.txt", "y.txt", "z.txt"]).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("y.txt", At::Gone, At::Here),
            ("z.txt", At::Gone, At::Here),
        ])
    );
}

#[tokio::test]
async fn two_empty_sides_pair_nothing() {
    assert_eq!(pair_up(&[], &[]).await, plan(&[]));
}

#[tokio::test]
async fn one_pairing_reads_one_entry_from_each_side() {
    let mut walk = Walk::new(Scripted::of(&["a.txt", "b.txt"]), Scripted::of(&["a.txt"]));
    let _ = walk.next().await.expect("a pairing for a.txt");
    assert_eq!(walk.src.stream.reads, 1, "the source is read once");
    assert_eq!(walk.dst.stream.reads, 1, "the destination is read once");
}

#[tokio::test]
async fn a_side_sorting_ahead_is_not_read_again_while_it_waits() {
    // The destination holds a key that sorts last, so it should sit at the head while
    // the source is drained past it.
    let mut walk = Walk::new(Scripted::of(&["a.txt", "b.txt"]), Scripted::of(&["z.txt"]));
    let _ = walk.next().await.expect("a.txt");
    let _ = walk.next().await.expect("b.txt");
    assert_eq!(
        walk.dst.stream.reads, 1,
        "the waiting side is read once, not once per pairing"
    );
}

#[tokio::test]
async fn a_failure_costing_one_key_is_reported_and_the_merge_carries_on() {
    let mut walk = Walk::new(
        Scripted::from(vec![Err(a_failure()), Ok(entry("b.txt"))]),
        Scripted::of(&["b.txt"]),
    );
    let reported = walk.next().await.expect("the failure");
    assert!(
        matches!(reported, Err(StreamError::MalformedListing { .. })),
        "the failure reaches the caller as it was reported"
    );
    let after = walk.next().await.expect("b.txt").expect("a pairing");
    assert_eq!(after.key(), "b.txt");
    assert_eq!(at(after.source()), At::Here);
    assert_eq!(at(after.destination()), At::Here);
}

#[tokio::test]
async fn a_failure_does_not_discard_the_other_sides_entry() {
    // `z.txt` sorts last, so it is sitting at the destination head when the source
    // fails. That is the entry a careless implementation would drop.
    let mut walk = Walk::new(
        Scripted::from(vec![Ok(entry("a.txt")), Err(a_failure())]),
        Scripted::of(&["z.txt"]),
    );
    let first = walk.next().await.expect("a.txt").expect("a pairing");
    assert_eq!(first.key(), "a.txt");
    let _ = walk.next().await.expect("the failure");
    let after = walk.next().await.expect("z.txt").expect("a pairing");
    assert_eq!(
        after.key(),
        "z.txt",
        "the entry held at the other head survives the failure"
    );
    assert_eq!(at(after.destination()), At::Here);
}

// The other path out of a stretch. The test above has the side that reported the failure produce
// the key that passes it; here the failure's side produces nothing more, and the key comes from
// the other side. That side is the one asking, so the answer has to prune as it goes — otherwise
// every later key is compared against every stretch the run has ever held, and deciding one key
// costs what the whole run lost rather than a fixed amount.
#[tokio::test]
async fn stretches_are_released_by_the_side_that_asks_about_them() {
    let failures: Vec<Result<Entry<()>, StreamError>> = (0..8)
        .map(|i| {
            Err(StreamError::Walk(WalkError::new(
                Some(PathBuf::from(format!("/root/d{i}"))),
                WalkErrorKind::DirectoryUnreadable,
                Box::from("permission denied"),
            )))
        })
        .collect();
    // The failing side ends after its failures; every key comes from the other side.
    let mut walk = Walk::new(
        Scripted::from(failures),
        Scripted::of(&["z0.txt", "z1.txt", "z2.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);

    for _ in 0..8 {
        let _ = walk.next().await.expect("a failure");
    }
    assert_eq!(
        walk.src.gap.under.len(),
        8,
        "each failure should be held where it is reported"
    );

    // One key from the other side, sorting past all of them.
    let _ = walk.next().await.expect("z0.txt");
    assert!(
        walk.src.gap.under.is_empty(),
        "the side being asked about kept {} stretches the merge is past",
        walk.src.gap.under.len()
    );

    // And the keys after it still decide, which is what the pruning must not break.
    let mut remaining = 0;
    while let Some(result) = walk.next().await {
        assert!(result.is_ok(), "a later key stopped deciding");
        remaining += 1;
    }
    assert_eq!(remaining, 2, "the run did not finish the other side");
}

// A directory nobody could open, taken from the design's own example.
fn a_lost_directory() -> StreamError {
    StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root/photos/2019")),
        WalkErrorKind::DirectoryUnreadable,
        Box::from("permission denied"),
    ))
}

#[tokio::test]
async fn keys_a_lost_directory_hid_are_unknown_and_not_absent() {
    // The source cannot open `photos/2019/` and carries on at `photos/2020/`. The
    // destination holds a key inside the directory nobody read, and reporting it absent
    // is what would delete it.
    let mut walk = Walk::new(
        Scripted::from(vec![
            Ok(entry("photos/2018/a.jpg")),
            Err(a_lost_directory()),
            Ok(entry("photos/2020/b.jpg")),
        ]),
        Scripted::of(&[
            "photos/2018/a.jpg",
            "photos/2019/x.jpg",
            "photos/2020/b.jpg",
        ]),
    );
    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    assert_eq!(
        seen,
        plan(&[
            ("photos/2018/a.jpg", At::Here, At::Here),
            ("photos/2019/x.jpg", At::UnknownRange, At::Here),
            ("photos/2020/b.jpg", At::Here, At::Here),
        ]),
        "the key inside the unread directory is unknown, and the sibling after it compares"
    );
}

#[tokio::test]
async fn a_key_outside_the_lost_subtree_is_absent() {
    // The failure cost `photos/2019/` and nothing else, so `z.txt` is absent as usual. What
    // bounds it is the prefix taken from the failure's own path: the side producing a later key
    // settles nothing, because a failure can be heard before the keys it hid.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(a_lost_directory()), Ok(entry("m.txt"))]),
        Scripted::of(&["m.txt", "z.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    let _ = walk.next().await.expect("the failure");
    let m = walk.next().await.expect("m.txt").expect("a pairing");
    assert_eq!(m.key(), "m.txt");
    let z = walk.next().await.expect("z.txt").expect("a pairing");
    assert_eq!(
        at(z.source()),
        At::Gone,
        "`z.txt` is past everything the failure hid, so the source is simply absent"
    );
}

// Both kinds of loss can land on one side at the same position: a listing that named the object
// it dropped, and a failure whose extent could not be worked out. The named one is the finer
// answer and reporting it would tell a consumer the keys around it are accounted for, which on a
// side that also lost a range they are not.
#[tokio::test]
async fn a_named_loss_inside_an_unknown_range_reports_the_range() {
    // The unnameable failure first, so the side is already holding a range when the named one
    // arrives. Order does not matter to the answer, only that both are held.
    let unnameable = StreamError::MalformedListing {
        key: None,
        what: "size",
    };
    let mut walk = Walk::new(
        Scripted::from(vec![
            Err(unnameable),
            Err(a_lost_key("m.txt")),
            Ok(entry("z.txt")),
        ]),
        Scripted::of(&["m.txt", "z.txt"]),
    );

    let mut reported = None;
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            if pairing.key() == "m.txt" {
                reported = Some(match pairing.source() {
                    SideState::Unknown(lost) => *lost,
                    other => panic!("m.txt read as {other:?} rather than unknown"),
                });
            }
        }
    }

    assert_eq!(
        reported,
        Some(KeysLost::UnknownRange),
        "a key named as lost on a side that also lost a range reported only itself"
    );
}

// A side going backwards would have a key read as absent and then as present, so one key would
// collect both a delete and a transfer while the plan called itself whole. The only guard on the
// one root known to list unordered is a debug assertion, which compiles out, so the merge checks
// what it actually depends on: that each side's keys strictly increase.
#[tokio::test]
async fn a_side_going_backwards_does_not_give_one_key_two_actions() {
    // The source goes backwards, and the destination holds a key after that point. That key is
    // the one the guarantee is about: without the check it reads as absent from the source, and
    // absence is what licenses removing it.
    let mut walk = Walk::new(
        Scripted::of(&["b.txt", "a.txt"]),
        Scripted::of(&["a.txt", "b.txt", "d.txt"]),
    );

    // Which side of the failure each decision fell on. Nothing can recall a key decided before
    // the disorder surfaced — the merge had no way to know yet — so the guarantee is about the
    // keys after it.
    let mut decided: Vec<(String, At, At, bool)> = Vec::new();
    let mut failures = 0;
    while let Some(next) = walk.next().await {
        match next {
            Ok(pairing) => decided.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
                failures > 0,
            )),
            Err(_) => failures += 1,
        }
    }

    assert_eq!(failures, 1, "the disorder was not reported");
    assert!(
        !walk.is_plan_complete(),
        "a run that could not read one side's positions called its plan whole"
    );
    // No key may appear twice, which is how one would collect two actions.
    let mut keys: Vec<&str> = decided.iter().map(|(k, _, _, _)| k.as_str()).collect();
    keys.sort();
    let before = keys.len();
    keys.dedup();
    assert_eq!(before, keys.len(), "a key was decided twice: {decided:?}");
    // Nothing decided after the disorder reads as absent on the side that caused it, since
    // absence is what licenses a delete.
    for (key, source, _, after) in &decided {
        if *after {
            assert_ne!(
                *source,
                At::Gone,
                "{key} read as absent on a side whose positions had stopped meaning anything"
            );
        }
    }
    assert!(
        decided.iter().any(|(_, _, _, after)| *after),
        "the test decided nothing after the disorder, so it proved nothing"
    );
}

// A listing that dropped one object and named which.
fn a_lost_key(key: &str) -> StreamError {
    StreamError::MalformedListing {
        key: Some(key.to_string()),
        what: "size",
    }
}

fn obstructed(key: &str, why: Obstruction) -> Entry<()> {
    let mut entry = entry(key);
    entry.meta.obstruction = Some(why);
    entry
}

// Pair every key and report whether the walk called its plan complete.
async fn pair_all(mut walk: Walk<Scripted, Scripted>) -> (Vec<(String, At, At)>, bool) {
    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    (seen, walk.is_plan_complete())
}

#[tokio::test]
async fn a_source_link_the_walk_did_not_follow_holds_back_the_keys_under_it() {
    let walk = Walk::new(
        Scripted::from(vec![
            Ok(entry("a.txt")),
            Ok(obstructed("photos", Obstruction::UnfollowedLink)),
        ]),
        Scripted::of(&["photos", "photos/2019/x.jpg", "photos/y.jpg", "q.txt"]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(
        seen,
        plan(&[
            ("a.txt", At::Here, At::Gone),
            ("photos", At::Here, At::Here),
            ("photos/2019/x.jpg", At::UnknownRange, At::Here),
            ("photos/y.jpg", At::UnknownRange, At::Here),
            ("q.txt", At::Gone, At::Here),
        ])
    );
    assert!(
        !complete,
        "a plan that skipped keys under a link called itself complete"
    );
}

#[tokio::test]
async fn an_excluded_source_link_holds_back_the_keys_under_it_and_makes_no_pairing() {
    let walk = Walk::new(
        Scripted::from(vec![
            Ok(entry("a.txt")),
            Ok(obstructed("photos", Obstruction::ExcludedLink)),
        ]),
        Scripted::of(&["photos/y.jpg", "q.txt"]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(
        seen,
        plan(&[
            ("a.txt", At::Here, At::Gone),
            ("photos/y.jpg", At::UnknownRange, At::Here),
            ("q.txt", At::Gone, At::Here),
        ])
    );
    assert!(
        !complete,
        "a plan that skipped keys under an excluded link called itself complete"
    );
}

// Skipping past `videos` passes every key before it. The destination's `photos/y.jpg` sorts
// before `videos`, so it pairs first, while the hold on `photos` still stands.
#[tokio::test]
async fn a_second_excluded_link_keeps_the_hold_of_the_first() {
    let walk = Walk::new(
        Scripted::from(vec![
            Ok(obstructed("photos", Obstruction::ExcludedLink)),
            Ok(obstructed("videos", Obstruction::ExcludedLink)),
        ]),
        Scripted::of(&["photos/y.jpg"]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(seen, plan(&[("photos/y.jpg", At::UnknownRange, At::Here)]));
    assert!(
        !complete,
        "a plan that skipped keys under an excluded link called itself complete"
    );
}

// The same rule protects a hold from a failure. Skipping past `photos` must not pass the
// destination's `a/x.txt` before it pairs under the unreadable directory `a`.
#[tokio::test]
async fn an_excluded_link_keeps_the_hold_of_an_earlier_failure() {
    let walk = Walk::new(
        Scripted::from(vec![
            Err(StreamError::Walk(WalkError::new(
                Some(PathBuf::from("/root/a")),
                WalkErrorKind::DirectoryUnreadable,
                Box::from("permission denied"),
            ))),
            Ok(obstructed("photos", Obstruction::ExcludedLink)),
        ]),
        Scripted::of(&["a/x.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    let (seen, _) = pair_all(walk).await;
    assert_eq!(seen, plan(&[("a/x.txt", At::UnknownRange, At::Here)]));
}

// The mirror case on the destination side. Skipping past the destination's `videos` must not
// pass the source's `photos/y.jpg` before it pairs.
#[tokio::test]
async fn an_excluded_destination_link_waits_for_an_earlier_source_key() {
    let walk = Walk::new(
        Scripted::of(&["photos/y.jpg"]),
        Scripted::from(vec![
            Err(StreamError::Walk(WalkError::new(
                Some(PathBuf::from("/root/photos")),
                WalkErrorKind::DirectoryUnreadable,
                Box::from("permission denied"),
            ))),
            Ok(obstructed("videos", Obstruction::ExcludedLink)),
        ]),
    )
    .with_roots(None, Some(PathBuf::from("/root")));
    let (seen, _) = pair_all(walk).await;
    assert_eq!(seen, plan(&[("photos/y.jpg", At::Here, At::UnknownRange)]));
}

#[tokio::test]
async fn an_excluded_destination_link_makes_no_pairing() {
    let walk = Walk::new(
        Scripted::of(&["photos/y.jpg"]),
        Scripted::from(vec![Ok(obstructed("photos", Obstruction::ExcludedLink))]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(seen, plan(&[("photos/y.jpg", At::Here, At::Gone)]));
    assert!(complete);
}

#[tokio::test]
async fn a_source_link_with_nothing_under_it_leaves_the_plan_complete() {
    let walk = Walk::new(
        Scripted::from(vec![Ok(obstructed("photos", Obstruction::UnfollowedLink))]),
        Scripted::of(&["photos", "q.txt"]),
    );
    let (_, complete) = pair_all(walk).await;
    assert!(complete);
}

// A download writes through a destination link the walk left unfollowed, as `download_objects`
// does, so that link holds nothing back.
#[tokio::test]
async fn an_unfollowed_destination_link_holds_nothing_back() {
    let walk = Walk::new(
        Scripted::of(&["photos/y.jpg"]),
        Scripted::from(vec![Ok(obstructed("photos", Obstruction::UnfollowedLink))]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(
        seen,
        plan(&[
            ("photos", At::Gone, At::Here),
            ("photos/y.jpg", At::Here, At::Gone),
        ])
    );
    assert!(complete);
}

// A socket cannot hold a directory, so its name holds back no keys under it.
#[tokio::test]
async fn a_source_socket_holds_nothing_back() {
    let walk = Walk::new(
        Scripted::from(vec![Ok(obstructed("sock", Obstruction::NothingToRead))]),
        Scripted::of(&["sock/x.txt"]),
    );
    let (seen, complete) = pair_all(walk).await;
    assert_eq!(
        seen,
        plan(&[
            ("sock", At::Here, At::Gone),
            ("sock/x.txt", At::Gone, At::Here),
        ])
    );
    assert!(complete);
}

async fn drain(mut walk: Walk<Scripted, Scripted>) -> Vec<(String, At, At)> {
    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    seen
}

#[tokio::test]
async fn a_key_a_side_named_as_lost_is_unknown_where_it_sorts() {
    // The destination drops `m.txt` and says so while handing over `a.txt`, so the answer is
    // due three keys later. Its neighbours are unaffected.
    let walk = Walk::new(
        Scripted::of(&["a.txt", "m.txt", "q.txt", "z.txt"]),
        Scripted::from(vec![
            Ok(entry("a.txt")),
            Err(a_lost_key("m.txt")),
            Ok(entry("q.txt")),
            Ok(entry("z.txt")),
        ]),
    );
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("m.txt", At::Here, At::UnknownKey),
            ("q.txt", At::Here, At::Here),
            ("z.txt", At::Here, At::Here),
        ]),
        "one key is unknown on the side that lost it, and the keys around it compare"
    );
}

#[tokio::test]
async fn a_key_the_source_lost_is_unknown_there() {
    // Downloading makes the source a listing, so the side naming a dropped key can be
    // either one.
    //
    // `q.txt` is the key that shows only `m.txt` is affected. Treating the
    // failure as a stretch would hold every destination-only key until the source spoke
    // again, and `q.txt` sits inside that window.
    let walk = Walk::new(
        Scripted::from(vec![
            Ok(entry("a.txt")),
            Err(a_lost_key("m.txt")),
            Ok(entry("z.txt")),
        ]),
        Scripted::of(&["a.txt", "m.txt", "q.txt", "z.txt"]),
    );
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("m.txt", At::UnknownKey, At::Here),
            ("q.txt", At::Gone, At::Here),
            ("z.txt", At::Here, At::Here),
        ])
    );
}

#[tokio::test]
async fn a_held_key_is_let_go_once_the_merge_is_past_it() {
    // Held keys are bounded by what the walk has open, which only holds while keys nobody
    // reaches are let go. Without that the list grows for the length of the run.
    let mut walk = Walk::new(
        Scripted::of(&["z.txt"]),
        Scripted::from(vec![Err(a_lost_key("m.txt")), Ok(entry("z.txt"))]),
    );
    let _ = walk.next().await.expect("the failure");
    assert_eq!(walk.dst.lost.len(), 1, "the key is held when it is named");
    let _ = walk.next().await.expect("z.txt");
    assert!(
        walk.dst.lost.is_empty(),
        "a key the merge passed without reaching is no longer held"
    );
}

#[tokio::test]
async fn the_side_producing_keys_lets_go_of_its_own_held_ones() {
    // The third pairing path. Answering for a key a side did not produce prunes that side, and
    // pairing two present sides prunes both — but a side producing keys while the other has
    // none was pruning nothing, so its held set grew for the whole run.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(a_lost_key("m.txt")), Ok(entry("z.txt"))]),
        Scripted::of(&[]),
    );
    let _ = walk.next().await.expect("the failure");
    assert_eq!(walk.src.lost.len(), 1, "the key is held when it is named");
    let _ = walk.next().await.expect("z.txt");
    assert!(
        walk.src.lost.is_empty(),
        "a key the producing side has passed is no longer held"
    );
}

#[tokio::test]
async fn a_held_key_is_let_go_while_answering_for_the_absent_side() {
    // The same bound, on the path where the other side has nothing at this key. Pairing two
    // present sides is what released the key above; here that never runs, so the release has
    // to happen while the absent side is being answered for.
    let mut walk = Walk::new(
        Scripted::of(&["z.txt"]),
        Scripted::from(vec![Err(a_lost_key("m.txt"))]),
    );
    let _ = walk.next().await.expect("the failure");
    assert_eq!(walk.dst.lost.len(), 1, "the key is held when it is named");
    let _ = walk.next().await.expect("z.txt");
    assert!(
        walk.dst.lost.is_empty(),
        "a key passed while the absent side was answered for is no longer held"
    );
}

#[tokio::test]
async fn a_stretch_the_merge_has_passed_is_no_longer_held() {
    // Stretches are bounded the same way. A key that is past the prefix and outside it puts
    // everything the failure hid behind the merge, and holding on would grow the list for the
    // length of the run.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(a_lost_directory()), Ok(entry("z.txt"))]),
        Scripted::of(&["z.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    let _ = walk.next().await.expect("the failure");
    assert_eq!(
        walk.src.gap.under.len(),
        1,
        "the stretch is held where it is reported"
    );
    let _ = walk.next().await.expect("z.txt");
    assert!(
        walk.src.gap.under.is_empty(),
        "a stretch the merge is past is no longer held"
    );
}

#[tokio::test]
async fn a_lost_key_nobody_reaches_leaves_later_keys_alone() {
    // `m.txt` is named as lost and neither side ever produces it, so the answer is due at a
    // key that never arrives. `z.txt` must not inherit it.
    let walk = Walk::new(
        Scripted::of(&["z.txt"]),
        Scripted::from(vec![Err(a_lost_key("m.txt"))]),
    );
    assert_eq!(
        drain(walk).await,
        plan(&[("z.txt", At::Here, At::Gone)]),
        "a held key that nobody reached says nothing about a later one"
    );
}

// One file the walk could not read. The error names a path, not a key, and it arrives at
// the directory's position rather than where the file sorts inside it.
fn one_file_unread() -> StreamError {
    StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root/photos/2019/b.jpg")),
        WalkErrorKind::PermissionDenied,
        Box::from("permission denied"),
    ))
}

#[tokio::test]
async fn a_lost_key_nobody_could_name_holds_the_side_open() {
    // Which key was lost cannot be worked out here, so no later key on that side may be
    // called absent — that is what would delete the object matching the file nobody read.
    let walk = Walk::new(
        Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt", "q.txt", "z.txt"]),
    );
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("q.txt", At::UnknownRange, At::Here),
            ("z.txt", At::UnknownRange, At::Here),
        ]),
        "every later key stays unknown on the side that lost one it could not name"
    );
}

// A destination that was not there when the run started, and by the time the walk reached it was
// there and would not open. Only absence licenses treating every key as missing; a refusal has to
// reach the caller, because reading it as absence sends the whole source without asking the
// comparison about any of it.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_destination_that_appears_unreadable_is_not_read_as_absent() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().join("appears-later");

    // Built while nothing is there, which is the state the swallow is for.
    let destination = LocalDestination::new(
        FsWalker::builder()
            .sort(SortOrder::WholeWalk)
            .recursive(true)
            .build()
            .walk(FsWalkContext::builder().root(&root).build()),
        &root,
    );

    // Then it turns up, and will not open.
    std::fs::create_dir(&root).expect("a directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    let readable = std::fs::read_dir(&root).is_ok();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    if readable {
        // Running as a user the mode does not bind, so there is nothing to test.
        return;
    }
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).expect("chmod");

    let mut destination = destination;
    let first = destination.next_entry().await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).expect("chmod");

    match first {
        Some(Err(StreamError::Walk(err))) => {
            assert_eq!(err.kind(), WalkErrorKind::SourceUnreadable);
        }
        Some(Ok(entry)) => panic!("an unreadable destination produced {:?}", entry.key),
        Some(Err(other)) => panic!("an unexpected failure: {other}"),
        None => panic!("a destination that refused to be read was reported as empty"),
    }
}

// A directory that lists but whose children cannot be stat'd: readable, not searchable.
// `None` when the mode has no effect, as it has none for root.
#[cfg(unix)]
fn unstattable_dir(root: &std::path::Path) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let locked = root.join("locked");
    std::fs::create_dir(&locked).expect("a directory");
    std::fs::write(locked.join("secret.txt"), "").expect("a file inside it");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o400)).expect("chmod");
    let listable = std::fs::read_dir(&locked).is_ok();
    let stattable = std::fs::metadata(locked.join("secret.txt")).is_ok();
    if listable && !stattable {
        Some(locked)
    } else {
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        None
    }
}

#[cfg_attr(miri, ignore)]
#[cfg(unix)]
#[tokio::test]
async fn a_real_walk_failure_is_named_against_the_root_the_walk_reports() {
    use std::os::unix::fs::PermissionsExt;

    // The root this layer holds and the paths the walk puts in its errors have to agree on
    // form. They do only because the walk is built without canonicalizing its root — turn
    // that on and the root stays `/var/...` while the walk reports `/private/var/...`, the
    // prefix no longer strips, and every key after the failure goes back to unknown. A
    // scripted failure cannot catch that, because the test writes both halves.
    let dir = tempfile::tempdir().expect("a temp dir");
    let Some(locked) = unstattable_dir(dir.path()) else {
        return;
    };

    let walker = Walker::builder().build();
    let mut walk = Walk::new(
        walker.local_walk(dir.path().to_path_buf()),
        Scripted::of(&["locked/secret.txt", "z.txt"]),
    )
    .with_roots(Some(dir.path().to_path_buf()), None);

    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");

    assert_eq!(
        seen,
        plan(&[
            ("locked/secret.txt", At::UnknownKey, At::Here),
            ("z.txt", At::Gone, At::Here),
        ]),
        "the unreadable file's own key is unknown, and the key after it is absent"
    );
}

#[cfg_attr(miri, ignore)]
#[cfg(unix)]
#[tokio::test]
async fn two_keys_lost_from_one_directory_are_both_unknown() {
    use std::os::unix::fs::PermissionsExt;

    // A directory that lists but whose children cannot be stat'd, holding two files. The walk
    // reports both in the order the filesystem hands them over, which is not key order.
    let dir = tempfile::tempdir().expect("a temp dir");
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).expect("a directory");
    std::fs::write(locked.join("z.txt"), "").expect("a file");
    std::fs::write(locked.join("a.txt"), "").expect("a file");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o400)).expect("chmod");
    let usable =
        std::fs::read_dir(&locked).is_ok() && std::fs::metadata(locked.join("a.txt")).is_err();
    assert!(
        usable,
        "this platform does not produce the state under test"
    );

    let walker = Walker::builder().build();
    let mut walk = Walk::new(
        walker.local_walk(dir.path().to_path_buf()),
        Scripted::of(&["locked/a.txt", "locked/z.txt"]),
    )
    .with_roots(Some(dir.path().to_path_buf()), None);

    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("chmod");

    assert_eq!(
        seen,
        plan(&[
            ("locked/a.txt", At::UnknownKey, At::Here),
            ("locked/z.txt", At::UnknownKey, At::Here),
        ]),
        "both files exist and both were reported unreadable, so neither may read as absent"
    );
}

#[cfg_attr(miri, ignore)]
#[cfg(unix)]
#[tokio::test]
async fn two_stretches_lost_from_one_side_are_both_covered() {
    use std::os::unix::fs::PermissionsExt;

    // Two range losses on one walk. The cycle on `mlink` is noticed while the root is read, so it
    // arrives first; descending into `adir` then fails, and its prefix sorts earlier. Keeping one
    // stretch at a time forgets whichever arrived first.
    let dir = tempfile::tempdir().expect("a temp dir");
    let adir = dir.path().join("adir");
    std::fs::create_dir(&adir).expect("a directory");
    std::fs::write(adir.join("inner.txt"), "").expect("a file inside it");
    std::fs::set_permissions(&adir, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    std::os::unix::fs::symlink(".", dir.path().join("mlink")).expect("a self-referring link");
    let usable = std::fs::read_dir(&adir).is_err();
    assert!(
        usable,
        "this platform does not produce the state under test"
    );

    let walker = Walker::builder().follow_symlinks(true).build();
    let mut walk = Walk::new(
        walker.local_walk(dir.path().to_path_buf()),
        Scripted::of(&["adir/inner.txt", "mlink/deep.txt"]),
    )
    .with_roots(Some(dir.path().to_path_buf()), None);

    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    std::fs::set_permissions(&adir, std::fs::Permissions::from_mode(0o755)).expect("chmod");

    assert_eq!(
        seen,
        plan(&[
            ("adir/inner.txt", At::UnknownRange, At::Here),
            ("mlink/deep.txt", At::UnknownRange, At::Here),
        ]),
        "nobody enumerated under either, so neither may read as absent"
    );
}

#[cfg_attr(miri, ignore)]
#[cfg(unix)]
#[tokio::test]
async fn a_loss_reported_before_its_own_position_still_covers_the_keys_it_hid() {
    // A symlink pointing at its own ancestor. The walk notices while reading the parent, so the
    // failure arrives ahead of every sibling — including ones sorting earlier than the subtree
    // it hid. A key inside that subtree must not read as absent.
    let dir = tempfile::tempdir().expect("a temp dir");
    std::fs::write(dir.path().join("a.txt"), "").expect("a file");
    std::os::unix::fs::symlink(".", dir.path().join("zlink")).expect("a self-referring link");

    let walker = Walker::builder().follow_symlinks(true).build();
    let mut walk = Walk::new(
        walker.local_walk(dir.path().to_path_buf()),
        Scripted::of(&["a.txt", "zlink/deep.txt"]),
    )
    .with_roots(Some(dir.path().to_path_buf()), None);

    let mut seen = Vec::new();
    while let Some(next) = walk.next().await {
        if let Ok(pairing) = next {
            seen.push((
                pairing.key().to_string(),
                at(pairing.source()),
                at(pairing.destination()),
            ));
        }
    }
    assert_eq!(
        seen,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("zlink/deep.txt", At::UnknownRange, At::Here),
        ]),
        "nobody enumerated under the link, so what is there cannot be read from position"
    );
}

#[tokio::test]
async fn a_walk_failure_names_one_key_once_the_root_is_known() {
    // The same unreadable file, with the root supplied. `/root/photos/2019/b.jpg` under
    // `/root` is the key `photos/2019/b.jpg`, so that one key is held and `z.txt` after it
    // is absent as usual — where without a root the whole side would read as unknown.
    let walk = Walk::new(
        Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt", "photos/2019/b.jpg", "z.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("photos/2019/b.jpg", At::UnknownKey, At::Here),
            ("z.txt", At::Gone, At::Here),
        ]),
        "one key is unknown, and the key after it compares"
    );
}

#[tokio::test]
async fn a_named_lost_key_is_reported_as_one_key() {
    // Naming the key is what lets a consumer act on it alone, which is what `OneKey`
    // promises and `UnknownRange` does not.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt", "photos/2019/b.jpg"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    let _ = walk.next().await.expect("the failure");
    let _ = walk.next().await.expect("a.txt");
    let lost = walk.next().await.expect("the lost key").expect("a pairing");
    assert_eq!(lost.key(), "photos/2019/b.jpg");
    assert!(
        matches!(lost.source(), SideState::Unknown(KeysLost::OneKey)),
        "got {:?}",
        lost.source()
    );
}

#[tokio::test]
async fn a_key_lost_without_a_name_is_reported_as_a_range() {
    // What a consumer is told has to match what was concluded. `OneKey` says the keys
    // around it are known, and acting on that means holding one key back and trusting the
    // rest — the opposite of what this side can promise.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(one_file_unread()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt", "z.txt"]),
    );
    let _ = walk.next().await.expect("the failure");
    let _ = walk.next().await.expect("a.txt");
    let z = walk.next().await.expect("z.txt").expect("a pairing");
    assert!(
        matches!(z.source(), SideState::Unknown(KeysLost::UnknownRange)),
        "got {:?}",
        z.source()
    );
}

// A local name that is not text, so no key could carry it.
fn a_name_no_key_can_carry() -> StreamError {
    StreamError::UnkeyableName(PathBuf::from("/root/photos/caf\u{FFFD}.txt"))
}

#[tokio::test]
async fn a_name_no_key_can_carry_holds_nothing_back() {
    // Such a name never becomes a key, so no pairing ever asks about it and there is nothing
    // to answer for. Treating the side as unable to account for itself would report every
    // later key as unknown, suppressing every delete in the run over a key the comparison
    // never saw.
    let walk = Walk::new(
        Scripted::from(vec![Err(a_name_no_key_can_carry()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt", "photos/caf\u{FFFD}.txt", "z.txt"]),
    );
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("photos/caf\u{FFFD}.txt", At::Gone, At::Here),
            ("z.txt", At::Gone, At::Here),
        ]),
        "the object at the lossy key is an orphan like any other, and later keys compare"
    );
}

#[tokio::test]
async fn a_name_no_key_can_carry_still_leaves_the_plan_incomplete() {
    // Nothing is held, but a file was not accounted for, so the run cannot claim it covered
    // everything it was pointed at.
    let mut walk = Walk::new(
        Scripted::from(vec![Err(a_name_no_key_can_carry()), Ok(entry("a.txt"))]),
        Scripted::of(&["a.txt"]),
    );
    while walk.next().await.is_some() {}
    assert!(!walk.is_plan_complete());
}

#[tokio::test]
async fn a_run_a_fatal_failure_ended_reports_a_partial_plan() {
    // The merge stopped, so most of the keyspace was never looked at. A caller reading the
    // short plan as a whole one is the mistake this answer exists to prevent.
    let mut walk = Walk::new(
        Scripted::that_stops(vec![Err(a_fatal_failure())]),
        Scripted::of(&["a.txt", "b.txt", "c.txt"]),
    );
    while walk.next().await.is_some() {}
    assert_eq!(walk.progress(), Progress::Stopped);
    assert!(!walk.is_plan_complete());
}

#[tokio::test]
async fn a_clean_run_reports_a_whole_plan() {
    let mut walk = Walk::new(Scripted::of(&["a.txt"]), Scripted::of(&["a.txt"]));
    while walk.next().await.is_some() {}
    assert!(walk.is_plan_complete());
}

#[tokio::test]
async fn a_file_that_went_away_leaves_the_plan_whole() {
    // A file that vanished mid-walk is an answer: the file is gone. The walk lost no key, so
    // an active tree keeps a whole plan.
    let gone = StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root/b.txt")),
        WalkErrorKind::Vanished,
        Box::from("not there any more"),
    ));
    let mut walk = Walk::new(
        Scripted::from(vec![Ok(entry("a.txt")), Err(gone), Ok(entry("c.txt"))]),
        Scripted::of(&["a.txt", "c.txt"]),
    );
    let mut errors = 0;
    while let Some(next) = walk.next().await {
        errors += usize::from(next.is_err());
    }
    assert_eq!(errors, 1, "the walk did not report the vanished file");
    assert!(
        walk.is_plan_complete(),
        "a file that went away marked the plan incomplete"
    );
}

#[tokio::test]
async fn a_run_that_lost_a_key_reports_a_partial_plan() {
    let mut walk = Walk::new(
        Scripted::of(&["a.txt"]),
        Scripted::from(vec![Ok(entry("a.txt")), Err(a_lost_key("m.txt"))]),
    );
    while walk.next().await.is_some() {}
    assert!(
        !walk.is_plan_complete(),
        "a caller cannot tell it acted on less than it asked about unless the run says so"
    );
}

#[tokio::test]
async fn a_fatal_failure_on_the_source_ends_the_merge() {
    let mut walk = Walk::new(
        Scripted::that_stops(vec![Err(a_fatal_failure())]),
        Scripted::of(&["a.txt", "b.txt", "c.txt"]),
    );
    let reported = walk.next().await.expect("the failure");
    assert!(reported.is_err(), "the failure reaches the caller");
    assert!(
        walk.next().await.is_none(),
        "a side that has stopped cannot say a key is absent, so nothing more is paired"
    );
    assert_eq!(walk.progress(), Progress::Stopped);
}

#[tokio::test]
async fn a_fatal_failure_on_the_destination_ends_the_merge() {
    let mut walk = Walk::new(
        Scripted::of(&["a.txt", "b.txt", "c.txt"]),
        Scripted::that_stops(vec![Err(a_fatal_failure())]),
    );
    let reported = walk.next().await.expect("the failure");
    assert!(reported.is_err(), "the failure reaches the caller");
    assert!(
        walk.next().await.is_none(),
        "nothing may be written to or removed from a side nobody could read"
    );
    assert_eq!(walk.progress(), Progress::Stopped);
}

#[tokio::test]
async fn keys_past_the_end_of_the_destination_still_pair() {
    assert_eq!(
        pair_up(&["a.txt", "y.txt", "z.txt"], &["a.txt"]).await,
        plan(&[
            ("a.txt", At::Here, At::Here),
            ("y.txt", At::Here, At::Gone),
            ("z.txt", At::Here, At::Gone),
        ])
    );
}

fn a_client() -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("us-west-2"))
        .http_client(aws_smithy_runtime::client::http::test_util::NeverClient::new())
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

// Walk a real tree through the configuration this layer applies, and report each key with
// whatever stops it being transferred.
//
// Driven through the key stream, so the answer read here is the one production sets rather
// than a rule restated in the test.
async fn walk_locally(
    root: &std::path::Path,
    filter: KeyFilter,
) -> Vec<(String, Option<Obstruction>)> {
    let walker = Walker::builder().filter(Arc::new(filter)).build();
    let mut walk = walker.local_walk(root.to_path_buf());
    let mut seen = Vec::new();
    while let Some(next) = walk.next_entry().await {
        let entry = next.expect("this tree reads cleanly");
        seen.push((entry.key, entry.meta.obstruction));
    }
    // Left in the order the walk produced, because that order is what the merge depends
    // on and a sort here would hide it.
    seen
}

#[cfg_attr(miri, ignore)]
#[cfg(unix)]
#[tokio::test]
async fn a_named_pipe_is_walked_and_cannot_have_its_bytes_sent() {
    use nix::sys::stat::Mode;

    let dir = tempfile::tempdir().expect("a temp dir");
    std::fs::write(dir.path().join("a.txt"), b"x").expect("a regular file");
    let fifo = dir.path().join("pipe");
    nix::unistd::mkfifo(&fifo, Mode::S_IRWXU).expect("a named pipe");
    assert!(
        std::fs::symlink_metadata(&fifo).is_ok(),
        "the pipe has to exist for this test to mean anything"
    );

    assert_eq!(
        walk_locally(dir.path(), KeyFilter::new(Vec::new())).await,
        vec![
            ("a.txt".to_string(), None),
            ("pipe".to_string(), Some(Obstruction::NothingToRead)),
        ],
        "a special file is walked, and says nothing can be read from it"
    );
    // `a.txt` before `pipe` is alphabetical either way, so this says nothing about
    // ordering; `the_local_side_is_walked_in_key_order` is what pins that.
}

#[tokio::test]
async fn the_local_side_is_walked_in_key_order() {
    // A listing puts `a/b.txt` before `z.txt`, because `a/` sorts before `z`. A walk that
    // orders each directory among itself hands over the root's files first and descends
    // afterwards, so `z.txt` would come out ahead of `a/b.txt` — two keys the merge would
    // then pair against the wrong counterparts.
    let dir = tempfile::tempdir().expect("a temp dir");
    std::fs::create_dir(dir.path().join("a")).expect("a directory");
    std::fs::write(dir.path().join("a/b.txt"), b"x").expect("a nested file");
    std::fs::write(dir.path().join("z.txt"), b"x").expect("a root file");

    let seen: Vec<String> = walk_locally(dir.path(), KeyFilter::new(Vec::new()))
        .await
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(seen, vec!["a/b.txt".to_string(), "z.txt".to_string()]);
}

#[tokio::test]
async fn one_rule_set_decides_what_the_local_side_yields() {
    let dir = tempfile::tempdir().expect("a temp dir");
    std::fs::create_dir(dir.path().join("logs")).expect("a directory");
    std::fs::write(dir.path().join("keep.txt"), b"x").expect("a kept file");
    std::fs::write(dir.path().join("logs/drop.txt"), b"x").expect("an excluded file");

    let filter = KeyFilter::new(vec![Rule::exclude("logs/*")]);
    assert_eq!(
        walk_locally(dir.path(), filter).await,
        vec![("keep.txt".to_string(), None)],
        "an excluded key never reaches the merge, on either side"
    );
}

#[tokio::test]
async fn the_listing_asks_for_one_unbroken_run_of_keys() {
    // A delimiter rolls keys up into prefixes and a continuation token starts the listing
    // part way through. Either shortens this side, and the merge reads a short side as
    // keys that are not there. The rule matches only a request carrying neither, so a walk
    // that sets one gets no answer and the listing fails.
    let listing = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output::builder()
        .set_contents(Some(vec![
            // A folder marker: zero bytes under a name ending in the delimiter. The
            // walker drops these by default, and attaching a filter must not replace that.
            Object::builder()
                .key("photos/")
                .size(0)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                .build(),
            Object::builder()
                .key("photos/a.jpg")
                .size(1)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                .build(),
            // The rules below exclude this one. A listing side that yields it anyway
            // would pair it against a local key the rules also hid, and the merge would
            // read one of them as deleted.
            Object::builder()
                .key("photos/thumbs/t.jpg")
                .size(1)
                .last_modified(aws_smithy_types::DateTime::from_secs(1_700_000_000))
                .build(),
        ]))
        .build();
    let rule = aws_smithy_mocks::mock!(aws_sdk_s3::Client::list_objects_v2)
        .match_requests(|req| {
            req.delimiter().is_none()
                && req.continuation_token().is_none()
                // Asked for, so an archived object can be told from a restored one. A listing
                // without it reports no restore status at all, and every archived key would be
                // skipped for the life of the bucket.
                && req.optional_object_attributes()
                    == [aws_sdk_s3::types::OptionalObjectAttributes::RestoreStatus]
        })
        .then_output(move || listing.clone());
    let client =
        aws_smithy_mocks::mock_client!(aws_sdk_s3, aws_smithy_mocks::RuleMode::MatchAny, &[rule]);

    let walker = Walker::builder()
        .filter(Arc::new(KeyFilter::new(vec![Rule::exclude("thumbs/*")])))
        .build();
    let mut walk = walker
        .remote_walk(
            client,
            "amzn-s3-demo-bucket".to_string(),
            Some("photos/".to_string()),
        )
        .expect("an ordinary bucket builds");
    let mut keys = Vec::new();
    let mut obstructions = Vec::new();
    while let Some(next) = walk.next_entry().await {
        let entry = next.expect("the listing answers");
        obstructions.push(entry.meta.obstruction);
        keys.push(entry.key);
    }
    assert_eq!(
        obstructions,
        vec![None],
        "a listed object can have its bytes read, which is all a listing can say today"
    );
    assert_eq!(
        keys,
        vec!["a.jpg".to_string()],
        "the folder marker and the excluded key are both absent, and the rules reach this \
             side as well as the local one"
    );
}

// The window between asking whether the root exists and walking it. If something appears at
// that name in between, the walk fails for a reason absence does not excuse, and swallowing it
// would plan a transfer for every key into a place nothing can be written.
#[tokio::test]
async fn a_destination_that_appeared_after_the_check_still_ends_the_run() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let root = dir.path().join("appeared");
    std::fs::write(&root, b"x").expect("write the file");
    let walker = Walker::builder().build();
    let mut local = LocalDestination::new(walker.local_walk(root.clone()), &root);
    // The check ran before the file appeared.
    local.absent = true;
    let mut walk = Walk::new(Scripted::of(&["a.txt"]), local).with_roots(None, Some(root));
    let mut errors = 0;
    while let Some(next) = walk.next().await {
        if next.is_err() {
            errors += 1;
        }
    }
    assert_eq!(
        (errors, walk.is_plan_complete()),
        (1, false),
        "only a root that is genuinely missing is treated as holding no keys"
    );
}

// The merge holds a bounded number of names for followed links, and past the bound it holds
// every later delete. A name the merge has passed stops counting toward the bound.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn the_names_followed_links_hold_stay_bounded() {
    use std::os::unix::fs::symlink;

    // The root contains more links than the bound allows names for. Each link holds its own
    // name and a target the merge reaches only after `0.txt`.
    fn forward(root: &Path) {
        for n in 0..=ALIASED_NAMES_HELD / 2 {
            let target = root.join(format!("z/t{n:04}"));
            std::fs::create_dir_all(&target).expect("a target");
            symlink(&target, root.join(format!("a{n:04}"))).expect("a link");
        }
    }
    // The same number of links sit one per directory. The merge passes each link's names before
    // the walk reads the next directory.
    fn passed(root: &Path) {
        for n in 0..=ALIASED_NAMES_HELD / 2 {
            let dir = root.join(format!("d{n:04}"));
            std::fs::create_dir_all(dir.join("real")).expect("a target");
            std::fs::write(dir.join("real/x.txt"), b"x").expect("a file");
            symlink("real", dir.join("link")).expect("a link");
        }
    }

    type Case = (&'static str, fn(&Path), At);
    let cases: [Case; 2] = [
        ("forward links", forward, At::UnknownRange),
        ("passed links", passed, At::Gone),
    ];
    for (name, build, expected) in cases {
        let dir = tempfile::tempdir().expect("a temp dir");
        let root = dir.path().to_path_buf();
        build(&root);
        std::fs::write(root.join("0.txt"), b"stale").expect("a stale file");
        std::fs::write(root.join("zz.txt"), b"stale").expect("a stale file");

        let walker = Walker::builder().follow_symlinks(true).build();
        let local = LocalDestination::new(walker.local_walk(root.clone()), &root);
        let mut walk = Walk::new(Scripted::of(&[]), local).with_roots(None, Some(root));
        let mut most = 0;
        let mut stale = Vec::new();
        while let Some(next) = walk.next().await {
            most = most.max(walk.src.gap.under.len());
            let pairing = next.expect("the walk does not fail");
            if pairing.key().ends_with(".txt") && !pairing.key().contains('/') {
                stale.push((pairing.key().to_string(), at(pairing.source())));
            }
        }
        assert!(
            most <= ALIASED_NAMES_HELD,
            "{name}: {most} names held at once"
        );
        assert_eq!(
            stale,
            [
                ("0.txt".to_string(), expected),
                ("zz.txt".to_string(), expected)
            ],
            "{name}"
        );
    }
}

// A link the walk finds only after the merge reached its target leaves the plan incomplete,
// because a delete under the target may already have gone out. A key the target's name only
// extends, such as `link.txt` beside `link`, does not reach the target.
#[cfg(unix)]
#[cfg_attr(miri, ignore)]
#[tokio::test]
async fn a_link_found_after_the_merge_reached_its_target_leaves_the_plan_incomplete() {
    use std::os::unix::fs::symlink;

    fn late(root: &Path) {
        std::fs::create_dir_all(root.join("real")).expect("a target");
        std::fs::write(root.join("real/x.txt"), b"x").expect("a file");
        std::fs::create_dir(root.join("z")).expect("a directory");
        symlink("../real", root.join("z/link")).expect("a link");
    }
    fn beside(root: &Path) {
        std::fs::create_dir_all(root.join("link")).expect("a target");
        std::fs::write(root.join("link/x.txt"), b"x").expect("a file");
        std::fs::write(root.join("link.txt"), b"x").expect("a file");
        std::fs::create_dir(root.join("link.v2")).expect("a directory");
        symlink("../link", root.join("link.v2/alias")).expect("a link");
    }

    type Case = (&'static str, fn(&Path), &'static [&'static str], bool);
    let cases: [Case; 2] = [
        ("z/link -> ../real", late, &["z/link/x.txt"], false),
        (
            "link.v2/alias -> ../link beside link.txt",
            beside,
            &["link.txt", "link.v2/alias/x.txt", "link/x.txt"],
            true,
        ),
    ];
    for (name, build, source, complete) in cases {
        let dir = tempfile::tempdir().expect("a temp dir");
        let root = dir.path().to_path_buf();
        build(&root);
        let walker = Walker::builder().follow_symlinks(true).build();
        let local = LocalDestination::new(walker.local_walk(root.clone()), &root);
        let mut walk = Walk::new(Scripted::of(source), local).with_roots(None, Some(root));
        while let Some(next) = walk.next().await {
            next.expect("the walk does not fail");
        }
        assert_eq!(walk.is_plan_complete(), complete, "{name}");
    }
}

// A destination that exists and is not a directory is still fatal: nothing can be written
// into it, so carrying on would plan transfers that cannot land.
#[tokio::test]
async fn a_download_into_a_file_still_ends_the_run() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x").expect("write the file");
    let walker = Walker::builder().build();
    let local = LocalDestination::new(walker.local_walk(file.clone()), &file);
    let mut walk = Walk::new(Scripted::of(&["a.txt"]), local).with_roots(None, Some(file));
    let mut errors = 0;
    let mut pairings = 0;
    while let Some(next) = walk.next().await {
        match next {
            Ok(_) => pairings += 1,
            Err(_) => errors += 1,
        }
    }
    assert_eq!(
        (pairings, errors, walk.is_plan_complete()),
        (0, 1, false),
        "a destination that is not a directory ends the run and says the plan is partial"
    );
}

#[tokio::test]
async fn a_download_into_a_missing_root_plans_every_key() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let missing = dir.path().join("newdir");
    let walker = Walker::builder().build();
    let local = LocalDestination::new(walker.local_walk(missing.clone()), &missing);
    let listing = Scripted::of(&["a.txt", "b.txt", "c.txt"]);
    let mut walk = Walk::new(listing, local).with_roots(None, Some(missing));
    let mut pairings = 0;
    let mut errors = 0;
    while let Some(next) = walk.next().await {
        match next {
            Ok(_) => pairings += 1,
            Err(_) => errors += 1,
        }
    }
    assert_eq!(
        (pairings, errors, walk.is_plan_complete()),
        (3, 0, true),
        "a destination directory that does not exist yet holds no keys, \
             so every listed key is missing there and the plan is whole"
    );
}

#[tokio::test]
async fn a_loss_naming_the_root_itself_covers_every_key_under_it() {
    let err = StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root")),
        WalkErrorKind::DirectoryUnreadable,
        Box::from("permission denied"),
    ));
    let walk = Walk::new(
        Scripted::from(vec![Err(err), Ok(entry("zzz.txt"))]),
        Scripted::of(&["a.txt", "m.txt", "zzz.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("a.txt", At::UnknownRange, At::Here),
            ("m.txt", At::UnknownRange, At::Here),
            ("zzz.txt", At::Here, At::Here),
        ]),
        "a failure naming the root hid every key under it"
    );
}

#[test]
fn a_stretch_is_held_across_its_whole_span_and_let_go_after_it() {
    // The span is the name and everything under `name/`. What makes this worth a unit test is
    // that the two boundaries sit one byte apart: `/` is 0x2F, so `.` `-` `!` and a space all
    // extend the name into keys sorting *before* the subtree, while `0` onwards sort after it.
    let stretch = Stretch {
        under: Some("link".to_string()),
    };
    for key in ["link", "link/inner.txt", "link/a/b/c"] {
        assert!(stretch.covers(key), "{key} is one of the keys it hid");
        assert!(!stretch.passed_by(key), "{key} does not settle the span");
    }
    for key in ["link.txt", "link-old", "link!", "link 2"] {
        assert!(
            !stretch.covers(key),
            "{key} is a neighbour, not a key it hid"
        );
        assert!(
            !stretch.passed_by(key),
            "{key} sorts below the subtree, so it settles nothing"
        );
    }
    for key in ["link0", "linkfoo", "zzz.txt"] {
        assert!(!stretch.covers(key), "{key} is a neighbour");
        assert!(stretch.passed_by(key), "{key} sorts past the whole span");
    }
    assert!(
        !stretch.passed_by("a.txt"),
        "a key before the name settles nothing either"
    );
}

// Every name against every key, over an alphabet picked to straddle the delimiter: `.` sorts just
// below `/` and `0` just above, which is where releasing a stretch too early or too late shows up.
//
// Two things have to agree. `passed_by` says whether one stretch is behind a key, and
// `release_passed` drops a whole set at once by splitting it — a different calculation that has to
// reach the same answer, or a stretch is let go while the keys it hid are still ahead.
#[test]
fn releasing_a_set_of_stretches_agrees_with_asking_about_each_one() {
    let alphabet = ["!", ".", "/", "0", "a", "b", "é"];
    let mut keys = vec![String::new()];
    for _ in 0..3 {
        let longer: Vec<String> = keys
            .iter()
            .flat_map(|k| alphabet.iter().map(move |c| format!("{k}{c}")))
            .collect();
        keys.extend(longer);
    }
    keys.sort();
    keys.dedup();
    let names: Vec<&String> = keys.iter().filter(|k| !k.is_empty()).collect();

    for key in &keys {
        // What asking one at a time says.
        let mut expected: Vec<&str> = names
            .iter()
            .filter(|name| {
                !Stretch {
                    under: Some((**name).clone()),
                }
                .passed_by(key)
            })
            .map(|name| name.as_str())
            .collect();
        expected.sort_unstable();

        // What dropping the whole set at once says.
        let mut stretches = Stretches::default();
        for name in &names {
            stretches.add(Stretch {
                under: Some((*name).clone()),
            });
        }
        stretches.release_passed(key);
        let mut kept: Vec<&str> = stretches.under.iter().map(String::as_str).collect();
        kept.sort_unstable();

        assert_eq!(kept, expected, "the two disagree at key {key:?}");
    }
}

#[tokio::test]
async fn a_sibling_sorting_below_the_subtree_does_not_release_it() {
    // `link.txt` extends the lost name with `.`, which is 0x2E, and a key order puts that
    // before `/` at 0x2F — so the whole subtree is still ahead when this sibling arrives.
    // Releasing here reads every key the failure hid as absent, and delete mode removes the
    // lot. The same shape costs a whole tree when the lost name is a directory: `photos.zip`
    // beside an unreadable `photos/` would give up every object under it.
    let err = StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root/link")),
        WalkErrorKind::DirectoryUnreadable,
        Box::from("permission denied"),
    ));
    let walk = Walk::new(
        Scripted::from(vec![Err(err), Ok(entry("link.txt"))]),
        Scripted::of(&["link.txt", "link/inner.txt", "zzz.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("link.txt", At::Here, At::Here),
            ("link/inner.txt", At::UnknownRange, At::Here),
            ("zzz.txt", At::Gone, At::Here),
        ]),
        "a key that sorts below the subtree settles nothing about it"
    );
}

#[tokio::test]
async fn a_lost_name_of_unknown_type_is_covered_with_its_subtree() {
    let err = StreamError::Walk(WalkError::new(
        Some(PathBuf::from("/root/link")),
        WalkErrorKind::DirectoryUnreadable,
        Box::from("permission denied"),
    ));
    let walk = Walk::new(
        Scripted::from(vec![Err(err), Ok(entry("zzz.txt"))]),
        Scripted::of(&["link", "link/inner.txt", "zzz.txt"]),
    )
    .with_roots(Some(PathBuf::from("/root")), None);
    assert_eq!(
        drain(walk).await,
        plan(&[
            ("link", At::UnknownRange, At::Here),
            ("link/inner.txt", At::UnknownRange, At::Here),
            ("zzz.txt", At::Here, At::Here),
        ]),
        "the name whose type was unknown is as unaccounted for as its subtree"
    );
}

// A directory bucket does not list in key order, and the merge reads absence from position:
// a key the listing holds but reports late reads as a key the source does not have, which
// delete mode removes from the destination. The runtime order check cannot save it, because it
// fires when the late key finally arrives, after the removal for the key ahead of it went out.
// So the merge turns such a root down where a caller names one.
#[tokio::test]
async fn a_directory_bucket_root_is_refused_in_both_directions() {
    let dir = tempfile::tempdir().expect("a temp dir");
    for bucket in ["amzn-s3-demo-bucket--usw2-az1--x-s3", "anything--x-s3"] {
        let ctx = || {
            LocalAndBucket::builder()
                .local_root(dir.path())
                .client(a_client())
                .bucket(bucket)
                .build()
        };
        assert!(
            Walker::builder().build().uploading(ctx()).is_err(),
            "an upload accepted the directory bucket {bucket}, whose listing has no order"
        );
        assert!(
            Walker::builder().build().downloading(ctx()).is_err(),
            "a download accepted the directory bucket {bucket}"
        );
    }
    // An ordinary bucket still builds: the refusal names one kind of root.
    assert!(Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(dir.path())
                .client(a_client())
                .bucket("amzn-s3-demo-bucket")
                .build()
        )
        .is_ok());
}

#[tokio::test]
async fn a_walk_is_built_against_both_roots() {
    let dir = tempfile::tempdir().expect("a temp dir");
    let walk = Walker::builder()
        .build()
        .uploading(
            LocalAndBucket::builder()
                .local_root(dir.path())
                .client(a_client())
                .bucket("amzn-s3-demo-bucket")
                .prefix("photos/")
                .build(),
        )
        .expect("an ordinary bucket builds");
    assert_eq!(
        walk.progress(),
        Progress::Pairing,
        "a walk that has read nothing has not finished"
    );
    // Without this the local side cannot turn a failure's path into a key, and one
    // unreadable file would hold that side's whole account open.
    assert_eq!(
        walk.src.root.as_deref(),
        Some(dir.path()),
        "the side that walks a filesystem is told the root it walks"
    );
    assert_eq!(
        walk.dst.root, None,
        "a listing names its own lost keys, so it needs no root"
    );
}

#[tokio::test]
async fn the_prefix_reaches_the_listing_in_either_direction() {
    // The prefix is what roots the S3 side. Dropping it lists the whole bucket, and under
    // delete mode every object outside the prefix then reads as a key the source does not
    // have — a delete planned for each one.
    let dir = tempfile::tempdir().expect("a temp dir");
    let context = || {
        LocalAndBucket::builder()
            .local_root(dir.path())
            .client(a_client())
            .bucket("amzn-s3-demo-bucket")
            .prefix("photos/")
            .build()
    };
    let up = Walker::builder()
        .build()
        .uploading(context())
        .expect("an ordinary bucket builds");
    assert_eq!(
        up.dst.stream.prefix(),
        Some("photos/"),
        "an upload lists the destination under the prefix it was given"
    );
    let down = Walker::builder()
        .build()
        .downloading(context())
        .expect("an ordinary bucket builds");
    assert_eq!(
        down.src.stream.prefix(),
        Some("photos/"),
        "a download lists the source under the same prefix"
    );
}

#[tokio::test]
async fn a_download_walks_the_same_two_roots_the_other_way_round() {
    // The same context serves both directions, so the thing worth pinning is which side is
    // told the local root. Getting it backwards would leave a download unable to name the
    // key behind an unreadable local file, and one such file would take the whole
    // destination side out of the comparison.
    let dir = tempfile::tempdir().expect("a temp dir");
    let walk = Walker::builder()
        .build()
        .downloading(
            LocalAndBucket::builder()
                .local_root(dir.path())
                .client(a_client())
                .bucket("amzn-s3-demo-bucket")
                .build(),
        )
        .expect("an ordinary bucket builds");
    assert_eq!(
        walk.src.root, None,
        "the listing is the source here, and it names its own lost keys"
    );
    assert_eq!(
        walk.dst.root.as_deref(),
        Some(dir.path()),
        "the local tree is the destination here, so it is the side told the root"
    );
}
