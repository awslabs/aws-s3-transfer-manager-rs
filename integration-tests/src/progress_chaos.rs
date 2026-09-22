/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Does the metrics surface survive partial failure?
//!
//! Written as a spike that pinned two defects as executable evidence: a
//! composite counted only its successful children's bytes, and never established a byte
//! denominator at all. Both assertions carried a note naming what they should become, and
//! both have since been inverted — these tests now guard the fixed behaviour, so a
//! regression to either defect turns them red.
//!
//! What they cover: a directory upload where a deterministic fraction of children fail
//! after having already pushed real bytes. The parent must count those bytes, and its
//! denominator must cover every entry the walk enumerated.
//!
//! Failure injection is deterministic, not random: the mock server registers
//! faults per `(bucket, key)`, so "10%" means a fault on every 10th key. Same
//! keys, same outcome, every run.
//!
//! Sizing: every file is a two-part multipart upload, and the fault fires only
//! after part 1 has already succeeded. That is the shape that exposes the byte
//! accounting — a child that fails having already pushed real bytes.

use std::collections::BTreeSet;
use std::time::Duration;

use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::types::{FailedTransferPolicy, PartSize, RuntimeMode};
use s3_mock_server::{FaultType, Occurrence};
use tempfile::TempDir;
use tokio::time::timeout;

use crate::harness::{mock_tm_with, MockTm};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

/// 5 MiB parts. This is a floor, not a preference: `multipart_threshold()` and
/// `part_size()` both clamp up to `MIN_MULTIPART_PART_SIZE_BYTES` = 5 MiB
/// (config.rs:16, :181-190), so a smaller value is silently raised and every
/// file would go single-part `PutObject` instead.
const PART: u64 = 5 * 1024 * 1024;
/// Two parts per file, so part 1 can succeed and record its bytes before part 2
/// fails the child. A single-part child records nothing on failure
/// (`record_io` runs after success, upload/transfer.rs:718), so there would be
/// no lost bytes to observe.
const FILE_SIZE: usize = (2 * PART) as usize;
const FILE_COUNT: usize = 20;
const BUCKET: &str = "chaos-bucket";
/// Matching requests that pass cleanly before the fault becomes eligible. One,
/// so `UploadPart` #1 succeeds and every later part fails.
const FAULT_SKIP: u32 = 1;

async fn setup() -> MockTm {
    mock_tm_with(RuntimeMode::Managed, |b| {
        b.part_size(PartSize::Target(PART))
            .multipart_threshold(PartSize::Target(PART))
    })
    .await
}

/// `FILE_COUNT` files of `FILE_SIZE`, named `NNN.bin`.
fn dataset() -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for i in 0..FILE_COUNT {
        std::fs::write(
            dir.path().join(format!("{i:03}.bin")),
            vec![i as u8; FILE_SIZE],
        )
        .expect("write file");
    }
    dir
}

/// Keys selected to fail, at roughly `percent` of `FILE_COUNT`.
fn doomed_keys(prefix: &str, percent: usize) -> BTreeSet<String> {
    let every = 100 / percent;
    (0..FILE_COUNT)
        .filter(|i| i % every == 0)
        .map(|i| format!("{prefix}{i:03}.bin"))
        .collect()
}

/// One run of the chaos scenario at a given failure rate.
struct Run {
    /// What `handle.metrics()` reported after `join()` returned.
    network_tx_at_join: u64,
    /// What it reported a moment later, with no further work requested.
    network_tx_after: u64,
    total_bytes: Option<u64>,
    uploaded: u64,
    failed: usize,
    /// Failures that carry no local path at all.
    failed_without_path: usize,
}

async fn run_chaos(percent: usize, prefix: &str) -> Run {
    let m = setup().await;
    let dir = dataset();
    let doomed = doomed_keys(prefix, percent);

    // UploadPart #1 succeeds and records PART bytes into the child's own
    // metrics; every later part returns 500 forever, so the child fails having
    // genuinely pushed bytes. `ServiceError` is honoured on both `put_object`
    // and `upload_part` in the mock (s3s.rs:538, :703).
    for key in &doomed {
        m.server.insert_fault(
            BUCKET,
            key,
            FaultType::ServiceError { status: 500 },
            FAULT_SKIP,
            Occurrence::Always,
        );
    }

    let handle = m
        .client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir.path())
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix(prefix)
        // Default is Abort, which cancels surviving siblings and makes join()
        // return Err. Continue is what a CLI reporting per-file results uses.
        .failure_policy(FailedTransferPolicy::Continue)
        .initiate()
        .expect("initiate");

    let output = handle
        .join()
        .await
        .expect("join should not error under Continue policy");

    let network_tx_at_join = output.metrics().network_tx;
    let total_bytes = output.metrics().total_bytes;
    // Nothing is asked to run here. If the number moves, work was still in
    // flight when the terminal metrics were published.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let network_tx_after = output.metrics().network_tx;

    let failed_without_path = output
        .failed_transfers()
        .iter()
        .filter(|f| f.source_path().is_none())
        .count();

    let run = Run {
        network_tx_at_join,
        network_tx_after,
        total_bytes,
        uploaded: output.objects_uploaded(),
        failed: output.failed_transfers().len(),
        failed_without_path,
    };

    m.handle.shutdown().await.expect("shutdown");
    run
}

/// Bytes each doomed child pushed before dying: `FAULT_SKIP` successful parts.
fn expected_lost_bytes(percent: usize) -> u64 {
    let every = 100 / percent;
    let doomed = (0..FILE_COUNT).filter(|i| i % every == 0).count() as u64;
    doomed * FAULT_SKIP as u64 * PART
}

fn expected_success_bytes(percent: usize) -> u64 {
    let every = 100 / percent;
    let ok = (0..FILE_COUNT).filter(|i| i % every != 0).count() as u64;
    ok * FILE_SIZE as u64
}

async fn assert_chaos(percent: usize, prefix: &str) {
    let r = run_chaos(percent, prefix).await;

    println!(
        "\n=== {percent}% failure rate ===\n\
         objects uploaded ........ {}\n\
         objects failed .......... {}  (of which no source_path: {})\n\
         parent network_tx ....... {}\n\
         success-only bytes ...... {}\n\
         bytes pushed by doomed .. {}  <- lost if parent == success-only\n\
         total_bytes ............. {:?}\n\
         network_tx at join ...... {}\n\
         network_tx 250ms later .. {}\n",
        r.uploaded,
        r.failed,
        r.failed_without_path,
        r.network_tx_at_join,
        expected_success_bytes(percent),
        expected_lost_bytes(percent),
        r.total_bytes,
        r.network_tx_at_join,
        r.network_tx_after,
    );

    // FIXED: a composite counts every byte its children moved, including bytes moved
    // by a child that then failed.
    //
    // This used to assert `network_tx_at_join == expected_success_bytes(percent)` —
    // the defect. The fold into the parent ran once, at reap, inside the success arm
    // only, so a child that pushed most of its object and then failed contributed
    // nothing, not late but never, and a bar built on `metrics()` could not reach
    // 100% on any run with a failure. `MetricsState` now carries a parent link and
    // `record_io` walks it, so a child's bytes reach the parent as they move and the
    // reap-time folds are gone.
    assert!(
        expected_lost_bytes(percent) > 0,
        "the scenario must actually push bytes that then fail, or it proves nothing"
    );
    // Bounds, not equality. `expected_lost_bytes` models how much a doomed child gets
    // through before its fault fires (`FAULT_SKIP` parts each), which is an upper
    // estimate: a doomed child can be cancelled, or reach its fault sooner, so the real
    // figure sits between the two. The load-bearing half is the lower bound — strictly
    // greater than success-only is what proves a failed child's bytes are counted.
    assert!(
        r.network_tx_at_join > expected_success_bytes(percent),
        "the parent must count bytes pushed by children that later failed; \
         {} == {} means the rollup regressed to a success-arm fold",
        r.network_tx_at_join,
        expected_success_bytes(percent)
    );
    assert!(
        r.network_tx_at_join <= expected_success_bytes(percent) + expected_lost_bytes(percent),
        "the parent counted {} but at most {} could have been pushed — a total above \
         that means something is counted twice",
        r.network_tx_at_join,
        expected_success_bytes(percent) + expected_lost_bytes(percent)
    );

    // FIXED: a composite establishes a byte denominator.
    //
    // This used to assert `total_bytes.is_none()` — `set_total_bytes` had three call
    // sites, all leaf transfers, so a directory transfer had no denominator at all and
    // a percentage was undefined. Both composites now accumulate the sizes of every
    // entry their walk enumerates and seal the total once enumeration is quiescent.
    //
    // Every file is enumerated whatever its eventual outcome, so the denominator is the
    // whole dataset — not the part that succeeded. That is the point: a numerator that
    // now includes failed children's bytes needs a denominator on the same basis.
    assert_eq!(
        Some(FILE_COUNT as u64 * FILE_SIZE as u64),
        r.total_bytes,
        "a sealed composite total must cover every enumerated entry"
    );

    // Quiescence probe — NOT a reproduction. `signal_terminal`'s doc says it "is
    // safe to call while in-flight work is still draining" (transfer.rs:761) and
    // the per-transfer cancellation_token is never cancelled, so a post-terminal
    // record_io is reachable in principle. This scenario does not hit it: the
    // number is stable. Under `Continue` every child is reaped before the parent
    // goes terminal, so there is nothing left in flight. Reproducing it needs the
    // Abort path, where siblings are cancelled while still executing.
    assert_eq!(
        r.network_tx_at_join, r.network_tx_after,
        "this scenario is quiescent; a change here means the probe found something"
    );
}

#[tokio::test]
async fn chaos_10_percent_failure() {
    timeout(TEST_TIMEOUT, assert_chaos(10, "ten/"))
        .await
        .expect("chaos_10_percent_failure timed out");
}

#[tokio::test]
async fn chaos_15_percent_failure() {
    // 100/15 = 6, so every 6th key fails: 4 of 20.
    timeout(TEST_TIMEOUT, assert_chaos(15, "fifteen/"))
        .await
        .expect("chaos_15_percent_failure timed out");
}

/// Separate from the byte assertions: `state.failed` holds two different
/// populations — children that failed, and walk entries that never became a
/// transfer (`FailedUpload { input: None, .. }`). A consumer computing
/// "objects attempted" from `failed_transfers().len()` cannot tell them apart.
#[tokio::test]
async fn chaos_failed_list_mixes_two_populations() {
    timeout(TEST_TIMEOUT, async {
        let r = run_chaos(10, "mixed/").await;
        println!(
            "failed={} without_path={} uploaded={}",
            r.failed, r.failed_without_path, r.uploaded
        );
        assert_eq!(
            r.uploaded + r.failed as u64,
            FILE_COUNT as u64,
            "uploaded + failed should account for every discovered entry"
        );
    })
    .await
    .expect("chaos_failed_list_mixes_two_populations timed out");
}

// ---------------------------------------------------------------------------
// Abort path. `Continue` reaps every child before the parent goes terminal, so
// it cannot exercise post-terminal mutation. `Abort` can: a child failure makes
// the parent cancel its siblings via `cancel_descriptor`, which calls
// `signal_terminal()` unconditionally (scheduler.rs:396-409) while those
// siblings may still be inside `execute`. `signal_terminal`'s own doc says it
// "is safe to call while in-flight work is still draining" (transfer.rs:761),
// and the per-transfer cancellation_token is never cancelled anywhere.
// ---------------------------------------------------------------------------

/// An abort run observed WITHOUT calling `join()`, so the handle survives and
/// `metrics()` can be sampled after the transfer has gone terminal.
struct AbortRun {
    /// `network_tx` at the first poll that reported a terminal status.
    tx_at_terminal: u64,
    /// `network_tx` after a further wait, with nothing new requested.
    tx_settled: u64,
    status_at_terminal: String,
    polls_to_terminal: u32,
}

async fn run_abort(percent: usize, prefix: &str) -> AbortRun {
    let m = setup().await;
    let dir = dataset();
    for key in doomed_keys(prefix, percent) {
        m.server.insert_fault(
            BUCKET,
            &key,
            FaultType::ServiceError { status: 500 },
            FAULT_SKIP,
            Occurrence::Always,
        );
    }

    let handle = m
        .client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir.path())
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix(prefix)
        // Default, stated explicitly: one child failure aborts the operation.
        .failure_policy(FailedTransferPolicy::Abort)
        .initiate()
        .expect("initiate");

    // Deliberately NOT join(): it takes `self`, and the whole point is to hold
    // the handle across the terminal transition and keep reading counters.
    let mut polls = 0u32;
    let (tx_at_terminal, status_at_terminal) = loop {
        polls += 1;
        let st = handle.status();
        if st != aws_sdk_s3_transfer_manager::types::TransferStatus::Active {
            break (handle.metrics().network_tx, format!("{st:?}"));
        }
        assert!(polls < 60_000, "transfer never reached a terminal status");
        tokio::time::sleep(Duration::from_millis(1)).await;
    };

    // Nothing is asked to run in this window. Any movement is work that was
    // still in flight when the terminal status became visible.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let tx_settled = handle.metrics().network_tx;

    drop(handle);
    m.handle.shutdown().await.expect("shutdown");
    AbortRun {
        tx_at_terminal,
        tx_settled,
        status_at_terminal,
        polls_to_terminal: polls,
    }
}

/// Under `Abort`, a parent's counters keep moving after it reports terminal — and that is
/// correct rather than a leak.
///
/// `signal_terminal` is safe to call while in-flight work drains and the per-transfer token is
/// never cancelled, so a child's `record_io` can land after the parent is `Failed`. Because the
/// rollup walks the parent chain as bytes move rather than folding at reap, those late bytes
/// reach the parent too. A consumer reading `metrics()` after the terminal therefore sees a
/// *larger* number, which is what §3's byte-ordering invariant promises and what makes
/// `metrics()` safe to read at any time — the alternative, freezing the parent at terminal,
/// would under-report every aborted run by whatever was in flight.
///
/// **Two assertions are deliberately absent, and both have been tried.** `tx_settled >=
/// tx_at_terminal` compares two reads of a counter that only ever `fetch_add`s, so it cannot fail.
/// `moved > 0` depends on work still being in flight at the terminal, which is a scheduling
/// outcome, so it fails on a healthy run under load. The post-terminal number is printed instead;
/// what is asserted is a floor and a ceiling, and both can fail. The body says which is which.
///
/// The test was also once named `..._parent_aggregate_stays_zero`, describing a parent that stayed
/// at 0 — true when the fold ran at reap inside the success arm, false since the rollup landed:
/// two runs read 60 MiB and 10 MiB at terminal, with 30 MiB and 60 MiB arriving after.
///
/// Those figures vary by run because where the abort lands relative to the drain is a race, which
/// is why the assertions below are a floor, a ceiling and a non-zero rather than any equality.
/// The floor is not a guess: a doomed child fails only after pushing `FAULT_SKIP` parts, so the
/// abort is *caused by* a child that has already moved bytes, and `tx_at_terminal` cannot be 0
/// unless the rollup itself is broken.
#[tokio::test]
async fn chaos_abort_parent_counts_bytes_that_land_after_terminal() {
    timeout(TEST_TIMEOUT, async {
        let r = run_abort(15, "abort/").await;
        let moved = r.tx_settled.saturating_sub(r.tx_at_terminal);
        println!(
            "\n=== abort path, 15% seeded failures, no join() ===\n\
             status at first terminal poll .. {}\n\
             polls to terminal .............. {}\n\
             network_tx at terminal ......... {}\n\
             network_tx 500ms later ......... {}\n\
             moved AFTER terminal ........... {} bytes\n",
            r.status_at_terminal, r.polls_to_terminal, r.tx_at_terminal, r.tx_settled, moved,
        );

        // The rollup reaches the parent on the abort path, not only the happy one. This is the
        // assertion that fails if `record_io` stops walking the parent chain -- reverting that
        // walk reads 0 here, which is exactly what the pre-rollup defect looked like.
        assert!(
            r.tx_at_terminal > 0,
            "a parent must count its children's bytes as they move, including under Abort; \
             0 means the rollup regressed to a reap-time fold"
        );

        // `moved` is printed and deliberately not asserted, in either direction.
        //
        // Non-zero cannot be asserted: whether any bytes are still in flight at the terminal is a
        // scheduling outcome, so a fixed-delay sample cannot tell "the counters froze" from "the
        // drain finished before we looked". Under CPU starvation the second happens and
        // `moved > 0` fails on a healthy run.
        //
        // `tx_settled >= tx_at_terminal` cannot be asserted either, for the opposite reason: both
        // are reads of a counter that only ever `fetch_add`s, so it cannot fail and proves
        // nothing. Reaching for it as the "safe" alternative is how this test lost its teeth once
        // already.
        //
        // So the property is observed here and pinned elsewhere: the two assertions around this
        // comment are a floor and a ceiling, and both can fail.

        // Ceiling: the parent cannot report more payload than the dataset contains. Loose by
        // design -- an aborted run moves well under the total -- but it is the guard against a
        // reap-time fold being re-added *beside* the live rollup, which counts every byte twice.
        let dataset = FILE_COUNT as u64 * FILE_SIZE as u64;
        assert!(
            r.tx_settled <= dataset,
            "parent reported {} bytes against a {dataset}-byte dataset; over the total means \
             bytes are being counted twice",
            r.tx_settled
        );
    })
    .await
    .expect("chaos_abort_parent_counts_bytes_that_land_after_terminal timed out");
}

// ---------------------------------------------------------------------------
// §6.1 — can a consumer do `mv --recursive` safely from TODAY's surface?
//
// Exactly-once delivery would exist to serve delete-on-success. If that is
// already safe post-hoc from `failed_transfers()`, per-entry events buy
// incrementality rather than correctness, and nothing on the stream has to be
// promised. These two tests answer that rather than arguing it.
//
// Safety has two failure modes, and they are not symmetric:
//   DESTRUCTIVE — delete a source whose object is not in S3. Unrecoverable.
//   STRANDING   — leave a source whose object IS in S3. Recoverable, untidy.
// ---------------------------------------------------------------------------

/// What a naive `mv` would do, and whether it was right.
struct MvOutcome {
    would_delete: usize,
    /// Files it would delete that are NOT in the bucket. Any non-zero is data loss.
    destructive: Vec<String>,
    /// Files it would keep that ARE in the bucket.
    stranded: Vec<String>,
}

async fn simulate_mv(percent: usize, prefix: &str, policy: FailedTransferPolicy) -> MvOutcome {
    let m = setup().await;
    let dir = dataset();
    let doomed = doomed_keys(prefix, percent);
    for key in &doomed {
        m.server.insert_fault(
            BUCKET,
            key,
            FaultType::ServiceError { status: 500 },
            FAULT_SKIP,
            Occurrence::Always,
        );
    }

    let handle = m
        .client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir.path())
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix(prefix)
        .failure_policy(policy.clone())
        .initiate()
        .expect("initiate");

    // Every source today's surface tells a consumer did not succeed. That is the
    // failure list and nothing else: a child cancelled by an abort carries no
    // per-object error, so it appears in no list at all. Under `Abort` the
    // information moves to the error, but it is still only the failures.
    //
    // This is the gap the event stream exists to close, and reading only what the
    // surface actually offers is what makes it measurable here: a cancelled source
    // is indistinguishable from a successful one, so an `mv` that deletes on
    // "not reported failed" deletes a source whose object never landed.
    let failed_paths: BTreeSet<String> = match handle.join().await {
        Ok(out) => out
            .failed_transfers()
            .iter()
            .filter_map(|f| f.source_path().map(|p| p.display().to_string()))
            .collect(),
        Err(e) => e
            .failed_uploads()
            .map(|fs| {
                fs.iter()
                    .filter_map(|f| f.source_path().map(|p| p.display().to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    };

    // What actually landed in S3.
    let in_bucket: BTreeSet<String> = m
        .server
        .list_objects(BUCKET, Some(prefix))
        .await
        .expect("list")
        .into_iter()
        .map(|e| e.key)
        .collect();

    // The naive rule: delete every source we were not told failed.
    let mut would_delete = 0usize;
    let mut destructive = Vec::new();
    let mut stranded = Vec::new();
    for i in 0..FILE_COUNT {
        let path = dir.path().join(format!("{i:03}.bin")).display().to_string();
        let key = format!("{prefix}{i:03}.bin");
        let told_failed = failed_paths.contains(&path);
        let landed = in_bucket.contains(&key);
        if told_failed {
            if landed {
                stranded.push(key);
            }
        } else {
            would_delete += 1;
            if !landed {
                destructive.push(key);
            }
        }
    }

    m.handle.shutdown().await.expect("shutdown");
    MvOutcome {
        would_delete,
        destructive,
        stranded,
    }
}

#[tokio::test]
async fn mv_from_todays_surface_is_safe_under_continue() {
    timeout(TEST_TIMEOUT, async {
        let r = simulate_mv(15, "mvcont/", FailedTransferPolicy::Continue).await;
        println!(
            "\n=== mv via failed_transfers(), Continue policy ===\n\
             would delete ..... {}\n\
             DESTRUCTIVE ...... {} {:?}\n\
             stranded ......... {} {:?}\n",
            r.would_delete,
            r.destructive.len(),
            r.destructive,
            r.stranded.len(),
            r.stranded,
        );
        assert!(
            r.destructive.is_empty(),
            "post-hoc mv under Continue must never delete a source that is not in S3"
        );
    })
    .await
    .expect("mv_from_todays_surface_is_safe_under_continue timed out");
}

// ---------------------------------------------------------------------------
// Do per-entry events reach the sink, and what does a slow consumer cost?
//
// Delivery is bounded and lossy: the sink sends with `try_send` and nothing is
// reserved, so `stream.dropped()` is the exact price of a consumer that reads too
// slowly. The pairing assertion is the real test: every `Decided` must be matched
// by exactly one `Settled` for the same id, and no id may settle twice.
// ---------------------------------------------------------------------------

use aws_sdk_s3_transfer_manager::events::{self, Outcome, TransferEvent};
use std::collections::BTreeMap;

/// What the consumer saw on the stream.
struct Drained {
    initiated: usize,
    finished: usize,
    succeeded: usize,
    failed: usize,
    cancelled: usize,
    /// Ids finished more than once. Must always be empty.
    double_finished: Vec<u64>,
    /// Ids announced but never finished.
    unfinished: Vec<u64>,
    /// Settled without a matching Decided.
    orphan_finished: Vec<u64>,
    dropped: u64,
}

/// Run the chaos scenario with an events sink attached.
///
/// `capacity` is the channel size. `drain_live` selects the consumer shape:
/// `true` drains concurrently (the realistic case), `false` holds the stream and
/// reads nothing until the transfer is over (the adversarial case).
async fn run_with_events(
    percent: usize,
    prefix: &str,
    capacity: usize,
    drain_live: bool,
    policy: FailedTransferPolicy,
) -> Drained {
    let m = setup().await;
    let dir = dataset();
    for key in doomed_keys(prefix, percent) {
        m.server.insert_fault(
            BUCKET,
            &key,
            FaultType::ServiceError { status: 500 },
            FAULT_SKIP,
            Occurrence::Always,
        );
    }

    let (sink, mut stream) = events::channel(std::num::NonZeroUsize::new(capacity).unwrap());

    let handle = m
        .client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir.path())
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix(prefix)
        .failure_policy(policy)
        .events(sink)
        .initiate()
        .expect("initiate");

    let collected = if drain_live {
        // Drain on a separate task while the transfer runs.
        let collector = tokio::spawn(async move {
            let mut seen = Vec::new();
            while let Some(ev) = stream.next().await {
                seen.push(ev);
            }
            (seen, stream.dropped())
        });
        let _ = handle.join().await;
        // Every sink clone lives on the transfer, so `next()` returns None once
        // the transfer is dropped and its lifecycles go with it.
        collector.await.expect("collector")
    } else {
        let _ = handle.join().await;
        let mut seen = Vec::new();
        // `Empty` and `Disconnected` both end this loop; the transfer has already
        // joined, so there is nothing to come back for.
        while let Ok(ev) = stream.try_next() {
            seen.push(ev);
        }
        (seen, stream.dropped())
    };
    let (seen, dropped) = collected;

    let mut init_ids: BTreeMap<u64, usize> = BTreeMap::new();
    let mut fin_ids: BTreeMap<u64, usize> = BTreeMap::new();
    let mut d = Drained {
        initiated: 0,
        finished: 0,
        succeeded: 0,
        failed: 0,
        cancelled: 0,
        double_finished: Vec::new(),
        unfinished: Vec::new(),
        orphan_finished: Vec::new(),
        dropped,
    };
    for ev in &seen {
        match ev {
            TransferEvent::Decided { id, .. } => {
                d.initiated += 1;
                *init_ids.entry(*id).or_default() += 1;
            }
            TransferEvent::Settled { id, outcome, .. } => {
                d.finished += 1;
                *fin_ids.entry(*id).or_default() += 1;
                match outcome {
                    Outcome::Succeeded { .. } => d.succeeded += 1,
                    Outcome::Failed { .. } => d.failed += 1,
                    Outcome::Cancelled { .. } => d.cancelled += 1,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    for (id, n) in &fin_ids {
        if *n > 1 {
            d.double_finished.push(*id);
        }
        if !init_ids.contains_key(id) {
            d.orphan_finished.push(*id);
        }
    }
    for id in init_ids.keys() {
        // id 0 is the spike's placeholder for an entry that never got a child.
        if *id != 0 && !fin_ids.contains_key(id) {
            d.unfinished.push(*id);
        }
    }

    m.handle.shutdown().await.expect("shutdown");
    d
}

fn report(label: &str, d: &Drained) {
    println!(
        "\n=== {label} ===\n\
         initiated ......... {}\n\
         finished .......... {}\n\
           succeeded ....... {}\n\
           failed .......... {}\n\
           cancelled ....... {}\n\
         double-finished ... {:?}\n\
         unfinished ........ {:?}\n\
         orphan finished ... {:?}\n\
         DROPPED by sink ... {}\n",
        d.initiated,
        d.finished,
        d.succeeded,
        d.failed,
        d.cancelled,
        d.double_finished,
        d.unfinished,
        d.orphan_finished,
        d.dropped,
    );
}

/// Generous capacity, consumer draining concurrently. Nothing should be lost.
#[tokio::test]
async fn events_reach_sink_10_percent() {
    timeout(TEST_TIMEOUT, async {
        let d = run_with_events(10, "ev10/", 256, true, FailedTransferPolicy::Continue).await;
        report("events, 10% failures, capacity 256, live drain", &d);

        // 1 root + 20 children.
        assert_eq!(d.initiated, FILE_COUNT + 1, "one root plus every child");
        assert_eq!(d.finished, d.initiated, "every announced transfer finished");
        assert!(d.double_finished.is_empty(), "exactly-once terminal");
        assert!(d.unfinished.is_empty(), "no announced transfer left owing");
        assert!(
            d.orphan_finished.is_empty(),
            "no terminal without a decision"
        );
        assert_eq!(d.dropped, 0, "nothing lost at capacity 256");
        // 18 children succeed + the root; 2 children fail.
        assert_eq!(d.failed, 2, "two children failed");
        assert_eq!(d.succeeded, FILE_COUNT - 2 + 1, "18 children plus the root");
    })
    .await
    .expect("events_reach_sink_10_percent timed out");
}

#[tokio::test]
async fn events_reach_sink_15_percent() {
    timeout(TEST_TIMEOUT, async {
        let d = run_with_events(15, "ev15/", 256, true, FailedTransferPolicy::Continue).await;
        report("events, 15% failures, capacity 256, live drain", &d);

        assert_eq!(d.initiated, FILE_COUNT + 1);
        assert_eq!(d.finished, d.initiated);
        assert!(d.double_finished.is_empty(), "exactly-once terminal");
        assert!(d.unfinished.is_empty());
        assert_eq!(d.dropped, 0);
        assert_eq!(d.failed, 4, "four children failed");
        assert_eq!(d.succeeded, FILE_COUNT - 4 + 1);
    })
    .await
    .expect("events_reach_sink_15_percent timed out");
}

/// The adversarial consumer: holds the stream, reads nothing until the end.
/// With no reservation and a small channel, this is where events are lost.
#[tokio::test]
async fn events_are_lost_when_channel_is_small_and_undrained() {
    timeout(TEST_TIMEOUT, async {
        let d = run_with_events(15, "evsmall/", 4, false, FailedTransferPolicy::Continue).await;
        report("events, 15% failures, capacity 4, NO live drain", &d);

        // The measurement, not a wish: at capacity 4 with no drain, sends fail.
        // Loss is what bounded delivery buys, and `dropped()` is what keeps it
        // from being silent.
        assert!(
            d.dropped > 0,
            "capacity 4 with no drain must lose events -- if it does not, the \
             scenario is not exercising the full-channel path"
        );
        // Even when events are lost, no id may be finished twice.
        assert!(
            d.double_finished.is_empty(),
            "the obligation swap must hold even when sends fail"
        );
        assert!(
            d.orphan_finished.is_empty(),
            "a terminal never precedes its own decision"
        );
    })
    .await
    .expect("events_are_lost_when_channel_is_small_and_undrained timed out");
}

/// The default policy, where §2 showed per-object outcomes go missing today.
/// Do events still account for every child?
#[tokio::test]
async fn events_under_abort_policy() {
    timeout(TEST_TIMEOUT, async {
        let d = run_with_events(15, "evabort/", 256, true, FailedTransferPolicy::Abort).await;
        report("events, 15% failures, ABORT policy (the default)", &d);

        assert!(d.double_finished.is_empty(), "exactly-once terminal");
        assert!(
            d.unfinished.is_empty(),
            "every announced child must still reach a terminal event under Abort"
        );
        assert_eq!(d.dropped, 0, "nothing lost at capacity 256");
        println!(
            "accounted: {} initiated / {} finished ({} ok, {} failed, {} cancelled)",
            d.initiated, d.finished, d.succeeded, d.failed, d.cancelled
        );
    })
    .await
    .expect("events_under_abort_policy timed out");
}

#[tokio::test]
async fn mv_from_todays_surface_under_abort() {
    timeout(TEST_TIMEOUT, async {
        let r = simulate_mv(15, "mvabort/", FailedTransferPolicy::Abort).await;
        println!(
            "\n=== mv via failed_uploads(), Abort policy (the DEFAULT) ===\n\
             would delete ..... {}\n\
             DESTRUCTIVE ...... {} {:?}\n\
             stranded ......... {} {:?}\n",
            r.would_delete,
            r.destructive.len(),
            r.destructive,
            r.stranded.len(),
            r.stranded,
        );
        // The motivating defect for a per-object event stream, asserted rather than
        // asserted away: under the DEFAULT policy, a consumer doing `mv` from
        // today's surface deletes sources whose objects never reached S3.
        //
        // The cause is that an abort cancels siblings, and a cancelled child appears
        // in no list a caller can read -- it carries no per-object error, so
        // `failed_uploads()` does not name it and nothing else does either. "Not
        // reported failed" is therefore not the same as "succeeded", and every
        // consumer that assumes it is loses data.
        //
        // Asserted as non-empty on purpose. If this ever starts passing empty, the
        // surface grew a way to see cancelled children and this test should become
        // the safety assertion instead.
        assert!(
            !r.destructive.is_empty(),
            "expected today's surface to be unsafe under Abort; if it is now safe, \
             the cancelled-child gap was closed and this assertion should flip"
        );
    })
    .await
    .expect("mv_from_todays_surface_under_abort timed out");
}
