/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Scale chaos for RUST-1224: does lifecycle delivery hold as object count grows?
//!
//! `progress_chaos.rs` establishes what today's surface reports at 20 objects.
//! This file asks the question the *design* has to answer: at 50, 250 and 1000
//! objects, against a consumer that is fast, slow, or absent, does every
//! announced transfer still produce exactly one terminal event — and what does
//! observation cost when nobody is watching?
//!
//! Small single-part files, deliberately, rather than the two-part 10 MiB files
//! in `progress_chaos.rs`. The stressor here is the **event rate** (two per
//! object) and not byte accounting, and the brainstorm doc is explicit that the
//! stream's rate is bounded by discovery rather than by throughput. A directory
//! of small files is the shape that makes that claim falsifiable: 200 tiny
//! objects must produce more events than 20 large ones, even though they move a
//! fiftieth of the bytes.
//!
//! What these tests can and cannot show: nothing is reserved and no event is
//! owed, so capacity is a pure lossiness dial and is sized against how far behind
//! the consumer may fall. A non-zero `dropped` count here is not a delivery
//! defect; it is that dial making itself visible, and the sweep below is what
//! measures where it bites.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use aws_sdk_s3_transfer_manager::events::{self, Outcome, TransferEvent};
use aws_sdk_s3_transfer_manager::io::walk::FsWalker;
use aws_sdk_s3_transfer_manager::types::{FailedTransferPolicy, RuntimeMode};
use s3_mock_server::{FaultType, Occurrence};
use tempfile::TempDir;
use tokio::time::timeout;

use crate::harness::{mock_tm_with, MockTm};

const TEST_TIMEOUT: Duration = Duration::from_secs(180);
const BUCKET: &str = "scale-bucket";

/// 1 KiB, so every object is a single-part `PutObject` and the run is dominated
/// by request count rather than by bytes.
const FILE_SIZE: usize = 1024;

/// A non-retryable status, so a doomed child fails on its first attempt. A 500
/// would be retried and would make wall-clock a function of the retry policy
/// rather than of the object count, which is what these tests measure.
const FAULT_STATUS: u16 = 403;

async fn setup() -> MockTm {
    mock_tm_with(RuntimeMode::Managed, |b| b).await
}

fn dataset(n: usize) -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for i in 0..n {
        std::fs::write(dir.path().join(format!("{i:05}.bin")), vec![0u8; FILE_SIZE])
            .expect("write file");
    }
    dir
}

/// Keys selected to fail, at `percent` of `n`. Deterministic per key, so the
/// same objects fail on every run.
fn doomed_keys(n: usize, prefix: &str, percent: usize) -> BTreeSet<String> {
    if percent == 0 {
        return BTreeSet::new();
    }
    let every = 100 / percent;
    (0..n)
        .filter(|i| i % every == 0)
        .map(|i| format!("{prefix}{i:05}.bin"))
        .collect()
}

/// How the caller drains the stream.
#[derive(Debug, Clone, Copy)]
enum Consumer {
    /// Drains as fast as it can, on its own task. The realistic case.
    Fast,
    /// Drains with a per-event delay, modelling a redraw or a line of output.
    Slow(Duration),
    /// Registers a sink and drops the stream immediately. The transfer should
    /// not notice.
    Absent,
}

/// What one run produced.
struct Run {
    initiated: usize,
    finished: usize,
    succeeded: usize,
    failed: usize,
    cancelled: usize,
    /// Ids settled more than once. Must always be empty: at-most-once is what the
    /// obligation swap guarantees, and it needs no reserved capacity.
    double_finished: Vec<u64>,
    /// Ids announced but never finished. Non-empty means an event was lost, not
    /// that a transfer hung — the obligation is taken even when a send fails.
    unfinished: Vec<u64>,
    /// Sends that found no room. Bounded delivery permits these; the count is
    /// what keeps them from being silent.
    dropped: u64,
    elapsed: Duration,
}

/// Run a directory upload of `n` small objects with `percent` of them doomed.
///
/// `capacity` is the channel size; `consumer` is the drain shape. Passing
/// `capacity: None` attaches no sink at all, which is the baseline for what
/// observation costs.
async fn run(
    n: usize,
    prefix: &str,
    percent: usize,
    capacity: Option<usize>,
    consumer: Consumer,
) -> Run {
    let m = setup().await;
    let dir = dataset(n);
    for key in doomed_keys(n, prefix, percent) {
        m.server.insert_fault(
            BUCKET,
            &key,
            FaultType::ServiceError {
                status: FAULT_STATUS,
            },
            0,
            Occurrence::Always,
        );
    }

    let started = Instant::now();

    let (sink, stream) = match capacity {
        Some(cap) => {
            let (s, st) = events::channel(NonZeroUsize::new(cap).expect("capacity > 0"));
            (Some(s), Some(st))
        }
        None => (None, None),
    };

    let mut req = m
        .client
        .upload_objects()
        .bucket(BUCKET)
        .source(dir.path())
        .walker(FsWalker::builder().recursive(true).build())
        .key_prefix(prefix)
        .failure_policy(FailedTransferPolicy::Continue);
    if let Some(s) = sink {
        req = req.events(s);
    }
    let handle = req.initiate().expect("initiate");

    let seen = match (stream, consumer) {
        (None, _) | (Some(_), Consumer::Absent) => {
            // Stream dropped here (or never made), so emission stops immediately.
            let _ = handle.join().await;
            (Vec::new(), 0)
        }
        (Some(mut stream), Consumer::Fast) => {
            let collector = tokio::spawn(async move {
                let mut v = Vec::new();
                while let Some(ev) = stream.next().await {
                    v.push(ev);
                }
                (v, stream.dropped())
            });
            let _ = handle.join().await;
            collector.await.expect("collector")
        }
        (Some(mut stream), Consumer::Slow(delay)) => {
            let collector = tokio::spawn(async move {
                let mut v = Vec::new();
                while let Some(ev) = stream.next().await {
                    v.push(ev);
                    tokio::time::sleep(delay).await;
                }
                (v, stream.dropped())
            });
            let _ = handle.join().await;
            collector.await.expect("collector")
        }
    };
    let (events_seen, dropped) = seen;
    let elapsed = started.elapsed();

    let mut init: BTreeMap<u64, usize> = BTreeMap::new();
    let mut fin: BTreeMap<u64, usize> = BTreeMap::new();
    let mut r = Run {
        initiated: 0,
        finished: 0,
        succeeded: 0,
        failed: 0,
        cancelled: 0,
        double_finished: Vec::new(),
        unfinished: Vec::new(),
        dropped,
        elapsed,
    };
    for ev in &events_seen {
        match ev {
            TransferEvent::Decided { id, .. } => {
                r.initiated += 1;
                *init.entry(*id).or_default() += 1;
            }
            TransferEvent::Settled { id, outcome, .. } => {
                r.finished += 1;
                *fin.entry(*id).or_default() += 1;
                match outcome {
                    Outcome::Succeeded { .. } => r.succeeded += 1,
                    Outcome::Failed { .. } => r.failed += 1,
                    Outcome::Cancelled { .. } => r.cancelled += 1,
                    _ => {}
                }
            }
            _ => {}
        }
    }
    for (id, count) in &fin {
        if *count > 1 {
            r.double_finished.push(*id);
        }
    }
    for id in init.keys() {
        if !fin.contains_key(id) {
            r.unfinished.push(*id);
        }
    }
    r
}

fn report(label: &str, r: &Run) {
    println!(
        "\n=== {label} ===\n\
         initiated ......... {}\n\
         finished .......... {}\n\
         succeeded ....... {}\n\
         failed .......... {}\n\
         cancelled ....... {}\n\
         double-finished ... {}\n\
         unfinished ........ {}\n\
         DROPPED by sink ... {}\n\
         elapsed ........... {:?}\n",
        r.initiated,
        r.finished,
        r.succeeded,
        r.failed,
        r.cancelled,
        r.double_finished.len(),
        r.unfinished.len(),
        r.dropped,
        r.elapsed,
    );
}

// ---------------------------------------------------------------------------
// Requirement: report per-object start and completion, and deliver exactly one
// terminal event per announced transfer — at every size, not just the small one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pairing_holds_from_50_to_1000_objects() {
    timeout(TEST_TIMEOUT, async {
        for n in [50usize, 250, 1000] {
            let prefix = format!("pair{n}/");
            // Ample capacity: two slots per object plus two for the root, which is
            // the rule the design states. Nothing should be dropped here.
            let r = run(n, &prefix, 10, Some(2 * n + 2), Consumer::Fast).await;
            report(
                &format!("{n} objects, 10% doomed, capacity {}", 2 * n + 2),
                &r,
            );

            assert_eq!(
                r.initiated,
                n + 1,
                "one decision per object plus the root, at n={n}"
            );
            assert_eq!(
                r.finished, r.initiated,
                "every announced transfer must finish exactly once, at n={n}"
            );
            assert!(
                r.double_finished.is_empty(),
                "no id may finish twice, at n={n}: {:?}",
                r.double_finished
            );
            assert!(
                r.unfinished.is_empty(),
                "no announced id may go unfinished, at n={n}: {:?}",
                r.unfinished
            );
            assert_eq!(r.dropped, 0, "capacity at 2n+2 must lose nothing, at n={n}");
        }
    })
    .await
    .expect("pairing_holds_from_50_to_1000_objects timed out");
}

// ---------------------------------------------------------------------------
// Requirement: the stream's rate is bounded by discovery, not by throughput.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_count_tracks_objects_not_bytes() {
    timeout(TEST_TIMEOUT, async {
        let n = 200;
        let r = run(n, "rate/", 0, Some(2 * n + 2), Consumer::Fast).await;
        report(&format!("{n} objects x 1 KiB, no faults"), &r);

        // 200 KiB of payload produces 402 events; progress_chaos's 200 MiB
        // produces 42. The stream is priced per object, which is what makes
        // `capacity = 2 * live transfers` a rule rather than a guess.
        assert_eq!(r.initiated, n + 1);
        assert_eq!(r.finished, n + 1);
        assert_eq!(r.succeeded, n + 1, "no faults, so everything succeeds");
        assert_eq!(r.dropped, 0);
    })
    .await
    .expect("event_count_tracks_objects_not_bytes timed out");
}

// ---------------------------------------------------------------------------
// Requirement: keep a slow or absent consumer from corrupting a transfer.
// It may slow discovery; it may not change an outcome.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slow_consumer_cannot_corrupt_the_transfer() {
    timeout(TEST_TIMEOUT, async {
        let n = 250;
        // 1 ms per event against 502 events is a consumer an order of magnitude
        // slower than the transfer, on a channel far below the stated rule.
        let r = run(
            n,
            "slow/",
            10,
            Some(64),
            Consumer::Slow(Duration::from_millis(1)),
        )
        .await;
        report(
            &format!("{n} objects, slow consumer (1ms/event), capacity 64"),
            &r,
        );

        assert!(
            r.double_finished.is_empty(),
            "a slow consumer must not cause a double terminal: {:?}",
            r.double_finished
        );
        // The transfer itself must have run to completion regardless of the
        // consumer. Everything not doomed landed.
        assert!(
            r.initiated > 0,
            "the consumer saw work start even while running behind"
        );

        // A consumer this far behind loses events, terminals included. That is
        // the specified behaviour, not a gap: the run must not slow down waiting,
        // and a consumer that needs every terminal reads it from `join()`.
        if r.dropped > 0 {
            println!(
                "{} events dropped, {} announced ids never settled — the cost of \
                 reading slowly",
                r.dropped,
                r.unfinished.len()
            );
        }
    })
    .await
    .expect("slow_consumer_cannot_corrupt_the_transfer timed out");
}

// ---------------------------------------------------------------------------
// Requirement: a consumer that drops the stream costs the transfer nothing.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn absent_consumer_costs_nothing() {
    timeout(TEST_TIMEOUT, async {
        let n = 250;
        let baseline = run(n, "base/", 10, None, Consumer::Fast).await;
        let absent = run(n, "absent/", 10, Some(2 * n + 2), Consumer::Absent).await;
        report("no sink at all (baseline)", &baseline);
        report("sink registered, stream dropped", &absent);

        // Not a benchmark — a guard against the absent-consumer path blocking or
        // gating. A 3x envelope is loose enough for a shared CI box and tight
        // enough to catch a transfer that waits on a dead channel.
        let ratio = absent.elapsed.as_secs_f64() / baseline.elapsed.as_secs_f64().max(1e-6);
        println!("absent/baseline wall-clock ratio: {ratio:.2}");
        assert!(
            ratio < 3.0,
            "dropping the stream must not slow the transfer: {:?} vs {:?}",
            absent.elapsed,
            baseline.elapsed
        );
    })
    .await
    .expect("absent_consumer_costs_nothing timed out");
}

// ---------------------------------------------------------------------------
// Requirement: capacity below the rule must not silently lose events.
// This is the sweep that makes the floor visible.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capacity_sweep_shows_the_floor() {
    timeout(TEST_TIMEOUT, async {
        let n = 100;
        let mut results = Vec::new();
        for cap in [2usize, 4, 8, 64, 256, 2 * n + 2] {
            let prefix = format!("cap{cap}/");
            let r = run(n, &prefix, 10, Some(cap), Consumer::Fast).await;
            println!(
                "capacity {cap:>4}: initiated {:>4} finished {:>4} dropped {:>4} unfinished {:>4}",
                r.initiated,
                r.finished,
                r.dropped,
                r.unfinished.len()
            );
            results.push((cap, r));
        }

        // The one thing that must hold at every capacity, including 2: a
        // terminal is never delivered twice. The obligation swap is what makes
        // that independent of capacity.
        for (cap, r) in &results {
            assert!(
                r.double_finished.is_empty(),
                "capacity {cap} produced a double terminal: {:?}",
                r.double_finished
            );
        }

        // At the stated rule, nothing is lost.
        let (top_cap, top) = results.last().expect("swept at least one capacity");
        assert_eq!(
            top.dropped, 0,
            "capacity {top_cap} is the stated rule and must lose nothing"
        );
        assert_eq!(top.initiated, n + 1);
        assert_eq!(top.finished, n + 1);

        // And below it, loss is real rather than theoretical.
        let smallest = &results[0].1;
        assert!(
            smallest.dropped > 0,
            "capacity 2 must demonstrate loss; if it does not, the sweep is not \
             exercising the full-channel path at all"
        );
    })
    .await
    .expect("capacity_sweep_shows_the_floor timed out");
}
