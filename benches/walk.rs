/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Filesystem walk benchmarks: cost per entry, and what ordering adds on top.
// `Native` does no sorting and is the default; `WithinDirectory` is breadth-first with each
// directory sorted; `WholeWalk` is depth-first in key order, which must descend a subtree before
// emitting a sibling that sorts after it.
//
// Caches are warm — criterion re-walks the same fixture and there is no portable way to drop page
// cache — so every duration here is a floor, and what the device would add is unmeasured.
//
// Fixtures go wherever `TMPDIR` points, since that is what `TempDir` honours. Several distributions
// back `/tmp` with RAM, so a run that does not set it may measure no device at all. Any figure
// quoted from here is worth nothing without saying which filesystem held the fixtures.

use aws_sdk_s3_transfer_manager::io::walk::{FsWalkContext, FsWalker, SortOrder};
use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// One directory holding every file.
fn build_wide(root: &Path, files: usize) -> usize {
    for i in 0..files {
        std::fs::write(root.join(format!("f{i:06}.dat")), "").unwrap();
    }
    files
}

// A single chain of directories with a few files in each.
fn build_deep(root: &Path, depth: usize, files_per_dir: usize) -> usize {
    let mut dir = root.to_path_buf();
    for d in 0..depth {
        dir = dir.join(format!("d{d}"));
        std::fs::create_dir(&dir).unwrap();
        for i in 0..files_per_dir {
            std::fs::write(dir.join(format!("f{i:04}.dat")), "").unwrap();
        }
    }
    depth * files_per_dir
}

// `fanout` subdirectories per level, `files_per_dir` files in each.
fn build_balanced(root: &Path, fanout: usize, depth: usize, files_per_dir: usize) -> usize {
    let mut count = 0;
    for i in 0..files_per_dir {
        std::fs::write(root.join(format!("f{i:04}.dat")), "").unwrap();
        count += 1;
    }
    if depth > 0 {
        for i in 0..fanout {
            let sub = root.join(format!("d{i:04}"));
            std::fs::create_dir(&sub).unwrap();
            count += build_balanced(&sub, fanout, depth - 1, files_per_dir);
        }
    }
    count
}

// Subdirectories all sort before the root's files, so a whole-walk order has to descend before it
// can emit anything: it reads the root, then the first subdirectory, and stops there. That is one
// extra directory read, not every subtree, which makes this the cheap end of the range —
// `chain_100deep` below is the expensive end, where the descent goes all the way down.
fn build_front_loaded(root: &Path, subdirs: usize, files_per_dir: usize) -> usize {
    let mut count = 0;
    for i in 0..subdirs {
        let sub = root.join(format!("aaa{i:04}"));
        std::fs::create_dir(&sub).unwrap();
        for j in 0..files_per_dir {
            std::fs::write(sub.join(format!("f{j:04}.dat")), "").unwrap();
            count += 1;
        }
    }
    for i in 0..files_per_dir {
        std::fs::write(root.join(format!("zzz{i:04}.dat")), "").unwrap();
        count += 1;
    }
    count
}

// The first key in order lives alone in a tiny directory, and everything else sorts
// after it:
//
//     root/
//       aaa/f.dat            <- first key in order, always one read away
//       zzz/d0000/f0000.dat  <- 1000 files per directory
//       zzz/d0000/...
//       zzz/d0001/...        <- one more directory per 1000 files
//
// Growing `bulk` grows the total without changing what a key-ordered walk reads before
// emitting that first entry: the root, then `aaa/`. The bulk sits under one `zzz/`, so
// the root holds exactly two entries at every size.
fn build_early_first_key(root: &Path, bulk: usize) -> usize {
    let first = root.join("aaa");
    std::fs::create_dir(&first).unwrap();
    std::fs::write(first.join("f.dat"), "").unwrap();

    let bulk_root = root.join("zzz");
    std::fs::create_dir(&bulk_root).unwrap();

    let mut count = 1;
    let mut dir_index = 0;
    while count < bulk {
        let sub = bulk_root.join(format!("d{dir_index:04}"));
        std::fs::create_dir(&sub).unwrap();
        for j in 0..1_000 {
            if count >= bulk {
                break;
            }
            std::fs::write(sub.join(format!("f{j:04}.dat")), "").unwrap();
            count += 1;
        }
        dir_index += 1;
    }
    count
}

fn walker(order: SortOrder) -> FsWalker {
    walker_with(order, false)
}

fn walker_with(order: SortOrder, follow_symlinks: bool) -> FsWalker {
    FsWalker::builder()
        .recursive(true)
        .sort_order(order)
        .follow_symlinks(follow_symlinks)
        .build()
}

// Following symlinks adds an open()+fstat per directory for cycle detection.
// Same fixture, same run, so the delta is attributable.
fn drain_follow(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    follow_symlinks: bool,
    expected: usize,
) -> usize {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker_with(SortOrder::WithinDirectory, follow_symlinks).walk(ctx);
        let mut n = 0;
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                n += 1;
            }
        }
        // The rate is computed from the fixture's own count, so a walk that gave up early would be
        // reported as a fast one. Opening a handle per directory for cycle detection is a failure
        // point this arm has and the other does not.
        assert_eq!(
            n, expected,
            "walk yielded {n} entries, fixture holds {expected}"
        );
        n
    })
}

fn walk_cycle_detection_cost(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dir = TempDir::new().unwrap();
    let count = build_balanced(dir.path(), 10, 3, 10);

    let mut group = c.benchmark_group("walk_cycle_detection");
    group.throughput(Throughput::Elements(count as u64));
    for (label, follow) in [("no_follow", false), ("follow_symlinks", true)] {
        group.bench_function(BenchmarkId::new("balanced_1110dirs", label), |b| {
            b.iter(|| black_box(drain_follow(&rt, dir.path(), follow, count)));
        });
    }
    group.finish();
}

// `expected` is the fixture's own count. Checking it here is what stops a walk that yielded nothing
// from being timed and reported as throughput: the figure would look excellent and mean nothing.
fn drain(rt: &tokio::runtime::Runtime, root: &Path, order: SortOrder, expected: usize) -> usize {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(order).walk(ctx);
        let mut n = 0;
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                n += 1;
            }
        }
        assert_eq!(
            n, expected,
            "walk yielded {n} entries, fixture holds {expected}"
        );
        n
    })
}

// Panics rather than returning a whole-walk duration when no entry ever arrives. Timing an empty
// walk and calling it first-entry latency would manufacture the rising curve the scaling benchmark
// exists to rule out.
fn time_to_first_entry(
    rt: &tokio::runtime::Runtime,
    root: &Path,
    order: SortOrder,
    expected_first: &str,
) -> Duration {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(order).walk(ctx);
        let start = Instant::now();
        let first = walk.next().await;
        let elapsed = start.elapsed();
        // Which key arrives is the measurement, not merely that one did. An error absorbed here
        // would be timed as part of the wait, and an ordering that changed which entry comes first
        // would still produce a number.
        let entry = first
            .expect("no entry arrived, so there is no first-entry time to report")
            .expect("an error before the first entry would be timed as the wait for it");
        // The relative path, not the file name: in a chain of directories every level holds a
        // `f0000.dat`, so the name alone cannot say which one arrived.
        let arrived = entry.relative_path().to_string_lossy().replace('\\', "/");
        assert_eq!(arrived, expected_first, "the wrong entry arrived first");
        elapsed
    })
}

fn walk_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut fixtures: Vec<(&str, TempDir, usize)> = Vec::new();
    for shape in 0..3 {
        let dir = TempDir::new().unwrap();
        let (label, count) = match shape {
            0 => ("wide_1dir_10k", build_wide(dir.path(), 10_000)),
            1 => ("deep_100dirs_100each", build_deep(dir.path(), 100, 100)),
            _ => (
                "balanced_f10_d3_10each",
                build_balanced(dir.path(), 10, 3, 10),
            ),
        };
        fixtures.push((label, dir, count));
    }

    let mut group = c.benchmark_group("walk_throughput");
    for (label, dir, count) in &fixtures {
        group.throughput(Throughput::Elements(*count as u64));
        for (mode, order) in [
            // What ships by default, and what this branch's base did: no sort at all. Without it the
            // two sorted arms only measure one comparison against another, since both pay a
            // per-directory sort.
            ("native", SortOrder::Native),
            ("within_directory", SortOrder::WithinDirectory),
            ("whole_walk", SortOrder::WholeWalk),
        ] {
            group.bench_with_input(BenchmarkId::new(*label, mode), &order, |b, &order| {
                b.iter(|| black_box(drain(&rt, dir.path(), order, *count)));
            });
        }
    }
    group.finish();
}

fn walk_first_entry_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let front = TempDir::new().unwrap();
    assert!(build_front_loaded(front.path(), 100, 10) > 0);

    // A chain is the other shape, and the harder one. Key order puts `d1/` before `f0000.dat`, so
    // the first key in order sits at the bottom and every directory above it is read first, where
    // ordering within a directory takes the file it already has.
    const CHAIN: usize = 100;
    let deep = TempDir::new().unwrap();
    assert!(build_deep(deep.path(), CHAIN, 100) > 0);
    let bottom = (0..CHAIN)
        .map(|d| format!("d{d}"))
        .collect::<Vec<_>>()
        .join("/");

    let mut group = c.benchmark_group("walk_first_entry");
    for (mode, order, front_first, deep_first) in [
        (
            "within_directory",
            SortOrder::WithinDirectory,
            "zzz0000.dat".to_string(),
            "d0/f0000.dat".to_string(),
        ),
        (
            "whole_walk",
            SortOrder::WholeWalk,
            "aaa0000/f0000.dat".to_string(),
            format!("{bottom}/f0000.dat"),
        ),
    ] {
        group.bench_function(BenchmarkId::new("front_loaded_100dirs", mode), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += time_to_first_entry(&rt, front.path(), order, &front_first);
                }
                total
            });
        });
        group.bench_function(BenchmarkId::new("chain_100deep", mode), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += time_to_first_entry(&rt, deep.path(), order, &deep_first);
                }
                total
            });
        });
    }
    group.finish();
}

// Whether the wait for the first entry depends on how many entries exist in total.
// Same leftmost path at every size, so a rising curve here means the walk is reading
// more than it needs to before it can emit anything.
fn walk_first_entry_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut fixtures: Vec<(usize, TempDir)> = Vec::new();
    for bulk in [1_000usize, 10_000, 50_000] {
        let dir = TempDir::new().unwrap();
        assert!(build_early_first_key(dir.path(), bulk) > 0);
        fixtures.push((bulk, dir));
    }

    let mut group = c.benchmark_group("walk_first_entry_scaling");
    for (bulk, dir) in &fixtures {
        for (mode, order) in [
            ("within_directory", SortOrder::WithinDirectory),
            ("whole_walk", SortOrder::WholeWalk),
        ] {
            group.bench_function(BenchmarkId::new(mode, bulk), |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += time_to_first_entry(&rt, dir.path(), order, "aaa/f.dat");
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

criterion_group!(
    benches,
    walk_throughput,
    walk_first_entry_latency,
    walk_first_entry_scaling,
    walk_cycle_detection_cost
);

fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
