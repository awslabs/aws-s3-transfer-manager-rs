/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Filesystem walk benchmarks: cost per entry, and what ordering adds on top.
// `WithinDirectory` is breadth-first; `WholeWalk` is depth-first in key order,
// which must descend a subtree before emitting a sibling that sorts after it.
//
// Caches are warm — criterion re-walks the same fixture and there is no portable
// way to drop page cache — so these are upper bounds.

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

// Subdirectories all sort before the root's files: worst case for key order,
// where every subtree must be read before the first entry can be emitted.
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
fn drain_follow(rt: &tokio::runtime::Runtime, root: &Path, follow_symlinks: bool) -> usize {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker_with(SortOrder::WithinDirectory, follow_symlinks).walk(ctx);
        let mut n = 0;
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                n += 1;
            }
        }
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
            b.iter(|| black_box(drain_follow(&rt, dir.path(), follow)));
        });
    }
    group.finish();
}

fn drain(rt: &tokio::runtime::Runtime, root: &Path, order: SortOrder) -> usize {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(order).walk(ctx);
        let mut n = 0;
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                n += 1;
            }
        }
        n
    })
}

fn time_to_first_entry(rt: &tokio::runtime::Runtime, root: &Path, order: SortOrder) -> Duration {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(order).walk(ctx);
        let start = Instant::now();
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                break;
            }
        }
        start.elapsed()
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
            ("within_directory", SortOrder::WithinDirectory),
            ("whole_walk", SortOrder::WholeWalk),
        ] {
            group.bench_with_input(BenchmarkId::new(*label, mode), &order, |b, &order| {
                b.iter(|| black_box(drain(&rt, dir.path(), order)));
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

    let dir = TempDir::new().unwrap();
    assert!(build_front_loaded(dir.path(), 100, 10) > 0);

    let mut group = c.benchmark_group("walk_first_entry");
    for (mode, order) in [
        ("within_directory", SortOrder::WithinDirectory),
        ("whole_walk", SortOrder::WholeWalk),
    ] {
        group.bench_function(BenchmarkId::new("front_loaded_100dirs", mode), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += time_to_first_entry(&rt, dir.path(), order);
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
                        total += time_to_first_entry(&rt, dir.path(), order);
                    }
                    total
                });
            });
        }
    }
    group.finish();
}

// Peak memory, measured by counting allocations in this bench crate. Criterion is not
// involved: it runs many iterations and allocates as it goes, which would swamp the
// figure being read.
mod peak {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PEAK: AtomicUsize = AtomicUsize::new(0);

    pub struct Tracking;

    // SAFETY: `GlobalAlloc` requires an implementor to keep the per-method contracts
    // and never to unwind. Both methods below forward their arguments to `System`
    // unchanged and return its pointer verbatim, so `System` keeps those contracts.
    // The added bookkeeping is `AtomicUsize` arithmetic, which cannot panic and
    // allocates nothing, so it neither unwinds nor re-enters this allocator.
    unsafe impl GlobalAlloc for Tracking {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: `alloc` requires its caller to pass a `layout` of non-zero
            // size, and `layout` reaches `System::alloc` unchanged, so the obligation
            // this method's caller already owes is exactly the one being discharged.
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
                PEAK.fetch_max(live, Ordering::Relaxed);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            // SAFETY: the caller guarantees `ptr` is currently allocated via this
            // allocator and that `layout` is the layout it was allocated with. Every
            // pointer this allocator hands out came from `System::alloc` under that
            // same `layout`, so the block is one `System` may free.
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    // Peak is reset to whatever is live now, so the figure covers only what follows.
    pub fn start() {
        PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    // Bytes held at the highest point since `start`, over what was already live then.
    pub fn since_start(baseline: usize) -> usize {
        PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
    }

    pub fn live() -> usize {
        LIVE.load(Ordering::Relaxed)
    }
}

#[global_allocator]
static ALLOC: peak::Tracking = peak::Tracking;

fn peak_bytes_draining(rt: &tokio::runtime::Runtime, root: &Path, order: SortOrder) -> usize {
    let baseline = peak::live();
    peak::start();
    let drained = drain(rt, root, order);
    assert!(drained > 0);
    peak::since_start(baseline)
}

// Whether ordering entries by key makes peak memory grow with the number of entries.
// Both traversals run on the same trees: breadth-first already shipped, so it is the
// baseline that says which growth belongs to key order and which belongs to reading a
// directory at all.
//
// Printed rather than asserted: the figures are what the design records.
fn report_peak_memory() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    println!("\npeak bytes while draining a walk");
    println!(
        "{:<30} {:>8} {:>12} {:>12}",
        "shape", "entries", "within_directory", "whole_walk"
    );

    // Bounded fanout, growing total: the case that has to stay flat.
    for bulk in [1_000usize, 10_000, 50_000] {
        let dir = TempDir::new().unwrap();
        let count = build_early_first_key(dir.path(), bulk);
        let breadth = peak_bytes_draining(&rt, dir.path(), SortOrder::WithinDirectory);
        let ordered = peak_bytes_draining(&rt, dir.path(), SortOrder::WholeWalk);
        println!(
            "{:<30} {count:>8} {breadth:>12} {ordered:>12}",
            "bounded_fanout_1000_per_dir"
        );
    }

    // One directory, growing: its children have to be held either way.
    for files in [1_000usize, 10_000, 50_000] {
        let dir = TempDir::new().unwrap();
        let count = build_wide(dir.path(), files);
        let breadth = peak_bytes_draining(&rt, dir.path(), SortOrder::WithinDirectory);
        let ordered = peak_bytes_draining(&rt, dir.path(), SortOrder::WholeWalk);
        println!(
            "{:<30} {count:>8} {breadth:>12} {ordered:>12}",
            "one_wide_directory"
        );
    }

    // A nested chain, deepening: this is the term key order pays for. Depth-first has
    // to keep every directory on the path open to know what comes next, so peak tracks
    // depth times fanout. Breadth-first walking the same chain holds one level.
    for depth in [25usize, 50, 100] {
        let dir = TempDir::new().unwrap();
        let count = build_deep(dir.path(), depth, 100);
        let breadth = peak_bytes_draining(&rt, dir.path(), SortOrder::WithinDirectory);
        let ordered = peak_bytes_draining(&rt, dir.path(), SortOrder::WholeWalk);
        println!(
            "{:<30} {count:>8} {breadth:>12} {ordered:>12}",
            format!("chain_depth_{depth}_100_per_dir")
        );
    }
    println!();
}

criterion_group!(
    benches,
    walk_throughput,
    walk_first_entry_latency,
    walk_first_entry_scaling,
    walk_cycle_detection_cost
);

fn main() {
    report_peak_memory();
    benches();

    Criterion::default().configure_from_args().final_summary();
}
