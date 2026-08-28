/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Filesystem walk benchmarks: cost per entry, and what ordering adds on top.
// `sort` is breadth-first; `key_order` is depth-first in ListObjectsV2 key order,
// which must descend a subtree before emitting a sibling that sorts after it.
//
// Caches are warm — criterion re-walks the same fixture and there is no portable
// way to drop page cache — so these are upper bounds.

use aws_sdk_s3_transfer_manager::io::walk::{FsWalkContext, FsWalker};
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

fn walker(key_order: bool) -> FsWalker {
    FsWalker::builder()
        .recursive(true)
        .sort(true)
        .key_order(key_order)
        .build()
}

fn drain(rt: &tokio::runtime::Runtime, root: &Path, key_order: bool) -> usize {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(key_order).walk(ctx);
        let mut n = 0;
        while let Some(result) = walk.next().await {
            if result.is_ok() {
                n += 1;
            }
        }
        n
    })
}

fn time_to_first_entry(rt: &tokio::runtime::Runtime, root: &Path, key_order: bool) -> Duration {
    rt.block_on(async {
        let ctx = FsWalkContext::builder().root(root).build();
        let mut walk = walker(key_order).walk(ctx);
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
        for (mode, key_order) in [("breadth", false), ("key_order", true)] {
            group.bench_with_input(
                BenchmarkId::new(*label, mode),
                &key_order,
                |b, &key_order| {
                    b.iter(|| black_box(drain(&rt, dir.path(), key_order)));
                },
            );
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
    for (mode, key_order) in [("breadth", false), ("key_order", true)] {
        group.bench_function(BenchmarkId::new("front_loaded_100dirs", mode), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += time_to_first_entry(&rt, dir.path(), key_order);
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, walk_throughput, walk_first_entry_latency);

fn main() {
    benches();

    Criterion::default().configure_from_args().final_summary();
}
