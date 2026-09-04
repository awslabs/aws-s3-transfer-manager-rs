/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Buffer-pool mechanism benchmarks.
//!
//! The groups isolate acquisition, mutable collection, immutable ownership,
//! fragmented-pool search, and contention through the public memory API.
//! Pools are prepared before timed loops, so ordinary results exclude virtual
//! range reservation and initial block preparation unless a scenario states
//! otherwise.
//!
//! Payload initialization is included where the scenario models response
//! collection. Lifecycle groups use batched setup to time only the named
//! ownership transition. Exact allocator work is asserted separately by the
//! ignored private arena probe; these benchmarks measure elapsed cost without
//! exposing private counters to the benchmark crate.

use std::hint::black_box;
use std::io::IoSlice;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use aws_sdk_s3_transfer_manager::memory::{
    BufferPool, MemoryBudgetConfig, PooledBufMut, Reservation, SegmentedBytes,
};
use bytes::{Buf, BufMut};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const GIB: usize = 1024 * MIB;
const FRAME_BYTES: usize = 16 * KIB;
const PART_BYTES: usize = 8 * MIB;
const BLOCK_BYTES: usize = 128 * MIB;

/// A fully prepared pool with one reservation spanning its configured capacity.
///
/// Optional occupancy remains live for the state lifetime, creating a stable
/// packed prefix without including setup in the timed acquisition.
struct PreparedPool {
    pool: BufferPool,
    reservation: Reservation,
    _occupied: Option<PooledBufMut>,
}

impl PreparedPool {
    /// Prepares an empty pool and retains authority for its complete capacity.
    fn new(capacity: usize) -> Self {
        Self::with_occupancy_fraction(capacity, 0, 1)
    }

    /// Prepares a pool with `occupied_parts / total_parts` held live.
    ///
    /// The retained buffer keeps a packed prefix occupied for the complete
    /// benchmark state lifetime. The requested capacity must divide cleanly
    /// enough for the scenario's block-aligned fractions.
    fn with_occupancy_fraction(capacity: usize, occupied_parts: usize, total_parts: usize) -> Self {
        assert!(total_parts != 0 && occupied_parts < total_parts);
        let pool = BufferPool::builder()
            .memory_budget(MemoryBudgetConfig::Limit(capacity))
            .build()
            .expect("benchmark pool should build");
        assert_eq!(
            pool.carrier_size(),
            FRAME_BYTES,
            "benchmark assumes the production 16 KiB carrier"
        );
        let configured = usize::try_from(pool.metrics().configured_capacity_bytes())
            .expect("configured capacity should fit usize");
        assert_eq!(configured, capacity);
        let reservation = pool
            .try_reserve(configured)
            .expect("benchmark reservation should not fail")
            .expect("empty benchmark pool should grant immediately");
        assert_eq!(
            pool.metrics().prepared_capacity_bytes(),
            configured as u64,
            "block-aligned capacity should prepare exactly"
        );

        let occupied_bytes = configured / total_parts * occupied_parts;
        let occupied = (occupied_bytes != 0).then(|| {
            pool.acquire(&reservation, occupied_bytes)
                .expect("benchmark occupancy should fit its reservation")
        });
        assert_eq!(
            pool.metrics().charged_capacity_bytes(),
            occupied_bytes as u64
        );

        Self {
            pool,
            reservation,
            _occupied: occupied,
        }
    }
}

/// Prepares a pool without retaining a reservation.
fn prepared_unreserved_pool(capacity: usize) -> BufferPool {
    let pool = BufferPool::builder()
        .memory_budget(MemoryBudgetConfig::Limit(capacity))
        .build()
        .expect("benchmark pool should build");
    let prepared = pool
        .acquire_unreserved(capacity)
        .expect("benchmark preparation should succeed");
    drop(prepared);
    assert_eq!(pool.metrics().prepared_capacity_bytes(), capacity as u64);
    pool
}

/// Measures acquisition and final-owner return from already prepared storage.
///
/// Carrier-sized requests expose fixed per-acquisition overhead. Part-sized
/// requests show how that overhead amortizes when one claim spans 512 carriers.
/// Reserved and unreserved paths use equivalent prepared 128 MiB pools.
fn benchmark_acquire_release(c: &mut Criterion) {
    let reserved = PreparedPool::new(BLOCK_BYTES);
    let unreserved = prepared_unreserved_pool(BLOCK_BYTES);
    let mut group = c.benchmark_group("buffer_pool/acquire_release");

    for (name, bytes) in [("carrier_16k", FRAME_BYTES), ("part_8m", PART_BYTES)] {
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_with_input(BenchmarkId::new("reserved", name), &bytes, |b, &bytes| {
            b.iter(|| {
                let buffer = reserved
                    .pool
                    .acquire(&reserved.reservation, bytes)
                    .expect("reserved benchmark acquisition should succeed");
                black_box(buffer.capacity());
                drop(buffer);
            });
        });
        group.bench_with_input(BenchmarkId::new("unreserved", name), &bytes, |b, &bytes| {
            b.iter(|| {
                let buffer = unreserved
                    .acquire_unreserved(bytes)
                    .expect("unreserved benchmark acquisition should succeed");
                black_box(buffer.capacity());
                drop(buffer);
            });
        });
    }
    group.finish();
}

/// Collects one part through 512 carrier-sized growth operations.
fn collect_incrementally(pool: &BufferPool, reservation: &Reservation) -> SegmentedBytes {
    let mut buffer = pool
        .acquire(reservation, FRAME_BYTES)
        .expect("initial frame acquisition should succeed");
    for frame in 0..PART_BYTES / FRAME_BYTES {
        if frame != 0 {
            buffer
                .reserve(FRAME_BYTES)
                .expect("incremental frame growth should succeed");
        }
        buffer.put_bytes(0x5a, FRAME_BYTES);
    }
    buffer.freeze()
}

/// Acquires and initializes one complete part before publication.
fn initialized_upfront(pool: &BufferPool, reservation: &Reservation) -> PooledBufMut {
    let mut buffer = pool
        .acquire(reservation, PART_BYTES)
        .expect("part acquisition should succeed");
    buffer.put_bytes(0x5a, PART_BYTES);
    buffer
}

/// Acquires, initializes, and freezes one complete part as one timed operation.
fn collect_upfront(pool: &BufferPool, reservation: &Reservation) -> SegmentedBytes {
    initialized_upfront(pool, reservation).freeze()
}

/// Collects two parts by alternating each carrier-sized growth operation.
///
/// Alternation models producers whose allocations interleave in the shared
/// arena and therefore cannot rely on one producer receiving adjacent carriers.
fn collect_interleaved_incrementally(
    pool: &BufferPool,
    reservation: &Reservation,
) -> [SegmentedBytes; 2] {
    let mut first = pool
        .acquire(reservation, FRAME_BYTES)
        .expect("first frame acquisition should succeed");
    let mut second = pool
        .acquire(reservation, FRAME_BYTES)
        .expect("second frame acquisition should succeed");
    for frame in 0..PART_BYTES / FRAME_BYTES {
        if frame != 0 {
            first
                .reserve(FRAME_BYTES)
                .expect("first incremental growth should succeed");
            second
                .reserve(FRAME_BYTES)
                .expect("second incremental growth should succeed");
        }
        first.put_bytes(0x5a, FRAME_BYTES);
        second.put_bytes(0xa5, FRAME_BYTES);
    }
    [first.freeze(), second.freeze()]
}

/// Acquires two complete parts before either is initialized.
fn collect_pair_upfront(pool: &BufferPool, reservation: &Reservation) -> [SegmentedBytes; 2] {
    let mut first = pool
        .acquire(reservation, PART_BYTES)
        .expect("first part acquisition should succeed");
    let mut second = pool
        .acquire(reservation, PART_BYTES)
        .expect("second part acquisition should succeed");
    first.put_bytes(0x5a, PART_BYTES);
    second.put_bytes(0xa5, PART_BYTES);
    [first.freeze(), second.freeze()]
}

/// Returns the immutable presentation-segment count without consuming bytes.
fn segment_count(buffer: &SegmentedBytes) -> usize {
    let mut slices = std::iter::repeat_with(|| IoSlice::new(&[]))
        .take(PART_BYTES / FRAME_BYTES)
        .collect::<Vec<_>>();
    let count = buffer.chunks_vectored(&mut slices);
    assert_eq!(buffer.remaining(), PART_BYTES);
    count
}

/// Compares frame-driven growth with complete-response acquisition.
///
/// Each timed iteration includes acquisition, payload initialization, freeze,
/// immutable segment construction, and final owner return. The preflight
/// segment counts report the presentation topology exercised by each timing.
fn benchmark_collector_strategy(c: &mut Criterion) {
    let state = PreparedPool::new(BLOCK_BYTES);
    let incremental_segments =
        segment_count(&collect_incrementally(&state.pool, &state.reservation));
    let upfront_segments = segment_count(&collect_upfront(&state.pool, &state.reservation));
    let interleaved = collect_interleaved_incrementally(&state.pool, &state.reservation);
    let interleaved_segments = interleaved.each_ref().map(segment_count);
    let upfront_pair = collect_pair_upfront(&state.pool, &state.reservation);
    let upfront_pair_segments = upfront_pair.each_ref().map(segment_count);
    eprintln!(
        "buffer-pool collector layout: incremental_segments={incremental_segments} \
         upfront_segments={upfront_segments} \
         interleaved_segments={interleaved_segments:?} \
         upfront_pair_segments={upfront_pair_segments:?}"
    );

    let mut group = c.benchmark_group("buffer_pool/collect_part");
    group.sample_size(20);

    group.throughput(Throughput::Bytes(PART_BYTES as u64));
    group.bench_function("incremental_16k_growth", |b| {
        b.iter(|| {
            let buffer = collect_incrementally(&state.pool, &state.reservation);
            black_box(buffer.remaining());
            drop(buffer);
        });
    });
    group.bench_function("upfront_8m_acquire", |b| {
        b.iter(|| {
            let buffer = collect_upfront(&state.pool, &state.reservation);
            black_box(buffer.remaining());
            drop(buffer);
        });
    });

    group.throughput(Throughput::Bytes((2 * PART_BYTES) as u64));
    group.bench_function("interleaved_16k_growth_pair", |b| {
        b.iter(|| {
            let buffers = collect_interleaved_incrementally(&state.pool, &state.reservation);
            black_box(buffers.iter().map(Buf::remaining).sum::<usize>());
            drop(buffers);
        });
    });
    group.bench_function("upfront_8m_acquire_pair", |b| {
        b.iter(|| {
            let buffers = collect_pair_upfront(&state.pool, &state.reservation);
            black_box(buffers.iter().map(Buf::remaining).sum::<usize>());
            drop(buffers);
        });
    });
    group.finish();
}

/// Measures ownership transitions after an 8 MiB payload is initialized.
///
/// Batched setup is excluded. The cases time whole-value freeze, publication
/// as 512 carrier prefixes, and destruction of all immutable owners.
fn benchmark_buffer_lifecycle(c: &mut Criterion) {
    let state = PreparedPool::new(BLOCK_BYTES);
    let mut group = c.benchmark_group("buffer_pool/lifecycle");
    group.throughput(Throughput::Bytes(PART_BYTES as u64));
    group.sample_size(20);

    group.bench_function("freeze_8m", |b| {
        b.iter_batched(
            || initialized_upfront(&state.pool, &state.reservation),
            PooledBufMut::freeze,
            BatchSize::PerIteration,
        );
    });
    group.bench_function("publish_16k_prefixes_8m", |b| {
        b.iter_batched(
            || initialized_upfront(&state.pool, &state.reservation),
            |mut buffer| {
                let mut views = Vec::with_capacity(PART_BYTES / FRAME_BYTES);
                while !buffer.is_empty() {
                    let initialized = buffer.initialized_chunk().len();
                    views.push(buffer.publish_prefix(initialized));
                }
                black_box(views)
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("drop_frozen_8m", |b| {
        b.iter_batched(
            || collect_upfront(&state.pool, &state.reservation),
            drop,
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Measures one-carrier acquisition as registry size grows under stable packing.
///
/// Seven eighths of each pool remain occupied. The timed request therefore
/// exercises optimistic misses and serialized reuse against 8 through 256
/// registered blocks without measuring pool construction or occupancy setup.
fn benchmark_packed_pool_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool/packed_pool");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    for (name, capacity) in [
        ("1g_8_blocks", GIB),
        ("4g_32_blocks", 4 * GIB),
        ("16g_128_blocks", 16 * GIB),
        ("32g_256_blocks", 32 * GIB),
    ] {
        group.bench_function(BenchmarkId::new("reserved_16k", name), |b| {
            let state = PreparedPool::with_occupancy_fraction(capacity, 7, 8);
            b.iter(|| {
                let buffer = state
                    .pool
                    .acquire(&state.reservation, FRAME_BYTES)
                    .expect("packed-pool acquisition should succeed");
                black_box(buffer.capacity());
                drop(buffer);
            });
        });
    }
    group.finish();
}

/// Executes one synchronized batch of carrier acquire/drop operations.
///
/// The start and completion barriers are paid once per batch and amortized
/// across `iterations` operations on each thread.
fn run_contended_batch(
    pool: &BufferPool,
    reservation: &Reservation,
    threads: usize,
    iterations: u64,
) -> Duration {
    let ready = Barrier::new(threads + 1);
    let start = Barrier::new(threads + 1);
    let done = Barrier::new(threads + 1);

    thread::scope(|scope| {
        for _ in 0..threads {
            let ready = &ready;
            let start = &start;
            let done = &done;
            scope.spawn(move || {
                ready.wait();
                start.wait();
                for _ in 0..iterations {
                    let buffer = pool
                        .acquire(reservation, FRAME_BYTES)
                        .expect("contended acquisition should succeed");
                    black_box(buffer.capacity());
                    drop(buffer);
                }
                done.wait();
            });
        }

        ready.wait();
        let started = Instant::now();
        start.wait();
        done.wait();
        started.elapsed()
    })
}

/// Measures aggregate reserved acquisition under shared-pool contention.
///
/// Every thread uses one pool and reservation, repeatedly acquiring and
/// returning one carrier. The sweep exposes admission, bitmap, owner-reference,
/// and cache-line contention from one through 64 threads.
fn benchmark_reserved_contention(c: &mut Criterion) {
    let state = PreparedPool::new(BLOCK_BYTES);
    let mut group = c.benchmark_group("buffer_pool/contention");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for threads in [1usize, 2, 8, 32, 64] {
        group.throughput(Throughput::Elements(threads as u64));
        group.bench_with_input(
            BenchmarkId::new("reserved_16k", threads),
            &threads,
            |b, &threads| {
                b.iter_custom(|iterations| {
                    run_contended_batch(&state.pool, &state.reservation, threads, iterations)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_acquire_release,
    benchmark_collector_strategy,
    benchmark_buffer_lifecycle,
    benchmark_packed_pool_scaling,
    benchmark_reserved_contention
);
criterion_main!(benches);
