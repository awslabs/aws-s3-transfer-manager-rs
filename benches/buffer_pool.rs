/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Buffer-pool mechanism benchmarks.
//!
//! The groups isolate acquisition, mutable collection, immutable ownership,
//! final-owner accounting, fragmented-pool search, sustained whole-part
//! placement, and contention through the public memory API. Pools are prepared
//! before timed loops, so ordinary results exclude virtual range reservation
//! and initial block preparation unless a scenario states otherwise.
//!
//! Payload initialization is included where the scenario models response
//! collection. Lifecycle groups use batched setup to time only the named
//! ownership transition. Exact allocator work is asserted separately by the
//! ignored private arena probe; these benchmarks measure elapsed cost without
//! exposing private counters to the benchmark crate.

use std::future::Future;
use std::hint::black_box;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use aws_sdk_s3_transfer_manager::memory::{
    BufferPool, MemoryBudgetConfig, PooledBufMut, Reservation, ReserveFuture, SegmentedBytes,
};
use bytes::{Buf, BufMut};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const GIB: usize = 1024 * MIB;
const FRAME_BYTES: usize = 16 * KIB;
const PART_BYTES: usize = 8 * MIB;
const BLOCK_BYTES: usize = 128 * MIB;
/// Four production blocks keep the churn fixture bounded while permitting
/// concurrent whole-part placement across multiple slots.
const CHURN_CAPACITY_BYTES: usize = 4 * BLOCK_BYTES;
/// Replacement batches sampled to reduce one-run scheduler noise without
/// materially extending benchmark startup.
const CHURN_PREFLIGHT_BATCHES: usize = 16;

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

/// Whether owner return remains covered by an open reservation envelope.
#[derive(Clone, Copy)]
enum ReturnCoverage {
    /// Owners return before the reservation closes; closure completes the
    /// timed release sequence.
    Covered,
    /// The reservation closes during setup, so the timed owner return repays
    /// uncovered charges.
    Uncovered,
}

impl ReturnCoverage {
    /// Returns the stable benchmark identifier for this accounting state.
    fn name(self) -> &'static str {
        match self {
            Self::Covered => "covered",
            Self::Uncovered => "uncovered",
        }
    }
}

/// How an immutable value releases its carrier owners.
#[derive(Clone, Copy)]
enum ReturnPattern {
    /// Destroys the complete value at once.
    WholeValue,
    /// Advances one carrier at a time before destroying the empty cursor.
    Progressive,
}

impl ReturnPattern {
    /// Returns the stable benchmark identifier for this release pattern.
    fn name(self) -> &'static str {
        match self {
            Self::WholeValue => "whole",
            Self::Progressive => "progressive",
        }
    }
}

/// One immutable value and any reservation retained through its return.
struct OwnerReturnUnit {
    value: SegmentedBytes,
    reservation: Option<Reservation>,
}

impl OwnerReturnUnit {
    /// Releases the value using the selected consumer behavior, then closes
    /// retained coverage.
    fn release(self, pattern: ReturnPattern) {
        let Self {
            mut value,
            reservation,
        } = self;
        match pattern {
            ReturnPattern::WholeValue => drop(value),
            ReturnPattern::Progressive => {
                while value.has_remaining() {
                    let count = value.remaining().min(FRAME_BYTES);
                    value.advance(count);
                }
                drop(value);
            }
        }
        drop(reservation);
    }
}

/// Prepared values split into timed and topology-preserving cohorts.
struct OwnerReturnScenario {
    timed: Vec<OwnerReturnUnit>,
    held: Vec<OwnerReturnUnit>,
}

impl OwnerReturnScenario {
    /// Releases only the selected cohort; held-owner cleanup occurs after the
    /// measured interval.
    fn measure(self, pattern: ReturnPattern) -> Duration {
        let Self { timed, held } = self;
        let elapsed = release_owner_cohort(timed, pattern);
        drop(held);
        elapsed
    }
}

/// Converts matching reservations and immutable values into return units.
fn owner_return_units(
    reservations: Vec<Reservation>,
    values: Vec<SegmentedBytes>,
    coverage: ReturnCoverage,
) -> Vec<OwnerReturnUnit> {
    assert_eq!(reservations.len(), values.len());
    reservations
        .into_iter()
        .zip(values)
        .map(|(reservation, value)| {
            let reservation = match coverage {
                ReturnCoverage::Covered => Some(reservation),
                ReturnCoverage::Uncovered => {
                    reservation.close_acquisition();
                    None
                }
            };
            OwnerReturnUnit { value, reservation }
        })
        .collect()
}

/// Builds complete part acquisitions that ordinarily form one presentation
/// segment per value.
fn prepare_packed_return(
    pool: &BufferPool,
    parts: usize,
    coverage: ReturnCoverage,
) -> OwnerReturnScenario {
    let mut reservations = Vec::with_capacity(parts);
    let mut values = Vec::with_capacity(parts);
    for _ in 0..parts {
        let reservation = pool
            .try_reserve(PART_BYTES)
            .expect("owner-return reservation should not fail")
            .expect("owner-return reservation should fit");
        let value = collect_upfront(pool, &reservation);
        assert_eq!(
            segment_count(&value),
            1,
            "packed owner-return value should have one presentation segment"
        );
        reservations.push(reservation);
        values.push(value);
    }
    OwnerReturnScenario {
        timed: owner_return_units(reservations, values, coverage),
        held: Vec::new(),
    }
}

/// Builds part values by alternating carrier-sized growth across producers.
///
/// Each producer retains its own reservation, but competing growth prevents
/// later carriers from normally adjoining its previous carrier. Presentation
/// fragmentation therefore changes without changing the 512 carrier owners
/// returned by each complete 8 MiB value.
fn prepare_fragmented_return(
    pool: &BufferPool,
    parts: usize,
    timed_parts: usize,
    coverage: ReturnCoverage,
) -> OwnerReturnScenario {
    assert!(parts >= 2);
    assert!(timed_parts != 0 && timed_parts <= parts);

    let mut reservations = Vec::with_capacity(parts);
    let mut buffers = Vec::with_capacity(parts);
    for _ in 0..parts {
        let reservation = pool
            .try_reserve(PART_BYTES)
            .expect("fragmented owner-return reservation should not fail")
            .expect("fragmented owner-return reservation should fit");
        let buffer = pool
            .acquire(&reservation, FRAME_BYTES)
            .expect("initial fragmented carrier should fit");
        reservations.push(reservation);
        buffers.push(buffer);
    }

    for frame in 0..PART_BYTES / FRAME_BYTES {
        for buffer in &mut buffers {
            if frame != 0 {
                buffer
                    .reserve(FRAME_BYTES)
                    .expect("fragmented carrier growth should fit");
            }
            buffer.put_bytes(0x5a, FRAME_BYTES);
        }
    }

    let values = buffers
        .into_iter()
        .map(|buffer| {
            let value = buffer.freeze();
            assert!(
                segment_count(&value) > 1,
                "interleaved owner-return value should be fragmented"
            );
            value
        })
        .collect();
    let mut units = owner_return_units(reservations, values, coverage);
    let held = units.split_off(timed_parts);
    OwnerReturnScenario { timed: units, held }
}

/// Releases one prepared cohort concurrently without timing thread creation.
///
/// Workers reach the ready barrier before timing starts. The measured interval
/// includes the start and completion barriers so concurrent cases should be
/// compared with each other rather than treated as pure destructor latency.
fn release_owner_cohort(units: Vec<OwnerReturnUnit>, pattern: ReturnPattern) -> Duration {
    if units.len() == 1 {
        let started = Instant::now();
        units
            .into_iter()
            .next()
            .expect("single-owner cohort should not be empty")
            .release(pattern);
        return started.elapsed();
    }

    let ready = Barrier::new(units.len() + 1);
    let start = Barrier::new(units.len() + 1);
    let done = Barrier::new(units.len() + 1);
    thread::scope(|scope| {
        for unit in units {
            let ready = &ready;
            let start = &start;
            let done = &done;
            scope.spawn(move || {
                ready.wait();
                start.wait();
                unit.release(pattern);
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

/// Waker used to verify that useful return work exposes a queued reservation.
#[derive(Default)]
struct ReservationWake {
    wakes: AtomicUsize,
}

impl Wake for ReservationWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

/// One queued reservation retained across the timed owner return.
struct QueuedReservation {
    future: Pin<Box<ReserveFuture>>,
    wake: Arc<ReservationWake>,
}

impl QueuedReservation {
    /// Polls a fresh request into the pool FIFO.
    fn new(pool: &BufferPool, bytes: usize) -> Self {
        let wake = Arc::new(ReservationWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(pool.reserve(bytes));
        assert!(
            future.as_mut().poll(&mut context).is_pending(),
            "benchmark reservation should enter the FIFO"
        );
        Self { future, wake }
    }

    /// Consumes the grant exposed by the timed return.
    fn finish(mut self) {
        assert!(
            self.wake.wakes.load(Ordering::Relaxed) != 0,
            "owner return did not wake the queued reservation"
        );
        let waker = Waker::from(Arc::clone(&self.wake));
        let mut context = Context::from_waker(&waker);
        let Poll::Ready(result) = self.future.as_mut().poll(&mut context) else {
            panic!("woken benchmark reservation remained pending");
        };
        drop(result.expect("queued benchmark reservation should succeed"));
    }
}

/// Measures the transition that makes one blocked FIFO head eligible.
///
/// In the covered case, returning the owners precedes reservation closure. In
/// the uncovered case, setup closes the reservation and the final owner return
/// repays the charge that prevents the queued reservation from being granted.
fn measure_queued_owner_return(pool: &BufferPool, coverage: ReturnCoverage) -> Duration {
    let filler = pool
        .try_reserve(BLOCK_BYTES - PART_BYTES)
        .expect("filler reservation should not fail")
        .expect("filler reservation should fit");
    let target = pool
        .try_reserve(PART_BYTES)
        .expect("target reservation should not fail")
        .expect("target reservation should fit");
    let value = collect_upfront(pool, &target);
    let queued = QueuedReservation::new(pool, PART_BYTES);
    let mut units = owner_return_units(vec![target], vec![value], coverage);

    let started = Instant::now();
    units
        .pop()
        .expect("queued return should contain one target")
        .release(ReturnPattern::WholeValue);
    let elapsed = started.elapsed();

    queued.finish();
    drop(filler);
    elapsed
}

/// Measures immutable-owner destruction independently of acquisition setup.
///
/// Covered and uncovered cases return identical carrier owners. Packed and
/// interleaved construction distinguish presentation traversal from accounting,
/// while whole-value and progressive cases model request-body destruction and
/// incremental consumers. The queued-head cases measure useful admission work;
/// all other cases measure the empty-FIFO return path.
///
/// Upload `PartBuffer` values normally enter the uncovered case because their
/// reservation closes before the immutable part is returned. Covered cases are
/// a control for keeping planned demand live through owner destruction.
fn benchmark_owner_return(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool/owner_return");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for coverage in [ReturnCoverage::Covered, ReturnCoverage::Uncovered] {
        let packed_pool = prepared_unreserved_pool(BLOCK_BYTES);
        group.throughput(Throughput::Bytes(PART_BYTES as u64));
        group.bench_function(BenchmarkId::new("packed_whole_1", coverage.name()), |b| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    measured += prepare_packed_return(&packed_pool, 1, coverage)
                        .measure(ReturnPattern::WholeValue);
                }
                measured
            });
        });

        let fragmented_pool = prepared_unreserved_pool(BLOCK_BYTES);
        for pattern in [ReturnPattern::WholeValue, ReturnPattern::Progressive] {
            group.bench_function(
                BenchmarkId::new(format!("fragmented_{}_1", pattern.name()), coverage.name()),
                |b| {
                    b.iter_custom(|iterations| {
                        let mut measured = Duration::ZERO;
                        for _ in 0..iterations {
                            measured += prepare_fragmented_return(&fragmented_pool, 2, 1, coverage)
                                .measure(pattern);
                        }
                        measured
                    });
                },
            );
        }

        let concurrent_pool = prepared_unreserved_pool(BLOCK_BYTES);
        group.throughput(Throughput::Bytes((8 * PART_BYTES) as u64));
        group.bench_function(
            BenchmarkId::new("fragmented_whole_8", coverage.name()),
            |b| {
                b.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;
                    for _ in 0..iterations {
                        measured += prepare_fragmented_return(&concurrent_pool, 8, 8, coverage)
                            .measure(ReturnPattern::WholeValue);
                    }
                    measured
                });
            },
        );

        let queued_pool = prepared_unreserved_pool(BLOCK_BYTES);
        group.throughput(Throughput::Bytes(PART_BYTES as u64));
        group.bench_function(
            BenchmarkId::new("packed_whole_1_queued_head", coverage.name()),
            |b| {
                b.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;
                    for _ in 0..iterations {
                        measured += measure_queued_owner_return(&queued_pool, coverage);
                    }
                    measured
                });
            },
        );
    }
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

/// Release order used by the sustained whole-part topology benchmark.
#[derive(Clone, Copy)]
enum PartReleaseOrder {
    /// Releases one rotating, logically adjacent group of parts.
    Ordered,
    /// Releases a fixed-seed shuffled group from across the live cohort.
    Shuffled,
}

impl PartReleaseOrder {
    /// Returns the stable benchmark identifier for this order.
    fn name(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Shuffled => "shuffled",
        }
    }
}

/// Whether replacement claims begin after or during owner return.
#[derive(Clone, Copy)]
enum PartReplacementSchedule {
    /// Return the complete selected cohort before starting any claim.
    ReleasedFirst,
    /// Let each worker return one part and immediately claim its replacement.
    Overlapped,
}

impl PartReplacementSchedule {
    /// Returns a stable name while preserving the established control names.
    fn benchmark_name(self, order: PartReleaseOrder) -> String {
        match self {
            Self::ReleasedFirst => order.name().to_owned(),
            Self::Overlapped => format!("overlap_{}", order.name()),
        }
    }
}

/// Measurements accumulated while replacing one batch of live parts.
#[derive(Default)]
struct PartChurnMeasurements {
    /// Complete part acquisitions observed.
    acquisitions: u64,
    /// Sum of per-worker acquisition latency.
    acquisition_time: Duration,
    /// Replacements represented by one presentation segment.
    contiguous: u64,
    /// Replacements represented by multiple presentation segments.
    segmented: u64,
}

impl PartChurnMeasurements {
    /// Adds one replacement batch to the aggregate.
    fn add(&mut self, other: Self) {
        self.acquisitions += other.acquisitions;
        self.acquisition_time += other.acquisition_time;
        self.contiguous += other.contiguous;
        self.segmented += other.segmented;
    }
}

/// A full pool whose immutable part owners are replaced in completion batches.
///
/// The capacity spans four production blocks. The released-first control
/// exposes the complete free cohort before concurrent refill. The overlapped
/// schedule gives each worker one live part to return immediately before its
/// claim, allowing claims to observe physical return publication in progress.
///
/// One long-lived reservation spans the pool so admission remains stable. The
/// fixture measures physical placement and immutable topology, not reservation
/// queueing or uncovered-return accounting.
struct WholePartChurn {
    state: PreparedPool,
    parts: Vec<Option<SegmentedBytes>>,
    release_indices: Vec<usize>,
    ordered_start: usize,
    shuffle_state: u64,
    part_bytes: usize,
}

impl WholePartChurn {
    /// Fills a four-block cohort with initialized immutable parts.
    fn new(part_bytes: usize) -> Self {
        assert_eq!(CHURN_CAPACITY_BYTES % part_bytes, 0);
        let state = PreparedPool::new(CHURN_CAPACITY_BYTES);
        let part_count = CHURN_CAPACITY_BYTES / part_bytes;
        let mut parts = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            parts.push(Some(Self::acquire_part(
                &state.pool,
                &state.reservation,
                part_bytes,
            )));
        }

        Self {
            state,
            parts,
            release_indices: Vec::with_capacity(part_count),
            ordered_start: 0,
            shuffle_state: 0x9e37_79b9_7f4a_7c15,
            part_bytes,
        }
    }

    /// Acquires, initializes, and freezes one complete part.
    fn acquire_part(
        pool: &BufferPool,
        reservation: &Reservation,
        part_bytes: usize,
    ) -> SegmentedBytes {
        let mut buffer = pool
            .acquire(reservation, part_bytes)
            .expect("whole-part benchmark acquisition should succeed");
        Self::initialize_part(&mut buffer, part_bytes);
        buffer.freeze()
    }

    /// Initializes one complete acquired part.
    fn initialize_part(buffer: &mut PooledBufMut, part_bytes: usize) {
        buffer.put_bytes(0x5a, part_bytes);
    }

    /// Selects the owners released before the next refill.
    fn select_release_indices(&mut self, order: PartReleaseOrder) {
        self.release_indices.clear();
        let part_count = self.parts.len();
        let release_count = part_count / 2;

        match order {
            PartReleaseOrder::Ordered => {
                for offset in 0..release_count {
                    self.release_indices
                        .push((self.ordered_start + offset) % part_count);
                }
                self.ordered_start = (self.ordered_start + release_count) % part_count;
            }
            PartReleaseOrder::Shuffled => {
                self.release_indices.extend(0..part_count);
                for upper in (1..part_count).rev() {
                    let selected = self.next_random() as usize % (upper + 1);
                    self.release_indices.swap(upper, selected);
                }
                self.release_indices.truncate(release_count);
            }
        }
    }

    /// Advances the benchmark's deterministic pseudo-random sequence.
    fn next_random(&mut self) -> u64 {
        let mut value = self.shuffle_state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.shuffle_state = value;
        value
    }

    /// Replaces one batch under the selected return/claim schedule.
    ///
    /// `acquisition_time` is the sum of independently measured worker
    /// latencies, not the wall-clock duration of the concurrent batch.
    fn replace_batch(
        &mut self,
        order: PartReleaseOrder,
        schedule: PartReplacementSchedule,
    ) -> PartChurnMeasurements {
        self.select_release_indices(order);
        let mut released = Vec::with_capacity(self.release_indices.len());
        for &index in &self.release_indices {
            released.push((
                index,
                Some(
                    self.parts[index]
                        .take()
                        .expect("selected benchmark part should be live"),
                ),
            ));
        }
        if matches!(schedule, PartReplacementSchedule::ReleasedFirst) {
            for (_, part) in &mut released {
                drop(
                    part.take()
                        .expect("released-first benchmark part should be live"),
                );
            }
        }

        let ready = Barrier::new(self.release_indices.len());
        let replacements = thread::scope(|scope| {
            let mut handles = Vec::with_capacity(self.release_indices.len());
            for (index, part) in released {
                let ready = &ready;
                let pool = &self.state.pool;
                let reservation = &self.state.reservation;
                let part_bytes = self.part_bytes;
                handles.push(scope.spawn(move || {
                    ready.wait();
                    drop(part);
                    let started = Instant::now();
                    let mut buffer = pool
                        .acquire(reservation, part_bytes)
                        .expect("whole-part benchmark acquisition should succeed");
                    let acquisition_time = started.elapsed();
                    Self::initialize_part(&mut buffer, part_bytes);
                    let part = buffer.freeze();
                    let (part, contiguous) = match part.try_into_contiguous() {
                        Ok(bytes) => (SegmentedBytes::from(bytes), true),
                        Err(segmented) => (segmented, false),
                    };
                    (index, part, acquisition_time, contiguous)
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .expect("whole-part benchmark worker should not panic")
                })
                .collect::<Vec<_>>()
        });

        let mut measurements = PartChurnMeasurements::default();
        for (index, part, acquisition_time, contiguous) in replacements {
            measurements.acquisition_time += acquisition_time;
            measurements.acquisitions += 1;
            if contiguous {
                measurements.contiguous += 1;
            } else {
                measurements.segmented += 1;
            }
            self.parts[index] = Some(part);
        }
        measurements
    }
}

/// Reports one warmed topology sample before Criterion timing begins.
fn report_whole_part_preflight(
    churn: &mut WholePartChurn,
    order: PartReleaseOrder,
    schedule: PartReplacementSchedule,
    part_name: &str,
) {
    let mut preflight = PartChurnMeasurements::default();
    for _ in 0..CHURN_PREFLIGHT_BATCHES {
        preflight.add(churn.replace_batch(order, schedule));
    }
    let total = preflight.contiguous + preflight.segmented;
    assert!(total != 0, "whole-part preflight produced no acquisitions");
    let contiguous_percent = preflight.contiguous as f64 * 100.0 / total as f64;
    let acquisition_ns = preflight.acquisition_time.as_nanos() / u128::from(preflight.acquisitions);
    let prepared_bytes = churn.state.pool.metrics().prepared_capacity_bytes();
    let occupancy_percent = CHURN_CAPACITY_BYTES as f64 * 100.0 / prepared_bytes as f64;
    eprintln!(
        "buffer-pool whole-part topology: part={part_name} schedule={} order={} \
         acquisitions={} acquisition_mean_ns={acquisition_ns} \
         prepared_bytes={prepared_bytes} occupancy_percent={occupancy_percent:.2} \
         contiguous={} segmented={} contiguous_percent={contiguous_percent:.2}",
        schedule.benchmark_name(order),
        order.name(),
        preflight.acquisitions,
        preflight.contiguous,
        preflight.segmented,
    );
}

/// Measures sustained whole-part reuse and the topology it produces.
///
/// Ordered and fixed-seed shuffled selection vary which owners complete. The
/// released-first schedule isolates competing claims after a stable free-space
/// publication. The overlapped schedule pairs each return with its replacement
/// claim, exposing transient holes and return-accounting contention. Each
/// iteration replaces half of a four-block cohort. Criterion reports the
/// complete release, fill, freeze, and owner-replacement cost; the summary
/// printed before timing reports mean per-worker acquisition latency and the
/// fraction of replacements that can enter the SDK as one contiguous body.
///
/// Shuffled selection does not require a segmented result. Placement depends on
/// concurrent execution and host scheduling, so contiguity is measured output
/// rather than a benchmark pass condition.
fn benchmark_whole_part_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_pool/whole_part_churn");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    for (part_name, part_bytes) in [("8m", 8 * MIB), ("16m", 16 * MIB)] {
        let replacement_bytes = CHURN_CAPACITY_BYTES / 2;
        group.throughput(Throughput::Bytes(replacement_bytes as u64));

        for schedule in [
            PartReplacementSchedule::ReleasedFirst,
            PartReplacementSchedule::Overlapped,
        ] {
            for order in [PartReleaseOrder::Ordered, PartReleaseOrder::Shuffled] {
                let mut preflight_reported = false;
                group.bench_with_input(
                    BenchmarkId::new(schedule.benchmark_name(order), part_name),
                    &part_bytes,
                    |b, &part_bytes| {
                        if !preflight_reported {
                            let mut preflight = WholePartChurn::new(part_bytes);
                            report_whole_part_preflight(&mut preflight, order, schedule, part_name);
                            preflight_reported = true;
                        }

                        let mut churn = WholePartChurn::new(part_bytes);
                        b.iter_custom(|iterations| {
                            let started = Instant::now();
                            for _ in 0..iterations {
                                black_box(churn.replace_batch(order, schedule));
                            }
                            started.elapsed()
                        });
                    },
                );
            }
        }
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
    benchmark_owner_return,
    benchmark_packed_pool_scaling,
    benchmark_whole_part_churn,
    benchmark_reserved_contention
);
criterion_main!(benches);
