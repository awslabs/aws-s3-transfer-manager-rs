/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

pub(crate) mod latency;

pub use crate::runtime::buffer_pool::MemoryMetrics;

use std::fmt;
use std::ops;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Point-in-time operational metrics for one transfer-manager client.
///
/// Metric groups are sampled independently. The representation remains private
/// so additional client-level metric groups can be added without changing
/// callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientMetrics {
    memory: MemoryMetrics,
}

impl ClientMetrics {
    pub(crate) fn new(memory: MemoryMetrics) -> Self {
        Self { memory }
    }

    /// Returns the shared payload-memory pool sample.
    ///
    /// If the client uses an explicit pool shared with another client or
    /// component, this sample describes that complete shared pool.
    pub fn memory(&self) -> &MemoryMetrics {
        &self.memory
    }
}

/// I/O bytes transferred from a completed work item.
///
/// Records bytes moved in each direction. Does not carry timing — the time
/// dimension is provided by [`IOWindow`] bucketing: bytes are attributed to
/// the bucket current when `record()` is called (i.e. operation completion
/// time, not spread across the operation's actual duration). At high
/// concurrency this averages out; at low concurrency a single long request
/// may skew the bucket it lands in.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct IoSample {
    /// Bytes sent over network (upload)
    pub network_tx: u64,
    /// Bytes received from network (download)
    pub network_rx: u64,
    /// Bytes read from disk (upload source)
    pub disk_read: u64,
    /// Bytes written to disk (download sink)
    pub disk_write: u64,
}

/// Aggregate measurements for transfer-manager service requests.
///
/// One logical request may include multiple transfer-manager reissues. SDK
/// attempts hidden inside one `send()` are intentionally not inferred here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestMetrics {
    pub(crate) requests: u64,
    pub(crate) elapsed: Duration,
    pub(crate) max_elapsed: Duration,
    pub(crate) retry_reissues: u64,
    pub(crate) throttle_reissues: u64,
    pub(crate) hedge_reissues: u64,
    pub(crate) retry_exhaustions: u64,
    pub(crate) backoff_duration: Duration,
}

impl RequestMetrics {
    /// Complete one logical request after its final response or failure.
    pub(crate) fn record_request(&mut self, elapsed: Duration) {
        self.requests = self.requests.saturating_add(1);
        self.elapsed = self.elapsed.saturating_add(elapsed);
        self.max_elapsed = self.max_elapsed.max(elapsed);
    }

    /// Record an ordinary transport reissue and its selected backoff.
    pub(crate) fn record_retry_reissue(&mut self, backoff: Duration) {
        self.retry_reissues = self.retry_reissues.saturating_add(1);
        self.record_backoff(backoff);
    }

    /// Record a throttling reissue and its selected backoff.
    pub(crate) fn record_throttle_reissue(&mut self, backoff: Duration) {
        self.throttle_reissues = self.throttle_reissues.saturating_add(1);
        self.record_backoff(backoff);
    }

    /// Record the one deadline-driven hedge and its selected backoff.
    pub(crate) fn record_hedge_reissue(&mut self, backoff: Duration) {
        self.hedge_reissues = self.hedge_reissues.saturating_add(1);
        self.record_backoff(backoff);
    }

    /// Record that a retryable failure reached the transfer-manager budget.
    pub(crate) fn record_retry_exhaustion(&mut self) {
        self.retry_exhaustions = self.retry_exhaustions.saturating_add(1);
    }

    fn record_backoff(&mut self, backoff: Duration) {
        self.backoff_duration = self.backoff_duration.saturating_add(backoff);
    }
}

impl ops::AddAssign<&Self> for RequestMetrics {
    fn add_assign(&mut self, rhs: &Self) {
        self.requests = self.requests.saturating_add(rhs.requests);
        self.elapsed = self.elapsed.saturating_add(rhs.elapsed);
        self.max_elapsed = self.max_elapsed.max(rhs.max_elapsed);
        self.retry_reissues = self.retry_reissues.saturating_add(rhs.retry_reissues);
        self.throttle_reissues = self.throttle_reissues.saturating_add(rhs.throttle_reissues);
        self.hedge_reissues = self.hedge_reissues.saturating_add(rhs.hedge_reissues);
        self.retry_exhaustions = self.retry_exhaustions.saturating_add(rhs.retry_exhaustions);
        self.backoff_duration = self.backoff_duration.saturating_add(rhs.backoff_duration);
    }
}

/// Lock-free aggregate backing transfer- and client-level request metrics.
#[derive(Debug, Default)]
pub(crate) struct RequestMetricsState {
    requests: AtomicU64,
    elapsed_ns: AtomicU64,
    max_elapsed_ns: AtomicU64,
    retry_reissues: AtomicU64,
    throttle_reissues: AtomicU64,
    hedge_reissues: AtomicU64,
    retry_exhaustions: AtomicU64,
    backoff_ns: AtomicU64,
}

impl RequestMetricsState {
    /// Merge one completed request measurement into this aggregate.
    pub(crate) fn record(&self, metrics: &RequestMetrics) {
        saturating_fetch_add(&self.requests, metrics.requests);
        let elapsed_ns = duration_ns(metrics.elapsed);
        saturating_fetch_add(&self.elapsed_ns, elapsed_ns);
        self.max_elapsed_ns
            .fetch_max(duration_ns(metrics.max_elapsed), Ordering::Relaxed);
        saturating_fetch_add(&self.retry_reissues, metrics.retry_reissues);
        saturating_fetch_add(&self.throttle_reissues, metrics.throttle_reissues);
        saturating_fetch_add(&self.hedge_reissues, metrics.hedge_reissues);
        saturating_fetch_add(&self.retry_exhaustions, metrics.retry_exhaustions);
        saturating_fetch_add(&self.backoff_ns, duration_ns(metrics.backoff_duration));
    }

    /// Return a point-in-time aggregate.
    pub(crate) fn snapshot(&self) -> RequestMetrics {
        RequestMetrics {
            requests: self.requests.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_ns.load(Ordering::Relaxed)),
            max_elapsed: Duration::from_nanos(self.max_elapsed_ns.load(Ordering::Relaxed)),
            retry_reissues: self.retry_reissues.load(Ordering::Relaxed),
            throttle_reissues: self.throttle_reissues.load(Ordering::Relaxed),
            hedge_reissues: self.hedge_reissues.load(Ordering::Relaxed),
            retry_exhaustions: self.retry_exhaustions.load(Ordering::Relaxed),
            backoff_duration: Duration::from_nanos(self.backoff_ns.load(Ordering::Relaxed)),
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn saturating_fetch_add(value: &AtomicU64, increment: u64) {
    let mut current = value.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(increment);
        match value.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

/// Number of buckets to use for calculating IO windows
const IO_WINDOW_BUCKETS: usize = 10;

struct Bucket {
    bytes: AtomicU64,
    /// Monotonic index identifying which rotation owns this slot.
    /// A bucket is live when `current_idx - epoch < IO_WINDOW_BUCKETS`.
    epoch: AtomicUsize,
}

/// Bucketed sliding window for throughput measurement.
///
/// Divides the measurement window into `IO_WINDOW_BUCKETS` equal-duration
/// buckets arranged in a ring. Each bucket accumulates bytes during its time
/// slice. Stale buckets are identified by comparing their epoch against the
/// current monotonic index — no per-bucket timestamps needed.
///
/// `throughput()` sums live buckets and divides by `window_duration` (fixed
/// denominator), so partially-filled windows during ramp-up or after gaps
/// naturally report lower throughput.
pub(crate) struct IOWindow {
    buckets: [Bucket; IO_WINDOW_BUCKETS],
    /// Monotonic counter. `current_idx % IO_WINDOW_BUCKETS` is the active slot.
    current_idx: AtomicUsize,
    /// Start time of the current bucket's time slice.
    bucket_start: Mutex<Instant>,
    bucket_duration: Duration,
    window_duration: Duration,
}

impl IOWindow {
    pub(crate) fn new(window_duration: Duration) -> Self {
        let bucket_duration = window_duration / IO_WINDOW_BUCKETS as u32;
        let buckets = std::array::from_fn(|_| Bucket {
            bytes: AtomicU64::new(0),
            epoch: AtomicUsize::new(0),
        });
        Self {
            buckets,
            current_idx: AtomicUsize::new(0),
            bucket_start: Mutex::new(Instant::now()),
            bucket_duration,
            window_duration,
        }
    }

    /// Advance the ring to catch up with elapsed time. Advances
    /// `bucket_start` by `bucket_duration` increments (not wall-clock reset)
    /// to keep buckets aligned to a consistent time grid.
    ///
    /// If more than `IO_WINDOW_BUCKETS` durations have elapsed, the entire
    /// ring is stale — clears all buckets in one step instead of looping.
    fn maybe_rotate(&self, guard: &mut std::sync::MutexGuard<'_, Instant>) -> usize {
        let elapsed = guard.elapsed();
        if elapsed < self.bucket_duration {
            return self.current_idx.load(Ordering::Acquire);
        }

        let steps = (elapsed.as_nanos() / self.bucket_duration.as_nanos()) as usize;

        if steps >= IO_WINDOW_BUCKETS {
            let current = self.current_idx.load(Ordering::Acquire) + steps;
            self.current_idx.store(current, Ordering::Release);
            for bucket in self.buckets.iter() {
                bucket.bytes.store(0, Ordering::Release);
                bucket.epoch.store(current, Ordering::Release);
            }
            **guard += self.bucket_duration * steps as u32;
            current
        } else {
            let mut current = self.current_idx.load(Ordering::Acquire);
            while guard.elapsed() >= self.bucket_duration {
                current = self.current_idx.fetch_add(1, Ordering::AcqRel) + 1;
                let slot = current % IO_WINDOW_BUCKETS;
                self.buckets[slot].bytes.store(0, Ordering::Release);
                self.buckets[slot].epoch.store(current, Ordering::Release);
                **guard += self.bucket_duration;
            }
            current
        }
    }

    /// Add bytes to the current bucket, rotating first if needed.
    pub(crate) fn add(&self, bytes: u64) {
        if let Ok(mut guard) = self.bucket_start.try_lock() {
            self.maybe_rotate(&mut guard);
        }
        let slot = self.current_idx.load(Ordering::Acquire) % IO_WINDOW_BUCKETS;
        self.buckets[slot].bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Throughput in bytes/sec over the window.
    pub(crate) fn throughput(&self) -> f64 {
        let mut guard = self.bucket_start.lock().unwrap();
        let current = self.maybe_rotate(&mut guard);
        let sum: u64 = self
            .buckets
            .iter()
            .filter(|b| current - b.epoch.load(Ordering::Acquire) < IO_WINDOW_BUCKETS)
            .map(|b| b.bytes.load(Ordering::Acquire))
            .sum();
        if sum == 0 {
            return 0.0;
        }
        sum as f64 / self.window_duration.as_secs_f64()
    }

    /// True if no live bucket has any bytes.
    pub(crate) fn is_idle(&self) -> bool {
        let mut guard = self.bucket_start.lock().unwrap();
        let current = self.maybe_rotate(&mut guard);
        self.buckets.iter().all(|b| {
            current - b.epoch.load(Ordering::Acquire) >= IO_WINDOW_BUCKETS
                || b.bytes.load(Ordering::Acquire) == 0
        })
    }
}

impl fmt::Debug for IOWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOWindow")
            .field("window_duration", &self.window_duration)
            .field("throughput_bps", &self.throughput())
            .finish()
    }
}

/// Sliding window throughput counters broken down by I/O direction.
///
/// Tracks network (sent/received) and disk (read/written) bytes in
/// separate sliding windows.
pub(crate) struct IOCounters {
    network_tx: IOWindow,
    network_rx: IOWindow,
    disk_read: IOWindow,
    disk_write: IOWindow,
}

impl IOCounters {
    /// Create counters with the given window duration for all windows.
    pub(crate) fn new(window_duration: Duration) -> Self {
        Self {
            network_tx: IOWindow::new(window_duration),
            network_rx: IOWindow::new(window_duration),
            disk_read: IOWindow::new(window_duration),
            disk_write: IOWindow::new(window_duration),
        }
    }

    /// Record an I/O sample into the appropriate windows.
    pub(crate) fn record(&self, sample: &IoSample) {
        self.network_tx.add(sample.network_tx);
        self.network_rx.add(sample.network_rx);
        self.disk_read.add(sample.disk_read);
        self.disk_write.add(sample.disk_write);
    }

    /// Network throughput (sent + received) in bytes/sec.
    pub(crate) fn network_throughput(&self) -> f64 {
        self.network_tx.throughput() + self.network_rx.throughput()
    }

    /// Disk throughput (read + written) in bytes/sec.
    #[allow(dead_code)] // TODO: telemetry observability
    pub(crate) fn disk_throughput(&self) -> f64 {
        self.disk_read.throughput() + self.disk_write.throughput()
    }

    /// Total throughput across all directions in bytes/sec.
    #[allow(dead_code)] // TODO: telemetry observability
    pub(crate) fn total_throughput(&self) -> f64 {
        self.network_throughput() + self.disk_throughput()
    }

    /// True if all windows are idle.
    pub(crate) fn is_idle(&self) -> bool {
        self.network_tx.is_idle()
            && self.network_rx.is_idle()
            && self.disk_read.is_idle()
            && self.disk_write.is_idle()
    }
}

impl fmt::Debug for IOCounters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOCounters")
            .field("network_sent_bps", &self.network_tx.throughput())
            .field("network_received_bps", &self.network_rx.throughput())
            .field("disk_read_bps", &self.disk_read.throughput())
            .field("disk_written_bps", &self.disk_write.throughput())
            .finish()
    }
}

/// Snapshot of transfer progress and IO metrics.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct TransferMetrics {
    /// Bytes sent over network (upload payload).
    pub network_tx: u64,
    /// Bytes received from network (download payload).
    pub network_rx: u64,
    /// Bytes read from disk.
    pub disk_read: u64,
    /// Bytes written to disk.
    pub disk_write: u64,
    /// Expected total payload bytes, if known.
    pub total_bytes: Option<u64>,
    /// Time from when the transfer was initiated until it recorded its
    /// finish, or until this snapshot if it has not.
    ///
    /// The finish is recorded close to, but not in a fixed order with, the
    /// change of the handle's status, so a snapshot taken around that change
    /// may not hold the final value. The metrics in an operation's output do.
    pub elapsed: std::time::Duration,
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    #[test]
    fn request_metrics_merge_preserves_counts_totals_and_maximum() {
        let mut aggregate = super::RequestMetrics::default();
        aggregate.record_request(Duration::from_millis(10));
        aggregate.record_retry_reissue(Duration::from_millis(2));

        let mut next = super::RequestMetrics::default();
        next.record_request(Duration::from_millis(30));
        next.record_throttle_reissue(Duration::from_millis(4));
        next.record_hedge_reissue(Duration::from_millis(1));
        next.record_retry_exhaustion();

        aggregate += &next;

        assert_eq!(
            aggregate,
            super::RequestMetrics {
                requests: 2,
                elapsed: Duration::from_millis(40),
                max_elapsed: Duration::from_millis(30),
                retry_reissues: 1,
                throttle_reissues: 1,
                hedge_reissues: 1,
                retry_exhaustions: 1,
                backoff_duration: Duration::from_millis(7),
            }
        );
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn request_metrics_state_merges_concurrent_updates_exactly() {
        const THREADS: u64 = 8;
        const UPDATES: u64 = 1_000;

        let state = Arc::new(super::RequestMetricsState::default());
        let sample = super::RequestMetrics {
            requests: 1,
            elapsed: Duration::from_micros(20),
            max_elapsed: Duration::from_micros(20),
            retry_reissues: 1,
            throttle_reissues: 2,
            hedge_reissues: 3,
            retry_exhaustions: 4,
            backoff_duration: Duration::from_micros(5),
        };

        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let state = state.clone();
                std::thread::spawn(move || {
                    for _ in 0..UPDATES {
                        state.record(&sample);
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        let updates = THREADS * UPDATES;
        assert_eq!(
            state.snapshot(),
            super::RequestMetrics {
                requests: updates,
                elapsed: Duration::from_micros(20 * updates),
                max_elapsed: Duration::from_micros(20),
                retry_reissues: updates,
                throttle_reissues: 2 * updates,
                hedge_reissues: 3 * updates,
                retry_exhaustions: 4 * updates,
                backoff_duration: Duration::from_micros(5 * updates),
            }
        );
    }

    #[test]
    fn request_metrics_state_saturates_duration_and_counter_overflow() {
        let state = super::RequestMetricsState::default();
        state.record(&super::RequestMetrics {
            requests: u64::MAX,
            elapsed: Duration::MAX,
            max_elapsed: Duration::MAX,
            retry_reissues: u64::MAX,
            throttle_reissues: u64::MAX,
            hedge_reissues: u64::MAX,
            retry_exhaustions: u64::MAX,
            backoff_duration: Duration::MAX,
        });
        state.record(&super::RequestMetrics {
            requests: 1,
            elapsed: Duration::from_nanos(1),
            max_elapsed: Duration::from_nanos(1),
            retry_reissues: 1,
            throttle_reissues: 1,
            hedge_reissues: 1,
            retry_exhaustions: 1,
            backoff_duration: Duration::from_nanos(1),
        });

        assert_eq!(
            state.snapshot(),
            super::RequestMetrics {
                requests: u64::MAX,
                elapsed: Duration::from_nanos(u64::MAX),
                max_elapsed: Duration::from_nanos(u64::MAX),
                retry_reissues: u64::MAX,
                throttle_reissues: u64::MAX,
                hedge_reissues: u64::MAX,
                retry_exhaustions: u64::MAX,
                backoff_duration: Duration::from_nanos(u64::MAX),
            }
        );
    }

    // --- IOWindow tests ---

    use super::IOWindow;

    #[test]
    fn io_window_empty_returns_zero() {
        let w = IOWindow::new(Duration::from_millis(500));
        assert_eq!(w.throughput(), 0.0);
        assert!(w.is_idle());
    }

    #[test]
    fn io_window_single_add() {
        // 1MB in a 1s window = 1MB/s
        let w = IOWindow::new(Duration::from_secs(1));
        w.add(1_000_000);
        let tp = w.throughput();
        assert!((tp - 1_000_000.0).abs() < 1.0, "expected ~1MB/s, got {tp}");
        assert!(!w.is_idle());
    }

    #[test]
    fn io_window_multiple_adds_accumulate() {
        let w = IOWindow::new(Duration::from_secs(1));
        w.add(500_000);
        w.add(500_000);
        let tp = w.throughput();
        assert!((tp - 1_000_000.0).abs() < 1.0, "expected ~1MB/s, got {tp}");
    }

    #[test]
    fn io_window_stale_buckets_evicted() {
        let w = IOWindow::new(Duration::from_millis(100));
        w.add(1_000_000);
        assert!(!w.is_idle());
        // Sleep past the full window duration
        std::thread::sleep(Duration::from_millis(150));
        // Trigger rotation
        w.add(0);
        assert!(w.is_idle(), "stale data should have been evicted");
        assert_eq!(w.throughput(), 0.0);
    }

    #[test]
    fn io_window_spans_multiple_buckets() {
        // Window = 500ms, bucket = 50ms. Add to two different buckets.
        let w = IOWindow::new(Duration::from_millis(500));
        w.add(500_000);
        // Sleep past one bucket duration to force rotation
        std::thread::sleep(Duration::from_millis(60));
        w.add(500_000);
        let tp = w.throughput();
        // Both buckets live, total = 1MB / 0.5s = 2MB/s
        assert!(
            (tp - 2_000_000.0).abs() < 100_000.0,
            "expected ~2MB/s, got {tp}"
        );
    }

    // --- IOCounters tests ---

    use super::IOCounters;
    use super::IoSample;

    #[test]
    fn io_counters_network_throughput() {
        let c = IOCounters::new(Duration::from_secs(1));
        c.record(&IoSample {
            network_tx: 1_000_000,
            network_rx: 2_000_000,
            ..Default::default()
        });
        let tp = c.network_throughput();
        assert!((tp - 3_000_000.0).abs() < 1.0, "expected ~3MB/s, got {tp}");
    }

    #[test]
    fn io_counters_disk_throughput() {
        let c = IOCounters::new(Duration::from_secs(1));
        c.record(&IoSample {
            disk_read: 4_000_000,
            disk_write: 1_000_000,
            ..Default::default()
        });
        let tp = c.disk_throughput();
        assert!((tp - 5_000_000.0).abs() < 1.0, "expected ~5MB/s, got {tp}");
    }

    #[test]
    fn io_counters_total_throughput() {
        let c = IOCounters::new(Duration::from_secs(1));
        c.record(&IoSample {
            network_tx: 1_000_000,
            network_rx: 2_000_000,
            ..Default::default()
        });
        c.record(&IoSample {
            disk_read: 3_000_000,
            disk_write: 4_000_000,
            ..Default::default()
        });
        let tp = c.total_throughput();
        assert!(
            (tp - 10_000_000.0).abs() < 1.0,
            "expected ~10MB/s, got {tp}"
        );
    }

    #[test]
    fn io_counters_is_idle_when_all_idle() {
        let c = IOCounters::new(Duration::from_millis(100));
        assert!(c.is_idle());
        c.record(&IoSample {
            network_tx: 1_000,
            ..Default::default()
        });
        assert!(!c.is_idle());
        std::thread::sleep(Duration::from_millis(150));
        // Trigger rotation on the window that has data
        c.record(&IoSample::default());
        assert!(c.is_idle());
    }
}
