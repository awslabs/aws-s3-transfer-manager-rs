/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

pub(crate) mod latency;

pub use crate::runtime::buffer_pool::MemoryMetrics;

use std::fmt::{self, Display};
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

/// Units of measurement
pub mod unit {
    use std::{fmt, str::FromStr};

    /// SI byte units
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ByteUnit {
        /// 1 byte
        Byte,
        /// 1000 bits (125 bytes)
        Kilobit,
        /// 2<sup>10</sup> bytes.
        Kibibyte,
        /// 125 * 10<sup>3</sup> bytes.
        Megabit,
        /// 2<sup>20</sup> bytes.
        Mebibyte,
        /// 125 * 10<sup>6</sup> bytes.
        Gigabit,
        /// 2<sup>30</sup> bytes.
        Gibibyte,
    }

    impl ByteUnit {
        /// Convert some number of bytes into this unit as an `f64`
        pub fn convert(&self, bytes: u64) -> f64 {
            bytes as f64 / self.as_bytes_u64() as f64
        }

        /// Figure out the best unit to display the given number of bytes in
        /// and return a [`ByteCountDisplayContext`] with the appropriate units set
        pub fn display(total_bytes: u64) -> ByteCountDisplayContext {
            let units = &[ByteUnit::Gibibyte, ByteUnit::Mebibyte, ByteUnit::Kibibyte];
            let mut unit = ByteUnit::Byte;
            for u in units {
                if total_bytes >= u.as_bytes_u64() {
                    unit = *u;
                    break;
                }
            }

            ByteCountDisplayContext::new(total_bytes, unit)
        }

        /// The number of bits represented by this unit
        pub const fn as_bits_u64(&self) -> u64 {
            self.as_bits_usize() as u64
        }

        /// The number of bits represented by this unit
        pub const fn as_bits_usize(&self) -> usize {
            match self {
                ByteUnit::Byte => 8,
                ByteUnit::Kilobit => 1_000,
                ByteUnit::Kibibyte => 1 << 13,
                ByteUnit::Megabit => 1_000_000,
                ByteUnit::Mebibyte => 1 << 23,
                ByteUnit::Gigabit => 1_000_000_000,
                ByteUnit::Gibibyte => 1 << 33,
            }
        }

        /// The number of bytes represented by this unit
        pub const fn as_bytes_u64(&self) -> u64 {
            self.as_bytes_usize() as u64
        }

        /// The number of bytes represented by this unit
        pub const fn as_bytes_usize(&self) -> usize {
            self.as_bits_usize() >> 3
        }

        pub(crate) const fn as_str(&self) -> &'static str {
            match self {
                ByteUnit::Byte => "B",
                ByteUnit::Kilobit => "Kb",
                ByteUnit::Kibibyte => "KiB",
                ByteUnit::Megabit => "Mb",
                ByteUnit::Mebibyte => "MiB",
                ByteUnit::Gigabit => "Gb",
                ByteUnit::Gibibyte => "GiB",
            }
        }
    }

    impl AsRef<str> for ByteUnit {
        fn as_ref(&self) -> &str {
            self.as_str()
        }
    }

    impl FromStr for ByteUnit {
        type Err = crate::error::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let unit = match s {
                "B" => ByteUnit::Byte,
                "Kb" => ByteUnit::Kilobit,
                "KiB" => ByteUnit::Kibibyte,
                "Mb" => ByteUnit::Megabit,
                "MiB" => ByteUnit::Mebibyte,
                "Gb" => ByteUnit::Gigabit,
                "GiB" => ByteUnit::Gibibyte,
                _ => {
                    return Err(crate::error::invalid_input(format!(
                        "unknown byte unit '{}'",
                        s
                    )))
                }
            };

            Ok(unit)
        }
    }

    /// Display context to format a value representing number of bytres in a particular unit
    #[derive(Debug)]
    pub struct ByteCountDisplayContext {
        /// The throughput measurment to display
        pub total_bytes: u64,
        /// The precise unit to display the throughput as
        pub unit: ByteUnit,
    }

    impl ByteCountDisplayContext {
        /// Create a new display context for the number of bytes in a specific unit
        pub fn new(total_bytes: u64, unit: ByteUnit) -> Self {
            Self { total_bytes, unit }
        }
    }

    impl fmt::Display for ByteCountDisplayContext {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.total_bytes.is_multiple_of(self.unit.as_bytes_u64()) {
                let converted = self.total_bytes / self.unit.as_bytes_u64();
                return write!(f, "{converted} {}", self.unit.as_str());
            }
            let precision = f.precision().unwrap_or(3);
            write!(
                f,
                "{1:.*} {2:}",
                precision,
                self.unit.convert(self.total_bytes),
                self.unit.as_str()
            )
        }
    }
}

/// Measured bytes transferred over some duration
#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    bytes_transferred: u64,
    elapsed: Duration,
}

impl Throughput {
    /// Create a new throughput measurement with the given bytes read and time elapsed
    pub const fn new(bytes_transferred: u64, elapsed: Duration) -> Throughput {
        Throughput {
            bytes_transferred,
            elapsed,
        }
    }

    /// Create a new throughput measurement assuming a one second duration
    ///
    /// This is convenience for:
    ///
    /// ```
    /// use std::time::Duration;
    /// use aws_sdk_s3_transfer_manager::metrics::{unit, Throughput};
    /// let bytes_transferred = 5 * unit::ByteUnit::Mebibyte.as_bytes_u64();
    /// assert_eq!(
    ///     Throughput::new(bytes_transferred, Duration::from_secs(1)),
    ///     Throughput::new_bytes_per_sec(bytes_transferred)
    /// );
    /// ```
    pub const fn new_bytes_per_sec(bytes_transferred: u64) -> Throughput {
        Self::new(bytes_transferred, Duration::from_secs(1))
    }

    /// Convert this throughput into a specific unit per second
    pub fn as_unit_per_sec(&self, unit: unit::ByteUnit) -> f64 {
        (self.bytes_transferred as f64 / unit.as_bytes_u64() as f64) / self.elapsed.as_secs_f64()
    }

    /// Convert this throughput into bytes / sec
    pub fn as_bytes_per_sec(&self) -> f64 {
        self.as_unit_per_sec(unit::ByteUnit::Byte)
    }

    /// Total bytes transferred
    pub const fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred
    }

    /// Returns a type that can be used to format/display this throughput in a particular unit
    pub fn display_as(&self, unit: unit::ByteUnit) -> ThroughputDisplayContext<'_> {
        ThroughputDisplayContext {
            throughput: self,
            unit,
        }
    }
}

impl PartialEq for Throughput {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes_per_sec() == other.as_bytes_per_sec()
    }
}

impl PartialOrd for Throughput {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_bytes_per_sec()
            .partial_cmp(&other.as_bytes_per_sec())
    }
}

/// Add two throughputs
impl ops::Add for Throughput {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() + rhs.as_bytes_per_sec();

        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Subtract throughput
impl ops::Sub for Throughput {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        let bps = self.as_bytes_per_sec() - rhs.as_bytes_per_sec();
        Throughput::new_bytes_per_sec(bps.round() as u64)
    }
}

/// Multiply throughput by a scalar
impl ops::Mul<u64> for Throughput {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred * rhs, self.elapsed)
    }
}

/// Divide throughput by a scalar
impl ops::Div<u64> for Throughput {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        Throughput::new(self.bytes_transferred / rhs, self.elapsed)
    }
}

impl fmt::Display for Throughput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = ThroughputDisplayContext {
            throughput: self,
            unit: unit::ByteUnit::Megabit,
        };
        Display::fmt(&context, f)
    }
}

/// Display context to format throughput in a particular unit
#[derive(Debug)]
pub struct ThroughputDisplayContext<'a> {
    /// The throughput measurment to display
    pub throughput: &'a Throughput,
    /// The precise unit to display the throughput as
    pub unit: unit::ByteUnit,
}

impl fmt::Display for ThroughputDisplayContext<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(precision) = f.precision() {
            write!(
                f,
                "{1:.*} {2:}/s",
                precision,
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        } else {
            write!(
                f,
                "{} {}/s",
                self.throughput.as_unit_per_sec(self.unit),
                self.unit.as_str()
            )
        }
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

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use crate::metrics::unit::ByteCountDisplayContext;

    use super::{unit::ByteUnit, Throughput};

    #[test]
    fn test_throughput_display() {
        // default
        assert_eq!(
            "1 Mb/s",
            format!("{}", Throughput::new(125_000, Duration::from_secs(1)))
        );

        let t = Throughput::new(1_000_000, Duration::from_secs(1));

        // explicit
        assert_eq!("1000000 B/s", format!("{}", t.display_as(ByteUnit::Byte)));
        assert_eq!("8000 Kb/s", format!("{}", t.display_as(ByteUnit::Kilobit)));
        assert_eq!(
            "976.5625 KiB/s",
            format!("{}", t.display_as(ByteUnit::Kibibyte))
        );
        assert_eq!("8 Mb/s", format!("{}", t.display_as(ByteUnit::Megabit)));
        assert_eq!(
            "0.954 MiB/s",
            format!("{:.3}", t.display_as(ByteUnit::Mebibyte))
        );
        assert_eq!("0.008 Gb/s", format!("{}", t.display_as(ByteUnit::Gigabit)));
        assert_eq!(
            "0.00093 GiB/s",
            format!("{:.5}", t.display_as(ByteUnit::Gibibyte))
        );
    }

    #[test]
    fn test_from_str() {
        let units = &[
            ByteUnit::Byte,
            ByteUnit::Kilobit,
            ByteUnit::Kibibyte,
            ByteUnit::Megabit,
            ByteUnit::Mebibyte,
            ByteUnit::Gigabit,
            ByteUnit::Gibibyte,
        ];

        for u in units {
            let u2 = ByteUnit::from_str(u.as_str()).unwrap();
            assert_eq!(*u, u2);
        }

        assert!(ByteUnit::from_str("kb").is_err());
    }

    #[test]
    fn test_ops() {
        let t = Throughput::new_bytes_per_sec(10 * ByteUnit::Mebibyte.as_bytes_u64());
        let t2 = Throughput::new_bytes_per_sec(5 * ByteUnit::Mebibyte.as_bytes_u64());

        assert_eq!(
            t + t2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() + t2.bytes_transferred())
        );
        assert_eq!(t - t2, t2);
        assert_eq!(
            t * 2,
            Throughput::new_bytes_per_sec(t.bytes_transferred() * 2)
        );
        assert_eq!(t / 2, t2);
    }

    #[test]
    fn test_byte_display_context() {
        assert_eq!("1 KiB", format!("{}", ByteUnit::display(1024)));
        assert_eq!("1 MiB", format!("{}", ByteUnit::display(1024 * 1024)));
        assert_eq!(
            "1 GiB",
            format!("{}", ByteUnit::display(1024 * 1024 * 1024))
        );

        assert_eq!("727 B", format!("{}", ByteUnit::display(727)));
        assert_eq!(
            "0.710 KiB",
            format!("{}", ByteCountDisplayContext::new(727, ByteUnit::Kibibyte))
        );
        assert_eq!("3.420 KiB", format!("{}", ByteUnit::display(3502)));
        assert_eq!("3.41992 KiB", format!("{:.5}", ByteUnit::display(3502)));

        assert_eq!("7.201 MiB", format!("{}", ByteUnit::display(7550498)));
        assert_eq!(
            "0.007 GiB",
            format!(
                "{}",
                ByteCountDisplayContext::new(7550498, ByteUnit::Gibibyte)
            )
        );

        assert_eq!("1.016 GiB", format!("{}", ByteUnit::display(1091242563)));
        assert_eq!(
            "1040.690 MiB",
            format!(
                "{}",
                ByteCountDisplayContext::new(1091242563, ByteUnit::Mebibyte)
            )
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
