/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;
use std::fmt::{self, Display};
use std::ops;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A monotonically increasing numeric value.
#[derive(Debug, Clone, Default)]
pub struct IncreasingCounter {
    value: Arc<AtomicU64>,
}

impl IncreasingCounter {
    /// Create a new counter starting at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the counter by the given amount and return the new value.
    pub fn increment(&self, amount: u64) -> u64 {
        self.value.fetch_add(amount, Ordering::Relaxed) + amount
    }

    /// Get the current value of the counter.
    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A value that can increase or decrease over time.
#[derive(Debug, Clone, Default)]
pub struct Gauge {
    value: Arc<AtomicU64>,
}

impl Gauge {
    /// Create a new gauge starting at zero.
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set the gauge to the given value and return the new value.
    pub fn set(&self, value: u64) -> u64 {
        self.value.store(value, Ordering::Relaxed);
        value
    }

    /// Increment the gauge by the given amount and return the new value.
    pub fn increment(&self, amount: u64) -> u64 {
        self.value.fetch_add(amount, Ordering::Relaxed) + amount
    }

    /// Decrement the gauge by the given amount and return the new value.
    pub fn decrement(&self, amount: u64) -> u64 {
        self.value.fetch_sub(amount, Ordering::Relaxed) - amount
    }

    /// Get the current value of the gauge.
    pub fn value(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A statistical distribution of values with configurable buckets.
#[derive(Debug)]
pub struct Histogram {
    inner: Arc<Mutex<HistogramInner>>,
}

#[derive(Debug)]
struct HistogramInner {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    bucket_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
}

impl Histogram {
    /// Create a new histogram with default buckets for latency measurements.
    pub fn new() -> Self {
        Self::with_buckets(default_latency_buckets())
    }

    /// Create a new histogram with custom bucket boundaries.
    pub fn with_buckets(bucket_bounds: Vec<f64>) -> Self {
        let bucket_counts = vec![0; bucket_bounds.len()];
        Self {
            inner: Arc::new(Mutex::new(HistogramInner {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
                bucket_bounds,
                bucket_counts,
            })),
        }
    }

    /// Record a value in the histogram.
    pub fn record(&self, value: f64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.count += 1;
            inner.sum += value;
            inner.min = inner.min.min(value);
            inner.max = inner.max.max(value);

            // Find the appropriate bucket
            for (i, &bound) in inner.bucket_bounds.iter().enumerate() {
                if value <= bound {
                    inner.bucket_counts[i] += 1;
                    break;
                }
            }
        }
    }

    /// Get the number of recorded values.
    pub fn count(&self) -> u64 {
        self.inner.lock().map(|inner| inner.count).unwrap_or(0)
    }

    /// Get the sum of all recorded values.
    pub fn sum(&self) -> f64 {
        self.inner.lock().map(|inner| inner.sum).unwrap_or(0.0)
    }

    /// Get the mean of all recorded values.
    pub fn mean(&self) -> f64 {
        if let Ok(inner) = self.inner.lock() {
            if inner.count > 0 {
                inner.sum / inner.count as f64
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    /// Get the minimum recorded value.
    pub fn min(&self) -> f64 {
        self.inner.lock().map(|inner| inner.min).unwrap_or(0.0)
    }

    /// Get the maximum recorded value.
    pub fn max(&self) -> f64 {
        self.inner.lock().map(|inner| inner.max).unwrap_or(0.0)
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

//TODO: maybe this should be a try_clone and it should fail if the mutex is poisoned?
impl Clone for Histogram {
    fn clone(&self) -> Self {
        if let Ok(inner) = self.inner.lock() {
            Self::with_buckets(inner.bucket_bounds.clone())
        } else {
            Self::new()
        }
    }
}

/// Default bucket boundaries for latency measurements in seconds.
fn default_latency_buckets() -> Vec<f64> {
    vec![
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

/// Configuration for throughput sampling.
#[derive(Debug, Clone)]
pub struct SamplingConfig {
    /// Interval between samples.
    pub interval: Duration,
    /// Maximum number of samples to retain.
    pub max_samples: usize,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(100),
            max_samples: 100,
        }
    }
}

/// Throughput metrics with min/max/average tracking and optional sampling.
#[derive(Debug)]
pub struct ThroughputMetrics {
    // Statistics using scaled u64 for atomic operations (bytes per second * 1000)
    min_throughput_bps: AtomicU64,
    max_throughput_bps: AtomicU64,
    avg_throughput_bps: AtomicU64,
    total_bytes: AtomicU64,
    start_time: std::time::Instant,

    // Optional sampling
    history: Option<Arc<Mutex<ThroughputHistory>>>,
}

#[derive(Debug)]
struct ThroughputHistory {
    pub(crate) samples: VecDeque<ThroughputSample>,
    last_sample_time: Option<std::time::Instant>,
    sample_interval: Duration,
    max_samples: usize,
}

#[derive(Debug, Clone)]
#[allow(unused)]
struct ThroughputSample {
    timestamp: std::time::Instant,
    bytes_per_second: f64,
}

impl ThroughputMetrics {
    /// Create new throughput metrics without sampling.
    pub fn new() -> Self {
        Self::with_sampling(None)
    }

    /// Create new throughput metrics with optional sampling configuration.
    pub fn with_sampling(sampling_config: Option<SamplingConfig>) -> Self {
        let history = sampling_config.map(|config| {
            Arc::new(Mutex::new(ThroughputHistory {
                samples: VecDeque::with_capacity(config.max_samples),
                last_sample_time: None,
                sample_interval: config.interval,
                max_samples: config.max_samples,
            }))
        });

        Self {
            min_throughput_bps: AtomicU64::new(u64::MAX),
            max_throughput_bps: AtomicU64::new(0),
            avg_throughput_bps: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            start_time: std::time::Instant::now(),
            history,
        }
    }

    /// Record bytes transferred and update throughput statistics.
    pub fn record_bytes(&self, bytes: u64) {
        let total = self.total_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let elapsed = self.start_time.elapsed();

        if elapsed.as_secs_f64() > 0.0 {
            let current_bps = (total as f64 / elapsed.as_secs_f64()) * 1000.0;
            let current_bps_scaled = current_bps as u64;

            // Update min (but not if it's the initial MAX value)
            let mut current_min = self.min_throughput_bps.load(Ordering::Relaxed);
            while current_bps_scaled < current_min {
                match self.min_throughput_bps.compare_exchange_weak(
                    current_min,
                    current_bps_scaled,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_min = x,
                }
            }

            // Update max
            let mut current_max = self.max_throughput_bps.load(Ordering::Relaxed);
            while current_bps_scaled > current_max {
                match self.max_throughput_bps.compare_exchange_weak(
                    current_max,
                    current_bps_scaled,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(x) => current_max = x,
                }
            }

            // Update average (same as current for cumulative calculation)
            self.avg_throughput_bps
                .store(current_bps_scaled, Ordering::Relaxed);

            // Update sampling history if enabled
            if let Some(ref history) = self.history {
                self.update_sample_history(history, current_bps / 1000.0);
            }
        }
    }

    fn update_sample_history(&self, history: &Arc<Mutex<ThroughputHistory>>, bps: f64) {
        if let Ok(mut hist) = history.try_lock() {
            let now = std::time::Instant::now();
            if hist.last_sample_time.is_none()
                || now.duration_since(hist.last_sample_time.unwrap_or(Instant::now()))
                    >= hist.sample_interval
            {
                hist.samples.push_back(ThroughputSample {
                    timestamp: now,
                    bytes_per_second: bps,
                });

                if hist.samples.len() > hist.max_samples {
                    hist.samples.pop_front();
                }

                hist.last_sample_time = Some(now);
            }
        }
    }

    /// Get the minimum throughput observed.
    pub fn min(&self) -> Throughput {
        let bps_scaled = self.min_throughput_bps.load(Ordering::Relaxed);
        if bps_scaled == u64::MAX {
            Throughput::new_bytes_per_sec(0)
        } else {
            Throughput::new_bytes_per_sec(bps_scaled / 1000)
        }
    }

    /// Get the maximum throughput observed.
    pub fn max(&self) -> Throughput {
        let bps_scaled = self.max_throughput_bps.load(Ordering::Relaxed);
        Throughput::new_bytes_per_sec(bps_scaled / 1000)
    }

    /// Get the average throughput.
    pub fn avg(&self) -> Throughput {
        let bps_scaled = self.avg_throughput_bps.load(Ordering::Relaxed);
        Throughput::new_bytes_per_sec(bps_scaled / 1000)
    }

    /// Get the total bytes transferred.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

impl Default for ThroughputMetrics {
    fn default() -> Self {
        Self::new()
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
            if self.total_bytes % self.unit.as_bytes_u64() == 0 {
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

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use crate::metrics::unit::ByteCountDisplayContext;

    use super::{
        unit::ByteUnit, Gauge, Histogram, IncreasingCounter, SamplingConfig, Throughput,
        ThroughputMetrics,
    };

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

    #[test]
    fn test_counter() {
        let counter = IncreasingCounter::new();
        assert_eq!(counter.value(), 0);

        assert_eq!(counter.increment(5), 5);
        assert_eq!(counter.value(), 5);

        assert_eq!(counter.increment(3), 8);
        assert_eq!(counter.value(), 8);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new();
        assert_eq!(gauge.value(), 0);

        assert_eq!(gauge.set(10), 10);
        assert_eq!(gauge.value(), 10);

        assert_eq!(gauge.increment(5), 15);
        assert_eq!(gauge.value(), 15);

        assert_eq!(gauge.decrement(3), 12);
        assert_eq!(gauge.value(), 12);
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::new();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.sum(), 0.0);

        histogram.record(1.0);
        histogram.record(2.0);
        histogram.record(3.0);

        assert_eq!(histogram.count(), 3);
        assert_eq!(histogram.sum(), 6.0);
        assert_eq!(histogram.mean(), 2.0);
        assert_eq!(histogram.min(), 1.0);
        assert_eq!(histogram.max(), 3.0);
    }

    #[test]
    fn test_throughput_metrics() {
        let metrics = ThroughputMetrics::new();

        // Record some bytes
        metrics.record_bytes(1000);
        std::thread::sleep(Duration::from_millis(10));
        metrics.record_bytes(2000);

        assert_eq!(metrics.total_bytes(), 3000);
        assert!(metrics.avg().bytes_transferred() > 0);
        assert!(metrics.max().bytes_transferred() >= metrics.min().bytes_transferred());
    }

    #[test]
    fn test_sampling_config() {
        // Test that max_samples is respected
        let one_sample_config = SamplingConfig {
            interval: Duration::from_millis(5),
            max_samples: 1,
        };

        let metrics = ThroughputMetrics::with_sampling(Some(one_sample_config));
        metrics.record_bytes(1000);
        std::thread::sleep(Duration::from_millis(10));
        metrics.record_bytes(2000);
        assert_eq!(metrics.history.unwrap().lock().unwrap().samples.len(), 1);

        // Test that the interval is respected
        let long_interval_config = SamplingConfig {
            interval: Duration::from_millis(50),
            max_samples: 10,
        };

        let metrics = ThroughputMetrics::with_sampling(Some(long_interval_config));
        metrics.record_bytes(1000);
        std::thread::sleep(Duration::from_millis(10));
        metrics.record_bytes(2000);
        assert_eq!(
            metrics
                .history
                .clone()
                .unwrap()
                .lock()
                .unwrap()
                .samples
                .len(),
            1
        );

        std::thread::sleep(Duration::from_millis(50));
        metrics.record_bytes(2000);
        assert_eq!(metrics.history.unwrap().lock().unwrap().samples.len(), 2);
    }
}
