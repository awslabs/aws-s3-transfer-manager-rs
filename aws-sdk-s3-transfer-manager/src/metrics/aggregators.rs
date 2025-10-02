//TODO: remove this when more of these are used (the warnings were annoying)
#![allow(unused)]

use std::{
    collections::VecDeque,
    fmt::{self, Display},
    ops,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use crate::metrics::{
    instruments::{Gauge, Histogram, IncreasingCounter, UpDownCounter},
    unit,
};

/// Client-level metrics aggregating all transfers
#[derive(Debug, Clone, Default)]
pub struct ClientMetrics {
    /// Total number of transfers initiated
    transfers_initiated: IncreasingCounter,
    /// Total number of transfers completed successfully
    transfers_successful: IncreasingCounter,
    /// Total number of transfers that failed
    transfers_failed: IncreasingCounter,
    /// Total bytes transferred across all operations
    total_bytes_transferred: IncreasingCounter,
    /// Number of currently active transfers
    active_transfers: Gauge,
    /// Overall throughput metrics
    throughput: ThroughputMetrics,
}

impl ClientMetrics {
    /// Create new client metrics
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Increment transfers initiated counter
    pub(crate) fn increment_transfers_initiated(&self) {
        self.transfers_initiated.increment(1);
        self.active_transfers.increment(1.0);
    }

    /// Increment transfers completed counter
    pub(crate) fn increment_transfers_successful(&self) {
        self.transfers_successful.increment(1);
        self.active_transfers.decrement(1.0);
    }

    /// Increment transfers failed counter
    pub(crate) fn increment_transfers_failed(&self) {
        self.transfers_failed.increment(1);
        self.active_transfers.decrement(1.0);
    }

    /// Add bytes to total transferred
    pub(crate) fn add_bytes_transferred(&self, bytes: u64) {
        self.total_bytes_transferred.increment(bytes);
    }

    /// Get the number of transfers initiated
    pub fn transfers_initiated(&self) -> u64 {
        self.transfers_initiated.value()
    }

    /// Get the number of transfers completed
    pub fn transfers_successful(&self) -> u64 {
        self.transfers_successful.value()
    }

    /// Get the number of transfers failed
    pub fn transfers_failed(&self) -> u64 {
        self.transfers_failed.value()
    }

    /// Get the total bytes transferred
    pub fn total_bytes_transferred(&self) -> u64 {
        self.total_bytes_transferred.value()
    }

    /// Get the number of currently active transfers
    pub fn active_transfers(&self) -> f64 {
        self.active_transfers.value()
    }
}

/// Metrics for individual transfer operations
#[derive(Debug, Default)]
pub struct TransferMetrics {
    /// Number of bytes transferred for this operation
    bytes_transferred: IncreasingCounter,
    /// Number of parts/chunks completed
    parts_completed: IncreasingCounter,
    /// Number of parts/chunks failed
    parts_failed: IncreasingCounter,
    /// Transfer-specific throughput metrics
    throughput: ThroughputMetrics,
    /// Request latency histogram
    request_latency: Histogram,
    /// Detect whether this transfer failed
    pub(crate) transfer_failed: AtomicBool,
}

impl TransferMetrics {
    /// Create new transfer metrics
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Get bytes transferred
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred.value()
    }

    /// Get parts completed
    pub fn parts_completed(&self) -> u64 {
        self.parts_completed.value()
    }

    /// Get parts failed
    pub fn parts_failed(&self) -> u64 {
        self.parts_failed.value()
    }

    /// Get throughput metrics
    pub fn throughput(&self) -> &ThroughputMetrics {
        &self.throughput
    }

    /// Get request latency histogram
    pub fn request_latency(&self) -> &Histogram {
        &self.request_latency
    }

    /// Determine if the transfer has failed
    pub fn is_failed(&self) -> bool {
        self.transfer_failed.load(Ordering::Relaxed)
    }

    /// Increment bytes transferred by the given amount
    pub(crate) fn increment_bytes_transferred(&self, amount: u64) {
        self.bytes_transferred.increment(amount);
    }

    /// Increment parts completed by the given amount
    pub(crate) fn increment_parts_completed(&self, amount: u64) {
        self.parts_completed.increment(amount);
    }

    /// Increment parts failed by the given amount
    pub(crate) fn increment_parts_failed(&self, amount: u64) {
        self.parts_failed.increment(amount);
    }

    /// Record that a transfer has failed
    pub(crate) fn mark_transfer_failed(&self) {
        self.transfer_failed.store(true, Ordering::Relaxed);
    }
}

/// Scheduler-level metrics for concurrency tracking
#[derive(Debug, Clone, Default)]
pub(crate) struct SchedulerMetrics {
    // Total tasks in flight
    inflight: Arc<AtomicU64>,
    // High water mark for inflight tasks
    max_inflight: Gauge,
    // Count of permits acquired
    permits_acquired: UpDownCounter,
    // Count of failed permit acquisitions
    permit_acquisition_failures: IncreasingCounter,
    // Tracking the wait time of permits
    permit_wait_time: Histogram,

    // percentage of max concurrency in use
    scheduler_saturation: Gauge,
}

impl SchedulerMetrics {
    /// Create new scheduler metrics
    pub(crate) fn new() -> Self {
        Self {
            inflight: Arc::new(AtomicU64::new(0)),
            max_inflight: Gauge::new(),
            permits_acquired: UpDownCounter::new(),
            permit_acquisition_failures: IncreasingCounter::new(),
            permit_wait_time: Histogram::new(),
            scheduler_saturation: Gauge::new(),
        }
    }

    /// Increment the number of in-flight requests and returns the number currently in-flight after
    /// incrementing.
    pub(crate) fn increment_inflight(&self) -> u64 {
        (self.inflight.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Decrement the number of in-flight requests and returns the number currently in-flight after
    /// decrementing.
    pub(crate) fn decrement_inflight(&self) -> u64 {
        (self.inflight.fetch_sub(1, Ordering::Relaxed) - 1)
    }

    /// Get the current number of in-flight requests
    #[cfg(test)]
    pub(crate) fn inflight(&self) -> u64 {
        self.inflight.load(Ordering::Relaxed)
    }

    /// Update max inflight metric
    pub(crate) fn update_max_inflight(&self, current_inflight: u64) {
        let current_max = self.max_inflight.value();
        let current_inflight = current_inflight as f64;
        if current_inflight > current_max {
            self.max_inflight.set(current_inflight);
        }
    }

    /// Record permit acquisition
    pub(crate) fn record_permit_acquisition(&self) {
        self.permits_acquired.increment(1);
    }

    /// Record permit release
    pub(crate) fn record_permit_release(&self) {
        self.permits_acquired.decrement(1);
    }

    /// Record permit acquisition failure
    pub(crate) fn record_permit_acquisition_failure(&self) {
        self.permit_acquisition_failures.increment(1);
    }

    /// Record permit wait time
    pub(crate) fn record_permit_wait_time(&self, duration_secs: f64) {
        self.permit_wait_time.record(duration_secs);
    }

    /// Set scheduler saturation percentage
    pub(crate) fn set_scheduler_saturation(&self, inflight: u64, max_capacity: u64) {
        let saturation = if max_capacity == 0 {
            0.0
        } else {
            (inflight as f64 / max_capacity as f64) * 100.0
        };
        self.scheduler_saturation.set(saturation);
    }

    /// Get permit acquisitions counter
    pub(crate) fn permits_acquired(&self) -> &UpDownCounter {
        &self.permits_acquired
    }

    /// Get permit wait time histogram
    pub(crate) fn permit_wait_time(&self) -> &Histogram {
        &self.permit_wait_time
    }

    /// Get permit acquisition failures counter
    pub(crate) fn permit_acquisition_failures(&self) -> &IncreasingCounter {
        &self.permit_acquisition_failures
    }

    /// Get max inflight gauge
    pub(crate) fn max_inflight(&self) -> &Gauge {
        &self.max_inflight
    }

    /// Get scheduler saturation gauge
    pub(crate) fn scheduler_saturation(&self) -> &Gauge {
        &self.scheduler_saturation
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct TokenBucketMetrics {
    max_tokens: u64,
    available_tokens: UpDownCounter,
    token_wait_time: Histogram,
}

impl TokenBucketMetrics {
    // Create new TokenBucketMetrics with max_tokens
    pub(crate) fn new(max_tokens: u64) -> Self {
        Self {
            max_tokens,
            available_tokens: UpDownCounter::new_value(max_tokens),
            token_wait_time: Histogram::new(),
        }
    }

    /// Get available tokens gauge
    pub(crate) fn available_tokens(&self) -> &UpDownCounter {
        &self.available_tokens
    }

    /// Record token acquisition
    pub(crate) fn record_token_acquisition(&self) {
        self.available_tokens.decrement(1);
    }

    /// Record token release
    pub(crate) fn record_token_release(&self) {
        self.available_tokens.increment(1);
    }

    /// Record token wait time
    pub(crate) fn record_token_wait_time(&self, wait_time_secs: f64) {
        self.token_wait_time.record(wait_time_secs);
    }
}

/// Configuration for throughput sampling.
#[derive(Debug, Clone)]
pub(crate) struct SamplingConfig {
    /// Interval between samples.
    pub interval: Duration,
    /// Maximum number of samples to retain.
    pub max_samples: u64,
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
    // Number of times record_bytes called (for calculating cumulative avg)
    count: AtomicU64,

    // Optional sampling
    history: Option<Arc<Mutex<ThroughputHistory>>>,
}

#[derive(Debug)]
struct ThroughputHistory {
    pub(crate) samples: VecDeque<ThroughputSample>,
    last_sample_time: Option<std::time::Instant>,
    sample_interval: Duration,
    max_samples: u64,
}

#[derive(Debug, Clone)]
#[allow(unused)]
struct ThroughputSample {
    timestamp: std::time::Instant,
    bytes_per_second: f64,
}

impl ThroughputMetrics {
    /// Create new throughput metrics without sampling.
    pub(crate) fn new() -> Self {
        Self::with_sampling(None)
    }

    /// Create new throughput metrics with optional sampling configuration.
    pub(crate) fn with_sampling(sampling_config: Option<SamplingConfig>) -> Self {
        let history = sampling_config.map(|config| {
            Arc::new(Mutex::new(ThroughputHistory {
                samples: VecDeque::with_capacity(config.max_samples as usize),
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
            count: AtomicU64::new(0),
        }
    }

    /// Record bytes transferred and update throughput statistics.
    pub(crate) fn record_bytes(&self, bytes: u64) {
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

            // Update cumulative average
            let old_count = self.count.load(Ordering::Relaxed);
            let old_avg = self.avg_throughput_bps.load(Ordering::Relaxed);

            let new_count = old_count + 1;
            let new_avg = (current_bps_scaled + (old_count * old_avg)) / new_count;

            self.count.store(new_count, Ordering::Relaxed);
            self.avg_throughput_bps.store(new_avg, Ordering::Relaxed);

            // Update sampling history if enabled
            if let Some(ref history) = self.history {
                self.update_sample_history(history.clone(), current_bps / 1000.0);
            }
        }
    }

    fn update_sample_history(&self, history: Arc<Mutex<ThroughputHistory>>, bps: f64) {
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

                if hist.samples.len() > hist.max_samples as usize {
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

    /// Calculate the cumulative average throughput.
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

impl Clone for ThroughputMetrics {
    fn clone(&self) -> Self {
        // Create a new ThroughputMetrics with the same sampling configuration
        let sampling_config = self.history.as_ref().map(|history| {
            if let Ok(hist) = history.lock() {
                SamplingConfig {
                    interval: hist.sample_interval,
                    max_samples: hist.max_samples,
                }
            } else {
                SamplingConfig::default()
            }
        });

        Self::with_sampling(sampling_config)
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
    /// use aws_sdk_s3_transfer_manager::metrics::aggregators::Throughput;
    /// use aws_sdk_s3_transfer_manager::metrics::unit;
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
    use std::time::Duration;

    use super::{unit::ByteUnit, SamplingConfig, Throughput, ThroughputMetrics};

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
        let max_sample_metrics = ThroughputMetrics::with_sampling(Some(one_sample_config));

        // Record one sample, wait much longer than the interval, record another sample
        max_sample_metrics.record_bytes(1000);
        std::thread::sleep(Duration::from_millis(100));
        max_sample_metrics.record_bytes(2000);

        // Ensure only one sample recorded
        assert_eq!(
            max_sample_metrics
                .history
                .unwrap()
                .lock()
                .unwrap()
                .samples
                .len(),
            1
        );

        // Test that the interval is respected
        let long_interval_config = SamplingConfig {
            interval: Duration::from_millis(500),
            max_samples: 10,
        };
        let long_interval_metrics = ThroughputMetrics::with_sampling(Some(long_interval_config));

        // Record one sample, wait much less than the interval, record another sample
        long_interval_metrics.record_bytes(1000);
        std::thread::sleep(Duration::from_millis(5));
        long_interval_metrics.record_bytes(2000);

        // Ensure the second sample was not recorded
        assert_eq!(
            long_interval_metrics
                .history
                .clone()
                .unwrap()
                .lock()
                .unwrap()
                .samples
                .len(),
            1
        );

        // Wait again much longer than the interval this time
        std::thread::sleep(Duration::from_millis(600));
        long_interval_metrics.record_bytes(2000);

        // Ensure the new sample was recorded
        assert_eq!(
            long_interval_metrics
                .history
                .unwrap()
                .lock()
                .unwrap()
                .samples
                .len(),
            2
        );
    }

    #[test]
    fn test_scheduler_metrics() {
        use crate::metrics::aggregators::SchedulerMetrics;

        let metrics = SchedulerMetrics::new();

        assert_eq!(metrics.permits_acquired().value(), 0);
        metrics.record_permit_acquisition();
        assert_eq!(metrics.permits_acquired().value(), 1);

        assert_eq!(metrics.permit_acquisition_failures().value(), 0);
        metrics.record_permit_acquisition_failure();
        assert_eq!(metrics.permit_acquisition_failures().value(), 1);

        metrics.record_permit_wait_time(0.5);
        assert!(metrics.permit_wait_time().count() > 0);

        metrics.update_max_inflight(10);
        assert_eq!(metrics.max_inflight().value(), 10.0);
        // Should not update since it is lower
        metrics.update_max_inflight(5);
        assert_eq!(metrics.max_inflight().value(), 10.0);
        // Should update since it is higher
        metrics.update_max_inflight(15);
        assert_eq!(metrics.max_inflight().value(), 15.0);

        metrics.set_scheduler_saturation(10, 50);
        assert_eq!(metrics.scheduler_saturation().value(), 20.0);
    }
}
