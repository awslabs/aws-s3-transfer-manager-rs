use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

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
/// Minimum value is 0.
#[derive(Debug, Clone, Default)]
pub struct Gauge {
    value: Arc<AtomicU64>,
}

impl Gauge {
    /// Create a new gauge starting at 0.
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
    /// If the decrement would cause underflow, the gauge is clamped at 0.
    pub fn decrement(&self, amount: u64) -> u64 {
        // TODO: This can be done more cleanly when the atomic `update` method stabilizes
        loop {
            let current = self.value.load(Ordering::Relaxed);
            let new_value = current.saturating_sub(amount);

            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return new_value,
                Err(_) => continue,
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{Gauge, Histogram, IncreasingCounter};

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
}
