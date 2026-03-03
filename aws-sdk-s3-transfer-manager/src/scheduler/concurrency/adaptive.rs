/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Adaptive concurrency controller.
//!
//! Three-state machine that adjusts concurrency target based on observed network
//! goodput (bytes sent + received per measurement window).
//!
//! ## Algorithm
//!
//! **SlowStart**: Exponential growth (2x per window). Accepts if throughput improved
//! by >= 10% over the previous window. On rejection, transitions to StableProbing.
//!
//! **StableProbing**: Linear upward probes (+2% per window). Accepts if throughput
//! improved by >= 2%. After N consecutive rejections (default 3), transitions to
//! StableShedding.
//!
//! **StableShedding**: Accelerating downward probes (-2%, -4%, -6%, ...). Accepts if
//! throughput stays within 2% of peak (max of recent history). On rejection (throughput
//! degraded too much), transitions back to StableProbing.
//!
//! ## Error-Aware Transitions
//!
//! Throttle errors (503, 429, SlowDown) trigger an immediate transition to
//! StableShedding regardless of current state, bypassing the measurement window.
//!
//! ## Thread Safety
//!
//! `on_completion` is called from multiple worker threads. Network bytes are
//! accumulated via atomic add (lock-free hot path). The algorithm state is behind
//! a Mutex, touched only at window boundaries via `try_lock` to avoid blocking
//! workers.
//!
//! Adapted from the SimpleSlowStart algorithm.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{CompletionSample, ConcurrencyController, ErrorKind};

/// Configuration for the adaptive concurrency controller.
#[derive(Debug, Clone)]
pub(crate) struct AdaptiveConfig {
    /// Starting concurrency before any measurements.
    pub initial_concurrency: usize,
    /// Hard ceiling on concurrency. None means unlimited.
    pub max_concurrency: Option<usize>,
    /// How long each measurement window lasts.
    pub window_duration: Duration,
    /// Minimum completions before the algorithm runs. If fewer completions
    /// arrive within window_duration, the window extends until this threshold
    /// is met or max_window_duration is reached. Prevents the algorithm from
    /// acting on noisy signal during cold start (connection warmup, DNS, TLS).
    pub min_completions_per_window: usize,
    /// Hard cap on window extension. If min_completions hasn't been reached
    /// by this duration, the algorithm runs with whatever data is available.
    pub max_window_duration: Duration,
    /// SlowStart growth factor (default 2.0 = double each window).
    pub slow_start_multiplier: f64,
    /// SlowStart acceptance threshold. Throughput must improve by at least
    /// this ratio (default 0.10 = 10%) to accept an upward probe.
    pub slow_start_acceptance_margin: f64,
    /// StableProbing/StableShedding acceptance threshold (default 0.02 = 2%).
    /// Upward probes must improve by this much. Downward probes must stay
    /// within this much of peak.
    pub stable_acceptance_margin: f64,
    /// Probe size as fraction of current concurrency (default 0.02 = 2%).
    /// StableProbing: additive increment. StableShedding: multiplicative
    /// reduction that accelerates with consecutive accepted probes.
    pub stable_probe_pct: f64,
    /// Minimum change per probe step (default 5).
    pub min_probe_size: usize,
    /// Maximum change per probe step (default 200).
    pub max_probe_size: usize,
    /// Number of recent goodput measurements to retain (default 10).
    /// StableShedding compares against the max of this history.
    pub history_size: usize,
    /// Consecutive rejections in StableProbing before transitioning to
    /// StableShedding (default 3).
    pub shedding_transition_threshold: usize,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_concurrency: 10,
            max_concurrency: None,
            // Measurement window. 500ms is responsive while still smoothing
            // per-request variance.
            // Extended automatically if min_completions not met (cold start).
            window_duration: Duration::from_millis(500),
            // ~80MB at 8MB parts. One full round of completions at initial=10.
            min_completions_per_window: 10,
            // Don't wait forever for completions on a slow connection.
            max_window_duration: Duration::from_secs(5),
            // SlowStart: 10 -> 20 -> 40 -> 80 -> 160
            slow_start_multiplier: 2.0,
            // SlowStart requires 10% improvement to accept a doubling.
            slow_start_acceptance_margin: 0.10,
            // Stable states require 2% improvement (probing) or 2% of peak (shedding).
            stable_acceptance_margin: 0.02,
            // Probe size: 2% of current concurrency per step.
            stable_probe_pct: 0.02,
            // Don't probe smaller than 5 or larger than 200 per step.
            min_probe_size: 5,
            max_probe_size: 200,
            // StableShedding compares against max of last 10 windows.
            history_size: 10,
            // 3 consecutive rejections in StableProbing triggers shedding.
            shedding_transition_threshold: 3,
        }
    }
}

/// Algorithm phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Exponential growth (2x). Finding the ballpark.
    SlowStart,
    /// Small upward probes (+2%). Tracking the optimum.
    StableProbing,
    /// Accelerating downward probes. Shedding excess concurrency.
    StableShedding,
}

/// Mutable algorithm state, protected by Mutex. Only touched at window boundaries.
#[derive(Debug)]
struct AlgorithmState {
    phase: Phase,
    /// Last concurrency level where a probe was accepted.
    accepted_concurrency: usize,
    /// Concurrency level being tested in the current window.
    probing_concurrency: usize,
    /// Circular buffer of recent goodput measurements (bytes/sec).
    recent_goodput: VecDeque<f64>,
    /// Consecutive probe results in the same direction within the current phase.
    /// SlowStart: consecutive accepts (unused, transitions on first reject).
    /// StableProbing: consecutive rejections (triggers shedding at threshold).
    /// StableShedding: consecutive accepts (accelerates reduction).
    consecutive_probes: usize,
    window_start: Instant,
}

/// Accumulates bytes over a measurement window.
struct IOWindow {
    bytes: AtomicU64,
    completions: AtomicU64,
}

impl IOWindow {
    fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            completions: AtomicU64::new(0),
        }
    }

    fn add(&self, bytes_sent: u64, bytes_received: u64) {
        self.bytes
            .fetch_add(bytes_sent + bytes_received, Ordering::Relaxed);
        self.completions.fetch_add(1, Ordering::Relaxed);
    }

    /// Resets accumulators and returns (bytes, completions).
    fn take(&self) -> (u64, u64) {
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let completions = self.completions.swap(0, Ordering::Relaxed);
        (bytes, completions)
    }
}

impl fmt::Debug for IOWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IOWindow")
            .field("bytes", &self.bytes.load(Ordering::Relaxed))
            .field("completions", &self.completions.load(Ordering::Relaxed))
            .finish()
    }
}

/// Adaptive concurrency controller.
///
/// Adjusts the concurrency target based on observed network goodput using a
/// three-state machine (SlowStart, StableProbing, StableShedding). See module
/// docs for algorithm details.
#[derive(Debug)]
pub(crate) struct AdaptiveConcurrencyController {
    config: AdaptiveConfig,
    /// Current concurrency target. Read lock-free by `target()`.
    target: AtomicUsize,
    /// Network bytes accumulated during the current measurement window (sent + received).
    ///
    /// Note: control plane completions (CreateMPU, CompleteMPU) contribute to
    /// the completion count but carry zero bytes, slightly diluting the goodput
    /// signal. Accepted as noise -- not worth filtering.
    network_window: IOWindow,
    /// Algorithm state. Only accessed at window boundaries.
    state: Mutex<AlgorithmState>,
}

impl AdaptiveConcurrencyController {
    pub(crate) fn new(config: AdaptiveConfig) -> Self {
        let initial = config.initial_concurrency;
        Self {
            target: AtomicUsize::new(initial),
            network_window: IOWindow::new(),
            state: Mutex::new(AlgorithmState {
                phase: Phase::SlowStart,
                accepted_concurrency: initial,
                probing_concurrency: initial,
                recent_goodput: VecDeque::with_capacity(config.history_size),
                consecutive_probes: 0,
                window_start: Instant::now(),
            }),
            config,
        }
    }

    /// Check if the measurement window has elapsed and enough data has been
    /// collected to run the algorithm. Uses try_lock to avoid blocking workers.
    ///
    /// Returns early (no-op) if:
    /// - Another thread holds the lock (contention avoidance)
    /// - The minimum window duration hasn't elapsed yet
    /// - The window has elapsed but fewer than min_completions have arrived
    ///   and we haven't hit max_window_duration yet (cold start protection:
    ///   avoids acting on noisy signal from connection warmup, DNS, TLS)
    fn maybe_update_window(&self) {
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        let elapsed = state.window_start.elapsed();
        if elapsed < self.config.window_duration {
            return;
        }

        // Extend the window if we don't have enough completions for a
        // meaningful signal, but cap at max_window_duration to avoid stalling.
        let completions = self.network_window.completions.load(Ordering::Relaxed);
        if (completions as usize) < self.config.min_completions_per_window
            && elapsed < self.config.max_window_duration
        {
            return;
        }

        let (bytes, _completions) = self.network_window.take();
        let elapsed = state.window_start.elapsed();
        state.window_start = Instant::now();

        let goodput = if elapsed.is_zero() {
            0.0
        } else {
            bytes as f64 / elapsed.as_secs_f64()
        };

        let new_target = self.run_algorithm(&mut state, goodput);
        let clamped = match self.config.max_concurrency {
            Some(max) => new_target.min(max),
            None => new_target,
        }
        .max(1);
        self.target.store(clamped, Ordering::Relaxed);
    }

    /// Run the three-state algorithm with a new goodput measurement.
    /// Returns the next probing concurrency level.
    fn run_algorithm(&self, state: &mut AlgorithmState, goodput: f64) -> usize {
        // Record measurement before comparing
        state.recent_goodput.push_back(goodput);
        if state.recent_goodput.len() > self.config.history_size {
            state.recent_goodput.pop_front();
        }

        // Bootstrap: need at least 2 measurements to compute ratios
        if state.recent_goodput.len() < 2 {
            state.probing_concurrency =
                (state.accepted_concurrency as f64 * self.config.slow_start_multiplier) as usize;
            return self.clamp_probe(state.probing_concurrency, state.accepted_concurrency);
        }

        // TODO(adaptive): exercised check -- reject probe if peak in_flight
        // didn't reach accepted_concurrency. Needs pool.in_flight_count()
        // plumbed to the controller.

        match state.phase {
            Phase::SlowStart => self.run_slow_start(state, goodput),
            Phase::StableProbing => self.run_stable_probing(state, goodput),
            Phase::StableShedding => self.run_stable_shedding(state, goodput),
        }
    }

    /// SlowStart: exponential upward probing. Accept if throughput improved
    /// by >= slow_start_acceptance_margin over the previous measurement.
    /// On rejection, transition to StableProbing.
    fn run_slow_start(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let previous = state.recent_goodput[state.recent_goodput.len() - 2];
        let ratio = ratio(measured, previous);
        let accept = ratio >= 1.0 + self.config.slow_start_acceptance_margin;

        if accept {
            state.accepted_concurrency = state.probing_concurrency;
            state.probing_concurrency =
                (state.accepted_concurrency as f64 * self.config.slow_start_multiplier) as usize;
        } else {
            state.phase = Phase::StableProbing;
            state.consecutive_probes = 0;
            state.probing_concurrency = (state.accepted_concurrency as f64
                * (1.0 + self.config.stable_probe_pct))
                .ceil() as usize;
        }

        self.clamp_probe(state.probing_concurrency, state.accepted_concurrency)
    }

    /// StableProbing: linear upward probes (+stable_probe_pct). Accept if
    /// throughput improved by >= stable_acceptance_margin over the previous
    /// measurement. After shedding_transition_threshold consecutive rejections,
    /// transition to StableShedding.
    fn run_stable_probing(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let previous = state.recent_goodput[state.recent_goodput.len() - 2];
        let ratio = ratio(measured, previous);
        let accept = ratio >= 1.0 + self.config.stable_acceptance_margin;

        if accept {
            state.accepted_concurrency = state.probing_concurrency;
            state.consecutive_probes = 0;
        } else {
            state.consecutive_probes += 1;
            if state.consecutive_probes >= self.config.shedding_transition_threshold {
                state.phase = Phase::StableShedding;
                state.consecutive_probes = 0;
            }
        }

        state.probing_concurrency = (state.accepted_concurrency as f64
            * (1.0 + self.config.stable_probe_pct))
            .ceil() as usize;

        self.clamp_probe(state.probing_concurrency, state.accepted_concurrency)
    }

    /// StableShedding: accelerating downward probes. Each consecutive accepted
    /// probe reduces by an increasing multiple of stable_probe_pct (2%, 4%, 6%...).
    /// Accepts if throughput stays within stable_acceptance_margin of peak
    /// (max of recent history). On rejection, transitions back to StableProbing.
    fn run_stable_shedding(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let max_recent = state
            .recent_goodput
            .iter()
            .fold(0.0f64, |acc, &x| acc.max(x));
        let ratio = ratio(measured, max_recent);
        let accept = ratio >= 1.0 - self.config.stable_acceptance_margin;

        if accept {
            state.accepted_concurrency = state.probing_concurrency;
            state.consecutive_probes += 1;
            let reduction = self.config.stable_probe_pct * (state.consecutive_probes + 1) as f64;
            state.probing_concurrency =
                (state.accepted_concurrency as f64 * (1.0 - reduction)) as usize;
        } else {
            state.phase = Phase::StableProbing;
            state.consecutive_probes = 0;
            state.probing_concurrency = (state.accepted_concurrency as f64
                * (1.0 + self.config.stable_probe_pct))
                .ceil() as usize;
        }

        self.clamp_probe(state.probing_concurrency, state.accepted_concurrency)
    }

    /// Clamp probe to [min_probe_size, accepted + max_probe_size] and >= 1.
    fn clamp_probe(&self, probe: usize, accepted: usize) -> usize {
        probe
            .max(self.config.min_probe_size)
            .min(accepted + self.config.max_probe_size)
            .max(1)
    }
}

fn ratio(measured: f64, baseline: f64) -> f64 {
    if baseline > 0.0 {
        measured / baseline
    } else {
        0.0
    }
}

impl ConcurrencyController for AdaptiveConcurrencyController {
    fn target(&self) -> usize {
        self.target.load(Ordering::Relaxed)
    }

    fn on_completion(&self, sample: &CompletionSample) {
        self.network_window
            .add(sample.metrics.bytes_sent, sample.metrics.bytes_received);

        // Throttle error: immediate transition to shedding
        if sample.error == Some(ErrorKind::Throttle) {
            if let Ok(mut state) = self.state.lock() {
                if state.phase != Phase::StableShedding {
                    state.phase = Phase::StableShedding;
                    state.consecutive_probes = 0;
                }
            }
        }

        self.maybe_update_window();
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::WorkKind;
    use super::super::{CompletionSample, ErrorKind, IoMetrics};
    use super::*;

    struct TestController(AdaptiveConcurrencyController);

    impl TestController {
        fn new(config: AdaptiveConfig) -> Self {
            Self(AdaptiveConcurrencyController::new(config))
        }

        /// Feed a goodput measurement and run the algorithm. Returns new target.
        fn record_goodput(&self, goodput: f64) -> usize {
            let mut state = self.0.state.lock().unwrap();
            self.0.run_algorithm(&mut state, goodput)
        }

        fn phase(&self) -> Phase {
            self.0.state.lock().unwrap().phase
        }

        fn accepted(&self) -> usize {
            self.0.state.lock().unwrap().accepted_concurrency
        }

        fn consecutive_probes(&self) -> usize {
            self.0.state.lock().unwrap().consecutive_probes
        }

        fn history_len(&self) -> usize {
            self.0.state.lock().unwrap().recent_goodput.len()
        }
    }

    fn test_config() -> AdaptiveConfig {
        AdaptiveConfig::default()
    }

    fn sample(bytes_sent: u64) -> CompletionSample {
        CompletionSample {
            metrics: IoMetrics {
                bytes_sent,
                ..Default::default()
            },
            duration: Duration::from_millis(100),
            error: None,
            kind: WorkKind::Network,
        }
    }

    // -- Bootstrap --

    #[test]
    fn bootstrap_records_first_measurement_without_running_algorithm() {
        let tc = TestController::new(test_config());
        let target = tc.record_goodput(1000.0);
        // First measurement: no comparison possible, just set probing = initial * 2
        assert_eq!(tc.phase(), Phase::SlowStart);
        assert_eq!(target, 20); // 10 * 2.0
        assert_eq!(tc.accepted(), 10); // unchanged
        assert_eq!(tc.history_len(), 1);
    }

    // -- SlowStart --

    #[test]
    fn slow_start_accepts_on_sufficient_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        let target = tc.record_goodput(1200.0); // 20% improvement, > 10% threshold
        assert_eq!(tc.phase(), Phase::SlowStart);
        // accepted moves to probing (20), new probing = 20 * 2 = 40
        assert_eq!(tc.accepted(), 20);
        assert_eq!(target, 40);
    }

    #[test]
    fn slow_start_rejects_insufficient_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1050.0); // 5% improvement, < 10% threshold
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 10); // unchanged, probe rejected
    }

    // -- StableProbing --

    #[test]
    fn stable_probing_accepts_on_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1000.0); // reject -> StableProbing, accepted=10
                                   // Now in StableProbing. probing = ceil(10 * 1.02) = 11
        tc.record_goodput(1030.0); // 3% improvement over previous 1000, > 2%
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 11); // accepted the probe
        assert_eq!(tc.consecutive_probes(), 0); // reset on accept
    }

    #[test]
    fn stable_probing_rejects_to_shedding_after_threshold() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1000.0); // reject -> StableProbing
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding
        assert_eq!(tc.phase(), Phase::StableShedding);
    }

    // -- StableShedding --

    #[test]
    fn stable_shedding_accepts_when_throughput_near_peak() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1000.0); // -> StableProbing
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding
                                   // Peak is 1000. 985 is within 2%.
        tc.record_goodput(985.0);
        assert_eq!(tc.phase(), Phase::StableShedding);
        assert_eq!(tc.consecutive_probes(), 1); // accepted, accelerating
    }

    #[test]
    fn stable_shedding_rejects_to_probing_when_throughput_drops() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1000.0); // -> StableProbing
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding
                                   // Peak is 1000. 900 is 10% below -- well outside 2% margin.
        tc.record_goodput(900.0);
        assert_eq!(tc.phase(), Phase::StableProbing);
    }

    // -- Error-aware transitions --

    #[test]
    fn throttle_forces_shedding() {
        let config = AdaptiveConfig {
            window_duration: Duration::ZERO,
            min_completions_per_window: 0,
            ..Default::default()
        };
        let tc = TestController::new(config);

        let mut s = sample(8_000_000);
        s.error = Some(ErrorKind::Throttle);
        tc.0.on_completion(&s);

        assert_eq!(tc.phase(), Phase::StableShedding);
    }

    // -- Clamping --

    #[test]
    fn max_concurrency_clamps_target() {
        let config = AdaptiveConfig {
            initial_concurrency: 100,
            max_concurrency: Some(150),
            window_duration: Duration::ZERO,
            min_completions_per_window: 0,
            ..Default::default()
        };
        let tc = TestController::new(config);

        // Feed two windows of increasing goodput through on_completion
        // to trigger SlowStart accept and target update.
        for _ in 0..20 {
            tc.0.on_completion(&sample(8_000_000));
        }
        // Force window boundary by sleeping briefly (window_duration is ZERO)
        std::thread::sleep(Duration::from_millis(1));
        for _ in 0..20 {
            tc.0.on_completion(&sample(16_000_000));
        }

        // Target should be clamped to max_concurrency
        assert!(tc.0.target() <= 150);
    }

    #[test]
    fn min_probe_size_enforced() {
        let config = AdaptiveConfig {
            initial_concurrency: 100,
            min_probe_size: 50,
            ..Default::default()
        };
        let tc = TestController::new(config);

        let clamped = tc.0.clamp_probe(3, 100);
        assert_eq!(clamped, 50);
    }
}
