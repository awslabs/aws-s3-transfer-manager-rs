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
//! **SlowStart**: Additive growth (+64 per window). Accepts if throughput improved
//! by >= 10% over the previous window. On rejection, transitions to StableProbing.
//!
//! **StableProbing**: Linear upward probes (+2% per window). Accepts if throughput
//! exceeds the best recent measurement by >= 2%. After N consecutive rejections
//! (default 3), transitions to StableShedding.
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
//! a Mutex, touched only at window boundaries via `try_lock`.
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
    pub initial_concurrency: usize,
    pub max_concurrency: Option<usize>,
    pub window: WindowConfig,
    pub slow_start: SlowStartConfig,
    pub stable: StableConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct WindowConfig {
    /// Window duration for throughput measurement.
    pub duration: Duration,
    /// Minimum completions required before the algorithm evaluates.
    /// Prevents acting on noisy signal during cold start.
    pub min_completions: usize,
    /// Maximum time to wait for min_completions before evaluating anyway.
    pub max_duration: Duration,
}

/// SlowStart phase settings.
#[derive(Debug, Clone)]
pub(crate) struct SlowStartConfig {
    /// Minimum time between evaluations.
    pub evaluation_interval: Duration,
    /// Throughput must improve by this ratio to accept a growth step.
    pub acceptance_margin: f64,
    /// Additive growth per accepted window.
    pub max_growth: usize,
}

/// StableProbing and StableShedding settings.
#[derive(Debug, Clone)]
pub(crate) struct StableConfig {
    /// Acceptance threshold for probing (improvement) and shedding (degradation).
    pub acceptance_margin: f64,
    /// Probe size as fraction of current concurrency.
    pub probe_pct: f64,
    /// Minimum change per probe step.
    pub min_probe_size: usize,
    /// Maximum change per probe step.
    pub max_probe_size: usize,
    /// Recent goodput measurements to retain.
    pub history_size: usize,
    /// Consecutive rejections before transitioning to StableShedding.
    pub shedding_transition_threshold: usize,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            initial_concurrency: 32,
            max_concurrency: None,
            window: WindowConfig {
                // 500ms is responsive while still smoothing per-request variance.
                duration: Duration::from_millis(500),
                // Enough completions for a meaningful throughput estimate.
                // Prevents acting on noisy signal during cold start.
                min_completions: 10,
                // Don't wait forever for completions on a slow connection.
                max_duration: Duration::from_secs(5),
            },
            slow_start: SlowStartConfig {
                // 500ms between evaluations. Fast enough for responsive ramp,
                // long enough for meaningful throughput measurement.
                evaluation_interval: Duration::from_millis(500),
                // 10% improvement required to accept growth step.
                acceptance_margin: 0.10,
                // +64 connections per accepted window.
                max_growth: 64,
            },
            stable: StableConfig {
                // 2% improvement (probing) or 2% of peak (shedding).
                acceptance_margin: 0.02,
                // 2% of current concurrency per probe step.
                probe_pct: 0.02,
                // Don't probe smaller than 5 or larger than 200 per step.
                min_probe_size: 5,
                max_probe_size: 200,
                // StableShedding compares against max of last 10 windows.
                history_size: 10,
                // 3 consecutive rejections in StableProbing triggers shedding.
                shedding_transition_threshold: 3,
            },
        }
    }
}

/// Algorithm phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Additive growth. Finding the ballpark.
    SlowStart = 0,
    /// Small upward probes (+2%). Tracking the optimum.
    StableProbing = 1,
    /// Accelerating downward probes. Shedding excess concurrency.
    StableShedding = 2,
}

impl Phase {
    fn from_usize(v: usize) -> Self {
        match v {
            0 => Phase::SlowStart,
            1 => Phase::StableProbing,
            2 => Phase::StableShedding,
            _ => Phase::StableProbing,
        }
    }
}

/// Mutable algorithm state, protected by Mutex. Touched at window boundaries
/// and during phase transitions (including throttle-triggered transitions from
/// `on_completion`).
#[derive(Debug)]
struct AlgorithmState {
    /// Last concurrency level where a probe was accepted.
    accepted_concurrency: usize,
    /// Concurrency level being tested in the current window.
    probing_concurrency: usize,
    /// Circular buffer of recent goodput measurements (bytes/sec).
    recent_goodput: VecDeque<f64>,
    /// Consecutive probe results in the same direction within the current phase.
    /// StableProbing: consecutive rejections (triggers shedding at threshold).
    /// StableShedding: consecutive accepts (accelerates reduction).
    /// Unused during SlowStart.
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
    /// Current number of in-flight work items.
    in_flight: AtomicUsize,
    /// Peak in-flight during the current measurement window.
    peak_in_flight: AtomicUsize,
    /// Network bytes accumulated during the current measurement window (sent + received).
    ///
    /// Note: control plane completions (CreateMPU, CompleteMPU) contribute to
    /// the completion count but carry zero bytes, slightly diluting the goodput
    /// signal. Accepted as noise -- not worth filtering.
    network_window: IOWindow,
    /// Algorithm phase. Stored as AtomicUsize for lock-free reads.
    phase: AtomicUsize,
    /// Algorithm state. Only accessed at window boundaries.
    state: Mutex<AlgorithmState>,
}

impl AdaptiveConcurrencyController {
    pub(crate) fn new(config: AdaptiveConfig) -> Self {
        let initial = config.initial_concurrency;
        Self {
            target: AtomicUsize::new(initial),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
            network_window: IOWindow::new(),
            phase: AtomicUsize::new(Phase::SlowStart as usize),
            state: Mutex::new(AlgorithmState {
                accepted_concurrency: initial,
                probing_concurrency: initial,
                recent_goodput: VecDeque::with_capacity(config.stable.history_size),
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
        let window_duration = match Phase::from_usize(self.phase.load(Ordering::Relaxed)) {
            Phase::SlowStart => self.config.slow_start.evaluation_interval,
            _ => self.config.window.duration,
        };
        if elapsed < window_duration {
            return;
        }

        let completions = self.network_window.completions.load(Ordering::Relaxed);
        if (completions as usize) < self.config.window.min_completions {
            if elapsed < self.config.window.max_duration {
                // Not enough signal yet, keep waiting.
                return;
            }
            // Window expired with too few completions (inter-transfer gap).
            // Discard — don't record or evaluate.
            self.network_window.take();
            state.window_start = Instant::now();
            self.peak_in_flight.swap(0, Ordering::Relaxed);
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY,
                completions,
                window_ms = elapsed.as_millis() as u64,
                "window discarded, insufficient completions"
            );
            return;
        }

        let (bytes, completions) = self.network_window.take();
        let elapsed = state.window_start.elapsed();
        state.window_start = Instant::now();
        let peak = self.peak_in_flight.swap(0, Ordering::Relaxed);

        let goodput = if elapsed.is_zero() {
            0.0
        } else {
            bytes as f64 / elapsed.as_secs_f64()
        };

        tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY,
            goodput_mbps = goodput / 1_000_000.0,
            peak,
            completions,
            window_ms = elapsed.as_millis() as u64,
            "concurrency window evaluated"
        );

        let new_target = self.run_algorithm(&mut state, goodput, peak);
        let clamped = match self.config.max_concurrency {
            Some(max) => new_target.min(max),
            None => new_target,
        }
        .max(1);
        let old_target = self.target.swap(clamped, Ordering::Relaxed);
        if old_target != clamped {
            tracing::debug!(target: crate::telemetry::TARGET_CONCURRENCY, old_target, new_target = clamped, phase = ?Phase::from_usize(self.phase.load(Ordering::Relaxed)), "concurrency target updated");
        }
    }

    /// Transition to a new phase. Clears history only when leaving SlowStart
    /// (different operating regime). StableProbing ↔ StableShedding transitions
    /// preserve history — they're fine-tuning around the same operating point.
    fn transition_to(&self, state: &mut AlgorithmState, new_phase: Phase) {
        let old_phase = Phase::from_usize(self.phase.load(Ordering::Relaxed));
        self.phase.store(new_phase as usize, Ordering::Relaxed);
        if old_phase == Phase::SlowStart {
            state.recent_goodput.clear();
        }
        state.consecutive_probes = 0;
        tracing::debug!(target: crate::telemetry::TARGET_CONCURRENCY, from = ?old_phase, to = ?new_phase, accepted = state.accepted_concurrency, "concurrency phase transition");
    }

    /// Run the three-state algorithm with a new goodput measurement.
    /// Returns the next probing concurrency level.
    fn run_algorithm(
        &self,
        state: &mut AlgorithmState,
        goodput: f64,
        peak_in_flight: usize,
    ) -> usize {
        // Record measurement before comparing
        state.recent_goodput.push_back(goodput);
        if state.recent_goodput.len() > self.config.stable.history_size {
            state.recent_goodput.pop_front();
        }

        // Bootstrap: need at least 2 measurements to compute ratios
        if state.recent_goodput.len() < 2 {
            if Phase::from_usize(self.phase.load(Ordering::Relaxed)) == Phase::SlowStart {
                state.probing_concurrency =
                    state.accepted_concurrency + self.config.slow_start.max_growth;
            }
            return self.clamp_probe(state.probing_concurrency, state.accepted_concurrency);
        }

        // Exercised check: reject probe if we never actually ran at the probed level.
        // If peak in-flight didn't reach accepted_concurrency, the throughput measurement
        // doesn't reflect the probed concurrency — skip the algorithm and re-probe.
        if peak_in_flight < state.accepted_concurrency {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY,
                peak_in_flight,
                accepted = state.accepted_concurrency,
                "probe skipped, not exercised"
            );
            return self.clamp_probe(state.probing_concurrency, state.accepted_concurrency);
        }

        match Phase::from_usize(self.phase.load(Ordering::Relaxed)) {
            Phase::SlowStart => self.run_slow_start(state, goodput),
            Phase::StableProbing => self.run_stable_probing(state, goodput),
            Phase::StableShedding => self.run_stable_shedding(state, goodput),
        }
    }

    /// SlowStart: additive growth (+max_growth per window). Accepts if throughput
    /// improved by >= acceptance_margin. On rejection, transitions to StableProbing.
    fn run_slow_start(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let previous = state.recent_goodput[state.recent_goodput.len() - 2];
        let ratio = ratio(measured, previous);
        let accept = ratio >= 1.0 + self.config.slow_start.acceptance_margin;

        if accept {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::SlowStart, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, "probe accepted");
            state.accepted_concurrency = state.probing_concurrency;
            state.probing_concurrency =
                state.accepted_concurrency + self.config.slow_start.max_growth;
        } else {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::SlowStart, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, threshold = self.config.slow_start.acceptance_margin, "SlowStart plateau");
            self.transition_to(state, Phase::StableProbing);
            state.probing_concurrency = (state.accepted_concurrency as f64
                * (1.0 + self.config.stable.probe_pct))
                .ceil() as usize;
        }

        self.clamp_probe(state.probing_concurrency, state.accepted_concurrency)
    }

    /// StableProbing: linear upward probes (+stable_probe_pct). Accepts if
    /// throughput exceeds the best recent measurement by >= stable_acceptance_margin.
    /// Comparing against the best (not just previous) prevents noise-driven
    /// acceptance at high throughput. After shedding_transition_threshold
    /// consecutive rejections, transitions to StableShedding.
    fn run_stable_probing(&self, state: &mut AlgorithmState, measured: f64) -> usize {
        let best_recent = state
            .recent_goodput
            .iter()
            .rev()
            .skip(1)
            .copied()
            .fold(0.0f64, f64::max);
        let ratio = ratio(measured, best_recent);
        let accept = ratio >= 1.0 + self.config.stable.acceptance_margin;

        if accept {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::StableProbing, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, "probe accepted");
            state.accepted_concurrency = state.probing_concurrency;
            state.consecutive_probes = 0;
        } else {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::StableProbing, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, threshold = self.config.stable.acceptance_margin, "probe rejected");
            state.consecutive_probes += 1;
            if state.consecutive_probes >= self.config.stable.shedding_transition_threshold {
                self.transition_to(state, Phase::StableShedding);
            }
        }

        state.probing_concurrency = (state.accepted_concurrency as f64
            * (1.0 + self.config.stable.probe_pct))
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
        let accept = ratio >= 1.0 - self.config.stable.acceptance_margin;

        if accept {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::StableShedding, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, "probe accepted");
            state.accepted_concurrency = state.probing_concurrency;
            state.consecutive_probes += 1;
            let reduction = self.config.stable.probe_pct * (state.consecutive_probes + 1) as f64;
            state.probing_concurrency =
                (state.accepted_concurrency as f64 * (1.0 - reduction)) as usize;
        } else {
            tracing::trace!(target: crate::telemetry::TARGET_CONCURRENCY, phase = ?Phase::StableShedding, probing = state.probing_concurrency, accepted = state.accepted_concurrency, ratio, threshold = self.config.stable.acceptance_margin, "probe rejected");
            self.transition_to(state, Phase::StableProbing);
            state.probing_concurrency = (state.accepted_concurrency as f64
                * (1.0 + self.config.stable.probe_pct))
                .ceil() as usize;
        }

        self.clamp_probe(state.probing_concurrency, state.accepted_concurrency)
    }

    /// Clamp probe to [min_probe_size, accepted + max_probe_size] and >= 1.
    fn clamp_probe(&self, probe: usize, accepted: usize) -> usize {
        probe
            .max(self.config.stable.min_probe_size)
            .min(accepted + self.config.stable.max_probe_size)
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

    fn on_dispatch(&self) {
        let n = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_in_flight.fetch_max(n, Ordering::Relaxed);
    }

    fn on_completion(&self, sample: &CompletionSample) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.network_window
            .add(sample.metrics.bytes_sent, sample.metrics.bytes_received);

        // Throttle error: immediate transition to shedding
        if sample.error == Some(ErrorKind::Throttle) {
            if let Ok(mut state) = self.state.lock() {
                if Phase::from_usize(self.phase.load(Ordering::Relaxed)) != Phase::StableShedding {
                    self.transition_to(&mut state, Phase::StableShedding);
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
        /// Defaults peak in-flight to accepted_concurrency (exercised).
        fn record_goodput(&self, goodput: f64) -> usize {
            let mut state = self.0.state.lock().unwrap();
            let peak = state.accepted_concurrency;
            self.0.run_algorithm(&mut state, goodput, peak)
        }

        /// Feed a goodput measurement with an explicit peak in-flight value.
        fn record_goodput_with_peak(&self, goodput: f64, peak: usize) -> usize {
            let mut state = self.0.state.lock().unwrap();
            self.0.run_algorithm(&mut state, goodput, peak)
        }

        fn phase(&self) -> Phase {
            Phase::from_usize(self.0.phase.load(Ordering::Relaxed))
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
        // First measurement: bootstrap sets probing = 32 + 64 = 96
        assert_eq!(tc.phase(), Phase::SlowStart);
        assert_eq!(target, 96);
        assert_eq!(tc.accepted(), 32); // unchanged
        assert_eq!(tc.history_len(), 1);
    }

    // -- SlowStart windowed evaluation --

    #[test]
    fn slow_start_stays_in_phase_on_throughput_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1200.0); // 20% improvement, > 10% threshold
                                   // Stays in SlowStart, accepted moves to probing level
        assert_eq!(tc.phase(), Phase::SlowStart);
        assert_eq!(tc.accepted(), 96); // accepted = previous probing (32+64)
    }

    #[test]
    fn slow_start_rejects_insufficient_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1050.0); // 5% improvement, < 10% threshold -> StableProbing
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 32); // accepted = current_target
        assert_eq!(tc.history_len(), 0); // cleared by transition
    }

    // -- StableProbing --

    #[test]
    fn stable_probing_accepts_on_improvement() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (history cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap
        tc.record_goodput(1030.0); // probing=33, 1030/1000=1.03 > 1.02, accept
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 33); // accepted the probe
        assert_eq!(tc.consecutive_probes(), 0); // reset on accept
    }

    #[test]
    fn stable_probing_rejects_to_shedding_after_threshold() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding
        assert_eq!(tc.phase(), Phase::StableShedding);
    }

    // -- StableShedding --

    #[test]
    fn stable_shedding_accepts_when_throughput_near_peak() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding (history preserved)
        tc.record_goodput(985.0); // peak=1000, 985 within 2%, accept
        assert_eq!(tc.phase(), Phase::StableShedding);
        assert_eq!(tc.consecutive_probes(), 1); // accepted, accelerating
    }

    #[test]
    fn stable_shedding_rejects_to_probing_when_throughput_drops() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        tc.record_goodput(1000.0); // reject 3 -> StableShedding (history preserved)
        tc.record_goodput(900.0); // peak=1000, 10% below, outside 2% margin
        assert_eq!(tc.phase(), Phase::StableProbing);
    }

    // -- Error-aware transitions --

    #[test]
    fn throttle_forces_shedding() {
        let config = AdaptiveConfig {
            window: WindowConfig {
                duration: Duration::ZERO,
                min_completions: 0,
                ..AdaptiveConfig::default().window
            },
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
            window: WindowConfig {
                duration: Duration::ZERO,
                min_completions: 0,
                ..AdaptiveConfig::default().window
            },
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
            stable: StableConfig {
                min_probe_size: 50,
                ..AdaptiveConfig::default().stable
            },
            ..Default::default()
        };
        let tc = TestController::new(config);

        let clamped = tc.0.clamp_probe(3, 100);
        assert_eq!(clamped, 50);
    }

    // -- Exercised check --

    #[test]
    fn exercised_check_rejects_unexercised_probe() {
        // Test exercised check in StableProbing where it matters for windowed evaluation.
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap, accepted=32
        let target = tc.record_goodput_with_peak(5000.0, 5); // peak=5, below accepted=32
        assert_eq!(tc.accepted(), 32); // not accepted, probe not exercised
        assert!(target >= 32); // still probing
    }

    #[test]
    fn exercised_check_passes_when_exercised() {
        // In StableProbing, peak in-flight matches accepted, throughput improved.
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // SlowStart reject -> StableProbing (cleared)
        tc.record_goodput(1000.0); // StableProbing bootstrap, accepted=32
        tc.record_goodput_with_peak(1030.0, 32); // peak=32, 3% > 2%
        assert_eq!(tc.accepted(), 33); // accepted the probe
    }

    // -- SlowStart additive growth --

    #[test]
    fn slow_start_additive_growth() {
        let tc = TestController::new(test_config());
        // Bootstrap: first measurement, probing = 32 + 64 = 96
        let target = tc.record_goodput(1000.0);
        assert_eq!(target, 96);
        assert_eq!(tc.accepted(), 32);
        // Accept: throughput improved 20%, accepted = 96, probing = 96 + 64 = 160
        let target = tc.record_goodput(1200.0);
        assert_eq!(tc.accepted(), 96);
        assert_eq!(target, 160);
    }

    #[test]
    fn slow_start_exits_on_plateau() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1050.0); // 5% < 10% threshold -> StableProbing
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.accepted(), 32); // accepted stays at initial (probe rejected)
    }

    // -- Gap filtering --

    #[test]
    fn gap_windows_not_recorded() {
        // Windows with very few completions (inter-transfer gaps) should be
        // discarded, not recorded in history.
        let config = AdaptiveConfig {
            window: WindowConfig {
                duration: Duration::ZERO,
                min_completions: 10,
                max_duration: Duration::from_secs(5),
            },
            ..Default::default()
        };
        let controller = AdaptiveConcurrencyController::new(config);
        // Simulate a gap: only 3 completions (below min_completions=10)
        for _ in 0..3 {
            controller.on_dispatch();
            controller.on_completion(&sample(1_000_000));
        }
        // Force window evaluation by sleeping past duration
        std::thread::sleep(Duration::from_millis(1));
        // One more completion to trigger maybe_update_window
        controller.on_dispatch();
        controller.on_completion(&sample(1_000_000));
        // History should be empty — gap window was discarded
        let state = controller.state.lock().unwrap();
        assert_eq!(
            state.recent_goodput.len(),
            0,
            "gap window was recorded in history"
        );
    }

    // -- History management --

    #[test]
    fn history_cleared_on_slow_start_exit() {
        let tc = TestController::new(test_config());
        tc.record_goodput(1000.0); // SlowStart bootstrap
        assert_eq!(tc.history_len(), 1);
        tc.record_goodput(1000.0); // SlowStart reject → StableProbing
        assert_eq!(tc.phase(), Phase::StableProbing);
        assert_eq!(tc.history_len(), 0); // clean slate for stable phase
    }

    #[test]
    fn history_preserved_between_stable_phases() {
        let tc = TestController::new(test_config());
        // Get to StableProbing
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // → StableProbing (history cleared)

        // Build up history in StableProbing
        tc.record_goodput(1000.0); // bootstrap
        tc.record_goodput(1000.0); // reject 1
        tc.record_goodput(1000.0); // reject 2
        let history_before = tc.history_len();
        assert!(history_before >= 3);

        tc.record_goodput(1000.0); // reject 3 → StableShedding
        assert_eq!(tc.phase(), Phase::StableShedding);
        // History must survive the transition — one more entry was added
        assert!(
            tc.history_len() > history_before,
            "history lost on StableProbing → StableShedding: {} -> {}",
            history_before,
            tc.history_len()
        );
    }

    #[test]
    fn stable_probing_does_not_creep_on_flat_throughput() {
        let tc = TestController::new(test_config());
        // Get to StableProbing
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // → StableProbing

        // Feed flat throughput for many windows
        let initial_accepted = tc.accepted();
        for _ in 0..20 {
            tc.record_goodput(1000.0);
        }
        // Accepted should not have crept upward significantly
        let final_accepted = tc.accepted();
        assert!(
            final_accepted <= initial_accepted + 5,
            "StableProbing crept from {} to {} on flat throughput",
            initial_accepted,
            final_accepted
        );
    }

    #[test]
    fn shedding_retains_peak_knowledge_from_probing() {
        let tc = TestController::new(test_config());
        // Get to StableProbing
        tc.record_goodput(1000.0); // SlowStart bootstrap
        tc.record_goodput(1000.0); // → StableProbing

        // StableProbing sees high throughput
        tc.record_goodput(5000.0); // bootstrap
        tc.record_goodput(5000.0); // reject 1 (5000 vs best 5000 = 1.0 < 1.02)
        tc.record_goodput(5000.0); // reject 2
        tc.record_goodput(5000.0); // reject 3 → StableShedding
        assert_eq!(tc.phase(), Phase::StableShedding);

        // StableShedding should compare against 5000 (peak from probing history).
        // A measurement of 4000 is 20% below peak — should reject.
        tc.record_goodput(4000.0);
        assert_eq!(
            tc.phase(),
            Phase::StableProbing,
            "StableShedding accepted 4000 vs peak 5000 — lost peak knowledge"
        );
    }
}
